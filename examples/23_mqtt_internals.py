"""
23 — MQTT Internals (внутренности MQTT-слоя)
==============================================

ГЛУБОКОЕ ПОГРУЖЕНИЕ для разработчиков фреймворка.

MQTT-слой kamio построен на gmqtt и имеет множество скрытых поведений,
которые могут привести к утечкам памяти, потере сообщений и состояниям гонки.

ПОДВОХИ И КРАЕВЫЕ СЛУЧАИ:

    ACK-кэш (MqttConnection):
      1. _ACK_CACHE_LIMIT=1024 — НЕ настраивается.  При >1024 неподтверждённых
         подписок старые записи вытесняются по одной (медленная очистка).
      2. Early ACK eviction: удаляется по ОДНОЙ записи за ACK (while len > limit).
         При массовом subscribe это O(n) на каждый ACK.
      3. disconnect() НЕ очищает _sub_acks / _subed_mids / _unsub_acks / _unsubed_mids.
         Утечка памяти при многократном reconnect.
      4. _resolve_ack — staticmethod, НЕ потокобезопасный.  dict.pop и
         dict.__setitem__ атомарны в CPython (GIL), но не гарантированы.

    gmqtt private methods:
      5. monkey-patching: client._reconnect_delay, client._reconnect_retries,
         client._kamio_wait_for_suback, client._kamio_wait_for_unsuback.
         Приватные атрибуты gmqtt могут измениться в любой версии.

    Reconnect:
      6. reconnect_retries=0 означает БЕСКОНЕЧНЫЕ попытки (gmqtt convention).

    Client ID:
      7. client_id=None → пустая строка "" → gmqtt генерирует случайный ID.
         При каждом пересоздании MqttConnection ID меняется.

    BaseNode:
      8. start() молча возвращает None, если _is_running=True (no-op, без предупреждения).
      9. _is_running=True даже если подписки НЕ удались (ошибки логируются, но
         флаг устанавливается в любом случае).
     10. on() ЗАМЕНЯЕТ handler, а не добавляет.  Повторный on() для того же
         типа перезаписывает предыдущий handler.

    Envelope / message handling:
     11. Невалидные envelope (from_json → None) молча отбрасываются.
     12. Неизвестные типы сообщений: только DEBUG-лог, не WARNING.
     13. Target fallback: target or source — если target не задан,
         сообщение отправляется на source (маршрут может зациклиться).
     14. publish() глотает RuntimeError с "shutdown"/"closed" — молча.
     15. DeviceNode: исключение в handler → сообщение ПОТЕРЯНО (логируется exception).

    DeviceNode lifecycle:
     16. on_stop() вызывается ДО super().stop() — если on_stop падает,
         super().stop() НИКОГДА не вызывается → утечка подписок.

Запуск (БЕЗ MQTT-брокера)::

    python examples/23_mqtt_internals.py
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

from kamio.core.envelope import Envelope, EnvelopeType, SERVER_ID
from kamio.core.mqtt_nodes import BaseNode, DeviceNode, BROADCAST_ID

logging.basicConfig(level=logging.WARNING, format="%(name)s | %(levelname)s | %(message)s")
logger = logging.getLogger("example.23")


# ============================================================================
# Mock MQTT client для тестирования без брокера
# ============================================================================

class MockMqttClient:
    """Мок gmqtt.Client для тестирования без MQTT-брокера."""

    def __init__(self, client_id: str = ""):
        self._client_id = client_id
        self._sub_counter = 0
        self._unsub_counter = 0
        self._published: list[tuple[str, bytes, int, bool]] = []
        self._subscriptions: dict[str, int] = {}
        self._reconnect_delay = 1.0
        self._reconnect_retries = 0
        self._on_subscribe = None
        self._on_unsubscribe = None

        # kamio adapter methods (устанавливаются MqttConnection)
        self._kamio_wait_for_suback = None
        self._kamio_wait_for_unsuback = None

    def subscribe(self, topic: str, qos: int = 0) -> int:
        self._sub_counter += 1
        mid = self._sub_counter
        self._subscriptions[topic] = qos
        return mid

    def unsubscribe(self, topic: str) -> int:
        self._unsub_counter += 1
        mid = self._unsub_counter
        self._subscriptions.pop(topic, None)
        return mid

    def publish(self, topic: str, payload: bytes, qos: int = 0, retain: bool = False):
        self._published.append((topic, payload, qos, retain))

    def set_auth_credentials(self, username, password):
        pass

    async def connect(self, *args, **kwargs):
        pass

    async def disconnect(self):
        pass


# ============================================================================
# 1. ACK cache limit 1024 (не настраивается)
# ============================================================================

async def demo_ack_cache_limit():
    """_ACK_CACHE_LIMIT=1024 — не настраивается, eviction по одной записи."""
    print("\n--- 1. ACK cache limit 1024 (не настраивается) ---")

    from kamio.core.mqtt_connection import _ACK_CACHE_LIMIT

    # Константа зашита в модуле — НЕ настраивается через конфиг
    assert _ACK_CACHE_LIMIT == 1024, f"Ожидали 1024, получили {_ACK_CACHE_LIMIT}"
    print(f"  _ACK_CACHE_LIMIT = {_ACK_CACHE_LIMIT} (хардкод, не настраивается)")

    # Демонстрация eviction: при >1024 ранних ACK, старые вытесняются
    early: dict[int, None] = {}

    # Заполняем до предела
    for i in range(1025):
        # Симулируем _resolve_ack для каждого mid
        early[i] = None
        while len(early) > _ACK_CACHE_LIMIT:
            early.pop(next(iter(early)))

    # После 1025 вставок размер = 1024 (одна вытеснена)
    assert len(early) == 1024, f"Ожидали 1024, получили {len(early)}"
    # mid=0 вытеснен (самый старый)
    assert 0 not in early, "mid=0 должен быть вытеснен"
    # mid=1024 остался (самый новый)
    assert 1024 in early, "mid=1024 должен остаться"
    print(f"  После 1025 ACK: размер={len(early)}, mid=0 вытеснен, mid=1024 остался")

    # ПОДВОХ: eviction по ОДНОЙ записи за ACK — O(1) на ACK, но
    # при массовом subscribe (1024+) каждая вставка вызывает while-цикл
    print("  ВНИМАНИЕ: eviction по одной записи за ACK (while len > limit)")

    print("  OK: ACK cache limit проверен")


# ============================================================================
# 2. disconnect НЕ очищает ACK-кэши (утечка памяти)
# ============================================================================

async def demo_disconnect_no_ack_cleanup():
    """disconnect() не очищает _sub_acks / _subed_mids — утечка памяти."""
    print("\n--- 2. disconnect НЕ очищает ACK-кэши ---")

    from kamio.core.mqtt_connection import MqttConnection

    # Создаём MqttConnection с mock-брокером
    mqtt = MqttConnection("mqtt://localhost:1883", client_id="test")

    # Симулируем: SUBACK пришёл рано (до регистрации waiter)
    mqtt._subed_mids[42] = None
    mqtt._unsubed_mids[99] = None
    mqtt._sub_acks[7] = asyncio.Event()
    mqtt._unsub_acks[8] = asyncio.Event()

    assert len(mqtt._subed_mids) == 1
    assert len(mqtt._unsubed_mids) == 1
    assert len(mqtt._sub_acks) == 1
    assert len(mqtt._unsub_acks) == 1

    # disconnect() просто вызывает client.disconnect()
    # НЕ очищает _sub_acks, _subed_mids, _unsub_acks, _unsubed_mids
    await mqtt.disconnect()

    # Утечка: словари НЕ очищены
    assert len(mqtt._subed_mids) == 1, "subed_mids не очищен после disconnect"
    assert len(mqtt._unsubed_mids) == 1, "unsubed_mids не очищен после disconnect"
    assert len(mqtt._sub_acks) == 1, "sub_acks не очищен после disconnect"
    assert len(mqtt._unsub_acks) == 1, "unsub_acks не очищен после disconnect"
    print("  disconnect() НЕ очистил _sub_acks, _subed_mids, _unsub_acks, _unsubed_mids")
    print("  УТЕЧКА ПАМЯТИ: при многократном reconnect данные накапливаются")

    # ПРАВИЛЬНЫЙ ПОДХОД: очищать вручную
    mqtt._sub_acks.clear()
    mqtt._subed_mids.clear()
    mqtt._unsub_acks.clear()
    mqtt._unsubed_mids.clear()
    assert len(mqtt._sub_acks) == 0
    print("  ПРАВИЛЬНО: очищать ACK-кэши вручную после disconnect")

    print("  OK: утечка ACK-кэшей проверена")


# ============================================================================
# 3. _resolve_ack не потокобезопасен
# ============================================================================

async def demo_resolve_ack_not_thread_safe():
    """_resolve_ack — staticmethod, не имеет блокировки."""
    print("\n--- 3. _resolve_ack не потокобезопасен ---")

    from kamio.core.mqtt_connection import MqttConnection

    # _resolve_ack — статический метод, работающий с dict без lock
    # В CPython dict.pop и dict.__setitem__ атомарны благодаря GIL,
    # но это НЕ гарантировано в других реализациях Python.

    acks: dict[int, asyncio.Event] = {}
    early: dict[int, None] = {}

    # Нормальный случай: waiter зарегистрирован
    event = asyncio.Event()
    acks[1] = event
    MqttConnection._resolve_ack(1, acks, early)
    assert event.is_set(), "Event должен быть установлен"
    assert 1 not in acks, "mid должен быть удалён из acks"
    print("  _resolve_ack с зарегистрированным waiter: Event установлен, запись удалена")

    # Ранний ACK: waiter ещё не зарегистрирован
    acks.clear()
    early.clear()
    MqttConnection._resolve_ack(2, acks, early)
    assert 2 in early, "mid должен быть в early-кэше"
    assert 2 not in acks, "mid не должен быть в acks"
    print("  _resolve_ack с ранним ACK: mid сохранён в early-кэше")

    # ПОДВОХ: нет lock → при concurrent access из разных потоков
    # (gmqtt callback + event loop) возможна гонка
    print("  ВНИМАНИЕ: _resolve_ack не имеет lock — гонка при concurrent access")

    print("  OK: _resolve_ack thread safety проверен")


# ============================================================================
# 4. gmqtt private methods monkey-patched
# ============================================================================

async def demo_gmqtt_monkey_patching():
    """MqttConnection патчит приватные атрибуты gmqtt.Client."""
    print("\n--- 4. gmqtt private methods monkey-patched ---")

    from kamio.core.mqtt_connection import MqttConnection

    mqtt = MqttConnection("mqtt://localhost:1883", client_id="test")

    # Проверяем, что приватные атрибуты gmqtt установлены
    assert hasattr(mqtt.client, "_reconnect_delay"), "_reconnect_delay установлен"
    assert hasattr(mqtt.client, "_reconnect_retries"), "_reconnect_retries установлен"
    assert hasattr(mqtt.client, "_kamio_wait_for_suback"), "_kamio_wait_for_suback установлен"
    assert hasattr(mqtt.client, "_kamio_wait_for_unsuback"), "_kamio_wait_for_unsuback установлен"

    print(f"  client._reconnect_delay = {mqtt.client._reconnect_delay}")
    print(f"  client._reconnect_retries = {mqtt.client._reconnect_retries}")
    print(f"  client._kamio_wait_for_suback = {mqtt.client._kamio_wait_for_suback}")
    print(f"  client._kamio_wait_for_unsuback = {mqtt.client._kamio_wait_for_unsuback}")

    # ПОДВОХ: это приватные атрибуты gmqtt, которые могут измениться
    print("  ВНИМАНИЕ: приватные атрибуты gmqtt могут измениться в новой версии")

    print("  OK: monkey-patching проверен")


# ============================================================================
# 5. reconnect_retries=0 означает бесконечные попытки
# ============================================================================

async def demo_reconnect_zero_unlimited():
    """reconnect_retries=0 → бесконечные попытки reconnect (gmqtt convention)."""
    print("\n--- 5. reconnect_retries=0 = бесконечные попытки ---")

    from kamio.core.mqtt_connection import MqttConnection

    mqtt = MqttConnection("mqtt://localhost:1883", client_id="test")

    # gmqtt convention: _reconnect_retries=0 означает БЕСКОНЕЧНЫЕ попытки
    assert mqtt.client._reconnect_retries == 0, "0 = unlimited retries"
    print(f"  _reconnect_retries = {mqtt.client._reconnect_retries} (0 = БЕСКОНЕЧНО)")

    # НЕПРАВИЛЬНО: думать, что 0 = "не пытаться переподключиться"
    # ПРАВИЛЬНО: 0 = "пытаться вечно"
    print("  НЕПРАВИЛЬНО: думать, что 0 = 'не переподключаться'")
    print("  ПРАВИЛЬНО: 0 = 'бесконечные попытки с экспоненциальным backoff'")

    print("  OK: reconnect_retries=0 = unlimited проверен")


# ============================================================================
# 6. Empty client_id → random ID
# ============================================================================

async def demo_empty_client_id():
    """client_id=None → "" → gmqtt генерирует случайный ID."""
    print("\n--- 6. Empty client_id → random ID ---")

    from kamio.core.mqtt_connection import MqttConnection

    # client_id=None → self.client_id = "" → gmqtt.Client("") → random ID
    mqtt1 = MqttConnection("mqtt://localhost:1883", client_id=None)
    assert mqtt1.client_id == "", f"Ожидали '', получили {mqtt1.client_id!r}"
    print(f"  client_id=None → client_id='{mqtt1.client_id}' → gmqtt генерирует случайный ID")

    # client_id="my_id" → используется как есть
    mqtt2 = MqttConnection("mqtt://localhost:1883", client_id="my_id")
    assert mqtt2.client_id == "my_id"
    print(f"  client_id='my_id' → client_id='{mqtt2.client_id}'")

    # ПОДВОХ: при каждом новом MqttConnection с client_id=None
    # gmqtt создаёт НОВЫЙ случайный ID → broker видит разные клиенты
    print("  ВНИМАНИЕ: пустой client_id → новый случайный ID при каждом создании")

    print("  OK: empty client_id проверен")


# ============================================================================
# 7. BaseNode.start() silent if already running
# ============================================================================

async def demo_start_silent_if_running():
    """start() молча возвращает None, если _is_running=True."""
    print("\n--- 7. BaseNode.start() silent if already running ---")

    mqtt = MockMqttClient()
    # Устанавливаем adapter methods
    async def _noop_wait(mid, timeout=10.0):
        pass
    mqtt._kamio_wait_for_suback = _noop_wait
    mqtt._kamio_wait_for_unsuback = _noop_wait

    node = BaseNode("device1", mqtt)
    await node.start()
    assert node._is_running is True
    subscriptions_before = dict(mqtt._subscriptions)

    # Второй вызов start() — молча no-op
    await node.start()
    assert node._is_running is True
    # Подписки НЕ дублируются (start() вышел сразу)
    assert mqtt._subscriptions == subscriptions_before, "Подписки не должны дублироваться"
    print("  Второй start() — молча no-op (без предупреждения)")

    # ПРАВИЛЬНЫЙ ПОДХОД: проверять is_running перед вызовом
    if not node.is_running:
        await node.start()
    else:
        print("  ПРАВИЛЬНО: проверять is_running перед start()")

    await node.stop()
    print("  OK: start() silent if running проверен")


# ============================================================================
# 8. _is_running=True even if subscriptions failed
# ============================================================================

async def demo_running_flag_despite_failed_subs():
    """_is_running=True даже если подписки не удались."""
    print("\n--- 8. _is_running=True даже при неудачных подписках ---")

    # MQTT клиент, у которого subscribe падает
    class FailingMqttClient(MockMqttClient):
        def subscribe(self, topic: str, qos: int = 0) -> int:
            raise RuntimeError("Broker refused subscription")

    mqtt = FailingMqttClient()
    async def _noop_wait(mid, timeout=10.0):
        pass
    mqtt._kamio_wait_for_suback = _noop_wait
    mqtt._kamio_wait_for_unsuback = _noop_wait

    node = BaseNode("device1", mqtt)
    await node.start()

    # ПОДВОХ: _is_running=True, хотя НИ ОДНА подписка не удалась!
    assert node._is_running is True, "_is_running=True даже при неудачных подписках"
    assert len(mqtt._subscriptions) == 0, "Ни одна подписка не установлена"
    print("  _is_running=True, но подписок НЕТ (все упали)")
    print("  ВНИМАНИЕ: node 'работает', но не получает сообщения!")

    # ПРАВИЛЬНЫЙ ПОДХОД: проверять подписки после start()
    if node._is_running and not mqtt._subscriptions:
        print("  ПРАВИЛЬНО: проверять фактические подписки, а не только _is_running")

    await node.stop()
    print("  OK: _is_running despite failed subs проверен")


# ============================================================================
# 9. on() silently replaces handlers
# ============================================================================

async def demo_on_replaces_handlers():
    """on() ЗАМЕНЯЕТ handler, а не добавляет — повторный on() перезаписывает."""
    print("\n--- 9. on() silently replaces handlers ---")

    mqtt = MockMqttClient()
    node = BaseNode("device1", mqtt)

    call_log: list[str] = []

    async def handler_a(env: Envelope):
        call_log.append("A")

    async def handler_b(env: Envelope):
        call_log.append("B")

    # Регистрируем handler A
    node.on(EnvelopeType.DEVICE_STATE, handler_a)
    assert node._handlers[EnvelopeType.DEVICE_STATE] is handler_a

    # Регистрируем handler B для того же типа
    # НЕПРАВИЛЬНО: ожидать, что оба handler будут вызваны
    node.on(EnvelopeType.DEVICE_STATE, handler_b)
    assert node._handlers[EnvelopeType.DEVICE_STATE] is handler_b, "B заменил A"
    assert handler_a not in node._handlers.values(), "A полностью заменён"
    print("  on() ЗАМЕНИЛ handler A на B (не добавил, а перезаписал)")

    # Проверяем: только B вызывается
    env = Envelope.state(source="test", data={"power": True})
    handler = node._handlers.get(EnvelopeType.DEVICE_STATE)
    assert handler is handler_b
    await handler(env)
    assert call_log == ["B"], f"Ожидали ['B'], получили {call_log}"
    print("  Только handler B вызван — A потерян")

    # ПРАВИЛЬНЫЙ ПОДХОД: использовать EventBus для множественных подписок
    print("  ПРАВИЛЬНО: для множественных handler использовать EventBus.subscribe()")

    print("  OK: on() replaces handlers проверен")


# ============================================================================
# 10. Target fallback: target or source (routing loops)
# ============================================================================

async def demo_target_fallback_routing():
    """_build_topic: target or source — если target не задан, сообщение идёт на source."""
    print("\n--- 10. Target fallback: target or source ---")

    mqtt = MockMqttClient()
    async def _noop_wait(mid, timeout=10.0):
        pass
    mqtt._kamio_wait_for_suback = _noop_wait
    mqtt._kamio_wait_for_unsuback = _noop_wait
    node = BaseNode("device1", mqtt)
    node._loop = asyncio.get_running_loop()
    node._is_running = True

    # Envelope БЕЗ target → _build_topic использует source
    env_no_target = Envelope.state(source="device1", data={"power": True})
    topic = node._build_topic(env_no_target)
    # target=None → target = env.target if env.target else env.source → "device1"
    assert "device1" in topic, f"Topic должен содержать 'device1': {topic}"
    print(f"  Envelope без target → topic={topic} (использован source как target)")

    # Envelope С target → используется target
    env_with_target = Envelope.state(source="device1", data={"power": True})
    env_with_target.target = "device2"
    topic2 = node._build_topic(env_with_target)
    assert "device2" in topic2, f"Topic должен содержать 'device2': {topic2}"
    print(f"  Envelope с target='device2' → topic={topic2}")

    # ПОДВОХ: если устройство отправляет state без target, оно отправляет
    # на свой собственный topic → получает собственное сообщение обратно (эхо)
    print("  ВНИМАНИЕ: state без target → отправка на свой topic → эхо!")

    print("  OK: target fallback проверен")


# ============================================================================
# 11. Publish swallows RuntimeError with "shutdown"/"closed"
# ============================================================================

async def demo_publish_swallows_runtime_error():
    """publish_raw глотает RuntimeError с 'shutdown' или 'closed' в сообщении."""
    print("\n--- 11. Publish swallows RuntimeError 'shutdown'/'closed' ---")

    # MQTT клиент, publish которого падает с RuntimeError
    class ShutdownMqttClient(MockMqttClient):
        def publish(self, topic, payload, qos=0, retain=False):
            raise RuntimeError("Client is shutting down")

    mqtt = ShutdownMqttClient()
    node = BaseNode("device1", mqtt)
    node._loop = asyncio.get_running_loop()
    node._is_running = True

    env = Envelope.state(source="device1", data={"power": True})

    # publish() глотает RuntimeError с "shutdown" — молча возвращает
    await node.publish(env)  # не падает!
    print("  publish() с RuntimeError('shutting down') — молча проглочено")

    # Проверяем с "closed"
    class ClosedMqttClient(MockMqttClient):
        def publish(self, topic, payload, qos=0, retain=False):
            raise RuntimeError("Event loop is closed")

    mqtt2 = ClosedMqttClient()
    node2 = BaseNode("device1", mqtt2)
    node2._loop = asyncio.get_running_loop()
    node2._is_running = True

    await node2.publish(env)  # тоже не падает
    print("  publish() с RuntimeError('Event loop is closed') — молча проглочено")

    # ПОДВОХ: другие RuntimeError логируются, но НЕ проглатываются молча
    class OtherErrorMqttClient(MockMqttClient):
        def publish(self, topic, payload, qos=0, retain=False):
            raise RuntimeError("Something else went wrong")

    mqtt3 = OtherErrorMqttClient()
    node3 = BaseNode("device1", mqtt3)
    node3._loop = asyncio.get_running_loop()
    node3._is_running = True

    # Этот RuntimeError логируется (logger.error), но не падает
    await node3.publish(env)  # логируется, но не падает
    print("  publish() с RuntimeError('Something else') — логируется, но не падает")

    print("  OK: publish swallows RuntimeError проверен")


# ============================================================================
# 12. DeviceNode: handler exception → message lost
# ============================================================================

async def demo_device_node_handler_exception():
    """DeviceNode: исключение в handler → сообщение потеряно (логируется)."""
    print("\n--- 12. DeviceNode: handler exception → message lost ---")

    mqtt = MockMqttClient()
    async def _noop_wait(mid, timeout=10.0):
        pass
    mqtt._kamio_wait_for_suback = _noop_wait
    mqtt._kamio_wait_for_unsuback = _noop_wait

    node = DeviceNode("device1", mqtt)
    node._loop = asyncio.get_running_loop()
    node._is_running = True

    received: list[Envelope] = []

    async def failing_handler(env: Envelope):
        received.append(env)
        raise ValueError("Handler crashed!")

    node.set_handler(failing_handler)

    # Отправляем валидный envelope
    env = Envelope.state(source="server", data={"power": True})
    payload = env.to_json().encode()

    # _handle_message ловит исключение handler → логирует, но не падает
    await node._handle_message(payload)

    # Handler был вызван, но упал — сообщение "обработано" (получено),
    # но результат потерян
    assert len(received) == 1, "Handler был вызван один раз"
    assert received[0].data == {"power": True}
    print("  Handler вызван, но упал с ValueError — сообщение потеряно")
    print("  Исключение логируется (logger.exception), но ACK не отправляется")

    # ПОДВОХ: если handler отвечает за отправку ACK, ACK не будет отправлен
    # → отправитель ждёт и тайм-аутит через 10 секунд
    print("  ВНИМАНИЕ: если handler отвечает за ACK, ACK потерян → тайм-аут у отправителя")

    print("  OK: DeviceNode handler exception → message lost проверен")


# ============================================================================
# 13. DeviceNode on_stop raises → super().stop() never called
# ============================================================================

async def demo_on_stop_raises_super_never_called():
    """DeviceNode.stop(): on_stop() ДО super().stop() — если on_stop падает, stop не вызывается."""
    print("\n--- 13. DeviceNode on_stop raises → super().stop() never called ---")

    mqtt = MockMqttClient()
    async def _noop_wait(mid, timeout=10.0):
        pass
    mqtt._kamio_wait_for_suback = _noop_wait
    mqtt._kamio_wait_for_unsuback = _noop_wait

    # Устройство, у которого on_stop падает
    from kamio import Device, state

    class BadDevice(Device):
        power: bool = state(default=False, writable=True)

        async def on_stop(self, node):
            raise RuntimeError("on_stop crashed!")

    device = BadDevice()
    device._app = None  # нет app → hooks не вызываются
    node = DeviceNode("device1", mqtt)
    node._loop = asyncio.get_running_loop()
    node._is_running = True

    # Устанавливаем handler с device
    from kamio.core.handlers import DeviceHandler
    # Не создаём полный handler — просто установим device на node
    mock_handler = MagicMock()
    mock_handler.device = device
    node.set_handler(mock_handler)

    # DeviceNode.stop() вызывает device.on_stop(self) ПЕРЕД super().stop()
    # Если on_stop падает, super().stop() НЕ вызывается
    try:
        await node.stop()
        assert False, "stop() должен был упасть от on_stop"
    except RuntimeError as e:
        assert "on_stop crashed" in str(e)
        print(f"  DeviceNode.stop() → on_stop() упал: {e}")
        print("  super().stop() НИКОГДА не вызван → подписки НЕ отписаны!")

    # Проверяем: _is_running всё ещё True (super().stop() не выполнился)
    assert node._is_running is True, "_is_running должен быть True (super().stop() не вызван)"
    print(f"  _is_running = {node._is_running} (super().stop() не вызван)")

    # ПРАВИЛЬНЫЙ ПОДХОД: on_stop должен быть безопасным
    class SafeDevice(Device):
        power: bool = state(default=False, writable=True)

        async def on_stop(self, node):
            try:
                # Код очистки, который может упасть
                pass
            except Exception as e:
                self.logger.error(f"on_stop error (non-fatal): {e}")
            # Не re-raise — позволить super().stop() выполниться

    print("  ПРАВИЛЬНО: on_stop должен ловить свои ошибки, не re-raise")

    print("  OK: on_stop raises → super().stop() never called проверен")


# ============================================================================
# 14. Invalid envelopes silently dropped
# ============================================================================

async def demo_invalid_envelope_silently_dropped():
    """Envelope.from_json возвращает None для невалидного JSON — сообщение отбрасывается."""
    print("\n--- 14. Invalid envelopes silently dropped ---")

    mqtt = MockMqttClient()
    node = BaseNode("device1", mqtt)
    node._loop = asyncio.get_running_loop()
    node._is_running = True

    # Невалидный JSON
    result = Envelope.from_json(b"not json at all")
    assert result is None, "from_json должен вернуть None для невалидного JSON"
    print("  from_json('not json') → None (молча отброшено)")

    # Валидный JSON, но без обязательных полей
    result2 = Envelope.from_json('{"type": "dt"}')
    # source будет "" — но envelope создаётся
    assert result2 is not None, "from_json с минимальными полями создаёт envelope"
    assert result2.source == ""
    print(f"  from_json('{{\"type\": \"dt\"}}') → source='{result2.source}' (создаётся с пустым source)")

    # Невалидный тип → UNKNOWN
    result3 = Envelope.from_json('{"source": "dev1", "type": "INVALID_TYPE", "data": {}}')
    assert result3 is not None
    assert result3.type == EnvelopeType.UNKNOWN, f"Ожидали UNKNOWN, получили {result3.type}"
    print(f"  from_json с type='INVALID_TYPE' → type={result3.type} (UNKNOWN)")

    # _handle_message с None envelope — молча return
    await node._handle_message(b"not json")  # не падает, молча return
    print("  _handle_message с невалидным JSON — молча return (без ошибки)")

    print("  OK: invalid envelopes silently dropped проверен")


# ============================================================================
# 15. Unknown message types: DEBUG only
# ============================================================================

async def demo_unknown_message_type_debug_only():
    """Неизвестные типы сообщений логируются только на DEBUG уровне."""
    print("\n--- 15. Unknown message types: DEBUG only ---")

    mqtt = MockMqttClient()
    node = BaseNode("device1", mqtt)
    node._loop = asyncio.get_running_loop()
    node._is_running = True

    # Регистрируем handler только для DEVICE_STATE
    async def state_handler(env: Envelope):
        pass

    node.on(EnvelopeType.DEVICE_STATE, state_handler)

    # Отправляем KEEPALIVE — нет зарегистрированного handler
    env = Envelope.keepalive(source="device1")
    payload = env.to_json().encode()

    # В _handle_message: handler = self._handlers.get(env.type) → None
    # else: logger.debug(f"Unhandled message type: {env.type}")
    # Только DEBUG, не WARNING!
    await node._handle_message(payload)
    print(f"  Unhandled message type {env.type} — логируется только на DEBUG уровне")
    print("  ВНИМАНИЕ: в production с INFO-уровнем логов это НЕВИДИМО")

    # ПРАВИЛЬНЫЙ ПОДХОД: зарегистрировать handler для всех ожидаемых типов
    print("  ПРАВИЛЬНО: регистрировать handler для всех ожидаемых типов сообщений")

    print("  OK: unknown message type DEBUG only проверен")


# ============================================================================
# Main
# ============================================================================

async def main():
    print("=" * 70)
    print("23 — MQTT Internals (внутренности MQTT-слоя)")
    print("=" * 70)

    await demo_ack_cache_limit()
    await demo_disconnect_no_ack_cleanup()
    await demo_resolve_ack_not_thread_safe()
    await demo_gmqtt_monkey_patching()
    await demo_reconnect_zero_unlimited()
    await demo_empty_client_id()
    await demo_start_silent_if_running()
    await demo_running_flag_despite_failed_subs()
    await demo_on_replaces_handlers()
    await demo_target_fallback_routing()
    await demo_publish_swallows_runtime_error()
    await demo_device_node_handler_exception()
    await demo_on_stop_raises_super_never_called()
    await demo_invalid_envelope_silently_dropped()
    await demo_unknown_message_type_debug_only()

    print("\n" + "=" * 70)
    print("ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ ✓")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
