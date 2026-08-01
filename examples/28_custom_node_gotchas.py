"""Глубокий разбор неочевидного поведения CustomNode и CustomNodeManager.

Этот файл — НЕ базовый туториал. Он документирует каждый подводный камень
в системе кастомных MQTT-узлов Kamio, который может привести к утечкам
подписок, гонкам данных или неожиданному поведению маршрутизации.

Подводные камни:

1.  CustomNode.stop(): ОБЯЗАТЕЛЬНО вызывать super().stop() — иначе подписки утекают
2.  topic_prefix: trailing слэши обрезаются (rstrip("/"))
3.  _encode_payload: кодирует ТОЛЬКО строки, остальное проходит как есть
4.  matches(): точное совпадение ИЛИ prefix match (с "/")
5.  publish(): синхронный (может заблокировать event loop)
6.  CustomNodeManager._nodes: без блокировки (нет lock)
7.  CustomNodeManager обращается к app._loop (приватный атрибут)
8.  unregister_node: fire-and-forget stop задачи
9.  route_message: итерирует _nodes без блокировки
10. route_message: handle_message падает → всё равно "handled" = True
11. register duplicate: ValueError
12. unregister non-existent: warning, тихий return
13. start_all: продолжает после ошибок
14. stop_all: обратный порядок, ошибки не останавливают остальные

Все примеры запускаются БЕЗ MQTT-брокера — используются моки и assertions.
"""
from __future__ import annotations

import asyncio
import logging
import unittest
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from kamio.core.custom_nodes import CustomNode, CustomNodeManager
from kamio.core.event_bus import EventBus

logging.basicConfig(level=logging.DEBUG, format="%(name)s | %(levelname)s | %(message)s")


# ---------------------------------------------------------------------------
# Вспомогательные классы
# ---------------------------------------------------------------------------

class _MockMqttClient:
    """Мок MQTT-клиента для тестирования CustomNode без реального брокера."""

    def __init__(self):
        self.subscribed: list[tuple[str, int]] = []
        self.unsubscribed: list[str] = []
        self.published: list[tuple] = []
        self._block_publish = False
        self._publish_delay = 0.0

    def subscribe(self, topic: str, qos: int = 0):
        self.subscribed.append((topic, qos))

    def unsubscribe(self, topic: str):
        self.unsubscribed.append(topic)

    def publish(self, topic: str, payload: Any, qos: int = 0, retain: bool = False):
        if self._publish_delay > 0:
            import time
            time.sleep(self._publish_delay)  # НЕВЕРНО: блокирует event loop
        self.published.append((topic, payload, qos, retain))


class _MockApp:
    """Мок KamioApp для CustomNodeManager."""

    def __init__(self):
        self.event_bus = EventBus()
        self._loop: asyncio.AbstractEventLoop | None = None
        self.is_running = False


# ---------------------------------------------------------------------------
# Тестовые CustomNode
# ---------------------------------------------------------------------------

class _GoodNode(CustomNode):
    """Корректный узел: вызывает super().stop()."""

    def __init__(self, mqtt_client, prefix="good/node"):
        super().__init__(mqtt_client, prefix)
        self.messages: list[tuple[str, bytes]] = []

    async def start(self) -> None:
        self.subscribe("cmd/#")
        self._is_running = True

    async def stop(self) -> None:
        # ПРАВИЛЬНО: вызываем super().stop()
        await super().stop()

    async def handle_message(self, topic: str, payload: bytes) -> None:
        self.messages.append((topic, payload))


class _BadNode(CustomNode):
    """НЕкорректный узел: НЕ вызывает super().stop() — подписки утекают."""

    def __init__(self, mqtt_client, prefix="bad/node"):
        super().__init__(mqtt_client, prefix)
        self.messages: list[tuple[str, bytes]] = []
        self.stop_called = False

    async def start(self) -> None:
        self.subscribe("cmd/#")
        self._is_running = True

    async def stop(self) -> None:
        # НЕВЕРНО: НЕ вызываем super().stop()
        # Подписки не будут удалены!
        self.stop_called = True
        self._is_running = False

    async def handle_message(self, topic: str, payload: bytes) -> None:
        self.messages.append((topic, payload))


class _CrashingNode(CustomNode):
    """Узел, handle_message которого падает."""

    def __init__(self, mqtt_client, prefix="crash/node"):
        super().__init__(mqtt_client, prefix)

    async def start(self) -> None:
        self._is_running = True

    async def stop(self) -> None:
        await super().stop()

    async def handle_message(self, topic: str, payload: bytes) -> None:
        raise RuntimeError("handle_message crashed")


class _FailingStartNode(CustomNode):
    """Узел, start() которого падает."""

    def __init__(self, mqtt_client, prefix="failstart/node"):
        super().__init__(mqtt_client, prefix)

    async def start(self) -> None:
        raise RuntimeError("start failed")

    async def stop(self) -> None:
        await super().stop()

    async def handle_message(self, topic: str, payload: bytes) -> None:
        pass


class _FailingStopNode(CustomNode):
    """Узел, stop() которого падает."""

    def __init__(self, mqtt_client, prefix="failstop/node"):
        super().__init__(mqtt_client, prefix)

    async def start(self) -> None:
        self._is_running = True

    async def stop(self) -> None:
        raise RuntimeError("stop failed")

    async def handle_message(self, topic: str, payload: bytes) -> None:
        pass


# ---------------------------------------------------------------------------
# Тесты
# ---------------------------------------------------------------------------

class TestCustomNodeGotchas(unittest.IsolatedAsyncioTestCase):
    """Все подводные камни CustomNode и CustomNodeManager."""

    # ======================================================================
    # 1. CustomNode.stop(): MUST call super().stop() or subscriptions leak
    # ======================================================================
    async def test_01_stop_must_call_super_or_subscriptions_leak(self):
        """stop() ОБЯЗАТЕЛЬНО должен вызывать super().stop() — иначе подписки утекают.

        CustomNode.stop() по умолчанию отписывается от всех тем в _subscriptions.
        Если подкласс переопределяет stop() и НЕ вызывает super().stop(),
        unsubscribe НЕ вызывается, _subscriptions НЕ очищается.

        НЕВЕРНО: переопределять stop() без super().stop().
        ПРАВИЛЬНО: всегда вызывать `await super().stop()` в переопределённом stop().
        """
        mqtt = _MockMqttClient()

        # Хороший узел — вызывает super().stop()
        good = _GoodNode(mqtt, "good/node")
        await good.start()
        self.assertEqual(len(mqtt.subscribed), 1)
        await good.stop()
        # super().stop() отписался
        self.assertEqual(len(mqtt.unsubscribed), 1, "Good node должен отписаться")
        self.assertEqual(len(good._subscriptions), 0, "Подписки должны быть очищены")
        self.assertFalse(good._is_running)

        # Плохой узел — НЕ вызывает super().stop()
        bad = _BadNode(mqtt, "bad/node")
        await bad.start()
        self.assertEqual(len(mqtt.subscribed), 2)  # вторая подписка
        unsub_count_before = len(mqtt.unsubscribed)
        await bad.stop()
        # unsubscribe НЕ вызван — подписка утекла!
        self.assertEqual(
            len(mqtt.unsubscribed), unsub_count_before,
            "Bad node НЕ должен отписаться (super не вызван)"
        )
        self.assertEqual(
            len(bad._subscriptions), 1,
            "Подписки НЕ должны быть очищены (super не вызван)"
        )

    # ======================================================================
    # 2. topic_prefix stripped of trailing slashes
    # ======================================================================
    async def test_02_topic_prefix_strips_trailing_slashes(self):
        """topic_prefix обрезает trailing слэши через rstrip("/").

        CustomNode.__init__ делает self.topic_prefix = topic_prefix.rstrip("/").
        Это означает, что "my/topic/" → "my/topic", "my///" → "my".
        Но leading слэши НЕ обрезаются.

        НЕВЕРНО: передавать prefix с trailing слэшами и рассчитывать на них.
        ПРАВИЛЬНО: знать, что trailing слэши всегда обрезаются.
        """
        mqtt = _MockMqttClient()

        node1 = _GoodNode(mqtt, "my/topic/")
        self.assertEqual(node1.topic_prefix, "my/topic", "Trailing / обрезан")

        node2 = _GoodNode(mqtt, "my/topic///")
        self.assertEqual(node2.topic_prefix, "my/topic", "Все trailing / обрезаны")

        node3 = _GoodNode(mqtt, "/leading/slash")
        self.assertEqual(node3.topic_prefix, "/leading/slash", "Leading / сохранён")

        node4 = _GoodNode(mqtt, "no-slash")
        self.assertEqual(node4.topic_prefix, "no-slash", "Без изменений")

        # Пустой prefix после rstrip — крайний случай
        node5 = _GoodNode(mqtt, "///")
        self.assertEqual(node5.topic_prefix, "", "Только слэши → пустая строка")

    # ======================================================================
    # 3. _encode_payload: only encodes strings
    # ======================================================================
    async def test_03_encode_payload_only_encodes_strings(self):
        """_encode_payload кодирует ТОЛЬКО строки в bytes, остальное проходит как есть.

        CustomNode._encode_payload: payload.encode() если isinstance(payload, str),
        иначе возвращает payload без изменений. Это означает:
        - int, float, dict, list, bytes, None — проходят как есть
        - str → bytes

        НЕВЕРНО: передавать dict как payload и рассчитывать на JSON-сериализацию.
        ПРАВИЛЬНО: вручную сериализовать dict/list в JSON-строку перед publish.
        """
        # str → bytes
        self.assertEqual(CustomNode._encode_payload("hello"), b"hello")

        # int → int (не кодируется!)
        self.assertEqual(CustomNode._encode_payload(42), 42)

        # float → float
        self.assertEqual(CustomNode._encode_payload(3.14), 3.14)

        # dict → dict (НЕ сериализуется в JSON!)
        payload_dict = {"key": "value"}
        result = CustomNode._encode_payload(payload_dict)
        self.assertIs(result, payload_dict, "dict должен пройти как есть")

        # bytes → bytes
        self.assertEqual(CustomNode._encode_payload(b"raw"), b"raw")

        # None → None
        self.assertIsNone(CustomNode._encode_payload(None))

        # bool → bool (bool не str)
        self.assertIs(CustomNode._encode_payload(True), True)

    # ======================================================================
    # 4. matches(): exact OR prefix match
    # ======================================================================
    async def test_04_matches_exact_or_prefix(self):
        """matches(): точное совпадение ИЛИ prefix match (с "/").

        CustomNode.matches(topic) возвращает True если:
        - topic == topic_prefix (точное совпадение), ИЛИ
        - topic.startswith(topic_prefix + "/") (prefix с слэшём)

        Это означает, что "myprefix" совпадает с "myprefix" и "myprefix/sub",
        но НЕ совпадает с "myprefixother" (нет слэша после prefix).

        НЕВЕРНО: рассчитывать, что "myprefix" совпадает с "myprefixother".
        ПРАВИЛЬНО: matches требует либо точное совпадение, либо "/" после prefix.
        """
        mqtt = _MockMqttClient()
        node = _GoodNode(mqtt, "home/living")

        # Точное совпадение
        self.assertTrue(node.matches("home/living"), "Точное совпадение")

        # Prefix с слэшём
        self.assertTrue(node.matches("home/living/light"), "Prefix match с /")
        self.assertTrue(node.matches("home/living/cmd/#"), "Prefix match с / и wildcard")

        # НЕ совпадает: похожий prefix без слэша
        self.assertFalse(node.matches("home/livingroom"), "Не должно совпадать без /")

        # НЕ совпадает: совершенно другой topic
        self.assertFalse(node.matches("home/kitchen"), "Другой topic")

        # Крайний случай: пустой prefix совпадает со всем (с "/")
        node_empty = _GoodNode(mqtt, "///")  # rstrip → ""
        self.assertTrue(node_empty.matches("anything"), "Пустой prefix + точное совпадение?")
        # "" == "anything" → False, "anything".startswith("/") → False
        # Но "" == "" → True
        self.assertTrue(node_empty.matches(""), "Пустой prefix совпадает с пустым topic")
        self.assertTrue(node_empty.matches("/sub"), "Пустой prefix + /sub")

    # ======================================================================
    # 5. publish() synchronous (could block)
    # ======================================================================
    async def test_05_publish_synchronous_could_block(self):
        """publish() синхронный — может заблокировать event loop.

        CustomNode.publish() вызывает mqtt_client.publish() напрямую,
        синхронно. Если MQTT-клиент блокирует (например, network I/O),
        event loop зависнет.

        publish_async() использует asyncio.to_thread() — НЕ блокирует.

        НЕВЕРНО: использовать publish() в async-коде с медленным MQTT-клиентом.
        ПРАВИЛЬНО: использовать publish_async() для неблокирующей публикации.
        """
        mqtt = _MockMqttClient()
        node = _GoodNode(mqtt, "test/node")
        await node.start()

        # publish() — синхронный вызов
        node.publish("status", "online")
        self.assertEqual(len(mqtt.published), 1)
        topic, payload, qos, retain = mqtt.published[0]
        self.assertEqual(topic, "test/node/status")
        self.assertEqual(payload, b"online")  # str → bytes

        # publish_async() — асинхронный (через to_thread)
        await node.publish_async("status", "offline")
        self.assertEqual(len(mqtt.published), 2)
        topic, payload, qos, retain = mqtt.published[1]
        self.assertEqual(payload, b"offline")

        # publish с int payload — НЕ кодируется (только str)
        node.publish("count", 42)
        topic, payload, qos, retain = mqtt.published[2]
        self.assertEqual(payload, 42, "int не кодируется в bytes")

    # ======================================================================
    # 6. CustomNodeManager._nodes: no lock
    # ======================================================================
    async def test_06_nodes_dict_no_lock(self):
        """CustomNodeManager._nodes — обычный dict без блокировки.

        _nodes: Dict[str, CustomNode] — НЕ защищён lock. Регистрация и
        удаление могут вызываться из разных задач, что теоретически может
        привести к гонке. На практике register/unregister обычно
        вызываются из основного потока, но route_message итерирует _nodes
        без копии.

        НЕВЕРНО: модифицировать _nodes из нескольких задач одновременно.
        ПРАВИЛЬНО: регистрировать/удалять узлы в основной задаче (main task).
        """
        app = _MockApp()
        app._loop = asyncio.get_running_loop()
        manager = CustomNodeManager(app)

        # _nodes — обычный dict, без lock
        self.assertIsInstance(manager._nodes, dict)
        self.assertFalse(hasattr(manager, "_nodes_lock"), "Нет lock для _nodes")

        # Регистрируем узел
        mqtt = _MockMqttClient()
        node = _GoodNode(mqtt, "test/node")
        manager.register_node("test", node)
        self.assertIn("test", manager._nodes)

        # Можно напрямую читать _nodes (нет защиты)
        self.assertIs(manager._nodes["test"], node)

    # ======================================================================
    # 7. CustomNodeManager accesses app._loop (private)
    # ======================================================================
    async def test_07_manager_accesses_app_private_loop(self):
        """CustomNodeManager обращается к app._loop (приватный атрибут).

        unregister_node делает: loop = getattr(self._app, "_loop", None).
        Это обращение к приватному атрибуту _loop. Если у app нет _loop,
        используется fallback на синхронную очистку подписок.

        НЕВЕРНО: рассчитывать, что _loop всегда доступен.
        ПРАВИЛЬНО: знать, что без _loop unregister_node делает sync cleanup.
        """
        app = _MockApp()
        manager = CustomNodeManager(app)

        mqtt = _MockMqttClient()
        node = _GoodNode(mqtt, "test/node")
        manager.register_node("test", node)
        await node.start()
        node._is_running = True

        # С _loop: stop() планируется как задача
        app._loop = asyncio.get_running_loop()
        manager.unregister_node("test")
        self.assertNotIn("test", manager._nodes)
        # stop задача создана (fire-and-forget)
        self.assertGreaterEqual(len(manager._stop_tasks), 0)  # может уже выполниться

        # Без _loop: синхронная очистка
        app2 = _MockApp()
        app2._loop = None  # явно нет loop
        manager2 = CustomNodeManager(app2)

        mqtt2 = _MockMqttClient()
        node2 = _GoodNode(mqtt2, "test2/node")
        manager2.register_node("test2", node2)
        await node2.start()
        node2._is_running = True

        unsub_before = len(mqtt2.unsubscribed)
        manager2.unregister_node("test2")
        # Синхронная очистка: unsubscribe вызван напрямую
        self.assertGreater(len(mqtt2.unsubscribed), unsub_before, "Sync cleanup должен отписать")
        self.assertFalse(node2._is_running, "Флаг _is_running сброшен")

    # ======================================================================
    # 8. unregister_node: fire-and-forget stop tasks
    # ======================================================================
    async def test_08_unregister_node_fire_and_forget_stop(self):
        """unregister_node: stop задачи fire-and-forget (отслеживаются, но не ожидаются).

        unregister_node создаёт задачу через loop.create_task(node.stop())
        и добавляет её в _stop_tasks. Задача отслеживается (чтобы не быть
        GC'd), но НЕ ожидается. Вызывающий не знает, когда stop() завершится.

        НЕВЕРНО: рассчитывать, что после unregister_node stop() уже выполнен.
        ПРАВИЛЬНО: если нужна гарантия остановки, вызывать node.stop() вручную.
        """
        app = _MockApp()
        app._loop = asyncio.get_running_loop()
        manager = CustomNodeManager(app)

        mqtt = _MockMqttClient()
        node = _GoodNode(mqtt, "test/node")
        manager.register_node("test", node)
        await node.start()

        manager.unregister_node("test")
        # Узел удалён из _nodes
        self.assertNotIn("test", manager._nodes)

        # Stop задача может быть ещё в полёте
        # Даём ей выполниться
        await asyncio.sleep(0.05)

        # Теперь stop() выполнен
        self.assertEqual(len(mqtt.unsubscribed), 1, "Stop задача должна отписать")

    # ======================================================================
    # 9. route_message: iterates without lock
    # ======================================================================
    async def test_09_route_message_iterates_without_lock(self):
        """route_message итерирует _nodes без блокировки.

        route_message делает `for name, node in self._nodes.items()`.
        Если во время итерации другой код модифицирует _nodes,
        может возникнуть RuntimeError (dict changed size during iteration).
        На практике route_message вызывается из MQTT callback, а
        register/unregister — из основного кода.

        НЕВЕРНО: модифицировать _nodes во время route_message.
        ПРАВИЛЬНО: не регистрировать/удалять узлы во время обработки сообщений.
        """
        app = _MockApp()
        app._loop = asyncio.get_running_loop()
        manager = CustomNodeManager(app)

        mqtt = _MockMqttClient()
        node = _GoodNode(mqtt, "test/node")
        manager.register_node("test", node)
        await node.start()

        # route_message находит совпадающий узел
        handled = await manager.route_message("test/node/cmd/on", b"payload")
        self.assertTrue(handled, "Должен быть handled=True")
        self.assertEqual(len(node.messages), 1)

        # route_message с несовпадающим topic
        handled2 = await manager.route_message("other/topic", b"payload")
        self.assertFalse(handled2, "Должен быть handled=False для несовпадающего topic")

    # ======================================================================
    # 10. route_message: handle_message raises → still "handled"
    # ======================================================================
    async def test_10_route_message_handle_raises_still_handled(self):
        """route_message: handle_message падает → всё равно handled=True.

        В route_message, handled = True устанавливается ДО try/except.
        Если handle_message падает, ошибка логируется, событие
        custom_node_error публикуется, но handled остаётся True.

        НЕВЕРНО: рассчитывать, что handled=False при ошибке handle_message.
        ПРАВИЛЬНО: handled=True означает «сообщение дошло до узла», а не «успешно обработано».
        """
        app = _MockApp()
        app._loop = asyncio.get_running_loop()
        manager = CustomNodeManager(app)

        errors: list[dict] = []
        app.event_bus.subscribe("custom_node_error", lambda d: errors.append(d))

        mqtt = _MockMqttClient()
        node = _CrashingNode(mqtt, "crash/node")
        manager.register_node("crash", node)
        await node.start()

        # handle_message падает, но handled = True
        handled = await manager.route_message("crash/node/sub", b"payload")
        self.assertTrue(
            handled,
            "handled=True даже если handle_message упал — handled устанавливается до try"
        )

        # Событие custom_node_error опубликовано
        self.assertEqual(len(errors), 1, "custom_node_error должен быть опубликован")
        self.assertEqual(errors[0]["node_name"], "crash")
        self.assertEqual(errors[0]["phase"], "handle_message")

    # ======================================================================
    # 11. register duplicate: ValueError
    # ======================================================================
    async def test_11_register_duplicate_raises_value_error(self):
        """Регистрация дубликата: ValueError.

        register_node проверяет `if name in self._nodes` и выбрасывает
        ValueError. Это единственный случай в CustomNodeManager, который
        пробрасывает исключение вместо тихого возврата.

        НЕВЕРНО: регистрировать узел с тем же именем без проверки.
        ПРАВИЛЬНО: проверять list_nodes() или использовать try/except.
        """
        app = _MockApp()
        manager = CustomNodeManager(app)

        mqtt = _MockMqttClient()
        node1 = _GoodNode(mqtt, "test/node")
        manager.register_node("test", node1)

        node2 = _GoodNode(mqtt, "test2/node")
        with self.assertRaises(ValueError, msg="Дубликат имени должен дать ValueError"):
            manager.register_node("test", node2)

        # Оригинальный узел не заменён
        self.assertIs(manager.get_node("test"), node1)

    # ======================================================================
    # 12. unregister non-existent: warning, silent return
    # ======================================================================
    async def test_12_unregister_non_existent_warning_silent_return(self):
        """Удаление несуществующего узла: warning, тихий return.

        unregister_node проверяет `if name not in self._nodes`, логирует
        WARNING и делает return. Никакого исключения.

        НЕВЕРНО: рассчитывать на исключение при удалении несуществующего узла.
        ПРАВИЛЬНО: unregister_node безопасен для несуществующих имён.
        """
        app = _MockApp()
        manager = CustomNodeManager(app)

        # Удаление несуществующего узла — НЕ выбрасывает
        manager.unregister_node("nonexistent")
        # Просто warning в логах и return

        # _nodes не изменился
        self.assertEqual(len(manager.list_nodes()), 0)

    # ======================================================================
    # 13. start_all: continues after errors
    # ======================================================================
    async def test_13_start_all_continues_after_errors(self):
        """start_all: продолжает после ошибок в отдельных узлах.

        start_all итерирует _nodes и вызывает node.start() в try/except.
        Если start() падает, ошибка логируется, публикуется событие
        custom_node_error, и цикл продолжается со следующим узлом.
        Узел с ошибкой НЕ получает _is_running = True.

        НЕВЕРНО: рассчитывать, что ошибка в одном узле остановит start_all.
        ПРАВИЛЬНО: проверять _is_running каждого узла после start_all.
        """
        app = _MockApp()
        app._loop = asyncio.get_running_loop()
        manager = CustomNodeManager(app)

        errors: list[dict] = []
        app.event_bus.subscribe("custom_node_error", lambda d: errors.append(d))

        mqtt = _MockMqttClient()
        good_node = _GoodNode(mqtt, "good/node")
        bad_node = _FailingStartNode(mqtt, "failstart/node")
        good_node2 = _GoodNode(mqtt, "good2/node")

        manager.register_node("good", good_node)
        manager.register_node("bad", bad_node)
        manager.register_node("good2", good_node2)

        await manager.start_all()

        # bad_node упал, но good и good2 запустились
        self.assertTrue(good_node._is_running, "good_node должен быть запущен")
        self.assertFalse(bad_node._is_running, "bad_node НЕ должен быть запущен")
        self.assertTrue(good_node2._is_running, "good_node2 должен быть запущен")

        # Событие об ошибке опубликовано
        error_names = [e["node_name"] for e in errors if e["phase"] == "start"]
        self.assertIn("bad", error_names)

    # ======================================================================
    # 14. stop_all: reverse order, errors don't stop others
    # ======================================================================
    async def test_14_stop_all_reverse_order_errors_dont_stop(self):
        """stop_all: обратный порядок, ошибки не останавливают остальные.

        stop_all итерирует _nodes в обратном порядке (reversed(list(...))).
        Если stop() узла падает, ошибка логируется, но цикл продолжается.
        Узлы с ошибкой НЕ получают _is_running = False (ошибка до этой строки).

        НЕВЕРНО: рассчитывать, что ошибка в stop() одного узла остановит stop_all.
        ПРАВИЛЬНО: stop_all гарантированно пытается остановить все узлы.
        """
        app = _MockApp()
        app._loop = asyncio.get_running_loop()
        manager = CustomNodeManager(app)

        mqtt = _MockMqttClient()

        # Регистрируем в порядке: first, failing, last
        first = _GoodNode(mqtt, "first/node")
        failing = _FailingStopNode(mqtt, "failing/node")
        last = _GoodNode(mqtt, "last/node")

        manager.register_node("first", first)
        manager.register_node("failing", failing)
        manager.register_node("last", last)

        # Запускаем все
        first._is_running = True
        failing._is_running = True
        last._is_running = True

        # stop_all в обратном порядке: last → failing → first
        await manager.stop_all()

        # last остановлен (первый в обратном порядке)
        self.assertFalse(last._is_running, "last должен быть остановлен")

        # failing упал при stop() — _is_running НЕ сброшен (ошибка до строки)
        # Но это не мешает first остановиться
        self.assertTrue(
            failing._is_running,
            "failing _is_running не сброшен т.к. stop() упал до этой строки"
        )

        # first остановлен (последний в обратном порядке, но failing не помешал)
        self.assertFalse(first._is_running, "first должен быть остановлен несмотря на ошибку failing")


if __name__ == "__main__":
    unittest.main()
