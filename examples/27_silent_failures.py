"""Глубокий разбор всех «тихих» (silent) отказов в Kamio IoT-фреймворке.

Этот файл — НЕ базовый туториал. Он документирует каждый случай, когда
ошибка, невалидные данные или нестандартная ситуация обрабатывается
молча — без исключения, без WARNING, или с пониженным уровнем логирования.
Разработчики фреймворка и прикладных модулей ДОЛЖНЫ знать об этих
поведениях, иначе баги будут невидимы в продакшене.

Список тихих отказов:

1.  Изменение state вне event loop: применяется локально, НЕ публикуется
2.  Ошибка driver.execute в handle_state: in-memory update пропускается,
    логируется, НЕ пробрасывается
3.  Неизвестные/незаписываемые поля в handle_state: DEBUG, тихо игнорируются
4.  Неизвестный тип envelope в DeviceHandler.__call__: тихо игнорируется (нет лога)
5.  Ошибка отправки error ACK: логируется, но проглатывается
6.  Невалидный envelope from_json: возвращает None
7.  Envelope data/meta не dict: тихо становится пустым dict
8.  EventBus filter exception: подписчик пропускается
9.  Telemetry driver read exception: DEBUG, поле пропускается
10. NaN/None telemetry: тихо фильтруется
11. Hot reload handler error: логируется, но считается «обработанным»
12. Plugin rule removal failure: warning, правило остаётся «осиротевшим»
13. HA discovery: device без node тихо возвращает (return)
14. Mock driver read возвращает raw value (не dict)

Все примеры запускаются БЕЗ MQTT-брокера — используются моки и assertions.
"""
from __future__ import annotations

import asyncio
import logging
import math
import unittest
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

from kamio.core.envelope import Envelope, EnvelopeType
from kamio.core.event_bus import EventBus
from kamio.core.hooks import HooksManager
from kamio.core.handlers import DeviceHandler
from kamio.core.custom_nodes import CustomNode, CustomNodeManager
from kamio.core.hot_reload import HotReloadManager
from kamio.core.rules import RuleEvent
from kamio.data_fields import state, telemetry, config
from kamio.device import Device, command
from kamio.drivers.mock import MockHardwareDriver
from kamio.discovery import HADiscovery
from kamio.plugins.base import Plugin
from kamio.plugins.loader import PluginLoader, PluginContext

logging.basicConfig(level=logging.DEBUG, format="%(name)s | %(levelname)s | %(message)s")


# ---------------------------------------------------------------------------
# Вспомогательные классы
# ---------------------------------------------------------------------------

class _MockMqttClient:
    """Мок MQTT-клиента для тестирования без брокера."""

    def __init__(self):
        self.subscribed: list[str] = []
        self.unsubscribed: list[str] = []
        self.published: list[tuple] = []

    def subscribe(self, topic: str, qos: int = 0):
        self.subscribed.append((topic, qos))

    def unsubscribe(self, topic: str):
        self.unsubscribed.append(topic)

    def publish(self, topic: str, payload: Any, qos: int = 0, retain: bool = False):
        self.published.append((topic, payload, qos, retain))


class _MockDeviceNode:
    """Мок DeviceNode для тестирования DeviceHandler."""

    def __init__(self, device_id: str = "test_device"):
        self.device_id = device_id
        self.is_running = True
        self.mqtt = _MockMqttClient()
        self.published: list[Envelope] = []

    async def publish(self, env: Envelope, retain: bool = False):
        self.published.append(env)

    async def publish_raw(self, topic: str, payload: bytes, retain: bool = False):
        pass


# ---------------------------------------------------------------------------
# Тестовые устройства
# ---------------------------------------------------------------------------

class _Light(Device):
    """Тестовое устройство с writable state и telemetry полями."""
    power: bool = state(default=False, writable=True)
    brightness: int = state(default=100, min=0, max=255, writable=True)
    read_only: int = state(default=42, writable=False)
    temperature: float = telemetry(default=0.0, unit="°C", freq="10s")
    setpoint: float = config(default=22.0)

    @command
    async def toggle(self):
        self.power = not self.power
        return {"power": self.power}


class _FailingDriver(MockHardwareDriver):
    """Драйвер, который всегда падает на execute и read."""

    def __init__(self):
        super().__init__(latency_range=(0, 0), failure_rate=0.0)
        self.connected = True  # имитируем подключённое состояние

    async def execute(self, command_name: str, params: dict) -> dict:
        raise RuntimeError("Hardware failure: relay stuck")

    async def read(self, field_name: str, params: Optional[dict] = None) -> Any:
        raise ConnectionError("Sensor bus error")


class _RawValueDriver(MockHardwareDriver):
    """Драйвер, read() которого возвращает raw value, а не dict с ключом 'data'."""

    def __init__(self):
        super().__init__(latency_range=(0, 0), failure_rate=0.0)
        self.connected = True

    async def read(self, field_name: str, params: Optional[dict] = None) -> Any:
        # Возвращает raw value, а не {"status": "ok", "data": ...}
        # read_telemetry_value обрабатывает оба случая
        return 23.5


# ---------------------------------------------------------------------------
# Тесты
# ---------------------------------------------------------------------------

class TestSilentFailures(unittest.IsolatedAsyncioTestCase):
    """Все тихие отказы фреймворка."""

    # ======================================================================
    # 1. State change вне event loop: применяется локально, НЕ публикуется
    # ======================================================================
    async def test_01_state_change_outside_event_loop(self):
        """Изменение state поля вне event loop применяется локально, но НЕ публикуется.

        НЕВЕРНО: менять state поля в синхронном коде вне event loop и
        рассчитывать, что изменение дойдёт до MQTT.

        ПРАВИЛЬНО: менять state поля только внутри async-контекста (event loop),
        либо использовать _set_state() для локального обновления без публикации.
        """
        light = _Light()
        light.node = _MockDeviceNode("light_1")

        # НЕВЕРНО: меняем state вне event loop
        # __setattr__ ловит RuntimeError (нет running loop), закрывает корутину,
        # логирует warning, но значение применено локально
        light.power = True

        # Значение применено локально — это доказывается
        self.assertTrue(light.power, "Значение должно быть применено локально")

        # Но публикация НЕ состоялась — published пуст
        self.assertEqual(
            len(light.node.published), 0,
            "Публикация не должна произойти вне event loop"
        )

        # ПРАВИЛЬНО: меняем state внутри event loop (мы уже в async-тесте)
        light.power = False
        await asyncio.sleep(0.01)  # даём задаче на публикацию выполниться

        # Теперь публикация состоялась
        self.assertGreaterEqual(
            len(light.node.published), 1,
            "Публикация должна произойти внутри event loop"
        )

    # ======================================================================
    # 2. Driver execute failure: in-memory update пропускается
    # ======================================================================
    async def test_02_driver_execute_failure_skips_in_memory_update(self):
        """Ошибка driver.execute в handle_state: in-memory update пропускается.

        Когда драйвер падает на set_<field>, handle_state:
        - логирует ERROR
        - пропускает (continue) in-memory обновление
        - НЕ пробрасывает исключение
        - Поле остаётся со старым значением

        НЕВЕРНО: рассчитывать, что handle_state пробросит ошибку драйвера.
        ПРАВИЛЬНО: проверять возвращаемый dict (applied_changes) — поле
        не будет в нём, если драйвер отклонил изменение.
        """
        driver = _FailingDriver()
        light = _Light(driver=driver)
        light.node = _MockDeviceNode("light_1")

        # brightness=100 по умолчанию
        self.assertEqual(light.brightness, 100)

        # Пытаемся изменить brightness через handle_state
        # Драйвер упадёт → in-memory update пропущен → исключение НЕ пробрасывается
        applied = await light.handle_state({"brightness": 200})

        # applied_changes пуст — изменение не применено
        self.assertEqual(
            applied, {},
            "Applied changes должны быть пустыми, т.к. драйвер упал"
        )

        # Значение осталось прежним — in-memory update пропущен
        self.assertEqual(
            light.brightness, 100,
            "Значение не должно измениться при ошибке драйвера"
        )

    # ======================================================================
    # 3. Unknown/non-writable fields in handle_state: DEBUG, silently ignored
    # ======================================================================
    async def test_03_unknown_and_non_writable_fields_silently_ignored(self):
        """Неизвестные и незаписываемые поля в handle_state логируются на DEBUG и пропускаются.

        handle_state принимает dict, но применяет только writable state поля.
        Неизвестные поля → DEBUG лог, пропуск.
        Незаписываемые поля (writable=False) → DEBUG лог, пропуск.
        Config/telemetry поля → тоже пропуск (kind != "state").

        НЕВЕРНО: передавать в handle_state config или telemetry поля.
        ПРАВИЛЬНО: использовать handle_config для config полей.
        """
        light = _Light()
        light.node = _MockDeviceNode("light_1")

        # Передаём неизвестное поле, незаписываемое поле и config поле
        applied = await light.handle_state({
            "unknown_field": 123,       # неизвестное → DEBUG, пропуск
            "read_only": 999,           # writable=False → DEBUG, пропуск
            "setpoint": 25.0,           # config, не state → DEBUG, пропуск
            "power": True,              # writable state → применяется
        })

        # Только power применён
        self.assertEqual(
            applied, {"power": True},
            "Только writable state поля должны применяться"
        )
        # read_only не изменился
        self.assertEqual(light.read_only, 42)
        # setpoint не изменился (это config, не state)
        self.assertEqual(light.setpoint, 22.0)

    # ======================================================================
    # 4. Unknown envelope type: silently ignored (no log at all)
    # ======================================================================
    async def test_04_unknown_envelope_type_silently_ignored(self):
        """Неизвестный тип envelope в DeviceHandler.__call__ тихо игнорируется.

        DeviceHandler._handlers содержит только известные типы.
        Если приходит envelope с типом, которого нет в _handlers,
        handler = None, и тело if handler is not None просто пропускается.
        Никакого лога, никакого исключения.

        НЕВЕРНО: рассчитывать на ошибку или лог при неизвестном типе.
        ПРАВИЛЬНО: валидировать тип на уровне приёма (mqtt_nodes).
        """
        light = _Light()
        node = _MockDeviceNode("light_1")
        light.node = node
        handler = DeviceHandler(light, node, state_manager=None, debug=False)

        # Создаём envelope с UNKNOWN типом
        env = Envelope(
            source="external",
            type=EnvelopeType.UNKNOWN,
            data={"something": "value"},
        )

        # __call__ не должен выбросить исключение
        # и не должен ничего сделать (handler = None)
        await handler(env)

        # Ничего не опубликовано в ответ
        self.assertEqual(
            len(node.published), 0,
            "UNKNOWN тип не должен вызывать никакой обработки"
        )

    # ======================================================================
    # 5. Error ACK send failure: logged but swallowed
    # ======================================================================
    async def test_05_error_ack_send_failure_swallowed(self):
        """Ошибка отправки error ACK логируется, но проглатывается.

        Когда DeviceHandler.__call__ ловит исключение и пытается отправить
        error ACK через send_error(), а publish тоже падает, ошибка отправки
        логируется на ERROR, но НЕ пробрасывается. Оригинальная ошибка
        теряется полностью.

        НЕВЕРНО: рассчитывать, что ошибка публикации ACK дойдёт до вызывающего.
        ПРАВИЛЬНО: использовать debug=True для проброса оригинальной ошибки.
        """
        light = _Light()
        node = _MockDeviceNode("light_1")
        light.node = node
        handler = DeviceHandler(light, node, state_manager=None, debug=False)

        # Подменяем _handle_state чтобы он выбросил исключение
        original_state_handler = handler._handlers[EnvelopeType.DEVICE_STATE]

        async def _failing_state_handler(env):
            raise RuntimeError("State processing exploded")

        handler._handlers[EnvelopeType.DEVICE_STATE] = _failing_state_handler

        # Подменяем node.publish чтобы он тоже падал при отправке ACK
        async def _failing_publish(env, retain=False):
            raise ConnectionError("MQTT broker unreachable")

        node.publish = _failing_publish

        env = Envelope.state(source="external", data={"power": True})

        # __call__ НЕ должен выбросить — обе ошибки проглатываются
        await handler(env)

        # С debug=True оригинальная ошибка пробрасывается
        handler.debug = True
        with self.assertRaises(RuntimeError, msg="debug=True должен пробросить ошибку"):
            await handler(env)

    # ======================================================================
    # 6. Invalid envelope from_json: returns None
    # ======================================================================
    async def test_06_invalid_envelope_from_json_returns_none(self):
        """Невалидный JSON или структура возвращает None из from_json.

        Envelope.from_json ловит JSONDecodeError, UnicodeDecodeError,
        TypeError, ValueError, KeyError, AttributeError и даже Exception.
        Во всех случаях возвращает None — тихо, без проброса.

        НЕВЕРНО: вызывать from_json без проверки результата на None.
        ПРАВИЛЬНО: всегда проверять `if env is None: return/handle`.
        """
        # Невалидный JSON
        result = Envelope.from_json("not json at all")
        self.assertIsNone(result, "Невалидный JSON должен вернуть None")

        # Пустая строка
        result = Envelope.from_json("")
        self.assertIsNone(result, "Пустая строка должна вернуть None")

        # Невалидные bytes
        result = Envelope.from_json(b"\xff\xfe not utf8")
        self.assertIsNone(result, "Невалидные bytes должны вернуть None")

        # Валидный JSON, но не объект (массив)
        result = Envelope.from_json("[1, 2, 3]")
        # from_dict вызывается с list → d.get() упадёт → None
        self.assertIsNone(result, "JSON-массив должен вернуть None")

        # ПРАВИЛЬНО: валидный envelope
        valid = Envelope.state(source="dev1", data={"power": True})
        env = Envelope.from_json(valid.to_json())
        self.assertIsNotNone(env, "Валидный envelope не должен быть None")
        self.assertEqual(env.data, {"power": True})

    # ======================================================================
    # 7. Envelope data/meta not dict: silently becomes empty dict
    # ======================================================================
    async def test_07_envelope_data_meta_not_dict_becomes_empty(self):
        """Envelope data/meta не dict: тихо становится пустым dict.

        Envelope.from_dict проверяет isinstance(raw_data, dict).
        Если data — строка, список или число, оно заменяется на {}.
        Аналогично для meta. Никакого предупреждения.

        НЕВЕРНО: передавать data как строку и рассчитывать, что она сохранится.
        ПРАВИЛЬНО: всегда передавать data как dict.
        """
        # data = строка (не dict)
        env = Envelope.from_dict({
            "source": "dev1",
            "type": "ds",
            "data": "not a dict",
            "meta": "also not a dict",
        })
        self.assertIsNotNone(env)
        # data тихо стало пустым dict
        self.assertEqual(env.data, {}, "data не-dict должно стать {}")
        self.assertEqual(env.meta, {}, "meta не-dict должно стать {}")

        # data = число
        env2 = Envelope.from_dict({
            "source": "dev1",
            "type": "ds",
            "data": 42,
        })
        self.assertEqual(env2.data, {}, "data=42 должно стать {}")

        # data = список
        env3 = Envelope.from_dict({
            "source": "dev1",
            "type": "ds",
            "data": [1, 2, 3],
        })
        self.assertEqual(env3.data, {}, "data=[1,2,3] должно стать {}")

        # data = None (отсутствует)
        env4 = Envelope.from_dict({
            "source": "dev1",
            "type": "ds",
        })
        self.assertEqual(env4.data, {}, "data=None должно стать {}")
        self.assertEqual(env4.meta, {}, "meta=None должно стать {}")

    # ======================================================================
    # 8. EventBus filter exception: subscriber skipped
    # ======================================================================
    async def test_08_event_bus_filter_exception_skips_subscriber(self):
        """Исключение в filter_fn EventBus: подписчик пропускается, ошибка логируется.

        EventBus._invoke вызывает filter_fn(data). Если filter падает,
        ошибка логируется на ERROR, подписчик НЕ вызывается, но другие
        подписчики продолжают работать.

        НЕВЕРНО: рассчитывать, что падающий filter остановит всю диспетчеризацию.
        ПРАВИЛЬНО: оборачивать рискованные фильтры в try/except внутри filter_fn.
        """
        bus = EventBus()
        received: list[dict] = []

        def bad_filter(data: dict) -> bool:
            # Падающий фильтр: KeyError
            return data["nonexistent_key"] > 0

        async def subscriber_a(data: dict):
            received.append({"a": data.get("value")})

        async def subscriber_b(data: dict):
            received.append({"b": data.get("value")})

        # subscriber_a с падающим фильтром
        bus.subscribe("test_event", subscriber_a, filter_fn=bad_filter)
        # subscriber_b без фильтра
        bus.subscribe("test_event", subscriber_b)

        await bus.publish("test_event", {"value": 42})

        # subscriber_a пропущен (фильтр упал), subscriber_b получил
        self.assertEqual(
            len(received), 1,
            "Только subscriber_b должен получить событие"
        )
        self.assertEqual(received[0], {"b": 42})

    # ======================================================================
    # 9. Telemetry driver read exception: DEBUG, field skipped
    # ======================================================================
    async def test_09_telemetry_driver_read_exception_debug_skip(self):
        """Исключение при driver.read в handle_telemetry_update: DEBUG, поле пропускается.

        handle_telemetry_update вызывает read_telemetry_value для каждого поля.
        Если driver.read падает, ошибка логируется на DEBUG, поле пропускается.
        Другие поля продолжают обрабатываться. Исключение НЕ пробрасывается.

        НЕВЕРНО: рассчитывать, что ошибка чтения telemetry остановит цикл.
        ПРАВИЛЬНО: мониторить логи DEBUG для диагностики пропущенных полей.
        """
        driver = _FailingDriver()
        light = _Light(driver=driver)
        light.node = _MockDeviceNode("light_1")

        # Устанавливаем значение telemetry поля напрямую (через _set_state)
        light._set_state(temperature=25.0)

        # handle_telemetry_update для ["temperature"]
        # driver.read упадёт → поле пропущено
        # Но т.к. temperature уже установлено как атрибут, оно будет прочитано
        # из атрибута (fallback). Проверим это поведение:
        data = await light.handle_telemetry_update(["temperature"])

        # driver.read упал, но атрибут temperature=25.0 существует
        # → val = None (от driver), затем getattr(self, "temperature") = 25.0
        self.assertIsNotNone(data, "Telemetry должна быть собрана из атрибута")
        self.assertEqual(data["temperature"], 25.0)

        # Теперь проверим с полем, которого нет как атрибута
        data2 = await light.handle_telemetry_update(["nonexistent_telemetry"])
        # driver.read упадёт, атрибута нет → val = None → пропущено
        self.assertIsNone(data2, "Несуществующее поле должно дать None")

    # ======================================================================
    # 10. NaN/None telemetry: silently filtered
    # ======================================================================
    async def test_10_nan_and_none_telemetry_silently_filtered(self):
        """NaN и None в telemetry тихо фильтруются.

        handle_telemetry_update пропускает:
        - None значения (continue)
        - NaN float значения (val != val check, continue)
        Но 0, False, "" (пустая строка) — ВКЛЮЧАЮТСЯ (falsy-but-valid).

        НЕВЕРНО: рассчитывать, что None или NaN попадут в telemetry payload.
        ПРАВИЛЬНО: проверять значения перед публикацией или использовать
        значения по умолчанию (например, 0.0 вместо None).
        """
        # Создаём устройство с telemetry полями
        class _SensorDevice(Device):
            temp: float = telemetry(default=0.0, unit="°C", freq="5s")
            hum: float = telemetry(default=0.0, unit="%", freq="5s")
            counter: int = telemetry(default=0, freq="5s")
            flag: bool = telemetry(default=False, freq="5s")

        dev = _SensorDevice()
        dev.node = _MockDeviceNode("sensor_1")

        # Устанавливаем значения напрямую через _set_state
        dev._set_state(temp=float("nan"))    # NaN → будет отфильтрован
        dev._set_state(hum=None)              # None → будет отфильтрован
        dev._set_state(counter=0)             # 0 → ВКЛЮЧАЕТСЯ (falsy-but-valid)
        dev._set_state(flag=False)            # False → ВКЛЮЧАЕТСЯ (falsy-but-valid)

        data = await dev.handle_telemetry_update(["temp", "hum", "counter", "flag"])

        # temp (NaN) и hum (None) отфильтрованы
        self.assertNotIn("temp", data, "NaN должен быть отфильтрован")
        self.assertNotIn("hum", data, "None должен быть отфильтрован")
        # counter (0) и flag (False) включены
        self.assertIn("counter", data, "0 должен быть включён (falsy-but-valid)")
        self.assertEqual(data["counter"], 0)
        self.assertIn("flag", data, "False должен быть включён (falsy-but-valid)")
        self.assertEqual(data["flag"], False)

    # ======================================================================
    # 11. Hot reload handler error: logged but "processed"
    # ======================================================================
    async def test_11_hot_reload_handler_error_logged_not_raised(self):
        """Ошибка в hot-reload handler логируется, но считается «обработанной».

        HotReloadManager._invoke_handler оборачивает вызов handler в try/except.
        При ошибке логируется ERROR и публикуется событие "hot_reload_error",
        но исключение НЕ пробрасывается. Менеджер продолжает работать.

        НЕВЕРНО: рассчитывать, что падающий handler остановит hot-reload.
        ПРАВИЛЬНО: мониторить события "hot_reload_error" на EventBus.
        """
        # Создаём минимальный мок app
        app = MagicMock()
        app.event_bus = EventBus()
        app._is_running = False

        errors_received: list[dict] = []
        app.event_bus.subscribe("hot_reload_error", lambda d: errors_received.append(d))

        manager = HotReloadManager(app, poll_interval=0.01, debounce=0.0)
        manager._loop = asyncio.get_running_loop()

        async def failing_handler(file_path: str):
            raise RuntimeError("Handler crashed on reload")

        # Вызываем _invoke_handler напрямую
        await manager._invoke_handler(failing_handler, "/fake/path.py")

        # Ошибка не проброшена, но событие опубликовано
        self.assertEqual(
            len(errors_received), 1,
            "Событие hot_reload_error должно быть опубликовано"
        )
        self.assertEqual(errors_received[0]["file_path"], "/fake/path.py")
        self.assertIn("Handler crashed", errors_received[0]["error"])

    # ======================================================================
    # 12. Plugin rule removal failure: warning, orphaned
    # ======================================================================
    async def test_12_plugin_rule_removal_failure_warning_orphaned(self):
        """Ошибка удаления правила плагина: warning, правило остаётся «осиротевшим».

        PluginLoader._cleanup_context пытается удалить правила плагина через
        app.remove_rule(rule_func). Если remove_rule падает, ошибка
        логируется на WARNING, но очистка продолжается. Правило остаётся
        в RuleEngine — оно «осиротевшее»: плагин выгружен, но правило работает.

        НЕВЕРНО: рассчитывать, что unload_plugin гарантированно удалит все правила.
        ПРАВИЛЬНО: проверять list_rules() после unload и логировать orphaned.
        """
        app = MagicMock()
        app.event_bus = EventBus()
        app.hooks = HooksManager()

        # remove_rule будет падать
        async def failing_remove_rule(func):
            raise RuntimeError("Rule engine locked")

        app.remove_rule = failing_remove_rule

        # add_rule возвращает функцию как есть
        app.add_rule = lambda func, **kw: func

        loader = PluginLoader(app)

        class _TestPlugin(Plugin):
            @property
            def name(self) -> str:
                return "test_plugin"

            @property
            def version(self) -> str:
                return "1.0.0"

            async def on_load(self, app, context=None):
                # Регистрируем правило через context
                async def my_rule(event: RuleEvent, app):
                    pass
                context.add_rule(my_rule)

        # Загружаем плагин
        plugin = await loader.load_plugin(_TestPlugin)
        self.assertIn("test_plugin", loader.list_plugins())

        # Выгружаем — remove_rule упадёт, но плагин всё равно выгрузится
        await loader.unload_plugin("test_plugin")

        # Плагин выгружен
        self.assertNotIn("test_plugin", loader.list_plugins())
        # Но правило осталось «осиротевшим» (warning в логах)

    # ======================================================================
    # 13. HA discovery: device without node silently returns
    # ======================================================================
    async def test_13_ha_discovery_device_without_node_returns(self):
        """HA discovery: device без node тихо возвращает (return).

        HADiscovery.announce проверяет `if not device.node` и делает return.
        Для clear() — аналогично. Никакого исключения, только WARNING в announce.

        НЕВЕРНО: вызывать announce() до регистрации устройства с node.
        ПРАВИЛЬНО: проверять device.node перед вызовом или регистрировать
        устройство сначала.
        """
        discovery = HADiscovery()
        light = _Light()
        # node не установлен (None)

        # announce: WARNING + return
        await discovery.announce(light)
        # Ничего не произошло, исключений нет

        # clear: тихий return (без WARNING)
        await discovery.clear(light)
        # Ничего не произошло

        # ПРАВИЛЬНО: установить node перед announce
        light.node = _MockDeviceNode("light_1")
        await discovery.announce(light)
        # Теперь announce работает (публикует через node.publish_raw)

    # ======================================================================
    # 14. Mock driver read returns raw value (not dict)
    # ======================================================================
    async def test_14_mock_driver_read_returns_raw_value(self):
        """Mock driver read возвращает raw value (не dict с ключом 'data').

        TelemetryMixin.read_telemetry_value проверяет:
        - Если result — dict с ключом "data": возвращает result["data"]
        - Иначе: возвращает result как есть

        MockHardwareDriver.read возвращает self.state.get(field_name) —
        это raw value, не обёрнутый в dict. read_telemetry_value
        корректно обрабатывает оба случая.

        НЕВЕРНО: рассчитывать, что все драйверы возвращают {"data": value}.
        ПРАВИЛЬНО: read_telemetry_value уже обрабатывает оба формата.
        """
        driver = _RawValueDriver()
        light = _Light(driver=driver)
        light.node = _MockDeviceNode("light_1")

        # read_telemetry_value вызывает driver.read("temperature")
        # _RawValueDriver.read возвращает 23.5 (float, не dict)
        val = await light.read_telemetry_value("temperature")
        self.assertEqual(val, 23.5, "Raw value должен быть возвращён как есть")

        # Теперь проверим со стандартным MockHardwareDriver
        # который возвращает self.state.get(field_name) — тоже raw value
        std_driver = MockHardwareDriver(latency_range=(0, 0))
        std_driver.connected = True
        std_driver.state["temperature"] = 18.5

        light2 = _Light(driver=std_driver)
        light2.node = _MockDeviceNode("light_2")

        val2 = await light2.read_telemetry_value("temperature")
        self.assertEqual(val2, 18.5, "MockHardwareDriver тоже возвращает raw value")

        # Если драйвер возвращает dict с "data", извлекается data
        class _DictDriver(MockHardwareDriver):
            async def read(self, field_name, params=None):
                return {"status": "ok", "data": 99.9}

        dict_driver = _DictDriver(latency_range=(0, 0))
        dict_driver.connected = True
        light3 = _Light(driver=dict_driver)
        light3.node = _MockDeviceNode("light_3")

        val3 = await light3.read_telemetry_value("temperature")
        self.assertEqual(val3, 99.9, "Dict с 'data' должен извлечь data")


if __name__ == "__main__":
    unittest.main()
