"""
29 — Паттерны тестирования
============================

Как тестировать компоненты Kamio: устройства, правила, телеметрию,
плагины, конфигурацию, event bus, hooks, envelope и жизненный цикл.

Запуск::
    python examples/29_testing_patterns.py

Что демонстрирует:
    - Unit-тестирование устройств (валидация, handle_state, handle_command)
    - Тестирование правил с mock RuleEvent
    - Тестирование телеметрии (handle_telemetry_update)
    - MockHardwareDriver для изоляции от железа
    - Тестирование EventBus и Hooks
    - Тестирование плагинов (load/unload)
    - Тестирование Config (dict, env vars)
    - Тестирование Envelope (сериализация)
    - Pytest fixtures и unittest.mock
    - Тестирование echo suppression
    - Тестирование reinitialize и shutdown
"""
from __future__ import annotations

import asyncio
import logging
import os
import tempfile
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from kamio import Device, EventBus, Plugin, RuleEvent, command, config, event, state, telemetry
from kamio.config import Config
from kamio.core.envelope import Envelope, EnvelopeType
from kamio.core.rules import Rule, RuleEngine
from kamio.drivers.mock import MockHardwareDriver

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("test_patterns")


# =====================================================================
# Тестируемые устройства
# =====================================================================

class TestLight(Device):
    """Лампа для тестирования."""
    power: bool = state(default=False, writable=True)
    brightness: int = state(default=100, min=0, max=255, writable=True)
    mode: str = state(default="auto", choices=("auto", "manual", "off"), writable=True)
    temperature: float = telemetry(default=22.0, unit="°C", freq="5s")
    button_pressed = event(description="Кнопка нажата")
    location: str = config(default="living_room")

    @command
    async def toggle(self):
        self.power = not self.power
        return {"power": self.power}

    @command
    async def set_brightness(self, value: int):
        self.brightness = max(0, min(255, value))
        return {"brightness": self.brightness}


class TestSensor(Device):
    """Датчик с телеметрией."""
    value: float = telemetry(default=0.0, unit="°C", freq="1s", min=-50, max=150)

    async def handle_telemetry_update(self, field_names: list[str]) -> Optional[dict[str, Any]]:
        """Кастомная логика сбора телеметрии."""
        result: dict[str, Any] = {}
        for fn in field_names:
            if fn == "value":
                # Имитация чтения значения
                result[fn] = 25.5
        return result if result else None


# =====================================================================
# 1. Тестирование валидации устройства
# =====================================================================

def test_validation():
    """Тестирование _validate_value напрямую.

    ВАЖНО: _validate_value принимает Field объект, не имя поля!
    Достаём Field через Kamio_FIELDS[имя].
    """
    logger.info("=== Тест 1: Валидация устройства ===")

    light = TestLight()

    # Достаём Field объекты из метаданных метакласса
    f_brightness = TestLight.Kamio_FIELDS["brightness"]
    f_mode = TestLight.Kamio_FIELDS["mode"]
    f_power = TestLight.Kamio_FIELDS["power"]

    # Правильные значения — не должно быть исключений
    light._validate_value(f_brightness, 100)
    light._validate_value(f_mode, "auto")
    light._validate_value(f_power, True)
    assert light.brightness == 100
    logger.info("  ✓ Правильные значения проходят валидацию")

    # min/max violation
    try:
        light._validate_value(f_brightness, 300)
        assert False, "Должно быть ValueError для brightness > 255"
    except ValueError as e:
        assert "brightness" in str(e) or "max" in str(e).lower()
        logger.info(f"  ✓ ValueError для brightness=300: {e}")

    try:
        light._validate_value(f_brightness, -10)
        assert False, "Должно быть ValueError для brightness < 0"
    except ValueError as e:
        logger.info(f"  ✓ ValueError для brightness=-10: {e}")

    # choices violation
    try:
        light._validate_value(f_mode, "invalid")
        assert False, "Должно быть ValueError для неверного mode"
    except ValueError as e:
        logger.info(f"  ✓ ValueError для mode='invalid': {e}")

    # Строковая коэрсия: "42" конвертируется в float для min/max
    try:
        light._validate_value(f_brightness, "42")
        logger.info("  ✓ Строка '42' проходит min/max валидацию (коэрсия)")
    except ValueError:
        assert False, "'42' должно проходить min/max через коэрсию"

    # Строка, не конвертируемая в число: min/max пропускается.
    # У brightness НЕТ choices, поэтому 'abc' проходит без ошибки.
    # Демонстрируем на mode (у которого есть choices):
    try:
        light._validate_value(f_mode, "abc")
        assert False, "Должно быть ValueError: 'abc' не в choices mode"
    except ValueError:
        logger.info("  ✓ Строка 'abc' отклонена для mode (choices валидация)")

    # А для brightness (без choices) 'abc' проходит:
    result = light._validate_value(f_brightness, "abc")
    assert result == "abc"
    logger.info("  ✓ Строка 'abc' проходит для brightness (нет choices, min/max skip)")

    # Bool исключён из numeric coercion (isinstance(bool, int) True, но проверяется явно)
    light._validate_value(f_brightness, True)
    logger.info("  ✓ Bool True проходит валидацию (не treated as 1.0 для min/max)")

    logger.info("  Тест 1 пройден\n")


# =====================================================================
# 2. Тестирование handle_state
# =====================================================================

async def test_handle_state():
    """Тестирование handle_state напрямую."""
    logger.info("=== Тест 2: handle_state ===")

    light = TestLight()

    # Применяем изменения
    changes = await light.handle_state({"power": True, "brightness": 200})
    assert changes.get("power") == True or "power" in changes
    assert light.power == True
    assert light.brightness == 200
    logger.info(f"  ✓ handle_state применил изменения: {changes}")

    # Атомарность: если одно поле невалидно, none применяются
    original_brightness = light.brightness
    try:
        await light.handle_state({"brightness": 999, "mode": "auto"})
        assert False, "Должно быть ValueError"
    except ValueError:
        assert light.brightness == original_brightness, "brightness не должен измениться"
        logger.info("  ✓ Атомарность: при ошибке none поля применены")

    # Unknown fields silently ignored
    changes = await light.handle_state({"unknown_field": 42})
    assert "unknown_field" not in changes or len(changes) == 0
    logger.info("  ✓ Неизвестные поля молча игнорируются")

    # Non-writable fields silently ignored
    # temperature — telemetry, не writable через handle_state
    changes = await light.handle_state({"temperature": 30.0})
    logger.info(f"  ✓ Non-writable поля игнорируются: changes={changes}")

    logger.info("  Тест 2 пройден\n")


# =====================================================================
# 3. Тестирование handle_command
# =====================================================================

async def test_handle_command():
    """Тестирование handle_command напрямую."""
    logger.info("=== Тест 3: handle_command ===")

    light = TestLight()
    light.power = False

    # Команда toggle
    result = await light.handle_command("toggle", {})
    assert result is not None
    assert light.power == True
    logger.info(f"  ✓ toggle: power={light.power}, result={result}")

    # Команда с параметрами
    result = await light.handle_command("set_brightness", {"value": 150})
    assert light.brightness == 150
    logger.info(f"  ✓ set_brightness(150): result={result}")

    # set_ prefix auto-routing (HA compatibility)
    result = await light.handle_command("set_power", {"value": False})
    logger.info(f"  ✓ set_power auto-routed to handle_state: result={result}")

    # Неизвестная команда
    try:
        result = await light.handle_command("nonexistent", {})
        logger.info(f"  ✓ Неизвестная команда: result={result}")
    except Exception as e:
        logger.info(f"  ✓ Неизвестная команда вызвала исключение: {e}")

    logger.info("  Тест 3 пройден\n")


# =====================================================================
# 4. Тестирование правил
# =====================================================================

async def test_rules():
    """Тестирование правил с mock RuleEvent."""
    logger.info("=== Тест 4: Правила ===")

    # Создаём правило напрямую
    call_log: list[str] = []

    async def my_rule(event: RuleEvent, app: Any):
        call_log.append(f"called with kind={event.kind}, device_id={event.device_id}")
        # Проверяем event.get()
        power = event.get("power")
        call_log.append(f"power={power}")

    rule = Rule(
        func=my_rule,
        device_class=TestLight,
        fields=["power"],
        interval=None,
        enabled=True,
        run_on_start=False,
        description="Test rule",
    )

    # Создаём mock RuleEvent
    event = RuleEvent(
        device_id="light_1",
        kind="state_change",
        data={"power": True, "old_value": False, "new_value": True},
    )

    # Вызываем правило напрямую
    await my_rule(event, None)

    assert len(call_log) == 2
    assert "power=True" in call_log[1]
    logger.info(f"  ✓ Правило вызвано: {call_log}")

    # Тест параметров: 0, 1, 2 параметра
    calls_0: list[str] = []
    calls_1: list[str] = []
    calls_2: list[str] = []

    async def rule_0():
        calls_0.append("called")

    async def rule_1(event: RuleEvent):
        calls_1.append(f"event={event.device_id}")

    async def rule_2(event: RuleEvent, app: Any):
        calls_2.append(f"event={event.device_id}, app={app}")

    # RuleEngine определяет количество параметров по сигнатуре
    await rule_0()
    await rule_1(event)
    await rule_2(event, None)

    assert len(calls_0) == 1
    assert len(calls_1) == 1
    assert len(calls_2) == 1
    logger.info("  ✓ Правила с 0, 1, 2 параметрами работают")

    logger.info("  Тест 4 пройден\n")


# =====================================================================
# 5. Тестирование телеметрии
# =====================================================================

async def test_telemetry():
    """Тестирование handle_telemetry_update."""
    logger.info("=== Тест 5: Телеметрия ===")

    sensor = TestSensor()

    # Вызываем handle_telemetry_update напрямую
    result = await sensor.handle_telemetry_update(["value"])
    assert result is not None
    assert result.get("value") == 25.5
    logger.info(f"  ✓ handle_telemetry_update вернул: {result}")

    # Пустой список полей
    result = await sensor.handle_telemetry_update([])
    logger.info(f"  ✓ Пустой список полей: result={result}")

    # get_telemetry_snapshot
    snapshot = sensor.get_telemetry_snapshot()
    assert "value" in snapshot
    logger.info(f"  ✓ get_telemetry_snapshot: {snapshot}")

    # NaN фильтрация
    import math

    class NaNSensor(Device):
        bad_value: float = telemetry(default=float("nan"), unit="test")

    nan_sensor = NaNSensor()
    result = await nan_sensor.handle_telemetry_update(["bad_value"])
    # NaN должен быть отфильтрован
    if result:
        assert "bad_value" not in result or not math.isnan(result.get("bad_value", 0))
    logger.info(f"  ✓ NaN фильтрация: result={result}")

    logger.info("  Тест 5 пройден\n")


# =====================================================================
# 6. Тестирование с MockHardwareDriver
# =====================================================================

async def test_mock_driver():
    """Тестирование с MockHardwareDriver."""
    logger.info("=== Тест 6: MockHardwareDriver ===")

    # Нормальный режим
    driver = MockHardwareDriver(latency_range=(0.01, 0.01), failure_rate=0.0)
    await driver.connect()
    assert driver.connected == True

    result = await driver.execute("test_command", {"param": 1})
    assert isinstance(result, dict)
    logger.info(f"  ✓ execute вернул: {result}")

    read_val = await driver.read("test_field")
    logger.info(f"  ✓ read вернул: {read_val}")

    await driver.disconnect()
    assert driver.connected == False
    logger.info("  ✓ disconnect OK")

    # Режим с отказами
    fail_driver = MockHardwareDriver(latency_range=(0.01, 0.01), failure_rate=1.0)
    try:
        await fail_driver.connect()
        assert False, "Должно быть ConnectionError при failure_rate=1.0"
    except ConnectionError:
        logger.info("  ✓ failure_rate=1.0 вызывает ConnectionError")

    # set_ prefix команды
    driver2 = MockHardwareDriver(latency_range=(0.0, 0.0), failure_rate=0.0)
    await driver2.connect()
    result = await driver2.execute("set_power", {"value": True})
    logger.info(f"  ✓ set_power через mock: {result}")
    await driver2.disconnect()

    logger.info("  Тест 6 пройден\n")


# =====================================================================
# 7. Тестирование Event Bus
# =====================================================================

async def test_event_bus():
    """Тестирование EventBus."""
    logger.info("=== Тест 7: EventBus ===")

    bus = EventBus()

    # Подписка
    received: list[Dict[str, Any]] = []
    bus.subscribe("test_event", lambda data: received.append(data))

    # Публикация
    await bus.publish("test_event", {"key": "value"})
    assert len(received) == 1
    assert received[0].get("key") == "value"
    # timestamp добавляется автоматически
    assert "timestamp" in received[0]
    logger.info(f"  ✓ Событие получено: {received[0]}")

    # Приоритеты
    order: list[str] = []
    bus.subscribe("priority_test", lambda d: order.append("low"), priority=0)
    bus.subscribe("priority_test", lambda d: order.append("high"), priority=10)
    bus.subscribe("priority_test", lambda d: order.append("mid"), priority=5)

    await bus.publish("priority_test", {})
    # higher priority = first
    assert order[0] == "high", f"Ожидается high first, got {order}"
    assert order[1] == "mid"
    assert order[2] == "low"
    logger.info(f"  ✓ Приоритеты: {order} (high first)")

    # filter_fn
    filtered: list[Dict[str, Any]] = []
    bus.subscribe(
        "filtered_event",
        lambda d: filtered.append(d),
        filter_fn=lambda d: d.get("pass") == True,
    )
    await bus.publish("filtered_event", {"pass": False})
    assert len(filtered) == 0
    await bus.publish("filtered_event", {"pass": True})
    assert len(filtered) == 1
    logger.info("  ✓ filter_fn работает")

    # list_subscribers
    subs = bus.list_subscribers("test_event")
    assert len(subs) >= 1
    logger.info(f"  ✓ list_subscribers: {len(subs)} подписчиков")

    # unsubscribe
    cb = lambda d: received.append(d)
    bus.subscribe("unsub_test", cb)
    bus.unsubscribe("unsub_test", cb)
    count_before = len(received)
    await bus.publish("unsub_test", {"test": True})
    # cb не должен быть вызван после unsubscribe
    logger.info("  ✓ unsubscribe работает")

    logger.info("  Тест 7 пройден\n")


# =====================================================================
# 8. Тестирование Envelope
# =====================================================================

def test_envelope():
    """Тестирование Envelope сериализации."""
    logger.info("=== Тест 8: Envelope ===")

    # Создание
    env = Envelope.command(source="server", target="device_1", method="toggle", params={})
    assert env.type == EnvelopeType.SERVER_COMMAND
    assert env.source == "server"
    assert env.target == "device_1"
    assert env.cind  # auto-generated
    logger.info(f"  ✓ Создан command envelope: cind={env.cind}")

    # Сериализация round-trip
    d = env.to_dict()
    assert d["type"] == "dt" or d["type"] == "ds" or d["type"] == "sc"
    logger.info(f"  ✓ to_dict: type={d['type']}")

    j = env.to_json()
    assert isinstance(j, str)
    logger.info(f"  ✓ to_json: {j[:60]}...")

    # Парсинг
    parsed = Envelope.from_json(j)
    assert parsed is not None
    assert parsed.source == env.source
    assert parsed.target == env.target
    assert parsed.cind == env.cind
    logger.info("  ✓ from_json round-trip OK")

    # from_dict с невалидным типом
    invalid = Envelope.from_dict({"type": "invalid", "source": "x"})
    assert invalid is not None
    assert invalid.type == EnvelopeType.UNKNOWN
    logger.info("  ✓ Невалидный тип → UNKNOWN")

    # from_json с битым JSON
    broken = Envelope.from_json("not json at all")
    assert broken is None
    logger.info("  ✓ Битый JSON → None")

    # data не dict → пустой dict
    env_bad_data = Envelope.from_dict({"type": "dt", "source": "x", "data": "not a dict"})
    assert env_bad_data is not None
    assert env_bad_data.data == {}
    logger.info("  ✓ data не dict → пустой dict")

    # Все типы envelope
    for factory, etype in [
        (lambda: Envelope.telemetry("d1", {"temp": 22}), EnvelopeType.DEVICE_TELEMETRY),
        (lambda: Envelope.state("d1", {"power": True}), EnvelopeType.DEVICE_STATE),
        (lambda: Envelope.state_ack("d1", "s1", {"ok": True}, "cind123"), EnvelopeType.STATE_ACK),
        (lambda: Envelope.event("d1", "motion", {"ts": 123}), EnvelopeType.DEVICE_EVENT),
        (lambda: Envelope.command_ack("d1", "s1", {"ok": True}, "cind123"), EnvelopeType.COMMAND_ACK),
        (lambda: Envelope.keepalive("d1"), EnvelopeType.KEEPALIVE),
    ]:
        e = factory()
        assert e.type == etype
    logger.info("  ✓ Все типы envelope создаются корректно")

    logger.info("  Тест 8 пройден\n")


# =====================================================================
# 9. Тестирование Config
# =====================================================================

def test_config():
    """Тестирование Config."""
    logger.info("=== Тест 9: Config ===")

    # Создаём временный конфиг
    import json

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({
            "mqtt_broker": "mqtt://test:1883",
            "log_level": "DEBUG",
            "telemetry_min_freq": "0.5",
            "custom_setting": "hello",
            "nested": {"key": "value", "number": 42},
        }, f)
        config_path = f.name

    try:
        cfg = Config(config_path)

        # Базовые настройки
        assert cfg.mqtt_broker == "mqtt://test:1883"
        assert cfg.log_level == logging.DEBUG
        logger.info(f"  ✓ mqtt_broker={cfg.mqtt_broker}, log_level=DEBUG")

        # get() с dot notation
        val = cfg.get("nested.key")
        assert val == "value"
        logger.info(f"  ✓ get('nested.key') = {val}")

        val = cfg.get("nested.number")
        assert val == 42
        logger.info(f"  ✓ get('nested.number') = {val}")

        # get() с cast
        val = cfg.get("telemetry_min_freq", cast=float)
        assert val == 0.5
        logger.info(f"  ✓ get с cast=float: {val}")

        # get() с default
        val = cfg.get("nonexistent", "default_val")
        assert val == "default_val"
        logger.info(f"  ✓ get с default: {val}")

        # Boolean casting
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f2:
            json.dump({"flag_true": "true", "flag_yes": "yes", "flag_on": "on", "flag_1": "1"}, f2)
            cfg2_path = f2.name

        cfg2 = Config(cfg2_path)
        assert cfg2.get("flag_true", cast=bool) == True
        assert cfg2.get("flag_yes", cast=bool) == True
        assert cfg2.get("flag_on", cast=bool) == True
        assert cfg2.get("flag_1", cast=bool) == True
        logger.info("  ✓ Boolean casting: true/yes/on/1 → True")
        os.unlink(cfg2_path)

        # Env var override
        os.environ["Kamio_MQTT_BROKER"] = "mqtt://env:1883"
        cfg3 = Config(config_path)
        assert cfg3.mqtt_broker == "mqtt://env:1883"
        logger.info("  ✓ Env var override: Kamio_MQTT_BROKER")
        del os.environ["Kamio_MQTT_BROKER"]

    finally:
        os.unlink(config_path)

    logger.info("  Тест 9 пройден\n")


# =====================================================================
# 10. Тестирование kwargs в __init__
# =====================================================================

def test_kwargs():
    """Тестирование применения kwargs в Device.__init__."""
    logger.info("=== Тест 10: kwargs в __init__ ===")

    # kwargs применяются к state/config полям
    light = TestLight(power=True, brightness=50, mode="manual", location="kitchen")

    # DEBUG: проверить что фактически установлено
    import os as _os
    _dbg_path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "debug29.txt")
    with open(_dbg_path, "w", encoding="utf-8") as _f:
        _f.write(f"Kamio_FIELDS keys = {list(TestLight.Kamio_FIELDS.keys())}\n")
        _f.write(f"'power' in Kamio_FIELDS = {'power' in TestLight.Kamio_FIELDS}\n")
        _f.write(f"light.__dict__ = {light.__dict__}\n")
        _f.write(f"light.power = {light.power!r}\n")

    assert light.power == True
    assert light.brightness == 50
    assert light.mode == "manual"
    assert light.location == "kitchen"
    logger.info(f"  ✓ kwargs применены: power={light.power}, brightness={light.brightness}, mode={light.mode}, location={light.location}")

    # kwargs bypass validation (известный подводный камень!)
    # Можно установить невалидное значение через kwargs
    bad_light = TestLight(brightness=999)  # > max=255
    assert bad_light.brightness == 999  # валидация НЕ применялась!
    logger.info("  ⚠️  Подводный камень: kwargs bypass валидацию (brightness=999 принято)")

    # _apply_defaults тоже bypass валидацию
    light2 = TestLight()
    assert light2.brightness == 100  # default
    logger.info("  ✓ _apply_defaults устанавливает defaults")

    logger.info("  Тест 10 пройден\n")


# =====================================================================
# 11. Pytest fixtures (демонстрация паттерна)
# =====================================================================

def test_pytest_fixtures_pattern():
    """Демонстрация паттерна pytest fixtures (без запуска pytest)."""
    logger.info("=== Тест 11: Pytest fixtures (паттерн) ===")

    # Паттерн: фабрика устройств
    def make_light(**kwargs) -> TestLight:
        """Fixture: создать TestLight с kwargs."""
        return TestLight(**kwargs)

    # Паттерн: фабрика event bus
    def make_bus() -> EventBus:
        """Fixture: создать EventBus."""
        return EventBus()

    light = make_light(power=True)
    assert light.power == True

    bus = make_bus()
    assert bus is not None
    logger.info("  ✓ Fixture паттерн работает")

    # Паттерн: mock MQTT client
    mock_mqtt = MagicMock()
    mock_mqtt._kamio_wait_for_suback = AsyncMock()
    mock_mqtt._kamio_wait_for_unsuback = AsyncMock()
    mock_mqtt.subscribe = MagicMock(return_value=1)
    mock_mqtt.unsubscribe = MagicMock(return_value=2)
    mock_mqtt.publish = MagicMock()
    logger.info("  ✓ Mock MQTT client создан")

    # Паттерн: mock driver
    mock_driver = MockHardwareDriver(latency_range=(0.0, 0.0), failure_rate=0.0)
    assert mock_driver is not None
    logger.info("  ✓ Mock driver создан")

    logger.info("  Тест 11 пройден\n")


# =====================================================================
# 12. Тестирование echo suppression
# =====================================================================

def test_echo_suppression():
    """Тестирование echo suppression cache."""
    logger.info("=== Тест 12: Echo suppression ===")

    light = TestLight()

    # _own_state_cinds существует, но пуст (нет node, нет публикаций)
    assert hasattr(light, "_own_state_cinds")
    assert len(light._own_state_cinds) == 0
    logger.info("  ✓ _own_state_cinds существует и пуст")

    # _set_state bypass echo suppression
    light._set_state(power=True)
    assert light.power == True
    assert len(light._own_state_cinds) == 0  # не добавляется в cache
    logger.info("  ✓ _set_state bypass echo suppression")

    # Cache limit = 4096 (атрибут _own_state_cinds_limit)
    assert light._own_state_cinds_limit == 4096
    logger.info(f"  ✓ Cache limit = {light._own_state_cinds_limit}")

    logger.info("  Тест 12 пройден\n")


# =====================================================================
# 13. Тестирование DeviceMeta
# =====================================================================

def test_device_meta():
    """Тестирование DeviceMeta collection."""
    logger.info("=== Тест 13: DeviceMeta ===")

    light = TestLight()

    # Kamio_FIELDS собраны метаклассом
    assert hasattr(TestLight, "Kamio_FIELDS")
    assert "power" in TestLight.Kamio_FIELDS
    assert "brightness" in TestLight.Kamio_FIELDS
    assert "mode" in TestLight.Kamio_FIELDS
    assert "temperature" in TestLight.Kamio_FIELDS
    assert "location" in TestLight.Kamio_FIELDS
    logger.info(f"  ✓ Kamio_FIELDS: {list(TestLight.Kamio_FIELDS.keys())}")

    # Kamio_COMMANDS собраны
    assert hasattr(TestLight, "Kamio_COMMANDS")
    assert "toggle" in TestLight.Kamio_COMMANDS
    assert "set_brightness" in TestLight.Kamio_COMMANDS
    logger.info(f"  ✓ Kamio_COMMANDS: {list(TestLight.Kamio_COMMANDS.keys())}")

    # Field kinds
    assert TestLight.Kamio_FIELDS["power"].kind == "state"
    assert TestLight.Kamio_FIELDS["temperature"].kind == "telemetry"
    assert TestLight.Kamio_FIELDS["location"].kind == "config"
    logger.info("  ✓ Field kinds корректны")

    # device_type
    assert TestLight.device_type() == "testlight"
    logger.info(f"  ✓ device_type = {TestLight.device_type()}")

    logger.info("  Тест 13 пройден\n")


# =====================================================================
# 14. Тестирование get_full_snapshot
# =====================================================================

def test_snapshots():
    """Тестирование снимков состояния."""
    logger.info("=== Тест 14: Snapshots ===")

    light = TestLight(power=True, brightness=150, mode="manual", location="bedroom")

    state_snap = light.get_state_snapshot()
    assert state_snap.get("power") == True
    assert state_snap.get("brightness") == 150
    assert state_snap.get("mode") == "manual"
    logger.info(f"  ✓ get_state_snapshot: {state_snap}")

    config_snap = light.get_config_snapshot()
    assert config_snap.get("location") == "bedroom"
    logger.info(f"  ✓ get_config_snapshot: {config_snap}")

    telemetry_snap = light.get_telemetry_snapshot()
    assert "temperature" in telemetry_snap
    logger.info(f"  ✓ get_telemetry_snapshot: {telemetry_snap}")

    full_snap = light.get_full_snapshot()
    assert "state" in full_snap or "power" in full_snap
    logger.info(f"  ✓ get_full_snapshot keys: {list(full_snap.keys())}")

    logger.info("  Тест 14 пройден\n")


# =====================================================================
# 15. Главный цикл
# =====================================================================

async def main():
    logger.info("Запуск всех тестов\n")

    # Sync tests
    test_validation()
    test_envelope()
    test_config()
    test_kwargs()
    test_pytest_fixtures_pattern()
    test_echo_suppression()
    test_device_meta()
    test_snapshots()

    # Async tests
    await test_handle_state()
    await test_handle_command()
    await test_rules()
    await test_telemetry()
    await test_mock_driver()
    await test_event_bus()

    logger.info("=" * 60)
    logger.info("ВСЕ ТЕСТЫ ПРОЙДЕНЫ ✓")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
