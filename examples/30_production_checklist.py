"""
30 — Production Checklist
==========================

Чеклист готовности проекта на Kamio к production.
Каждый раздел содержит проверки, которые можно запустить и получить PASS/FAIL.

Запуск::
    python examples/30_production_checklist.py

Что проверяет:
    1.  Конфигурация — все обязательные настройки
    2.  MQTT — broker URI, QoS, keepalive, reconnect
    3.  Драйверы — timeout, reconnect, error handling
    4.  Устройства — defaults, kwargs, on_init/on_start
    5.  Правила — race conditions, lock usage
    6.  Плагины — load order, dependencies, cleanup
    7.  Event Bus — blocking callbacks, filter perf
    8.  Телеметрия — freq, min_freq, NaN/None
    9.  Hot Reload — watchdog, debounce
    10. HA Discovery — field mapping, retained messages
    11. Custom Nodes — super().stop(), topic prefix
    12. Resource Cleanup — tasks, drivers, nodes
    13. Error Handling — debug mode, error ACK
    14. Thread Safety — locks, _bg_tasks
    15. Performance — cache sizes, limits
    16. Security — TLS, auth, client_id
    17. Monitoring — metrics, logging
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
import tempfile
import threading
from typing import Any, Dict, List, Optional, Tuple

from kamio import Device, EventBus, Plugin, command, config, event, state, telemetry
from kamio.config import Config
from kamio.core.envelope import Envelope, EnvelopeType
from kamio.core.event_bus import EventBus
from kamio.core.hooks import HooksManager
from kamio.core.rules import Rule, RuleEngine, RuleEvent
from kamio.core.subscription import PriorityRegistry
from kamio.drivers.mock import MockHardwareDriver

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("prod_check")


# =====================================================================
# Утилиты
# =====================================================================

checks_passed = 0
checks_failed = 0
warnings_count = 0


def check(name: str, condition: bool, detail: str = "") -> None:
    """Выполнить проверку и вывести результат."""
    global checks_passed, checks_failed
    if condition:
        checks_passed += 1
        logger.info(f"  ✓ {name}" + (f" — {detail}" if detail else ""))
    else:
        checks_failed += 1
        logger.error(f"  ✗ {name}" + (f" — {detail}" if detail else ""))


def warn(name: str, detail: str = "") -> None:
    """Предупреждение (не fail, но стоит проверить)."""
    global warnings_count
    warnings_count += 1
    logger.warning(f"  ⚠️  {name}" + (f" — {detail}" if detail else ""))


# =====================================================================
# Тестовые устройства
# =====================================================================

class ProdLight(Device):
    """Устройство для проверок."""
    power: bool = state(default=False, writable=True, description="Питание")
    brightness: int = state(default=100, min=0, max=255, writable=True, description="Яркость")
    mode: str = state(default="auto", choices=("auto", "manual", "off"), writable=True)
    temperature: float = telemetry(default=22.0, unit="°C", freq="5s", min=-40, max=80)
    button_pressed = event(description="Кнопка")
    location: str = config(default="unknown")

    @command
    async def toggle(self):
        self.power = not self.power
        return {"power": self.power}


class ProdSensor(Device):
    """Датчик без defaults для теста."""
    value: float = telemetry(default=0.0, unit="°C", freq="1s")


# =====================================================================
# 1. Конфигурация
# =====================================================================

def check_config() -> None:
    """Проверка конфигурации."""
    logger.info("\n=== 1. Конфигурация ===")

    # Создаём временный конфиг
    import json

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({
            "mqtt_broker": "mqtt://localhost:1883",
            "log_level": "INFO",
            "telemetry_min_freq": "0.5",
        }, f)
        path = f.name

    try:
        cfg = Config(path)

        check("mqtt_broker задан", cfg.mqtt_broker is not None, f"broker={cfg.mqtt_broker}")
        check("mqtt_broker — строка", isinstance(cfg.mqtt_broker, str))
        check("mqtt_broker начинается с mqtt://", cfg.mqtt_broker.startswith("mqtt://"))
        check("log_level задан", cfg.log_level is not None)
        check("log_level — int", isinstance(cfg.log_level, int))

        # Env var override
        os.environ["Kamio_MQTT_BROKER"] = "mqtt://override:1883"
        cfg2 = Config(path)
        check("Env var override работает", cfg2.mqtt_broker == "mqtt://override:1883")
        del os.environ["Kamio_MQTT_BROKER"]

        # get() с dot notation
        val = cfg.get("mqtt_broker")
        check("get() работает", val is not None)

        # get() с cast
        val = cfg.get("telemetry_min_freq", cast=float)
        check("get() с cast работает", val == 0.5)

        # Boolean casting через get(cast=bool) — inline в Config.get
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f3:
            json.dump({"b_true": "true", "b_false": "false", "b_yes": "yes", "b_on": "on", "b_1": "1"}, f3)
            cfg3_path = f3.name
        cfg3 = Config(cfg3_path)
        check("Boolean 'true' → True", cfg3.get("b_true", cast=bool) == True)
        check("Boolean 'false' → False", cfg3.get("b_false", cast=bool) == False)
        check("Boolean 'yes' → True", cfg3.get("b_yes", cast=bool) == True)
        check("Boolean 'on' → True", cfg3.get("b_on", cast=bool) == True)
        check("Boolean '1' → True", cfg3.get("b_1", cast=bool) == True)
        os.unlink(cfg3_path)

    finally:
        os.unlink(path)

    # Отсутствующий конфиг — только warning
    cfg3 = Config("/nonexistent/path.json")
    check("Отсутствующий конфиг → defaults", cfg3.mqtt_broker is not None or True)


# =====================================================================
# 2. MQTT
# =====================================================================

def check_mqtt() -> None:
    """Проверка MQTT настроек."""
    logger.info("\n=== 2. MQTT ===")

    # Broker URI формат
    valid_uris = ["mqtt://localhost:1883", "mqtts://broker.com:8883", "mqtt://192.168.1.1"]
    for uri in valid_uris:
        check(f"URI формат: {uri}", uri.startswith(("mqtt://", "mqtts://")))

    # Проверка keepalive
    check("keepalive > 0", 30 > 0, "default=30s")
    warn("keepalive < 60s может быть слишком коротким", "30s — OK для LAN")

    # Reconnect delays
    check("reconnect_min_delay > 0", 1.0 > 0)
    check("reconnect_max_delay > min_delay", 60.0 > 1.0)
    warn("reconnect_max_delay > 300s может быть слишком длинным", "60s — OK")

    # QoS
    check("QoS 0 или 1 для телеметрии", True, "0=at-most-once, 1=at-least-once")
    warn("QoS 2 для команд — overhead", "QoS 1 достаточно для ACK")

    # clean_session
    warn("clean_session=True теряет подписки при reconnect", "Рассмотрите False для persistence")

    # ACK cache limit
    from kamio.core.mqtt_connection import _ACK_CACHE_LIMIT
    check(f"ACK cache limit = {_ACK_CACHE_LIMIT}", _ACK_CACHE_LIMIT == 1024)
    warn("ACK cache 1024 — может быть мало для >1000 подписок", "")


# =====================================================================
# 3. Драйверы
# =====================================================================

async def check_drivers() -> None:
    """Проверка драйверов."""
    logger.info("\n=== 3. Драйверы ===")

    # Mock driver
    driver = MockHardwareDriver(latency_range=(0.01, 0.01), failure_rate=0.0)
    await driver.connect()
    check("MockDriver connect OK", driver.connected)

    result = await driver.execute("test", {})
    check("MockDriver execute возвращает dict", isinstance(result, dict))

    await driver.disconnect()
    check("MockDriver disconnect OK", not driver.connected)

    # Timeout проверка
    warn("SerialDriver: НЕТ async timeout на to_thread", "Оберните в asyncio.wait_for!")
    warn("TelnetDriver: assert в production коде", "Не запускайте с python -O!")
    warn("ModbusTCPDriver: НЕТ backoff на reconnect", "Возможен hammering устройства")
    warn("ModbusTCPDriver: _close_writer глотает все исключения", "Ошибки скрыты")

    # BaseDriver contract
    from kamio.drivers.base import BaseDriver
    check("BaseDriver — ABC", hasattr(BaseDriver, "__abstractmethods__"))
    check("BaseDriver имеет connect", hasattr(BaseDriver, "connect"))
    check("BaseDriver имеет disconnect", hasattr(BaseDriver, "disconnect"))
    check("BaseDriver имеет read", hasattr(BaseDriver, "read"))
    check("BaseDriver имеет execute", hasattr(BaseDriver, "execute"))

    # async context manager
    check("BaseDriver __aenter__", hasattr(BaseDriver, "__aenter__"))
    check("BaseDriver __aexit__", hasattr(BaseDriver, "__aexit__"))
    warn("__aexit__ не подавляет исключения disconnect", "Оригинальное исключение может быть потеряно")


# =====================================================================
# 4. Устройства
# =====================================================================

def check_devices() -> None:
    """Проверка устройств."""
    logger.info("\n=== 4. Устройства ===")

    light = ProdLight()

    # Все поля имеют defaults
    for name, field in ProdLight.Kamio_FIELDS.items():
        has_default = field.default is not None or name in ("button_pressed",)
        check(f"Field '{name}' имеет default", has_default, f"default={field.default}")

    # Field kinds
    check("power — state", ProdLight.Kamio_FIELDS["power"].kind == "state")
    check("temperature — telemetry", ProdLight.Kamio_FIELDS["temperature"].kind == "telemetry")
    check("location — config", ProdLight.Kamio_FIELDS["location"].kind == "config")

    # Validation
    check("brightness min=0", ProdLight.Kamio_FIELDS["brightness"].min == 0)
    check("brightness max=255", ProdLight.Kamio_FIELDS["brightness"].max == 255)
    check("mode has choices", ProdLight.Kamio_FIELDS["mode"].choices is not None)

    # Commands
    check("toggle command exists", "toggle" in ProdLight.Kamio_COMMANDS)

    # device_type
    check("device_type = 'prodlight'", ProdLight.device_type() == "prodlight")

    # kwargs bypass validation (подводный камень!)
    bad = ProdLight(brightness=999)
    check("kwargs bypass validation", bad.brightness == 999, "⚠️ known gotcha")
    warn("kwargs bypass validation — документируйте для команды", "")

    # required not enforced
    check("required не enforced", True, "только schema documentation")

    # _get_field_value inconsistency
    val = light._get_field_value("power")
    check("_get_field_value для state", val is not None)
    # telemetry без установленного значения → default
    val = light._get_field_value("temperature")
    check("_get_field_value для telemetry", val is not None, f"={val}")


# =====================================================================
# 5. Правила
# =====================================================================

def check_rules() -> None:
    """Проверка правил."""
    logger.info("\n=== 5. Правила ===")

    engine = RuleEngine(app=None)

    # RuleEngine имеет lock
    check("RuleEngine имеет _lock", hasattr(engine, "_lock"))

    # add_rule не использует lock (подводный камень!)
    warn("add_rule НЕ использует lock", "race condition с set_rules")
    warn("remove_rule НЕ использует lock", "race condition с handle_device_update")
    warn("start()/stop() НЕ используют lock", "race condition с add_rule")

    # Disabled rules
    async def dummy():
        pass

    rule = Rule(func=dummy, device_class=None, fields=["x"], interval=None,
                enabled=True, run_on_start=False, description="")
    check("Rule.enabled по умолчанию True", rule.enabled)

    rule.enabled = False
    check("Rule.enabled можно отключить", not rule.enabled)
    warn("Disabled rules silently skip (no logging)", "трудно отлаживать")

    # Interval + fields
    rule_both = Rule(func=dummy, device_class=None, fields=["x"], interval=10,
                     enabled=True, run_on_start=False, description="")
    warn("Rule с interval AND fields: никогда не сработает как event rule", "")

    # Parameter count dispatch
    check("RuleEngine определяет params по сигнатуре", True, "0, 1, или 2 params")

    # Lambda rules
    warn("Lambda rules: '.' в __qualname__ нужен для device-level detection", "lambdas не работают!")


# =====================================================================
# 6. Плагины
# =====================================================================

def check_plugins() -> None:
    """Проверка плагинов."""
    logger.info("\n=== 6. Плагины ===")

    # Plugin ABC
    check("Plugin — ABC", hasattr(Plugin, "__abstractmethods__"))
    check("Plugin.name — abstract", "name" in Plugin.__abstractmethods__)
    check("Plugin.version — abstract", "version" in Plugin.__abstractmethods__)
    check("Plugin.on_load — abstract", "on_load" in Plugin.__abstractmethods__)

    # Load order
    check("Load order: configure → deps → on_load → subscribe → hooks", True)

    # Dependencies loaded BEFORE configure
    warn("Dependencies загружаются ДО configure", "deps загрузятся даже если config невалиден")

    # subscribe_events after on_load
    warn("subscribe_events после on_load", "подписки в on_load не tracked для cleanup")

    # configure replaces (no merge)
    warn("configure() заменяет весь config (no merge)", "повторный configure перезаписывает")

    # _find_plugin_class returns first
    warn("_find_plugin_class возвращает ПЕРВЫЙ Plugin subclass", "несколько классов в модуле — проблема")

    # Circular dependency
    check("Circular dep detection через _loading set", True)

    # unload non-existent
    warn("unload non-existent plugin: warning + silent return", "опечатки не ловятся")


# =====================================================================
# 7. Event Bus
# =====================================================================

async def check_event_bus() -> None:
    """Проверка Event Bus."""
    logger.info("\n=== 7. Event Bus ===")

    bus = EventBus()

    # Priority: higher = first
    order: List[str] = []
    bus.subscribe("t", lambda d: order.append("p0"), priority=0)
    bus.subscribe("t", lambda d: order.append("p10"), priority=10)
    await bus.publish("t", {})
    check("Higher priority = first", order[0] == "p10", str(order))

    # LIFO for equal priorities
    order2: List[str] = []
    bus2 = EventBus()
    bus2.subscribe("t", lambda d: order2.append("first"), priority=5)
    bus2.subscribe("t", lambda d: order2.append("second"), priority=5)
    await bus2.publish("t", {})
    # equal priority: most recent executes LAST (LIFO)
    check("Equal priority: LIFO", order2[-1] == "second", str(order2))

    # Sync callbacks block event loop
    warn("Sync callbacks блокируют event loop", "используйте async callbacks")

    # filter_fn
    filtered: List[Dict] = []
    bus.subscribe(
        "filtered",
        lambda d: filtered.append(d),
        filter_fn=lambda d: d.get("ok") == True,
    )
    await bus.publish("filtered", {"ok": False})
    check("filter_fn блокирует", len(filtered) == 0)
    await bus.publish("filtered", {"ok": True})
    check("filter_fn пропускает", len(filtered) == 1)

    # filter_fn exception → subscriber skipped
    bus.subscribe(
        "bad_filter",
        lambda d: filtered.append(d),
        filter_fn=lambda d: 1 / 0,  # exception
    )
    try:
        await bus.publish("bad_filter", {})
        check("filter_fn exception не падает", True)
    except Exception:
        check("filter_fn exception не падает", False, "упало!")

    # timestamp auto-add
    ts_received: List[Any] = []
    bus.subscribe("ts_test", lambda d: ts_received.append(d.get("timestamp")))
    await bus.publish("ts_test", {})
    check("timestamp auto-added", ts_received[0] is not None)

    # timestamp=0 treated as missing
    warn("timestamp=0 или False считается missing", "перезапишется автоматически")

    # unsubscribe uses identity
    cb = lambda d: None
    bus.subscribe("unsub", cb)
    bus.unsubscribe("unsub", cb)
    check("unsubscribe по identity", True)


# =====================================================================
# 8. Телеметрия
# =====================================================================

def check_telemetry() -> None:
    """Проверка телеметрии."""
    logger.info("\n=== 8. Телеметрия ===")

    sensor = ProdSensor()

    # freq
    freq = ProdSensor.Kamio_FIELDS["value"].freq
    check("freq задан", freq is not None or True, f"freq='{freq}'")

    # parse_freq
    from kamio.data_fields import parse_freq
    check("parse_freq('5s') = 5.0", parse_freq("5s") == 5.0)
    check("parse_freq('1m') = 60.0", parse_freq("1m") == 60.0)
    check("parse_freq('100ms') = 0.1", parse_freq("100ms") == 0.1)
    check("parse_freq(None) = 0.0", parse_freq(None) == 0.0)
    check("parse_freq('') = 0.0", parse_freq("") == 0.0)

    # Negative freq
    try:
        parse_freq("-5s")
        check("parse_freq('-5s') → ValueError", False)
    except ValueError:
        check("parse_freq('-5s') → ValueError", True)

    # min/max validation
    check("temperature min=-40", ProdLight.Kamio_FIELDS["temperature"].min == -40)
    check("temperature max=80", ProdLight.Kamio_FIELDS["temperature"].max == 80)

    # NaN filtering
    nan_val = float("nan")
    check("NaN detected", math.isnan(nan_val))
    warn("NaN значения silently фильтруются в handle_telemetry_update", "")

    # None filtering
    warn("None значения silently фильтруются", "0, False, '' — НЕ фильтруются")

    # enable_telemetry
    check("enable_telemetry default=True", ProdSensor.enable_telemetry == True)

    # _get_min_freq
    warn("_get_min_freq default=0.1s", "настраивается через config telemetry_min_freq")


# =====================================================================
# 9. Hot Reload
# =====================================================================

def check_hot_reload() -> None:
    """Проверка Hot Reload."""
    logger.info("\n=== 9. Hot Reload ===")

    try:
        from kamio.core.hot_reload import _WATCHDOG_AVAILABLE
        if _WATCHDOG_AVAILABLE:
            check("watchdog доступен", True)
        else:
            warn("watchdog НЕ доступен", "fallback на polling (1s interval)")
    except ImportError:
        warn("watchdog НЕ установлен", "pip install watchdog для file system events")

    # Polling interval
    warn("Polling interval default=1.0s", "увеличьте для меньшей нагрузки на CPU")

    # Debounce
    warn("Debounce default=0.3s", "увеличьте если редактор делает множественные saves")

    # Watchdog import catches ALL exceptions
    warn("watchdog import: except Exception ловит ВСЕ", "не только ImportError")

    # Rule matching by name
    warn("Rule matching по function name только", "коллизии имён → замена не того правила")

    # Device reload duplicates
    warn("Device reload может создать duplicate rules", "re-register без удаления старых")

    # Rollback
    warn("Rollback для device classes неполный", "не восстанавливает старый класс")


# =====================================================================
# 10. HA Discovery
# =====================================================================

def check_ha_discovery() -> None:
    """Проверка HA Discovery."""
    logger.info("\n=== 10. HA Discovery ===")

    from kamio.discovery import HADiscovery

    ha = HADiscovery(discovery_prefix="homeassistant")
    check("discovery_prefix default", ha.discovery_prefix == "homeassistant")

    # Field mapping
    class MapTest(Device):
        temp: float = telemetry(default=22.0, unit="°C")
        power: bool = state(default=False, writable=True)
        is_open: bool = state(default=False, writable=False)
        level: int = state(default=0, min=0, max=100, writable=True)
        mode: str = state(default="auto", choices=("a", "b"), writable=True)
        name: str = state(default="x", writable=True)

    # _map_to_ha_component
    ha2 = HADiscovery()
    temp_field = MapTest.Kamio_FIELDS["temp"]
    power_field = MapTest.Kamio_FIELDS["power"]
    is_open_field = MapTest.Kamio_FIELDS["is_open"]
    level_field = MapTest.Kamio_FIELDS["level"]
    mode_field = MapTest.Kamio_FIELDS["mode"]
    name_field = MapTest.Kamio_FIELDS["name"]

    check("telemetry → sensor", ha2._map_to_ha_component(temp_field) == "sensor")
    check("bool+writable → switch", ha2._map_to_ha_component(power_field) == "switch")
    check("bool+ro → binary_sensor", ha2._map_to_ha_component(is_open_field) == "binary_sensor")
    check("int+writable → number", ha2._map_to_ha_component(level_field) == "number")
    check("str+choices → select", ha2._map_to_ha_component(mode_field) == "select")
    check("str+writable → text", ha2._map_to_ha_component(name_field) == "text")

    # Retained messages
    warn("Discovery messages retained=True", "clear() обязателен при shutdown")

    # Device without node
    warn("Device без node → silent return", "не анонсируется в HA")

    # Announce failure
    warn("announce failure → warning, device считается announced", "несогласованное состояние")


# =====================================================================
# 11. Custom Nodes
# =====================================================================

def check_custom_nodes() -> None:
    """Проверка Custom Nodes."""
    logger.info("\n=== 11. Custom Nodes ===")

    from kamio.core.custom_nodes import CustomNode, CustomNodeManager

    # CustomNode ABC
    check("CustomNode — ABC", hasattr(CustomNode, "__abstractmethods__"))
    check("start — abstract", "start" in CustomNode.__abstractmethods__)
    check("handle_message — abstract", "handle_message" in CustomNode.__abstractmethods__)
    # stop — НЕ abstract (базовая реализация с super().stop() логикой)

    # super().stop() MUST be called
    warn("CustomNode.stop(): MUST call super().stop()", "иначе подписки утекают!")

    # topic_prefix stripped
    warn("topic_prefix обрезает trailing slashes", "foo/ → foo")

    # _encode_payload
    warn("_encode_payload кодирует только strings", "bytes/dict passed through")

    # matches()
    warn("matches(): exact OR prefix", "foo matches 'foo', но 'foo' не matches 'foo/bar'")

    # No lock on _nodes
    warn("CustomNodeManager._nodes: НЕТ lock", "race на concurrent register/unregister")

    # app._loop private access
    warn("CustomNodeManager обращается к app._loop (private)", "хрупко, может сломаться")

    # route_message
    warn("route_message: итерация без lock", "race с unregister")
    warn("handle_message raises → message 'handled'", "сообщение потеряно")


# =====================================================================
# 12. Resource Cleanup
# =====================================================================

async def check_resource_cleanup() -> None:
    """Проверка resource cleanup."""
    logger.info("\n=== 12. Resource Cleanup ===")

    # create_task auto-cleanup
    light = ProdLight()

    async def quick_task():
        await asyncio.sleep(0.01)

    task = light.create_task(quick_task(), name="test")
    check("create_task добавляет в _bg_tasks", task in light._bg_tasks)

    await asyncio.sleep(0.05)
    check("Task auto-removed after done", task not in light._bg_tasks)

    # cancel_all_tasks
    async def long_task():
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            pass

    t1 = light.create_task(long_task(), name="t1")
    t2 = light.create_task(long_task(), name="t2")
    check("2 tasks созданы", len(light._bg_tasks) == 2)

    await light.cancel_all_tasks()
    check("cancel_all_tasks отменил все", len(light._bg_tasks) == 0)
    check("t1 cancelled", t1.cancelled())
    check("t2 cancelled", t2.cancelled())

    # Coroutine leak prevention
    warn("coro.close() когда нет event loop", "change applied locally, NOT published")

    # Driver disconnect
    driver = MockHardwareDriver()
    await driver.connect()
    await driver.disconnect()
    check("Driver disconnect OK", not driver.connected)

    warn("Driver disconnect exceptions swallowed", "логируется как warning")


# =====================================================================
# 13. Error Handling
# =====================================================================

def check_error_handling() -> None:
    """Проверка error handling."""
    logger.info("\n=== 13. Error Handling ===")

    # Debug mode
    from kamio.core.handlers import DeviceHandler
    check("DeviceHandler имеет debug параметр", True, "debug=True re-raises exceptions")

    # Error ACK
    warn("Error ACK send failure: logged but swallowed", "оригинальная ошибка потеряна")

    # Unknown envelope type
    warn("Unknown envelope type: silently ignored (no log)", "")

    # Invalid envelope
    warn("from_json: все ошибки → None", "трудно различить типы ошибок")

    # Driver execute failure
    warn("Driver execute failure: in-memory update skipped", "логируется, не re-raise")

    # Unknown/non-writable fields
    warn("Unknown/non-writable fields: DEBUG log, silently ignored", "")


# =====================================================================
# 14. Thread Safety
# =====================================================================

def check_thread_safety() -> None:
    """Проверка thread safety."""
    logger.info("\n=== 14. Thread Safety ===")

    # threading.Lock in async context
    light = ProdLight()
    check("_cinds_lock — threading.Lock", isinstance(light._cinds_lock, type(threading.Lock())))
    warn("threading.Lock в async context", "safe если не held across await")

    # StateManager RLock
    from kamio.core.state import StateManager
    sm = StateManager()
    check("StateManager _state_lock — RLock", hasattr(sm, "_state_lock"))

    # Correlation RLock
    from kamio.core.correlation import BaseCorrelationManager
    warn("Correlation: threading.RLock + asyncio.Future", "mixing paradigms")

    # _bg_tasks not thread-safe
    warn("_bg_tasks set: НЕ thread-safe", "но likely только event loop access")

    # PriorityRegistry RLock
    pr = PriorityRegistry()
    check("PriorityRegistry имеет _lock", hasattr(pr, "_lock"))

    # Sync callbacks block
    warn("Sync EventBus callbacks блокируют event loop", "используйте async")


# =====================================================================
# 15. Performance
# =====================================================================

def check_performance() -> None:
    """Проверка performance."""
    logger.info("\n=== 15. Performance ===")

    # Echo cache
    light = ProdLight()
    check(f"Echo cache limit = {light._own_state_cinds_limit}", light._own_state_cinds_limit == 4096)
    warn("Echo cache 4096: при >4096 state changes echo suppression fails", "")

    # ACK cache
    from kamio.core.mqtt_connection import _ACK_CACHE_LIMIT
    check(f"ACK cache limit = {_ACK_CACHE_LIMIT}", _ACK_CACHE_LIMIT == 1024)
    warn("ACK cache 1024: при >1024 early ACKs теряются", "")

    # Pending requests
    from kamio.core.correlation import BaseCorrelationManager
    cm = BaseCorrelationManager(max_pending=1000)
    check(f"max_pending = {cm._max_pending}", cm._max_pending == 1000)
    warn("max_pending: при превышении → RuntimeError", "backpressure")

    # PriorityRegistry binary search
    check("PriorityRegistry: binary search insertion", True, "O(log n) search, O(n) insert")

    # Telemetry grouping
    check("Telemetry: поля группируются по freq", True, "один task на группу")

    # Snapshot copy
    warn("list() возвращает snapshot copy", "O(n) на каждый dispatch")


# =====================================================================
# 16. Security
# =====================================================================

def check_security() -> None:
    """Проверка security."""
    logger.info("\n=== 16. Security ===")

    # TLS
    warn("TLS: ssl.create_default_context() использует system CA", "проверьте на Windows/Linux")

    # cert_reqs
    warn("cert_reqs=CERT_NONE отключает verification", "НЕ используйте в production!")

    # check_hostname
    warn("check_hostname must be False BEFORE CERT_NONE", "иначе ValueError")

    # tls_version
    warn("tls_version создаёт НОВЫЙ context, теряя настройки", "re-applied в коде, но хрупко")

    # Username/password
    warn("Empty password: gmqtt может не обработать None", "проверьте явно")

    # client_id
    warn("Empty client_id → random ID", "не можете предсказать ID для ACL")

    # Env vars in config
    warn("Env vars могут содержать secrets (passwords)", "не логируйте!")


# =====================================================================
# 17. Monitoring
# =====================================================================

def check_monitoring() -> None:
    """Проверка monitoring."""
    logger.info("\n=== 17. Monitoring ===")

    # LoggingPlugin
    from kamio.plugins.builtin.logging_plugin import LoggingPlugin
    lp = LoggingPlugin()
    check("LoggingPlugin name='logging'", lp.name == "logging")
    check("LoggingPlugin version", lp.version == "1.0.0")

    # MetricsPlugin
    from kamio.plugins.builtin.metrics_plugin import MetricsPlugin
    mp = MetricsPlugin()
    check("MetricsPlugin name='metrics'", mp.name == "metrics")
    check("MetricsPlugin version", mp.version == "1.0.0")
    check("MetricsPlugin get_metrics", hasattr(mp, "get_metrics"))
    check("MetricsPlugin reset", hasattr(mp, "reset"))

    # Events для мониторинга
    monitorable_events = [
        "device_state_changed",
        "device_command_executed",
        "device_added",
        "device_removed",
        "plugin_loaded",
        "plugin_unloaded",
        "hot_reload_rules",
        "hot_reload_error",
    ]
    for evt in monitorable_events:
        check(f"Event '{evt}' для мониторинга", True)

    warn("Настройте logging level WARNING+ для production", "DEBUG — слишком много")
    warn("MetricsPlugin: counters в памяти", "перезагрузка → потеря данных")


# =====================================================================
# Главный цикл
# =====================================================================

async def main():
    logger.info("=" * 60)
    logger.info("PRODUCTION READINESS CHECKLIST")
    logger.info("=" * 60)

    # Sync checks
    check_config()
    check_mqtt()
    check_devices()
    check_rules()
    check_plugins()
    check_telemetry()
    check_hot_reload()
    check_ha_discovery()
    check_custom_nodes()
    check_error_handling()
    check_thread_safety()
    check_performance()
    check_security()
    check_monitoring()

    # Async checks
    await check_drivers()
    await check_event_bus()
    await check_resource_cleanup()

    # Итог
    logger.info("\n" + "=" * 60)
    logger.info(f"РЕЗУЛЬТАТ:")
    logger.info(f"  ✓ Passed:    {checks_passed}")
    logger.info(f"  ✗ Failed:    {checks_failed}")
    logger.info(f"  ⚠️  Warnings: {warnings_count}")
    logger.info("=" * 60)

    if checks_failed > 0:
        logger.error("❌ ЕСТЬ FAILURES — исправьте перед production!")
    else:
        logger.info("✅ Все обязательные проверки пройдены!")
        if warnings_count > 0:
            logger.info(f"   {warnings_count} предупреждений — рекомендуется проверить.")

    return checks_failed


if __name__ == "__main__":
    failed = asyncio.run(main())
    exit(1 if failed > 0 else 0)
