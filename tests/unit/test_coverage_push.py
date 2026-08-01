"""Targeted tests to push coverage from 88% to 90%+.

Covers gaps in:
- gpio.py (gpiod mock)
- loader.py (plugin loader edge cases)
- lifecycle.py (start/stop/run)
- device.py (lifecycle methods)
- serial.py (connect/disconnect with mock pyserial)
- mqtt_connection.py (connect/disconnect/ack waiting)
- http.py (connect without aiohttp)
"""

from __future__ import annotations

import asyncio
import sys
import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kamio import Device, KamioApp, command, state, telemetry
from kamio.data_fields import Field


# ---------------------------------------------------------------------------
# GPIO — mock gpiod module
# ---------------------------------------------------------------------------
@pytest.fixture
def fake_gpiod():
    """Inject a fake gpiod module so GPIOChipDriver can be tested on Windows."""
    fake = types.ModuleType("gpiod")
    fake.LINE_REQ_DIR_IN = 1
    fake.LINE_REQ_DIR_OUT = 2

    class FakeChip:
        def __init__(self, path):
            self.path = path

        def get_line(self, pin):
            return FakeLine(pin)

        def close(self):
            pass

    class FakeLine:
        def __init__(self, pin):
            self.pin = pin
            self._value = 0

        def request(self, consumer="Kamio", type=1):
            self._req_type = type

        def get_value(self):
            return self._value

        def set_value(self, val):
            self._value = val

        def release(self):
            pass

    fake.Chip = FakeChip
    fake.FakeLine = FakeLine

    old = sys.modules.get("gpiod")
    sys.modules["gpiod"] = fake
    from kamio.drivers import gpio as gpio_mod

    gpio_mod.gpiod = fake
    yield fake
    if old is not None:
        sys.modules["gpiod"] = old
    else:
        del sys.modules["gpiod"]
    gpio_mod.gpiod = None if old is None else old


@pytest.mark.asyncio
async def test_gpio_connect_and_disconnect(fake_gpiod):
    from kamio.drivers.gpio import GPIOChipDriver

    drv = GPIOChipDriver(chip_path="/dev/gpiochip0")
    await drv.connect()
    assert drv.chip is not None
    await drv.disconnect()
    assert drv.chip is None


@pytest.mark.asyncio
async def test_gpio_connect_failure(fake_gpiod):
    from kamio.drivers.gpio import GPIOChipDriver

    fake_gpiod.Chip = MagicMock(side_effect=OSError("no such chip"))
    drv = GPIOChipDriver(chip_path="/dev/bad")
    with pytest.raises(OSError):
        await drv.connect()


@pytest.mark.asyncio
async def test_gpio_read(fake_gpiod):
    from kamio.drivers.gpio import GPIOChipDriver

    drv = GPIOChipDriver(chip_path="/dev/gpiochip0")
    await drv.connect()
    result = await drv.read("pin0", {"pin": 5})
    assert result["status"] == "ok"
    assert result["pin"] == 5
    await drv.disconnect()


@pytest.mark.asyncio
async def test_gpio_read_requires_pin(fake_gpiod):
    from kamio.drivers.gpio import GPIOChipDriver

    drv = GPIOChipDriver(chip_path="/dev/gpiochip0")
    await drv.connect()
    with pytest.raises(ValueError):
        await drv.read("pin0", {})
    await drv.disconnect()


@pytest.mark.asyncio
async def test_gpio_read_not_connected(fake_gpiod):
    from kamio.drivers.gpio import GPIOChipDriver

    drv = GPIOChipDriver(chip_path="/dev/gpiochip0")
    with pytest.raises(RuntimeError):
        await drv.read("pin0", {"pin": 5})


@pytest.mark.asyncio
async def test_gpio_execute_set_output(fake_gpiod):
    from kamio.drivers.gpio import GPIOChipDriver

    drv = GPIOChipDriver(chip_path="/dev/gpiochip0")
    await drv.connect()
    result = await drv.execute("set_output", {"pin": 3, "value": True})
    assert result["status"] == "ok"
    assert result["value"] is True
    await drv.disconnect()


@pytest.mark.asyncio
async def test_gpio_execute_not_connected(fake_gpiod):
    from kamio.drivers.gpio import GPIOChipDriver

    drv = GPIOChipDriver(chip_path="/dev/gpiochip0")
    with pytest.raises(RuntimeError):
        await drv.execute("set_output", {"pin": 3, "value": True})


@pytest.mark.asyncio
async def test_gpio_execute_requires_pin(fake_gpiod):
    from kamio.drivers.gpio import GPIOChipDriver

    drv = GPIOChipDriver(chip_path="/dev/gpiochip0")
    await drv.connect()
    with pytest.raises(ValueError):
        await drv.execute("set_output", {"value": True})
    await drv.disconnect()


@pytest.mark.asyncio
async def test_gpio_execute_unknown_command(fake_gpiod):
    from kamio.drivers.gpio import GPIOChipDriver

    drv = GPIOChipDriver(chip_path="/dev/gpiochip0")
    await drv.connect()
    with pytest.raises(NotImplementedError):
        await drv.execute("bogus", {"pin": 3, "value": 1})
    await drv.disconnect()


@pytest.mark.asyncio
async def test_gpio_direction_change_releases_line(fake_gpiod):
    from kamio.drivers.gpio import GPIOChipDriver

    drv = GPIOChipDriver(chip_path="/dev/gpiochip0")
    await drv.connect()
    await drv._get_line(7, direction="input")
    assert 7 in drv.lines
    await drv._get_line(7, direction="output")
    assert 7 in drv.lines
    await drv.disconnect()


@pytest.mark.asyncio
async def test_gpio_disconnect_releases_lines(fake_gpiod):
    from kamio.drivers.gpio import GPIOChipDriver

    drv = GPIOChipDriver(chip_path="/dev/gpiochip0")
    await drv.connect()
    await drv._get_line(1, direction="input")
    await drv._get_line(2, direction="output")
    assert len(drv.lines) == 2
    await drv.disconnect()
    assert len(drv.lines) == 0


@pytest.mark.asyncio
async def test_gpio_disconnect_release_error_logged(fake_gpiod):
    from kamio.drivers.gpio import GPIOChipDriver

    drv = GPIOChipDriver(chip_path="/dev/gpiochip0")
    await drv.connect()
    line = await drv._get_line(1, direction="input")
    line.release = MagicMock(side_effect=OSError("release failed"))
    await drv.disconnect()  # should not raise


# ---------------------------------------------------------------------------
# Serial — connect/disconnect with mock pyserial
# ---------------------------------------------------------------------------
@pytest.fixture
def fake_serial():
    fake = types.ModuleType("serial")

    class FakeSerial:
        def __init__(self, port, baudrate, timeout):
            self.port = port
            self.baudrate = baudrate
            self.timeout = timeout
            self._open = True

        def write(self, data):
            pass

        def read(self, size):
            return b"ok\n"

        def close(self):
            self._open = False

    fake.Serial = FakeSerial

    old = sys.modules.get("serial")
    sys.modules["serial"] = fake
    from kamio.drivers import serial as serial_mod

    serial_mod.serial = fake
    yield fake
    if old is not None:
        sys.modules["serial"] = old
    else:
        del sys.modules["serial"]
    serial_mod.serial = old


@pytest.mark.asyncio
async def test_serial_connect_and_disconnect(fake_serial):
    from kamio.drivers.serial import SerialDriver

    drv = SerialDriver(port="/dev/ttyUSB0", baudrate=9600)
    await drv.connect()
    assert drv.ser is not None
    await drv.disconnect()
    assert drv.ser is None


@pytest.mark.asyncio
async def test_serial_connect_failure(fake_serial):
    from kamio.drivers.serial import SerialDriver

    fake_serial.Serial = MagicMock(side_effect=OSError("port not found"))
    drv = SerialDriver(port="/dev/bad")
    with pytest.raises(OSError):
        await drv.connect()


@pytest.mark.asyncio
async def test_serial_read(fake_serial):
    from kamio.drivers.serial import SerialDriver

    drv = SerialDriver(port="/dev/ttyUSB0")
    await drv.connect()
    result = await drv.read("temp", {"command": "GET TEMP"})
    assert result["status"] == "ok"
    assert result["field"] == "temp"
    await drv.disconnect()


@pytest.mark.asyncio
async def test_serial_read_not_connected(fake_serial):
    from kamio.drivers.serial import SerialDriver

    drv = SerialDriver(port="/dev/ttyUSB0")
    with pytest.raises(RuntimeError):
        await drv.read("temp", {})


@pytest.mark.asyncio
async def test_serial_execute(fake_serial):
    from kamio.drivers.serial import SerialDriver

    drv = SerialDriver(port="/dev/ttyUSB0")
    await drv.connect()
    result = await drv.execute("send", {"command": "GET", "value": "TEMP"})
    assert result["status"] == "ok"
    assert result["command"] == "send"
    await drv.disconnect()


@pytest.mark.asyncio
async def test_serial_execute_not_connected(fake_serial):
    from kamio.drivers.serial import SerialDriver

    drv = SerialDriver(port="/dev/ttyUSB0")
    with pytest.raises(RuntimeError):
        await drv.execute("send", {"command": "GET"})


@pytest.mark.asyncio
async def test_serial_execute_no_response(fake_serial):
    from kamio.drivers.serial import SerialDriver

    drv = SerialDriver(port="/dev/ttyUSB0")
    await drv.connect()
    result = await drv.execute("send", {"command": "PING", "wait_response": False})
    assert result["response"] == ""
    await drv.disconnect()


# ---------------------------------------------------------------------------
# HTTP — connect without aiohttp
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_http_connect_raises_without_aiohttp():
    from kamio.drivers import http as http_mod

    old = http_mod.aiohttp
    http_mod.aiohttp = None
    try:
        drv = http_mod.HTTPDeviceDriver(base_url="http://example.com")
        with pytest.raises(ImportError):
            await drv.connect()
    finally:
        http_mod.aiohttp = old


# ---------------------------------------------------------------------------
# Lifecycle — start/stop/run
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_lifecycle_start_already_running(mock_mqtt):
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-lc")
    app._is_running = True
    await app.start()  # should return immediately


@pytest.mark.asyncio
async def test_lifecycle_stop_not_running(mock_mqtt):
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-lc2")
    await app.stop()


@pytest.mark.asyncio
async def test_lifecycle_start_and_stop(mock_mqtt):
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-lc3")
    await app.start()
    assert app.is_running is True
    await app.stop()
    assert app.is_running is False


@pytest.mark.asyncio
async def test_lifecycle_stop_continues_after_step_error(mock_mqtt):
    """stop() should not skip remaining cleanup if one step raises."""
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-lc-err")
    await app.start()

    # Make rules.stop() raise to verify that custom_nodes.stop_all() and
    # subsequent steps still run.
    original_rules_stop = app.rules.stop
    step_ran_after_error = []

    async def _failing_rules_stop():
        raise RuntimeError("rules stop failed")

    async def _tracking_custom_nodes_stop():
        step_ran_after_error.append(True)

    app.rules.stop = _failing_rules_stop
    app.custom_nodes.stop_all = _tracking_custom_nodes_stop

    await app.stop()
    # custom_nodes.stop_all should have been called despite rules.stop error.
    assert step_ran_after_error == [True]
    assert app.is_running is False


@pytest.mark.asyncio
async def test_lifecycle_stop_handles_post_stop_hook_error(mock_mqtt):
    """stop() should not raise if on_after_stop hook or app_stop event raises."""
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-lc-hook-err")
    await app.start()

    # Make event_bus.publish raise to cover the post-stop except branch.
    original_publish = app.event_bus.publish

    async def _failing_publish(event_type, data):
        if event_type == "app_stop":
            raise RuntimeError("publish failed")
        await original_publish(event_type, data)

    app.event_bus.publish = _failing_publish

    # Should not raise despite the publish error.
    await app.stop()
    assert app.is_running is False


@pytest.mark.asyncio
async def test_lifecycle_await_bg_tasks_timeout(mock_mqtt):
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-lc4")

    async def _forever():
        await asyncio.sleep(100)

    app._bg_tasks = set()
    task = asyncio.create_task(_forever())
    app._bg_tasks.add(task)
    await app._await_bg_tasks(timeout=0.1)
    assert task.cancelled() or task.done()


@pytest.mark.asyncio
async def test_lifecycle_await_bg_tasks_no_tasks(mock_mqtt):
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-lc5")
    await app._await_bg_tasks(timeout=1.0)


def test_lifecycle_run_catches_keyboard_interrupt(mock_mqtt):
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-lc6")
    with patch("asyncio.run", side_effect=KeyboardInterrupt()):
        app.run()


def test_lifecycle_run_catches_exception(mock_mqtt):
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-lc7")
    with patch("asyncio.run", side_effect=RuntimeError("crash")):
        app.run()


# ---------------------------------------------------------------------------
# Device — lifecycle methods
# ---------------------------------------------------------------------------
class LifeDevice(Device):
    power: bool = state(default=False, writable=True)
    temp: float = telemetry(default=0.0, freq="")

    @command
    async def toggle(self):
        self.power = not self.power
        return {"power": self.power}


@pytest.mark.asyncio
async def test_device_on_init_and_shutdown():
    d = LifeDevice()
    await d.on_init()
    await d.shutdown()
    await d.shutdown()  # idempotent


@pytest.mark.asyncio
async def test_device_reinitialize(mock_mqtt):
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-dev")
    app.register(LifeDevice)
    dev = await app.add_device("dev1", LifeDevice)
    await app.start()
    await dev.reinitialize()
    await app.stop()


def test_device_app_setter_warning(mock_mqtt):
    app1 = KamioApp(mqtt_broker=mock_mqtt, client_id="a1")
    app2 = KamioApp(mqtt_broker=mock_mqtt, client_id="a2")
    d = LifeDevice()
    d.app = app1
    d.app = app2  # should warn but not raise


@pytest.mark.asyncio
async def test_device_handle_command_not_found():
    d = LifeDevice()
    with pytest.raises(AttributeError):
        await d.handle_command("nonexistent", {})


@pytest.mark.asyncio
async def test_device_handle_state_applied():
    d = LifeDevice()
    result = await d.handle_state({"power": True})
    assert result == {"power": True}
    assert d.power is True


@pytest.mark.asyncio
async def test_device_handle_state_unknown_field_ignored():
    d = LifeDevice()
    result = await d.handle_state({"unknown_field": 123})
    assert result == {}


@pytest.mark.asyncio
async def test_device_handle_state_non_writable_ignored():
    d = LifeDevice()
    result = await d.handle_state({"temp": 99.0})
    assert result == {}


@pytest.mark.asyncio
async def test_device_handle_config():
    d = LifeDevice()
    result = await d.handle_config({})
    assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_device_handle_event():
    d = LifeDevice()
    await d.handle_event("motion", {"zone": 1})


@pytest.mark.asyncio
async def test_device_get_schema():
    schema = LifeDevice.get_schema()
    assert isinstance(schema, dict)


@pytest.mark.asyncio
async def test_device_handle_command_with_driver():
    d = LifeDevice()
    d.driver = MagicMock()
    d.driver.execute = AsyncMock(return_value={"status": "ok"})
    result = await d.handle_command("toggle", {})
    assert result == {"status": "ok"}


@pytest.mark.asyncio
async def test_device_handle_command_driver_not_implemented_fallback():
    d = LifeDevice()
    d.driver = MagicMock()
    d.driver.execute = AsyncMock(side_effect=NotImplementedError("nope"))
    result = await d.handle_command("toggle", {})
    assert result == {"power": True}


# ---------------------------------------------------------------------------
# MqttConnection — connect/disconnect/acks
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_mqtt_connection_connect(mock_mqtt):
    from kamio.core.mqtt_connection import MqttConnection

    conn = MqttConnection(broker_uri="mqtt://localhost:1883", client_id="test-conn")
    conn.client.connect = AsyncMock()
    await conn.connect()
    conn.client.connect.assert_awaited_once()


@pytest.mark.asyncio
async def test_mqtt_connection_disconnect(mock_mqtt):
    from kamio.core.mqtt_connection import MqttConnection

    conn = MqttConnection(broker_uri="mqtt://localhost:1883", client_id="test-conn2")
    conn.client.disconnect = AsyncMock()
    await conn.disconnect()
    conn.client.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_mqtt_connection_connect_raises(mock_mqtt):
    from kamio.core.mqtt_connection import MqttConnection

    conn = MqttConnection(broker_uri="mqtt://localhost:1883", client_id="test-conn3")
    conn.client.connect = AsyncMock(side_effect=ConnectionRefusedError("refused"))
    with pytest.raises(ConnectionRefusedError):
        await conn.connect()


def test_mqtt_connection_with_username():
    from kamio.core.mqtt_connection import MqttConnection

    conn = MqttConnection(broker_uri="mqtt://user:pass@localhost:1883", client_id="auth")
    assert conn.host == "localhost"
    assert conn.port == 1883


def test_mqtt_connection_default_port():
    from kamio.core.mqtt_connection import MqttConnection

    conn = MqttConnection(broker_uri="mqtt://localhost", client_id="def-port")
    assert conn.port == 1883


@pytest.mark.asyncio
async def test_mqtt_connection_wait_for_suback_early():
    from kamio.core.mqtt_connection import MqttConnection

    conn = MqttConnection(broker_uri="mqtt://localhost:1883", client_id="test-ack")
    # Simulate early SUBACK
    conn._subed_mids[42] = None
    await conn._wait_for_suback(42, timeout=0.5)  # should return immediately
    assert 42 not in conn._subed_mids


@pytest.mark.asyncio
async def test_mqtt_connection_wait_for_suback_timeout():
    from kamio.core.mqtt_connection import MqttConnection

    conn = MqttConnection(broker_uri="mqtt://localhost:1883", client_id="test-ack2")
    with pytest.raises(TimeoutError):
        await conn._wait_for_suback(99, timeout=0.1)


@pytest.mark.asyncio
async def test_mqtt_connection_wait_for_suback_resolved():
    from kamio.core.mqtt_connection import MqttConnection

    conn = MqttConnection(broker_uri="mqtt://localhost:1883", client_id="test-ack3")

    async def _resolve():
        await asyncio.sleep(0.05)
        conn._on_subscribe(conn.client, 7, [])

    asyncio.create_task(_resolve())
    await conn._wait_for_suback(7, timeout=1.0)


@pytest.mark.asyncio
async def test_mqtt_connection_wait_for_unsuback_early():
    from kamio.core.mqtt_connection import MqttConnection

    conn = MqttConnection(broker_uri="mqtt://localhost:1883", client_id="test-uack")
    conn._unsubed_mids[10] = None
    await conn._wait_for_unsuback(10, timeout=0.5)
    assert 10 not in conn._unsubed_mids


@pytest.mark.asyncio
async def test_mqtt_connection_wait_for_unsuback_timeout():
    from kamio.core.mqtt_connection import MqttConnection

    conn = MqttConnection(broker_uri="mqtt://localhost:1883", client_id="test-uack2")
    with pytest.raises(TimeoutError):
        await conn._wait_for_unsuback(88, timeout=0.1)


def test_mqtt_connection_ack_cache_limit():
    from kamio.core.mqtt_connection import _ACK_CACHE_LIMIT, MqttConnection

    conn = MqttConnection(broker_uri="mqtt://localhost:1883", client_id="test-cache")
    # Add more than the limit to trigger eviction of the oldest entries.
    for i in range(_ACK_CACHE_LIMIT + 5):
        conn._resolve_ack(i, conn._sub_acks, conn._subed_mids)
    # The cache should be bounded — at most _ACK_CACHE_LIMIT entries, and
    # the oldest 5 should have been evicted (only the newest remain).
    assert len(conn._subed_mids) <= _ACK_CACHE_LIMIT
    # Oldest entries (0..4) should be gone; newest should still be present.
    assert 0 not in conn._subed_mids
    assert _ACK_CACHE_LIMIT + 4 in conn._subed_mids


# ---------------------------------------------------------------------------
# MqttConnection — _build_ssl_context
# ---------------------------------------------------------------------------
def test_ssl_context_default_is_secure():
    """create_default_context() path must require certs and check hostname."""
    import ssl

    from kamio.core.mqtt_connection import MqttConnection

    ctx = MqttConnection._build_ssl_context({})
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is True


def test_ssl_context_explicit_check_hostname_false():
    import ssl

    from kamio.core.mqtt_connection import MqttConnection

    ctx = MqttConnection._build_ssl_context({"check_hostname": False})
    assert ctx.check_hostname is False


def test_ssl_context_cert_reqs_string():
    import ssl

    from kamio.core.mqtt_connection import MqttConnection

    ctx = MqttConnection._build_ssl_context({"cert_reqs": "OPTIONAL"})
    assert ctx.verify_mode == ssl.CERT_OPTIONAL


def test_ssl_context_tls_version_preserves_verification():
    """Regression: specifying tls_version must NOT silently disable verification.

    Previously a fresh SSLContext was created losing verify_mode/check_hostname
    from create_default_context().  Now they must be re-applied.
    """
    import ssl

    from kamio.core.mqtt_connection import MqttConnection

    ctx = MqttConnection._build_ssl_context({"tls_version": ssl.PROTOCOL_TLS_CLIENT})
    # The secure defaults must survive the new-SSLContext path.
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is True


def test_ssl_context_tls_version_with_explicit_options():
    import ssl

    from kamio.core.mqtt_connection import MqttConnection

    ctx = MqttConnection._build_ssl_context(
        {"tls_version": ssl.PROTOCOL_TLS_CLIENT, "cert_reqs": "NONE", "check_hostname": False}
    )
    assert ctx.verify_mode == ssl.CERT_NONE
    assert ctx.check_hostname is False


def test_ssl_context_tls_version_constant():
    import ssl

    from kamio.core.mqtt_connection import MqttConnection

    ctx = MqttConnection._build_ssl_context({"tls_version": ssl.PROTOCOL_TLS_CLIENT})
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is True


# ---------------------------------------------------------------------------
# Plugin loader — edge cases
# ---------------------------------------------------------------------------
class _PBase:
    """Helper mixin to satisfy Plugin abstract interface."""

    name = "base"
    version = "1.0.0"
    dependencies = []

    async def on_load(self, app, context=None):
        pass

    async def on_unload(self, app):
        pass

    def subscribe_events(self, context):
        pass

    def register_hooks(self, context):
        pass


def _make_plugin_class(name, deps=None):
    """Dynamically create a concrete Plugin subclass for testing."""
    from kamio.plugins.base import Plugin

    class _P(Plugin):
        @property
        def name(self):
            return name

        @property
        def version(self):
            return "1.0.0"

        @property
        def dependencies(self):
            return deps or []

        async def on_load(self, app, context=None):
            pass

        async def on_unload(self, app):
            pass

        def subscribe_events(self, context):
            pass

        def register_hooks(self, context):
            pass

    return _P


@pytest.mark.asyncio
async def test_plugin_loader_load_not_a_plugin(mock_mqtt):
    from kamio.plugins.loader import PluginLoader

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-pl")
    loader = PluginLoader(app)
    with pytest.raises(TypeError):
        await loader.load_plugin(str)


@pytest.mark.asyncio
async def test_plugin_loader_load_already_loaded(mock_mqtt):
    from kamio.plugins.loader import PluginLoader

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-pl2")
    loader = PluginLoader(app)
    P = _make_plugin_class("test-already")
    await loader.load_plugin(P)
    with pytest.raises(ValueError, match="already loaded"):
        await loader.load_plugin(P)


@pytest.mark.asyncio
async def test_plugin_loader_unload_not_found(mock_mqtt):
    from kamio.plugins.loader import PluginLoader

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-pl3")
    loader = PluginLoader(app)
    # Should log a warning and return, not raise
    await loader.unload_plugin("nonexistent")


@pytest.mark.asyncio
async def test_plugin_loader_unload_with_dependents(mock_mqtt):
    from kamio.plugins.loader import PluginLoader

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-pl4")
    loader = PluginLoader(app)
    Base = _make_plugin_class("base-mod")
    Dep = _make_plugin_class("dep-mod", deps=["base-mod"])
    loader.register_class("base-mod", Base)
    await loader.load_plugin(Dep)
    with pytest.raises(ValueError, match="required by"):
        await loader.unload_plugin("base-mod")
    await loader.unload_plugin("dep-mod")
    await loader.unload_plugin("base-mod")


@pytest.mark.asyncio
async def test_plugin_loader_circular_dependency(mock_mqtt):
    from kamio.plugins.loader import PluginLoader

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-pl5")
    loader = PluginLoader(app)
    A = _make_plugin_class("circ-a", deps=["circ-b"])
    B = _make_plugin_class("circ-b", deps=["circ-a"])
    loader.register_class("circ-b", B)
    loader.register_class("circ-a", A)
    with pytest.raises(ValueError, match="Circular"):
        await loader.load_plugin(A)


@pytest.mark.asyncio
async def test_plugin_loader_missing_dependency(mock_mqtt):
    from kamio.plugins.loader import PluginLoader

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-pl6")
    loader = PluginLoader(app)
    P = _make_plugin_class("needs-missing", deps=["does-not-exist"])
    with pytest.raises(ValueError, match="could not be resolved"):
        await loader.load_plugin(P)


@pytest.mark.asyncio
async def test_plugin_loader_unload_all(mock_mqtt):
    from kamio.plugins.loader import PluginLoader

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-pl7")
    loader = PluginLoader(app)
    P1 = _make_plugin_class("p1")
    P2 = _make_plugin_class("p2", deps=["p1"])
    loader.register_class("p1", P1)
    await loader.load_plugin(P2)
    await loader.unload_all()
    assert len(loader._loaded) == 0


@pytest.mark.asyncio
async def test_plugin_loader_on_unload_error_still_cleans(mock_mqtt):
    from kamio.plugins.base import Plugin
    from kamio.plugins.loader import PluginLoader

    class BadUnload(Plugin):
        @property
        def name(self):
            return "bad-unload"

        @property
        def version(self):
            return "1.0.0"

        async def on_load(self, app, context=None):
            pass

        async def on_unload(self, app):
            raise RuntimeError("unload failed")

        def subscribe_events(self, context):
            pass

        def register_hooks(self, context):
            pass

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-pl8")
    loader = PluginLoader(app)
    await loader.load_plugin(BadUnload)
    with pytest.raises(RuntimeError):
        await loader.unload_plugin("bad-unload")
    assert "bad-unload" not in loader._loaded


@pytest.mark.asyncio
async def test_plugin_loader_on_load_failure_cleans_context_with_rule(mock_mqtt):
    """A plugin that registers a rule then raises in on_load must not leak it."""
    from kamio.plugins.base import Plugin
    from kamio.plugins.loader import PluginLoader

    class RuleThenFail(Plugin):
        @property
        def name(self):
            return "rule-then-fail"

        @property
        def version(self):
            return "1.0.0"

        async def on_load(self, app, context=None):
            # Register a rule via the context before failing so the cleanup
            # path that removes rules is exercised.
            async def _r(event, app):
                pass

            context.add_rule(_r, device=None)
            raise RuntimeError("fail after rule")

        async def on_unload(self, app):
            pass

        def subscribe_events(self, context):
            pass

        def register_hooks(self, context):
            pass

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-pl-rule-fail")
    loader = PluginLoader(app)
    with pytest.raises(RuntimeError):
        await loader.load_plugin(RuleThenFail)
    # Plugin must not be left in the loaded registry.
    assert "rule-then-fail" not in loader._loaded
    assert "rule-then-fail" not in loader._contexts


@pytest.mark.asyncio
async def test_plugin_loader_load_from_module_not_found(mock_mqtt):
    from kamio.plugins.loader import PluginLoader

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-pl9")
    loader = PluginLoader(app)
    with pytest.raises(ImportError):
        await loader.load_from_module("nonexistent.module.xyz")


@pytest.mark.asyncio
async def test_plugin_loader_load_plugins_from_bad_directory(mock_mqtt):
    from kamio.plugins.loader import PluginLoader

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-pl10")
    loader = PluginLoader(app)
    result = await loader.load_plugins_from_directory("/nonexistent/path")
    assert result == []


@pytest.mark.asyncio
async def test_plugin_loader_get_plugin_info(mock_mqtt):
    from kamio.plugins.loader import PluginLoader

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-pl11")
    loader = PluginLoader(app)
    P = _make_plugin_class("info-test")
    await loader.load_plugin(P)
    info = loader.get_plugin("info-test")
    assert info is not None
    assert info.name == "info-test"
    names = loader.list_plugins()
    assert "info-test" in names
    await loader.unload_all()
