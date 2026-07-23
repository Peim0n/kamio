from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import pytest
from kamio import Device, KamioApp, command, config, event, state, telemetry
from kamio.core.automation import EventBus, HooksManager
from kamio.core.rules import RuleEvent
from kamio.core.custom_nodes import CustomNode
from kamio.core.transport import Envelope, EnvelopeType
from kamio.plugins.builtin import LoggingPlugin, MetricsPlugin


# ---------------------------------------------------------------------------
# EventBus
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_event_bus_priority_invokes_high_priority_first():
    bus = EventBus()
    order = []

    async def low(data: dict):
        order.append("low")

    async def high(data: dict):
        order.append("high")

    bus.subscribe("test", low, priority=0)
    bus.subscribe("test", high, priority=10)
    await bus.publish("test", {"value": 1})

    assert order == ["high", "low"]


@pytest.mark.asyncio
async def test_event_bus_filter_fn_skips_mismatched_events():
    bus = EventBus()
    received = []

    def only_critical(data: dict) -> bool:
        return data.get("level") == "critical"

    bus.subscribe("alert", lambda d: received.append(d), filter_fn=only_critical)

    await bus.publish("alert", {"level": "info"})
    await bus.publish("alert", {"level": "critical"})
    await bus.publish("alert", {"level": "warning"})

    assert len(received) == 1
    assert received[0]["level"] == "critical"


@pytest.mark.asyncio
async def test_event_bus_unsubscribe_removes_callback():
    bus = EventBus()
    received = []

    def handler(data: dict):
        received.append(data)

    bus.subscribe("chan", handler)
    bus.unsubscribe("chan", handler)
    await bus.publish("chan", {"value": 1})

    assert received == []


@pytest.mark.asyncio
async def test_event_bus_clear_removes_subscribers():
    bus = EventBus()
    received = []

    bus.subscribe("chan", lambda d: received.append(d))
    bus.clear("chan")
    await bus.publish("chan", {"value": 1})

    assert received == []

    bus.subscribe("a", lambda d: received.append(d))
    bus.subscribe("b", lambda d: received.append(d))
    bus.clear()
    await bus.publish("a", {})
    await bus.publish("b", {})

    assert received == []


@pytest.mark.asyncio
async def test_event_bus_list_subscribers_and_event_types():
    bus = EventBus()

    def a(data: dict): ...

    def b(data: dict): ...

    bus.subscribe("chan", a, priority=1)
    bus.subscribe("chan", b, priority=2)

    subscribers = bus.list_subscribers("chan")
    assert len(subscribers) == 2
    # Higher priority comes first.
    assert subscribers[0] is b
    assert subscribers[1] is a

    assert "chan" in bus.event_types()


@pytest.mark.asyncio
async def test_event_bus_callback_error_does_not_stop_others():
    bus = EventBus()
    received = []

    async def boom(data: dict):
        raise RuntimeError("boom")

    async def ok(data: dict):
        received.append(data)

    bus.subscribe("chan", boom)
    bus.subscribe("chan", ok)
    await bus.publish("chan", {"value": 1})

    assert len(received) == 1


# ---------------------------------------------------------------------------
# HooksManager
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hooks_priority_invokes_high_priority_first():
    hooks = HooksManager()
    order = []

    async def low(*args):
        order.append("low")

    async def high(*args):
        order.append("high")

    hooks.register("x", low, priority=0)
    hooks.register("x", high, priority=10)
    await hooks.trigger("x")

    assert order == ["high", "low"]


@pytest.mark.asyncio
async def test_hooks_unregister_and_clear():
    hooks = HooksManager()
    called = []

    def h(*args, **kwargs):
        called.append(1)

    hooks.register("x", h)
    assert h in hooks.list_hooks("x")

    hooks.unregister("x", h)
    assert h not in hooks.list_hooks("x")
    await hooks.trigger("x")
    assert called == []

    hooks.register("x", h)
    hooks.clear("x")
    await hooks.trigger("x")
    assert called == []

    hooks.register("y", h)
    hooks.register("z", h)
    hooks.clear()
    assert hooks.list_hooks("y") == []
    assert hooks.list_hooks("z") == []


@pytest.mark.asyncio
async def test_hooks_error_does_not_stop_others():
    hooks = HooksManager()
    ok = []

    async def boom(*args):
        raise RuntimeError("boom")

    async def good(*args):
        ok.append(1)

    hooks.register("x", boom)
    hooks.register("x", good)
    await hooks.trigger("x")

    assert ok == [1]


# ---------------------------------------------------------------------------
# KamioApp hook / event aliases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_app_unregister_hook():
    app = KamioApp()
    called = []

    def h(*args):
        called.append(1)

    app.register_hook("on_before_start", h)
    app.unregister_hook("on_before_start", h)
    await app.start()
    await app.stop()

    assert called == []


@pytest.mark.asyncio
async def test_app_logger_property():
    app = KamioApp()
    assert isinstance(app.logger, logging.Logger)


# ---------------------------------------------------------------------------
# Device snapshots, telemetry helpers and reinitialization
# ---------------------------------------------------------------------------


class SnapshotDevice(Device):
    power: bool = state(default=False, writable=True)
    target: float = state(default=22.0, writable=False)
    temp: float = telemetry(default=21.0, unit="°C", freq="1m")
    host: str = config(default="localhost")
    alarm = event(description="Alarm")


@pytest.mark.asyncio
async def test_device_state_snapshot():
    d = SnapshotDevice()
    snapshot = d.get_state_snapshot()
    assert snapshot["power"] is False
    assert snapshot["target"] == 22.0


@pytest.mark.asyncio
async def test_device_config_snapshot():
    d = SnapshotDevice()
    snapshot = d.get_config_snapshot()
    assert snapshot["host"] == "localhost"


@pytest.mark.asyncio
async def test_device_telemetry_snapshot():
    d = SnapshotDevice()
    snapshot = d.get_telemetry_snapshot()
    assert snapshot["temp"] == 21.0


@pytest.mark.asyncio
async def test_device_full_snapshot_contains_all_fields():
    d = SnapshotDevice()
    full = d.get_full_snapshot()
    assert "power" in full
    assert "target" in full
    assert "temp" in full
    assert "host" in full


@pytest.mark.asyncio
async def test_device_reinitialize_runs_without_error():
    d = SnapshotDevice()
    await d.reinitialize()
    assert d.power is False
    assert d.temp == 21.0


@pytest.mark.asyncio
async def test_device_shutdown_runs_without_error():
    d = SnapshotDevice()
    await d.shutdown()
    assert d.power is False


@pytest.mark.asyncio
async def test_device_telemetry_helpers_callable():
    d = SnapshotDevice()
    await d.read_telemetry_value("temp")
    update = await d.handle_telemetry_update(["temp"])
    assert update is None or isinstance(update, dict)
    await d.publish_telemetry({"temp": 22.0})


# ---------------------------------------------------------------------------
# CustomNode extended API
# ---------------------------------------------------------------------------


class EchoNode(CustomNode):
    def __init__(self, mqtt_client, topic_prefix):
        super().__init__(mqtt_client, topic_prefix)
        self.received: list[tuple[str, bytes]] = []
        self.published: list[tuple[str, Any, int, bool]] = []

    async def start(self):
        self.subscribe("cmd/#")
        self.subscribe_absolute("global/ctrl")

    async def stop(self):
        pass

    async def handle_message(self, topic: str, payload: bytes):
        self.received.append((topic, payload))


@pytest.mark.asyncio
async def test_custom_node_absolute_subscribe_and_publish():
    from unittest.mock import MagicMock

    client = MagicMock()
    client.subscribe = MagicMock(return_value=(0, 1))
    client.publish = MagicMock(return_value=(0, 1))

    node = EchoNode(client, "node")
    await node.start()
    assert client.subscribe.call_count == 2

    node.publish("out", b"hello")
    assert client.publish.called

    node.publish_absolute("global/ctrl", b"ack")
    assert client.publish.call_count == 2


@pytest.mark.asyncio
async def test_custom_node_publish_async():
    from unittest.mock import MagicMock

    client = MagicMock()
    client.publish = MagicMock(return_value=(0, 1))

    node = EchoNode(client, "node")
    await node.publish_async("out", b"async")
    assert client.publish.called


# ---------------------------------------------------------------------------
# HADiscovery
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ha_discovery_is_lazy():
    app = KamioApp()
    assert app.ha_discovery is None

    app.enable_ha_discovery(prefix="homeassistant")
    assert app.ha_discovery is not None

    app.disable_ha_discovery()
    # Disabling does not delete the instance, only disables integration.
    assert app.ha_discovery is not None


# ---------------------------------------------------------------------------
# HotReload
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hot_reload_watch_directory_and_state():
    app = KamioApp()
    path = os.path.abspath("/tmp/rules")
    app.watch_directory(path, "*.py", lambda p: None)
    watched = app.hot_reload.list_watched()
    assert any(path in w for w in watched)

    assert app.hot_reload.is_enabled is False
    app.enable_hot_reload()
    assert app.hot_reload.is_enabled is True
    await app.disable_hot_reload()
    assert app.hot_reload.is_enabled is False


def test_hot_reload_handler_factories_exist():
    app = KamioApp()
    assert callable(app.hot_reload.make_rules_handler())
    assert callable(app.hot_reload.make_devices_handler())
    assert callable(app.hot_reload.make_config_handler())


# ---------------------------------------------------------------------------
# Builtin plugins
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_metrics_plugin_loads_and_exposes_metrics():
    app = KamioApp()
    await app.load_plugin(MetricsPlugin)
    metrics = app.get_plugin("metrics")
    assert metrics is not None
    assert callable(getattr(metrics, "get_metrics", None))

    before = metrics.get_metrics()
    await app.publish_event("test_metric_event", {"x": 1})
    after = metrics.get_metrics()

    # The plugin is documented as an in-memory event counter.
    assert isinstance(after, dict)


@pytest.mark.asyncio
async def test_logging_plugin_loads_with_config(tmp_path):
    app = KamioApp()
    log_file = tmp_path / "app.log"
    await app.load_plugin(LoggingPlugin, config={"file": str(log_file), "level": "INFO"})
    assert app.get_plugin("logging") is not None
