from __future__ import annotations

import asyncio

import pytest
from unittest.mock import MagicMock

from kamio import Device, KamioApp, command, config, event, state, telemetry
from kamio.core.rules import RuleEvent
from kamio.core.custom_nodes import CustomNode
from kamio.core.transport import parse


class Switch(Device):
    power: bool = state(default=False, writable=True)
    brightness: int = state(default=100, min=0, max=255, writable=True)
    mode: str = state(default="auto", choices=("auto", "manual", "off"))
    energy: float = telemetry(default=0.0, unit="Wh")
    host: str = config(default="localhost")
    clicked: str = event(description="Click event")

    @command
    async def turn_on(self):
        await self.handle_state({"power": True})
        return {"power": self.power}


class MotionSensor(Device):
    motion: bool = state(default=False, writable=True)


class DummyNode(CustomNode):
    async def start(self):
        pass

    async def stop(self):
        pass

    async def handle_message(self, topic, payload):
        pass


def test_app_device_decorator_registers_class():
    app = KamioApp()

    @app.device
    class Lamp(Device):
        power: bool = state(default=False, writable=True)

    assert "lamp" in app.registered_types


def test_app_device_decorator_with_call_syntax():
    app = KamioApp()

    @app.device()
    class Bulb(Device):
        power: bool = state(default=False, writable=True)

    assert "bulb" in app.registered_types


@pytest.mark.asyncio
async def test_add_rule_explicitly_and_remove():
    app = KamioApp()
    calls = []

    async def on_motion(event: RuleEvent, app: KamioApp):
        calls.append(event.data)

    app.add_rule(on_motion, device=MotionSensor, fields=["motion"])
    device = await app.add_device("hall", MotionSensor)

    await device.handle_state({"motion": True})
    assert len(calls) == 1
    assert calls[0]["motion"] is True

    await app.remove_rule(on_motion)
    await device.handle_state({"motion": False})
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_remove_rule_twice_is_safe():
    app = KamioApp()

    @app.rule(device=MotionSensor, fields=["motion"])
    async def on_motion(event, app):
        pass

    await app.add_device("hall", MotionSensor)
    await app.remove_rule(on_motion)
    await app.remove_rule(on_motion)  # should not raise


@pytest.mark.asyncio
async def test_hooks_priority_and_clear():
    app = KamioApp()
    order = []

    app.register_hook("on_before_start", lambda: order.append("low"), priority=0)
    app.register_hook("on_before_start", lambda: order.append("high"), priority=10)

    hooks = app.hooks.list_hooks("on_before_start")
    assert len(hooks) == 2

    await app.start()
    await app.stop()

    assert order[0] == "high"
    assert order[1] == "low"

    app.hooks.clear("on_before_start")
    assert app.hooks.list_hooks("on_before_start") == []


@pytest.mark.asyncio
async def test_event_bus_filter_priority_and_unsubscribe():
    app = KamioApp()
    received = []

    def low(d):
        received.append(("low", d.get("level")))

    def high(d):
        received.append(("high", d.get("level")))

    app.subscribe_event("alert", low, priority=0)
    app.subscribe_event("alert", high, priority=1, filter_fn=lambda d: d.get("level") == "critical")

    await app.publish_event("alert", {"level": "info"})
    await app.publish_event("alert", {"level": "critical"})
    await asyncio.sleep(0.05)

    assert ("low", "info") in received
    assert ("low", "critical") in received
    assert ("high", "critical") in received
    assert ("high", "info") not in received

    app.unsubscribe_event("alert", low)
    app.event_bus.clear("alert")
    assert app.event_bus.list_subscribers("alert") == []


def test_ha_discovery_is_none_until_enabled():
    app = KamioApp()
    assert app.ha_discovery is None

    app.enable_ha_discovery(prefix="homeassistant")
    assert app.ha_discovery is not None
    assert app.ha_discovery.discovery_prefix == "homeassistant"

    app.disable_ha_discovery()


@pytest.mark.asyncio
async def test_device_lifecycle_hooks_fire():
    app = KamioApp()
    events = []

    app.register_hook("on_device_added", lambda d: events.append("added"))
    app.register_hook("on_device_started", lambda d: events.append("started"))
    app.register_hook("on_device_stopped", lambda d: events.append("stopped"))

    await app.add_device("s", Switch)
    assert "added" in events

    await app.start()
    await asyncio.sleep(0.05)
    assert "started" in events

    await app.stop()
    assert "stopped" in events


def test_device_snapshots():
    device = Switch()

    state_snap = device.get_state_snapshot()
    assert "power" in state_snap
    assert state_snap["power"] is False

    config_snap = device.get_config_snapshot()
    assert config_snap["host"] == "localhost"

    telemetry_snap = device.get_telemetry_snapshot()
    assert "energy" in telemetry_snap

    full_snap = device.get_full_snapshot()
    assert isinstance(full_snap, dict)


@pytest.mark.asyncio
async def test_device_on_init_and_lifecycle_methods_exist():
    device = Switch()
    await device.on_init()
    await device.on_start(node=MagicMock())
    await device.on_stop(node=MagicMock())
    await device.reinitialize()
    await device.shutdown()


@pytest.mark.asyncio
async def test_custom_node_publish_async():
    client = MagicMock()
    client.publish = MagicMock(return_value=(0, 1))
    node = DummyNode(client, "ns")
    await node.publish_async("status", b"ok")
    assert client.publish.called


def test_topic_parse_understands_current_and_legacy_format():
    assert parse("Kamio/v1/myid/ds") == ("myid", "ds")
    assert parse("Kamio/myid/dt") == ("myid", "dt")
