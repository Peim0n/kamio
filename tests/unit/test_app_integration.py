from __future__ import annotations

import asyncio
import os

import pytest

from kamio import Device, KamioApp, command, state


class Light(Device):
    power: bool = state(default=False, writable=True)

    @command
    async def turn_on(self):
        await self.handle_state({"power": True})
        return {"power": self.power}


class MotionSensor(Device):
    motion: bool = state(default=False, writable=True)


@pytest.mark.asyncio
async def test_kamio_app_lifecycle():
    app = KamioApp()
    assert not app.is_running
    await app.start()
    assert app.is_running
    await app.stop()
    assert not app.is_running


@pytest.mark.asyncio
async def test_add_and_remove_device():
    app = KamioApp()
    device = await app.add_device("living_room", Light)
    assert app.devices["living_room"] is device
    assert isinstance(device, Light)
    await app.remove_device("living_room")
    assert "living_room" not in app.devices


@pytest.mark.asyncio
async def test_create_device_by_type():
    app = KamioApp()
    await app.add_device("motion", MotionSensor)
    device = await app.create_device("hall", "motionsensor")
    assert isinstance(device, MotionSensor)
    assert app.devices["hall"] is device


@pytest.mark.asyncio
async def test_register_device_class():
    app = KamioApp()
    app.register(Light)
    assert "light" in app.registered_types


@pytest.mark.asyncio
async def test_app_rule_cross_device():
    app = KamioApp()
    triggered = []

    @app.rule(device=MotionSensor, fields=["motion"])
    async def on_motion(event, app):
        light = app.devices.get("living_room")
        if light:
            await light.handle_command("turn_on", {})
        triggered.append(event.data)

    await app.add_device("living_room", Light)
    await app.add_device("hall", MotionSensor)
    sensor = app.devices["hall"]
    await sensor.handle_state({"motion": True})
    await asyncio.sleep(0.05)
    assert any(d.get("motion") is True for d in triggered)
    assert app.devices["living_room"].power is True


@pytest.mark.asyncio
async def test_event_bus_publish_and_subscribe():
    app = KamioApp()
    received = []
    app.subscribe_event("custom_alert", lambda d: received.append(d))
    await app.publish_event("custom_alert", {"level": "critical"})
    await asyncio.sleep(0.05)
    assert received[0]["level"] == "critical"


@pytest.mark.asyncio
async def test_hooks_trigger():
    app = KamioApp()
    called = []
    app.register_hook("on_before_start", lambda: called.append("before"))
    app.register_hook("on_after_start", lambda: called.append("after"))
    await app.start()
    await app.stop()
    assert "before" in called
    assert "after" in called


@pytest.mark.asyncio
async def test_hot_reload_facade():
    app = KamioApp()
    assert hasattr(app, "hot_reload")
    # Hot reload methods should exist and not raise on basic usage.
    app.enable_hot_reload()
    await app.disable_hot_reload()
    app.watch_file("dummy.py", lambda p: None)
    assert os.path.abspath("dummy.py") in app.hot_reload.list_watched()
