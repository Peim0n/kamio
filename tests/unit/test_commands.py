from __future__ import annotations

import pytest
from kamio import Device, KamioApp, command, state
from kamio.drivers.base import BaseDriver


class Switch(Device):
    power: bool = state(default=False, writable=True)
    reset_count: int = 0

    @command
    async def toggle(self):
        self.power = not self.power
        return {"power": self.power}

    @command
    def reset(self):
        self.power = False
        self.reset_count += 1
        return {"power": self.power, "reset_count": self.reset_count}

    @command
    async def set_power(self, value: bool):
        self.power = value
        return {"power": self.power}

    @command
    async def turn_on(self):
        # Commands may apply state changes through the public handle_state path.
        await self.handle_state({"power": True})
        return {"power": self.power}


class DriverRelay(BaseDriver):
    def __init__(self):
        super().__init__()
        self.calls = []

    async def connect(self):
        pass

    async def disconnect(self):
        pass

    async def read(self, field_name, params=None):
        return None

    async def execute(self, command_name, params):
        self.calls.append((command_name, params))
        if command_name == "driver_toggle":
            return {"driver": True, "command": command_name}
        raise NotImplementedError(command_name)



@pytest.mark.asyncio
async def test_async_command_runs_and_changes_state():
    app = KamioApp()
    device = await app.add_device("switch", Switch)
    result = await device.handle_command("toggle", {})
    assert device.power is True
    assert result == {"power": True}


@pytest.mark.asyncio
async def test_sync_command_runs_in_async_context():
    app = KamioApp()
    device = await app.add_device("switch", Switch)
    device.power = True
    result = await device.handle_command("reset", {})
    assert device.power is False
    assert device.reset_count == 1
    assert result == {"power": False, "reset_count": 1}


@pytest.mark.asyncio
async def test_command_with_parameters():
    app = KamioApp()
    device = await app.add_device("switch", Switch)
    result = await device.handle_command("set_power", {"value": True})
    assert device.power is True
    assert result == {"power": True}


@pytest.mark.asyncio
async def test_command_through_driver_execute():
    app = KamioApp()
    driver = DriverRelay()
    device = await app.add_device("switch", Switch, driver=driver)
    result = await device.handle_command("driver_toggle", {"value": True})
    assert result == {"driver": True, "command": "driver_toggle"}


@pytest.mark.asyncio
async def test_not_implemented_error_in_driver_falls_back_to_device():
    app = KamioApp()
    driver = DriverRelay()
    device = await app.add_device("switch", Switch, driver=driver)
    result = await device.handle_command("toggle", {})
    assert device.power is True
    assert result == {"power": True}


@pytest.mark.asyncio
async def test_command_triggers_state_change_and_rule():
    app = KamioApp()
    events = []

    @app.rule(device=Switch, fields=["power"])
    async def on_power(event, app):
        events.append(event.data)

    device = await app.add_device("switch", Switch)
    await device.handle_command("turn_on", {})
    assert device.power is True
    # Rule should be triggered by state change published by handle_state.
    assert any(e.get("power") is True for e in events)


@pytest.mark.asyncio
async def test_send_command_between_devices():
    app = KamioApp()
    await app.add_device("a", Switch)
    await app.add_device("b", Switch)
    target = app.devices["b"]
    result = await target.handle_command("set_power", {"value": True})
    assert target.power is True
    assert result == {"power": True}


@pytest.mark.asyncio
async def test_command_errors_are_propagated():
    app = KamioApp()
    device = await app.add_device("switch", Switch)
    with pytest.raises(Exception):
        await device.handle_command("unknown_command", {})
