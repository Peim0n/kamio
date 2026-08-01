from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from kamio import Device, command, config, event, state, telemetry


class ExampleDevice(Device):
    power: bool = state(default=False, writable=True)
    brightness: int = state(default=100, min=0, max=255, writable=True)
    mode: str = state(default="auto", choices=("auto", "manual", "off"))
    temperature: float = telemetry(default=20.0, unit="°C", freq="5s")
    host: str = config(default="localhost")
    button: str = event(description="Button press")

    @command
    async def toggle(self):
        self.power = not self.power
        return {"power": self.power}


class EmptyDevice(Device):
    pass


def test_device_type_lowercase():
    assert ExampleDevice.device_type() == "exampledevice"
    assert EmptyDevice.device_type() == "emptydevice"


def test_kamio_fields_collect_state_telemetry_config():
    assert "power" in ExampleDevice.Kamio_FIELDS
    assert "brightness" in ExampleDevice.Kamio_FIELDS
    assert "temperature" in ExampleDevice.Kamio_FIELDS
    assert "host" in ExampleDevice.Kamio_FIELDS
    assert "button" not in ExampleDevice.Kamio_FIELDS


def test_kamio_events_collect_event_fields():
    assert "button" in ExampleDevice.Kamio_EVENTS


def test_kamio_commands_collect_command_methods():
    assert "toggle" in ExampleDevice.Kamio_COMMANDS


def test_get_schema_includes_fields_commands_events():
    schema = ExampleDevice.get_schema()
    assert isinstance(schema, dict)
    # Schema must describe all fields, commands and events per docs.
    assert "power" in str(schema)
    assert "toggle" in str(schema)
    assert "button" in str(schema)


def test_get_fields_filter_by_kind():
    assert "temperature" in ExampleDevice.get_fields(kind="telemetry")
    assert "power" in ExampleDevice.get_fields(kind="state")
    assert "host" in ExampleDevice.get_fields(kind="config")


def test_get_states_filter_writable():
    writable = ExampleDevice.get_states(writable=True)
    assert "power" in writable
    assert "brightness" in writable
    # mode has choices but is writable by default per state API.
    assert "mode" in writable


def test_get_telemetry_returns_telemetry_fields():
    telemetry_fields = ExampleDevice.get_telemetry()
    assert "temperature" in telemetry_fields
    assert "power" not in telemetry_fields


def test_get_commands_returns_command_names():
    commands = ExampleDevice.get_commands()
    assert "toggle" in commands


@pytest.mark.asyncio
async def test_device_instance_initializes_defaults():
    device = ExampleDevice()
    assert device.power is False
    assert device.brightness == 100
    assert device.mode == "auto"
    assert device.temperature == 20.0
    assert device.host == "localhost"


@pytest.mark.asyncio
async def test_device_constructor_kwargs_applied_to_state_fields():
    """Constructor kwargs matching state field names should set field values."""
    device = ExampleDevice(power=True, brightness=50, mode="manual")
    assert device.power is True
    assert device.brightness == 50
    assert device.mode == "manual"


@pytest.mark.asyncio
async def test_device_constructor_ignores_non_field_kwargs():
    """Constructor kwargs that don't match any field should be silently ignored."""
    device = ExampleDevice(unknown_param="ignored")
    assert device.power is False  # default preserved


@pytest.mark.asyncio
async def test_handle_state_applies_valid_changes():
    device = ExampleDevice()
    applied = await device.handle_state({"power": True, "brightness": 200})
    assert device.power is True
    assert device.brightness == 200
    assert "power" in applied
    assert "brightness" in applied


@pytest.mark.asyncio
async def test_handle_state_rejects_out_of_range():
    device = ExampleDevice()
    with pytest.raises((ValueError, AssertionError)):
        await device.handle_state({"brightness": 300})


@pytest.mark.asyncio
async def test_handle_state_rejects_invalid_choice():
    device = ExampleDevice()
    with pytest.raises((ValueError, AssertionError)):
        await device.handle_state({"mode": "unknown"})


@pytest.mark.asyncio
async def test_handle_config_applies_config_changes():
    device = ExampleDevice()
    applied = await device.handle_config({"host": "broker.local"})
    assert device.host == "broker.local"
    assert "host" in applied


@pytest.mark.asyncio
async def test_handle_command_runs_command():
    device = ExampleDevice()
    result = await device.handle_command("toggle", {})
    assert device.power is True
    assert result == {"power": True}


@pytest.mark.asyncio
async def test_command_method_is_async_callable():
    device = ExampleDevice()
    result = await ExampleDevice.Kamio_COMMANDS["toggle"](device)
    assert device.power is True


@pytest.mark.asyncio
async def test_lifecycle_hooks_run_async():
    node = MagicMock()
    node.is_running = True
    node.start_telemetry = AsyncMock()
    node.stop_telemetry = AsyncMock()
    device = ExampleDevice()
    await device.on_init()
    await device.on_start(node=node)
    await device.on_stop(node=node)


@pytest.mark.asyncio
async def test_emit_and_handle_event():
    device = ExampleDevice()
    payload = {"button": "power"}
    # emit publishes; handle_event processes incoming events.
    await device.emit("button", payload)
    await device.handle_event("button", payload)
