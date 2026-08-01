from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from kamio import Device, KamioApp, state


class Dimmer(Device):
    power: bool = state(default=False, writable=True)
    brightness: int = state(default=0, min=0, max=255, writable=True)


@pytest.mark.asyncio
async def test_handle_state_triggers_on_state_changed_callback():
    app = KamioApp()
    device = await app.add_device("dimmer", Dimmer)
    device._on_state_changed = AsyncMock()
    await device.handle_state({"power": True, "brightness": 100})
    assert device.power is True
    assert device.brightness == 100
    # Callback should fire for each changed field.
    assert device._on_state_changed.call_count >= 2


@pytest.mark.asyncio
async def test_set_state_silent_updates_without_callback():
    app = KamioApp()
    device = await app.add_device("dimmer", Dimmer)
    device._on_state_changed = AsyncMock()
    device._on_rules_trigger = AsyncMock()
    device._set_state(power=True, brightness=50)
    assert device.power is True
    assert device.brightness == 50
    device._on_state_changed.assert_not_awaited()
    device._on_rules_trigger.assert_not_awaited()


@pytest.mark.asyncio
async def test_request_state_sync_exists_and_runs():
    app = KamioApp()
    device = await app.add_device("dimmer", Dimmer)
    # Should not raise and should publish current state values.
    await device.request_state_sync()
    assert device.power is False
    assert device.brightness == 0


@pytest.mark.asyncio
async def test_request_full_sync_exists_and_runs():
    app = KamioApp()
    device = await app.add_device("dimmer", Dimmer)
    await device.request_full_sync()
    assert device.power is False
    assert device.brightness == 0
