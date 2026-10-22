from __future__ import annotations

import asyncio

import pytest
from conftest import wait_until

from kamio import Device, KamioApp, command, state


class Switch(Device):
    power: bool = state(default=False, writable=True)

    @command
    async def turn_on(self):
        await self.handle_state({"power": True})
        return {"power": self.power}


@pytest.mark.asyncio
async def test_two_apps_start_and_stop_independently():
    app1 = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="start_a")
    app2 = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="start_b")

    assert not app1.is_running
    assert not app2.is_running

    await app1.start()
    assert app1.is_running

    await app2.start()
    assert app2.is_running

    await app1.stop()
    assert not app1.is_running

    await app2.stop()
    assert not app2.is_running


@pytest.mark.asyncio
async def test_two_apps_state_sync_propagates_via_broker():
    app1 = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="sync_a")
    app2 = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="sync_b")

    await app1.start()
    await app2.start()

    try:
        await app1.add_device("sw", Switch)
        await app2.add_device("sw", Switch)

        assert app2.devices["sw"].power is False

        device1 = app1.devices["sw"]
        await device1.handle_state({"power": True})
        await device1.request_state_sync()

        assert await wait_until(lambda: app2.devices["sw"].power is True, timeout=5.0)
    finally:
        await app1.stop()
        await app2.stop()


@pytest.mark.asyncio
async def test_two_apps_command_from_app1_to_app2_device():
    app1 = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="cmd_a")
    app2 = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="cmd_b")

    await app1.start()
    await app2.start()

    try:
        await app1.add_device("ctrl", Switch)
        await app2.add_device("light", Switch)

        assert app2.devices["light"].power is False

        controller = app1.devices["ctrl"]
        await controller.send_command("light", "turn_on", {})

        assert await wait_until(lambda: app2.devices["light"].power is True, timeout=5.0)
    finally:
        await app1.stop()
        await app2.stop()


@pytest.mark.asyncio
async def test_two_apps_different_devices_do_not_interact():
    app1 = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="iso_a")
    app2 = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="iso_b")

    await app1.start()
    await app2.start()

    try:
        await app1.add_device("sw", Switch)
        await app2.add_device("other", Switch)

        assert app2.devices["other"].power is False

        await app1.devices["sw"].handle_state({"power": True})
        await app1.devices["sw"].request_state_sync()

        # Flush already-queued deliveries; no matching subscription exists on
        # app2, so nothing may ever arrive for "other".
        for _ in range(20):
            await asyncio.sleep(0)

        assert app2.devices["other"].power is False
    finally:
        await app1.stop()
        await app2.stop()
