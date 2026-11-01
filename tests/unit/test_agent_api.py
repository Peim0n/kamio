"""Agent-facing API: errors, manifest, audit log, guardrails, DSL."""

from __future__ import annotations

import pytest

from kamio import (
    DriverNotAllowedError,
    KamioApp,
    KamioError,
    PolicyError,
    Device,
    event,
    make_device_class,
    state,
    telemetry,
    when,
)
from kamio.drivers.mock import MockHardwareDriver


class Lamp(Device):
    power: bool = state(default=False, writable=True)


class Sensor(Device):
    temperature: float = telemetry(default=20.0, freq=1)


@pytest.mark.asyncio
async def test_get_manifest_describe_audit(mock_mqtt):
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt)
    dev = await app.add_device("lamp1", Lamp)

    async def r(ev, app):
        pass

    app.add_rule(r, device=Lamp, fields=["power"])

    m = app.get_manifest()
    assert m["devices"][0]["id"] == "lamp1"
    assert m["devices"][0]["schema"]["fields"]["power"]["kind"] == "state"
    assert m["rules"][0]["name"] == "r"
    d = app.describe()
    assert d["devices"] == {"lamp1": "lamp"}
    assert "add_device" in d["recent_actions"]


@pytest.mark.asyncio
async def test_guardrails(mock_mqtt):
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, allowed_drivers=["MockHardwareDriver"], max_devices=1)
    await app.add_device("d1", Lamp)
    with pytest.raises(PolicyError):
        await app.add_device("d2", Lamp)

    class BadDrv(MockHardwareDriver):
        pass

    app_b = KamioApp(mqtt_broker=mock_mqtt, allowed_drivers=["MockHardwareDriver"])
    with pytest.raises(DriverNotAllowedError):
        await app_b.add_device("d3", Lamp, driver=BadDrv())


@pytest.mark.asyncio
async def test_dry_run_and_denied_commands(mock_mqtt):
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, dry_run=True, denied_commands=["reboot"])
    dev = await app.add_device("lamp1", Lamp)

    out = await dev.handle_state({"power": True})
    assert out["dry_run"] is True and out["would_apply"] == {"power": True}
    assert dev.power is False  # state untouched

    cmd = await dev.handle_command("set_power", {"value": True})
    assert cmd["dry_run"] is True
    # Denied commands take precedence over dry-run.
    with pytest.raises(ValueError, match="denied"):
        await dev.handle_command("reboot", {})


@pytest.mark.asyncio
async def test_pause_resume_automation(mock_mqtt):
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt)

    async def r(ev, app):
        pass

    app.add_rule(r, device=Lamp)
    assert app.pause_automation() == 1
    assert all(not x.enabled for x in app.rules.rules)
    assert app.resume_automation() == 1
    assert all(x.enabled for x in app.rules.rules)


@pytest.mark.asyncio
async def test_dsl_make_device_and_when(mock_mqtt):
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt)

    Sens = make_device_class("sens", {"temp": telemetry(default=0.0)})
    assert Sens.device_type() == "sens"
    target = await app.add_device("fan1", Lamp)
    await app.add_device("s1", Sens)

    when(Sens, "temp", ">", 28).then_set(app, "fan1", "power", True)
    rule = app.rules.rules[-1]
    assert rule.func._kamio_dsl["when"]["op"] == ">"

    # Fire the rule path directly.
    event = type("E", (), {})()
    event.data = {"temp": 30.0}
    await rule.func(event, app)
    assert target.power is True

    with pytest.raises(ValueError):
        make_device_class("bad name!", {"x": state()})
    with pytest.raises(ValueError):
        when(Sens, "nope", ">", 1)
    with pytest.raises(ValueError):
        when(Sens, "temp", "~", 1)


def test_errors_are_kamio_and_builtin():
    from kamio import ValidationError, DeviceNotFoundError

    assert issubclass(ValidationError, ValueError)
    assert issubclass(DeviceNotFoundError, KamioError)
