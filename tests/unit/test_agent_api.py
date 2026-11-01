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


@pytest.mark.asyncio
async def test_apply_manifest_roundtrip(mock_mqtt):
    mock_mqtt.simulate_connect()
    src = KamioApp(mqtt_broker=mock_mqtt)
    Sens = make_device_class("sens", {"temp": telemetry(default=0.0)})
    await src.add_device("s1", Sens)
    await src.add_device("fan1", Lamp)
    when(Sens, "temp", ">", 28).then_set(src, "fan1", "power", True)
    m = src.get_manifest()

    dst = KamioApp(mqtt_broker=mock_mqtt)
    rep = await dst.apply_manifest(m)
    assert set(rep["devices"]["created"]) == {"s1", "fan1"}
    assert rep["rules"]["created"]
    assert dst.registry.get_instance("s1").device_type() == "sens"

    # Idempotent-ish: second apply updates instead of recreating.
    rep2 = await dst.apply_manifest(m)
    assert set(rep2["devices"]["updated"]) == {"s1", "fan1"}
    # Bad entries collect errors, never raise.
    bad = await dst.apply_manifest({"devices": [{"id": 0}], "rules": [{"name": "x"}]})
    assert bad["errors"] and "x" in bad["rules"]["skipped"]


@pytest.mark.asyncio
async def test_mcp_tools_end_to_end(mock_mqtt):
    from kamio import KamioTools

    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt)
    tools = KamioTools(app)
    assert len(tools.list_tools()) >= 12

    r = await tools.call_tool(
        "create_device",
        {
            "device_id": "t1",
            "type": "thing",
            "fields": {"power": {"kind": "state", "default": False, "writable": True}},
        },
    )
    assert r["ok"] and (await tools.call_tool("list_devices", {}))["result"] == {"t1": "thing"}

    r = await tools.call_tool("set_state", {"device_id": "t1", "state": {"power": True}})
    assert r["ok"] and app.registry.get_instance("t1").power is True

    r = await tools.call_tool(
        "call_command", {"device_id": "t1", "method": "set_power", "params": {"value": False}}
    )
    assert r["ok"]
    assert (await tools.call_tool("get_device_state", {"device_id": "t1"}))["result"][
        "power"
    ] is False

    assert (
        await tools.call_tool(
            "add_condition_rule",
            {
                "device_type": "thing",
                "field": "power",
                "op": "==",
                "value": True,
                "target_id": "t1",
                "target_field": "power",
                "target_value": False,
            },
        )
    )["ok"]
    assert (await tools.call_tool("pause_automation", {}))["result"] >= 1
    assert (await tools.call_tool("resume_automation", {}))["result"] >= 1
    assert (await tools.call_tool("remove_rule", {"rule_name": "when_power==True_set_t1.power"}))[
        "ok"
    ]
    assert (await tools.call_tool("apply_manifest", {"manifest": {}}))["ok"]
    assert (await tools.call_tool("audit_log", {"limit": 3}))["result"]
    assert (await tools.call_tool("describe", {}))["ok"]
    assert (await tools.call_tool("get_manifest", {}))["ok"]
    assert (await tools.call_tool("remove_device", {"device_id": "t1"}))["ok"]

    # Errors are reported, not raised.
    assert (await tools.call_tool("nope", {}))["ok"] is False
    assert (await tools.call_tool("get_device_state", {"device_id": "ghost"}))["ok"] is False
    assert (await tools.call_tool("remove_rule", {"rule_name": "ghost"}))["ok"] is False
    assert (
        await tools.call_tool(
            "add_condition_rule",
            {
                "device_type": "ghost",
                "field": "x",
                "op": ">",
                "value": 1,
                "target_id": "t",
                "target_field": "f",
                "target_value": 1,
            },
        )
    )["ok"] is False


def test_errors_validation_attrs_and_dsl_spec_errors():
    from kamio import ValidationError
    e = ValidationError("bad", field="power", value="x")
    assert e.field == "power" and e.value == "x" and isinstance(e, ValueError)
    # make_device_class rejects non-Field specs and non-Device classes.
    with pytest.raises(TypeError):
        make_device_class("ok_name", {"f": "not a field"})
    with pytest.raises(TypeError):
        when("notaclass", "f", ">", 1)


@pytest.mark.asyncio
async def test_apply_manifest_updates_state_and_errors(mock_mqtt):
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt)
    # Manifest updating an EXISTING device's state/config, plus a rule
    # whose device type is unknown -> error collected, not raised.
    Lamp2 = make_device_class("l2", {"power": state(default=False, writable=True)})
    await app.add_device("d1", Lamp2)
    rep = await app.apply_manifest({
        "devices": [{"id": "d1", "type": "l2",
                     "schema": {"fields": {"power": {"kind": "state", "writable": True}}},
                     "state": {"power": True}}],
        "rules": [{"name": "n1", "dsl": {"when": {"device": "Ghost", "field": "x",
                                                 "op": ">", "value": 1},
                                          "then": {"device_id": "d1", "field": "power",
                                                   "value": False}}}],
    })
    assert app.registry.get_instance("d1").power is True
    assert rep["errors"] and "Ghost" in rep["errors"][0]
