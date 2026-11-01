"""31 — Agent-facing API: manifest, DSL, guardrails, MCP-style tools.

How a programmatic agent (LLM, automation controller) drives Kamio:
- create devices and rules from JSON-friendly specs (no Python classes needed),
- snapshot the whole system into a manifest and restore it back,
- operate inside guardrails (driver/command policies, dry_run),
- call the same operations through the MCP-style tool dispatcher.

Runs without a broker: add_device() only starts MQTT nodes when the app is
started, so every call below is fully local.
"""

from __future__ import annotations

import asyncio

from kamio import (
    DriverNotAllowedError,
    KamioApp,
    KamioTools,
    PolicyError,
    make_device_class,
    state,
    telemetry,
    when,
)


async def demo_manifest_and_describe() -> None:
    print("=== get_manifest / describe / audit_log ===")
    app = KamioApp()
    Lamp = make_device_class("lamp", {"power": state(default=False, writable=True)})
    await app.add_device("lamp1", Lamp)
    when(Lamp, "power", "==", True).then_set(app, "lamp1", "power", True)

    manifest = app.get_manifest()
    assert manifest["devices"][0]["id"] == "lamp1"
    assert manifest["rules"][0]["dsl"]["when"]["field"] == "power"
    print("[OK] manifest describes devices + DSL rules")

    summary = app.describe()
    assert summary["devices"] == {"lamp1": "lamp"}
    assert "add_device" in summary["recent_actions"]
    print(f"[OK] describe: {summary['devices']}, actions={summary['recent_actions']}")


async def demo_guardrails() -> None:
    print("\n=== Guardrails: allowed_drivers, denied_commands, max_devices, dry_run ===")
    from kamio.drivers.mock import MockHardwareDriver

    app = KamioApp(
        allowed_drivers=["MockHardwareDriver"],
        max_devices=1,
        denied_commands=["reboot"],
        dry_run=True,
    )
    Lamp = make_device_class("lamp", {"power": state(default=False, writable=True)})
    lamp = await app.add_device("lamp1", Lamp)

    try:
        await app.add_device("lamp2", Lamp)
        raise AssertionError("max_devices not enforced")
    except PolicyError:
        print("[OK] max_devices -> PolicyError")

    class ExoticDriver(MockHardwareDriver):
        pass

    app2 = KamioApp(allowed_drivers=["MockHardwareDriver"])
    try:
        await app2.add_device("x", Lamp, driver=ExoticDriver())
        raise AssertionError("driver policy not enforced")
    except DriverNotAllowedError:
        print("[OK] allowed_drivers -> DriverNotAllowedError")

    out = await lamp.handle_state({"power": True})
    assert out == {"dry_run": True, "would_apply": {"power": True}}
    assert lamp.power is False
    print("[OK] dry_run validates without applying")

    try:
        await lamp.handle_command("reboot", {})
        raise AssertionError("denied command not blocked")
    except ValueError:
        print("[OK] denied_commands rejected")

    app.dry_run = False
    when(Lamp, "power", "==", True).then_set(app, "lamp1", "power", False)
    assert app.pause_automation() == 1
    assert app.resume_automation() == 1
    print("[OK] pause/resume_automation toggles every rule")


async def demo_apply_manifest_roundtrip() -> None:
    print("\n=== apply_manifest: JSON -> running system ===")
    src = KamioApp()
    Sens = make_device_class("sens", {"temp": telemetry(default=0.0)})
    Lamp = make_device_class("lamp", {"power": state(default=False, writable=True)})
    await src.add_device("s1", Sens)
    await src.add_device("fan1", Lamp)
    when(Sens, "temp", ">", 28).then_set(src, "fan1", "power", True)

    manifest = src.get_manifest()
    dst = KamioApp()
    report = await dst.apply_manifest(manifest)
    assert set(report["devices"]["created"]) == {"s1", "fan1"}
    assert report["rules"]["created"]
    assert dst.registry.get_instance("fan1").device_type() == "lamp"
    print(f"[OK] round-trip recreated {report['devices']['created']} + rules")
    assert report["errors"] == []


async def demo_mcp_tools() -> None:
    print("\n=== KamioTools: MCP-style dispatcher ===")
    app = KamioApp()
    tools = KamioTools(app)
    assert len(tools.list_tools()) >= 12

    r = await tools.call_tool(
        "create_device",
        {
            "device_id": "heater",
            "type": "heater",
            "fields": {"power": {"kind": "state", "default": False, "writable": True}},
        },
    )
    assert r["ok"], r
    r = await tools.call_tool("set_state", {"device_id": "heater", "state": {"power": True}})
    assert r["ok"] and app.registry.get_instance("heater").power is True
    print("[OK] create_device + set_state via tool calls")

    r = await tools.call_tool("audit_log", {"limit": 3})
    assert r["ok"] and r["result"]
    bad = await tools.call_tool("no_such_tool", {})
    assert bad["ok"] is False and "unknown tool" in bad["error"]
    print("[OK] errors return {'ok': False, 'error': ...} — never raise")


async def main() -> None:
    await demo_manifest_and_describe()
    await demo_guardrails()
    await demo_apply_manifest_roundtrip()
    await demo_mcp_tools()
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
