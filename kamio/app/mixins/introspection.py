from __future__ import annotations

import time
from collections import deque
from typing import Any, Dict, List


class IntrospectionMixin:
    """System snapshot, audit log, and human/agent-readable description."""

    def _init_introspection(self: Any) -> None:
        self.audit_log: deque = deque(maxlen=200)

    def _audit(self: Any, action: str, **data: Any) -> None:
        self.audit_log.append({"ts": time.time(), "action": action, **data})

    def get_manifest(self: Any) -> Dict[str, Any]:
        """Return the full system state as a JSON-serializable dict."""
        devices: List[Dict[str, Any]] = []
        for device_id, dev in self.registry.instances.items():
            devices.append(
                {
                    "id": device_id,
                    "type": dev.device_type(),
                    "schema": type(dev).get_schema(),
                    "state": dev.get_state_snapshot(),
                    "config": dev.get_config_snapshot(),
                    "telemetry": dev.get_telemetry_snapshot(),
                }
            )
        rules = []
        for rule in self.rules.rules:
            entry: Dict[str, Any] = {
                "name": rule.func.__name__,
                "device_type": rule.device_type,
                "fields": rule.fields,
                "interval": rule.interval,
                "enabled": rule.enabled,
                "run_on_start": rule.run_on_start,
                "description": rule.description,
            }
            dsl = getattr(rule.func, "_kamio_dsl", None)
            if dsl is not None:
                entry["dsl"] = dsl
            rules.append(entry)
        return {
            "kamio_version": __import__("kamio").__version__,
            "running": self._is_running,
            "devices": devices,
            "rules": rules,
            "plugins": [
                {"name": name, "version": self.plugin_loader.get_plugin(name).version}
                for name in self.plugin_loader.list_plugins()
            ],
            "custom_nodes": [
                {"name": n, "topic_prefix": self.custom_nodes.get_node(n).topic_prefix}
                for n in self.custom_nodes.list_nodes()
            ],
            "policies": {
                "allowed_drivers": self._allowed_drivers,
                "denied_commands": self._denied_commands,
                "max_devices": self._max_devices or None,
                "dry_run": self.dry_run,
            },
        }

    def describe(self: Any) -> Dict[str, Any]:
        """Compact human/agent-readable summary of what the app is doing."""
        m = self.get_manifest()
        return {
            "running": m["running"],
            "devices": {d["id"]: d["type"] for d in m["devices"]},
            "rules": [
                f"{r['name']} ({'enabled' if r['enabled'] else 'disabled'})" for r in m["rules"]
            ],
            "plugins": m["plugins"],
            "custom_nodes": m["custom_nodes"],
            "mqtt_connected": bool(self.mqtt_client.is_connected),
            "recent_actions": [a["action"] for a in list(self.audit_log)[-10:]],
        }

    async def apply_manifest(self: Any, manifest: Dict[str, Any]) -> Dict[str, Any]:
        """Apply a manifest produced by :meth:`get_manifest` (or authored by an agent).

        Creates missing devices from their embedded schema, updates
        state/config on all devices, and registers DSL rules
        (``rules[].dsl``). Python-function rules cannot be reconstructed
        from JSON and are reported as skipped. Never raises for a single
        bad entry — errors are collected into the report.
        """
        from kamio.data_fields import config as cfg_field
        from kamio.data_fields import event as evt_field
        from kamio.data_fields import state as st_field
        from kamio.data_fields import telemetry as tl_field
        from kamio.dsl import make_device_class, when

        report: Dict[str, Any] = {
            "devices": {"created": [], "updated": [], "skipped": []},
            "rules": {"created": [], "skipped": []},
            "errors": [],
        }
        _factories = {
            "state": st_field,
            "telemetry": tl_field,
            "config": cfg_field,
            "event": evt_field,
        }
        for dspec in manifest.get("devices", []):
            did = dspec.get("id")
            try:
                if not isinstance(did, str) or not did:
                    raise ValueError(f"invalid device id {did!r}")
                existing = self.registry.get_instance(did)
                if existing is None:
                    fields_spec = {}
                    for fname, fspec in dspec.get("schema", {}).get("fields", {}).items():
                        factory = _factories.get(fspec.get("kind"))
                        if factory is None:
                            continue
                        fields_spec[fname] = factory(
                            default=fspec.get("default"),
                            description=fspec.get("description") or "",
                            **{
                                k: v
                                for k, v in fspec.items()
                                if k in ("writable", "unit", "min_value", "max_value", "choices")
                                and v is not None
                            },
                        )
                    cls = make_device_class(dspec.get("type", "device"), fields_spec)
                    await self.add_device(did, cls)
                    report["devices"]["created"].append(did)
                    existing = self.registry.get_instance(did)
                else:
                    report["devices"]["updated"].append(did)
                if existing is not None:
                    if dspec.get("state"):
                        await existing.handle_state(dspec["state"])
                    if dspec.get("config"):
                        await existing.handle_config(dspec["config"])
            except Exception as e:
                report["errors"].append(f"device {did!r}: {e}")
        for rspec in manifest.get("rules", []):
            try:
                dsl = rspec.get("dsl")
                if not dsl:
                    report["rules"]["skipped"].append(rspec.get("name"))
                    continue
                w, t = dsl["when"], dsl["then"]
                dtype = self.registry.get_class(w["device"].lower())
                if dtype is None:
                    raise ValueError(f"device type {w['device']!r} not registered")
                when(dtype, w["field"], w["op"], w["value"]).then_set(
                    self, t["device_id"], t["field"], t["value"]
                )
                report["rules"]["created"].append(rspec.get("name"))
            except Exception as e:
                report["errors"].append(f"rule {rspec.get('name')!r}: {e}")
        self._audit("apply_manifest", **{k: len(v) for k, v in report["devices"].items()})
        return report

    def pause_automation(self: Any) -> int:
        """Disable every rule; returns how many were paused."""
        paused = 0
        for rule in self.rules.rules:
            if rule.enabled:
                rule.enabled = False
                paused += 1
        self._audit("pause_automation", paused=paused)
        return paused

    def resume_automation(self: Any) -> int:
        """Re-enable all rules paused by :meth:`pause_automation`."""
        resumed = 0
        for rule in self.rules.rules:
            if not rule.enabled:
                rule.enabled = True
                resumed += 1
        self._audit("resume_automation", resumed=resumed)
        return resumed
