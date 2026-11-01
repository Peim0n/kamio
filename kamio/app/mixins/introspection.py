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
