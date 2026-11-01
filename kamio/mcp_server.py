from __future__ import annotations

"""MCP-style tool surface for programmatic agents.

``KamioTools`` exposes the app's public operations as named tools with a
JSON-serializable schema — the same shape MCP (Model Context Protocol)
expects, so a thin transport shim can wrap it (stdio/SSE) without changing
the call surface. Returns plain dicts; every failure is captured into
``{"ok": False, "error": ...}`` instead of raising.
"""

from typing import Any, Dict, List

from kamio.data_fields import config as cfg_field
from kamio.data_fields import event as evt_field
from kamio.data_fields import state as st_field
from kamio.data_fields import telemetry as tl_field
from kamio.dsl import make_device_class, when


class KamioTools:
    """Callable tool registry bound to a :class:`KamioApp` instance."""

    _FACTORIES = {"state": st_field, "telemetry": tl_field, "config": cfg_field, "event": evt_field}

    def __init__(self, app) -> None:
        self.app = app

    def list_tools(self) -> List[Dict[str, Any]]:
        """Return the tool schema — names, descriptions, argument shapes."""
        return [
            {"name": "get_manifest", "description": "Full system snapshot (JSON)."},
            {"name": "describe", "description": "Compact summary of the running system."},
            {"name": "list_devices", "description": "Device ids and types."},
            {"name": "get_device_state", "args": {"device_id": "str"}},
            {
                "name": "create_device",
                "args": {
                    "device_id": "str",
                    "type": "str",
                    "fields": "{name: {kind, default?, writable?, min_value?, max_value?, choices?}}",
                },
            },
            {"name": "remove_device", "args": {"device_id": "str"}},
            {"name": "set_state", "args": {"device_id": "str", "state": "{field: value}"}},
            {
                "name": "call_command",
                "args": {"device_id": "str", "method": "str", "params": "dict"},
            },
            {
                "name": "add_condition_rule",
                "args": {
                    "device_type": "str",
                    "field": "str",
                    "op": "str",
                    "value": "any",
                    "target_id": "str",
                    "target_field": "str",
                    "target_value": "any",
                },
            },
            {"name": "remove_rule", "args": {"rule_name": "str"}},
            {"name": "pause_automation", "description": "Disable all rules."},
            {"name": "resume_automation", "description": "Re-enable all rules."},
            {"name": "apply_manifest", "args": {"manifest": "dict"}},
            {"name": "audit_log", "args": {"limit": "int?"}},
        ]

    async def call_tool(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Dispatch a tool call. Always returns a dict, never raises."""
        handler = getattr(self, f"_tool_{name}", None)
        if handler is None:
            return {"ok": False, "error": f"unknown tool {name!r}"}
        try:
            result = handler(**(arguments or {}))
            if hasattr(result, "__await__"):
                result = await result
            return {"ok": True, "result": result}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    # ----------------------------------------------------------
    def _tool_get_manifest(self) -> Any:
        return self.app.get_manifest()

    def _tool_describe(self) -> Any:
        return self.app.describe()

    def _tool_list_devices(self) -> Any:
        return {i: d.device_type() for i, d in self.app.registry.instances.items()}

    def _tool_get_device_state(self, device_id: str) -> Any:
        dev = self.app.registry.get_instance(device_id)
        if dev is None:
            raise ValueError(f"device {device_id!r} not found")
        return dev.get_full_snapshot()

    async def _tool_create_device(self, device_id: str, type: str, fields: dict) -> Any:
        spec = {
            n: self._FACTORIES[f["kind"]](
                default=f.get("default"),
                description=f.get("description") or "",
                **{
                    k: v
                    for k, v in f.items()
                    if k in ("writable", "unit", "min_value", "max_value", "choices")
                    and v is not None
                },
            )
            for n, f in fields.items()
        }
        await self.app.add_device(device_id, make_device_class(type, spec))
        return device_id

    async def _tool_remove_device(self, device_id: str) -> Any:
        await self.app.remove_device(device_id)
        return device_id

    async def _tool_set_state(self, device_id: str, state: dict) -> Any:
        dev = self.app.registry.get_instance(device_id)
        if dev is None:
            raise ValueError(f"device {device_id!r} not found")
        return await dev.handle_state(state)

    async def _tool_call_command(self, device_id: str, method: str, params: dict) -> Any:
        dev = self.app.registry.get_instance(device_id)
        if dev is None:
            raise ValueError(f"device {device_id!r} not found")
        return await dev.handle_command(method, params or {})

    def _tool_add_condition_rule(
        self,
        device_type: str,
        field: str,
        op: str,
        value: Any,
        target_id: str,
        target_field: str,
        target_value: Any,
    ) -> Any:
        cls = self.app.registry.get_class(device_type)
        if cls is None:
            raise ValueError(f"device type {device_type!r} not registered")
        rule = when(cls, field, op, value).then_set(self.app, target_id, target_field, target_value)
        return rule.__name__

    async def _tool_remove_rule(self, rule_name: str) -> Any:
        target = next((r for r in self.app.rules.rules if r.func.__name__ == rule_name), None)
        if target is None:
            raise ValueError(f"rule {rule_name!r} not found")
        await self.app.remove_rule(target.func)
        return rule_name

    def _tool_pause_automation(self) -> Any:
        return self.app.pause_automation()

    def _tool_resume_automation(self) -> Any:
        return self.app.resume_automation()

    async def _tool_apply_manifest(self, manifest: dict) -> Any:
        return await self.app.apply_manifest(manifest)

    def _tool_audit_log(self, limit: int = 20) -> Any:
        return list(self.app.audit_log)[-limit:]
