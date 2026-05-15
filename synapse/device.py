from __future__ import annotations

import asyncio
import logging
from typing import Any, ClassVar, TYPE_CHECKING, Optional, Dict

from synapse.data_fields import Field
from synapse.core.envelope import Envelope
from synapse.core.mixins import TelemetryMixin
from synapse.core.device_meta import DeviceMeta

if TYPE_CHECKING:
    from synapse.core.mqtt_nodes import DeviceNode
    from synapse.drivers.base import BaseDriver
    from .app import SynapseApp

def command(func: Any) -> Any:
    """Decorator for device commands."""
    func._is_command = True
    func._command_name = func.__name__
    return func

class Device(TelemetryMixin, metaclass=DeviceMeta):
    """
    Base class for all Synapse Devices.
    Handles MQTT communication, state synchronization, and RPC commands.
    """
    SYNAPSE_FIELDS: ClassVar[Dict[str, Field]]
    SYNAPSE_COMMANDS: ClassVar[Dict[str, Any]]
    SYNAPSE_EVENTS: ClassVar[Dict[str, Field]]
    _cached_schema: ClassVar[Optional[Dict[str, Any]]] = None

    def __init__(self, driver: Optional[BaseDriver] = None, **kwargs):
        logger_name = f"synapse.device.{self.device_type()}"
        super().__init__(logger_name=logger_name)

        self.node: Optional[DeviceNode] = None
        self.driver: Optional[BaseDriver] = driver
        self._app: Optional[SynapseApp] = None

        # Apply default values for state and config fields
        self._apply_defaults()

    @property
    def app(self) -> SynapseApp:
        """Returns the SynapseApp instance this device is attached to."""
        if self._app is None:
            raise RuntimeError(f"Device '{self.device_type()}' is not attached to any SynapseApp.")
        return self._app

    @app.setter
    def app(self, value: SynapseApp):
        from .app import SynapseApp as _SynapseApp
        if not isinstance(value, _SynapseApp):
            raise TypeError(f"Expected SynapseApp instance, got {type(value).__name__}")

        if self._app is not None and self._app is not value:
            self.logger.warning(f"Device {self.device_type()} is being re-attached to a different SynapseApp")
        self._app = value

    @classmethod
    def device_type(cls) -> str:
        return cls.__name__.lower()

    def _apply_defaults(self):
        """Sets initial values for all fields from metadata."""
        for name, field in self.SYNAPSE_FIELDS.items():
            if not hasattr(self, name):
                setattr(self, name, field.default)

    async def on_init(self, **kwargs):
        """Async initialization hook. Connects driver if present."""
        if self.driver:
            try:
                await self.driver.connect()
            except Exception as e:
                self.logger.error(f"Driver connection failed: {e}")

    async def on_start(self, node: DeviceNode):
        if not node.is_running:
            return
        await self.start_telemetry()

    async def on_stop(self, node: DeviceNode):
        if self.driver:
            await self.driver.disconnect()
        await self.cancel_all_tasks()

    async def shutdown(self):
        if self.driver:
            await self.driver.disconnect()
        await self.cancel_all_tasks()

    async def reinitialize(self):
        if self.node:
            await self.on_stop(self.node)
            await self.on_start(self.node)

    def _get_field_value(self, field_name: str) -> Any:
        field = self.SYNAPSE_FIELDS.get(field_name)
        if not field:
            return None
        return getattr(self, field_name, field.default if field.kind in ("state", "config") else None)

    def _validate_value(self, field: Field, value: Any) -> Any:
        """Validates field value. Raises ValueError on failure."""
        if field.choices and value not in field.choices:
            raise ValueError(f"Invalid value for {field.name}: {value}. Must be one of {field.choices}")

        if field.min is not None and isinstance(value, (int, float)) and value < field.min:
            raise ValueError(f"Value for {field.name} is too low: {value} < {field.min}")

        if field.max is not None and isinstance(value, (int, float)) and value > field.max:
            raise ValueError(f"Value for {field.name} is too high: {value} > {field.max}")

        return value

    async def handle_state(self, data: dict):
        """Handles state changes."""
        applied_changes = {}
        for key, value in data.items():
            field = self.SYNAPSE_FIELDS.get(key)
            if field and field.kind == "state" and field.writable:
                valid_value = self._validate_value(field, value)

                if self.driver:
                    try:
                        await self.driver.execute(f"set_{key}", {"value": valid_value})
                    except Exception as e:
                        self.logger.error(f"Driver execution failed for {key}: {e}")

                setattr(self, key, valid_value)
                applied_changes[key] = valid_value
        return applied_changes

    async def handle_config(self, data: dict):
        """Handles configuration changes."""
        applied_changes = {}
        for key, value in data.items():
            field = self.SYNAPSE_FIELDS.get(key)
            if field and field.kind == "config":
                valid_value = self._validate_value(field, value)
                setattr(self, key, valid_value)
                applied_changes[key] = valid_value
        return applied_changes

    def get_state_snapshot(self) -> Dict[str, Any]:
        return {n: self._get_field_value(n) for n, f in self.SYNAPSE_FIELDS.items() if f.kind == "state"}

    def get_config_snapshot(self) -> Dict[str, Any]:
        return {n: self._get_field_value(n) for n, f in self.SYNAPSE_FIELDS.items() if f.kind == "config"}

    def get_telemetry_snapshot(self) -> Dict[str, Any]:
        return {n: self._get_field_value(n) for n, f in self.SYNAPSE_FIELDS.items() if f.kind == "telemetry"}

    def get_full_snapshot(self) -> Dict[str, Any]:
        return {**self.get_state_snapshot(), **self.get_config_snapshot(), **self.get_telemetry_snapshot()}

    @classmethod
    def get_schema(cls) -> Dict[str, Any]:
        if cls._cached_schema:
            return cls._cached_schema

        cls._cached_schema = {
            "type": cls.device_type(),
            "fields": {
                n: {
                    "kind": f.kind,
                    "type": str(f.python_type.__name__) if hasattr(f.python_type, "__name__") else str(f.python_type),
                    "default": f.default,
                    "writable": f.writable,
                    "freq": f.freq,
                    "unit": getattr(f, "unit", None),
                    "required": getattr(f, "required", False)
                } for n, f in cls.SYNAPSE_FIELDS.items()
            },
            "commands": list(cls.SYNAPSE_COMMANDS.keys()),
            "events": {
                n: {
                    "kind": f.kind,
                    "type": str(f.python_type.__name__) if hasattr(f.python_type, "__name__") else str(f.python_type),
                    "unit": getattr(f, "unit", None)
                } for n, f in cls.SYNAPSE_EVENTS.items()
            }
        }
        return cls._cached_schema

    async def emit(self, event_name: str, payload: dict):
        if self.node:
            await self.node.emit_event(event_name, payload)

    async def _safe_publish(self, env: Envelope):
        """TelemetryMixin publication implementation."""
        if not self.node:
            return
        try:
            await self.node.publish(env)
        except Exception as e:
            self.logger.error(f"Failed to publish {env.type}: {e}")

    async def request_state_sync(self):
        if self.node:
            data = self.get_state_snapshot()
            env = Envelope.state(source=self.node.device_id, data=data)
            await self._safe_publish(env)

    async def request_full_sync(self):
        if self.node:
            data = self.get_full_snapshot()
            env = Envelope.state(source=self.node.device_id, data=data)
            await self._safe_publish(env)

    async def handle_command(self, method_name: str, params: dict) -> Any:
        if self.driver:
            try:
                return await self.driver.execute(method_name, params)
            except NotImplementedError:
                pass
            except Exception as e:
                self.logger.error(f"Driver command execution failed: {e}")
                raise

        method = self.SYNAPSE_COMMANDS.get(method_name)
        if not method:
            raise AttributeError(f"Command {method_name} not found")

        if asyncio.iscoroutinefunction(method):
            return await method(self, **params)
        return method(self, **params)

    async def handle_event(self, event_name: str, payload: dict):
        pass

    @classmethod
    def get_fields(cls, kind: str | None = None, writable: bool | None = None) -> Dict[str, Field]:
        fields = cls.SYNAPSE_FIELDS
        if kind is None and writable is None:
            return fields.copy()
        result: Dict[str, Field] = {}
        for name, field_obj in fields.items():
            if kind is not None and field_obj.kind != kind:
                continue
            if writable is not None and field_obj.writable != writable:
                continue
            result[name] = field_obj
        return result

    @classmethod
    def get_telemetry(cls) -> Dict[str, Field]:
        return cls.get_fields(kind="telemetry")

    @classmethod
    def get_states(cls, writable: bool | None = None) -> Dict[str, Field]:
        return cls.get_fields(kind="state", writable=writable)

    @classmethod
    def get_commands(cls) -> Dict[str, Any]:
        return cls.SYNAPSE_COMMANDS.copy()
