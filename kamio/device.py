from __future__ import annotations

import asyncio
import logging
import threading
from typing import TYPE_CHECKING, Any, Awaitable, Callable, ClassVar, Dict, Optional

from kamio.core.device_meta import DeviceMeta
from kamio.core.envelope import Envelope
from kamio.core.mixins import TelemetryMixin
from kamio.data_fields import Field

if TYPE_CHECKING:
    from kamio.core.mqtt_nodes import DeviceNode
    from kamio.drivers.base import BaseDriver

    from .app import KamioApp


def command(func: Any = None, *, name: Optional[str] = None) -> Any:
    """
    Decorator to expose a device method as an RPC command.

    Decorated methods are callable remotely via MQTT and via
    ``device.handle_command(name, params)``.  They must be defined
    inside a :class:`Device` subclass.

    Example::

        class SmartLight(Device):
            power: bool = state(default=False, writable=True)

            @command
            async def toggle(self):
                self.power = not self.power
                return {"power": self.power}

            @command(name="switch_on")
            async def turn_on(self):
                self.power = True
    """

    def wrapper(f: Any) -> Any:
        f._is_command = True
        f._command_name = name or f.__name__
        return f

    if func is None:
        return wrapper
    return wrapper(func)


def rule(
    func: Any = None, *, fields: Optional[list] = None, description: Optional[str] = None
) -> Any:
    """
    Decorator to define a device-level automation rule.

    Decorated methods are automatically registered as rules when the device class is registered.
    They react to changes in the device's own fields.

    Example::

        class SmartLight(Device):
            power: bool = state(default=False, writable=True)

            @rule(fields=["power"])
            async def on_power_change(self, event: RuleEvent, app):
                if event.data.get("power"):
                    print("Light turned on")
    """

    def wrapper(f: Any) -> Any:
        f._is_rule = True
        f._rule_fields = fields
        f._rule_description = description
        return f

    if func is None:
        return wrapper
    return wrapper(func)


class Device(TelemetryMixin, metaclass=DeviceMeta):
    """
    Base class for all Kamio IoT devices.

    Subclass ``Device`` and annotate fields with :func:`state`,
    :func:`telemetry`, :func:`config`, or :func:`event` to describe
    your device.  Commands are decorated with :func:`command`.

    The metaclass :class:`DeviceMeta` collects field descriptors into
    ``Kamio_FIELDS``, ``Kamio_COMMANDS``, and ``Kamio_EVENTS``
    at class-definition time.

    Lifecycle (managed by :class:`KamioApp`):

    1. ``__init__`` — field defaults applied.
    2. ``on_init``  — async hook; driver ``connect()`` called if present.
    3. ``on_start`` — telemetry loop started; ``on_device_started`` hook fired.
    4. … normal operation …
    5. ``on_stop``  — driver disconnected; tasks cancelled.

    Example::

        class SmartLight(Device):
            power:      bool  = state(default=False, writable=True)
            brightness: int   = state(default=100, min=0, max=255)
            energy_wh:  float = telemetry(unit="Wh", freq="30s")

            @command
            async def toggle(self):
                self.power = not self.power
                return {"power": self.power}
    """

    Kamio_FIELDS: ClassVar[Dict[str, Field]]
    Kamio_COMMANDS: ClassVar[Dict[str, Any]]
    Kamio_EVENTS: ClassVar[Dict[str, Field]]
    _cached_schema: ClassVar[Optional[Dict[str, Any]]] = None

    def __init__(
        self, driver: Optional[BaseDriver] = None, keepalive_interval: float = 30.0, **kwargs
    ):
        """Initialize the device.

        Args:
            driver: Optional driver instance implementing :class:`BaseDriver`.
            keepalive_interval: Seconds between keepalive messages (default 30.0).
            **kwargs: Applied to matching state/config fields after defaults.
        """
        logger_name = f"Kamio.device.{self.device_type()}"
        super().__init__(logger_name=logger_name)

        self.node: Optional[DeviceNode] = None
        self.driver: Optional[BaseDriver] = driver
        self._app: Optional[KamioApp] = None
        self._keepalive_interval = keepalive_interval
        self._keepalive_task: Optional[asyncio.Task] = None

        # Injected by DeviceHandler to decouple Device from KamioApp internals.
        self._on_state_changed: Optional[Callable[[str, str, Any, Any], Awaitable[None]]] = None
        self._on_rules_trigger: Optional[Callable[[str, Dict[str, Any]], Awaitable[None]]] = None
        # Bounded set of our own published state cinds awaiting echo suppression.
        # Capped to avoid unbounded growth if echoes never arrive (e.g. broker
        # drops the message); oldest entries are evicted when the cap is hit.
        # Guarded by a lock because __setattr__ can fire from multiple
        # concurrent tasks setting different state fields.
        self._own_state_cinds: set[str] = set()
        self._own_state_cinds_order: list[str] = []
        self._own_state_cinds_limit = 4096
        self._cinds_lock = threading.Lock()

        # Apply default values for state and config fields
        self._apply_defaults()

        # Apply constructor kwargs to matching state/config fields.  This lets
        # callers pass field values directly to add_device() (e.g.
        # ``add_device("ctrl", Controller, target_light_id="hallway_light")``)
        # without overriding on_init just to read them.  Uses
        # object.__setattr__ to bypass validation/publication — the node is
        # not set up yet and these are initial values, not state changes.
        for key, value in kwargs.items():
            if key in self.Kamio_FIELDS:
                object.__setattr__(self, key, value)

    @property
    def app(self) -> KamioApp:
        """
        The :class:`KamioApp` this device is attached to.

        Raises:
            RuntimeError: If accessed before the device is registered with an app
                          (i.e. before :meth:`KamioApp.add_device` is called).
        """
        if self._app is None:
            raise RuntimeError(f"Device '{self.device_type()}' is not attached to any KamioApp.")
        return self._app

    @app.setter
    def app(self, value: KamioApp):
        """Set the KamioApp reference. Warns if re-attaching to a different app."""
        if self._app is not None and self._app is not value:
            self.logger.warning(
                f"Device {self.device_type()} is being re-attached to a different app"
            )
        self._app = value

    @classmethod
    def device_type(cls) -> str:
        """Return the lowercase class name used as device type identifier."""
        return cls.__name__.lower()

    def __setattr__(self, name: str, value: Any) -> None:
        """Validate and publish state field changes. Internal attributes and non-field names bypass validation."""
        # Internal attributes bypass field logic entirely.
        if name.startswith("_") or name in ("node", "driver", "logger"):
            object.__setattr__(self, name, value)
            return

        field = self.Kamio_FIELDS.get(name)
        if field is not None:
            # Validate all field kinds (state, telemetry, config) so direct
            # assignment honours min/max/choices just like handle_state().
            self._validate_value(field, value)
            if field.kind == "state":
                old = self.__dict__.get(name, field.default)
                self.__dict__[name] = value
                if old != value and hasattr(self, "node") and self.node:
                    env = Envelope.state(source=self.node.device_id, data={name: value})
                    with self._cinds_lock:
                        self._own_state_cinds.add(env.cind)
                        self._own_state_cinds_order.append(env.cind)
                        # Evict oldest entries if the echo-suppression cache is full.
                        while len(self._own_state_cinds_order) > self._own_state_cinds_limit:
                            old_cind = self._own_state_cinds_order.pop(0)
                            self._own_state_cinds.discard(old_cind)
                    coro = self.node.publish(env, retain=True)
                    try:
                        loop = asyncio.get_running_loop()
                        task = loop.create_task(coro)
                        self._bg_tasks.add(task)
                        task.add_done_callback(self._bg_tasks.discard)
                    except RuntimeError:
                        # No running event loop — cancel the coroutine to
                        # avoid an "coroutine was never awaited" warning and
                        # resource leak.
                        coro.close()
                        self.logger.warning(
                            f"Cannot publish state change for '{name}' outside "
                            f"an event loop; change applied locally only."
                        )
            else:
                # telemetry / config / event: store without MQTT publication.
                self.__dict__[name] = value
        else:
            object.__setattr__(self, name, value)

    def _set_state(self, data: Optional[dict] = None, **fields) -> None:
        """Update state fields silently without publishing to MQTT.

        Use this when mirroring state received from another device,
        to avoid re-publishing what was already published.

        Accepts either a dict or keyword arguments::

            self._set_state(event.data)
            self._set_state(power_status=True, brightness=80)
        """
        for name, value in (data or fields).items():
            object.__setattr__(self, name, value)

    def _apply_defaults(self):
        """Sets initial values for all fields from metadata."""
        for name, field in self.Kamio_FIELDS.items():
            if not hasattr(self, name):
                object.__setattr__(self, name, field.default)

    async def on_init(self, **kwargs):
        """Async initialization hook. Connects driver if present.

        Raises:
            Exception: If the driver fails to connect.  The caller (typically
                ``create_device``) must handle this so the device is not
                registered in a broken state.
        """
        if self.driver:
            try:
                await self.driver.connect()
            except Exception as e:
                self.logger.error(f"Driver connection failed: {e}")
                raise

    async def on_start(self, node: DeviceNode):
        """Called by the framework when the device node starts. Starts telemetry loop and keepalive."""
        if not node.is_running:
            return
        await self.start_telemetry()
        await self._start_keepalive()
        if self._app is not None:
            await self._app.hooks.trigger("on_device_started", self)

    async def on_stop(self, node: DeviceNode):
        """Called by the framework when the device node stops. Disconnects driver and cancels tasks."""
        await self.shutdown()
        if self._app is not None:
            await self._app.hooks.trigger("on_device_stopped", self)

    async def shutdown(self):
        """Forcefully shut down the device: disconnect driver and cancel all background tasks."""
        if self._keepalive_task and not self._keepalive_task.done():
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
        if self.driver:
            await self.driver.disconnect()
        await self.cancel_all_tasks()

    async def _start_keepalive(self):
        """Start the keepalive loop for this device."""
        if self._keepalive_interval <= 0:
            self.logger.debug("Keepalive disabled (interval <= 0)")
            return

        if not self.node:
            self.logger.warning("Cannot start keepalive: no node attached")
            return

        async def _keepalive_loop():
            device_id = self.node.device_id if self.node else "unknown"
            self.logger.info(
                f"Keepalive started for device {device_id} (interval: {self._keepalive_interval}s)"
            )

            while self.node and self.node.is_running:
                try:
                    await asyncio.sleep(self._keepalive_interval)

                    if not self.node or not self.node.is_running:
                        self.logger.warning(
                            f"Device {device_id}: node stopped, exiting keepalive loop"
                        )
                        break

                    # Send keepalive envelope
                    env = Envelope.keepalive(source=device_id)
                    await self._safe_publish(env)
                    self.logger.debug(f"Keepalive sent for device {device_id}")

                except asyncio.CancelledError:
                    self.logger.warning(f"Device {device_id}: keepalive loop cancelled")
                    break
                except Exception as e:
                    self.logger.error(f"Keepalive error for device {device_id}: {e}")
                    await asyncio.sleep(self._keepalive_interval)

            self.logger.info(f"Keepalive stopped for device {device_id}")

        self._keepalive_task = asyncio.create_task(_keepalive_loop())

    async def reinitialize(self):
        """Stop and restart the device in-place (e.g. after config change).

        Disconnects and reconnects the driver, then restarts telemetry and
        keepalive loops via ``on_stop`` / ``on_start``.

        If the driver fails to reconnect, ``on_start`` is **not** called —
        starting telemetry/keepalive on a device with a broken driver would
        produce a stream of errors.  The device remains stopped and the
        exception is re-raised so the caller can handle it.
        """
        if self.node:
            await self.on_stop(self.node)
            # Reconnect driver — on_start does not call on_init, so we must
            # reconnect the driver explicitly here.
            if self.driver:
                try:
                    await self.driver.connect()
                except Exception as e:
                    self.logger.error(f"Driver reconnection failed during reinitialize: {e}")
                    # Do NOT call on_start — telemetry/keepalive on a broken
                    # driver would spam errors.  Leave the device stopped.
                    raise
            await self.on_start(self.node)

    def _get_field_value(self, field_name: str) -> Any:
        """Return the current value of a field by name, or None if not found."""
        field = self.Kamio_FIELDS.get(field_name)
        if not field:
            return None
        return getattr(
            self, field_name, field.default if field.kind in ("state", "config") else None
        )

    def _validate_value(self, field: Field, value: Any) -> Any:
        """Validates field value. Raises ValueError on failure."""
        if field.choices and value not in field.choices:
            raise ValueError(
                f"Invalid value for {field.name}: {value}. Must be one of {field.choices}"
            )

        # Attempt numeric coercion for min/max validation so that string
        # values like "42" are still range-checked.  If the value cannot be
        # coerced to a number, the min/max check is skipped (the field's
        # own type coercion happens elsewhere).
        numeric_value: Optional[float] = None
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            numeric_value = float(value)
        elif isinstance(value, str):
            try:
                numeric_value = float(value)
            except ValueError:
                pass

        if field.min is not None and numeric_value is not None and numeric_value < field.min:
            raise ValueError(f"Value for {field.name} is too low: {value} < {field.min}")

        if field.max is not None and numeric_value is not None and numeric_value > field.max:
            raise ValueError(f"Value for {field.name} is too high: {value} > {field.max}")

        return value

    async def handle_state(self, data: dict) -> dict:
        """
        Apply a dict of state changes to this device.

        Only fields declared as ``state(writable=True)`` are applied.
        Unknown or non-writable fields are logged at DEBUG and skipped.
        After applying changes, matching automation rules are triggered
        automatically via :meth:`RuleEngine.handle_device_update`.

        Args:
            data: Mapping of field name → new value,
                  e.g. ``{"power": True, "brightness": 200}``.

        Returns:
            Dict of actually applied changes (subset of ``data``).

        Raises:
            ValueError: If a value fails min/max/choices validation.
        """
        applied_changes = {}
        device_id: Optional[str] = self.node.device_id if self.node else None
        validated: Dict[str, Any] = {}

        for key, value in data.items():
            field = self.Kamio_FIELDS.get(key)
            if not field:
                self.logger.debug(f"handle_state: unknown field '{key}' — ignored")
                continue
            if field.kind != "state" or not field.writable:
                self.logger.debug(
                    f"handle_state: field '{key}' (kind={field.kind!r}, writable={field.writable}) "
                    f"is not a writable state — ignored"
                )
                continue
            validated[key] = self._validate_value(field, value)

        for key, valid_value in validated.items():
            if self.driver:
                try:
                    await self.driver.execute(f"set_{key}", {"value": valid_value})
                except NotImplementedError:
                    # Driver does not implement set_*; fall through to in-memory update.
                    pass
                except Exception as e:
                    self.logger.error(f"Driver execution failed for {key}: {e}")
                    continue  # skip in-memory update if hardware rejected the change

            old_value = getattr(self, key, None)
            setattr(self, key, valid_value)

            # Only publish and trigger rules for actual changes.
            if old_value != valid_value:
                applied_changes[key] = valid_value
                if self._on_state_changed is not None and device_id is not None:
                    await self._on_state_changed(device_id, key, old_value, valid_value)

            # MQTT publication for state fields is handled by __setattr__.

        if applied_changes and device_id is not None and self._on_rules_trigger is not None:
            await self._on_rules_trigger(device_id, applied_changes)

        return applied_changes

    async def handle_config(self, data: dict) -> dict:
        """
        Apply a dict of configuration changes to this device.

        Only fields declared with :func:`config` are applied;
        others are silently ignored.

        Args:
            data: Mapping of config field name → new value.

        Returns:
            Dict of actually applied changes.

        Raises:
            ValueError: If a value fails validation constraints.
        """
        applied_changes = {}
        for key, value in data.items():
            field = self.Kamio_FIELDS.get(key)
            if field and field.kind == "config":
                valid_value = self._validate_value(field, value)
                setattr(self, key, valid_value)
                applied_changes[key] = valid_value
        return applied_changes

    def get_state_snapshot(self) -> Dict[str, Any]:
        """Return current values of all ``state`` fields."""
        return {
            n: self._get_field_value(n) for n, f in self.Kamio_FIELDS.items() if f.kind == "state"
        }

    def get_config_snapshot(self) -> Dict[str, Any]:
        """Return current values of all ``config`` fields."""
        return {
            n: self._get_field_value(n) for n, f in self.Kamio_FIELDS.items() if f.kind == "config"
        }

    def get_telemetry_snapshot(self) -> Dict[str, Any]:
        """Return current values of all ``telemetry`` fields."""
        return {
            n: self._get_field_value(n)
            for n, f in self.Kamio_FIELDS.items()
            if f.kind == "telemetry"
        }

    def get_full_snapshot(self) -> Dict[str, Any]:
        """Return current values of all fields (state + config + telemetry) merged."""
        return {
            **self.get_state_snapshot(),
            **self.get_config_snapshot(),
            **self.get_telemetry_snapshot(),
        }

    @staticmethod
    def _field_type_name(field: Field) -> str:
        """Return a human-readable type name for a Field's python_type."""
        python_type = field.python_type
        if python_type is None:
            return "None"
        return str(python_type.__name__) if hasattr(python_type, "__name__") else str(python_type)

    @classmethod
    def get_schema(cls) -> Dict[str, Any]:
        """
        Return the JSON-serialisable schema for this device type.

        Result is cached on the class after the first call.
        Includes fields, commands, and events with type/unit metadata.
        """
        if cls._cached_schema is not None:
            return cls._cached_schema

        cls._cached_schema = {
            "type": cls.device_type(),
            "fields": {
                n: {
                    "kind": f.kind,
                    "type": cls._field_type_name(f),
                    "default": f.default,
                    "writable": f.writable,
                    "freq": f.freq,
                    "unit": getattr(f, "unit", None),
                    "required": getattr(f, "required", False),
                }
                for n, f in cls.Kamio_FIELDS.items()
            },
            "commands": list(cls.Kamio_COMMANDS.keys()),
            "events": {
                n: {
                    "kind": f.kind,
                    "type": cls._field_type_name(f),
                    "unit": getattr(f, "unit", None),
                }
                for n, f in cls.Kamio_EVENTS.items()
            },
        }
        return cls._cached_schema

    async def emit(self, event_name: str, payload: dict) -> None:
        """
        Emit a device event over MQTT.

        Args:
            event_name: Name of the event field (must be declared with :func:`event`).
            payload:    Dict of data to send with the event.
        """
        if self.node:
            await self.node.emit_event(event_name, payload)

    async def send_command(
        self, target_device_id: str, method: str, params: dict, timeout: float = 10.0
    ) -> Envelope:
        """
        Send a command to another device via MQTT and await the ACK.

        Args:
            target_device_id: ID of the target device.
            method:           Command method name to call on the target device.
            params:           Parameters to pass to the command.
            timeout:          Timeout for waiting for acknowledgment (default 10.0s).

        Returns:
            The COMMAND_ACK envelope from the target device.
        """
        if not self.node:
            raise RuntimeError(f"Device '{self.device_type()}' has no node attached")
        return await self.app.server_node.call(
            target=target_device_id,
            method=method,
            params=params,
            timeout=timeout,
        )

    async def _safe_publish(self, env: Envelope):
        """TelemetryMixin publication implementation."""
        if not self.node:
            return
        try:
            await self.node.publish(env)
        except Exception as e:
            self.logger.error(f"Failed to publish {env.type}: {e}")

    async def _request_sync(self, snapshot: Dict[str, Any]) -> None:
        """Publish the given snapshot to MQTT if this device has a node."""
        if self.node:
            env = Envelope.state(source=self.node.device_id, data=snapshot)
            await self._safe_publish(env)

    async def request_state_sync(self) -> None:
        """Publish current state fields to MQTT immediately (out-of-band sync)."""
        await self._request_sync(self.get_state_snapshot())

    async def request_full_sync(self) -> None:
        """Publish all fields (state + config + telemetry) to MQTT immediately."""
        await self._request_sync(self.get_full_snapshot())

    def register_async_callback(self, topic: str, callback) -> None:
        """Register an async callback for a specific MQTT topic.

        gmqtt does not support per-topic message callbacks (unlike paho-mqtt).
        This method creates a lightweight CustomNode that subscribes to the
        given topic and forwards messages to the callback.

        Args:
            topic:    MQTT topic to subscribe to (absolute).
            callback: Async callback with signature ``(topic: str, payload: bytes)``.
        """
        if not self.node:
            raise RuntimeError("Device has no node attached")

        from kamio.core.custom_nodes import CustomNode

        device = self

        class _CallbackNode(CustomNode):
            def __init__(self, mqtt_client, topic_str: str, cb):
                super().__init__(mqtt_client, topic_str)
                self._cb = cb
                self._topic = topic_str

            async def start(self) -> None:
                self.subscribe_absolute(self._topic, qos=1)
                self._is_running = True

            async def stop(self) -> None:
                for t in self._subscriptions:
                    try:
                        self.mqtt_client.unsubscribe(t)
                    except Exception:
                        pass
                self._subscriptions.clear()
                self._is_running = False

            async def handle_message(self, topic: str, payload: bytes) -> None:
                import inspect

                if inspect.iscoroutinefunction(self._cb):
                    await self._cb(topic, payload)
                else:
                    self._cb(topic, payload)

        node_name = f"_cb_{id(callback)}"
        # Clean up any existing callback node for the same topic so that
        # re-registering doesn't leak old subscriptions.
        for existing_name in list(self.app.list_custom_nodes()):
            if not existing_name.startswith("_cb_"):
                continue
            existing_node = self.app.get_custom_node(existing_name)
            if existing_node and existing_node.topic_prefix == topic:
                self.app.unregister_custom_node(existing_name)
        node = _CallbackNode(self.node.mqtt, topic, callback)
        self.app.register_custom_node(node_name, node)
        # If the app is already running, start the node immediately.
        if self.app.is_running and self._app is not None:
            self._app._run_coro_threadsafe(node.start())
        self.logger.debug(f"Registered async callback for topic: {topic}")

    def unregister_async_callback(self, topic: str) -> None:
        """Unregister a previously registered async callback by topic.

        Finds and removes the CustomNode created by ``register_async_callback``.
        """
        if not self.node:
            return
        # Remove all callback nodes whose topic_prefix matches.
        for name in list(self.app.list_custom_nodes()):
            if not name.startswith("_cb_"):
                continue
            node = self.app.get_custom_node(name)
            if node and node.topic_prefix == topic:
                self.app.unregister_custom_node(name)
                self.logger.debug(f"Unregistered async callback for topic: {topic}")
                return

    async def handle_command(self, method_name: str, params: dict) -> Any:
        """
        Execute a device command by name.

        If a driver is attached, the command is forwarded to it first.
        Falls back to the method decorated with :func:`command` on the class.
        For writable state fields, ``set_<field>`` commands are auto-routed
        to :meth:`handle_state` for Home Assistant compatibility.
        Publishes a ``device_command_executed`` event on success.

        Args:
            method_name: Name of the command (matches the decorated method name).
            params:      Keyword arguments forwarded to the command function.

        Returns:
            Whatever the command function returns.

        Raises:
            AttributeError: If the command name is not registered.
            Exception:      Re-raises driver errors (after logging).
        """
        if self.driver:
            try:
                return await self.driver.execute(method_name, params)
            except NotImplementedError:
                pass
            except Exception as e:
                self.logger.error(f"Driver command execution failed: {e}")
                raise

        method = self.Kamio_COMMANDS.get(method_name)
        if not method:
            # Auto-route set_<field> commands to handle_state for HA compatibility.
            if method_name.startswith("set_"):
                field_name = method_name[4:]
                if field_name in self.Kamio_FIELDS:
                    field = self.Kamio_FIELDS[field_name]
                    if field.kind == "state" and field.writable:
                        value = params.get("value", params.get("field_value"))
                        return await self.handle_state({field_name: value})
            available = list(self.Kamio_COMMANDS.keys())
            raise AttributeError(
                f"Command '{method_name}' not found on {self.device_type()!r}. "
                f"Available commands: {available}"
            )

        if asyncio.iscoroutinefunction(method):
            result = await method(self, **params)
        else:
            result = method(self, **params)

        if self._app is not None:
            device_id = self.node.device_id if self.node else None
            await self._app.event_bus.publish(
                "device_command_executed",
                {
                    "device_id": device_id,
                    "command": method_name,
                    "params": params,
                    "result": result,
                },
            )
        return result

    async def handle_event(self, event_name: str, payload: dict) -> None:
        """
        Receive an incoming event directed at this device.

        Override in subclasses to react to inbound event messages.
        Default implementation is a no-op.
        """
        pass

    @classmethod
    def get_fields(
        cls,
        kind: str | None = None,
        writable: bool | None = None,
    ) -> Dict[str, Field]:
        """
        Return field descriptors filtered by kind and/or writability.

        Args:
            kind:     ``"state"``, ``"telemetry"``, or ``"config"``.
                      Pass ``None`` to skip kind filtering.
            writable: ``True`` / ``False`` to filter by writability.
                      Pass ``None`` to skip.

        Returns:
            A new dict ``{field_name: Field}``.
        """
        fields = cls.Kamio_FIELDS
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
        """Return all ``telemetry`` field descriptors."""
        return cls.get_fields(kind="telemetry")

    @classmethod
    def get_states(cls, writable: bool | None = None) -> Dict[str, Field]:
        """Return all ``state`` field descriptors, optionally filtered by writability."""
        return cls.get_fields(kind="state", writable=writable)

    @classmethod
    def get_commands(cls) -> Dict[str, Any]:
        """Return a copy of all registered command functions."""
        return cls.Kamio_COMMANDS.copy()
