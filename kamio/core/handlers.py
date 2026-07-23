from __future__ import annotations
import asyncio
import inspect
import logging
from typing import Any, Dict, Optional, TYPE_CHECKING

from kamio.core.envelope import Envelope, EnvelopeType

if TYPE_CHECKING:
    from ..device import Device
    from .mqtt_nodes import DeviceNode
    from .state import StateManager

logger = logging.getLogger("Kamio.handlers")


class DeviceHandler:
    """
    Routes and handles incoming MQTT envelopes for a single device instance.

    Created by :class:`DeviceRegistryMixin` for each registered device.
    Dispatches to :meth:`Device.handle_command`, :meth:`Device.handle_state`,
    :meth:`Device.handle_config`, and :meth:`Device.handle_event` based on
    the :class:`EnvelopeType`.

    Also injects two async callbacks into the device on construction so that
    :class:`Device` does not need to import or reference :class:`KamioApp`:

    - ``device._on_state_changed`` — publishes ``device_state_changed`` events.
    - ``device._on_rules_trigger`` — triggers matching automation rules.
    """

    def __init__(
        self,
        device: Device,
        node: DeviceNode,
        state_manager: Optional[StateManager] = None,
        debug: bool = False,
    ):
        self.device = device
        self.node = node
        self.state_manager = state_manager
        self.debug = debug
        self.logger = logging.getLogger(f"Kamio.handler.{node.device_id}")

        app = getattr(device, "_app", None)
        if app is not None:
            async def _on_state_changed(device_id: str, field: str, old_val: Any, new_val: Any) -> None:
                await app.event_bus.publish("device_state_changed", {
                    "device_id": device_id,
                    "field": field,
                    "old_value": old_val,
                    "new_value": new_val,
                })

            async def _on_rules_trigger(device_id: str, changes: Dict[str, Any]) -> None:
                await app.rules.handle_device_update(device_id, changes)

            device._on_state_changed = _on_state_changed
            device._on_rules_trigger = _on_rules_trigger

        self._handlers = {
            EnvelopeType.SERVER_COMMAND: self._handle_command,
            EnvelopeType.DEVICE_STATE: self._handle_state,
            EnvelopeType.STATE_ACK: self._handle_state_ack,
            EnvelopeType.DEVICE_EVENT: self._handle_event,
            EnvelopeType.DEVICE_CONFIG: self._handle_config,
            EnvelopeType.DEVICE_TELEMETRY: self._handle_telemetry,
        }

    async def __call__(self, env: Envelope):
        """Dispatch an incoming envelope to the appropriate handler method."""
        try:
            handler = self._handlers.get(env.type)
            if handler is not None:
                await handler(env)
        except Exception as e:
            self.logger.exception(f"Error handling {env.type}: {e}")
            if self.debug:
                raise
            await self.send_error(env, str(e))

    async def _handle_command(self, env: Envelope):
        """Execute a SERVER_COMMAND and reply with COMMAND_ACK (or error)."""
        method_name = env.data.get("method", "")
        params = env.data.get("params", {})
        self.logger.debug(
            f"Command '{method_name}' params={params} on device '{self.node.device_id}'"
        )

        try:
            method = self.device.Kamio_COMMANDS.get(method_name)
            if method is not None:
                sig = inspect.signature(method)
                extra: Dict[str, Any] = {}
                if "node" in sig.parameters:
                    extra["node"] = self.node
                if "app" in sig.parameters:
                    extra["app"] = getattr(self.device, "_app", None)
                if extra:
                    params = {**params, **extra}  # merge injected kwargs

            result = await self.device.handle_command(method_name, params)
            await self.send_ack(
                env, result={"result": result or {}}, response_type=EnvelopeType.COMMAND_ACK
            )
        except Exception as e:
            if self.debug:
                raise
            self.logger.error(
                f"Command '{method_name}' failed on device '{self.node.device_id}': {e}",
                exc_info=True,
            )
            await self.send_error(env, str(e))

    async def _handle_state(self, env: Envelope):
        """Apply a DEVICE_STATE update and reply with STATE_ACK (or error)."""
        if env.cind in self.device._own_state_cinds:
            # Our own echo; already applied locally, no need to re-apply.
            self.device._own_state_cinds.discard(env.cind)
            return
        try:
            applied = await self.device.handle_state(env.data)

            if not applied:
                # Nothing changed (e.g. duplicate update);
                # no need to sync state back or acknowledge.
                return

            if self.state_manager:
                await self.state_manager.handle_incoming(env)

            await self.send_ack(
                env, result={"result": applied}, response_type=EnvelopeType.STATE_ACK
            )
        except ValueError as e:
            self.logger.warning(f"State validation failed for device '{self.node.device_id}': {e}")
            await self.send_error(env, f"State validation failed: {e}")

    async def _handle_state_ack(self, env: Envelope):
        """Forward a STATE_ACK to StateManager to resolve pending corr-id waiter."""
        if self.state_manager:
            await self.state_manager.handle_incoming(env)

    async def _handle_config(self, env: Envelope):
        """Apply a DEVICE_CONFIG update and reply with COMMAND_ACK (or error)."""
        try:
            applied = await self.device.handle_config(env.data)
            await self.send_ack(
                env, result={"result": applied}, response_type=EnvelopeType.COMMAND_ACK
            )
        except ValueError as e:
            self.logger.warning(f"Config validation failed for device '{self.node.device_id}': {e}")
            await self.send_error(env, f"Config validation failed: {e}")

    async def _handle_event(self, env: Envelope):
        """Forward a DEVICE_EVENT to the device's handle_event hook."""
        await self.device.handle_event(env.data.get("event", ""), env.data.get("payload", {}))

    async def _handle_telemetry(self, env: Envelope):
        """Mirror telemetry into StateManager and trigger rules for own messages.

        Rules are only triggered when the telemetry originates from this device.
        Broadcast telemetry from other devices is stored in StateManager but
        does not fire this device's automation rules.
        """
        if self.state_manager:
            self.state_manager.update_state(env.source, env.data)

        app = getattr(self.device, "_app", None)
        if app:
            if env.source == self.node.device_id:
                await app.rules.handle_device_update(env.source, env.data)
            else:
                self.logger.debug(
                    f"Telemetry from '{env.source}' skipped rule-trigger "
                    f"(not this device '{self.node.device_id}')"
                )

    async def send_ack(
        self,
        original_env: Envelope,
        result: Optional[Dict[str, Any]] = None,
        status: str = "ok",
        response_type: Optional[EnvelopeType] = None,
        meta: Optional[Dict[str, Any]] = None,
    ):
        """Publish an acknowledgement envelope back to the sender."""
        data = result or {}
        data["status"] = status
        final_type = response_type
        if not final_type:
            final_type = EnvelopeType.COMMAND_ACK
            if original_env.type == EnvelopeType.DEVICE_STATE:
                final_type = EnvelopeType.STATE_ACK
        env = Envelope(
            source=self.node.device_id,
            target=original_env.source,
            type=final_type,
            data=data,
            cind=original_env.cind,
            meta=meta or {},
        )
        await self.node.publish(env)

    async def send_error(self, original_env: Envelope, error_msg: str):
        """Publish an error acknowledgement back to the sender."""
        await self.send_ack(original_env, result={"error": error_msg}, status="error")
