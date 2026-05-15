from __future__ import annotations
import asyncio
import logging
from typing import Optional, TYPE_CHECKING

from synapse.core.envelope import Envelope, EnvelopeType

if TYPE_CHECKING:
    from ..device import Device
    from .mqtt_nodes import DeviceNode
    from .state import StateManager

logger = logging.getLogger("synapse.handlers")

class DeviceHandler:
    """
    Handles incoming envelopes for a specific device.
    """
    def __init__(self, device: Device, node: DeviceNode, state_manager: Optional[StateManager] = None, debug: bool = False):
        self.device = device
        self.node = node
        self.state_manager = state_manager
        self.debug = debug
        self.logger = logging.getLogger(f"synapse.handler.{node.device_id}")

    async def __call__(self, env: Envelope):
        try:
            if env.type == EnvelopeType.SERVER_COMMAND:
                await self._handle_command(env)
            elif env.type == EnvelopeType.DEVICE_STATE:
                await self._handle_state(env)
            elif env.type == EnvelopeType.STATE_ACK:
                await self._handle_state_ack(env)
            elif env.type == EnvelopeType.DEVICE_EVENT:
                await self._handle_event(env)
            elif env.type == EnvelopeType.DEVICE_CONFIG:
                await self._handle_config(env)
            elif env.type == EnvelopeType.DEVICE_TELEMETRY:
                await self._handle_telemetry(env)
        except Exception as e:
            self.logger.exception(f"Error handling {env.type}: {e}")
            if self.debug: raise
            await self.send_error(env, str(e))

    async def _handle_command(self, env: Envelope):
        method_name = env.data.get("method", "")
        params = env.data.get("params", {})
        self.logger.debug(f"Executing command: {method_name}")

        try:
            method = self.device.SYNAPSE_COMMANDS.get(method_name)
            if method:
                # Check if method accepts 'node' argument
                import inspect
                sig = inspect.signature(method)
                kwargs = params.copy()
                if 'node' in sig.parameters:
                    kwargs['node'] = self.node
                
                if asyncio.iscoroutinefunction(method):
                    result = await method(self.device, **kwargs)
                else:
                    result = method(self.device, **kwargs)
            else:
                result = await self.device.handle_command(method_name, params)

            await self.send_ack(env, result={"result": result or {}}, response_type=EnvelopeType.COMMAND_ACK)
        except Exception as e:
            if self.debug: raise
            self.logger.exception(f"Command {method_name} failed")
            await self.send_error(env, str(e))

    async def _handle_state(self, env: Envelope):
        try:
            if self.state_manager:
                await self.state_manager.handle_incoming(env)

            applied = await self.device.handle_state(env.data)

            # Trigger rules if app is available
            if self.device.app:
                await self.device.app.rules.handle_device_update(self.node.device_id, self.device.get_full_snapshot())

            await self.send_ack(env, result={"result": applied}, response_type=EnvelopeType.STATE_ACK)
        except ValueError as e:
            await self.send_error(env, f"State validation failed: {e}")

    async def _handle_state_ack(self, env: Envelope):
        if self.state_manager:
            await self.state_manager.handle_incoming(env)

    async def _handle_config(self, env: Envelope):
        try:
            applied = await self.device.handle_config(env.data)
            await self.send_ack(env, result={"result": applied}, response_type=EnvelopeType.COMMAND_ACK)
        except ValueError as e:
            await self.send_error(env, f"Config validation failed: {e}")

    async def _handle_event(self, env: Envelope):
        await self.device.handle_event(env.data.get("event", ""), env.data.get("payload", {}))

    async def _handle_telemetry(self, env: Envelope):
        if self.state_manager:
            self.state_manager.update_from_telemetry(env.source, env.data)

        # Trigger rules if app is available
        if self.device.app:
            await self.device.app.rules.handle_device_update(self.node.device_id, env.data)

    async def send_ack(self, original_env: Envelope, result: Optional[dict] = None, status: str = "ok", response_type: Optional[EnvelopeType] = None, meta: Optional[dict] = None):
        data = result or {}
        data["status"] = status
        final_type = response_type
        if not final_type:
            final_type = EnvelopeType.COMMAND_ACK
            if original_env.type == EnvelopeType.DEVICE_STATE:
                final_type = EnvelopeType.STATE_ACK
        env = Envelope(source=self.node.device_id, target=original_env.source, type=final_type, data=data, cind=original_env.cind, meta=meta)
        await self.node.publish(env)

    async def send_error(self, original_env: Envelope, error_msg: str):
        await self.send_ack(original_env, result={"error": error_msg}, status="error")
