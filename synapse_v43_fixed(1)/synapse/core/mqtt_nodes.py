from __future__ import annotations
import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional, Coroutine, TYPE_CHECKING

from . import topics
from .envelope import Envelope, EnvelopeType, SERVER_ID

if TYPE_CHECKING:
    from .state import StateManager
    from .correlation import CommandManager

logger = logging.getLogger("synapse.nodes")

BROADCAST_ID = "all"
MAX_BATCH_SIZE = 100

class BaseNode:
    """Base node with lifecycle support."""
    def __init__(self, device_id: str, mqtt_client: Any):
        self.device_id = str(device_id)
        self.mqtt = mqtt_client
        self._handlers: Dict[EnvelopeType, Callable[[Envelope], Coroutine[Any, Any, None]]] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._is_running = False

    async def start(self):
        """Start node: subscribe to topics."""
        if self._is_running:
            return
        self._loop = asyncio.get_running_loop()
        self.mqtt.subscribe(f"{topics.PREFIX}/{self.device_id}/#")
        self.mqtt.subscribe(f"{topics.PREFIX}/{BROADCAST_ID}/#")
        self._is_running = True
        logger.info(f"Node {self.device_id} started")

    async def stop(self):
        """Graceful shutdown: unsubscribe and stop."""
        if not self._is_running: return
        self.mqtt.unsubscribe(f"{topics.PREFIX}/{self.device_id}/#")
        self.mqtt.unsubscribe(f"{topics.PREFIX}/{BROADCAST_ID}/#")
        self._is_running = False
        logger.info(f"Node {self.device_id} stopped")

    @property
    def is_running(self) -> bool:
        return self._is_running

    def _on_mqtt_message_callback(self, client, userdata, msg):
        """Safely transfer message to event loop."""
        if not self._is_running: return
        self._loop.call_soon_threadsafe(
            lambda: asyncio.create_task(self._handle_message(msg.payload))
        )

    async def _handle_message(self, payload: bytes):
        """Asynchronous processing of incoming message."""
        env = Envelope.from_json(payload)
        if not env: return

        handler = self._handlers.get(env.type)
        if handler:
            try:
                await handler(env)
            except Exception as e:
                logger.exception(f"Handler error for {env.type}: {e}")
        else:
            logger.debug(f"Unhandled message type: {env.type}")

    def on(self, msg_type: EnvelopeType, handler: Callable[[Envelope], Coroutine[Any, Any, None]]):
        """Register asynchronous handler with type validation."""
        if not isinstance(msg_type, EnvelopeType):
            raise TypeError("msg_type must be an instance of EnvelopeType")
        self._handlers[msg_type] = handler

    async def publish(self, env: Envelope, qos: int = 1, retain: bool = False):
        """Send message using centralized topics."""
        target = env.target if env.target else BROADCAST_ID
        try:
            topic_func = topics.get_topic_func(env.type)
            if topic_func:
                topic = topic_func(target)
            else:
                topic = f"{topics.PREFIX}/{target}/{env.type.value}"

            self.mqtt.publish(topic, env.to_json(), qos=qos, retain=retain)
        except Exception as e:
            logger.error(f"Publish error: {e}")

class ServerNode(BaseNode):
    """Server node with state and command manager support."""
    def __init__(
        self,
        mqtt_client: Any,
        state_manager: Optional[StateManager] = None,
        command_manager: Optional[CommandManager] = None
    ):
        super().__init__(SERVER_ID, mqtt_client)
        self.state_manager = state_manager
        self.command_manager = command_manager

    async def _handle_message(self, payload: bytes):
        env = Envelope.from_json(payload)
        if not env: return

        # 1. Let StateManager handle incoming data (ds, sa, dt)
        if self.state_manager:
            await self.state_manager.handle_incoming(env)
            if env.type == EnvelopeType.DEVICE_TELEMETRY:
                self.state_manager.update_from_telemetry(env.source, env.data)

        # 2. Let CommandManager handle acknowledgments (ca)
        if self.command_manager and env.type == EnvelopeType.COMMAND_ACK:
            if self.command_manager.handle_ack(env):
                return

        # 3. Base processing for registered handlers
        await super()._handle_message(payload)

    async def set_state(self, device_id: str, data: Dict[str, Any], timeout: float = 10.0) -> Dict[str, Any]:
        """Change state via StateManager."""
        if not self.state_manager:
            raise RuntimeError("StateManager not initialized in ServerNode")

        return await self.state_manager.set_state(
            device_id=device_id,
            data=data,
            publish_func=self.publish,
            source_id=self.device_id,
            timeout=timeout
        )

    async def call(self, target: str, method: str, params: dict, timeout: float = 10.0) -> Envelope:
        """RPC call via CommandManager."""
        if not self.command_manager:
            raise RuntimeError("CommandManager not initialized in ServerNode")

        return await self.command_manager.send_command(
            target=target,
            method=method,
            params=params,
            publish_func=self.publish,
            source_id=self.device_id,
            timeout=timeout
        )

class DeviceNode(BaseNode):
    """Thin transport node for a device."""
    def __init__(self, device_id: str, mqtt_client: Any):
        super().__init__(device_id, mqtt_client)
        self._handler: Optional[Callable[[Envelope], Coroutine[Any, Any, None]]] = None

    def set_handler(self, handler: Callable[[Envelope], Coroutine[Any, Any, None]]):
        """Sets external message handler (e.g., DeviceHandler)."""
        self._handler = handler

    async def start(self):
        await super().start()
        if self._handler and hasattr(self._handler, 'device'):
            await self._handler.device.on_start(self)

    async def stop(self):
        if self._handler and hasattr(self._handler, 'device'):
            await self._handler.device.on_stop(self)
        await super().stop()

    async def _handle_message(self, payload: bytes):
        """Redirects all messages to external handler if present."""
        env = Envelope.from_json(payload)
        if not env:
            return

        if self._handler:
            try:
                await self._handler(env)
            except Exception as e:
                logger.exception(f"Handler error in DeviceNode {self.device_id}: {e}")
        else:
            await super()._handle_message(payload)

    async def emit_event(self, event_name: str, data: dict, qos: int = 1):
        env = Envelope.event(source=self.device_id, event_name=event_name, data=data)
        await self.publish(env, qos=qos)

    async def respond(self, request_env: Envelope, result: dict):
        env = Envelope.command_ack(source=self.device_id, target=request_env.source, data=result, cind=request_env.cind)
        await self.publish(env, qos=1)

class GatewayNode(BaseNode):
    """Transit node with aggregation and limits."""
    def __init__(self, device_id: str, mqtt_client: Any):
        super().__init__(device_id, mqtt_client)
        self._buffer: List[Envelope] = []
        self._bg_tasks: List[asyncio.Task] = []

    async def forward(self, env: Envelope, new_target: str):
        env.target = str(new_target)
        await self.publish(env, qos=1)

    def run_every(self, seconds: float, callback: Callable[[], Coroutine[Any, Any, None]]):
        async def _loop():
            while self._is_running:
                await asyncio.sleep(seconds)
                try: await callback()
                except Exception as e: logger.error(f"BG Task error: {e}")

        task = self._loop.create_task(_loop())
        self._bg_tasks.append(task)
        return task

    def add_to_batch(self, env: Envelope):
        if len(self._buffer) < MAX_BATCH_SIZE:
            self._buffer.append(env)
        else:
            logger.warning("Gateway buffer full, dropping message")

    async def flush_batch(self, target: str = SERVER_ID):
        if not self._buffer: return
        batch_data = [e.to_dict() for e in self._buffer]
        env = Envelope(source=self.device_id, target=target, type=EnvelopeType.BATCH, data={"items": batch_data})
        await self.publish(env, qos=1)
        self._buffer.clear()

    async def stop(self):
        for t in self._bg_tasks: t.cancel()
        if self._bg_tasks:
            await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        await super().stop()
