from __future__ import annotations
import asyncio
import logging
from typing import Any, Callable, Dict, Optional, Coroutine, TYPE_CHECKING

from . import topics
from .envelope import Envelope, EnvelopeType, SERVER_ID

if TYPE_CHECKING:
    from .state import StateManager
    from .correlation import CommandManager

logger = logging.getLogger("Kamio.nodes")

BROADCAST_ID = "all"


class BaseNode:
    """Base node with lifecycle support."""

    def __init__(self, device_id: str, mqtt_client: Any):
        self.device_id = str(device_id)
        self.mqtt = mqtt_client
        self._handlers: Dict[EnvelopeType, Callable[[Envelope], Coroutine[Any, Any, None]]] = {}
        self._tasks: set[asyncio.Task] = set()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._is_running = False


    async def start(self):
        """Subscribe to MQTT topics and mark node as running.

        Subscribes to ``Kamio/v1/{device_id}/#`` and the broadcast wildcard.
        The legacy ``Kamio/{device_id}/{type}`` format is parsed by
        :func:`topics.parse` if a message is received, but the node only
        subscribes to the current versioned format.
        """
        if self._is_running:
            return

        self._loop = asyncio.get_running_loop()

        topics_to_sub = [
            f"{topics.BASE}/{self.device_id}/#",
            f"{topics.BASE}/{BROADCAST_ID}/#",
        ]

        node_logger = getattr(self, "logger", logger)
        for t in topics_to_sub:
            try:
                mid = self.mqtt.subscribe(t, qos=1)
                await self.mqtt._kamio_wait_for_suback(mid)
                node_logger.debug(f"Subscribed to: {t}")
            except Exception as e:
                node_logger.error(f"Failed to subscribe to {t}: {e}")

        self._is_running = True
        logger.info(f"Node {self.device_id} started successfully")

    async def stop(self):
        """Graceful shutdown: unsubscribe and stop."""
        if not self._is_running:
            return
        for task in list(self._tasks):
            if not task.done():
                task.cancel()
        self._tasks.clear()
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        try:
            mid = self.mqtt.unsubscribe(f"{topics.BASE}/{self.device_id}/#")
            await self.mqtt._kamio_wait_for_unsuback(mid)
        except Exception as e:
            logger.error(f"Failed to unsubscribe from {self.device_id}: {e}")
        try:
            mid = self.mqtt.unsubscribe(f"{topics.BASE}/{BROADCAST_ID}/#")
            await self.mqtt._kamio_wait_for_unsuback(mid)
        except Exception as e:
            logger.error(f"Failed to unsubscribe from {BROADCAST_ID}: {e}")
        self._is_running = False
        logger.info(f"Node {self.device_id} stopped")

    @property
    def is_running(self) -> bool:
        return self._is_running

    def dispatch(self, topic: str, payload: bytes) -> None:
        """Public entry point for routing an MQTT message to this node."""
        if not self._is_running or not self._loop:
            return

        dev_id, msg_type = topics.parse(topic)

        if dev_id != self.device_id and dev_id != BROADCAST_ID:
            return

        node_logger = getattr(self, "logger", logger)
        node_logger.info(f"Message received for {self.device_id}: {topic}, type={msg_type}")

        try:
            task = self._loop.create_task(self._handle_message(payload))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
        except RuntimeError:
            pass  # event loop closed during shutdown

    async def _handle_message(self, payload: bytes):
        """Asynchronous processing of incoming message."""
        env = Envelope.from_json(payload)
        if not env:
            return

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

    def _build_topic(self, env: Envelope) -> str:
        """Resolve the MQTT topic for a given envelope."""
        target = env.target if env.target else env.source
        topic_func = topics.get_topic_func(env.type)
        if topic_func:
            return topic_func(target)
        return f"{topics.PREFIX}/{target}/{env.type.value}"

    async def publish(self, env: Envelope, qos: int = 1, retain: bool = False):
        """Send message using centralized topics."""
        await self.publish_raw(
            self._build_topic(env), env.to_json().encode(), qos=qos, retain=retain
        )

    async def publish_raw(self, topic: str, payload: bytes, qos: int = 1, retain: bool = False):
        """Publish raw bytes to a specific topic (non-blocking)."""
        try:
            self.mqtt.publish(topic, payload, qos=qos, retain=retain)
        except RuntimeError as e:
            if "shutdown" in str(e).lower() or "closed" in str(e).lower():
                return  # normal during app stop
            logger.error(f"Publish error on {topic}: {e}")
        except Exception as e:
            logger.error(f"Publish error on {topic}: {e}")


class ServerNode(BaseNode):
    """Server node with state and command manager support."""

    def __init__(
        self,
        mqtt_client: Any,
        state_manager: Optional[StateManager] = None,
        command_manager: Optional[CommandManager] = None,
        device_id: Optional[str] = None,
    ):
        super().__init__(device_id or SERVER_ID, mqtt_client)
        self.state_manager = state_manager
        self.command_manager = command_manager

    async def _handle_message(self, payload: bytes):
        env = Envelope.from_json(payload)
        if not env:
            return

        if self.state_manager:
            await self.state_manager.handle_incoming(env)
            if env.type == EnvelopeType.DEVICE_TELEMETRY:
                self.state_manager.update_state(env.source, env.data)

        if self.command_manager and env.type == EnvelopeType.COMMAND_ACK:
            if self.command_manager.handle_ack(env):
                return

        handler = self._handlers.get(env.type)
        if handler:
            try:
                await handler(env)
            except Exception as e:
                logger.exception(f"Handler error for {env.type}: {e}")
        else:
            logger.debug(f"Unhandled message type: {env.type}")

    async def set_state(
        self, device_id: str, data: Dict[str, Any], timeout: float = 10.0
    ) -> Dict[str, Any]:
        """Change state via StateManager."""
        if not self.state_manager:
            raise RuntimeError("StateManager not initialized in ServerNode")

        return await self.state_manager.set_state(
            device_id=device_id,
            data=data,
            publish_func=self.publish,
            source_id=self.device_id,
            timeout=timeout,
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
            timeout=timeout,
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
        if self._handler and hasattr(self._handler, "device"):
            await self._handler.device.on_start(self)

    async def stop(self):
        if self._handler and hasattr(self._handler, "device"):
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
        """Publish a DEVICE_EVENT envelope for this device."""
        env = Envelope.event(source=self.device_id, event_name=event_name, data=data)
        await self.publish(env, qos=qos)

    async def respond(self, request_env: Envelope, result: dict):
        """Send a COMMAND_ACK back to the requester."""
        env = Envelope.command_ack(
            source=self.device_id, target=request_env.source, data=result, cind=request_env.cind
        )
        await self.publish(env, qos=1)
