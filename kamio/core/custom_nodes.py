from __future__ import annotations
import asyncio
import inspect
import logging
from abc import ABC, abstractmethod
from fnmatch import fnmatch
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from kamio.app import KamioApp

logger = logging.getLogger("Kamio.custom_nodes")


class CustomNode(ABC):
    """
    Abstract base class for custom MQTT nodes.

    A CustomNode subscribes to one or more MQTT topics under its `topic_prefix`
    and processes incoming messages. It can also publish messages back to the broker.

    Lifecycle:
        1. Register with app.register_custom_node(name, node)
        2. start() is called when KamioApp.start() runs (or immediately if already running)
        3. handle_message() is called for every matching MQTT message
        4. stop() is called when KamioApp.stop() runs

    Subclass must implement: start(), stop(), handle_message()
    """

    def __init__(self, mqtt_client, topic_prefix: str) -> None:
        self.mqtt_client = mqtt_client
        self.topic_prefix = topic_prefix.rstrip("/")
        self._subscriptions: List[str] = []
        self._is_running: bool = False
        self.logger = logging.getLogger(f"Kamio.node.{self.__class__.__name__}")

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    async def start(self) -> None:
        """Called when the node is started. Subscribe to topics here."""

    @abstractmethod
    async def stop(self) -> None:
        """Called when the node is stopped. Clean up resources here."""
        for topic in self._subscriptions:
            try:
                self.mqtt_client.unsubscribe(topic)
            except Exception as e:
                self.logger.warning(f"Failed to unsubscribe from {topic}: {e}")
        self._subscriptions.clear()

    @abstractmethod
    async def handle_message(self, topic: str, payload: bytes) -> None:
        """
        Called for every MQTT message whose topic starts with topic_prefix.

        Args:
            topic: Full MQTT topic string.
            payload: Raw message payload bytes.
        """

    # ------------------------------------------------------------------
    # Optional hooks
    # ------------------------------------------------------------------

    async def on_connect(self) -> None:
        """Called after MQTT broker connection is established."""

    async def on_disconnect(self) -> None:
        """Called after MQTT broker disconnection."""

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------

    def _resolve_topic(self, topic: str, absolute: bool) -> str:
        """Return absolute topic as-is or prefix a relative topic."""
        if absolute:
            return topic
        return f"{self.topic_prefix}/{topic}" if topic else self.topic_prefix

    @staticmethod
    def _encode_payload(payload: Any) -> Any:
        """Encode string payloads to bytes; leave other types unchanged."""
        return payload.encode() if isinstance(payload, str) else payload

    def subscribe(self, topic: str, qos: int = 0) -> None:
        """
        Subscribe to a topic relative to topic_prefix.

        E.g. subscribe("cmd/#") → subscribes to "<prefix>/cmd/#"
        """
        full_topic = self._resolve_topic(topic, absolute=False)
        self.mqtt_client.subscribe(full_topic, qos)
        self._subscriptions.append(full_topic)
        self.logger.debug(f"Subscribed: {full_topic}")

    def subscribe_absolute(self, topic: str, qos: int = 0) -> None:
        """Subscribe to an absolute topic (no prefix prepended)."""
        self.mqtt_client.subscribe(topic, qos)
        self._subscriptions.append(topic)
        self.logger.debug(f"Subscribed (absolute): {topic}")

    def publish(self, topic: str, payload: Any, qos: int = 0, retain: bool = False) -> None:
        """
        Publish to a topic relative to topic_prefix.

        Payload is encoded to bytes if it is a str.
        Note: this calls the underlying MQTT client synchronously; for a
        non-blocking variant from async code use ``publish_async``.
        """
        full_topic = self._resolve_topic(topic, absolute=False)
        self.mqtt_client.publish(
            full_topic,
            self._encode_payload(payload),
            qos=qos,
            retain=retain,
        )

    async def publish_async(
        self, topic: str, payload: Any, qos: int = 0, retain: bool = False
    ) -> None:
        """Non-blocking variant of :meth:`publish` (runs in a thread pool)."""
        full_topic = self._resolve_topic(topic, absolute=False)
        await asyncio.to_thread(
            self.mqtt_client.publish,
            full_topic,
            self._encode_payload(payload),
            qos=qos,
            retain=retain,
        )

    def publish_absolute(
        self, topic: str, payload: Any, qos: int = 0, retain: bool = False
    ) -> None:
        """Publish to an absolute topic (no prefix prepended)."""
        self.mqtt_client.publish(
            topic,
            self._encode_payload(payload),
            qos=qos,
            retain=retain,
        )

    def matches(self, topic: str) -> bool:
        """Return True if topic starts with this node's topic_prefix."""
        return topic == self.topic_prefix or topic.startswith(self.topic_prefix + "/")

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} prefix={self.topic_prefix!r} running={self._is_running}>"
        )


class CustomNodeManager:
    """
    Manages registration, lifecycle and message routing for CustomNode instances.

    Integrated into KamioApp via app.custom_nodes.
    Nodes receive messages routed from _on_mqtt_message before standard device nodes.
    """

    def __init__(self, app: "KamioApp") -> None:
        self._app = app
        self._nodes: Dict[str, CustomNode] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_node(self, name: str, node: CustomNode) -> None:
        """
        Register a custom node.

        Args:
            name: Unique identifier for the node.
            node: CustomNode instance.

        Raises:
            ValueError: If a node with the same name is already registered.
        """
        if name in self._nodes:
            raise ValueError(f"Custom node '{name}' is already registered")
        self._nodes[name] = node
        logger.info(f"Registered custom node: '{name}' prefix={node.topic_prefix!r}")

    def unregister_node(self, name: str) -> None:
        """
        Unregister a node by name.
        Safe if the node is not found (logs a warning).
        """
        if name not in self._nodes:
            logger.warning(f"unregister_node: '{name}' not found")
            return
        del self._nodes[name]
        logger.info(f"Unregistered custom node: '{name}'")

    def get_node(self, name: str) -> Optional[CustomNode]:
        """Return a registered node by name, or None."""
        return self._nodes.get(name)

    def list_nodes(self) -> List[str]:
        """Return names of all registered nodes."""
        return list(self._nodes.keys())

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start_all(self) -> None:
        """Start all registered nodes. Errors in one node don't stop others."""
        for name, node in self._nodes.items():
            try:
                await node.start()
            except Exception as e:
                logger.error(f"Failed to start custom node '{name}': {e}", exc_info=True)
                await self._app.event_bus.publish(
                    "custom_node_error",
                    {
                        "node_name": name,
                        "error": str(e),
                        "phase": "start",
                    },
                )
                continue
            node._is_running = True
            logger.info(f"Started custom node: '{name}'")
            await self._app.event_bus.publish(
                "custom_node_started",
                {
                    "node_name": name,
                    "topic_prefix": node.topic_prefix,
                },
            )

    async def stop_all(self) -> None:
        """Stop all registered nodes in reverse registration order."""
        for name, node in reversed(list(self._nodes.items())):
            if not node._is_running:
                continue
            try:
                await node.stop()
                node._is_running = False
                logger.info(f"Stopped custom node: '{name}'")
                await self._app.event_bus.publish(
                    "custom_node_stopped",
                    {
                        "node_name": name,
                    },
                )
            except Exception as e:
                logger.error(f"Failed to stop custom node '{name}': {e}", exc_info=True)

    # ------------------------------------------------------------------
    # Message routing
    # ------------------------------------------------------------------

    async def route_message(self, topic: str, payload: bytes) -> bool:
        """
        Route an MQTT message to every matching registered node.

        Returns True if at least one node handled the message.
        """
        handled = False
        for name, node in self._nodes.items():
            if node.matches(topic):
                try:
                    await node.handle_message(topic, payload)
                    handled = True
                except Exception as e:
                    logger.error(
                        f"Error in custom node '{name}' handle_message: {e}", exc_info=True
                    )
                    await self._app.event_bus.publish(
                        "custom_node_error",
                        {
                            "node_name": name,
                            "error": str(e),
                            "phase": "handle_message",
                            "topic": topic,
                        },
                    )
        return handled
