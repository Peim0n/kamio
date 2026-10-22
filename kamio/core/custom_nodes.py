from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from fnmatch import fnmatch
from functools import partial
from typing import TYPE_CHECKING, Any, Dict, List, Optional

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
        """Initialize the custom node.

        Args:
            mqtt_client: gmqtt.Client instance used for MQTT communication.
            topic_prefix: Topic prefix all subscriptions and publishes are rooted under.
        """
        self.mqtt_client = mqtt_client
        self.topic_prefix = topic_prefix.rstrip("/")
        # topic -> qos; kept so subscriptions can be restored on reconnect.
        self._subscriptions: Dict[str, int] = {}
        self._is_running: bool = False
        self.logger = logging.getLogger(f"Kamio.node.{self.__class__.__name__}")

    @property
    def is_running(self) -> bool:
        """Return whether the node is running."""
        return self._is_running

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    async def start(self) -> None:
        """Called when the node is started. Subscribe to topics here."""

    async def stop(self) -> None:
        """Called when the node is stopped.

        Default implementation unsubscribes from every topic the node
        registered via :meth:`subscribe` / :meth:`subscribe_absolute`.
        Subclasses that override this **should** call ``await super().stop()``
        so subscriptions are cleaned up; otherwise the unsubscribe logic
        below is skipped.
        """
        self._teardown_subscriptions()

    def _teardown_subscriptions(self) -> None:
        """Unsubscribe from all topics and mark the node as stopped."""
        for topic in list(self._subscriptions):
            try:
                self.mqtt_client.unsubscribe(topic)
            except Exception as e:
                self.logger.warning(f"Failed to unsubscribe from {topic}: {e}")
        self._subscriptions.clear()
        self._is_running = False

    async def _start_running(self) -> None:
        """Run :meth:`start` and mark the node as running.

        If ``start()`` raises (or the task is cancelled), any subscriptions
        the node managed to make are torn down so they never leak.
        """
        try:
            await self.start()
        except BaseException:
            self._teardown_subscriptions()
            raise
        self._is_running = True

    async def _stop_running(self) -> None:
        """Run :meth:`stop` and mark the node as stopped."""
        await self.stop()
        self._is_running = False

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
        self._subscriptions[full_topic] = qos
        self.logger.debug(f"Subscribed: {full_topic}")

    def subscribe_absolute(self, topic: str, qos: int = 0) -> None:
        """Subscribe to an absolute topic (no prefix prepended)."""
        self.mqtt_client.subscribe(topic, qos)
        self._subscriptions[topic] = qos
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
        """Awaitable variant of :meth:`publish`.

        ``gmqtt.Client.publish`` is itself non-blocking, so this is a thin
        wrapper that publishes directly on the event loop — running it in a
        worker thread would call into the asyncio client from the wrong
        thread (SCAN-13).
        """
        full_topic = self._resolve_topic(topic, absolute=False)
        self.mqtt_client.publish(
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
        """Return a developer-friendly representation."""
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
        """Initialize the custom node manager with an app reference."""
        self._app = app
        self._nodes: Dict[str, CustomNode] = {}
        # Track fire-and-forget stop() tasks from unregister_node.
        self._stop_tasks: set = set()
        # Nodes registered while the app is running are started by task;
        # keep the handles so unregister/stop_all can cancel them (SCAN-15).
        self._start_tasks: Dict[str, asyncio.Task] = {}

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

        # Documented lifecycle: a node registered while the app is running
        # starts immediately.  The task handle is kept so unregister_node()
        # can cancel it before it leaks subscriptions (SCAN-15).
        loop = getattr(self._app, "_loop", None)
        if (
            getattr(self._app, "_is_running", False)
            and isinstance(loop, asyncio.AbstractEventLoop)
            and loop.is_running()
        ):
            task = loop.create_task(self._start_and_log(name, node))
            self._start_tasks[name] = task
            task.add_done_callback(partial(self._forget_start_task, name))

    def _forget_start_task(self, name: str, task: asyncio.Task) -> None:
        """Done-callback removing a finished start task from tracking."""
        self._start_tasks.pop(name, None)

    async def _start_and_log(self, name: str, node: CustomNode) -> None:
        """Start a node registered while the app was already running."""
        try:
            await node._start_running()
        except Exception as e:
            logger.error(f"Failed to start custom node '{name}': {e}", exc_info=True)
            return
        # If the node was unregistered while start() was in flight, undo it.
        if self._nodes.get(name) is not node:
            node._teardown_subscriptions()
            return
        logger.info(f"Started custom node: '{name}'")

    def unregister_node(self, name: str) -> None:
        """
        Unregister a node by name.

        If the node is running, ``node.stop()`` is scheduled via the app's
        event loop so that MQTT subscriptions are cleaned up.  Safe if the
        node is not found (logs a warning).
        """
        if name not in self._nodes:
            logger.warning(f"unregister_node: '{name}' not found")
            return
        node = self._nodes.pop(name)

        # Cancel a still-in-flight start task; its own teardown cleans up
        # whatever subscriptions it had already made.
        start_task = self._start_tasks.pop(name, None)
        if start_task is not None and not start_task.done():
            start_task.cancel()

        # Clean up the node's MQTT subscriptions.  Cleanup is driven by
        # ``_subscriptions`` rather than ``is_running`` so a node whose
        # start() failed partway cannot leak topics (SCAN-15).  stop() is
        # async, so schedule it on the app's event loop if one is available;
        # fall back to a synchronous best-effort unsubscribe otherwise.
        if node.is_running:
            loop = getattr(self._app, "_loop", None)
            if isinstance(loop, asyncio.AbstractEventLoop) and not loop.is_closed():

                async def _stop_and_teardown() -> None:
                    try:
                        await node.stop()
                    except Exception as e:
                        logger.error(f"Error stopping custom node '{name}': {e}")
                    finally:
                        # Guaranteed cleanup even when a subclassed stop()
                        # forgets to call super().
                        node._teardown_subscriptions()

                task = loop.create_task(_stop_and_teardown())
                # Track the task so errors are logged and it isn't GC'd early.
                self._stop_tasks.add(task)
                task.add_done_callback(self._stop_tasks.discard)
            else:
                # No running loop — do a synchronous best-effort cleanup of
                # subscriptions so they don't leak past the node's lifetime.
                node._teardown_subscriptions()
        elif node._subscriptions:
            node._teardown_subscriptions()
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
        # Snapshot: unregister_node() may mutate _nodes while a start()
        # is in flight.
        for name, node in list(self._nodes.items()):
            if self._nodes.get(name) is not node:
                continue
            try:
                await node._start_running()
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
            # If the node was unregistered while its start() was in flight,
            # undo it — a node that is no longer registered must not keep
            # subscriptions (SCAN-15).
            if self._nodes.get(name) is not node:
                node._teardown_subscriptions()
                continue
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
        # Cancel any starts still in flight — their own teardown handles
        # partial subscriptions.
        for task in list(self._start_tasks.values()):
            task.cancel()
        self._start_tasks.clear()

        for name, node in reversed(list(self._nodes.items())):
            if node.is_running:
                try:
                    await node._stop_running()
                    logger.info(f"Stopped custom node: '{name}'")
                    await self._app.event_bus.publish(
                        "custom_node_stopped",
                        {
                            "node_name": name,
                        },
                    )
                except Exception as e:
                    logger.error(f"Failed to stop custom node '{name}': {e}", exc_info=True)
            # Subscriptions are tracked independently of is_running — a node
            # whose start() failed partway may still hold some (SCAN-15).
            if node._subscriptions:
                node._teardown_subscriptions()

    # ------------------------------------------------------------------
    # Reconnection
    # ------------------------------------------------------------------

    async def on_connect(self) -> None:
        """Restore subscriptions and fire ``on_connect`` hooks after (re)connect.

        With ``clean_session=True`` the broker drops subscriptions on
        disconnect, so every running node's topics must be re-subscribed.
        The node's own ``on_connect`` hook runs after its subscriptions
        are restored.
        """
        for name, node in list(self._nodes.items()):
            if not node.is_running:
                continue
            for topic, qos in list(node._subscriptions.items()):
                try:
                    node.mqtt_client.subscribe(topic, qos)
                except Exception as e:
                    node.logger.error(
                        f"Failed to re-subscribe '{name}' to {topic}: {e}", exc_info=True
                    )
            try:
                await node.on_connect()
            except Exception as e:
                logger.error(f"on_connect hook failed for custom node '{name}': {e}", exc_info=True)

    async def on_disconnect(self) -> None:
        """Fire ``on_disconnect`` hooks for all running nodes."""
        for name, node in list(self._nodes.items()):
            if not node.is_running:
                continue
            try:
                await node.on_disconnect()
            except Exception as e:
                logger.error(
                    f"on_disconnect hook failed for custom node '{name}': {e}",
                    exc_info=True,
                )

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
