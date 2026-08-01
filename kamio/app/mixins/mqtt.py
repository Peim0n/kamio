from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

from kamio.core import topics as mqtt_topics
from kamio.core.mqtt_nodes import BROADCAST_ID

logger = logging.getLogger("Kamio.app")


class MqttDispatchMixin:
    """Thread-safe helpers and MQTT callback dispatch."""

    _loop: Optional[asyncio.AbstractEventLoop]

    def _run_coro_threadsafe(self: Any, coro) -> None:
        """Schedule a coroutine on the running event loop."""
        if self._loop is None or not self._loop.is_running():
            return

        try:
            task = self._loop.create_task(coro)
        except RuntimeError:
            return
        tasks: Optional[set] = getattr(self, "_mqtt_bg_tasks", None)
        if tasks is None:
            tasks = set()
            self._mqtt_bg_tasks: set = tasks
        tasks.add(task)
        task.add_done_callback(tasks.discard)

    def _publish_event_threadsafe(self: Any, event_type: str, data: Dict[str, Any]) -> None:
        """Publish an event bus event from a non-asyncio (MQTT) thread if the loop is running."""
        self._run_coro_threadsafe(self.event_bus.publish(event_type, data))

    def _schedule_when_running(self: Any, coro) -> None:
        """Fire-and-forget a coroutine if an event loop is already running."""
        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(coro)
            tasks: Optional[set] = getattr(self, "_bg_tasks", None)
            if tasks is None:
                tasks = set()
                self._bg_tasks: set = tasks
            tasks.add(task)
            task.add_done_callback(tasks.discard)
        except RuntimeError:
            pass

    def _on_mqtt_connect(self: Any, client, flags, rc, properties=None):
        """gmqtt on_connect callback.

        ``flags`` is the connect-flags byte from the broker (gmqtt passes it
        in the second positional slot).  When the connection is restored
        after a disconnect, all device/server nodes must re-subscribe because
        ``clean_session=True`` subscriptions are not retained by the broker.
        """
        try:
            reason = getattr(rc, "name", rc)
            logger.info(f"MQTT connected (rc={reason})")
            conn = self._mqtt_conn
            self._publish_event_threadsafe(
                "mqtt_connected",
                {
                    "broker": conn.host if conn else None,
                    "port": conn.port if conn else None,
                    "rc": reason,
                },
            )
            # Re-subscribe all nodes on (re)connect.  With clean_session=True
            # the broker drops subscriptions on disconnect, so we must restore
            # them.  With clean_session=False this is a harmless no-op (gmqtt
            # deduplicates subscriptions by topic).
            self._resubscribe_all_nodes()
        except Exception as e:
            logger.exception(f"Error in _on_mqtt_connect: {e}")

    def _on_mqtt_disconnect(self: Any, client, packet, *args):
        try:
            reason = getattr(packet, "reason_code", packet)
            logger.warning(f"MQTT disconnected (rc={reason})")
            self._publish_event_threadsafe("mqtt_disconnected", {"rc": reason})
        except Exception as e:
            logger.exception(f"Error in _on_mqtt_disconnect: {e}")

    def _resubscribe_all_nodes(self: Any) -> None:
        """Re-subscribe the server node and all device nodes after a reconnect.

        Each node's ``start()`` is idempotent (it checks ``_is_running``), so
        we temporarily reset the flag to force re-subscription without
        re-invoking device lifecycle hooks.
        """
        for node in [self.server_node] + list(self._device_nodes.values()):
            if not getattr(node, "_is_running", False):
                continue
            # Force re-subscription by resetting the running flag.
            node._is_running = False
            self._run_coro_threadsafe(node._resubscribe())

    def _on_mqtt_message(self: Any, client, topic, payload, qos=0, properties=None):
        """Route an incoming MQTT message to custom nodes, event bus, and device nodes.

        Called from the gmqtt network task.  Messages that arrive before
        the asyncio event loop is running are silently dropped; QoS ensures the
        broker redelivers them once the loop is ready.
        """
        if self._loop is None or not self._is_running:
            return

        try:
            self._run_coro_threadsafe(self.custom_nodes.route_message(topic, payload))
            self._publish_event_threadsafe(
                "mqtt_message_received",
                {"topic": topic, "payload": payload, "qos": qos},
            )
            self.server_node.dispatch(topic, payload)
            device_id, _ = mqtt_topics.parse(topic)
            # Snapshot the device-node map so a concurrent add/remove cannot
            # raise KeyError / change dict size during iteration.
            nodes = self._device_nodes
            if device_id and device_id in nodes:
                nodes[device_id].dispatch(topic, payload)
            elif device_id == BROADCAST_ID:
                for node in list(nodes.values()):
                    node.dispatch(topic, payload)
        except Exception as e:
            logger.exception(f"Error dispatching MQTT message on {topic!r}: {e}")
