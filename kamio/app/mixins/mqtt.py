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
        tasks = getattr(self, "_mqtt_bg_tasks", None)
        if tasks is None:
            tasks = self._mqtt_bg_tasks = set()
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
            tasks = getattr(self, "_bg_tasks", None)
            if tasks is None:
                tasks = self._bg_tasks = set()
            tasks.add(task)
            task.add_done_callback(tasks.discard)
        except RuntimeError:
            pass

    def _on_mqtt_connect(self: Any, client, session_present, rc, properties=None):
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

    def _on_mqtt_disconnect(self: Any, client, packet, *args):
        reason = getattr(packet, "reason_code", packet)
        logger.warning(f"MQTT disconnected (rc={reason})")
        self._publish_event_threadsafe("mqtt_disconnected", {"rc": reason})

    def _on_mqtt_message(self: Any, client, topic, payload, qos=0, properties=None):
        """Route an incoming MQTT message to custom nodes, event bus, and device nodes.

        Called from the gmqtt network task.  Messages that arrive before
        the asyncio event loop is running are silently dropped; QoS ensures the
        broker redelivers them once the loop is ready.
        """
        if self._loop is None or not self._is_running:
            return

        self._run_coro_threadsafe(self.custom_nodes.route_message(topic, payload))
        self._publish_event_threadsafe(
            "mqtt_message_received",
            {"topic": topic, "payload": payload, "qos": qos},
        )
        self.server_node.dispatch(topic, payload)
        device_id, _ = mqtt_topics.parse(topic)
        if device_id and device_id in self._device_nodes:
            self._device_nodes[device_id].dispatch(topic, payload)
        elif device_id == BROADCAST_ID:
            for node in self._device_nodes.values():
                node.dispatch(topic, payload)
