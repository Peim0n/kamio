from __future__ import annotations

"""
Transport layer — MQTT connection, nodes, topics, and message envelopes.

Re-exports the relevant modules from ``Kamio.core`` for logical grouping.
Physical files remain in ``Kamio/core/`` for import compatibility.
"""
from kamio.core.envelope import SERVER_ID, Envelope, EnvelopeType
from kamio.core.mqtt_connection import MqttConnection
from kamio.core.mqtt_nodes import BROADCAST_ID, BaseNode, DeviceNode, ServerNode
from kamio.core.topics import (
    ALL,
    BASE,
    PREFIX,
    TOPIC_MAP,
    VERSION,
    command,
    command_ack,
    config,
    event,
    get_topic_func,
    keepalive,
    parse,
    state,
    state_ack,
    telemetry,
)

__all__ = [
    "MqttConnection",
    "BaseNode",
    "ServerNode",
    "DeviceNode",
    "BROADCAST_ID",
    "parse",
    "telemetry",
    "state",
    "state_ack",
    "command",
    "command_ack",
    "event",
    "config",
    "keepalive",
    "get_topic_func",
    "PREFIX",
    "VERSION",
    "BASE",
    "ALL",
    "TOPIC_MAP",
    "Envelope",
    "EnvelopeType",
    "SERVER_ID",
]
