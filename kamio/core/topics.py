"""
Centralized MQTT topic management for Kamio Core.

Topic format: ``Kamio/v1/{device_id}/{type}``
Legacy format: ``Kamio/{device_id}/{type}`` (parsed transparently by :func:`parse`).
"""
from __future__ import annotations
from typing import Optional, Tuple, Callable, Dict
from .envelope import EnvelopeType

PREFIX: str = "Kamio"
VERSION: str = "v1"
BASE: str = f"{PREFIX}/{VERSION}"

# Wildcards for subscriptions
ALL: str = f"{BASE}/#"
TELEMETRY_WILDCARD: str = f"{BASE}/+/dt"
STATE_WILDCARD: str = f"{BASE}/+/ds"
STATE_ACK_WILDCARD: str = f"{BASE}/+/sa"
COMMAND_WILDCARD: str = f"{BASE}/+/sc"
COMMAND_ACK_WILDCARD: str = f"{BASE}/+/ca"
EVENT_WILDCARD: str = f"{BASE}/+/de"
CONFIG_WILDCARD: str = f"{BASE}/+/conf"

def telemetry(device_id: str) -> str:
    return f"{BASE}/{device_id}/dt"

def state(device_id: str) -> str:
    return f"{BASE}/{device_id}/ds"

def state_ack(device_id: str) -> str:
    return f"{BASE}/{device_id}/sa"

def command(device_id: str) -> str:
    return f"{BASE}/{device_id}/sc"

def command_ack(device_id: str) -> str:
    return f"{BASE}/{device_id}/ca"

def event(device_id: str) -> str:
    return f"{BASE}/{device_id}/de"

def config(device_id: str) -> str:
    return f"{BASE}/{device_id}/conf"

def keepalive(device_id: str) -> str:
    return f"{BASE}/{device_id}/k"

def parse(topic: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parses topic and returns (device_id, type).
    Supports formats:
    - Kamio/v1/{device_id}/{type} (current)
    - Kamio/{device_id}/{type} (legacy)
    """
    parts = topic.split('/')

    # Current format (4 parts)
    if len(parts) == 4 and parts[0] == PREFIX and parts[1] == VERSION:
        return parts[2], parts[3]

    # Legacy format (3 parts)
    if len(parts) == 3 and parts[0] == PREFIX:
        return parts[1], parts[2]

    return None, None

# Mapping of envelope types to topic builder functions
TOPIC_MAP: Dict[EnvelopeType, Callable[[str], str]] = {
    EnvelopeType.DEVICE_TELEMETRY: telemetry,
    EnvelopeType.DEVICE_STATE: state,
    EnvelopeType.STATE_ACK: state_ack,
    EnvelopeType.SERVER_COMMAND: command,
    EnvelopeType.COMMAND_ACK: command_ack,
    EnvelopeType.DEVICE_EVENT: event,
    EnvelopeType.DEVICE_CONFIG: config,
    EnvelopeType.KEEPALIVE: keepalive,
}

def get_topic_func(msg_type: EnvelopeType) -> Optional[Callable[[str], str]]:
    """Returns the topic builder function for a given message type."""
    return TOPIC_MAP.get(msg_type)
