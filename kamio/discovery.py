from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .device import Device

logger = logging.getLogger("Kamio.discovery")


class HADiscovery:
    """
    Home Assistant MQTT Discovery support.

    Discovery messages are published with ``retain=True`` so that Home
    Assistant can pick them up after a broker/HA restart without waiting for
    the next announce cycle. Use :meth:`clear` to remove the discovery
    entries (publish an empty retained payload) when a device is removed.
    """

    def __init__(self, discovery_prefix: str = "homeassistant"):
        """Initialize the HA discovery publisher.

        Args:
            discovery_prefix: MQTT topic prefix for HA discovery (default "homeassistant").
        """
        self.discovery_prefix = discovery_prefix

    async def announce(self, device: "Device"):
        """Announce device to Home Assistant via MQTT (retained)."""
        if not device.node:
            logger.warning(f"Cannot announce device without an MQTT node")
            return

        device_id = device.node.device_id
        device_type = device.device_type()

        # This is a simplified version. Real HA discovery requires mapping
        # Kamio fields to HA entities (sensor, binary_sensor, switch, etc.)
        for name, field in device.Kamio_FIELDS.items():
            component = self._map_to_ha_component(field)
            if not component:
                continue

            topic = f"{self.discovery_prefix}/{component}/{device_id}/{name}/config"
            payload = {
                "name": f"{device_id} {name}",
                "state_topic": f"Kamio/v1/{device_id}/ds",
                "value_template": f"{{{{ value_json.data.{name} }}}}",
                "unique_id": f"{device_id}_{name}",
                "availability_topic": f"Kamio/v1/{device_id}/k",
                "device": {
                    "identifiers": [device_id],
                    "name": device_id,
                    "model": device_type,
                    "manufacturer": "Kamio Core",
                },
            }

            if field.kind == "telemetry" and getattr(field, "unit", None):
                payload["unit_of_measurement"] = field.unit

            if field.kind == "state" and field.writable:
                payload["command_topic"] = f"Kamio/v1/{device_id}/sc"
                if field.python_type == bool:
                    # Home Assistant sends a fixed payload for switches.
                    payload["payload_on"] = json.dumps(
                        {
                            "source": "ha",
                            "method": f"set_{name}",
                            "params": {"value": True},
                        }
                    )
                    payload["payload_off"] = json.dumps(
                        {
                            "source": "ha",
                            "method": f"set_{name}",
                            "params": {"value": False},
                        }
                    )
                else:
                    # Render a Kamio command envelope from the HA value.
                    payload["command_template"] = (
                        f'{{{{ {{"source": "ha", "method": "set_{name}", '
                        f'"params": {{"value": value | tojson}}}} | tojson }}}}'
                    )

            # retain=True so HA rediscovers after a restart without waiting.
            try:
                await device.node.publish_raw(topic, json.dumps(payload).encode(), retain=True)
                logger.info(f"Announced {name} to HA at {topic}")
            except Exception as e:
                logger.warning(f"Failed to announce {name} to HA at {topic}: {e}")

    async def clear(self, device: "Device") -> None:
        """Remove HA discovery entries for a device (publish empty retained payload).

        Should be called when a device is removed so HA does not keep showing it
        as ``unavailable`` forever.
        """
        if not device.node:
            return
        device_id = device.node.device_id
        for name, field in device.Kamio_FIELDS.items():
            component = self._map_to_ha_component(field)
            if not component:
                continue
            topic = f"{self.discovery_prefix}/{component}/{device_id}/{name}/config"
            try:
                await device.node.publish_raw(topic, b"", retain=True)
                logger.info(f"Cleared HA discovery for {name} at {topic}")
            except Exception as e:
                logger.warning(f"Failed to clear HA discovery for {name}: {e}")

    def _map_to_ha_component(self, field) -> str:
        """Map a Kamio field to a Home Assistant component type (sensor, switch, etc.)."""
        python_type = getattr(field, "python_type", None)
        if python_type is None and field.default is not None:
            python_type = type(field.default)
        if field.kind == "telemetry":
            return "sensor"
        if field.kind == "state":
            if python_type == bool:
                return "switch" if field.writable else "binary_sensor"
            if field.writable:
                # number for numeric writable state, select when constrained.
                if getattr(field, "choices", None):
                    return "select"
                if python_type in (int, float):
                    return "number"
                return "text"
            return "sensor"
        return ""
