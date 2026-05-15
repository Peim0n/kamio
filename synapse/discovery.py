import json
import logging
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .device import Device

logger = logging.getLogger("synapse.discovery")

class HADiscovery:
    """
    Home Assistant MQTT Discovery support.
    """
    def __init__(self, discovery_prefix: str = "homeassistant"):
        self.prefix = discovery_prefix

    async def announce(self, device: 'Device'):
        """Announce device to Home Assistant via MQTT."""
        if not device.node:
            return

        device_id = device.node.device_id
        device_type = device.device_type()
        
        # This is a simplified version. Real HA discovery requires mapping 
        # Synapse fields to HA entities (sensor, binary_sensor, switch, etc.)
        for name, field in device.SYNAPSE_FIELDS.items():
            component = self._map_to_ha_component(field)
            if not component:
                continue

            topic = f"{self.prefix}/{component}/{device_id}/{name}/config"
            payload = {
                "name": f"{device_id} {name}",
                "state_topic": f"synapse/v1/{device_id}/ds",
                "value_template": f"{{{{ value_json.data.{name} }}}}",
                "unique_id": f"{device_id}_{name}",
                "device": {
                    "identifiers": [device_id],
                    "name": device_id,
                    "model": device_type,
                    "manufacturer": "Synapse Core"
                }
            }
            
            if field.kind == "state" and field.writable:
                payload["command_topic"] = f"synapse/v1/{device_id}/sc"
                payload["command_template"] = json.dumps({
                    "source": "ha",
                    "method": f"set_{name}",
                    "params": {"value": "VALUE_PLACEHOLDER"}
                }).replace('"VALUE_PLACEHOLDER"', "{{ value }}")

            await device.node.publish_raw(topic, json.dumps(payload).encode())
            logger.info(f"Announced {name} to HA at {topic}")

    def _map_to_ha_component(self, field) -> str:
        if field.kind == "telemetry":
            return "sensor"
        if field.kind == "state":
            if field.python_type == bool:
                return "switch" if field.writable else "binary_sensor"
            return "sensor"
        return ""
