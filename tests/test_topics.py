import pytest
from synapse.core import topics
from synapse.core.mqtt_nodes import DeviceNode
from unittest.mock import MagicMock

def test_topic_parsing():
    # Current format
    dev_id, msg_type = topics.parse("synapse/v1/my_device/dt")
    assert dev_id == "my_device"
    assert msg_type == "dt"
    
    # Legacy format
    dev_id, msg_type = topics.parse("synapse/my_device/dt")
    assert dev_id == "my_device"
    assert msg_type == "dt"

@pytest.mark.asyncio
async def test_node_subscription_mismatch():
    mqtt = MagicMock()
    node = DeviceNode("dev123", mqtt)
    
    # This calls mqtt.subscribe(f"{topics.PREFIX}/{self.device_id}/#")
    # which is "synapse/dev123/#"
    await node.start()
    
    # But topics.telemetry("dev123") is "synapse/v1/dev123/dt"
    # "synapse/dev123/#" will NOT match "synapse/v1/dev123/dt"
    
    expected_topic = f"{topics.PREFIX}/{node.device_id}/#"
    mqtt.subscribe.assert_any_call(expected_topic)
    
    # Verify that the actual telemetry topic wouldn't be caught by this subscription
    actual_telemetry_topic = topics.telemetry("dev123")
    assert not actual_telemetry_topic.startswith(expected_topic)
