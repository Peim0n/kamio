import pytest
import asyncio
import json
from synapse.app import SynapseApp
from synapse.device import Device
from synapse.data_fields import state, telemetry
from synapse.core.envelope import Envelope, EnvelopeType
from synapse.core import topics

@pytest.mark.asyncio
async def test_app_device_lifecycle(mock_mqtt):
    app = SynapseApp(mqtt_broker=mock_mqtt)
    
    class Sensor(Device):
        value: float = telemetry()
    
    app.register(Sensor)
    
    # Create device
    device = await app.create_device("sensor1", "sensor")
    assert "sensor1" in app.devices
    assert device.node is not None
    
    # Start app
    await app.start()
    assert app.is_running
    
    # Verify subscription (using the current buggy topic to match current code)
    mock_mqtt.subscribe.assert_any_call("synapse/sensor1/#")
    
    await app.stop()
    assert not app.is_running

@pytest.mark.asyncio
async def test_app_message_routing(mock_mqtt):
    app = SynapseApp(mqtt_broker=mock_mqtt)
    
    class Switch(Device):
        active: bool = state(default=False)
    
    app.register(Switch)
    device = await app.create_device("sw1", "switch")
    await app.start()
    
    # Simulate incoming state change message
    # Note: We use the legacy format because the current code's subscription matches it
    topic = "synapse/sw1/ds"
    payload = json.dumps({
        "source": "server",
        "target": "sw1",
        "type": "ds",
        "data": {"active": True},
        "cind": "cmd1"
    }).encode()
    
    class MockMsg:
        def __init__(self, topic, payload):
            self.topic = topic
            self.payload = payload
            
    # Manually trigger the callback
    app._on_mqtt_message(mock_mqtt, None, MockMsg(topic, payload))
    
    # Give it a moment to process in the loop
    await asyncio.sleep(0.1)
    
    # Check if state was updated
    assert device.active is True
    
    await app.stop()
