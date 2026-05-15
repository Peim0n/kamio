import pytest
import asyncio
from unittest.mock import MagicMock
from synapse.app import SynapseApp
from synapse.device import Device
from synapse.drivers.mock import MockHardwareDriver

class FailDevice(Device):
    pass

@pytest.mark.asyncio
async def test_driver_failure_handling():
    # Mock driver that fails on connect
    driver = MockHardwareDriver(failure_rate=1.0)
    app = SynapseApp(mqtt_broker="mqtt://localhost:1883")
    app.register(FailDevice)
    
    device = await app.create_device("fail_dev", "faildevice", driver=driver)
    
    # Starting app should not crash even if driver fails to connect
    # (depending on implementation, but usually we want resilience)
    try:
        await app.start()
    except Exception as e:
        pytest.fail(f"App crashed on driver failure: {e}")
    
    # Try to execute command on failed driver
    with pytest.raises(RuntimeError):
        await driver.execute("any", {})
        
    await app.stop()

@pytest.mark.asyncio
async def test_mqtt_disconnect_resilience(mock_mqtt):
    app = SynapseApp(mqtt_broker=mock_mqtt)
    await app.start()
    
    # Simulate disconnect
    app.mqtt_client.is_connected.return_value = False
    
    # App should still be "running" but MQTT operations might fail or be queued
    assert app.is_running
    
    await app.stop()
