import pytest
import asyncio
import time
from synapse.app import SynapseApp
from synapse.device import Device
from synapse.data_fields import telemetry, state

class StressDevice(Device):
    value: int = telemetry(default=0)
    status: bool = state(default=False)

@pytest.mark.asyncio
async def test_stress_many_devices(mock_mqtt):
    app = SynapseApp(mqtt_broker=mock_mqtt)
    app.register(StressDevice)
    
    num_devices = 50
    devices = []
    
    start_time = time.time()
    for i in range(num_devices):
        dev = await app.create_device(f"stress_{i}", "stressdevice")
        devices.append(dev)
    
    creation_time = time.time() - start_time
    print(f"\nCreated {num_devices} devices in {creation_time:.2f}s")
    
    await app.start()
    
    # Simulate updates for all devices
    start_time = time.time()
    for dev in devices:
        dev.value += 1
        dev.status = True
        # We don't await sync here to simulate high load
        asyncio.create_task(dev.request_state_sync())
    
    # Give some time for processing
    await asyncio.sleep(1)
    
    update_time = time.time() - start_time
    print(f"Triggered updates for {num_devices} devices in {update_time:.2f}s")
    
    await app.stop()
    assert len(app.devices) == num_devices
