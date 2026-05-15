import pytest
from synapse.device import Device, command
from synapse.data_fields import telemetry, state

def test_device_meta_collection():
    class TestDevice(Device):
        temp: float = telemetry(unit="C")
        target: float = state(default=20.0)
        
        @command
        async def set_temp(self, value: float):
            pass

    assert "temp" in TestDevice.SYNAPSE_FIELDS
    assert "target" in TestDevice.SYNAPSE_FIELDS
    assert "set_temp" in TestDevice.SYNAPSE_COMMANDS
    assert TestDevice.SYNAPSE_FIELDS["temp"].kind == "telemetry"
    assert TestDevice.SYNAPSE_FIELDS["target"].kind == "state"

def test_device_inheritance():
    class BaseDevice(Device):
        base_state: int = state(default=1)
    
    class ChildDevice(BaseDevice):
        child_state: int = state(default=2)
    
    assert "base_state" in ChildDevice.SYNAPSE_FIELDS
    assert "child_state" in ChildDevice.SYNAPSE_FIELDS
    assert ChildDevice.SYNAPSE_FIELDS["base_state"].default == 1
    assert ChildDevice.SYNAPSE_FIELDS["child_state"].default == 2
