import pytest
import asyncio
from synapse.app import SynapseApp
from synapse.device import Device
from synapse.data_fields import state
from unittest.mock import AsyncMock, MagicMock

@pytest.mark.asyncio
async def test_rule_triggering():
    app = SynapseApp(mqtt_broker="mqtt://localhost:1883")
    
    class Light(Device):
        power: bool = state(default=False)
    
    app.register(Light)
    
    rule_called = asyncio.Event()
    
    @app.rule(device=Light, fields=["power"])
    async def on_power_change(snapshot, app_inst):
        if snapshot["update"].get("power") is True:
            rule_called.set()
            
    # Register instance so RuleEngine can find its class
    light_inst = Light()
    app.registry.register_instance("light1", light_inst)
    
    # Simulate device update
    await app.rules.handle_device_update("light1", {"power": True})
    
    try:
        await asyncio.wait_for(rule_called.wait(), timeout=1.0)
    except asyncio.TimeoutError:
        pytest.fail("Rule was not triggered")
