import pytest
import asyncio
from synapse.device import Device, command
from synapse.core.handlers import DeviceHandler
from synapse.core.envelope import Envelope, EnvelopeType
from unittest.mock import MagicMock, AsyncMock

@pytest.mark.asyncio
async def test_command_invocation_bug():
    class TestDevice(Device):
        @command
        async def my_cmd(self, value: int):
            return value * 2

    device = TestDevice()
    node = MagicMock()
    node.device_id = "test_dev"
    node.publish = AsyncMock()
    
    handler = DeviceHandler(device, node)
    
    # Create a command envelope
    env = Envelope(
        source="server",
        target="test_dev",
        type=EnvelopeType.SERVER_COMMAND,
        data={"method": "my_cmd", "params": {"value": 10}},
        cind="123"
    )
    
    # This should now succeed because DeviceHandler checks for 'node' parameter
    await handler(env)
    
    # Verify ack was sent with correct result
    node.publish.assert_called_once()
    ack_env = node.publish.call_args[0][0]
    assert ack_env.type == EnvelopeType.COMMAND_ACK
    assert ack_env.data["result"] == 20
    assert ack_env.data["status"] == "ok"
