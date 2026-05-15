import pytest
import asyncio
import paho.mqtt.client as mqtt
from unittest.mock import MagicMock, AsyncMock

@pytest.fixture
def mock_mqtt():
    client = MagicMock(spec=mqtt.Client)
    client.is_connected.return_value = True
    return client

@pytest.fixture
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
