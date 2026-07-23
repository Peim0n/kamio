from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from kamio import KamioApp
from kamio.core.custom_nodes import CustomNode


class BridgeNode(CustomNode):
    def __init__(self, mqtt_client, topic_prefix):
        super().__init__(mqtt_client, topic_prefix)
        self.received = []
        self.published = []

    async def start(self):
        self.subscribe("cmd/#")

    async def stop(self):
        pass

    async def handle_message(self, topic, payload):
        self.received.append((topic, payload))
        # Reply on a relative topic.
        self.publish("ack", b"ok")


@pytest.fixture
def mock_mqtt_client():
    client = MagicMock()
    client.subscribe = MagicMock(return_value=(0, 1))
    client.publish = MagicMock(return_value=(0, 1))
    return client


def test_custom_node_matches_topic_prefix(mock_mqtt_client):
    node = BridgeNode(mock_mqtt_client, "sensors")
    assert node.matches("sensors/room/temp") is True
    assert node.matches("other/room/temp") is False


@pytest.mark.asyncio
async def test_custom_node_register_list_and_unregister(mock_mqtt_client):
    app = KamioApp()
    node = BridgeNode(mock_mqtt_client, "bridge")
    app.register_custom_node("bridge", node)
    assert "bridge" in app.list_custom_nodes()
    assert app.get_custom_node("bridge") is node
    app.unregister_custom_node("bridge")
    assert app.get_custom_node("bridge") is None
    assert "bridge" not in app.list_custom_nodes()


@pytest.mark.asyncio
async def test_custom_node_subscribe_and_publish(mock_mqtt_client):
    app = KamioApp()
    node = BridgeNode(mock_mqtt_client, "bridge")
    app.register_custom_node("bridge", node)
    await node.start()
    assert mock_mqtt_client.subscribe.called
    await node.handle_message("bridge/cmd/power", b"on")
    assert node.received == [("bridge/cmd/power", b"on")]
    assert mock_mqtt_client.publish.called
    app.unregister_custom_node("bridge")


@pytest.mark.asyncio
async def test_custom_node_manager_route_message(mock_mqtt_client):
    app = KamioApp()
    node = BridgeNode(mock_mqtt_client, "bridge")
    app.register_custom_node("bridge", node)
    # DeviceNode vs CustomNode dispatch depends on manager; route_message should return True when handled.
    handled = await app.custom_nodes.route_message("bridge/cmd/power", b"on")
    assert handled is True
    assert node.received == [("bridge/cmd/power", b"on")]
