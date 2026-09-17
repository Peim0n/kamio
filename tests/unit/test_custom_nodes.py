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


def test_teardown_subscriptions_logs_and_clears_on_unsubscribe_error(mock_mqtt_client, caplog):
    """Teardown clears subscriptions and state even when unsubscribe fails."""
    mock_mqtt_client.unsubscribe.side_effect = RuntimeError("unsubscribe failed")
    node = BridgeNode(mock_mqtt_client, "bridge")
    node._subscriptions.append("bridge/cmd/#")
    node._is_running = True

    with caplog.at_level("WARNING"):
        node._teardown_subscriptions()

    assert node._subscriptions == []
    assert node.is_running is False
    assert "Failed to unsubscribe from bridge/cmd/#" in caplog.text


@pytest.mark.asyncio
async def test_unregister_running_node_cleans_subscriptions(mock_mqtt_client):
    """Unregistering a running node should schedule stop() to clean up subs."""
    app = KamioApp()
    node = BridgeNode(mock_mqtt_client, "bridge")
    app.register_custom_node("bridge", node)
    await node.start()
    node._is_running = True
    # Unregister while running — stop() should be scheduled on the loop.
    app.unregister_custom_node("bridge")
    assert app.get_custom_node("bridge") is None
    # Allow the scheduled stop() task to run.
    import asyncio

    await asyncio.sleep(0.01)
    # The default stop() in BridgeNode is a no-op pass, but the node should
    # no longer be registered.
    assert "bridge" not in app.list_custom_nodes()


@pytest.mark.asyncio
async def test_unregister_running_node_no_loop_fallback(mock_mqtt_client):
    """When no event loop is available, unregister does sync cleanup."""
    app = KamioApp()
    node = BridgeNode(mock_mqtt_client, "bridge")
    app.register_custom_node("bridge", node)
    await node.start()
    node._is_running = True
    # Simulate no loop available by setting _loop to None on the app.
    app._loop = None  # type: ignore[attr-defined]
    app.unregister_custom_node("bridge")
    assert app.get_custom_node("bridge") is None
    assert not node._is_running


@pytest.mark.asyncio
async def test_unregister_running_node_no_loop_clears_on_unsubscribe_error(mock_mqtt_client):
    """No-loop unregister cleanup tolerates unsubscribe errors."""
    mock_mqtt_client.unsubscribe.side_effect = RuntimeError("unsubscribe failed")
    app = KamioApp()
    node = BridgeNode(mock_mqtt_client, "bridge")
    app.register_custom_node("bridge", node)
    node._subscriptions.append("bridge/cmd/#")
    node._is_running = True
    app._loop = None  # type: ignore[attr-defined]

    app.unregister_custom_node("bridge")

    assert node._subscriptions == []
    assert node.is_running is False


@pytest.mark.asyncio
async def test_custom_node_manager_start_and_stop_update_running_state(mock_mqtt_client):
    """Manager lifecycle helpers own the node running-state transitions."""
    app = KamioApp()
    node = BridgeNode(mock_mqtt_client, "bridge")
    app.register_custom_node("bridge", node)

    await app.custom_nodes.start_all()
    assert node.is_running is True

    await app.custom_nodes.stop_all()
    assert node.is_running is False


@pytest.mark.asyncio
async def test_custom_node_manager_route_message(mock_mqtt_client):
    app = KamioApp()
    node = BridgeNode(mock_mqtt_client, "bridge")
    app.register_custom_node("bridge", node)
    # DeviceNode vs CustomNode dispatch depends on manager; route_message should return True when handled.
    handled = await app.custom_nodes.route_message("bridge/cmd/power", b"on")
    assert handled is True
    assert node.received == [("bridge/cmd/power", b"on")]
