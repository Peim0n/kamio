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
    assert node.matches("sensors") is True
    assert node.matches("sensorsroom/temp") is False
    assert node.matches("other/room/temp") is False


def test_custom_node_matches_empty_prefix_is_catch_all(mock_mqtt_client):
    node = BridgeNode(mock_mqtt_client, "///")
    assert node.topic_prefix == ""
    assert node.matches("any/topic") is True
    assert node.matches("") is True
    assert node.matches("/sub") is True


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
    node._subscriptions["bridge/cmd/#"] = 0
    node._is_running = True

    with caplog.at_level("WARNING"):
        node._teardown_subscriptions()

    assert node._subscriptions == {}
    assert node.is_running is False
    assert "Failed to unsubscribe from bridge/cmd/#" in caplog.text


@pytest.mark.asyncio
async def test_unregister_running_node_cleans_subscriptions(mock_mqtt_client):
    """Unregistering a running node schedules stop() + subscription cleanup.

    BridgeNode.stop() is a no-op (it does not call super().stop()); the
    manager must still guarantee unsubscribe + stopped state via its
    _stop_and_teardown finally block.
    """
    import asyncio

    from conftest import wait_until

    app = KamioApp()
    app._loop = asyncio.get_running_loop()
    node = BridgeNode(mock_mqtt_client, "bridge")
    app.register_custom_node("bridge", node)
    await node.start()
    node._is_running = True

    app.unregister_custom_node("bridge")
    assert app.get_custom_node("bridge") is None

    # Wait for the scheduled _stop_and_teardown task to finish.
    assert await wait_until(lambda: not app.custom_nodes._stop_tasks, timeout=2.0)
    mock_mqtt_client.unsubscribe.assert_called_with("bridge/cmd/#")
    assert node.is_running is False
    assert node._subscriptions == {}
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
    node._subscriptions["bridge/cmd/#"] = 0
    node._is_running = True
    app._loop = None  # type: ignore[attr-defined]

    app.unregister_custom_node("bridge")

    assert node._subscriptions == {}
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


def test_custom_node_manager_has_nodes(mock_mqtt_client):
    app = KamioApp()
    assert app.custom_nodes.has_nodes() is False
    app.custom_nodes.register_node("bridge", BridgeNode(mock_mqtt_client, "bridge"))
    assert app.custom_nodes.has_nodes() is True
    app.custom_nodes.unregister_node("bridge")
    assert app.custom_nodes.has_nodes() is False


@pytest.mark.asyncio
async def test_route_message_survives_unregister_during_handle(mock_mqtt_client):
    """unregister_node() inside handle_message must not break routing for the
    remaining nodes — route_message iterates a snapshot of the registry."""
    import asyncio

    app = KamioApp()

    removed = {}

    class SlowNode(CustomNode):
        async def start(self):
            pass

        async def handle_message(self, topic, payload):
            # Unregister both nodes mid-route, then yield so the dict
            # mutation lands while route_message is still iterating.
            app.custom_nodes.unregister_node("n2")
            await asyncio.sleep(0)
            removed["called"] = True

    n1 = SlowNode(mock_mqtt_client, "t")
    n2 = SlowNode(mock_mqtt_client, "t")
    app.register_custom_node("n1", n1)
    app.register_custom_node("n2", n2)

    handled = await app.custom_nodes.route_message("t/x", b"p")
    assert removed.get("called") is True
    assert handled is True  # no RuntimeError; n1 handled it
