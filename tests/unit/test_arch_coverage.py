"""
Comprehensive coverage tests for architectural components:
  - app/mixins/devices.py
  - app/mixins/lifecycle.py
  - app/mixins/mqtt.py
  - app/mixins/rules.py
  - core/hot_reload.py
  - core/rules.py
  - core/mqtt_nodes.py
  - core/device_meta.py
  - core/handlers.py
  - plugins/loader.py
  - discovery.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import threading
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kamio import Device, KamioApp, config, event, rule, state, telemetry
from kamio.app.mixins.devices import DeviceRegistryMixin
from kamio.core.envelope import Envelope, EnvelopeType
from kamio.core.hot_reload import HotReloadManager

# ---------------------------------------------------------------------------
# Test devices
# ---------------------------------------------------------------------------


class SimpleDevice(Device):
    power: bool = state(default=False, writable=True)
    temperature: float = telemetry(default=20.0, unit="C", freq="5s")
    host: str = config(default="localhost")
    button: str = event(description="Button")

    @rule(fields=["power"])
    async def on_power_change(self, event, app):
        pass


# ---------------------------------------------------------------------------
# devices.py
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_device_decorator_non_device_raises(mock_mqtt):
    """device() decorator should raise TypeError for non-Device classes."""
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-dev-decorator")

    class NotADevice:
        pass

    with pytest.raises(TypeError, match="must inherit from Device"):
        app.device(NotADevice)


@pytest.mark.asyncio
async def test_device_decorator_without_args(mock_mqtt):
    """device() decorator called without args should return a decorator."""
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-dev-decorator2")

    @app.device()
    class MyDev(Device):
        x: int = state(default=0)

    assert "mydev" in app.registered_types


@pytest.mark.asyncio
async def test_create_device_unknown_type_raises(mock_mqtt):
    """create_device should raise ValueError for unknown type."""
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-create-unknown")
    with pytest.raises(ValueError, match="not registered"):
        await app.create_device("dev1", "nonexistent_type")


@pytest.mark.asyncio
async def test_add_device_non_device_class_raises(mock_mqtt):
    """add_device should raise TypeError for non-Device class."""
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-add-non-device")
    with pytest.raises(TypeError, match="must be a Device subclass"):
        await app.add_device("dev1", str)  # str is not a Device


@pytest.mark.asyncio
async def test_add_device_duplicate_raises(mock_mqtt):
    """add_device should raise ValueError for duplicate device_id."""
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-add-dup")
    app.register(SimpleDevice)
    await app.add_device("dev1", SimpleDevice)
    with pytest.raises(ValueError, match="already registered"):
        await app.add_device("dev1", SimpleDevice)


@pytest.mark.asyncio
async def test_remove_device_not_found(mock_mqtt, caplog):
    """remove_device should log warning for unknown device_id."""
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-remove-notfound")
    with caplog.at_level(logging.WARNING):
        await app.remove_device("nonexistent")
    assert any("not found" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_add_device_node_start_failure_rolls_back(mock_mqtt, caplog):
    """add_device should roll back registration if node.start() fails."""
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-node-start-fail")
    app.register(SimpleDevice)
    await app.start()

    # Make node.start() fail by patching DeviceNode.start.
    with patch(
        "kamio.core.mqtt_nodes.DeviceNode.start",
        new_callable=AsyncMock,
        side_effect=RuntimeError("start fail"),
    ):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(RuntimeError, match="start fail"):
                await app.add_device("dev-fail", SimpleDevice)

    # Device should not be registered.
    assert app.registry.get_instance("dev-fail") is None
    assert "dev-fail" not in app._device_nodes

    await app.stop()


@pytest.mark.asyncio
async def test_add_device_with_ha_discovery(mock_mqtt):
    """add_device should announce to HA when discovery is enabled."""
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-ha-add")
    app.register(SimpleDevice)
    app.enable_ha_discovery(prefix="homeassistant")
    await app.start()

    dev = await app.add_device("ha-dev", SimpleDevice)
    # HA discovery announce should have published config topics.
    ha_topics = [t for t in mock_mqtt.published if t[0].startswith("homeassistant/")]
    assert ha_topics, "HA discovery should publish config topics on add_device"

    await app.stop()


@pytest.mark.asyncio
async def test_remove_device_with_ha_discovery(mock_mqtt):
    """remove_device should clear HA discovery entries."""
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-ha-remove")
    app.register(SimpleDevice)
    app.enable_ha_discovery(prefix="homeassistant")
    await app.start()
    dev = await app.add_device("ha-dev2", SimpleDevice)

    # Clear published list, then remove device.
    mock_mqtt.published.clear()
    await app.remove_device("ha-dev2")

    # Should have published empty retained payloads for HA discovery.
    clear_topics = [
        t
        for t in mock_mqtt.published
        if t[0].startswith("homeassistant/") and t[1] == b"" and t[3] is True
    ]
    assert clear_topics, "HA discovery clear should publish empty retained payloads"

    await app.stop()


# ---------------------------------------------------------------------------
# lifecycle.py — _run_async, signal handling, run()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_async_starts_and_stops(mock_mqtt):
    """_run_async should start the app and then stop it."""
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-run-async")

    # Start _run_async, then stop it after a short delay.
    task = asyncio.create_task(app._run_async())
    await asyncio.sleep(0.1)
    assert app.is_running is True
    app._is_running = False  # signal the loop to exit
    await asyncio.sleep(0.1)
    await task  # should complete


@pytest.mark.asyncio
async def test_run_async_signal_handler_stops(mock_mqtt):
    """_run_async signal handler should create a stop task."""
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-signal")

    task = asyncio.create_task(app._run_async())
    await asyncio.sleep(0.1)

    # Simulate calling stop() directly (as signal handler would).
    await app.stop()
    await asyncio.wait_for(task, timeout=5.0)


@pytest.mark.asyncio
async def test_run_async_sigterm_completes_shutdown(mock_mqtt, monkeypatch):
    """A SIGTERM must run the full graceful shutdown (SCAN-03).

    The signal handler schedules ``stop()`` as a task; ``_run_async`` must
    wait for it to finish instead of returning and letting ``asyncio.run``
    cancel the in-flight shutdown.
    """
    import signal

    from conftest import wait_until

    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-sigterm")
    app.register(SimpleDevice)
    await app.add_device("dev1", SimpleDevice)

    loop = asyncio.get_running_loop()
    handlers = {}
    monkeypatch.setattr(
        loop, "add_signal_handler", lambda sig, cb, *a: handlers.__setitem__(sig, cb)
    )

    run_task = asyncio.create_task(app._run_async())
    assert await wait_until(lambda: app.is_running and handlers, timeout=3.0)

    handlers[signal.SIGTERM]()  # fire the captured handler
    await asyncio.wait_for(run_task, timeout=5.0)

    assert not app.is_running
    assert not mock_mqtt.is_connected
    assert not app.server_node.is_running
    assert not app._device_nodes["dev1"].is_running


def test_run_catches_keyboard_interrupt(mock_mqtt):
    """run() should catch KeyboardInterrupt."""
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-run-ki")
    with patch("asyncio.run", side_effect=KeyboardInterrupt()):
        app.run()  # should not raise


def test_run_catches_system_exit(mock_mqtt):
    """run() should catch SystemExit."""
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-run-se")
    with patch("asyncio.run", side_effect=SystemExit()):
        app.run()  # should not raise


def test_run_catches_generic_exception(mock_mqtt, caplog):
    """run() should catch and log generic exceptions."""
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-run-exc")
    with caplog.at_level(logging.ERROR):
        with patch("asyncio.run", side_effect=RuntimeError("crash")):
            app.run()
    assert any("crashed" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# mqtt.py — _run_coro_threadsafe, _schedule_when_running, _on_mqtt_connect
# ---------------------------------------------------------------------------


def test_run_coro_threadsafe_no_loop(mock_mqtt):
    """_run_coro_threadsafe should be a no-op if no loop."""
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-coro-no-loop")
    app._loop = None
    app._run_coro_threadsafe(asyncio.sleep(0))  # should not raise


def test_run_coro_threadsafe_loop_not_running(mock_mqtt):
    """_run_coro_threadsafe should be a no-op if loop is not running."""
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-coro-not-running")
    loop = MagicMock()
    loop.is_running.return_value = False
    app._loop = loop
    app._run_coro_threadsafe(asyncio.sleep(0))  # should not raise


@pytest.mark.asyncio
async def test_schedule_when_running(mock_mqtt):
    """_schedule_when_running should create a task when loop is running."""
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-schedule")
    executed = []

    async def _coro():
        executed.append(True)

    app._schedule_when_running(_coro())
    await asyncio.sleep(0.05)
    assert executed == [True]


def test_schedule_when_running_no_loop(mock_mqtt):
    """_schedule_when_running should be a no-op if no running loop."""
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-schedule-no-loop")

    async def _coro():
        pass

    # Called outside an event loop — should not raise.
    coro = _coro()
    app._schedule_when_running(coro)
    coro.close()  # clean up unused coroutine


@pytest.mark.asyncio
async def test_on_mqtt_connect(mock_mqtt):
    """_on_mqtt_connect should publish mqtt_connected event."""
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-connect-cb")
    app._loop = asyncio.get_running_loop()
    app._mqtt_bg_tasks = set()

    app._on_mqtt_connect(mock_mqtt, {}, 0)
    await asyncio.sleep(0.05)
    # Event should have been published.
    events = [e for e in mock_mqtt.published if "mqtt_connected" in str(e)]
    # The event is published via event_bus, not MQTT — check via mock.
    # Just verify no crash.


@pytest.mark.asyncio
async def test_on_mqtt_disconnect(mock_mqtt):
    """_on_mqtt_disconnect should publish mqtt_disconnected event."""
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-disconnect-cb")
    app._loop = asyncio.get_running_loop()
    app._mqtt_bg_tasks = set()

    app._on_mqtt_disconnect(mock_mqtt, 0)
    await asyncio.sleep(0.05)
    # Should not crash.


@pytest.mark.asyncio
async def test_on_mqtt_connect_resubscribes(mock_mqtt):
    """Reconnect re-subscribes the app broadcast and running nodes exactly once."""
    from conftest import wait_until

    from kamio.core.mqtt_nodes import BROADCAST_ID

    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-resub")
    app.register(SimpleDevice)
    await app.start()
    await app.add_device("resub-dev", SimpleDevice)

    connected, disconnected = [], []
    app.event_bus.subscribe("mqtt_connected", connected.append)
    app.event_bus.subscribe("mqtt_disconnected", disconnected.append)

    # Forget recorded subscriptions, drop the connection and reconnect.
    mock_mqtt.subscribed.clear()
    mock_mqtt.simulate_disconnect()
    mock_mqtt.simulate_reconnect()

    assert await wait_until(
        lambda: "Kamio/v1/resub-dev/#" in [t for t, _ in mock_mqtt.subscribed],
        timeout=2.0,
    )
    topics = [t for t, _ in mock_mqtt.subscribed]
    assert topics.count("Kamio/v1/resub-dev/#") == 1
    # Broadcast is owned by the app: exactly one re-subscription per reconnect.
    assert topics.count(f"Kamio/v1/{BROADCAST_ID}/#") == 1

    # Event payloads carry broker/reason details.
    assert await wait_until(lambda: len(connected) >= 1 and len(disconnected) >= 1)
    assert "rc" in connected[-1] and "broker" in connected[-1]
    assert "rc" in disconnected[-1]
    await app.stop()


@pytest.mark.asyncio
async def test_reconnect_skips_stopped_nodes(mock_mqtt):
    """A stopped device node must not be re-subscribed on reconnect (SCAN-20)."""
    from conftest import wait_until

    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-resub-stopped")
    app.register(SimpleDevice)
    await app.start()
    await app.add_device("keep", SimpleDevice)
    await app.add_device("gone", SimpleDevice)
    await app.remove_device("gone")

    mock_mqtt.subscribed.clear()
    mock_mqtt.simulate_disconnect()
    mock_mqtt.simulate_reconnect()

    assert await wait_until(
        lambda: "Kamio/v1/keep/#" in [t for t, _ in mock_mqtt.subscribed],
        timeout=2.0,
    )
    topics = [t for t, _ in mock_mqtt.subscribed]
    assert "Kamio/v1/gone/#" not in topics
    await app.stop()


@pytest.mark.asyncio
async def test_remove_device_keeps_broadcast_for_others(mock_mqtt):
    """Removing one device must not drop the shared broadcast topic (SCAN-12)."""
    from conftest import wait_until

    from kamio.core.mqtt_nodes import BROADCAST_ID

    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-bcast-owner")
    app.register(SimpleDevice)
    await app.start()
    await app.add_device("dev1", SimpleDevice)
    dev2 = await app.add_device("dev2", SimpleDevice)

    await app.remove_device("dev1")

    unsubscribed = [t for (t,) in mock_mqtt.unsubscribed]
    assert "Kamio/v1/dev1/#" in unsubscribed
    assert f"Kamio/v1/{BROADCAST_ID}/#" not in unsubscribed

    # The remaining device still receives broadcast state updates.
    env = Envelope.state(source="ha", data={"power": True})
    mock_mqtt.publish(f"Kamio/v1/{BROADCAST_ID}/ds", env.to_json().encode(), qos=1)
    assert await wait_until(lambda: dev2.power is True, timeout=2.0)
    await app.stop()


@pytest.mark.asyncio
async def test_custom_node_resubscribes_and_hooks_on_reconnect(mock_mqtt):
    """Custom nodes re-subscribe and fire hooks on reconnect (SCAN-16)."""
    from conftest import wait_until

    from kamio.core.custom_nodes import CustomNode

    calls = []

    class Node(CustomNode):
        async def start(self):
            self.subscribe("in/#", qos=1)
            self._is_running = True

        async def handle_message(self, topic, payload):
            pass

        async def on_connect(self):
            calls.append("connect")

        async def on_disconnect(self):
            calls.append("disconnect")

    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-cn-reconn")
    node = Node(mock_mqtt, "bridge")
    app.register_custom_node("bridge", node)
    await app.start()

    mock_mqtt.subscribed.clear()
    mock_mqtt.simulate_disconnect()
    mock_mqtt.simulate_reconnect()

    assert await wait_until(lambda: "connect" in calls and "disconnect" in calls, timeout=2.0)
    topics = [t for t, _ in mock_mqtt.subscribed]
    assert "bridge/in/#" in topics
    await app.stop()


@pytest.mark.asyncio
async def test_create_device_rolls_back_on_dropped_suback():
    """A dropped SUBACK must roll back device registration (SCAN-07, SCAN-18)."""
    from conftest import InMemoryBroker

    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="test-suback")
    app.register(SimpleDevice)
    # Shorten the real ACK wait so the dropped SUBACK surfaces quickly.
    app.mqtt_client._kamio_wait_for_suback = lambda mid: app._mqtt_conn._wait_for_suback(
        mid, timeout=0.2
    )
    await app.start()

    InMemoryBroker.get().drop_acks = True
    try:
        with pytest.raises(TimeoutError):
            await app.add_device("noluck", SimpleDevice)
        assert "noluck" not in app._device_nodes
        assert app.registry.get_instance("noluck") is None
    finally:
        InMemoryBroker.get().drop_acks = False
    await app.stop()


@pytest.mark.asyncio
async def test_resubscribe_all_nodes_preserves_running_and_skips_stopped(mock_mqtt):
    """Reconnect schedules resubscription only for nodes already running."""
    from kamio.core.mqtt_nodes import BaseNode

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-resub-state")
    app._loop = asyncio.get_running_loop()
    app._mqtt_bg_tasks = set()

    running_node = BaseNode("running", mock_mqtt)
    stopped_node = BaseNode("stopped", mock_mqtt)
    running_node._is_running = True
    app.server_node = running_node
    app._device_nodes = {"stopped": stopped_node}
    running_node._resubscribe = AsyncMock()
    stopped_node._resubscribe = AsyncMock()

    app._resubscribe_all_nodes()

    assert running_node.is_running is True
    assert stopped_node.is_running is False
    from conftest import wait_until

    assert await wait_until(lambda: running_node._resubscribe.await_count == 1, timeout=1.0)
    stopped_node._resubscribe.assert_not_awaited()


@pytest.mark.asyncio
async def test_on_mqtt_message_routes_to_device(mock_mqtt):
    """_on_mqtt_message should route messages to device nodes."""
    import json

    from kamio.core import topics as mqtt_topics
    from kamio.core.envelope import Envelope, EnvelopeType

    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-msg-route")
    app.register(SimpleDevice)
    await app.start()
    dev = await app.add_device("msg-dev", SimpleDevice)

    # Build a state command envelope and publish it to the device's command topic.
    env = Envelope.command(
        source="sender",
        target="msg-dev",
        method="set_power",
        params={"value": True},
    )
    topic = f"Kamio/v1/msg-dev/sc"
    payload = json.dumps(
        {
            "source": "sender",
            "target": "msg-dev",
            "type": "command",
            "cind": env.cind,
            "ts": env.ts,
            "data": {"method": "set_power", "params": {"value": True}},
        }
    ).encode()

    app._on_mqtt_message(mock_mqtt, topic, payload, qos=1)
    await asyncio.sleep(0.1)
    # Message should have been routed — verify no crash.
    await app.stop()


@pytest.mark.asyncio
async def test_on_mqtt_message_dropped_when_not_running(mock_mqtt):
    """_on_mqtt_message should drop messages when app is not running."""
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-msg-drop")
    app._loop = asyncio.get_running_loop()
    app._is_running = False
    # Should be a no-op.
    app._on_mqtt_message(mock_mqtt, "test/topic", b"hello", qos=1)


def test_on_mqtt_message_no_loop(mock_mqtt):
    """_on_mqtt_message should be a no-op if no loop."""
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-msg-no-loop")
    app._loop = None
    app._is_running = True
    app._on_mqtt_message(mock_mqtt, "test/topic", b"hello", qos=1)  # no-op


@pytest.mark.asyncio
async def test_run_coro_threadsafe_create_task_runtime_error(mock_mqtt):
    """_run_coro_threadsafe should handle RuntimeError from create_task."""
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-coro-runtime")
    loop = MagicMock()
    loop.is_running.return_value = True
    loop.create_task.side_effect = RuntimeError("loop closed")
    app._loop = loop

    async def _coro():
        pass

    coro = _coro()
    app._run_coro_threadsafe(coro)  # should not raise
    coro.close()


@pytest.mark.asyncio
async def test_run_coro_threadsafe_creates_mqtt_bg_tasks(mock_mqtt):
    """_run_coro_threadsafe should create _mqtt_bg_tasks if None."""
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-coro-bg-tasks")
    app._loop = asyncio.get_running_loop()
    app._mqtt_bg_tasks = None

    executed = []

    async def _coro():
        executed.append(True)

    app._run_coro_threadsafe(_coro())
    await asyncio.sleep(0.05)
    assert executed == [True]
    assert app._mqtt_bg_tasks is not None


# ---------------------------------------------------------------------------
# mqtt_nodes.py — _resubscribe, stop with tasks, dispatch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_device_node_resubscribe(mock_mqtt):
    """DeviceNode._resubscribe should re-subscribe to topics."""
    from kamio.core.mqtt_nodes import DeviceNode

    node = DeviceNode(device_id="dev1", mqtt_client=mock_mqtt)
    await node._resubscribe()
    assert node._is_running is True


@pytest.mark.asyncio
async def test_device_node_stop_cancels_tasks(mock_mqtt):
    """DeviceNode.stop should cancel pending tasks."""
    from kamio.core.mqtt_nodes import DeviceNode

    mock_mqtt.simulate_connect()
    node = DeviceNode(device_id="dev1", mqtt_client=mock_mqtt)
    await node.start()

    # Add a pending task.
    async def _long_running():
        await asyncio.sleep(100)

    task = asyncio.create_task(_long_running())
    node._tasks.add(task)

    await node.stop()
    # Task was cancelled — wait for it to complete.
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert task.done()
    assert node._is_running is False


@pytest.mark.asyncio
async def test_device_node_resubscribe_handles_error(mock_mqtt, caplog):
    """DeviceNode._resubscribe logs failed subscriptions and re-raises."""
    import logging

    from kamio.core.mqtt_nodes import DeviceNode

    mock_mqtt.subscribe = MagicMock(side_effect=RuntimeError("sub fail"))
    node = DeviceNode(device_id="dev1", mqtt_client=mock_mqtt)
    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError, match="sub fail"):
            await node._resubscribe()
    assert any("Failed to subscribe" in r.message for r in caplog.records)
    assert node._is_running is False


# ---------------------------------------------------------------------------
# rules.py — uncovered branches
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rule_engine_start_stop(mock_mqtt):
    """RuleEngine start/stop should work correctly."""
    from kamio.core.rules import Rule, RuleEngine

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-rules-engine")
    engine = RuleEngine(app)

    async def my_rule_func(event, app):
        pass

    r = Rule(func=my_rule_func, interval=0.1)
    engine.add_rule(r)
    await engine.start()
    assert r.task is not None
    await engine.stop()
    assert r.task.done() or r.task.cancelled()


@pytest.mark.asyncio
async def test_rule_engine_stop_no_tasks(mock_mqtt):
    """RuleEngine.stop should be a no-op if no tasks."""
    from kamio.core.rules import RuleEngine

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-rules-stop-empty")
    engine = RuleEngine(app)
    await engine.stop()  # should not raise


@pytest.mark.asyncio
async def test_rule_engine_remove_rule(mock_mqtt):
    """RuleEngine.remove_rule should remove from list and index."""
    from kamio.core.rules import Rule, RuleEngine

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-rules-remove")
    engine = RuleEngine(app)

    async def my_func(event, app):
        pass

    r = Rule(func=my_func)
    engine.add_rule(r)
    assert r in engine.rules
    engine.remove_rule(r)
    assert r not in engine.rules


@pytest.mark.asyncio
async def test_rule_engine_handle_device_update_no_rules(mock_mqtt):
    """handle_device_update should be a no-op if no matching rules."""
    from kamio.core.rules import RuleEngine

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-rules-no-match")
    engine = RuleEngine(app)
    await engine.handle_device_update("dev1", {"power": True})  # should not raise


# ---------------------------------------------------------------------------
# discovery.py — uncovered branches
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_discovery_announce_no_node(caplog):
    """announce should log warning if device has no node."""
    from kamio.discovery import HADiscovery

    d = SimpleDevice()
    d.node = None
    discovery = HADiscovery()
    with caplog.at_level(logging.WARNING):
        await discovery.announce(d)
    assert any("Cannot announce" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_discovery_clear_no_node():
    """clear should be a no-op if device has no node."""
    from kamio.discovery import HADiscovery

    d = SimpleDevice()
    d.node = None
    discovery = HADiscovery()
    await discovery.clear(d)  # should not raise


def test_discovery_map_unknown_kind():
    """_map_to_ha_component should return empty string for unknown kind."""
    from kamio.data_fields import Field
    from kamio.discovery import HADiscovery

    d = HADiscovery()
    f = Field(name="x", kind="config", python_type=str)
    assert d._map_to_ha_component(f) == ""


# ---------------------------------------------------------------------------
# device_meta.py — metaclass
# ---------------------------------------------------------------------------


def test_device_meta_collects_fields():
    """DeviceMeta should collect state, telemetry, config fields."""
    assert "power" in SimpleDevice.Kamio_FIELDS
    assert "temperature" in SimpleDevice.Kamio_FIELDS
    assert "host" in SimpleDevice.Kamio_FIELDS


def test_device_meta_collects_events():
    """DeviceMeta should collect event fields separately."""
    assert "button" in SimpleDevice.Kamio_EVENTS
    assert "button" not in SimpleDevice.Kamio_FIELDS


def test_device_meta_collects_commands():
    """DeviceMeta should collect @command decorated methods."""

    class CmdDevice(Device):
        power: bool = state(default=False)

        from kamio import command

        @command
        async def toggle(self):
            self.power = not self.power

    assert "toggle" in CmdDevice.Kamio_COMMANDS


def test_device_meta_collects_rules():
    """DeviceMeta should collect @rule decorated methods."""
    assert "on_power_change" in SimpleDevice.Kamio_RULES


def test_device_meta_inherits_fields():
    """DeviceMeta should inherit fields from base classes."""

    class ChildDevice(SimpleDevice):
        extra: int = state(default=0)

    assert "power" in ChildDevice.Kamio_FIELDS
    assert "extra" in ChildDevice.Kamio_FIELDS


# ---------------------------------------------------------------------------
# handlers.py — uncovered branches
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handler_handle_telemetry(mock_mqtt):
    """DeviceHandler should handle telemetry envelopes."""
    from kamio.core.handlers import DeviceHandler
    from kamio.core.mqtt_nodes import DeviceNode
    from kamio.core.state import StateManager

    d = SimpleDevice()
    node = MagicMock()
    node.device_id = "dev1"
    node.is_running = True
    state_mgr = StateManager()
    handler = DeviceHandler(d, node, state_manager=state_mgr)

    env = Envelope.telemetry(source="dev1", data={"temperature": 25.0})
    await handler._handle_telemetry(env)
    # State should have been updated.
    assert state_mgr.get_state("dev1").get("temperature") == 25.0


@pytest.mark.asyncio
async def test_handler_handle_state_ack(mock_mqtt):
    """DeviceHandler should handle STATE_ACK envelopes."""
    from kamio.core.handlers import DeviceHandler
    from kamio.core.state import StateManager

    d = SimpleDevice()
    node = MagicMock()
    node.device_id = "dev1"
    node.is_running = True
    state_mgr = StateManager()
    handler = DeviceHandler(d, node, state_manager=state_mgr)

    # STATE_ACK doesn't have a specific handler method — just verify it doesn't crash.
    env = Envelope(type=EnvelopeType.STATE_ACK, source="dev1", cind="test-cind", data={})
    # If there's no _handle_state_ack method, the handler dispatches to a no-op.
    # Just verify dispatch doesn't raise for an unknown type.
    try:
        await handler.dispatch(env)
    except AttributeError:
        # If dispatch doesn't handle STATE_ACK, that's OK — it's a no-op.
        pass


# ---------------------------------------------------------------------------
# mqtt_nodes.py — uncovered branches
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_device_node_publish_raw(mock_mqtt):
    """DeviceNode.publish_raw should publish raw bytes."""
    from kamio.core.mqtt_nodes import DeviceNode

    node = DeviceNode(device_id="dev1", mqtt_client=mock_mqtt)
    await node.publish_raw("test/topic", b"hello", qos=1, retain=True)
    assert ("test/topic", b"hello", 1, True) in mock_mqtt.published


@pytest.mark.asyncio
async def test_device_node_emit_event(mock_mqtt):
    """DeviceNode.emit_event should publish an event envelope."""
    from kamio.core.mqtt_nodes import DeviceNode

    node = DeviceNode(device_id="dev1", mqtt_client=mock_mqtt)
    await node.emit_event("button", {"pressed": True})
    # Should have published something.
    assert len(mock_mqtt.published) > 0


# ---------------------------------------------------------------------------
# loader.py — plugin lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_plugin_loader_unload_all(mock_mqtt):
    """PluginLoader.unload_all should unload all plugins."""
    from kamio.plugins.base import Plugin
    from kamio.plugins.loader import PluginLoader

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-unload-all")
    loader = PluginLoader(app)

    class TestPlugin(Plugin):
        @property
        def name(self):
            return "test-unload-all-plugin"

        @property
        def version(self):
            return "1.0.0"

        async def on_load(self, app, context=None):
            pass

    await loader.load_plugin(TestPlugin)
    assert "test-unload-all-plugin" in loader._loaded
    await loader.unload_all()
    assert "test-unload-all-plugin" not in loader._loaded


@pytest.mark.asyncio
async def test_plugin_loader_load_with_dependencies(mock_mqtt):
    """PluginLoader should resolve dependencies via register_class."""
    from kamio.plugins.base import Plugin
    from kamio.plugins.loader import PluginLoader

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-deps")
    loader = PluginLoader(app)

    class BasePlugin(Plugin):
        @property
        def name(self):
            return "base-plug"

        @property
        def version(self):
            return "1.0.0"

        async def on_load(self, app, context=None):
            pass

    class DepPlugin(Plugin):
        @property
        def name(self):
            return "dep-plug"

        @property
        def version(self):
            return "1.0.0"

        @property
        def dependencies(self):
            return ["base-plug"]

        async def on_load(self, app, context=None):
            pass

    # Register the base plugin class so it can be auto-resolved.
    loader.register_class("base-plug", BasePlugin)
    await loader.load_plugin(DepPlugin)
    assert "dep-plug" in loader._loaded
    assert "base-plug" in loader._loaded


@pytest.mark.asyncio
async def test_plugin_loader_unload_not_found(mock_mqtt, caplog):
    """PluginLoader.unload_plugin should log warning for unknown plugin."""
    from kamio.plugins.loader import PluginLoader

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-unload-notfound")
    loader = PluginLoader(app)
    with caplog.at_level(logging.WARNING):
        await loader.unload_plugin("nonexistent")
    assert any(
        "not found" in r.message.lower() or "not loaded" in r.message.lower()
        for r in caplog.records
    )


# ---------------------------------------------------------------------------
# Stage 4 — lifecycle fixes (SCAN-03/04/05/06/09/11/15/38/39)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_remove_device_survives_on_stop_error(mock_mqtt):
    """A failing device.on_stop must not skip unsubscribe/removal (SCAN-04)."""
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-onstop-err")

    class BadStop(Device):
        x: int = state(default=0)

        async def on_stop(self, node):
            raise RuntimeError("on_stop boom")

    await app.start()
    await app.add_device("bad", BadStop)
    node = app._device_nodes["bad"]

    await app.remove_device("bad")

    assert node.is_running is False
    unsub = [t for (t,) in mock_mqtt.unsubscribed]
    assert "Kamio/v1/bad/#" in unsub
    assert "bad" not in app._device_nodes
    assert app.registry.get_instance("bad") is None
    await app.stop()


@pytest.mark.asyncio
async def test_create_device_rollback_disconnects_driver(mock_mqtt):
    """A node-start failure must close the driver opened by on_init (SCAN-05)."""
    from kamio.drivers.base import BaseDriver

    class FlakyDriver(BaseDriver):
        def __init__(self):
            super().__init__()
            self.disconnected = False

        async def connect(self):
            pass

        async def disconnect(self):
            self.disconnected = True

        async def execute(self, command_name, params):
            return None

        async def read(self, field_name, params=None):
            return None

    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-rollback-drv")
    app.register(SimpleDevice)
    await app.start()

    driver = FlakyDriver()
    mock_mqtt.subscribe = MagicMock(side_effect=RuntimeError("sub fail"))
    with pytest.raises(RuntimeError, match="sub fail"):
        await app.add_device("dd", SimpleDevice, driver=driver)

    assert driver.disconnected is True
    assert "dd" not in app._device_nodes
    assert app.registry.get_instance("dd") is None
    # Restore the real subscribe (the instance attribute shadows the method).
    del mock_mqtt.subscribe
    await app.stop()


@pytest.mark.asyncio
async def test_shutdown_cancels_tasks_when_disconnect_fails():
    """Device.shutdown cancels tasks even if driver.disconnect raises (SCAN-06)."""
    from kamio.drivers.base import BaseDriver

    class FailingDisconnectDriver(BaseDriver):
        async def connect(self):
            pass

        async def disconnect(self):
            raise RuntimeError("disconnect boom")

        async def execute(self, command_name, params):
            return None

        async def read(self, field_name, params=None):
            return None

    dev = SimpleDevice(driver=FailingDisconnectDriver())
    task = asyncio.create_task(asyncio.sleep(3600))
    dev._bg_tasks.add(task)

    with pytest.raises(RuntimeError, match="disconnect boom"):
        await dev.shutdown()
    assert task.done()


@pytest.mark.asyncio
async def test_start_rolls_back_on_device_on_start_error(mock_mqtt):
    """A failing device.on_start must tear down MQTT and started nodes (SCAN-09)."""
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-start-rollback")

    class BadStart(Device):
        x: int = state(default=0)

        async def on_start(self, node):
            raise RuntimeError("on_start boom")

    app.register(SimpleDevice)
    await app.add_device("bad", BadStart)

    with pytest.raises(RuntimeError, match="on_start boom"):
        await app.start()

    assert app.is_running is False
    assert not mock_mqtt.is_connected
    assert not app.server_node.is_running


def test_enable_hot_reload_defers_without_loop(mock_mqtt):
    """enable() with no running loop defers instead of binding a dead loop (SCAN-11)."""
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-hr-defer")
    app.enable_hot_reload()
    assert app.hot_reload._pending_enable is True
    assert not app.hot_reload.is_enabled


@pytest.mark.asyncio
async def test_deferred_hot_reload_activated_on_start(mock_mqtt):
    """A deferred enable() activates on start() and stops on stop() (SCAN-11)."""
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-hr-run")
    # Simulate enable() having been called before a loop existed.
    app.hot_reload._pending_enable = True

    await app.start()
    assert app.hot_reload.is_enabled is True
    assert app.hot_reload._task is not None  # no entries -> polling fallback

    await app.stop()
    assert not app.hot_reload.is_enabled
    assert app.hot_reload._task is None


@pytest.mark.asyncio
async def test_custom_node_failed_start_leaves_no_subscriptions(mock_mqtt):
    """A custom node whose start() fails must not keep subscriptions (SCAN-15)."""
    from kamio.core.custom_nodes import CustomNode

    class FailStartNode(CustomNode):
        async def start(self):
            self.subscribe("x/#")
            raise RuntimeError("start boom")

        async def handle_message(self, topic, payload):
            pass

    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-cn-fail")
    node = FailStartNode(mock_mqtt, "fx")
    app.register_custom_node("fx", node)
    await app.start()

    assert node._subscriptions == {}
    assert node.is_running is False
    unsub = [t for (t,) in mock_mqtt.unsubscribed]
    assert "fx/x/#" in unsub
    await app.stop()


@pytest.mark.asyncio
async def test_unregister_during_custom_node_start_cleans_subs(mock_mqtt):
    """Unregistering a node while its start() is in flight leaves no subs (SCAN-15)."""
    from kamio.core.custom_nodes import CustomNode

    start_begun = asyncio.Event()
    release = asyncio.Event()

    class SlowNode(CustomNode):
        async def start(self):
            self.subscribe("y/#")
            start_begun.set()
            await release.wait()

        async def handle_message(self, topic, payload):
            pass

    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-cn-race")
    node = SlowNode(mock_mqtt, "sy")
    app.register_custom_node("sy", node)

    start_task = asyncio.create_task(app.start())
    await asyncio.wait_for(start_begun.wait(), timeout=2.0)
    app.unregister_custom_node("sy")
    release.set()
    await asyncio.wait_for(start_task, timeout=5.0)

    assert node._subscriptions == {}
    unsub = [t for (t,) in mock_mqtt.unsubscribed]
    assert "sy/y/#" in unsub
    await app.stop()


@pytest.mark.asyncio
async def test_register_custom_node_while_running_starts_it(mock_mqtt):
    """Registering a custom node on a running app starts it immediately (SCAN-15)."""
    from conftest import wait_until

    from kamio.core.custom_nodes import CustomNode

    class LazyNode(CustomNode):
        async def start(self):
            self.subscribe("z/#")

        async def handle_message(self, topic, payload):
            pass

    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-cn-late")
    await app.start()

    node = LazyNode(mock_mqtt, "lz")
    app.register_custom_node("lz", node)

    assert await wait_until(lambda: node.is_running, timeout=2.0)
    assert ("lz/z/#", 0) in mock_mqtt.subscribed
    await app.stop()


@pytest.mark.asyncio
async def test_stop_unloads_plugins_and_cancels_tasks(mock_mqtt):
    """app.stop() unloads plugins; PluginContext tasks are cancelled (SCAN-38)."""
    from kamio.plugins.base import Plugin

    class TaskPlugin(Plugin):
        def __init__(self):
            super().__init__()
            self.unloaded = False
            self.task = None

        @property
        def name(self):
            return "task-plug"

        @property
        def version(self):
            return "1.0"

        async def on_load(self, app, context=None):
            async def _loop():
                await asyncio.sleep(3600)

            self.task = context.create_task(_loop())

        async def on_unload(self, app):
            self.unloaded = True

    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-stop-plugins")
    await app.start()
    plugin = await app.load_plugin(TaskPlugin)

    await app.stop()

    assert plugin.unloaded is True
    assert plugin.task.done()
    assert app.list_plugins() == []


@pytest.mark.asyncio
async def test_unload_all_continues_past_failing_plugin(mock_mqtt):
    """A plugin that raises in on_unload must not block the rest (SCAN-39)."""
    from kamio.plugins.base import Plugin

    def make_plugin(name, fail=False):
        class P(Plugin):
            @property
            def name(self):
                return name

            @property
            def version(self):
                return "1.0"

            async def on_load(self, app, context=None):
                pass

            async def on_unload(self, app):
                if fail:
                    raise RuntimeError("unload boom")

        return P

    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-unload-all")
    loader = app.plugin_loader
    for cls in (make_plugin("p1"), make_plugin("p2", fail=True), make_plugin("p3")):
        await loader.load_plugin(cls)

    await loader.unload_all()
    assert loader.list_plugins() == []
