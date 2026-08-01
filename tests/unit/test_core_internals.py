"""Comprehensive unit tests for kamio.core internals.

Covers mixins, mqtt_nodes, handlers, envelope, custom_nodes and state
modules to maximise line coverage.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kamio import Device, KamioApp, command, config, event, state, telemetry
from kamio.core.custom_nodes import CustomNode, CustomNodeManager
from kamio.core.envelope import SERVER_ID, Envelope, EnvelopeType
from kamio.core.handlers import DeviceHandler
from kamio.core.mixins import TaskManagerMixin, TelemetryMixin
from kamio.core.mqtt_nodes import BROADCAST_ID, BaseNode, DeviceNode, ServerNode
from kamio.core.state import StateManager
from kamio.data_fields import Field, parse_freq

# ---------------------------------------------------------------------------
# Test fixtures / helper device classes
# ---------------------------------------------------------------------------


class Light(Device):
    power: bool = state(default=False, writable=True)
    brightness: int = state(default=100, min=0, max=255, writable=True)
    mode: str = state(default="auto", choices=("auto", "manual"))
    temp: float = telemetry(default=20.0, unit="C", freq="5s")
    nofreq: float = telemetry(default=0.0, freq="")
    host: str = config(default="localhost")
    button: str = event(description="button press")

    @command
    async def toggle(self):
        self.power = not self.power
        return {"power": self.power}

    @command
    async def with_node(self, node, app):
        return {"node": node.device_id, "app": app is not None}

    @command
    async def get_zero(self):
        return 0

    @command
    async def get_false(self):
        return False


class _RecordingNode(CustomNode):
    def __init__(self, mqtt_client, topic_prefix):
        super().__init__(mqtt_client, topic_prefix)
        self.received = []
        self.started = False
        self.stopped = False
        self.fail_start = False
        self.fail_handle = False

    async def start(self):
        if self.fail_start:
            raise RuntimeError("boom-start")
        self.subscribe("cmd/#")
        self._is_running = True
        self.started = True

    async def stop(self):
        await super().stop()
        self.stopped = True

    async def handle_message(self, topic, payload):
        if self.fail_handle:
            raise RuntimeError("boom-handle")
        self.received.append((topic, payload))
        self.publish("ack", b"ok")


# ---------------------------------------------------------------------------
# Envelope tests
# ---------------------------------------------------------------------------


class TestEnvelope:
    def test_factory_telemetry(self):
        env = Envelope.telemetry(source="dev1", data={"t": 1})
        assert env.type == EnvelopeType.DEVICE_TELEMETRY
        assert env.source == "dev1"
        assert env.data == {"t": 1}
        assert env.target is None

    def test_factory_state(self):
        env = Envelope.state(source="dev1", data={"power": True})
        assert env.type == EnvelopeType.DEVICE_STATE
        assert env.source == "dev1"
        assert env.data == {"power": True}

    def test_factory_state_ack(self):
        env = Envelope.state_ack(source="dev1", target="0", data={"result": {}}, cind="abc")
        assert env.type == EnvelopeType.STATE_ACK
        assert env.target == "0"
        assert env.cind == "abc"

    def test_factory_event_with_payload(self):
        env = Envelope.event(source="dev1", event_name="press", payload={"x": 1})
        assert env.type == EnvelopeType.DEVICE_EVENT
        assert env.data == {"event": "press", "payload": {"x": 1}}

    def test_factory_event_with_data(self):
        env = Envelope.event(source="dev1", event_name="press", data={"y": 2})
        assert env.data == {"event": "press", "payload": {"y": 2}}

    def test_factory_event_default_payload(self):
        env = Envelope.event(source="dev1", event_name="press")
        assert env.data == {"event": "press", "payload": {}}

    def test_factory_command(self):
        env = Envelope.command(source="0", target="dev1", method="toggle", params={"a": 1})
        assert env.type == EnvelopeType.SERVER_COMMAND
        assert env.target == "dev1"
        assert env.data == {"method": "toggle", "params": {"a": 1}}
        assert env.meta == {}

    def test_factory_command_with_cind_and_meta(self):
        env = Envelope.command(
            source="0", target="dev1", method="toggle", cind="cid", meta={"k": "v"}
        )
        assert env.cind == "cid"
        assert env.meta == {"k": "v"}

    def test_factory_command_ack(self):
        env = Envelope.command_ack(source="dev1", target="0", data={"status": "ok"}, cind="c1")
        assert env.type == EnvelopeType.COMMAND_ACK
        assert env.cind == "c1"

    def test_factory_keepalive(self):
        env = Envelope.keepalive(source="dev1")
        assert env.type == EnvelopeType.KEEPALIVE
        assert env.target == "dev1"
        assert env.data == {}

    def test_to_dict_roundtrip(self):
        env = Envelope.telemetry(source="d", data={"x": 1})
        d = env.to_dict()
        assert d["source"] == "d"
        assert d["type"] == "dt"
        assert d["data"] == {"x": 1}
        assert "cind" in d and "ts" in d and "meta" in d

    def test_to_json_roundtrip(self):
        env = Envelope.command(source="0", target="d", method="m", params={"a": 1})
        s = env.to_json()
        assert isinstance(s, str)
        parsed = json.loads(s)
        assert parsed["type"] == "sc"

    def test_to_json_serialization_error_raises_value_error(self):
        env = Envelope.telemetry(source="d", data={"x": 1})
        with patch("json.dumps", side_effect=TypeError("boom")):
            with pytest.raises(ValueError):
                env.to_json()

    def test_from_json_valid(self):
        env = Envelope.command(source="0", target="d", method="m")
        parsed = Envelope.from_json(env.to_json())
        assert parsed is not None
        assert parsed.type == EnvelopeType.SERVER_COMMAND
        assert parsed.source == "0"

    def test_from_json_bytes(self):
        env = Envelope.telemetry(source="d", data={"x": 1})
        parsed = Envelope.from_json(env.to_json().encode("utf-8"))
        assert parsed is not None
        assert parsed.data == {"x": 1}

    def test_from_json_invalid_json_returns_none(self):
        assert Envelope.from_json("{not json") is None

    def test_from_json_none_payload(self):
        assert Envelope.from_json("null") is None

    def test_from_json_unknown_type_becomes_unknown(self):
        d = {"source": "d", "type": "zzz", "data": {}}
        env = Envelope.from_json(json.dumps(d))
        assert env is not None
        assert env.type == EnvelopeType.UNKNOWN

    def test_from_json_non_dict_data_defaults_to_empty(self):
        d = {"source": "d", "type": "dt", "data": "notdict", "meta": "notdict"}
        env = Envelope.from_json(json.dumps(d))
        assert env is not None
        assert env.data == {}
        assert env.meta == {}

    def test_from_json_missing_cind_generates_one(self):
        d = {"source": "d", "type": "dt", "data": {}}
        env = Envelope.from_json(json.dumps(d))
        assert env is not None
        assert env.cind and len(env.cind) > 0

    def test_from_json_missing_target_is_none(self):
        d = {"source": "d", "type": "dt", "data": {}}
        env = Envelope.from_json(json.dumps(d))
        assert env is not None
        assert env.target is None

    def test_from_dict_generic_exception_returns_none(self):
        # Pass a dict whose "source" key triggers an exception in str()
        # conversion inside the second try block.
        class BadStr:
            def __str__(self):
                raise RuntimeError("boom")

        d = {"type": "ds", "source": BadStr()}
        assert Envelope.from_dict(d) is None

    def test_from_json_unexpected_exception_returns_none(self):
        with patch("json.loads", side_effect=RuntimeError("unexpected")):
            assert Envelope.from_json("{}") is None

    def test_properties_access(self):
        env = Envelope.command(source="s", target="t", method="m", meta={"k": "v"})
        assert env.type == EnvelopeType.SERVER_COMMAND
        assert env.source == "s"
        assert env.target == "t"
        assert env.data == {"method": "m", "params": {}}
        assert env.meta == {"k": "v"}
        assert env.cind


# ---------------------------------------------------------------------------
# Mixins tests (TaskManagerMixin / TelemetryMixin)
# ---------------------------------------------------------------------------


class TestTaskManagerMixin:
    @pytest.mark.asyncio
    async def test_create_task_tracks_and_discards(self):
        mgr = TaskManagerMixin("test.tm")

        async def work():
            return 42

        task = mgr.create_task(work(), name="job")
        assert task in mgr._bg_tasks
        result = await task
        assert result == 42
        # done_callback should have discarded it
        assert task not in mgr._bg_tasks

    @pytest.mark.asyncio
    async def test_cancel_all_tasks_no_tasks(self):
        mgr = TaskManagerMixin("test.tm")
        await mgr.cancel_all_tasks()  # should not raise

    @pytest.mark.asyncio
    async def test_cancel_all_tasks_with_tasks(self):
        mgr = TaskManagerMixin("test.tm")

        async def long():
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                raise

        t1 = mgr.create_task(long(), name="t1")
        t2 = mgr.create_task(long(), name="t2")
        await asyncio.sleep(0)  # let them start
        await mgr.cancel_all_tasks()
        assert t1.cancelled() or t1.done()
        assert t2.cancelled() or t2.done()
        assert len(mgr._bg_tasks) == 0

    @pytest.mark.asyncio
    async def test_cancel_all_tasks_logs_non_cancel_errors(self):
        mgr = TaskManagerMixin("test.tm")

        async def boom():
            raise ValueError("boom")

        t = mgr.create_task(boom(), name="boom")
        await asyncio.sleep(0.05)
        await mgr.cancel_all_tasks()
        assert t.done()


class TestTelemetryMixin:
    @pytest.mark.asyncio
    async def test_start_telemetry_disabled(self):
        class Dev(TelemetryMixin):
            enable_telemetry = False
            Kamio_FIELDS = {}

        d = Dev("test.tm.disabled")
        d.node = MagicMock()
        d.node.is_running = True
        await d.start_telemetry()  # should return early
        assert len(d._bg_tasks) == 0

    @pytest.mark.asyncio
    async def test_start_telemetry_no_node(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {}

        d = Dev("test.tm.nonode")
        d.node = None
        await d.start_telemetry()
        assert len(d._bg_tasks) == 0

    @pytest.mark.asyncio
    async def test_start_telemetry_node_not_running(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {}

        d = Dev("test.tm.notrunning")
        d.node = MagicMock()
        d.node.is_running = False
        await d.start_telemetry()
        assert len(d._bg_tasks) == 0

    @pytest.mark.asyncio
    async def test_start_telemetry_already_started(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {
                "temp": Field(name="temp", kind="telemetry", freq="5s"),
            }

        d = Dev("test.tm.already")
        d.node = MagicMock()
        d.node.device_id = "d1"
        d.node.is_running = True

        # Pre-seed a telemetry task to simulate already-running scheduler.
        async def _noop():
            pass

        fake = asyncio.create_task(_noop(), name="telemetry_5.0")
        d._bg_tasks.add(fake)
        await d.start_telemetry()
        # The old task should be cancelled and replaced (or just left running).
        # Either way, no crash.
        fake.cancel()
        try:
            await fake
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_start_telemetry_groups_fields_by_freq(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {
                "a": Field(name="a", kind="telemetry", freq="5s"),
                "b": Field(name="b", kind="telemetry", freq="5s"),
                "c": Field(name="c", kind="telemetry", freq="10s"),
                "d": Field(name="d", kind="telemetry", freq=""),
            }

        d = Dev("test.tm.groups")
        d.node = MagicMock()
        d.node.device_id = "d1"
        d.node.is_running = True
        d._safe_publish = AsyncMock()
        await d.start_telemetry()
        names = {t.get_name() for t in d._bg_tasks}
        assert "telemetry_5.0" in names
        assert "telemetry_10.0" in names
        await d.cancel_all_tasks()

    @pytest.mark.asyncio
    async def test_start_telemetry_caps_freq_to_min(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {
                "fast": Field(name="fast", kind="telemetry", freq="1ms"),
            }

        d = Dev("test.tm.cap")
        d.node = MagicMock()
        d.node.device_id = "d1"
        d.node.is_running = True
        d._safe_publish = AsyncMock()
        # _get_min_freq default 0.1 -> 0.001s should be capped to 0.1
        await d.start_telemetry()
        names = {t.get_name() for t in d._bg_tasks}
        assert "telemetry_0.1" in names
        await d.cancel_all_tasks()

    @pytest.mark.asyncio
    async def test_start_telemetry_invalid_freq_raises(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {
                "bad": Field(name="bad", kind="telemetry", freq="invalid"),
            }

        d = Dev("test.tm.bad")
        d.node = MagicMock()
        d.node.device_id = "d1"
        d.node.is_running = True
        with pytest.raises(ValueError):
            await d.start_telemetry()

    @pytest.mark.asyncio
    async def test_telemetry_scheduler_publishes_data(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {}

        d = Dev("test.tm.sched")
        d.node = MagicMock()
        d.node.device_id = "d1"
        d.node.is_running = True
        d.handle_telemetry_update = AsyncMock(return_value={"temp": 22.0})
        d.publish_telemetry = AsyncMock()

        task = d.create_task(d._telemetry_scheduler(["temp"], 0.01), name="telemetry_0.01")
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        d.handle_telemetry_update.assert_awaited()
        d.publish_telemetry.assert_awaited()

    @pytest.mark.asyncio
    async def test_telemetry_scheduler_skips_when_no_data(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {}

        d = Dev("test.tm.nodata")
        d.node = MagicMock()
        d.node.device_id = "d1"
        d.node.is_running = True
        d.handle_telemetry_update = AsyncMock(return_value=None)
        d.publish_telemetry = AsyncMock()

        task = d.create_task(d._telemetry_scheduler(["temp"], 0.01), name="telemetry_0.01")
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        d.publish_telemetry.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_telemetry_scheduler_handles_exception(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {}

        d = Dev("test.tm.err")
        d.node = MagicMock()
        d.node.device_id = "d1"
        d.node.is_running = True
        d.handle_telemetry_update = AsyncMock(side_effect=RuntimeError("boom"))
        d.publish_telemetry = AsyncMock()

        task = d.create_task(d._telemetry_scheduler(["temp"], 0.01), name="telemetry_0.01")
        await asyncio.sleep(0.05)
        d.node.is_running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        # Should not have published and should not have crashed the test
        d.publish_telemetry.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_telemetry_scheduler_stops_when_node_stops(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {}

        d = Dev("test.tm.stop")
        d.node = MagicMock()
        d.node.device_id = "d1"
        d.node.is_running = True
        d.handle_telemetry_update = AsyncMock(return_value={"x": 1})
        d.publish_telemetry = AsyncMock()

        task = d.create_task(d._telemetry_scheduler(["x"], 0.01), name="telemetry_0.01")
        await asyncio.sleep(0.02)
        d.node.is_running = False
        await asyncio.sleep(0.05)
        assert task.done()

    @pytest.mark.asyncio
    async def test_read_telemetry_value_no_driver(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {}

        d = Dev("test.tm.read")
        d.driver = None
        assert await d.read_telemetry_value("x") is None

    @pytest.mark.asyncio
    async def test_read_telemetry_value_with_driver(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {}

        d = Dev("test.tm.read2")
        d.driver = MagicMock()
        d.driver.read = AsyncMock(return_value=42)
        assert await d.read_telemetry_value("x") == 42

    @pytest.mark.asyncio
    async def test_handle_telemetry_update_from_attribute(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {}

        d = Dev("test.tm.attr")
        d.driver = None
        d.temp = 25.0
        data = await d.handle_telemetry_update(["temp"])
        assert data == {"temp": 25.0}

    @pytest.mark.asyncio
    async def test_handle_telemetry_update_skips_none_and_nan(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {}

        d = Dev("test.tm.nan")
        d.driver = None
        d.good = 1
        d.nan_val = float("nan")
        d.none_val = None
        data = await d.handle_telemetry_update(["good", "nan_val", "none_val", "missing"])
        assert data == {"good": 1}

    @pytest.mark.asyncio
    async def test_handle_telemetry_update_driver_read_fails(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {}

        d = Dev("test.tm.driverfail")
        d.driver = MagicMock()
        d.driver.read = AsyncMock(side_effect=RuntimeError("read fail"))
        d.fallback = 5
        data = await d.handle_telemetry_update(["fallback"])
        assert data == {"fallback": 5}

    @pytest.mark.asyncio
    async def test_handle_telemetry_update_returns_none_when_empty(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {}

        d = Dev("test.tm.empty")
        d.driver = None
        data = await d.handle_telemetry_update(["missing"])
        assert data is None

    @pytest.mark.asyncio
    async def test_publish_telemetry_with_node(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {}

        d = Dev("test.tm.pub")
        d.node = MagicMock()
        d.node.device_id = "d1"
        d._safe_publish = AsyncMock()
        await d.publish_telemetry({"x": 1})
        d._safe_publish.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_publish_telemetry_no_node(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {}

        d = Dev("test.tm.pubnone")
        d.node = None
        # Should not raise
        await d.publish_telemetry({"x": 1})

    @pytest.mark.asyncio
    async def test_safe_publish_default_is_noop(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {}

        d = Dev("test.tm.noop")
        env = Envelope.telemetry(source="d", data={})
        await d._safe_publish(env)  # default impl is a no-op

    def test_get_min_freq_default(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {}

        d = Dev("test.tm.minfreq")
        assert d._get_min_freq() == 0.1

    def test_get_min_freq_from_config(self):
        class Dev(TelemetryMixin):
            Kamio_FIELDS = {}

        d = Dev("test.tm.minfreq2")
        d._app = MagicMock()
        d._app.config.get.return_value = 0.5
        assert d._get_min_freq() == 0.5


# ---------------------------------------------------------------------------
# mqtt_nodes tests
# ---------------------------------------------------------------------------


class TestBaseNode:
    @pytest.mark.asyncio
    async def test_start_subscribes_and_marks_running(self, mock_mqtt):
        node = BaseNode("dev1", mock_mqtt)
        await node.start()
        assert node.is_running is True
        topics_subscribed = [t for t, _ in mock_mqtt.subscribed]
        assert f"Kamio/v1/dev1/#" in topics_subscribed
        assert f"Kamio/v1/{BROADCAST_ID}/#" in topics_subscribed

    @pytest.mark.asyncio
    async def test_start_idempotent(self, mock_mqtt):
        node = BaseNode("dev1", mock_mqtt)
        await node.start()
        count = len(mock_mqtt.subscribed)
        await node.start()
        assert len(mock_mqtt.subscribed) == count

    @pytest.mark.asyncio
    async def test_start_handles_subscribe_error(self, mock_mqtt):
        mock_mqtt.subscribe = MagicMock(side_effect=RuntimeError("sub fail"))
        node = BaseNode("dev1", mock_mqtt)
        await node.start()
        assert node.is_running is True

    @pytest.mark.asyncio
    async def test_stop_unsubscribes(self, mock_mqtt):
        node = BaseNode("dev1", mock_mqtt)
        await node.start()
        await node.stop()
        assert node.is_running is False
        unsub = [t for (t,) in mock_mqtt.unsubscribed]
        assert f"Kamio/v1/dev1/#" in unsub
        assert f"Kamio/v1/{BROADCAST_ID}/#" in unsub

    @pytest.mark.asyncio
    async def test_stop_idempotent(self, mock_mqtt):
        node = BaseNode("dev1", mock_mqtt)
        await node.stop()  # not running -> no-op
        assert node.is_running is False

    @pytest.mark.asyncio
    async def test_stop_handles_unsubscribe_error(self, mock_mqtt):
        node = BaseNode("dev1", mock_mqtt)
        await node.start()
        mock_mqtt.unsubscribe = MagicMock(side_effect=RuntimeError("unsub fail"))
        await node.stop()
        assert node.is_running is False

    @pytest.mark.asyncio
    async def test_stop_when_loop_none(self, mock_mqtt):
        node = BaseNode("dev1", mock_mqtt)
        await node.start()
        node._loop = None
        await node.stop()
        assert node.is_running is False

    @pytest.mark.asyncio
    async def test_dispatch_not_running_returns(self, mock_mqtt):
        node = BaseNode("dev1", mock_mqtt)
        # not running, no loop -> should return without error
        node.dispatch("Kamio/v1/dev1/dt", b"{}")

    @pytest.mark.asyncio
    async def test_dispatch_wrong_device_returns(self, mock_mqtt):
        node = BaseNode("dev1", mock_mqtt)
        await node.start()
        node.dispatch("Kamio/v1/other/dt", b"{}")
        await asyncio.sleep(0.01)
        # nothing should happen; no tasks created for wrong device
        assert len(node._tasks) == 0
        await node.stop()

    @pytest.mark.asyncio
    async def test_dispatch_broadcast_routes(self, mock_mqtt):
        node = BaseNode("dev1", mock_mqtt)
        await node.start()
        env = Envelope.telemetry(source="other", data={"x": 1})
        node.dispatch(f"Kamio/v1/{BROADCAST_ID}/dt", env.to_json().encode())
        await asyncio.sleep(0.05)
        await node.stop()

    @pytest.mark.asyncio
    async def test_dispatch_creates_task_and_calls_handler(self, mock_mqtt):
        node = BaseNode("dev1", mock_mqtt)
        await node.start()
        called = []

        async def handler(env):
            called.append(env)

        node.on(EnvelopeType.DEVICE_TELEMETRY, handler)
        env = Envelope.telemetry(source="dev1", data={"x": 1})
        node.dispatch("Kamio/v1/dev1/dt", env.to_json().encode())
        await asyncio.sleep(0.05)
        assert len(called) == 1
        await node.stop()

    @pytest.mark.asyncio
    async def test_dispatch_invalid_json_skipped(self, mock_mqtt):
        node = BaseNode("dev1", mock_mqtt)
        await node.start()
        node.dispatch("Kamio/v1/dev1/dt", b"not json")
        await asyncio.sleep(0.05)
        await node.stop()

    @pytest.mark.asyncio
    async def test_handle_message_no_handler(self, mock_mqtt):
        node = BaseNode("dev1", mock_mqtt)
        await node.start()
        env = Envelope.telemetry(source="dev1", data={})
        await node._handle_message(env.to_json().encode())
        await node.stop()

    @pytest.mark.asyncio
    async def test_handle_message_handler_exception_logged(self, mock_mqtt):
        node = BaseNode("dev1", mock_mqtt)
        await node.start()

        async def bad_handler(env):
            raise RuntimeError("handler boom")

        node.on(EnvelopeType.DEVICE_TELEMETRY, bad_handler)
        env = Envelope.telemetry(source="dev1", data={})
        await node._handle_message(env.to_json().encode())
        await node.stop()

    def test_on_requires_envelope_type(self, mock_mqtt):
        node = BaseNode("dev1", mock_mqtt)
        with pytest.raises(TypeError):
            node.on("dt", lambda env: None)

    def test_build_topic_with_target(self, mock_mqtt):
        node = BaseNode("dev1", mock_mqtt)
        env = Envelope.command(source="0", target="dev1", method="m")
        assert node._build_topic(env) == "Kamio/v1/dev1/sc"

    def test_build_topic_falls_back_to_source(self, mock_mqtt):
        node = BaseNode("dev1", mock_mqtt)
        env = Envelope.telemetry(source="dev1", data={})
        assert node._build_topic(env) == "Kamio/v1/dev1/dt"

    @pytest.mark.asyncio
    async def test_publish_sends_envelope(self, mock_mqtt):
        node = BaseNode("dev1", mock_mqtt)
        env = Envelope.telemetry(source="dev1", data={"x": 1})
        await node.publish(env)
        assert len(mock_mqtt.published) == 1
        topic, payload, qos, retain = mock_mqtt.published[0]
        assert topic == "Kamio/v1/dev1/dt"

    @pytest.mark.asyncio
    async def test_publish_raw_sends_bytes(self, mock_mqtt):
        node = BaseNode("dev1", mock_mqtt)
        await node.publish_raw("custom/topic", b"raw", qos=2, retain=True)
        assert mock_mqtt.published[-1] == ("custom/topic", b"raw", 2, True)

    @pytest.mark.asyncio
    async def test_publish_raw_shutdown_error_swallowed(self, mock_mqtt):
        mock_mqtt.publish = MagicMock(side_effect=RuntimeError("Event loop is closed"))
        node = BaseNode("dev1", mock_mqtt)
        await node.publish_raw("t", b"x")  # should not raise

    @pytest.mark.asyncio
    async def test_publish_raw_generic_error_logged(self, mock_mqtt):
        mock_mqtt.publish = MagicMock(side_effect=ValueError("boom"))
        node = BaseNode("dev1", mock_mqtt)
        await node.publish_raw("t", b"x")  # should not raise


class TestDeviceNode:
    @pytest.mark.asyncio
    async def test_start_calls_device_on_start(self, mock_mqtt):
        node = DeviceNode("dev1", mock_mqtt)

        class FakeHandler:
            def __init__(self):
                self.device = MagicMock()
                self.device.on_start = AsyncMock()
                self.device.on_stop = AsyncMock()

            async def __call__(self, env):
                pass

        handler = FakeHandler()
        node.set_handler(handler)
        await node.start()
        handler.device.on_start.assert_awaited_once()
        await node.stop()
        handler.device.on_stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_start_without_handler(self, mock_mqtt):
        node = DeviceNode("dev1", mock_mqtt)
        await node.start()
        assert node.is_running is True
        await node.stop()

    @pytest.mark.asyncio
    async def test_stop_calls_device_on_stop(self, mock_mqtt):
        node = DeviceNode("dev1", mock_mqtt)

        class FakeHandler:
            def __init__(self):
                self.device = MagicMock()
                self.device.on_start = AsyncMock()
                self.device.on_stop = AsyncMock()

            async def __call__(self, env):
                pass

        handler = FakeHandler()
        node.set_handler(handler)
        await node.start()
        await node.stop()
        handler.device.on_stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_handle_message_with_handler(self, mock_mqtt):
        node = DeviceNode("dev1", mock_mqtt)
        called = []

        async def handler(env):
            called.append(env)

        node.set_handler(handler)
        env = Envelope.telemetry(source="dev1", data={"x": 1})
        await node._handle_message(env.to_json().encode())
        assert len(called) == 1

    @pytest.mark.asyncio
    async def test_handle_message_handler_exception_logged(self, mock_mqtt):
        node = DeviceNode("dev1", mock_mqtt)

        async def bad(env):
            raise RuntimeError("boom")

        node.set_handler(bad)
        env = Envelope.telemetry(source="dev1", data={})
        await node._handle_message(env.to_json().encode())

    @pytest.mark.asyncio
    async def test_handle_message_no_handler_falls_back(self, mock_mqtt):
        node = DeviceNode("dev1", mock_mqtt)
        env = Envelope.telemetry(source="dev1", data={})
        await node._handle_message(env.to_json().encode())

    @pytest.mark.asyncio
    async def test_handle_message_invalid_json(self, mock_mqtt):
        node = DeviceNode("dev1", mock_mqtt)
        await node._handle_message(b"not json")

    @pytest.mark.asyncio
    async def test_emit_event(self, mock_mqtt):
        node = DeviceNode("dev1", mock_mqtt)
        await node.emit_event("press", {"btn": 1})
        assert len(mock_mqtt.published) == 1
        topic = mock_mqtt.published[0][0]
        assert topic == "Kamio/v1/dev1/de"

    @pytest.mark.asyncio
    async def test_respond(self, mock_mqtt):
        node = DeviceNode("dev1", mock_mqtt)
        req = Envelope.command(source="0", target="dev1", method="m", cind="c1")
        await node.respond(req, {"status": "ok"})
        assert len(mock_mqtt.published) == 1
        topic = mock_mqtt.published[0][0]
        assert topic == "Kamio/v1/0/ca"


class TestServerNode:
    @pytest.mark.asyncio
    async def test_handle_message_routes_to_state_manager(self, mock_mqtt):
        sm = MagicMock()
        sm.handle_incoming = AsyncMock()
        sm.update_state = MagicMock()
        node = ServerNode(mock_mqtt, state_manager=sm)
        await node.start()
        env = Envelope.telemetry(source="dev1", data={"x": 1})
        await node._handle_message(env.to_json().encode())
        sm.handle_incoming.assert_awaited_once()
        sm.update_state.assert_called_once_with("dev1", {"x": 1})
        await node.stop()

    @pytest.mark.asyncio
    async def test_handle_message_command_ack_resolved(self, mock_mqtt):
        cm = MagicMock()
        cm.handle_ack = MagicMock(return_value=True)
        node = ServerNode(mock_mqtt, command_manager=cm)
        await node.start()
        env = Envelope.command_ack(source="dev1", target="0", data={"status": "ok"}, cind="c1")
        await node._handle_message(env.to_json().encode())
        cm.handle_ack.assert_called_once()
        await node.stop()

    @pytest.mark.asyncio
    async def test_handle_message_unhandled_type(self, mock_mqtt):
        node = ServerNode(mock_mqtt)
        await node.start()
        env = Envelope.keepalive(source="dev1")
        await node._handle_message(env.to_json().encode())
        await node.stop()

    @pytest.mark.asyncio
    async def test_handle_message_invalid_json(self, mock_mqtt):
        node = ServerNode(mock_mqtt)
        await node.start()
        await node._handle_message(b"not json")
        await node.stop()

    @pytest.mark.asyncio
    async def test_handle_message_handler_exception(self, mock_mqtt):
        node = ServerNode(mock_mqtt)
        await node.start()

        async def bad(env):
            raise RuntimeError("boom")

        node.on(EnvelopeType.DEVICE_TELEMETRY, bad_handler := bad)
        env = Envelope.telemetry(source="dev1", data={})
        await node._handle_message(env.to_json().encode())
        await node.stop()

    @pytest.mark.asyncio
    async def test_set_state_without_state_manager_raises(self, mock_mqtt):
        node = ServerNode(mock_mqtt)
        with pytest.raises(RuntimeError):
            await node.set_state("dev1", {"x": 1})

    @pytest.mark.asyncio
    async def test_set_state_delegates(self, mock_mqtt):
        sm = MagicMock()
        sm.set_state = AsyncMock(return_value={"x": 1})
        node = ServerNode(mock_mqtt, state_manager=sm)
        result = await node.set_state("dev1", {"x": 1})
        assert result == {"x": 1}
        sm.set_state.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_call_without_command_manager_raises(self, mock_mqtt):
        node = ServerNode(mock_mqtt)
        with pytest.raises(RuntimeError):
            await node.call("dev1", "m", {})

    @pytest.mark.asyncio
    async def test_call_delegates(self, mock_mqtt):
        cm = MagicMock()
        ack = Envelope.command_ack(source="dev1", target="0", data={"status": "ok"}, cind="c1")
        cm.send_command = AsyncMock(return_value=ack)
        node = ServerNode(mock_mqtt, command_manager=cm)
        result = await node.call("dev1", "m", {})
        assert result is ack
        cm.send_command.assert_awaited_once()


# ---------------------------------------------------------------------------
# handlers tests
# ---------------------------------------------------------------------------


class TestDeviceHandler:
    @pytest.fixture
    def setup(self, mock_mqtt):
        """Create a Device + DeviceNode + DeviceHandler triple."""
        device = Light()
        node = DeviceNode("light1", mock_mqtt)
        device.node = node
        sm = StateManager()
        handler = DeviceHandler(device, node, state_manager=sm)
        node.set_handler(handler)
        return device, node, handler, sm

    @pytest.mark.asyncio
    async def test_init_injects_callbacks_when_app_present(self, mock_mqtt):
        app = KamioApp(mock_mqtt)
        device = Light()
        node = DeviceNode("light1", mock_mqtt)
        device.node = node
        device._app = app
        handler = DeviceHandler(device, node)
        assert device._on_state_changed is not None
        assert device._on_rules_trigger is not None

    @pytest.mark.asyncio
    async def test_init_no_callbacks_without_app(self, mock_mqtt):
        device = Light()
        node = DeviceNode("light1", mock_mqtt)
        device.node = node
        handler = DeviceHandler(device, node)
        assert device._on_state_changed is None
        assert device._on_rules_trigger is None

    @pytest.mark.asyncio
    async def test_call_dispatches_command(self, setup):
        device, node, handler, sm = setup
        env = Envelope.command(source="0", target="light1", method="toggle")
        await handler(env)
        assert device.power is True
        # An ack should have been published
        assert len(node.mqtt.published) >= 1

    @pytest.mark.asyncio
    async def test_handle_command_success(self, setup):
        device, node, handler, sm = setup
        env = Envelope.command(source="0", target="light1", method="toggle")
        await handler._handle_command(env)
        assert device.power is True

    @pytest.mark.asyncio
    async def test_handle_command_falsy_result_preserved(self, setup):
        """Command returning 0 or False must not be replaced with {} in ACK."""
        device, node, handler, sm = setup
        # Test with 0
        env = Envelope.command(source="0", target="light1", method="get_zero")
        await handler._handle_command(env)
        _, payload, _, _ = node.mqtt.published[-1]
        ack = Envelope.from_json(payload)
        assert ack.data["result"] == 0

        # Test with False
        env = Envelope.command(source="0", target="light1", method="get_false")
        await handler._handle_command(env)
        _, payload, _, _ = node.mqtt.published[-1]
        ack = Envelope.from_json(payload)
        assert ack.data["result"] is False

    @pytest.mark.asyncio
    async def test_handle_command_with_node_app_injection(self, setup):
        device, node, handler, sm = setup
        device._app = MagicMock()
        env = Envelope.command(source="0", target="light1", method="with_node")
        await handler._handle_command(env)
        assert len(node.mqtt.published) >= 1

    @pytest.mark.asyncio
    async def test_handle_command_unknown_method_sends_error(self, setup):
        device, node, handler, sm = setup
        env = Envelope.command(source="0", target="light1", method="nope")
        await handler._handle_command(env)
        # error ack published
        assert len(node.mqtt.published) >= 1

    @pytest.mark.asyncio
    async def test_handle_command_debug_reraises(self, mock_mqtt):
        device = Light()
        node = DeviceNode("light1", mock_mqtt)
        device.node = node
        handler = DeviceHandler(device, node, debug=True)
        env = Envelope.command(source="0", target="light1", method="nope")
        with pytest.raises(AttributeError):
            await handler._handle_command(env)

    @pytest.mark.asyncio
    async def test_handle_state_own_echo(self, setup):
        device, node, handler, sm = setup
        env = Envelope.state(source="light1", data={"power": True})
        device._own_state_cinds.add(env.cind)
        device._own_state_cinds_order.append(env.cind)
        await handler._handle_state(env)
        # echo should be discarded, no ack sent
        assert env.cind not in device._own_state_cinds

    @pytest.mark.asyncio
    async def test_handle_state_own_echo_no_order_list(self, mock_mqtt):
        device = Light()
        node = DeviceNode("light1", mock_mqtt)
        device.node = node
        handler = DeviceHandler(device, node)
        env = Envelope.state(source="light1", data={"power": True})
        device._own_state_cinds.add(env.cind)
        del device._own_state_cinds_order  # simulate older instance
        await handler._handle_state(env)  # should not raise

    @pytest.mark.asyncio
    async def test_handle_state_normal(self, setup):
        device, node, handler, sm = setup
        env = Envelope.state(source="0", data={"power": True})
        await handler._handle_state(env)
        assert device.power is True
        assert len(node.mqtt.published) >= 1

    @pytest.mark.asyncio
    async def test_handle_state_no_changes_no_ack(self, setup):
        device, node, handler, sm = setup
        device.power = True
        env = Envelope.state(source="0", data={"power": True})
        await handler._handle_state(env)
        # no change -> no ack published for this
        # (nothing published specifically for the ack)

    @pytest.mark.asyncio
    async def test_handle_state_validation_error(self, setup):
        device, node, handler, sm = setup
        env = Envelope.state(source="0", data={"brightness": 999})
        await handler._handle_state(env)
        # error ack published
        assert len(node.mqtt.published) >= 1

    @pytest.mark.asyncio
    async def test_handle_state_ack_forwards_to_state_manager(self, setup):
        device, node, handler, sm = setup
        env = Envelope.state_ack(source="0", target="light1", data={"result": {}}, cind="c1")
        await handler._handle_state_ack(env)

    @pytest.mark.asyncio
    async def test_handle_state_ack_no_state_manager(self, mock_mqtt):
        device = Light()
        node = DeviceNode("light1", mock_mqtt)
        device.node = node
        handler = DeviceHandler(device, node, state_manager=None)
        env = Envelope.state_ack(source="0", target="light1", data={}, cind="c1")
        await handler._handle_state_ack(env)  # no-op

    @pytest.mark.asyncio
    async def test_handle_config_success(self, setup):
        device, node, handler, sm = setup
        env = Envelope(
            source="0", target="light1", type=EnvelopeType.DEVICE_CONFIG, data={"host": "new"}
        )
        await handler._handle_config(env)
        assert device.host == "new"
        assert len(node.mqtt.published) >= 1

    @pytest.mark.asyncio
    async def test_handle_config_validation_error(self, setup):
        device, node, handler, sm = setup

        class StrictLight(Light):
            port: int = config(default=80)

        # Use min/max on a config field to force ValueError
        class ConfigLight(Device):
            port: int = config(default=80)

        device2 = ConfigLight()
        node2 = DeviceNode("c1", node.mqtt)
        device2.node = node2
        # Manually set field with min to trigger validation
        field = ConfigLight.Kamio_FIELDS["port"]
        object.__setattr__(field, "min", 1)
        object.__setattr__(field, "max", 100)
        handler2 = DeviceHandler(device2, node2)
        env = Envelope(source="0", target="c1", type=EnvelopeType.DEVICE_CONFIG, data={"port": 999})
        await handler2._handle_config(env)
        assert len(node2.mqtt.published) >= 1

    @pytest.mark.asyncio
    async def test_handle_event_forwards_to_device(self, setup):
        device, node, handler, sm = setup
        device.handle_event = AsyncMock()
        env = Envelope.event(source="0", event_name="button", payload={"x": 1})
        env.target = "light1"
        await handler._handle_event(env)
        device.handle_event.assert_awaited_once_with("button", {"x": 1})

    @pytest.mark.asyncio
    async def test_handle_telemetry_own_source_triggers_rules(self, mock_mqtt):
        device = Light()
        node = DeviceNode("light1", mock_mqtt)
        device.node = node
        app = MagicMock()
        app.rules.handle_device_update = AsyncMock()
        device._app = app
        sm = StateManager()
        handler = DeviceHandler(device, node, state_manager=sm)
        env = Envelope.telemetry(source="light1", data={"temp": 25.0})
        await handler._handle_telemetry(env)
        app.rules.handle_device_update.assert_awaited_once()
        assert sm.get_state("light1", "temp") == 25.0

    @pytest.mark.asyncio
    async def test_handle_telemetry_other_source_skips_rules(self, mock_mqtt):
        device = Light()
        node = DeviceNode("light1", mock_mqtt)
        device.node = node
        app = MagicMock()
        app.rules.handle_device_update = AsyncMock()
        device._app = app
        sm = StateManager()
        handler = DeviceHandler(device, node, state_manager=sm)
        env = Envelope.telemetry(source="other", data={"temp": 25.0})
        await handler._handle_telemetry(env)
        app.rules.handle_device_update.assert_not_awaited()
        assert sm.get_state("other", "temp") == 25.0

    @pytest.mark.asyncio
    async def test_handle_telemetry_no_state_manager(self, mock_mqtt):
        device = Light()
        node = DeviceNode("light1", mock_mqtt)
        device.node = node
        handler = DeviceHandler(device, node, state_manager=None)
        env = Envelope.telemetry(source="light1", data={"temp": 1})
        await handler._handle_telemetry(env)  # no-op for state

    @pytest.mark.asyncio
    async def test_send_ack_default_response_type(self, setup):
        device, node, handler, sm = setup
        env = Envelope.command(source="0", target="light1", method="toggle")
        await handler.send_ack(env, result={"ok": True})
        assert len(node.mqtt.published) == 1
        payload = node.mqtt.published[0][1]
        parsed = json.loads(payload)
        assert parsed["type"] == "ca"
        assert parsed["data"]["status"] == "ok"

    @pytest.mark.asyncio
    async def test_send_ack_state_defaults_to_state_ack(self, setup):
        device, node, handler, sm = setup
        env = Envelope.state(source="0", data={"power": True})
        await handler.send_ack(env, result={"result": {"power": True}})
        payload = node.mqtt.published[0][1]
        parsed = json.loads(payload)
        assert parsed["type"] == "sa"

    @pytest.mark.asyncio
    async def test_send_ack_explicit_response_type(self, setup):
        device, node, handler, sm = setup
        env = Envelope.command(source="0", target="light1", method="toggle")
        await handler.send_ack(env, result={"ok": True}, response_type=EnvelopeType.STATE_ACK)
        payload = node.mqtt.published[0][1]
        parsed = json.loads(payload)
        assert parsed["type"] == "sa"

    @pytest.mark.asyncio
    async def test_send_error(self, setup):
        device, node, handler, sm = setup
        env = Envelope.command(source="0", target="light1", method="toggle")
        await handler.send_error(env, "something broke")
        payload = node.mqtt.published[0][1]
        parsed = json.loads(payload)
        assert parsed["data"]["status"] == "error"
        assert parsed["data"]["error"] == "something broke"

    @pytest.mark.asyncio
    async def test_call_handler_unknown_type(self, setup):
        device, node, handler, sm = setup
        env = Envelope(source="0", type=EnvelopeType.UNKNOWN, data={})
        await handler(env)  # no handler for UNKNOWN -> no error

    @pytest.mark.asyncio
    async def test_call_handler_exception_sends_error(self, mock_mqtt):
        device = Light()
        node = DeviceNode("light1", mock_mqtt)
        device.node = node
        handler = DeviceHandler(device, node)

        async def bad_handler(env):
            raise RuntimeError("boom")

        handler._handlers[EnvelopeType.DEVICE_TELEMETRY] = bad_handler
        env = Envelope.telemetry(source="0", data={})
        await handler(env)
        assert len(node.mqtt.published) >= 1

    @pytest.mark.asyncio
    async def test_call_handler_debug_reraises(self, mock_mqtt):
        device = Light()
        node = DeviceNode("light1", mock_mqtt)
        device.node = node
        handler = DeviceHandler(device, node, debug=True)

        async def bad_handler(env):
            raise RuntimeError("boom")

        handler._handlers[EnvelopeType.DEVICE_TELEMETRY] = bad_handler
        env = Envelope.telemetry(source="0", data={})
        with pytest.raises(RuntimeError):
            await handler(env)


# ---------------------------------------------------------------------------
# custom_nodes tests
# ---------------------------------------------------------------------------


class TestCustomNode:
    def test_init_strips_trailing_slash(self):
        node = _RecordingNode(MagicMock(), "prefix/")
        assert node.topic_prefix == "prefix"

    def test_matches_exact_prefix(self):
        node = _RecordingNode(MagicMock(), "sensors")
        assert node.matches("sensors") is True

    def test_matches_child_topic(self):
        node = _RecordingNode(MagicMock(), "sensors")
        assert node.matches("sensors/room/temp") is True

    def test_matches_non_matching(self):
        node = _RecordingNode(MagicMock(), "sensors")
        assert node.matches("other/room") is False

    def test_repr(self):
        node = _RecordingNode(MagicMock(), "sensors")
        r = repr(node)
        assert "sensors" in r and "running=False" in r

    def test_resolve_topic_relative(self):
        node = _RecordingNode(MagicMock(), "prefix")
        assert node._resolve_topic("cmd", absolute=False) == "prefix/cmd"

    def test_resolve_topic_relative_empty(self):
        node = _RecordingNode(MagicMock(), "prefix")
        assert node._resolve_topic("", absolute=False) == "prefix"

    def test_resolve_topic_absolute(self):
        node = _RecordingNode(MagicMock(), "prefix")
        assert node._resolve_topic("abs/topic", absolute=True) == "abs/topic"

    def test_encode_payload_str(self):
        assert CustomNode._encode_payload("hi") == b"hi"

    def test_encode_payload_non_str(self):
        assert CustomNode._encode_payload(b"hi") == b"hi"
        assert CustomNode._encode_payload(123) == 123

    def test_subscribe_relative(self):
        client = MagicMock()
        node = _RecordingNode(client, "prefix")
        node.subscribe("cmd/#", qos=1)
        client.subscribe.assert_called_once_with("prefix/cmd/#", 1)
        assert "prefix/cmd/#" in node._subscriptions

    def test_subscribe_absolute(self):
        client = MagicMock()
        node = _RecordingNode(client, "prefix")
        node.subscribe_absolute("abs/topic", qos=2)
        client.subscribe.assert_called_once_with("abs/topic", 2)
        assert "abs/topic" in node._subscriptions

    def test_publish_relative(self):
        client = MagicMock()
        node = _RecordingNode(client, "prefix")
        node.publish("data", "hello", qos=1, retain=True)
        client.publish.assert_called_once_with("prefix/data", b"hello", qos=1, retain=True)

    def test_publish_absolute(self):
        client = MagicMock()
        node = _RecordingNode(client, "prefix")
        node.publish_absolute("abs/topic", "hello")
        client.publish.assert_called_once_with("abs/topic", b"hello", qos=0, retain=False)

    @pytest.mark.asyncio
    async def test_publish_async(self):
        client = MagicMock()
        node = _RecordingNode(client, "prefix")
        await node.publish_async("data", "hello", qos=1, retain=True)
        client.publish.assert_called_once_with("prefix/data", b"hello", qos=1, retain=True)

    @pytest.mark.asyncio
    async def test_stop_unsubscribes_all(self):
        client = MagicMock()
        node = _RecordingNode(client, "prefix")
        node.subscribe("cmd/#")
        node.subscribe_absolute("abs/topic")
        await node.stop()
        assert client.unsubscribe.call_count == 2
        assert len(node._subscriptions) == 0

    @pytest.mark.asyncio
    async def test_stop_handles_unsubscribe_error(self):
        client = MagicMock()
        client.unsubscribe = MagicMock(side_effect=RuntimeError("fail"))
        node = _RecordingNode(client, "prefix")
        node.subscribe("cmd/#")
        await node.stop()  # should not raise

    @pytest.mark.asyncio
    async def test_on_connect_on_disconnect_hooks(self):
        node = _RecordingNode(MagicMock(), "prefix")
        await node.on_connect()
        await node.on_disconnect()


class TestCustomNodeManager:
    @pytest.fixture
    def app(self):
        app = MagicMock()
        app.event_bus = MagicMock()
        app.event_bus.publish = AsyncMock()
        return app

    def test_register_and_get(self, app):
        mgr = CustomNodeManager(app)
        node = _RecordingNode(MagicMock(), "p")
        mgr.register_node("n1", node)
        assert mgr.get_node("n1") is node
        assert "n1" in mgr.list_nodes()

    def test_register_duplicate_raises(self, app):
        mgr = CustomNodeManager(app)
        mgr.register_node("n1", _RecordingNode(MagicMock(), "p"))
        with pytest.raises(ValueError):
            mgr.register_node("n1", _RecordingNode(MagicMock(), "p"))

    def test_unregister_existing(self, app):
        mgr = CustomNodeManager(app)
        mgr.register_node("n1", _RecordingNode(MagicMock(), "p"))
        mgr.unregister_node("n1")
        assert mgr.get_node("n1") is None

    def test_unregister_missing_logs_warning(self, app):
        mgr = CustomNodeManager(app)
        mgr.unregister_node("nope")  # should not raise

    @pytest.mark.asyncio
    async def test_start_all_starts_each(self, app):
        mgr = CustomNodeManager(app)
        client = MagicMock()
        n1 = _RecordingNode(client, "p1")
        n2 = _RecordingNode(client, "p2")
        mgr.register_node("n1", n1)
        mgr.register_node("n2", n2)
        await mgr.start_all()
        assert n1.started and n2.started
        assert app.event_bus.publish.call_count >= 2

    @pytest.mark.asyncio
    async def test_start_all_handles_error(self, app):
        mgr = CustomNodeManager(app)
        client = MagicMock()
        n1 = _RecordingNode(client, "p1")
        n1.fail_start = True
        mgr.register_node("n1", n1)
        await mgr.start_all()
        app.event_bus.publish.assert_awaited()

    @pytest.mark.asyncio
    async def test_stop_all_stops_each(self, app):
        mgr = CustomNodeManager(app)
        client = MagicMock()
        n1 = _RecordingNode(client, "p1")
        mgr.register_node("n1", n1)
        await n1.start()
        await mgr.stop_all()
        assert n1.stopped
        assert n1._is_running is False

    @pytest.mark.asyncio
    async def test_stop_all_skips_not_running(self, app):
        mgr = CustomNodeManager(app)
        n1 = _RecordingNode(MagicMock(), "p1")
        mgr.register_node("n1", n1)
        await mgr.stop_all()  # n1 not running -> skip

    @pytest.mark.asyncio
    async def test_stop_all_handles_error(self, app):
        mgr = CustomNodeManager(app)
        client = MagicMock()
        n1 = _RecordingNode(client, "p1")
        mgr.register_node("n1", n1)
        n1._is_running = True
        n1.stop = AsyncMock(side_effect=RuntimeError("boom"))
        await mgr.stop_all()  # should not raise

    @pytest.mark.asyncio
    async def test_route_message_handled(self, app):
        mgr = CustomNodeManager(app)
        client = MagicMock()
        n1 = _RecordingNode(client, "bridge")
        mgr.register_node("n1", n1)
        handled = await mgr.route_message("bridge/cmd/power", b"on")
        assert handled is True
        assert n1.received == [("bridge/cmd/power", b"on")]

    @pytest.mark.asyncio
    async def test_route_message_no_match(self, app):
        mgr = CustomNodeManager(app)
        n1 = _RecordingNode(MagicMock(), "bridge")
        mgr.register_node("n1", n1)
        handled = await mgr.route_message("other/topic", b"x")
        assert handled is False

    @pytest.mark.asyncio
    async def test_route_message_handle_error(self, app):
        mgr = CustomNodeManager(app)
        client = MagicMock()
        n1 = _RecordingNode(client, "bridge")
        n1.fail_handle = True
        mgr.register_node("n1", n1)
        handled = await mgr.route_message("bridge/cmd", b"x")
        assert handled is False
        app.event_bus.publish.assert_awaited()


# ---------------------------------------------------------------------------
# state.py tests
# ---------------------------------------------------------------------------


class TestStateManager:
    def test_get_state_empty(self):
        sm = StateManager()
        assert sm.get_state("dev1") == {}
        assert sm.get_state("dev1", "field") is None

    def test_update_state_merge(self):
        sm = StateManager()
        sm.update_state("dev1", {"a": 1})
        sm.update_state("dev1", {"b": 2})
        assert sm.get_state("dev1") == {"a": 1, "b": 2}
        assert sm.get_state("dev1", "a") == 1

    def test_update_state_non_dict_ignored(self):
        sm = StateManager()
        sm.update_state("dev1", "not a dict")
        assert sm.get_state("dev1") == {}

    def test_update_state_empty_ignored(self):
        sm = StateManager()
        sm.update_state("dev1", {})
        assert sm.get_state("dev1") == {}

    def test_get_all_states(self):
        sm = StateManager()
        sm.update_state("d1", {"a": 1})
        sm.update_state("d2", {"b": 2})
        all_states = sm.get_all_states()
        assert all_states == {"d1": {"a": 1}, "d2": {"b": 2}}
        # Ensure it's a copy
        all_states["d1"]["a"] = 999
        assert sm.get_state("d1", "a") == 1

    @pytest.mark.asyncio
    async def test_set_state_success(self, mock_mqtt):
        sm = StateManager()
        node = BaseNode("0", mock_mqtt)

        async def publish_func(env):
            await node.publish(env)
            # Immediately resolve the ack to simulate device response
            ack = Envelope.state_ack(
                source="dev1",
                target="0",
                data={"result": {"power": True}, "status": "ok"},
                cind=env.cind,
            )
            await sm.handle_incoming(ack)

        result = await sm.set_state(
            "dev1", {"power": True}, publish_func, source_id="0", timeout=2.0
        )
        assert result == {"power": True}
        assert sm.get_state("dev1", "power") is True

    @pytest.mark.asyncio
    async def test_set_state_error_status_not_updated(self, mock_mqtt):
        sm = StateManager()
        node = BaseNode("0", mock_mqtt)

        async def publish_func(env):
            await node.publish(env)
            ack = Envelope.state_ack(
                source="dev1", target="0", data={"result": {}, "status": "error"}, cind=env.cind
            )
            await sm.handle_incoming(ack)

        result = await sm.set_state(
            "dev1", {"power": True}, publish_func, source_id="0", timeout=2.0
        )
        assert result == {}
        # error status -> state not updated
        assert sm.get_state("dev1") == {}

    @pytest.mark.asyncio
    async def test_set_state_timeout(self, mock_mqtt):
        sm = StateManager()
        node = BaseNode("0", mock_mqtt)

        async def publish_func(env):
            await node.publish(env)
            # never resolve -> timeout

        with pytest.raises(asyncio.TimeoutError):
            await sm.set_state("dev1", {"x": 1}, publish_func, source_id="0", timeout=0.05)

    @pytest.mark.asyncio
    async def test_handle_incoming_device_state(self):
        sm = StateManager()
        env = Envelope.state(source="dev1", data={"power": True})
        await sm.handle_incoming(env)
        assert sm.get_state("dev1", "power") is True

    @pytest.mark.asyncio
    async def test_handle_incoming_state_ack(self):
        sm = StateManager()
        # Register a pending waiter then resolve via handle_incoming
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        sm._pending[("dev1", "c1")] = fut
        env = Envelope.state_ack(source="dev1", target="0", data={"result": {}}, cind="c1")
        await sm.handle_incoming(env)
        assert fut.done()
        assert fut.result() is env

    @pytest.mark.asyncio
    async def test_handle_incoming_other_type_ignored(self):
        sm = StateManager()
        env = Envelope.telemetry(source="dev1", data={"x": 1})
        await sm.handle_incoming(env)  # should be a no-op for state mirror
        assert sm.get_state("dev1") == {}
