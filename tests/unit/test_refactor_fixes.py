"""Unit tests for the critical fixes added in this refactor pass.

Covers:
- ``parse_freq`` raising on bad input instead of silently returning 0.0
- ``Device.__setattr__`` validating state values via min/max/choices
- bounded ``_own_state_cinds`` echo-suppression cache
- ``DeviceRegistry`` thread-safety and ``unregister_instance``
- ``StateManager`` thread-safety
- ``HADiscovery.clear`` publishes empty retained payloads
- ``PriorityRegistry`` thread-safety under concurrent mutation
- ``KamioApp`` kwargs validation
"""

from __future__ import annotations

import asyncio
import threading

import pytest

from kamio import Device, KamioApp, state
from kamio.core.envelope import Envelope, EnvelopeType
from kamio.core.registry import DeviceRegistry
from kamio.core.state import StateManager
from kamio.core.subscription import PriorityRegistry
from kamio.data_fields import parse_freq
from kamio.discovery import HADiscovery


# ---------------------------------------------------------------------------
# parse_freq
# ---------------------------------------------------------------------------
def test_parse_freq_valid_units():
    assert parse_freq("500ms") == 0.5
    assert parse_freq("5s") == 5.0
    assert parse_freq("2m") == 120.0
    assert parse_freq("1h") == 3600.0
    assert parse_freq(10) == 10.0
    assert parse_freq(None) == 0.0
    assert parse_freq("") == 0.0


def test_parse_freq_raises_on_garbage():
    with pytest.raises(ValueError):
        parse_freq("abc")
    with pytest.raises(ValueError):
        parse_freq("5kg")
    with pytest.raises(ValueError):
        parse_freq("not-a-number")


def test_parse_freq_raises_on_negative():
    with pytest.raises(ValueError):
        parse_freq(-1)
    with pytest.raises(ValueError):
        parse_freq("-5s")


def test_parse_freq_raises_on_unsupported_type():
    with pytest.raises(ValueError):
        parse_freq(["5s"])


# ---------------------------------------------------------------------------
# Device.__setattr__ validation
# ---------------------------------------------------------------------------
class BoundedDevice(Device):
    level: int = state(default=0, min=0, max=100)
    color: str = state(default="red", choices=["red", "green", "blue"])


def test_device_setattr_validates_min_max():
    d = BoundedDevice()
    d.level = 50
    assert d.level == 50
    with pytest.raises(ValueError):
        d.level = -1
    with pytest.raises(ValueError):
        d.level = 101


def test_device_setattr_validates_choices():
    d = BoundedDevice()
    d.color = "green"
    assert d.color == "green"
    with pytest.raises(ValueError):
        d.color = "purple"


# ---------------------------------------------------------------------------
# Bounded _own_state_cinds
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_own_state_cinds_is_bounded():
    """Echo cinds recorded by real state assignment stay bounded (SCAN-29)."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock

    from kamio.device import EchoCache

    limit = 3
    d = BoundedDevice()
    d.node = MagicMock()
    d.node.publish = AsyncMock()
    d.node.is_running = True
    d.node.device_id = "dev1"
    d._echo_cache = EchoCache(limit=limit)

    # Drive limit+N real state changes through __setattr__.
    extra = 3
    for i in range(limit + extra):
        d.level = i + 1  # distinct values -> a publish each time
        for _ in range(10):
            await asyncio.sleep(0)

    cinds = [call.args[0].cind for call in d.node.publish.await_args_list]
    assert len(cinds) == limit + extra
    assert len(d._echo_cache) == limit
    # The `extra` oldest cinds were evicted; the newest `limit` remain.
    for evicted in cinds[:extra]:
        assert d._echo_cache.consume(evicted) is False
    for kept in cinds[extra:]:
        assert d._echo_cache.consume(kept) is True


def test_echo_cache_consume_and_evict():
    """EchoCache consumes a cind exactly once and evicts oldest first (SCAN-02)."""
    from kamio.device import EchoCache

    cache = EchoCache(limit=3)
    for cind in ("a", "b", "c"):
        cache.remember(cind)

    assert cache.consume("a") is True
    assert cache.consume("a") is False  # consumed exactly once

    cache.remember("d")
    cache.remember("e")  # over limit -> evicts "b"
    assert "b" not in cache
    assert "e" in cache
    assert len(cache) == 3


# ---------------------------------------------------------------------------
# DeviceRegistry thread-safety
# ---------------------------------------------------------------------------
class _DummyDevice(Device):
    flag: bool = state(default=False)


def test_registry_unregister_instance():
    reg = DeviceRegistry()
    dev = _DummyDevice()
    reg.register_instance("d1", dev)
    assert reg.get_instance("d1") is dev
    assert reg.unregister_instance("d1") is dev
    assert reg.get_instance("d1") is None
    # second unregister is safe
    assert reg.unregister_instance("d1") is None


def test_registry_concurrent_mutation():
    reg = DeviceRegistry()
    reg.register_class(_DummyDevice)

    def worker(idx: int) -> None:
        for i in range(200):
            did = f"d-{idx}-{i}"
            reg.register_instance(did, _DummyDevice())
            reg.unregister_instance(did)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # All workers added then removed their devices; registry should be empty.
    assert reg.instances == {}


def test_registry_instances_returns_snapshot():
    reg = DeviceRegistry()
    reg.register_instance("d1", _DummyDevice())
    snap = reg.instances
    snap["leak"] = _DummyDevice()
    # Mutating the snapshot must not affect the registry.
    assert "leak" not in reg.instances


# ---------------------------------------------------------------------------
# StateManager thread-safety
# ---------------------------------------------------------------------------
def test_state_manager_concurrent_update():
    sm = StateManager()

    def worker(idx: int) -> None:
        for i in range(200):
            sm.update_state(f"dev-{idx}", {"counter": i})

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # Each worker's last write should be visible.
    for idx in range(8):
        assert sm.get_state(f"dev-{idx}", "counter") == 199


# ---------------------------------------------------------------------------
# PriorityRegistry thread-safety
# ---------------------------------------------------------------------------
def test_priority_registry_concurrent_add_remove():
    reg = PriorityRegistry()

    def adder():
        for i in range(300):
            reg.add("evt", lambda: i, priority=i)

    def remover():
        for _ in range(300):
            reg.remove("evt", None, predicate=lambda stored, ref: False)

    threads = [threading.Thread(target=adder), threading.Thread(target=remover)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # No exception means the lock prevented corruption; just sanity-check list.
    reg.list("evt")


# ---------------------------------------------------------------------------
# HADiscovery.clear
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_ha_discovery_clear_publishes_empty_retained(mock_mqtt):
    client = mock_mqtt
    app = KamioApp(mqtt_broker=client, client_id="ha-app")
    app.register(BoundedDevice)
    dev = await app.add_device("bounded-1", BoundedDevice)
    discovery = HADiscovery()

    await discovery.announce(dev)
    announced = [t for t in client.published if t[0].endswith("/config")]
    assert announced, "announce should publish config topics"

    await discovery.clear(dev)
    cleared = [
        t for t in client.published if t[0].endswith("/config") and t[1] == b"" and t[3] is True
    ]
    assert len(cleared) == len(announced), "clear should empty each config topic"


@pytest.mark.asyncio
async def test_ha_discovery_announce_uses_retain(mock_mqtt):
    client = mock_mqtt
    app = KamioApp(mqtt_broker=client, client_id="ha-app2")
    app.register(BoundedDevice)
    dev = await app.add_device("bounded-2", BoundedDevice)
    discovery = HADiscovery()

    await discovery.announce(dev)
    retained = [t for t in client.published if t[3] is True]
    assert retained, "announce should publish retained config topics"


@pytest.mark.asyncio
async def test_ha_discovery_announce_continues_after_publish_error(mock_mqtt):
    """announce() should not abort if one publish_raw fails."""
    client = mock_mqtt
    app = KamioApp(mqtt_broker=client, client_id="ha-app-err")
    app.register(BoundedDevice)
    dev = await app.add_device("bounded-err", BoundedDevice)
    discovery = HADiscovery()

    # Make publish_raw fail on the first call, then succeed.
    original = dev.node.publish_raw
    call_count = [0]

    async def _flaky_publish_raw(topic, payload, qos=1, retain=False):
        call_count[0] += 1
        if call_count[0] == 1:
            raise RuntimeError("publish failed")
        await original(topic, payload, qos, retain)

    dev.node.publish_raw = _flaky_publish_raw
    # Should not raise despite the first publish failing.
    await discovery.announce(dev)
    # Subsequent publishes should have succeeded.
    assert call_count[0] > 1


# ---------------------------------------------------------------------------
# KamioApp kwargs validation
# ---------------------------------------------------------------------------
def test_app_rejects_unknown_kwargs_for_external_client(mock_mqtt):
    with pytest.raises(TypeError):
        KamioApp(mqtt_broker=mock_mqtt, client_id="x", bogus=True)


def test_app_rejects_unknown_kwargs_for_uri_broker():
    with pytest.raises(TypeError):
        KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="x", bogus=True)


# ---------------------------------------------------------------------------
# DeviceMeta override warnings
# ---------------------------------------------------------------------------
def test_device_meta_warns_on_field_override(caplog):
    class Parent(Device):
        level: int = state(default=0, min=0, max=100)

    with caplog.at_level("WARNING", logger="Kamio.device_meta"):

        class Child(Parent):
            level: str = state(default="x")  # type change

    assert any("overridden" in r.message for r in caplog.records)
