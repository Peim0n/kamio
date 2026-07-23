from __future__ import annotations

import asyncio
import gc
import tracemalloc

import pytest
from kamio import Device, KamioApp, command, state, rule
from kamio.core.rules import RuleEvent


class LoadDevice(Device):
    value: int = state(default=0, writable=True)


class LoadDeviceWithRule(Device):
    value: int = state(default=0, writable=True)
    rule_hits: int = 0

    @rule(fields=["value"])
    async def on_value_change(self, event: RuleEvent, app: KamioApp):
        """Device-level rule that tracks value changes."""
        self.rule_hits += 1


@pytest.mark.stress
@pytest.mark.asyncio
async def test_load_many_devices_and_rules():
    app = KamioApp()
    device_count = 30
    rule_count = 30
    updates = 200

    rule_hits = []

    for i in range(rule_count):

        @app.rule(device=LoadDevice)
        async def on_change(event, app, _i=i):
            rule_hits.append(_i)

    devices = []
    for i in range(device_count):
        d = await app.add_device(f"d{i}", LoadDevice)
        devices.append(d)

    await app.start()

    async def spam(dev):
        for v in range(updates):
            await dev.handle_state({"value": v})

    await asyncio.gather(*(spam(d) for d in devices))
    await asyncio.sleep(0.05)

    await app.stop()
    gc.collect()

    assert not app.is_running
    # Every update should trigger at least one rule execution.
    assert len(rule_hits) >= device_count * updates


@pytest.mark.stress
@pytest.mark.asyncio
async def test_memory_leak_after_cycles():
    tracemalloc.start()
    start = tracemalloc.take_snapshot()

    for _ in range(10):
        app = KamioApp()
        d = await app.add_device("d0", LoadDevice)
        await app.start()
        for _ in range(50):
            await d.handle_state({"value": 1})
        await app.stop()
        del app, d
        gc.collect()

    end = tracemalloc.take_snapshot()
    diff = end.compare_to(start, "lineno")
    total_increase = sum(s.size_diff for s in diff if s.size_diff > 0)
    # Allow small growth from interpreter caches; fail on large leak.
    assert total_increase < 5 * 1024 * 1024, f"Memory grew by {total_increase} bytes"


class CommandDevice(Device):
    counter: int = state(default=0, writable=True)

    @command
    async def inc(self):
        self.counter += 1
        return {"counter": self.counter}


@pytest.mark.stress
@pytest.mark.asyncio
async def test_parallel_command_execution():
    app = KamioApp()
    await app.add_device("cmd", CommandDevice)
    device = app.devices["cmd"]

    async def call():
        for _ in range(200):
            await device.handle_command("inc", {})

    await asyncio.gather(*(call() for _ in range(10)))
    assert device.counter == 2000


@pytest.mark.stress
@pytest.mark.asyncio
async def test_high_frequency_state_updates():
    app = KamioApp()
    rule_hits = 0

    @app.rule(device=LoadDevice)
    async def on_any(event, app):
        nonlocal rule_hits
        rule_hits += 1

    devices = [await app.add_device(f"h{i}", LoadDevice) for i in range(20)]
    await app.start()

    async def burst(dev):
        for v in range(100):
            await dev.handle_state({"value": v})

    await asyncio.gather(*(burst(d) for d in devices))
    await asyncio.sleep(0.05)
    await app.stop()

    assert rule_hits >= 20 * 100


@pytest.mark.stress
@pytest.mark.asyncio
async def test_load_many_devices_with_device_level_rules():
    """Test many devices each with their own device-level rules."""
    app = KamioApp()
    device_count = 50
    updates = 100

    devices = []
    for i in range(device_count):
        d = await app.add_device(f"dr{i}", LoadDeviceWithRule)
        devices.append(d)

    await app.start()

    async def spam(dev):
        for v in range(updates):
            await dev.handle_state({"value": v})

    await asyncio.gather(*(spam(d) for d in devices))
    await asyncio.sleep(0.05)

    await app.stop()

    # Each device should have its rule hit for every update
    total_device_rule_hits = sum(d.rule_hits for d in devices)
    assert total_device_rule_hits >= device_count * updates


@pytest.mark.stress
@pytest.mark.asyncio
async def test_mixed_app_and_device_level_rules():
    """Test app-level and device-level rules working together under load."""
    app = KamioApp()
    device_count = 30
    updates = 50
    app_rule_hits = 0

    @app.rule(device=LoadDeviceWithRule, fields=["value"])
    async def app_level_rule(event, app):
        nonlocal app_rule_hits
        app_rule_hits += 1

    devices = []
    for i in range(device_count):
        d = await app.add_device(f"mix{i}", LoadDeviceWithRule)
        devices.append(d)

    await app.start()

    async def spam(dev):
        for v in range(updates):
            await dev.handle_state({"value": v})

    await asyncio.gather(*(spam(d) for d in devices))
    await asyncio.sleep(0.05)

    await app.stop()

    # Both app-level and device-level rules should have fired
    total_device_rule_hits = sum(d.rule_hits for d in devices)
    assert total_device_rule_hits >= device_count * updates
    assert app_rule_hits >= device_count * updates


@pytest.mark.stress
@pytest.mark.asyncio
async def test_device_level_rules_memory_stability():
    """Test that device-level rules don't cause memory leaks over many cycles."""
    tracemalloc.start()
    start = tracemalloc.take_snapshot()

    for cycle in range(5):
        app = KamioApp()
        devices = []
        for i in range(20):
            d = await app.add_device(f"mem{i}", LoadDeviceWithRule)
            devices.append(d)

        await app.start()

        async def spam(dev):
            for v in range(50):
                await dev.handle_state({"value": v})

        await asyncio.gather(*(spam(d) for d in devices))
        await asyncio.sleep(0.05)

        await app.stop()
        del app, devices
        gc.collect()

    end = tracemalloc.take_snapshot()
    diff = end.compare_to(start, "lineno")
    total_increase = sum(s.size_diff for s in diff if s.size_diff > 0)
    # Allow small growth from interpreter caches; fail on large leak
    assert total_increase < 3 * 1024 * 1024, f"Memory grew by {total_increase} bytes"
