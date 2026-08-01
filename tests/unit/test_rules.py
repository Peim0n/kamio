from __future__ import annotations

import asyncio

import pytest

from kamio import Device, KamioApp, rule, state
from kamio.core.rules import RuleEvent


class Thermostat(Device):
    temperature: float = state(default=20.0, writable=True)
    target: float = state(default=22.0, writable=True)


class Sensor(Device):
    motion: bool = state(default=False, writable=True)


class SmartLight(Device):
    power: bool = state(default=False, writable=True)
    brightness: int = state(default=100, min=0, max=255, writable=True)

    @rule(fields=["power"])
    async def on_power_change(self, event: RuleEvent, app: KamioApp):
        """Device-level rule that reacts to power changes."""
        self.brightness = 200 if event.data.get("power") else 100


class CounterDevice(Device):
    count: int = state(default=0, writable=True)
    rule_calls: int = 0

    @rule(fields=["count"])
    async def on_count_change(self, event: RuleEvent, app: KamioApp):
        """Device-level rule that tracks how many times it was called."""
        self.rule_calls += 1


@pytest.mark.asyncio
async def test_app_rule_triggered_by_state_change():
    app = KamioApp()
    calls = []

    @app.rule(device=Thermostat, fields=["temperature"])
    async def on_temp(event: RuleEvent, app: KamioApp):
        calls.append((event.device_id, event.data, event.kind))

    device = await app.add_device("t1", Thermostat)
    await device.handle_state({"temperature": 25.0})

    assert len(calls) == 1
    assert calls[0][0] == "t1"
    assert calls[0][1]["temperature"] == 25.0
    assert calls[0][2] == "event"


@pytest.mark.asyncio
async def test_rule_field_filter_ignores_untracked_fields():
    app = KamioApp()
    calls = []

    @app.rule(device=Thermostat, fields=["temperature"])
    async def on_temp(event: RuleEvent, app: KamioApp):
        calls.append(event.data)

    device = await app.add_device("t1", Thermostat)
    await device.handle_state({"target": 25.0})

    assert calls == []


@pytest.mark.asyncio
async def test_unfiltered_rule_triggers_on_any_state_change():
    app = KamioApp()
    calls = []

    @app.rule(device=Thermostat)
    async def on_any(event: RuleEvent, app: KamioApp):
        calls.append(event.data)

    device = await app.add_device("t1", Thermostat)
    await device.handle_state({"temperature": 21.0, "target": 23.0})
    assert any(d.get("temperature") == 21.0 for d in calls)


@pytest.mark.asyncio
async def test_interval_rule_runs_periodically_and_on_start():
    app = KamioApp()
    calls = []

    @app.rule(interval=0.05, run_on_start=True)
    async def periodic(event: RuleEvent, app: KamioApp):
        calls.append(event.kind)

    await app.start()
    await asyncio.sleep(0.12)
    await app.stop()

    assert "interval" in calls
    # Should run at least twice (run_on_start + interval ticks).
    assert len(calls) >= 2


@pytest.mark.asyncio
async def test_add_rule_explicitly():
    app = KamioApp()
    called = []

    async def handler(event: RuleEvent, app: KamioApp):
        called.append(event.data)

    app.add_rule(handler, device=Sensor, fields=["motion"])
    device = await app.add_device("s1", Sensor)
    await device.handle_state({"motion": True})
    assert called[0]["motion"] is True


@pytest.mark.asyncio
async def test_remove_rule_is_async():
    app = KamioApp()
    calls = []

    @app.rule(device=Sensor, fields=["motion"])
    async def on_motion(event: RuleEvent, app: KamioApp):
        calls.append(event.data)

    device = await app.add_device("s1", Sensor)
    await app.remove_rule(on_motion)
    await device.handle_state({"motion": True})
    assert calls == []


@pytest.mark.asyncio
async def test_rule_for_base_class_uses_mro():
    class BaseSensor(Device):
        value: float = state(default=0.0, writable=True)

    class Specific(BaseSensor):
        pass

    app = KamioApp()
    calls = []

    @app.rule(device=BaseSensor, fields=["value"])
    async def on_base(event: RuleEvent, app: KamioApp):
        calls.append(event.data)

    device = await app.add_device("specific", Specific)
    await device.handle_state({"value": 42.0})
    assert calls[0]["value"] == 42.0


@pytest.mark.asyncio
async def test_rule_errors_do_not_stop_other_rules():
    app = KamioApp()
    ok = []

    @app.rule(device=Thermostat, fields=["temperature"])
    async def fail(event: RuleEvent, app: KamioApp):
        raise RuntimeError("boom")

    @app.rule(device=Thermostat, fields=["temperature"])
    async def succeed(event: RuleEvent, app: KamioApp):
        ok.append(event.data)

    device = await app.add_device("t1", Thermostat)
    await device.handle_state({"temperature": 30.0})
    assert ok[0]["temperature"] == 30.0


# ---------------------------------------------------------------------------
# Device-level @rule decorator tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_device_level_rule_auto_registered():
    """Device-level @rule decorator should auto-register when device class is registered."""
    app = KamioApp()

    # Register the device class - this should auto-register the @rule method
    app.register(SmartLight)

    # Check that the rule was added to the rule engine
    assert len(app.rules.rules) == 1
    rule = app.rules.rules[0]
    assert rule.device_class == SmartLight
    assert rule.fields == ["power"]


@pytest.mark.asyncio
async def test_device_level_rule_executes_on_state_change():
    """Device-level rule should execute when its monitored field changes."""
    app = KamioApp()

    device = await app.add_device("light1", SmartLight)
    assert device.brightness == 100
    assert device.power is False

    # Change power - should trigger device-level rule
    await device.handle_state({"power": True})

    # Rule should have changed brightness
    assert device.brightness == 200

    # Change power back
    await device.handle_state({"power": False})
    assert device.brightness == 100


@pytest.mark.asyncio
async def test_device_level_rule_with_field_filter():
    """Device-level rule with field filter should only trigger on specified fields."""
    app = KamioApp()

    device = await app.add_device("counter1", CounterDevice)
    assert device.rule_calls == 0

    # Change count - should trigger rule
    await device.handle_state({"count": 5})
    assert device.rule_calls == 1

    # Change count again
    await device.handle_state({"count": 10})
    assert device.rule_calls == 2


@pytest.mark.asyncio
async def test_device_level_rule_ignores_other_fields():
    """Device-level rule should not trigger when other fields change."""
    app = KamioApp()

    device = await app.add_device("light1", SmartLight)
    assert device.brightness == 100

    # Change brightness (not monitored by the rule)
    await device.handle_state({"brightness": 150})

    # Rule should NOT have changed brightness back to 100
    assert device.brightness == 150


@pytest.mark.asyncio
async def test_device_level_rule_with_app_level_rule_coexist():
    """Device-level and app-level rules should coexist and both trigger."""
    app = KamioApp()
    app_level_calls = []

    @app.rule(device=SmartLight, fields=["power"])
    async def app_level_rule(event: RuleEvent, app: KamioApp):
        app_level_calls.append(event.data)

    device = await app.add_device("light1", SmartLight)

    # Change power - both rules should trigger
    await device.handle_state({"power": True})

    # Device-level rule changed brightness
    assert device.brightness == 200
    # App-level rule was called
    assert len(app_level_calls) == 1
    assert app_level_calls[0]["power"] is True


@pytest.mark.asyncio
async def test_multiple_device_level_rules_on_same_device():
    """Multiple device-level rules on the same device should all work."""

    class MultiRuleDevice(Device):
        value: int = state(default=0, writable=True)
        flag: bool = state(default=False, writable=True)
        rule1_calls = 0
        rule2_calls = 0

        @rule(fields=["value"])
        async def on_value_change(self, event: RuleEvent, app: KamioApp):
            self.rule1_calls += 1

        @rule(fields=["flag"])
        async def on_flag_change(self, event: RuleEvent, app: KamioApp):
            self.rule2_calls += 1

    app = KamioApp()
    device = await app.add_device("multi1", MultiRuleDevice)

    # Both rules should be registered
    assert len(app.rules.rules) == 2

    # Trigger first rule
    await device.handle_state({"value": 42})
    assert device.rule1_calls == 1
    assert device.rule2_calls == 0

    # Trigger second rule
    await device.handle_state({"flag": True})
    assert device.rule1_calls == 1
    assert device.rule2_calls == 1


@pytest.mark.asyncio
async def test_device_level_rule_accesses_self_correctly():
    """Device-level rule should properly access self (device instance)."""

    class SelfAccessDevice(Device):
        value: int = state(default=0, writable=True)
        last_self = None

        @rule(fields=["value"])
        async def on_value_change(self, event: RuleEvent, app: KamioApp):
            # Store reference to self to verify it's the device instance
            self.last_self = self
            # Verify we can access other device state
            self.value = event.data.get("value", 0) * 2

    app = KamioApp()
    device = await app.add_device("self1", SelfAccessDevice)

    await device.handle_state({"value": 5})

    # Rule should have doubled the value
    assert device.value == 10
    # self should be the device instance
    assert device.last_self is device


@pytest.mark.asyncio
async def test_device_level_rule_inheritance():
    """Device-level rules should be inherited from base classes."""

    class BaseDevice(Device):
        value: int = state(default=0, writable=True)
        base_rule_calls = 0

        @rule(fields=["value"])
        async def base_rule(self, event: RuleEvent, app: KamioApp):
            self.base_rule_calls += 1

    class DerivedDevice(BaseDevice):
        extra: str = state(default="test", writable=True)
        derived_rule_calls = 0

        @rule(fields=["extra"])
        async def derived_rule(self, event: RuleEvent, app: KamioApp):
            self.derived_rule_calls += 1

    app = KamioApp()
    device = await app.add_device("derived1", DerivedDevice)

    # Both rules should be registered (from base and derived)
    assert len(app.rules.rules) == 2

    # Trigger base rule
    await device.handle_state({"value": 1})
    assert device.base_rule_calls == 1
    assert device.derived_rule_calls == 0

    # Trigger derived rule
    await device.handle_state({"extra": "changed"})
    assert device.base_rule_calls == 1
    assert device.derived_rule_calls == 1


@pytest.mark.asyncio
async def test_set_rules_replaces_rule_set_and_cancels_tasks():
    """set_rules should atomically replace the rule set and await cancelled tasks."""
    from kamio.core.rules import Rule, RuleEngine

    engine = RuleEngine(KamioApp())

    async def _r1(event, app):
        pass

    async def _r2(event, app):
        pass

    r1 = Rule(_r1, interval=0.01)
    r2 = Rule(_r2, interval=0.01)
    engine.add_rule(r1)
    engine.add_rule(r2)
    await engine.start()
    # Both interval tasks should be running.
    assert r1.task is not None and not r1.task.done()
    assert r2.task is not None and not r2.task.done()

    # Replace with an empty rule set — old tasks must be cancelled & awaited.
    await engine.set_rules([])
    assert len(engine.rules) == 0
    # Give cancelled tasks a moment to settle.
    await asyncio.sleep(0.01)
    assert r1.task.done()
    assert r2.task.done()
    await engine.stop()
