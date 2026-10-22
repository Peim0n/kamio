from __future__ import annotations

import asyncio
import random

import pytest
from conftest import wait_until

from kamio import Device, KamioApp, command, rule, state, telemetry
from kamio.core.envelope import Envelope, EnvelopeType
from kamio.core.rules import RuleEvent


class Relay(Device):
    active: bool = state(default=False, writable=True)
    rule_activations: int = 0

    @command
    async def activate(self):
        await self.handle_state({"active": True})
        return {"active": self.active}

    @rule(fields=["active"])
    async def on_active_change(self, event: RuleEvent, app: KamioApp):
        """Device-level rule that tracks activations."""
        if event.data.get("active") is True:
            self.rule_activations += 1


@pytest.mark.stress
@pytest.mark.asyncio
async def test_command_chain_across_20_apps():
    """Twenty KamioApp instances exchange commands through a shared in-memory broker.

    The first app is triggered, then each app sends a command to the next one
    until all twenty have been activated.
    """
    count = 20
    device_ids = [f"relay_{i}" for i in range(count)]
    apps: list[KamioApp] = []

    async def build_app(i: int) -> KamioApp:
        app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id=f"app_{i}")
        # Register the same device class under each app.
        app.register(Relay)

        @app.rule(device=Relay, fields=["active"])
        async def propagate(event: dict, app: KamioApp):
            if i < count - 1 and event.data.get("active") is True:
                target_id = device_ids[i + 1]
                await app.devices[device_ids[i]].send_command(
                    target_id, "activate", {}, timeout=60.0
                )

        await app.add_device(device_ids[i], Relay)
        return app

    apps = await asyncio.gather(*(build_app(i) for i in range(count)))
    await asyncio.gather(*(app.start() for app in apps))

    try:
        # Trigger the first device in the chain.
        await apps[0].devices[device_ids[0]].handle_command("activate", {})

        # Wait until propagation through the broker has activated every relay
        # and its device-level rule.
        assert await wait_until(
            lambda: all(
                apps[i].devices[device_ids[i]].active
                and apps[i].devices[device_ids[i]].rule_activations >= 1
                for i in range(count)
            ),
            timeout=60.0,
        ), "not all relays were activated"
    finally:
        await asyncio.gather(*(app.stop() for app in apps))


@pytest.mark.stress
@pytest.mark.asyncio
async def test_broadcast_to_all_20_apps():
    """A single broadcast message is received by every app in the fleet."""
    count = 20

    async def build_app(i: int) -> KamioApp:
        return KamioApp(mqtt_broker="mqtt://localhost:1883", client_id=f"broadcast_app_{i}")

    apps = await asyncio.gather(*(build_app(i) for i in range(count)))
    await asyncio.gather(*(app.start() for app in apps))

    received = [asyncio.Event() for _ in range(count)]
    for i, app in enumerate(apps):
        # ServerNode owns the app-level broadcast wildcard Kamio/v1/all/#.
        async def _on_broadcast(env, event=received[i]):
            if env.data.get("test") == 1:
                event.set()

        app.server_node.on(EnvelopeType.DEVICE_STATE, _on_broadcast)

    try:
        # Publish a state change broadcast from the first app.
        payload = Envelope.state(source="app_0", data={"test": 1}).to_json().encode()
        apps[0].mqtt_client.publish("Kamio/v1/all/ds", payload, qos=1)

        assert await wait_until(
            lambda: all(ev.is_set() for ev in received), timeout=30.0
        ), "not all 20 apps received the broadcast"
    finally:
        await asyncio.gather(*(app.stop() for app in apps))


@pytest.mark.stress
@pytest.mark.asyncio
async def test_mesh_commands_20_apps():
    """Each app sends a command to every other app: 20 * 19 = 380 interactions."""
    count = 20
    device_ids = [f"mesh_node_{i}" for i in range(count)]

    async def build_app(i: int) -> KamioApp:
        app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id=f"mesh_app_{i}")

        @app.device
        class Counter(Device):
            pings: int = state(default=0, writable=True)

            @command
            def ping(self):
                self.pings += 1
                return {"pings": self.pings}

        await app.add_device(device_ids[i], Counter)
        return app

    apps = await asyncio.gather(*(build_app(i) for i in range(count)))
    await asyncio.gather(*(app.start() for app in apps))

    try:

        async def send_from(i: int):
            sender = apps[i].devices[device_ids[i]]
            for j in range(count):
                if i != j:
                    await sender.send_command(device_ids[j], "ping", {}, timeout=60.0)

        # send_command awaits the command ACK, so once gather finishes every
        # ping has been applied on the target device.
        await asyncio.gather(*(send_from(i) for i in range(count)))

        for i, app in enumerate(apps):
            received = app.devices[device_ids[i]].pings
            assert received == count - 1, f"app {i} received {received} pings"
    finally:
        await asyncio.gather(*(app.stop() for app in apps))


@pytest.mark.stress
@pytest.mark.asyncio
async def test_telemetry_state_command_cascade_20_apps():
    """Telemetry -> state update -> rule -> command -> next telemetry, across 20 apps."""
    count = 20
    sensor_ids = [f"tc_sensor_{i}" for i in range(count)]
    relay_ids = [f"tc_relay_{i}" for i in range(count)]

    async def build_app(i: int) -> KamioApp:
        app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id=f"tc_app_{i}")

        @app.device
        class Sensor(Device):
            temp: float = telemetry(default=20.0, unit="°C")
            bumped: bool = state(default=False, writable=True)

            @command
            async def bump(self):
                await self.handle_state({"bumped": True})
                await self.publish_telemetry({"temp": 25.0})
                return {"temp": 25.0}

        @app.device
        class Relay(Device):
            active: bool = state(default=False, writable=True)
            status: str = telemetry(default="off")

            @command
            async def activate(self):
                await self.handle_state({"active": True})
                await self.publish_telemetry({"status": "on"})
                return {"active": self.active}

        @app.rule(device=Sensor, fields=["temp"])
        async def on_sensor_temp(event, app):
            if event.data.get("temp", 20.0) != 20.0:
                await app.devices[sensor_ids[i]].send_command(
                    relay_ids[i], "activate", {}, timeout=60.0
                )

        @app.rule(device=Relay, fields=["status"])
        async def on_relay_status(event, app):
            if i < count - 1 and event.data.get("status") == "on":
                await app.devices[relay_ids[i]].send_command(
                    sensor_ids[i + 1], "bump", {}, timeout=60.0
                )

        await app.add_device(sensor_ids[i], Sensor)
        await app.add_device(relay_ids[i], Relay)
        return app

    apps = await asyncio.gather(*(build_app(i) for i in range(count)))
    await asyncio.gather(*(app.start() for app in apps))

    try:
        # Trigger the cascade locally; the device command publishes telemetry,
        # which the broker echoes back to the same app and drives the rule chain.
        await apps[0].devices[sensor_ids[0]].handle_command("bump", {})

        assert await wait_until(
            lambda: all(
                apps[i].devices[sensor_ids[i]].bumped and apps[i].devices[relay_ids[i]].active
                for i in range(count)
            ),
            timeout=60.0,
        ), "cascade did not reach all apps"
    finally:
        await asyncio.gather(*(app.stop() for app in apps))


@pytest.mark.stress
@pytest.mark.asyncio
async def test_three_devices_per_app_full_cross_talk_20x3():
    """20 apps with 3 devices each: switch -> sensor telemetry -> actuator -> next switch."""
    app_count = 20
    switch_ids = [f"switch_{i}" for i in range(app_count)]
    sensor_ids = [f"sensor_{i}" for i in range(app_count)]
    actuator_ids = [f"actuator_{i}" for i in range(app_count)]

    async def build_app(i: int) -> KamioApp:
        app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id=f"multi_app_{i}")

        @app.device
        class Switch(Device):
            power: bool = state(default=False, writable=True)

            @command
            async def turn_on(self):
                await self.handle_state({"power": True})
                return {"power": self.power}

        @app.device
        class Sensor(Device):
            reading: float = telemetry(default=0.0, unit="V")
            sampled: bool = state(default=False, writable=True)

            @command
            async def sample(self):
                await self.handle_state({"sampled": True})
                await self.publish_telemetry({"reading": 1.0})
                return {"reading": 1.0}

        @app.device
        class Actuator(Device):
            moved: bool = state(default=False, writable=True)

            @command
            async def move(self):
                await self.handle_state({"moved": True})
                return {"moved": self.moved}

        @app.rule(device=Switch, fields=["power"])
        async def switch_drives_sensor(event, app):
            if event.data.get("power") is True:
                await app.devices[switch_ids[i]].send_command(
                    sensor_ids[i], "sample", {}, timeout=60.0
                )

        @app.rule(device=Sensor, fields=["reading"])
        async def sensor_drives_actuator(event, app):
            if event.data.get("reading", 0.0) > 0.0:
                await app.devices[sensor_ids[i]].send_command(
                    actuator_ids[i], "move", {}, timeout=60.0
                )

        @app.rule(device=Actuator, fields=["moved"])
        async def actuator_chains_to_next(event, app):
            if i < app_count - 1 and event.data.get("moved") is True:
                await app.devices[actuator_ids[i]].send_command(
                    switch_ids[i + 1], "turn_on", {}, timeout=60.0
                )

        await app.add_device(switch_ids[i], Switch)
        await app.add_device(sensor_ids[i], Sensor)
        await app.add_device(actuator_ids[i], Actuator)
        return app

    apps = await asyncio.gather(*(build_app(i) for i in range(app_count)))
    await asyncio.gather(*(app.start() for app in apps))

    try:
        await apps[0].devices[switch_ids[0]].handle_command("turn_on", {})

        assert await wait_until(
            lambda: all(
                apps[i].devices[switch_ids[i]].power
                and apps[i].devices[sensor_ids[i]].sampled
                and apps[i].devices[actuator_ids[i]].moved
                for i in range(app_count)
            ),
            timeout=60.0,
        ), "cross-talk chain did not reach all apps"
    finally:
        await asyncio.gather(*(app.stop() for app in apps))


@pytest.mark.stress
@pytest.mark.asyncio
async def test_one_app_100_devices_rule_broadcast():
    """One app with 100 toggles: a master pulse triggers a rule that toggles all 100 devices.

    This exercises the event bus, automation rules, and in-app command dispatch
    for a large number of devices in one application.
    """
    count = 100
    device_ids = [f"toggle_{i}" for i in range(count)]
    master_id = "master"
    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="single_app")

    @app.device
    class Toggle(Device):
        on: bool = state(default=False, writable=True)

        @command
        async def toggle(self):
            await self.handle_state({"on": not self.on})
            return {"on": self.on}

    @app.device
    class Master(Device):
        pulse: bool = state(default=False, writable=True)

    @app.rule(device=Master, fields=["pulse"])
    async def broadcast_pulse(event, app):
        if event.data.get("pulse") is True:
            await asyncio.gather(
                *(app.devices[dev_id].handle_command("toggle", {}) for dev_id in device_ids)
            )

    await app.add_device(master_id, Master)
    for device_id in device_ids:
        await app.add_device(device_id, Toggle)

    await app.start()
    try:
        await app.devices[master_id].handle_state({"pulse": True})

        assert await wait_until(
            lambda: all(app.devices[d].on for d in device_ids), timeout=30.0
        ), "not all 100 toggles were activated"
    finally:
        await app.stop()


class Counter(Device):
    """A test device with state, telemetry, commands and a built-in ping counter."""

    pings: int = state(default=0, writable=True)
    armed: bool = state(default=False, writable=True)
    temp: float = telemetry(default=20.0, unit="°C")
    device_rule_hits: int = 0

    @command
    def ping(self):
        self.pings += 1
        return {"pings": self.pings}

    @command
    async def arm(self):
        await self.handle_state({"armed": not self.armed})
        return {"armed": self.armed}

    @rule(fields=["armed"])
    async def on_arm_change(self, event: RuleEvent, app: KamioApp):
        """Device-level rule that tracks arm state changes."""
        if event.data.get("armed") is not None:
            self.device_rule_hits += 1

    @rule(fields=["temp"])
    async def on_temp_change(self, event: RuleEvent, app: KamioApp):
        """Device-level rule that tracks temperature changes."""
        if event.data.get("temp", 0.0) > 30.0:
            self.device_rule_hits += 1


@pytest.mark.stress
@pytest.mark.asyncio
async def test_20_apps_100_devices_random_interactions():
    """20 apps, 100 devices each: random commands, states, telemetry and rules."""
    random.seed(42)
    count_apps = 20
    per_app = 100
    interactions = 1000
    all_ids: list[tuple[int, str]] = []

    async def build_app(i: int) -> KamioApp:
        app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id=f"rand20_app_{i}")
        app.register(Counter)

        @app.rule(device=Counter, fields=["armed"])
        async def on_arm(event, app):
            if event.data.get("armed") is not None:
                await app.devices[event.device_id].publish_telemetry({"temp": 35.0})

        @app.rule(device=Counter, fields=["temp"])
        async def on_temp(event, app):
            if event.data.get("temp", 0.0) > 30.0:
                await app.devices[event.device_id].handle_command("ping", {})

        for j in range(per_app):
            dev_id = f"counter_{i}_{j}"
            await app.add_device(dev_id, Counter)
            all_ids.append((i, dev_id))
        return app

    apps = await asyncio.gather(*(build_app(i) for i in range(count_apps)))
    await asyncio.gather(*(app.start() for app in apps))

    sem = asyncio.Semaphore(50)

    async def act(_: int) -> None:
        action = random.choice(["ping", "arm", "set_arm", "telemetry"])
        src_app, src_id = random.choice(all_ids)
        tgt_app, tgt_id = random.choice(all_ids)
        async with sem:
            if action == "ping":
                if src_app == tgt_app:
                    await apps[tgt_app].devices[tgt_id].handle_command("ping", {})
                else:
                    await apps[src_app].devices[src_id].send_command(
                        tgt_id, "ping", {}, timeout=60.0
                    )
            elif action == "arm":
                if src_app == tgt_app:
                    await apps[tgt_app].devices[tgt_id].handle_command("arm", {})
                else:
                    await apps[src_app].devices[src_id].send_command(
                        tgt_id, "arm", {}, timeout=60.0
                    )
            elif action == "set_arm":
                await apps[tgt_app].devices[tgt_id].handle_command("arm", {})
            else:  # telemetry
                await apps[tgt_app].devices[tgt_id].publish_telemetry({"temp": 35.0})

    try:
        await asyncio.gather(*(act(i) for i in range(interactions)))

        # Every action produces exactly one ping — directly, or through the
        # arm -> telemetry -> rule -> ping chain which completes over MQTT.
        def total_pings() -> int:
            return sum(
                apps[i].devices[f"counter_{i}_{j}"].pings
                for i in range(count_apps)
                for j in range(per_app)
            )

        assert await wait_until(
            lambda: total_pings() == interactions, timeout=60.0
        ), f"expected {interactions} total pings, got {total_pings()}"

        # Verify device-level rules were triggered
        total_device_rule_hits = sum(
            apps[i].devices[f"counter_{i}_{j}"].device_rule_hits
            for i in range(count_apps)
            for j in range(per_app)
        )
        assert total_device_rule_hits > 0, "Device-level rules were not triggered"
    finally:
        await asyncio.gather(*(app.stop() for app in apps))


@pytest.mark.stress
@pytest.mark.asyncio
async def test_2_apps_1000_devices_random_interactions():
    """2 apps, 1000 devices each: random commands, states, telemetry and rules."""
    random.seed(7)
    count_apps = 2
    per_app = 1000
    interactions = 2000
    all_ids: list[tuple[int, str]] = []

    async def build_app(i: int) -> KamioApp:
        app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id=f"rand2_app_{i}")
        app.register(Counter)

        @app.rule(device=Counter, fields=["armed"])
        async def on_arm(event, app):
            if event.data.get("armed") is not None:
                await app.devices[event.device_id].publish_telemetry({"temp": 35.0})

        @app.rule(device=Counter, fields=["temp"])
        async def on_temp(event, app):
            if event.data.get("temp", 0.0) > 30.0:
                await app.devices[event.device_id].handle_command("ping", {})

        for j in range(per_app):
            dev_id = f"counter_{i}_{j}"
            await app.add_device(dev_id, Counter)
            all_ids.append((i, dev_id))
        return app

    apps = await asyncio.gather(*(build_app(i) for i in range(count_apps)))
    await asyncio.gather(*(app.start() for app in apps))

    sem = asyncio.Semaphore(50)

    async def act(_: int) -> None:
        action = random.choice(["ping", "arm", "set_arm", "telemetry"])
        src_app, src_id = random.choice(all_ids)
        tgt_app, tgt_id = random.choice(all_ids)
        async with sem:
            if action == "ping":
                if src_app == tgt_app:
                    await apps[tgt_app].devices[tgt_id].handle_command("ping", {})
                else:
                    await apps[src_app].devices[src_id].send_command(
                        tgt_id, "ping", {}, timeout=60.0
                    )
            elif action == "arm":
                if src_app == tgt_app:
                    await apps[tgt_app].devices[tgt_id].handle_command("arm", {})
                else:
                    await apps[src_app].devices[src_id].send_command(
                        tgt_id, "arm", {}, timeout=60.0
                    )
            elif action == "set_arm":
                await apps[tgt_app].devices[tgt_id].handle_command("arm", {})
            else:  # telemetry
                await apps[tgt_app].devices[tgt_id].publish_telemetry({"temp": 35.0})

    try:
        await asyncio.gather(*(act(i) for i in range(interactions)))

        # Every action produces exactly one ping — directly, or through the
        # arm -> telemetry -> rule -> ping chain which completes over MQTT.
        def total_pings() -> int:
            return sum(
                apps[i].devices[f"counter_{i}_{j}"].pings
                for i in range(count_apps)
                for j in range(per_app)
            )

        assert await wait_until(
            lambda: total_pings() == interactions, timeout=60.0
        ), f"expected {interactions} total pings, got {total_pings()}"

        # Verify device-level rules were triggered
        total_device_rule_hits = sum(
            apps[i].devices[f"counter_{i}_{j}"].device_rule_hits
            for i in range(count_apps)
            for j in range(per_app)
        )
        assert total_device_rule_hits > 0, "Device-level rules were not triggered"
    finally:
        await asyncio.gather(*(app.stop() for app in apps))


@pytest.mark.stress
@pytest.mark.asyncio
async def test_20_types_10_each_maximum_load():
    """One app with 20 device types, 10 states/telemetry/commands/rules each.

    Randomly triggers commands that toggle states, publish telemetry and fire
    cross-type rules.  Designed to stress the in-process event loop and rule
    engine with a large, heterogeneous device graph.
    """
    random.seed(123)
    type_count = 20
    per_type = 5
    state_count = 10
    interactions = 2000
    sem = asyncio.Semaphore(50)

    def make_type(i: int):
        name = f"Type{i:02d}"
        attrs: dict[str, Any] = {}
        attrs["calls"] = state(default=0, writable=True)
        for j in range(state_count):
            attrs[f"s{j}"] = state(default=False, writable=True)
            attrs[f"t{j}"] = telemetry(default=0.0)

        def make_cmd(j: int):
            async def cmd(self: Device) -> dict:
                self.calls += 1
                await self.handle_state({f"s{j}": not getattr(self, f"s{j}")})
                await self.publish_telemetry({f"t{j}": float(j)})
                return {"calls": self.calls}

            cmd.__name__ = f"cmd_{j}"
            return cmd

        for j in range(state_count):
            attrs[f"cmd_{j}"] = command(make_cmd(j))
        return type(name, (Device,), attrs)

    device_types = [make_type(i) for i in range(type_count)]
    device_ids_by_type: list[list[str]] = [[] for _ in range(type_count)]

    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="max_load")

    rule_invocations = 0

    for i, cls in enumerate(device_types):
        for k in range(per_type):
            dev_id = f"dev_{i:02d}_{k}"
            await app.add_device(dev_id, cls)
            device_ids_by_type[i].append(dev_id)

        # Each state field on type i triggers cmd_0 on the next type.
        for j in range(state_count):
            field = f"s{j}"

            def make_rule(i=i, j=j, field=field):
                async def rule(event, app):
                    nonlocal rule_invocations
                    if event.data.get(field) is True and i + 1 < type_count:
                        next_id = device_ids_by_type[i + 1][0]
                        if next_id in app.devices:
                            rule_invocations += 1
                            await app.devices[next_id].handle_command("cmd_0", {})

                return rule

            app.add_rule(make_rule(), device=cls, fields=[field])

    await app.start()

    all_ids = [dev_id for ids in device_ids_by_type for dev_id in ids]

    async def act(_: int) -> None:
        dev_id = random.choice(all_ids)
        cmd = random.choice([f"cmd_{j}" for j in range(state_count)])
        async with sem:
            await app.devices[dev_id].handle_command(cmd, {})

    try:
        # handle_state awaits _on_rules_trigger inline, so the whole
        # rule-driven cascade has drained once gather returns.
        await asyncio.gather(*(act(i) for i in range(interactions)))

        # Exact count: every direct command plus every rule-triggered cmd_0
        # increments `calls` exactly once.
        total_calls = sum(app.devices[dev_id].calls for ids in device_ids_by_type for dev_id in ids)
        assert total_calls == interactions + rule_invocations, (
            f"expected exactly {interactions + rule_invocations} calls "
            f"({interactions} direct + {rule_invocations} rule-triggered), "
            f"got {total_calls}"
        )
    finally:
        await app.stop()
