# Kamio Core Architecture v1.0.0b2

## Module Structure

```
Kamio/
  app/                        # KamioApp — package
    __init__.py               #   re-exports KamioApp + all mixins
    _application.py           #   KamioApp class
    mixins/
      lifecycle.py            #   LifecycleMixin   — start/stop/run/signal
      mqtt.py                 #   MqttDispatchMixin — MQTT callbacks, dispatch
      devices.py              #   DeviceRegistryMixin — add/remove/register device
      rules.py                #   RuleRegistryMixin — @rule, add_rule, remove_rule
      plugins.py              #   PluginFacadeMixin — load/unload/list plugins
      hot_reload.py           #   HotReloadFacadeMixin — watch_file/watch_directory
      custom_nodes.py         #   CustomNodeFacadeMixin — register/list custom nodes
      hooks_events.py         #   HookEventFacadeMixin — hooks + event_bus shortcuts
  device.py                   # Device + @command
  data_fields.py              # Field, state(), telemetry(), event(), config(), parse_freq()
  config.py                   # Config — JSON file + Kamio_* env variables
  discovery.py                # HADiscovery — Home Assistant MQTT Discovery (lazy)
  core/
    transport/                # namespace package for transport layer
      __init__.py             #   re-export: MqttConnection, nodes, topics, Envelope
    automation/               # namespace package for automation layer
      __init__.py             #   re-export: Rule, RuleEngine, RuleEvent, EventBus, HooksManager
    device_meta.py            # DeviceMeta — metaclass: Kamio_FIELDS/COMMANDS/EVENTS
    envelope.py               # Envelope — JSON wrapper for MQTT message
    event_bus.py              # EventBus — pub/sub with filter_fn and priorities
    hot_reload.py             # HotReloadManager — asyncio polling + debounce + watchdog
    custom_nodes.py           # CustomNode ABC + CustomNodeManager
    handlers.py               # DeviceHandler — dispatcher: Envelope → Device
    hooks.py                  # HooksManager — lifecycle hooks
    mixins.py                 # TelemetryMixin, TaskManagerMixin
    mqtt_connection.py        # MqttConnection — gmqtt client with auto-reconnect
    mqtt_nodes.py             # BaseNode, ServerNode, DeviceNode
    registry.py               # DeviceRegistry — registry of classes and instances
    rules.py                  # Rule, RuleEngine, RuleEvent
    state.py                  # StateManager — centralized state
    subscription.py           # PriorityRegistry, AsyncPriorityDispatcher
    topics.py                 # MQTT topic helpers + parse()
    correlation.py            # CommandManager — request/response correlation by cind
  plugins/
    base.py                   # Plugin — ABC
    loader.py                 # PluginLoader + PluginContext (scoped cleanup)
    builtin/
      logging_plugin.py       # LoggingPlugin — events → rotating log file
      metrics_plugin.py       # MetricsPlugin — in-memory event counters
  drivers/
    base.py                   # BaseDriver — 4 abstract methods
    mock.py                   # MockHardwareDriver — latency/failure simulation
    gpio.py                   # GPIOChipDriver
    serial.py                 # SerialDriver
    telnet.py                 # TelnetDriver
    http.py                   # HTTPDeviceDriver
    udp.py                    # UDPDriver
    modbus.py                 # ModbusTCPDriver (pure asyncio)

tests/
  unit/          # isolated component tests (config, fields, rules, hooks…)
  stress/        # tests under load (load_stability, stress)
  conftest.py    # root fixtures (mock_mqtt, app)
```

## Components

| Component | File | Role |
|---|---|---|
| `KamioApp` | `app/_application.py` | Main orchestrator. Assembled from 8 mixins |
| `Device` | `device.py` | Base IoT device class. Extended by the user |
| `DeviceMeta` | `core/device_meta.py` | Metaclass — collects Kamio_FIELDS / COMMANDS / EVENTS at class definition |
| `DeviceNode` | `core/mqtt_nodes.py` | MQTT node for a single device: subscribe/publish, filtering by `device_id` |
| `ServerNode` | `core/mqtt_nodes.py` | Application MQTT node: receives states and telemetry from all devices |
| `DeviceHandler` | `core/handlers.py` | Dispatcher: receives `Envelope`, calls `handle_state`/`handle_command`; injects callbacks into `Device` |
| `RuleEngine` | `core/rules.py` | Triggers rules on device event or by interval; index is always synchronized |
| `StateManager` | `core/state.py` | Stores the current state of all devices in memory |
| `DeviceRegistry` | `core/registry.py` | Stores device classes (`_classes`) and instances (`_instances`) |
| `Envelope` | `core/envelope.py` | JSON wrapper for MQTT message: source, target, type, data, cind |
| `BaseDriver` | `drivers/base.py` | Driver interface: `connect`, `disconnect`, `execute`, `read` |
| `TelemetryMixin` | `core/mixins.py` | Periodic telemetry publishing on a schedule (`freq`) |
| `TaskManagerMixin` | `core/mixins.py` | Creation, tracking, and cancellation of `asyncio.Task` |
| `Config` | `config.py` | Configuration from JSON file and `Kamio_*` environment variables |
| `HADiscovery` | `discovery.py` | Lazy-init: created only when `enable_ha_discovery()` is called |
| `HooksManager` | `core/hooks.py` | Lifecycle hooks: app, device, rule events. Sync/async, priorities |
| `EventBus` | `core/event_bus.py` | Public pub/sub: system + custom events, filter_fn, priority |
| `PluginContext` | `plugins/loader.py` | Scoped plugin context: tracks all subscriptions/hooks for clean unload |
| `Plugin` | `plugins/base.py` | ABC for plugins: on_load/unload, subscribe_events, register_hooks, dependencies |
| `PluginLoader` | `plugins/loader.py` | Loading by class/module/directory, dependency resolution |
| `HotReloadManager` | `core/hot_reload.py` | asyncio polling + debounce; reload rules/devices/config at runtime |
| `CustomNode` | `core/custom_nodes.py` | ABC for custom MQTT nodes: start/stop/handle_message, publish/subscribe helpers |
| `CustomNodeManager` | `core/custom_nodes.py` | Registration, startup, shutdown, and message routing to nodes |
| `MqttConnection` | `core/mqtt_connection.py` | gmqtt client with automatic reconnect (exponential backoff) |

## Application Lifecycle

```
KamioApp.__init__
  └── StateManager, CommandManager, RuleEngine, DeviceRegistry, HooksManager, EventBus
  └── HADiscovery = None  (lazy — created only in enable_ha_discovery())
  └── MqttConnection is created if mqtt_broker is a URI string
      otherwise a ready-made gmqtt.Client is used
  └── Callbacks: on_message → _on_mqtt_message
                 on_connect → _on_mqtt_connect → event_bus("mqtt_connected")
                 on_disconnect → _on_mqtt_disconnect → event_bus("mqtt_disconnected")

app.add_device("id", DeviceClass)  ─── or create_device("id", "type")
  └── DeviceClass is auto-registered in DeviceRegistry
  └── Device.__init__ → _apply_defaults() → defaults from Kamio_FIELDS
  └── device._app = self  (KamioApp)
  └── Device.on_init() → driver.connect()
  └── DeviceNode created, bound to Device
  └── DeviceHandler created:
        ├── injects device._on_state_changed  → app.event_bus.publish(...)
        └── injects device._on_rules_trigger  → app.rules.handle_device_update(...)
  └── DeviceRegistry.register_instance()
  └── hooks.trigger('on_device_added', device)
  └── event_bus.publish('device_added', {...})
  └── [if HA enabled] ha_discovery.announce(device)

app.start()
  └── hooks.trigger('on_before_start')
  └── mqtt_conn.connect()
  └── ServerNode.start() → subscribe("Kamio/v1/0/#", "Kamio/v1/all/#")
  └── DeviceNode.start() → subscribe("Kamio/v1/{id}/#", "Kamio/v1/all/#")
                        → Device.on_start() → start_telemetry()
  └── RuleEngine.start() → interval rules → asyncio.Task
  └── CustomNodeManager.start_all()
  └── hooks.trigger('on_after_start')
  └── event_bus.publish('app_start')

Incoming MQTT message (_on_mqtt_message — called from MQTT thread)
  └── custom_nodes.route_message()           [first — via asyncio threadsafe]
  └── event_bus("mqtt_message_received")     [threadsafe]
  └── server_node.dispatch()
  └── topics.parse(topic) → device_id
      ├── device_id found → device_nodes[device_id].dispatch()
      └── device_id == "all" → dispatch to all nodes  [broadcast only]

DeviceNode.dispatch → Envelope.parse → DeviceHandler(envelope)
  ├── DEVICE_STATE   → Device.handle_state()
  │     └── validate → driver.execute (if present) → setattr
  │         └── _on_state_changed(id, field, old, new)  [callback from DeviceHandler]
  │         └── _on_rules_trigger(id, changes)          [callback from DeviceHandler]
  ├── SERVER_COMMAND → Device.handle_command() → COMMAND_ACK
  ├── DEVICE_CONFIG  → Device.handle_config() → COMMAND_ACK
  ├── DEVICE_EVENT   → Device.handle_event()
  └── DEVICE_TELEMETRY → StateManager.update_state + rules.handle_device_update

app.stop()
  └── hooks.trigger('on_before_stop')
  └── RuleEngine.stop()          [timeout 5s]
  └── CustomNodeManager.stop_all()[timeout 5s]
  └── DeviceNode.stop() × N      [timeout 5s, gather]
        └── Device.on_stop() → driver.disconnect() → cancel_all_tasks()
        └── hooks.trigger('on_device_stopped')
  └── ServerNode.stop()          [timeout 5s]
  └── mqtt_conn.disconnect()
  └── hooks.trigger('on_after_stop')
  └── event_bus.publish('app_stop')
```

## MQTT Topics

| Topic | Direction | Envelope type |
|---|---|---|
| `Kamio/v1/{id}/dt` | device → server | `DEVICE_TELEMETRY` |
| `Kamio/v1/{id}/ds` | bidirectional | `DEVICE_STATE` |
| `Kamio/v1/{id}/sa` | server → device | `STATE_ACK` |
| `Kamio/v1/{id}/sc` | server → device | `SERVER_COMMAND` |
| `Kamio/v1/{id}/ca` | device → server | `COMMAND_ACK` |
| `Kamio/v1/{id}/de` | device → server | `DEVICE_EVENT` |
| `Kamio/v1/{id}/conf` | server → device | `DEVICE_CONFIG` |
| `Kamio/v1/{id}/k` | device → server | `KEEPALIVE` |
| `Kamio/v1/all/#` | server → all | broadcast |

Topic format: `Kamio/v1/{id}/{type}`. `topics.parse()` parses only the current format.

## Device Fields

```python
state(default, writable, min, max, choices)  # readable and controllable state
telemetry(unit, freq)                        # periodically sent data (read-only)
config(default)                              # configuration parameters (always writable)
event(description)                           # events (emit-only, not stored)
```

`DeviceMeta` collects all `Field`-typed annotations at class creation into:
- `Kamio_FIELDS` — state + telemetry + config (key → `Field`)
- `Kamio_EVENTS` — event (key → `Field`)
- `Kamio_COMMANDS` — methods with `@command` (name → callable)

## Decoupling Device from KamioApp

`Device.handle_state()` uses injectable callbacks instead of directly calling
`self.app.event_bus` and `self.app.rules`, avoiding tight coupling:

```python
# Injected by DeviceHandler at creation:
device._on_state_changed = async (device_id, field, old, new) → event_bus.publish(...)
device._on_rules_trigger = async (device_id, changes) → rules.handle_device_update(...)
```

This allows testing `Device` without `KamioApp`.

## Automation Rules

```python
# On device event + field filter (app-level)
@app.rule(device=Thermostat, fields=["temperature"])
async def on_temp(event: RuleEvent, app: KamioApp): ...

# By interval (with immediate execution)
@app.rule(interval=60.0, run_on_start=True)
async def periodic(event: RuleEvent, app: KamioApp): ...

# Device-level rules (inside device class)
class SmartLight(Device):
    power: bool = state(default=False, writable=True)

    @rule(fields=["power"])
    async def on_power_change(self, event: RuleEvent, app: KamioApp):
        # Automatically registered when the device class is registered
        pass

# Explicit registration (without decorator)
app.add_rule(func, device=Sensor, fields=["motion"])
```

`RuleEngine` — the `_event_rules_by_type` index is always up to date (updated in `add_rule`/`remove_rule`).
An error in one rule is logged, the rest continue running (`gather(return_exceptions=True)`).

Device-level rules (`@rule` decorator inside a device class) are automatically added to `RuleEngine` when the device class is registered via `app.register()` or `@app.device`.

## Plugins

```python
from kamio.plugins.base import Plugin

class MyPlugin(Plugin):
    name = "my_plugin"

    async def on_load(self, app, context):
        context.subscribe("device_state_changed", self._on_state)

    async def on_unload(self, app):
        pass  # cleanup is automatic via PluginContext
```

`PluginContext` tracks all plugin subscriptions/hooks → clean unload without leaks.

## Drivers

Implemented drivers: `MockHardwareDriver`, `GPIOChipDriver`, `SerialDriver`, `TelnetDriver`, `HTTPDeviceDriver`, `UDPDriver`, `ModbusTCPDriver`.

`BaseDriver` defines a contract of 4 methods:

```python
async def connect()                                  # connect to hardware
async def disconnect()                               # disconnect
async def execute(command_name, params)              # execute a command
async def read(field_name, params=None)              # read a value
```

If a device has a `driver`, `handle_command` and `handle_state` first try to execute through it.
The driver receives `command_name` and `params` (including `value`), allowing a unified `set_<field>` protocol or passing a command via `params["command"]`.
`NotImplementedError` in `execute` — fallback to the device method.

## Asynchrony

- All I/O is `async/await`, no blocking calls in the event loop
- MQTT callbacks run inside the gmqtt asyncio event loop; no additional thread safety is needed
- Telemetry — separate `asyncio.Task` per group of fields with the same `freq`
- Interval rules — separate `asyncio.Task` in `RuleEngine`
- Shutdown — `asyncio.wait_for(..., timeout=5.0)` for each stage
- `TaskManagerMixin.cancel_all_tasks()` — safe cancellation of all device tasks

## Roadmap

| Version | Feature | Status |
|---|---|---|
| v1.0.0b1 | Architectural refactoring: decoupling, new package structure | ✅ Done |
| v1.0.0b2 | Thread-safety, race condition fixes, memory management | ✅ Done |
| v1.0.0b2 | Bugfix: PluginLoader circular dependency detection | ✅ Done |
| v1.0.0b2 | Test coverage 94% (751 unit + 16 stress tests) | ✅ Done |

## Notable Changes

### Bug Fixes
- **PluginLoader** — circular dependency detection fixed: instance-level `_loading` set replaces the local `_visiting`, which was reset on recursive `load_plugin` calls.

### Test Coverage
- 751 unit tests cover all public components: drivers (GPIO, Serial, Modbus, Telnet, UDP, Mock, HTTP), core (envelope, state, correlation, mqtt_nodes, handlers, hot_reload, mqtt_connection), plugin loader (edge cases, circular deps, missing deps, unload cleanup), lifecycle (start/stop/run), device (handle_state/command/config/event), custom nodes, rules engine, config gotchas, architecture coverage.
- CI gate: `--cov-fail-under=90`, current coverage 94%.
