# API Documentation Kamio Core v1.0.0b3

This documentation describes all public classes and functions of the Kamio Core v1.0.0b3 framework.

## Table of Contents

- [KamioApp](#KamioApp)
- [HooksManager](#hooksmanager)
- [EventBus](#eventbus)
- [Plugin / PluginLoader](#plugin--pluginloader)
- [HotReloadManager](#hotreloadmanager)
- [CustomNode / CustomNodeManager](#customnode--customnodemanager)
- [Device](#device)
- [Field Definition Functions](#field-definition-functions)
- [command Decorator](#command-decorator)
- [Config](#config)
- [HADiscovery](#hadiscovery)
- [Drivers](#drivers-kamiodrivers)
- [Internal Components](#internal-components-kamiocore)
- [Namespace Packages](#namespace-packages)

## `KamioApp`

The main application class, orchestrating devices, rules, and MQTT communication.
Located in `Kamio.app` (package `Kamio/app/`, class in `_application.py`).

Assembled from 8 mixins: `LifecycleMixin`, `MqttDispatchMixin`, `DeviceRegistryMixin`,
`RuleRegistryMixin`, `PluginFacadeMixin`, `HotReloadFacadeMixin`, `CustomNodeFacadeMixin`, `HookEventFacadeMixin`.

### Initialization

```python
class KamioApp:
    def __init__(
        self,
        mqtt_broker: Union[str, gmqtt.Client, None] = None,
        client_id: Optional[str] = None,
        keepalive: int = 60,
        clean_session: bool = True,
        protocol: int = 5,
        log_level: Optional[int] = None,
        config_path: Optional[str] = None,
        **kwargs
    ):
        """
        Kamio application initialization.

        Parameters:
            mqtt_broker:   Broker URI ('mqtt://host:port') or a ready-made gmqtt.Client.
                           If not specified — taken from Config.
            client_id:     MQTT client ID. Auto-generated if not set.
            keepalive:     Keep-alive interval in seconds (default 60).
            clean_session: Clear session on connect (default True).
            protocol:      MQTT protocol version (default MQTTv5).
            log_level:     Python logging level. None = from Config or unchanged.
            config_path:   Path to JSON configuration file.
            **kwargs:      Additional parameters for MqttConnection
                           (transport, tls, reconnect_min_delay, reconnect_max_delay).
                           Unknown kwargs raise TypeError.

        Note:
            HADiscovery is NOT created during initialization. Call enable_ha_discovery()
            to activate Home Assistant integration.

        Note:
            Unknown kwargs raise TypeError instead of being silently ignored.
            shutdown_timeout (float, default 5.0) — configurable timeout for graceful shutdown.
        """
    ```

### Properties

```python
@property
def logger(self) -> logging.Logger:
    """Returns the application logger."""

@property
def is_running(self) -> bool:
    """Returns True if the application is running."""

@property
def devices(self) -> Dict[str, Device]:
    """Returns a snapshot dict of all active device instances."""

@property
def registered_types(self) -> List[str]:
    """Returns a list of registered device types."""
```

### Methods

```python
def device(self, cls: Optional[Type[Device]] = None) -> Callable[[Type[Device]], Type[Device]]:
    """
    Decorator for registering device classes.
    
    Usage:
        @app.device
        class MyDevice(Device): ...
        
        OR
        
        @app.device()
        class MyDevice(Device): ...
    """

def rule(self, device: Optional[Type[Device]] = None, *, interval: Optional[float] = None,
         fields: Optional[List[str]] = None, enabled: bool = True, run_on_start: bool = False,
         description: Optional[str] = None) -> Callable:
    """
    Decorator for registering automation rules.
    
    Parameters:
        device: Device class to track for changes
        interval: Execution interval in seconds (for periodic rules)
        fields: List of fields to track for changes
        enabled: Whether the rule is enabled
        run_on_start: Run interval-rule immediately on engine start
        description: Rule description
    
    Usage:
        @app.rule(device=MyDevice, fields=["temperature"])
        async def on_temp_change(snapshot, app): ...
        
        @app.rule(interval=60.0)
        async def periodic_task(snapshot, app): ...
    """

def add_rule(self, func: Callable[[RuleEvent, KamioApp], Any], device: Optional[Type[Device]] = None,
             *, interval: Optional[float] = None, fields: Optional[List[str]] = None,
             enabled: bool = True, run_on_start: bool = False, description: Optional[str] = None) -> Callable:
    """
    Explicit registration of a rule function.
    
    Alternative to the @app.rule decorator for dynamic rule registration.
    
    Parameters:
        func: Rule function to register
        device: Device class to track for changes
        interval: Execution interval in seconds (for periodic rules)
        fields: List of fields to track for changes
        enabled: Whether the rule is enabled
        run_on_start: Run interval-rule immediately on engine start
        description: Rule description

    Returns:
        The registered function
    
    Example:
        async def on_motion(snapshot, app): ...
        app.add_rule(on_motion, device=MotionSensor, fields=["motion"])
    """

def register(self, device_class: Type[Device]):
    """
    Registers a device class without using a decorator.
    
    Parameters:
        device_class: Device class to register
    """

async def create_device(self, device_id: str, device_type: str, **kwargs) -> Device:
    """
    Creates and starts a device instance.
    
    Parameters:
        device_id: Unique device identifier
        device_type: Device type (name of the registered class)
        **kwargs: Additional parameters for the device constructor
    
    Returns:
        The created device instance
    
    Example:
        device = await app.create_device("my_sensor", "thermostat", driver=my_driver)
    """

async def add_device(self, device_id: str, device_class: Type[Device], **kwargs) -> Device:
    """
    Simplified device creation method with automatic class registration.
    
    Recommended way to create devices.
    Automatically registers the device class if it is not yet registered.
    
    Parameters:
        device_id: Unique device identifier
        device_class: Device class (automatically registered if needed)
        **kwargs: Additional parameters for the device constructor
    
    Returns:
        The created device instance
    
    Example:
        device = await app.add_device("my_sensor", Thermostat, driver=my_driver)
    """

def run(self):
    """
    Blocking method to start the application.
    Recommended way to run in production.
    Handles SIGINT and SIGTERM signals for graceful shutdown.
    """

async def start(self):
    """
    Asynchronous application start.
    Connects to the MQTT broker and starts all device nodes.
    """

async def stop(self):
    """
    Asynchronous application stop.
    Gracefully stops all devices and disconnects from MQTT.
    """

async def remove_device(self, device_id: str) -> None:
    """
    Stops and removes a device from the registry.

    Calls the 'on_device_removed' hook before removal.
    Safe if device is not found (logs a warning).

    Parameters:
        device_id: Device identifier to remove
    """

async def remove_rule(self, func: Callable) -> None:
    """
    Removes a registered rule by function.

    Cancels the interval-rule background task (if any).
    Calls the 'on_rule_removed' hook before removal.
    Safe if rule is not found (logs a warning).

    Parameters:
        func: Rule function passed to @app.rule or app.add_rule
    """

def register_hook(self, event_type: str, hook: Callable, priority: int = 0) -> None:
    """
    Registers a lifecycle hook.

    Convenient alias for app.hooks.register().

    Parameters:
        event_type: Event name ('on_before_start', 'on_device_added', etc.)
        hook: Sync or async callable
        priority: Execution priority (higher = earlier, default 0)
    """

def unregister_hook(self, event_type: str, hook: Callable) -> None:
    """
    Removes a previously registered hook.

    Parameters:
        event_type: Event name
        hook: Hook function to remove
    """
```

## `HooksManager`

Manages application, device, and rule lifecycle hooks.
Accessible via `app.hooks`.

### Initialization

```python
class HooksManager:
    def __init__(self): ...
```

### Methods

```python
def register(self, event_type: str, hook: Callable, priority: int = 0) -> None:
    """
    Registers a hook for an event.

    Parameters:
        event_type: Event name
        hook: Sync or async callable. Called with arguments passed to trigger()
        priority: Hooks with higher values execute first (default 0)
    """

def unregister(self, event_type: str, hook: Callable) -> None:
    """Removes a hook for an event."""

def list_hooks(self, event_type: str) -> List[Callable]:
    """Returns a list of registered hooks in priority order."""

def clear(self, event_type: str = None) -> None:
    """
    Clears hooks.
    If event_type is not specified — clears all events.
    """

async def trigger(self, event_type: str, *args, **kwargs) -> None:
    """
    Calls all hooks for an event in priority order.

    Supports sync and async hooks.
    Errors in hooks are logged and do not interrupt execution of the rest.
    """
```

### Application Events

| Event | When called | Arguments |
|---|---|---|
| `on_before_start` | Before connecting to MQTT | — |
| `on_after_start` | After successful start | — |
| `on_before_stop` | Before stopping begins | — |
| `on_after_stop` | After full stop | — |

### Device Events

| Event | When called | Arguments |
|---|---|---|
| `on_device_added` | After device creation | `device: Device` |
| `on_device_removed` | Before device removal | `device: Device` |
| `on_device_started` | After `DeviceNode` starts | `device: Device` |
| `on_device_stopped` | After `DeviceNode` stops | `device: Device` |

### Rule Events

| Event | When called | Arguments |
|---|---|---|
| `on_rule_added` | When a rule is registered via `@app.rule` | `rule: Rule` |
| `on_rule_removed` | Before a rule is removed via `remove_rule` | `rule: Rule` |
| `on_rule_triggered` | After successful rule execution | `rule: Rule, snapshot: dict` |
| `on_rule_failed` | After a rule error | `rule: Rule, error: Exception` |

### Example

```python
async def on_new_device(device):
    print(f"New device: {device.device_type()}")

app.register_hook('on_device_added', on_new_device)
app.register_hook('on_rule_failed', lambda rule, err: logger.error(f"{getattr(rule, 'func', rule).__name__}: {err}"))
```

## `EventBus`

Public event bus for custom pub/sub logic. Accessible via `app.event_bus`.

> **Difference from `HooksManager`:** `HooksManager` — internal lifecycle interceptors. `EventBus` — public API for subscribing to system and custom events.

### Methods

```python
def subscribe(
    self,
    event_type: str,
    callback: Callable,
    filter_fn: Optional[Callable[[dict], bool]] = None,
    priority: int = 0,
) -> None:
    """
    Subscribe to an event.

    Parameters:
        event_type: Event name
        callback: Sync or async callable, receives a data dict
        filter_fn: Optional predicate (data) -> bool. callback is skipped when False
        priority: Higher = earlier (default 0)
    """

def unsubscribe(self, event_type: str, callback: Callable) -> None:
    """Remove subscription by callback identity."""

def list_subscribers(self, event_type: str) -> List[Callable]:
    """List of callbacks in priority order."""

def event_types(self) -> List[str]:
    """List of event types that have subscribers."""

def clear(self, event_type: str = None) -> None:
    """Clear subscribers. Without argument — all events."""

async def publish(self, event_type: str, data: dict) -> None:
    """
    Publish an event.

    Automatically adds 'timestamp' to data (if not present).
    Checks filter_fn before calling callback.
    Errors are logged, remaining callbacks continue execution.
    """
```

### Application Events

| Event | When | data fields |
|---|---|---|
| `app_start` | After start | `timestamp` |
| `app_stop` | After stop | `timestamp` |
| `mqtt_connected` | MQTT connection | `broker, port, rc, timestamp` |
| `mqtt_disconnected` | MQTT disconnection | `rc, timestamp` |
| `mqtt_message_received` | Incoming message | `topic, payload, qos, timestamp` |
| `device_added` | Device creation | `device_id, device_type, device, timestamp` |
| `device_removed` | Device removal | `device_id, device_type, timestamp` |
| `device_state_changed` | State change | `device_id, field, old_value, new_value, timestamp` |
| `device_command_executed` | Command execution | `device_id, command, params, result, timestamp` |
| `rule_added` | Rule registration | `rule, timestamp` |
| `rule_removed` | Rule removal | `rule, timestamp` |
| `rule_triggered` | Successful execution | `rule, snapshot, timestamp` |
| `rule_failed` | Rule error | `rule, error, timestamp` |

### `KamioApp` Methods

```python
def subscribe_event(self, event_type: str, callback: Callable, filter_fn=None, priority: int = 0) -> None:
    """Alias for app.event_bus.subscribe()."""

def unsubscribe_event(self, event_type: str, callback: Callable) -> None:
    """Alias for app.event_bus.unsubscribe()."""

async def publish_event(self, event_type: str, data: dict) -> None:
    """Publish a custom event."""
```

### Example

```python
# Subscribe with filter
app.subscribe_event(
    "device_state_changed",
    lambda d: print(f"{d['device_id']}.{d['field']} = {d['new_value']}"),
    filter_fn=lambda d: d.get("field") == "temperature",
)

# Custom event
await app.publish_event("sensor_alert", {"level": "critical", "sensor": "co2"})
```

## `Plugin` / `PluginLoader`

Plugin system for extending the framework without modifying the core.

### `Plugin` (ABC)

```python
from typing import Any, Optional
from kamio.plugins.loader import PluginContext

class Plugin(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...         # Unique plugin name

    @property
    @abstractmethod
    def version(self) -> str: ...      # Version string

    @property
    def description(self) -> str: ...  # Optional description

    @property
    def dependencies(self) -> List[str]: ...  # Names of prerequisite plugins

    def configure(self, config: Dict[str, Any]) -> None: ...
    async def on_load(self, app: KamioApp, context: Optional[PluginContext] = None) -> None: ...
    async def on_unload(self, app: KamioApp) -> None: ...
    def subscribe_events(self, event_bus: Any) -> None: ...
    def register_hooks(self, hooks: Any) -> None: ...
```

`PluginContext` (from `Kamio.plugins.loader`) is used for scoped registration of subscriptions and hooks.

### `PluginLoader`

```python
async def load_plugin(self, plugin_class: Type[Plugin], config: dict = None) -> Plugin:
    """
    Load a plugin by class.
    Calls configure() → on_load() → subscribe_events() → register_hooks().
    Automatically loads missing dependencies (transitively).
    Raises TypeError if the class does not inherit Plugin.
    Raises ValueError if the plugin is already loaded, a dependency is not found,
    or a circular dependency is detected.
    """

async def unload_plugin(self, plugin_name: str) -> None:
    """
    Calls on_unload() and removes from the registry.
    Raises ValueError if another loaded plugin depends on this one.
    Logs a warning and returns if the plugin is not found.
    Cleanup of subscriptions/hooks/rules is performed in try-finally,
    so resources do not leak even on error in on_unload().
    """

async def load_from_module(self, module_name: str, config: dict = None) -> Plugin:
    """Load a plugin from a Python module by its dotted path."""

async def load_plugins_from_directory(self, directory: str) -> List[Plugin]:
    """Load all plugins from a directory (*.py, not __*). Errors are logged and skipped."""

def register_class(self, name: str, plugin_class: Type[Plugin]) -> None:
    """
    Registers a plugin class by name so dependencies can
    be auto-loaded by name. Used by _ensure_dependencies.
    """

def get_plugin(self, plugin_name: str) -> Optional[Plugin]: ...
def list_plugins(self) -> List[str]: ...

async def unload_all(self) -> None:
    """Unloads all plugins in reverse load order."""

@property
def load_order(self) -> List[str]:
    """List of plugin names in their load order."""
```

#### Circular Dependency Detection

`PluginLoader` uses an instance-level `_loading` set to detect cycles
between recursive `load_plugin` calls. If plugin A depends on B, and B on A,
it raises `ValueError("Circular plugin dependency detected involving 'A'")`.

### plugin_loaded / plugin_unloaded Events

Published to `EventBus` automatically:

| Event | Fields |
|---|---|
| `plugin_loaded` | `plugin_name, plugin_version, timestamp` |
| `plugin_unloaded` | `plugin_name, timestamp` |

### `KamioApp` Methods

```python
await app.load_plugin(PluginClass, config={...})
await app.unload_plugin("plugin_name")
await app.load_from_module("my_module.MyPlugin", config={...})
await app.load_plugins_from_directory("/path/to/plugins")
app.get_plugin("plugin_name")   # -> Plugin | None
app.list_plugins()              # -> List[str]
```

### Built-in Plugins

| Class | Module | Description |
|---|---|---|
| `LoggingPlugin` | `Kamio.plugins.builtin.logging_plugin` | Events → rotating log file |
| `MetricsPlugin` | `Kamio.plugins.builtin.metrics_plugin` | In-memory event counters |

### Example

```python
from kamio.plugins.builtin import MetricsPlugin, LoggingPlugin

await app.load_plugin(LoggingPlugin, config={"file": "app.log", "level": "INFO"})
await app.load_plugin(MetricsPlugin)

metrics = app.get_plugin("metrics")
print(metrics.get_metrics())
```

## `HotReloadManager`

Hot reloading of rules, devices, and configuration without stopping the application.
Accessible via `app.hot_reload`.

### Methods

```python
def watch_file(self, path: str, handler: Callable) -> None:
    """Watch a file. handler(file_path) is called on change."""

def watch_directory(self, directory: str, pattern: str, handler: Callable) -> None:
    """Watch a directory by pattern (e.g. '*.py')."""

def enable(self) -> None:
    """Start asyncio polling loop (internal method)."""

def disable(self) -> None:
    """Stop polling loop (internal method)."""

def list_watched(self) -> List[str]:
    """List of watched paths."""

@property
def is_enabled(self) -> bool:
    """Returns True if HotReloadManager is active."""

# Ready-made handler factories:
def make_rules_handler(self) -> Callable: ...
def make_devices_handler(self) -> Callable: ...
def make_config_handler(self) -> Callable: ...
```

**Note:** To enable/disable hot reload, use the `KamioApp` facade methods:
- `app.enable_hot_reload()` - start polling
- `app.disable_hot_reload()` - stop polling

### Standalone Functions

```python
from kamio.core.hot_reload import (
    reload_rules_from_file,    # (file_path, app) -> bool
    reload_devices_from_file,  # (file_path, app) -> bool
    reload_config_from_file,   # (file_path, app) -> bool
)
```

### EventBus Events

| Event | Fields |
|---|---|
| `hot_reload_rules` | `file_path, replaced, timestamp` |
| `hot_reload_devices` | `file_path, updated_classes, timestamp` |
| `hot_reload_config` | `file_path, config, timestamp` |
| `hot_reload_error` | `file_path, error, timestamp` |

### `KamioApp` Methods

```python
app.enable_hot_reload()                            # enable polling
app.disable_hot_reload()                           # stop polling
app.watch_file(path, handler)                      # watch a file
app.watch_directory(directory, pattern, handler)   # watch a directory
```

### Example

```python
# Hot reload rules from a directory
app.watch_directory("rules/", "*.py", app.hot_reload.make_rules_handler())
app.enable_hot_reload()

# Hot reload config
app.watch_file("config.json", app.hot_reload.make_config_handler())
```

## `CustomNode` / `CustomNodeManager`

Extensible MQTT node system for specific protocols and custom logic.

### `CustomNode` (ABC)

```python
class CustomNode(ABC):
    def __init__(self, mqtt_client, topic_prefix: str): ...

    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def stop(self) -> None: ...

    @abstractmethod
    async def handle_message(self, topic: str, payload: bytes) -> None: ...

    async def on_connect(self) -> None: ...    # optional
    async def on_disconnect(self) -> None: ... # optional

    def subscribe(self, topic: str, qos: int = 0) -> None:
        """Subscribe to a topic relative to topic_prefix."""

    def subscribe_absolute(self, topic: str, qos: int = 0) -> None:
        """Subscribe to an absolute topic."""

    def publish(self, topic: str, payload, qos: int = 0, retain: bool = False) -> None:
        """Publish relative to topic_prefix."""

    def publish_absolute(self, topic: str, payload, qos: int = 0, retain: bool = False) -> None:
        """Publish to an absolute topic."""

    async def publish_async(self, topic: str, payload, qos: int = 0, retain: bool = False) -> None:
        """Asynchronous (non-blocking) publish relative to topic_prefix."""

    def matches(self, topic: str) -> bool:
        """Returns True if topic starts with topic_prefix."""
```

### `CustomNodeManager`

```python
def register_node(self, name: str, node: CustomNode) -> None:
    """Registration. ValueError if name is already taken."""

def unregister_node(self, name: str) -> None:
    """Safe removal."""

async def start_all(self) -> None:
    """Start all nodes. An error in one node does not stop others."""

async def stop_all(self) -> None:
    """Stop in reverse order."""

async def route_message(self, topic: str, payload: bytes) -> bool:
    """Route a message. True if at least one node handled it."""

def get_node(self, name: str) -> Optional[CustomNode]: ...
def list_nodes(self) -> List[str]: ...
```

### EventBus Events

| Event | Fields |
|---|---|
| `custom_node_started` | `node_name, topic_prefix, timestamp` |
| `custom_node_stopped` | `node_name, timestamp` |
| `custom_node_error` | `node_name, error, phase, timestamp` |

### `KamioApp` Methods

```python
app.register_custom_node(name, node)  # register
app.unregister_custom_node(name)      # remove
app.get_custom_node(name)             # -> CustomNode | None
app.list_custom_nodes()               # -> List[str]
```

### Example

```python
from kamio.core.custom_nodes import CustomNode

class MySensorBridge(CustomNode):
    async def start(self):
        self.subscribe("#")  # subscribe to <prefix>/#

    async def stop(self):
        pass

    async def handle_message(self, topic, payload):
        print(f"{topic}: {payload.decode()}")
        self.publish("ack", b"ok")

app.register_custom_node("sensors", MySensorBridge(app.mqtt_client, "sensors"))
```

## `Device`

Base class for all Kamio devices. Supports declarative field definitions (telemetry, state) and commands.

### Class Variables

```python
Kamio_FIELDS: ClassVar[Dict[str, Field]]
    """Dictionary of all device fields (telemetry, state, config)."""

Kamio_COMMANDS: ClassVar[Dict[str, Any]]
    """Dictionary of all device commands (methods with the @command decorator)."""

Kamio_EVENTS: ClassVar[Dict[str, Field]]
    """Dictionary of all device events."""

Kamio_RULES: ClassVar[Dict[str, Any]]
    """Dictionary of automatic device rules created by the @rule decorator."""
```

### Initialization

```python
def __init__(self, driver: Optional[BaseDriver] = None, keepalive_interval: float = 30.0, **kwargs):
    """
    Device initialization.
    
    Parameters:
        driver: Driver instance for hardware interaction
        keepalive_interval: Interval for sending keepalive messages in seconds (0 — disable)
        **kwargs: Additional parameters
    """
```

### Properties

```python
@property
def app(self) -> KamioApp:
    """Returns the KamioApp instance the device is attached to."""
```

### Attributes

```python
node: Optional[DeviceNode]
    """Device node for MQTT communication. Set by KamioApp on registration."""

driver: Optional[BaseDriver]
    """Device driver for hardware interaction."""
```

### Class Methods

```python
@classmethod
def device_type(cls) -> str:
    """
    Returns the string representation of the device type (class name in lowercase).
    
    Example:
        MyDevice.device_type() -> "mydevice"
    """

@classmethod
def get_schema(cls) -> Dict[str, Any]:
    """
    Returns the device schema with descriptions of all fields, commands, and events.
    
    Returns:
        Dictionary with device metadata
    """

@classmethod
def get_fields(cls, kind: str | None = None, writable: bool | None = None) -> Dict[str, Field]:
    """
    Returns device fields with filtering.
    
    Parameters:
        kind: Field type ("telemetry", "state", "config")
        writable: Filter by writability
    
    Returns:
        Dictionary of fields
    """

@classmethod
def get_telemetry(cls) -> Dict[str, Field]:
    """Returns all telemetry fields."""

@classmethod
def get_states(cls, writable: bool | None = None) -> Dict[str, Field]:
    """Returns all state fields."""

@classmethod
def get_commands(cls) -> Dict[str, Any]:
    """Returns all device commands."""
```

### Lifecycle Methods

```python
async def on_init(self, **kwargs):
    """
    Asynchronous initialization hook, called on device creation.
    Suitable for connecting the driver and initial setup.
    """

async def on_start(self, node: DeviceNode):
    """
    Hook called when the device node starts.
    Suitable for starting background tasks and beginning telemetry publishing.
    """

async def on_stop(self, node: DeviceNode):
    """
    Hook called when the device node stops.
    Suitable for graceful task termination and driver disconnection.
    """

async def shutdown(self):
    """
    Full device shutdown.
    Disconnects the driver and cancels all background tasks.
    """

async def reinitialize(self):
    """
    Device reinitialization (stop and restart).
    """
```

### State Methods

```python
def get_state_snapshot(self) -> Dict[str, Any]:
    """Returns a snapshot of all state fields."""

def get_config_snapshot(self) -> Dict[str, Any]:
    """Returns a snapshot of all config fields."""

def get_telemetry_snapshot(self) -> Dict[str, Any]:
    """Returns a snapshot of all telemetry fields."""

def get_full_snapshot(self) -> Dict[str, Any]:
    """Returns a full snapshot of all device fields."""

async def request_state_sync(self):
    """
    Requests synchronization of the current device state with the broker.
    Publishes a DEVICE_STATE message with current state field values.
    """

async def request_full_sync(self):
    """
    Requests full synchronization of all device fields.
    """
```

### Event Methods

```python
async def emit(self, event_name: str, payload: dict):
    """
    Publishes an event from the device.
    
    Parameters:
        event_name: Event name
        payload: Event data
    """

async def handle_event(self, event_name: str, payload: dict):
    """
    Handler for incoming events. Overridden in subclasses.
    """
```

### Command and State Handling Methods

```python
async def handle_state(self, data: dict) -> Dict[str, Any]:
    """
    Handles incoming state changes.
    Validates and applies changes to state fields.
    
    Parameters:
        data: Dictionary with state changes
    
    Returns:
        Dictionary of applied changes
    """

async def handle_config(self, data: dict) -> Dict[str, Any]:
    """
    Handles incoming configuration changes.
    
    Parameters:
        data: Dictionary with configuration changes
    
    Returns:
        Dictionary of applied changes
    """

async def handle_command(self, method_name: str, params: dict) -> Any:
    """
    Handles an incoming command.
    First tries to execute via the driver, then via internal methods.
    
    Parameters:
        method_name: Command name
        params: Command parameters
    
    Returns:
        Command execution result
    """
```

### Task Management Methods (from TaskManagerMixin)

```python
def create_task(self, coro, name: str = None):
    """
    Creates a background task that will be automatically cancelled on shutdown.
    
    Parameters:
        coro: Coroutine to execute
        name: Task name for logging
    """

async def cancel_all_tasks(self):
    """Cancels all background tasks of the device."""
```

### Telemetry, Publishing, and Async Callback Methods

```python
enable_telemetry: bool = True
    """Flag for automatic telemetry publishing. Can be overridden in a subclass."""

async def send_command(self, target_device_id: str, method: str, params: dict, timeout: float = 10.0) -> None:
    """Sends a command to another device via MQTT."""

async def publish_telemetry(self, data: dict) -> None:
    """Publishes a telemetry packet."""

async def start_telemetry(self) -> None:
    """Starts telemetry publishing loops for fields with freq."""

async def read_telemetry_value(self, field_name: str) -> Any:
    """Reads a telemetry field value from the driver."""

async def handle_telemetry_update(self, field_names: list[str]) -> Optional[dict[str, Any]]:
    """Collects current values of the specified telemetry fields."""

def register_async_callback(self, topic: str, callback) -> None:
    """Registers an async callback for an arbitrary MQTT topic."""

def unregister_async_callback(self, topic: str) -> None:
    """Removes a previously registered async callback."""
```

## Field Definition Functions

These functions are used for declarative field definitions in `Device` classes.

### `telemetry`

Defines a telemetry field — data that the device periodically sends.

```python
def telemetry(
    default: Any = None,
    *,
    unit: str = "",
    freq: str = "",
    description: str = "",
    min: float | None = None,
    max: float | None = None,
    required: bool = False,
    **metadata: Any,
) -> Any:
    """
    Defines a telemetry field (data sent by the device).
    
    Parameters:
        default: Default value
        unit: Unit of measurement (e.g., "°C", "%", "V")
        freq: Publishing frequency (e.g., "5s", "1m", "100ms")
        description: Field description
        min: Minimum value for validation
        max: Maximum value for validation
        required: Required field
        **metadata: Additional metadata
    
    Example:
        temperature: float = telemetry(unit="°C", freq="5s", description="Temperature")
    """
```

### `state`

Defines a state field — data that can be read and modified externally.

```python
def state(
    default: Any = None,
    *,
    writable: bool = True,
    description: str = "",
    min: float | None = None,
    max: float | None = None,
    choices: tuple | None = None,
    required: bool = False,
    **metadata: Any,
) -> Any:
    """
    Defines a state field (data that can be read and modified).
    
    Parameters:
        default: Default value
        writable: Whether it can be modified externally
        description: Field description
        min: Minimum value for validation
        max: Maximum value for validation
        choices: List of allowed values
        required: Required field
        **metadata: Additional metadata
    
    Example:
        power: bool = state(default=False, writable=True, description="Power state")
        mode: str = state(default="auto", choices=("auto", "manual", "off"))
    """
```

### `event`

Defines an event field — one-time notifications.

```python
def event(description: str = "", **metadata: Any) -> Any:
    """
    Defines an event field (e.g., button press, alert).
    
    Parameters:
        description: Event description
        **metadata: Additional metadata
    
    Example:
        button_pressed = event(description="Button press")
    
    Usage:
        await self.emit("button_pressed", {"button": "power"})
    """
```

### `config`

Defines a configuration field — parameters set during initialization.

```python
def config(default: Any = None, **metadata: Any) -> Any:
    """
    Defines a configuration field.
    
    Parameters:
        default: Default value
        **metadata: Additional metadata
    
    Example:
        host: str = config(default="localhost", description="Host address")
        port: int = config(default=8080)
    """
```

## `command` Decorator

```python
def command(func: Any) -> Any:
    """
    Decorator for device class methods that should be available as RPC commands.
    
    Commands can be invoked:
    - Via MQTT (topic Kamio/v1/{device_id}/sc)
    - Directly from Python code
    - From other devices via app.devices
    
    Example:
        @command
        async def set_brightness(self, value: int):
            self.brightness = value
            await self.request_state_sync()
            return {"brightness": self.brightness}
    
    Commands can be sync or async.
    """
```

## `rule` Decorator

```python
def rule(func: Any = None, *, fields: Optional[list] = None, description: Optional[str] = None) -> Any:
    """
    Decorator for device class methods that are automatically registered as rules.

    Rules are bound to the device and react to changes in specified fields.
    When a device class is registered, rules from Kamio_RULES are added to the RuleEngine.

    Parameters:
        fields: List of field names that trigger the rule on change
        description: Rule description

    Example:
        class SmartLight(Device):
            power: bool = state(default=False, writable=True)

            @rule(fields=["power"])
            async def on_power_change(self, event: RuleEvent, app):
                if event.data.get("power"):
                    print("Light turned on")
    """
```

## `Config`

Class for managing Kamio Core application configuration, supporting loading from JSON files and overriding via environment variables.

### Initialization

```python
class Config:
    def __init__(self, config_path: Optional[str] = None):
        """
        Configuration initialization.
        
        Parameters:
            config_path: Path to JSON configuration file
        
        Value priority:
            1. Environment variables (Kamio_ prefix)
            2. Configuration file
            3. Default values
        """
```

### Methods

```python
def get(self, key: str, default: Any = None, cast: Optional[Callable] = None) -> Any:
    """
    Gets a configuration value by key.

    Parameters:
        key: Configuration key
        default: Default value
        cast: Function to cast the value (int, float, bool, etc.)

    Returns:
        Configuration value

    Example:
        broker = config.get("mqtt_broker", "mqtt://localhost:1883")
    """
```

### Properties

```python
@property
def mqtt_broker(self) -> str:
    """Returns the MQTT broker address."""

@property
def log_level(self) -> int:
    """Returns the logging level."""

@property
def settings(self) -> Settings:
    """Returns a typed Settings object (mqtt_broker, log_level)."""
```

### Additional Configuration Parameters

```python
# Parameters for telemetry (used via config.get()):
telemetry_min_freq: float = 0.1  # Minimum telemetry publishing frequency in seconds
                                  # Values below this will be clamped to the specified minimum
                                  # Default: 0.1 seconds (100 ms)
                                  # Example: config.get("telemetry_min_freq", 0.1, cast=float)
```

### Environment Variables

```python
# Supported environment variables:
Kamio_MQTT_BROKER      # MQTT broker address
Kamio_LOG_LEVEL         # Logging level

# Nested keys are supported via double underscore:
# Kamio_MQTT__TLS__CAFILE corresponds to config.get("mqtt.tls.cafile")
```

## `HADiscovery`

Class for automatic device discovery in Home Assistant via MQTT Discovery.

> **Note:** the current `announce` implementation is simplified and does not cover the full
> mapping of Kamio fields to Home Assistant components (sensor, switch, etc.).
> Full discovery support is under development.

`HADiscovery` is created **lazily** — only when `app.enable_ha_discovery()` is called.
Before that, `app.ha_discovery is None`.

Use `KamioApp` methods:
```python
app.enable_ha_discovery(prefix="homeassistant")  # lazy-init + activation
app.disable_ha_discovery()                         # disable (instance is not removed)
```

Before calling `enable_ha_discovery()`, the `app.ha_discovery` property is `None`.

### Initialization (Internal)

```python
class HADiscovery:
    def __init__(self, discovery_prefix: str = "homeassistant"):
        """
        Parameters:
            discovery_prefix: Topic prefix for HA Discovery (default "homeassistant")
        """
```

### Methods

```python
async def announce(self, device: 'Device'):
    """
    Announces a device in Home Assistant via MQTT (retained).

    Discovery messages are published with retain=True so Home Assistant
    can discover devices after restart without waiting for the next
    announce cycle.

    Automatically maps Kamio fields to Home Assistant entities:
    - telemetry -> sensor
    - state (bool, writable=True) -> switch
    - state (bool, writable=False) -> binary_sensor
    - state (int/float, writable=True) -> number
    - state (str with choices, writable=True) -> select
    - state (str, writable=True) -> text
    - state (other types, writable=False) -> sensor

    Parameters:
        device: Device instance to announce

    Example:
        ha_discovery = HADiscovery()
        await ha_discovery.announce(my_device)
    """

async def clear(self, device: 'Device') -> None:
    """
    Removes a device's discovery entries from Home Assistant.

    Publishes an empty retained payload to each of the device's config topics
    so HA removes the entity. Call when removing a device from the application.

    Parameters:
        device: Device instance to clear

    Example:
        await app.ha_discovery.clear(device)
        await app.remove_device(device_id)
    """

def _map_to_ha_component(self, field) -> str:
    """
    Maps a Kamio field to a Home Assistant component.

    Returns:
        HA component name ("sensor", "switch", "binary_sensor",
        "number", "select", "text") or an empty string for unknown types.
    """
```

## Drivers (`Kamio.drivers`)

The `Kamio.drivers` module contains the base class for hardware drivers and various driver implementations for interacting with real equipment.

### `BaseDriver`

Abstract base class that all drivers must inherit from.

```python
class BaseDriver(ABC):
    """
    Base class for all Kamio drivers.
    Defines the interface for hardware interaction.
    """
    
    def __init__(self):
        """Driver initialization."""
        self.logger = logging.getLogger(f"Kamio.driver.{self.__class__.__name__}")
    
    @abstractmethod
    async def connect(self) -> None:
        """
        Establishes a connection to the hardware or service.
        Must be overridden in subclasses.
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """
        Disconnects from the hardware or service.
        Must be overridden in subclasses.
        """
        pass

    @abstractmethod
    async def execute(self, command_name: str, params: Dict[str, Any]) -> Any:
        """
        Executes a command on the hardware or service.
        
        Parameters:
            command_name: Command name
            params: Command parameters
        
        Returns:
            Command execution result
        """
        pass

    @abstractmethod
    async def read(self, field_name: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Reads a field value from the hardware or service.

        Parameters:
            field_name: Field name to read
            params:     Additional driver-specific parameters

        Returns:
            Field value
        """
        pass
    
    async def __aenter__(self) -> BaseDriver:
        """Context manager support."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager support."""
        await self.disconnect()
```

### Implemented Drivers:

#### `MockHardwareDriver`
Mock driver for testing and development.

```python
class MockHardwareDriver(BaseDriver):
    def __init__(self, latency_range: tuple = (0.01, 0.1),
                 failure_rate: float = 0.0,
                 initial_state: Optional[Dict[str, Any]] = None):
        """
        Parameters:
            latency_range: Latency range in seconds (min, max)
            failure_rate:  Probability of random failure (0.0 - 1.0)
            initial_state: Initial state for reads
        """
```

#### `GPIOChipDriver`
Driver for working with GPIO chips (requires `gpiod`).

```python
class GPIOChipDriver(BaseDriver):
    def __init__(self, chip_path: str = "/dev/gpiochip4"):
        """
        Parameters:
            chip_path: Path to the GPIO chip
        """
```

#### `TelnetDriver`
Driver for interacting with devices via Telnet.

```python
class TelnetDriver(BaseDriver):
    def __init__(self, host: str, port: int = 23, timeout: float = 5.0,
                 max_reconnect_attempts: int = 3):
        """
        Parameters:
            host: Host address
            port: Port (default 23)
            timeout: Operation timeout
            max_reconnect_attempts: Number of reconnection attempts
        """
```

#### `SerialDriver`
Driver for working with serial ports (requires `pyserial`).

```python
class SerialDriver(BaseDriver):
    def __init__(self, port: str, baudrate: int = 9600, timeout: float = 1.0):
        """
        Parameters:
            port: Port (e.g., "/dev/ttyUSB0" or "COM3")
            baudrate: Baud rate
            timeout: Read/write timeout in seconds
        """
```

#### `HTTPDeviceDriver`
Driver for interacting with HTTP/RESTful API (requires `aiohttp`).

```python
class HTTPDeviceDriver(BaseDriver):
    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None,
                 timeout: float = 10.0):
        """
        Parameters:
            base_url: API base URL
            headers: Default headers
            timeout: Request timeout
        """
```

#### `UDPDriver`
Driver for UDP protocols (request/response or plain send).

```python
class UDPDriver(BaseDriver):
    def __init__(self, host: str, port: int, timeout: float = 1.0, local_port: int = 0):
        """
        Parameters:
            host:        Target host
            port:        Target port
            timeout:     Response wait timeout
            local_port:  Local port (0 — OS-assigned)
        """
```

`execute(command_name, params)` sends `command_name` (or `params["command"]`, or `params["payload"]`).
If `params["wait_response"] == True`, returns received bytes.
`read(field_name, params)` sends `params["command"]` (or `field_name`) and returns the response.

#### `ModbusTCPDriver`
Driver for Modbus TCP (pure asyncio, no external dependencies).

```python
class ModbusTCPDriver(BaseDriver):
    def __init__(self, host: str, port: int = 502, unit_id: int = 1, timeout: float = 1.0):
        """
        Parameters:
            host:     Modbus gateway address
            port:     Port (default 502)
            unit_id:  Slave ID (default 1)
            timeout:  Response timeout
        """
```

Supported `execute` commands:
- `write_coil` / `coil` — `params["address"]`, `params["value"]` (bool)
- `write_register` / `register` — `params["address"]`, `params["value"]` (int)
- `write_registers` / `registers` — `params["address"]`, `params["values"]`

`read(field_name, params)` uses `params["command"]`: `coil`, `discrete`, `holding`, `input` and `address`, `count`.

## Internal Components (`Kamio.core`)

The `Kamio.core` module contains internal framework components. While not intended for direct use by end users, understanding them can be useful for advanced development.

### `DeviceMeta`
Metaclass responsible for collecting field and command metadata from device classes.

### `StateManager`
Manages the state of all registered devices.

```python
class StateManager:
    def get_state(self, device_id: str, field: Optional[str] = None) -> Any:
        """Returns device state or a single field."""
    
    def update_state(self, device_id: str, data: Dict[str, Any]) -> None:
        """Updates device state (including from telemetry)."""
    
    async def handle_incoming(self, envelope: Envelope):
        """Handles incoming state messages."""
```

### `CommandManager`
Handles command and response correlation.

### `RuleEngine`
Engine for executing automation rules.

The `_event_rules_by_type` index is kept up-to-date in `add_rule`/`remove_rule`
at all times — the redundant `_rebuild_index()` call during `start()` has been removed. `remove_rule` is protected against double removal.

```python
class RuleEngine:
    def add_rule(self, rule: Rule):
        """Adds a rule. Index is updated immediately."""

    def remove_rule(self, rule: Rule):
        """Removes a rule. Safe if the rule is already removed."""

    async def handle_device_update(self, device_id: str, data: Dict[str, Any]):
        """Handles a device update and triggers matching rules."""

    async def start(self):
        """Starts the engine: starts interval-rules as asyncio.Task."""

    async def stop(self):
        """Stops the engine: cancels all interval-tasks."""
```

### `DeviceRegistry`
Stores registered device classes and instances.

```python
class DeviceRegistry:
    def register_class(self, device_class: Type[Device]):
        """Registers a device class."""
    
    def register_instance(self, device_id: str, instance: Device):
        """Registers a device instance."""
    
    def get_class(self, device_type: str) -> Type[Device]:
        """Returns a device class by type."""
    
    @property
    def classes(self) -> Dict[str, Type[Device]]:
        """All registered classes."""
    
    @property
    def instances(self) -> Dict[str, Device]:
        """All registered instances."""
```

### `ServerNode`, `DeviceNode`
Abstractions for interacting with the MQTT broker on the server and device side respectively.

```python
class ServerNode:
    async def call(self, device_id: str, method: str, params: dict, timeout: float) -> Envelope:
        """Calls a command on a device and waits for a response."""
    
    async def set_state(self, device_id: str, state: dict, timeout: float) -> Any:
        """Sets device state."""

class DeviceNode:
    async def publish(self, envelope: Envelope):
        """Publishes a message."""
    
    async def emit_event(self, event_name: str, payload: dict):
        """Publishes an event."""
```

### `Envelope`, `EnvelopeType`
Define the message format used for internal communication.

```python
class Envelope:
    @staticmethod
    def state(source: str, data: dict) -> 'Envelope':
        """Creates a state message."""
    
    @staticmethod
    def telemetry(source: str, data: dict) -> 'Envelope':
        """Creates a telemetry message."""
    
    @staticmethod
    def command(source: str, target: str, method: str, params: dict) -> 'Envelope':
        """Creates a command."""
    
    @staticmethod
    def event(source: str, event_name: str, payload: dict) -> 'Envelope':
        """Creates an event."""
    
    @staticmethod
    def keepalive(source: str) -> 'Envelope':
        """Creates a keep-alive message."""

class EnvelopeType(Enum):
    DEVICE_STATE = "ds"      # Device state
    DEVICE_TELEMETRY = "dt"  # Telemetry
    DEVICE_EVENT = "de"      # Event
    SERVER_COMMAND = "sc"     # Server command
    COMMAND_ACK = "ca"       # Command acknowledgment
    STATE_ACK = "sa"         # State acknowledgment
    KEEPALIVE = "k"          # Keep-alive
    DEVICE_CONFIG = "conf"   # Configuration
```

### `topics`
Module for managing MQTT topics.

```python
def telemetry(device_id: str) -> str:       # Kamio/v1/{device_id}/dt
def state(device_id: str) -> str:           # Kamio/v1/{device_id}/ds
def state_ack(device_id: str) -> str:       # Kamio/v1/{device_id}/sa
def command(device_id: str) -> str:         # Kamio/v1/{device_id}/sc
def command_ack(device_id: str) -> str:    # Kamio/v1/{device_id}/ca
def event(device_id: str) -> str:          # Kamio/v1/{device_id}/de
def config(device_id: str) -> str:         # Kamio/v1/{device_id}/conf
def keepalive(device_id: str) -> str:       # Kamio/v1/{device_id}/k

def parse(topic: str) -> Tuple[Optional[str], Optional[str]]:
    """Parses Kamio/v1/{id}/{type} and returns (device_id, type)."""

def get_topic_func(msg_type: EnvelopeType) -> Optional[Callable[[str], str]]:
    """Returns a topic builder function by EnvelopeType."""

PREFIX: str = "Kamio"
VERSION: str = "v1"
BASE: str = "Kamio/v1"
ALL: str = "Kamio/v1/#"
TOPIC_MAP: Dict[EnvelopeType, Callable[[str], str]]
```

### `mixins`
Contains `TelemetryMixin` for periodic telemetry publishing and `TaskManagerMixin` for managing background tasks.

### `handlers`
Contains `DeviceHandler`, which dispatches incoming MQTT messages for a specific device.

On `DeviceHandler` creation, two callbacks are injected into the `Device`:
- `device._on_state_changed(device_id, field, old, new)` → `app.event_bus.publish(...)`
- `device._on_rules_trigger(device_id, changes)` → `app.rules.handle_device_update(...)`

This allows `Device` to not depend directly on `KamioApp`.

```python
class DeviceHandler:
    async def __call__(self, envelope: Envelope):
        """Handles an incoming message."""

    async def _handle_command(self, envelope: Envelope):
        """Handles a command → calls Device.handle_command() → publishes COMMAND_ACK."""

    async def _handle_state(self, envelope: Envelope):
        """Handles a state change → Device.handle_state() → callbacks."""

    async def _handle_telemetry(self, envelope: Envelope):
        """Handles telemetry → StateManager.update()."""

    async def _handle_event(self, envelope: Envelope):
        """Handles an event → Device.handle_event()."""
```

---

## Namespace Packages

Two namespace packages are provided for logical grouping:

### `Kamio.core.transport`

Transport layer. Re-exports:

```python
from kamio.core.transport import (
    MqttConnection,
    BaseNode, ServerNode, DeviceNode, BROADCAST_ID,
    Envelope, EnvelopeType, SERVER_ID,
    parse, telemetry, state, state_ack, command, command_ack,
    event, config, keepalive, get_topic_func,
    PREFIX, VERSION, BASE, ALL, TOPIC_MAP,
)
```

### `Kamio.core.automation`

Automation layer. Re-exports:

```python
from kamio.core.automation import (
    Rule, RuleEngine, RuleEvent,
    EventBus,
    HooksManager,
    PriorityRegistry, AsyncPriorityDispatcher,
)
```

> Physical files remain in `Kamio/core/` — imports via `Kamio.core.*` continue to work.

---

*Updated for Kamio Core v1.0.0b3*

---

## API Additions and Clarifications

### `RuleEvent`

```python
from typing import Any, Dict, Optional

class RuleEvent:
    def __init__(self, data: Dict[str, Any], device_id: Optional[str], kind: str) -> None: ...
    def get(self, key: str, default: Any = None) -> Any: ...
```

### `command` Decorator

```python
from kamio import command

class MyDevice(Device):
    @command
    async def set_brightness(self, value: int) -> Dict[str, Any]:
        """Device command."""
```

### `Device` — Initialization, Telemetry, and Helper Methods

```python
from typing import Any, Callable, Dict, List, Optional
from kamio.drivers.base import BaseDriver

class Device:
    def __init__(self, driver: Optional[BaseDriver] = None, keepalive_interval: float = 30.0, **kwargs) -> None: ...

    # Telemetry
    async def start_telemetry(self) -> None: ...
    async def publish_telemetry(self, data: Dict[str, Any]) -> None: ...
    async def handle_telemetry_update(self, field_names: List[str]) -> Optional[Dict[str, Any]]: ...
    async def read_telemetry_value(self, field_name: str) -> Any: ...

    # Helper RPC/utility methods
    async def send_command(self, target_device_id: str, method: str, params: dict, timeout: float = 10.0) -> None: ...
    async def shutdown(self) -> None: ...
    def register_async_callback(self, topic: str, callback: Callable) -> None: ...
    def unregister_async_callback(self, topic: str) -> None: ...
```

---

## API Additions

### `MqttConnection`

```python
from kamio.core.mqtt_connection import MqttConnection

class MqttConnection:
    def __init__(
        self,
        broker_uri: str,
        client_id: Optional[str] = None,
        keepalive: int = 60,
        clean_session: bool = True,
        protocol: int = 5,
        transport: str = "tcp",
        reconnect_min_delay: float = 1.0,
        reconnect_max_delay: float = 60.0,
        tls: Optional[dict] = None,
    ) -> None: ...

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...

    # SUBACK/UNSUBACK correlation
    async def _wait_for_suback(self, mid: int, timeout: float = 10.0) -> None: ...
    async def _wait_for_unsuback(self, mid: int, timeout: float = 10.0) -> None: ...
```

Encapsulates MQTT connection: `gmqtt.Client` creation, URI-based authentication
(`mqtt://user:pass@host:port`), TLS, and SUBACK/UNSUBACK correlation via
`asyncio.Event` with bounded cache for early ACKs (limit 1024).

### `PluginLoader` — Circular Dependency Fix

Circular dependency detection for plugins: previously, a local `_visiting` set
was reset on each recursive `load_plugin` call, leading to infinite recursion.
Now an instance-level `_loading` set is used, shared across all recursive calls.

### Test Coverage

- **751 unit tests** + 16 stress tests, all passing
- **Code coverage: 94%** (`--cov-fail-under=90` in CI)
- All drivers are covered (GPIO, Serial, Modbus, Telnet, UDP, Mock, HTTP),
  core components (envelope, state, correlation, mqtt_nodes, handlers,
  hot_reload), plugin loader, lifecycle, device lifecycle, mqtt_connection

### `HotReloadManager.__init__`

```python
def __init__(self, app: KamioApp, poll_interval: float = 1.0, debounce: float = 0.3) -> None: ...
```

### `CustomNode.publish_async`

```python
async def publish_async(self, topic: str, payload: Any, qos: int = 0, retain: bool = False) -> None: ...
```
