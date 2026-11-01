# CHANGELOG

All notable changes to Kamio will be documented in this file.

## [1.0.0b8] - 2026-11-01

### Documentation
- **README and PyPI description** — removed the stale `v1.0.0b4` from the README title and doc links (the PyPI project page renders the README); the version badge now reads the current release from PyPI. Dropped hardcoded versions from the headers of `docs/api.md` and `docs/architecture.md`.

## [1.0.0b7] - 2026-11-01

### Performance
- **No idle Tasks per inbound MQTT message** — `_on_mqtt_message` now schedules `CustomNodeManager.route_message` only when `has_nodes()` is true, and publishes `mqtt_message_received` only when `EventBus.has_subscribers()` is true. Before this, every message created two asyncio Tasks even when nothing consumed them (#13).
- **MQTT payloads decoded once** — the router parses the envelope once and shares it across the server node and every device node, so a broadcast to N devices no longer runs `json.loads` N times (`BaseNode.accepts` / `BaseNode.dispatch_envelope`). The per-message "Message received" log is now a lazy `DEBUG` instead of an eager `INFO` f-string (#14).
- **LoggingPlugin file I/O off the event loop** — records go through a `QueueHandler` onto a background `QueueListener` thread, so writes and rollover of the `RotatingFileHandler` no longer block the loop (#10).
- **Rule engine hot path cached** — `inspect.signature` parameter counts are cached per rule function, and the MRO-derived base device types are cached per device class (weak keys, safe with hot-reload) (#12).
- **Interval rules share one state snapshot** — `StateManager.get_states_snapshot()` returns a copy that is rebuilt only after a state update, instead of copying the whole state table on every tick of every interval rule. Rules that fire between updates receive the same dict, so treat `event.data` as read-only (#11).

## [1.0.0b4] - 2026-08-01

### Fixed
- **Python version requirement** — corrected `requires-python` from `>=3.9` to `>=3.10` in PyPI metadata. The codebase uses `dataclass(slots=True)` which requires Python 3.10+. Updated README badge and mypy config accordingly.
- Removed Python 3.9 from CI matrix and classifiers.

## [1.0.0b3] - 2026-08-01

### Fixed
- **Cross-platform test suite** — replaced real MQTT broker dependency with an in-memory broker (`InMemoryBroker`) in `tests/conftest.py`. On Linux, `gmqtt.Client.connect` opens a TCP socket synchronously and raises `ConnectionRefusedError` when no broker is listening on `127.0.0.1:1883`, causing 15 stress tests and 4 unit tests (`test_two_apps_interaction`) to fail on Ubuntu CI. The in-memory broker routes pub/sub between all `KamioApp` instances with proper MQTT wildcard matching (`+` single level, `#` multi level) and self-delivery (real MQTT clients receive their own published messages).

## [1.0.0b1] - 2026-07-23

### Added
- **Declarative devices** — `state`, `telemetry`, `event`, `config` fields via Python annotations
- **Drivers** — GPIO, Serial, Telnet, HTTP, UDP, Modbus TCP, Mock (latency/failure simulation)
- **MQTT v5** — reliable communication, auto-reconnect
- **Asynchrony** — fully `asyncio`, no blocking calls in the event loop
- **Automation rules** — reaction to field changes and periodic intervals
- **Plugin System** — isolated plugins with automatic cleanup via `PluginContext`
- **Hot-Reload** — reload rules and devices without stopping the application
- **Custom MQTT Nodes** — arbitrary MQTT nodes with message routing
- **Home Assistant Discovery** — lazy-init, activated only when `enable_ha_discovery()` is called
- **Configuration** — JSON files + environment variables `Kamio_*`
- **EventBus** — public pub/sub event bus with filter and priority support
- **HooksManager** — lifecycle hooks for app, device, rule events
- **UDPDriver** — async UDP driver for request/response and plain-send protocols
- **ModbusTCPDriver** — pure Modbus TCP implementation on `asyncio` + `struct`, no external dependencies

## [1.0.0b2] - 2026-08-01

### Added
- **Examples** — `examples/` directory with 30 self-contained scripts: 14 basic tutorials (minimal device, smart home, drivers, rules, plugins, custom nodes, event bus, telemetry, hot-reload, HA discovery, Modbus, lifecycle hooks, config, device interaction) and 16 deep-dive guides for framework developers (validation pitfalls, lifecycle ordering, echo suppression, threading vs async, driver edge cases, EventBus/HotReload/Plugin/MQTT/Rules internals, config/resource/silent-failure/custom-node gotchas, testing patterns, production checklist)

### Critical Fixes
- **GPIO driver** — all `gpiod` calls wrapped in `asyncio.to_thread()`, no longer block the event loop
- **Serial driver** — `readline()` limited to `read_limit` bytes (default 4096), protection against unbounded buffer growth
- **HTTP driver** — errors propagated as exceptions instead of masking in dict; added `raise_for_status()`
- **Modbus driver** — auto-reconnect on TCP connection drop, transaction retry
- **MetricsPlugin** — `threading.Lock` for thread-safety, proper `on_unload` for cleanup
- **PluginLoader** — topo-sort of dependencies with cycle detection, try-finally for rule cleanup on unload
- **HADiscovery** — `retain=True` for discovery messages, `clear()` method to remove devices from HA
- **Device.__setattr__** — min/max/choices validation on direct state field assignment
- **parse_freq** — `ValueError` instead of silently returning 0.0 for invalid input

### Thread-Safety
- `PriorityRegistry`, `DeviceRegistry`, `StateManager`, `BaseCorrelationManager` — `RLock` for all mutations and reads
- `DeviceRegistry.instances`/`.classes` return a snapshot instead of a live dict
- `DeviceRegistry.unregister_instance()` for safe removal
- `MqttConnection` — bounded cache for early SUBACK/UNSUBACK, `try-finally` in ACK waiter

### Race Condition Fixes
- `mqtt_nodes.dispatch()` — snapshot loop reference before `create_task`
- `hot_reload._schedule_call_in_loop()` — identity-check before removing pending handle
- `hot_reload` rollback — `RuleEngine.set_rules()` for atomic replacement
- `mqtt._on_mqtt_message()` — snapshot `_device_nodes` before iteration
- `correlation._wait_for_ack()` — identity-check in finally to prevent future loss

### Memory & Resource Management
- `Device._own_state_cinds` — bounded set with eviction of old entries (limit 4096)
- `LifecycleMixin.stop()` — await background tasks with configurable `shutdown_timeout`
- `LifecycleMixin.start()` — snapshot `_device_nodes` before iteration

### Configuration
- `Config._overlay_env()` — correct trimming of `Kamio_` prefix (case-insensitive)
- `Config.get()` — removed duplicate env-lookup (env already overlayed in `__init__`)
- `KamioApp.__init__` — validation of unknown kwargs with informative error

### Bug Fixes
- **PluginLoader circular dependency** — cycle detection was broken: local `_visiting` set was reset on every recursive `load_plugin` call. Replaced with instance-level `_loading` set shared across all recursive calls.

### Removed (legacy cleanup)
- **`app_mixins.py`** — backward-compat shim removed. Import mixins directly from `kamio.app.mixins.*`.
- **`RuleEvent.__getitem__`** — access via `event["update"]` / `event["device_id"]` removed. Use `event.data`, `event.device_id`, `event.kind`.
- **Legacy MQTT topic format** — `topics.parse()` no longer parses the 3-segment format `Kamio/{id}/{type}`. Only `Kamio/v1/{id}/{type}`.
- **Legacy plugin `on_load` signature** — parameter introspection removed. `on_load` is now always called as `on_load(app, context)`.
- **Device fallback to `app.rules`** — if `_on_rules_trigger` is not set, `handle_state` no longer falls back to `app.rules.handle_device_update` directly. The callback must be injected by `DeviceHandler`.

### CI
- Split into `lint-typecheck`, `test` (with 90% coverage), and `stress` jobs
- Coverage report with `--cov-fail-under=90` and upload to Codecov
- Stress tests marked with `@pytest.mark.stress` and run separately

### Tests
- 601 new unit tests: drivers (GPIO/Serial/Modbus/Telnet/UDP/Mock/HTTP), core internals (envelope, state, correlation, mqtt_nodes, handlers, hot_reload, mqtt_connection), plugin loader edge cases, lifecycle, device lifecycle, custom nodes, rules engine, config gotchas, architecture coverage
- Stress test fixes: `LoadDevice`/`LoadDeviceWithRule` default changed from `0` to `-1` so every `handle_state` update is a genuine change (the first update `{"value": 0}` was a no-op and left per-device rule counts short by one); memory-leak stress tests now filter `tracemalloc` diffs to `kamio` source files only, eliminating flakiness caused by leftover interpreter/logging allocations from prior heavy suites
- Code coverage: 68% → 94%
- Total: 751 unit + 16 stress tests, all passing
