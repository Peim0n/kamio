# CHANGELOG

All notable changes to Kamio will be documented in this file.

## [1.0.1b2] - 2026-11-02

### Changed
- **Driver dispatch contract enforced by `BaseDriver`** — every subclass `execute` is wrapped so a command rejected by the new `supports_command()` raises `NotImplementedError` before the subclass body runs; `Device.handle_command` / `handle_state` then fall back to the `@command` method / in-memory state as documented. Custom drivers get this automatically.
- **`commands=` allowlist on every stock driver** (keyword-only, `fnmatch` patterns, e.g. `commands=["set_*"]`). Telnet/Serial/UDP/HTTP/Mock still pass every command through when it is omitted.

### Fixed
- **`ModbusTCPDriver`** — unknown commands raised `ValueError`, so device `@command`s (all four in `examples/11_modbus_device.py`) and `handle_state` never ran; they now raise `NotImplementedError`. `set_` prefix is stripped with `removeprefix` instead of `replace`.
- **`GPIOChipDriver`** — foreign commands hit the "not connected" / "`pin` is required" checks (`RuntimeError`/`ValueError`) before `NotImplementedError`; the command name is now checked first.

## [1.0.1b1] - 2026-11-01

### Added
- `examples/31_agent_api.py` — runnable walkthrough of the agent-facing API (manifest round-trip, DSL rules, guardrails, KamioTools).
- `docs/api.md` — "Agent-facing API" section: errors hierarchy, guardrails, introspection, DSL, KamioTools reference.

## [1.0.0b12] - 2026-11-01

### Added
- **`app.apply_manifest()`** — apply a `get_manifest()` snapshot (or agent-authored JSON): creates devices from embedded schemas via `make_device_class`, applies state/config, registers DSL rules; per-entry failures collected into the report, never raised.
- **`KamioTools` (`kamio/mcp_server.py`)** — MCP-style tool surface: `list_tools()` schema + `call_tool()` dispatcher (`create_device`, `set_state`, `call_command`, `add_condition_rule`, `apply_manifest`, `audit_log`, etc.). Results are `{"ok": ..., "result"/"error": ...}` dicts — ready for a thin stdio/SSE MCP transport shim.

## [1.0.0b11] - 2026-11-01

### Added — agent-facing API layer
- **`kamio.errors`** — typed error hierarchy (`KamioError`, `ValidationError`, `DeviceNotFoundError`, `CommandNotFoundError`, `DriverNotAllowedError`, `PolicyError`, `KamioTimeoutError`); each also inherits the builtin it replaces for compatibility.
- **`app.get_manifest()`** — full JSON-serializable system snapshot: devices (id, type, schema, state/config/telemetry), rules (incl. DSL specs), plugins, custom nodes, active policies.
- **`app.describe()`** — compact human/agent-readable summary of the running system plus recent actions.
- **`app.audit_log`** — ring buffer of mutating operations (`add_device`, `remove_device`, `add_rule`, `remove_rule`, `pause_automation`, `resume_automation`).
- **Guardrails** — `KamioApp(allowed_drivers=[...], denied_commands=[...], max_devices=N, dry_run=True)`; denied commands raise `PolicyError`/`ValueError`, `dry_run` validates and reports `{"dry_run": True, "would_apply": ...}` without touching drivers or state; `pause_automation()`/`resume_automation()` act as a global rule kill-switch.
- **`kamio.dsl`** — `make_device_class(name, {"field": state(...)})` builds a Device subclass from a dict spec; `when(Sensor, "temp", ">", 28).then_set(app, "fan1", "power", True)` registers a serializable declarative condition rule.

## [1.0.0b10] - 2026-11-01

### Changed
- **`Config.get("log_level")` now returns `int`** — `Settings.log_level` is stored as a Python logging constant, so both accessors (`get()` and `.log_level`) return the same typed value. String names ("INFO", "DEBUG", …) from config/env are still accepted and converted.
- **`StateManager.get_states_snapshot()` is now read-only** — the shared snapshot is wrapped in `MappingProxyType` at both levels; mutating `event.data` inside a rule now raises `TypeError` instead of silently corrupting the snapshot other rules see.

### Fixed
- **HA Discovery availability** — entities now point at a dedicated `Kamio/v1/{id}/availability` topic with retained `online`/`offline` payloads instead of the keepalive topic (whose Envelope JSON left every entity `unavailable`).
- **Telemetry post-stop publish** — the scheduler re-checks `node.is_running` after each sleep, so no extra publish escapes during shutdown.
- **TLS config validation** — `check_hostname=True` combined with `cert_reqs=CERT_NONE` now raises a clear `ValueError` at configuration time.
- **Bare `Kamio_` env var** — an env var equal to the prefix alone no longer injects an empty-string config key.
- **UDP receive queue** — bounded at 256 datagrams (oldest dropped), preventing memory growth under a datagram flood.
- **`get_schema()` JSON-safety** — non-JSON-serializable field defaults are emitted as `repr()` so the schema stays serializable as documented.
- **`route_message` race** — iterating a snapshot of the node registry fixes a `RuntimeError` when a node unregisters mid-route.

### Removed
- **`Field.__get__`/`__set_name__`** — dead descriptor code; the metaclass removes `Field` objects from the class namespace, so the descriptor protocol never engaged.

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

## [1.0.0b6] - 2026-10-22

### Fixed
- **Hot-reload of rules always rolled back** — `@app.rule` stores `_Kamio_rule_kwargs` under the `"device"` key while `Rule()` expects `device_class`; the kwarg is now normalized so real rules files reload without a `TypeError`.
- **Hot-reload duplicated device rules** — re-registering a reloaded class appended rules on top of the old class's; the old class's rules are now removed first (and dropped entirely when the reloaded class declares none).
- **Watchdog ignored watches added after `enable()`** — only directories present at enable time were scheduled; new entries are now scheduled on the running observer with directory de-duplication.
- **`set_<telemetry>`/`set_<config>` ACKed `ok`** — setter commands for non-writable-state fields now fall through to the error path instead of returning a fake success ACK.
- **`send_ack` mutated the caller's dict** — the injected `status` key no longer leaks into the result dict passed by the caller.
- **Missing `device_command_executed` events** — the event is now published on every success path: driver-handled commands, `@command` methods, and the `set_<field>` auto-route.
- **Telemetry could fail to restart** — a finished `telemetry_*` task still awaiting its discard callback no longer blocks `start_telemetry()` (e.g. after `reinitialize()`).
- **`interval=0` busy-spun the rule scheduler** — `Rule` now validates `interval` (`>0` number required) at construction.
- **`Envelope.from_dict` raised on non-dict input** — JSON-valid non-object payloads now return `None` as documented.

## [1.0.0b5] - 2026-10-22

### Added
- **CI: examples job** — all `examples/*.py` run in CI against a real `eclipse-mosquitto:2` broker; long-running demos are capped via `KAMIO_EXAMPLE_MAX_DURATION` (`examples/_bounded.py`).
- **CI: gmqtt compatibility matrix** — `gmqtt==0.8.0` and latest `<0.9` × Python 3.10/3.13.
- **CI: dedicated stress job** running `pytest tests/stress -m stress`.
- **Kamio-side reconnect backoff** — `MqttConnection` implements `reconnect_min_delay`/`reconnect_max_delay` via gmqtt 0.8's public API.
- **Explicit SUBACK/UNSUBACK waiters** — subscription acknowledgements are awaited through Kamio adapters on the client.

### Fixed
- **gmqtt 0.8 callback semantics** — `on_disconnect(packet)` signature and public `reconnect_delay`/`reconnect_retries` properties replace private-attribute access.
- **Device field annotations** — string annotations are resolved to real runtime types (`LOC-01` regression fix).
- **Reconnect resubscription** — device/custom nodes restore subscriptions after reconnect; the app owns the shared `Kamio/v1/all/#` broadcast subscription so stopping one node cannot cut broadcasts for others.
- **Lifecycle rollback** — failed `start()`/`add_device()` fully unwinds (nodes stopped, subscriptions released, MQTT disconnected).
- **Plugin unload cleanup** — `PluginContext` tracks event subscriptions, hooks, rules and background tasks and tears them down on unload or failed load; failed background tasks are logged.
- **Telnet driver** — read timeout now raises `TimeoutError` and closes the connection instead of returning a fake `ok` result.
- **Echo suppression** — bounded `EchoCache` (`OrderedDict`, cap 4096) replaces the unbounded own-cind set.
- **Correlation manager** — lock-free asyncio implementation, pending-request cap, publish coroutine closed when overloaded.
- **Validation** — `_validate_value` coerces values to concrete field types (`"42"` → `42`), rejects non-convertible values and `bool` coercion into non-bool fields.
- **`CustomNode.matches("")`** — an empty topic prefix is a catch-all, matching documentation and tests.
- **LoggingPlugin overhead** — event payloads are no longer deep-copied when the configured event level is disabled.

### Tests
- ~25 deterministic regression tests across unit and stress suites; fixed sleeps replaced with `wait_until`/`wait_for` polling helpers; all 30 examples updated to match the implemented API and pass against a real broker.

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
