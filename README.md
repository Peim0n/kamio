# Kamio v1.0.0b2

![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)
![Version](https://img.shields.io/badge/version-1.0.0b2--beta-blue.svg)
![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)
![MQTT](https://img.shields.io/badge/MQTT-v5-orange.svg)
![Coverage](https://img.shields.io/badge/coverage-94%25-brightgreen.svg)
![Tests](https://github.com/Peim0n/kamio/actions/workflows/ci.yml/badge.svg)

**Kamio** is a declarative asynchronous IoT framework for Python built on MQTT. Describe your devices as classes, connect hardware with drivers, and automate with rules.

## Features

- **Declarative devices** — `state`, `telemetry`, `event`, `config` fields via Python annotations
- **Drivers** — GPIO, Serial, Telnet, HTTP, UDP, Modbus TCP, Mock (latency/failure simulation)
- **MQTT v5** — reliable communication, auto-reconnect, backward compatibility with legacy topics
- **Asynchronous** — fully `asyncio`, no blocking calls in the event loop
- **Automation rules** — react to field changes and periodic intervals
- **Plugin System** — isolated plugins with automatic cleanup via `PluginContext`
- **Hot-Reload** — reload rules and devices without stopping the application
- **Custom MQTT Nodes** — arbitrary MQTT nodes with message routing
- **Home Assistant Discovery** — lazy-init, activated only when `enable_ha_discovery()` is called
- **Configuration** — JSON files + `Kamio_*` environment variables

## Installation

```bash
pip install kamio
```

### Requirements

- Python 3.9+
- MQTT broker (Mosquitto recommended)

```bash
# Ubuntu/Debian
sudo apt-get install mosquitto mosquitto-clients
# macOS
brew install mosquitto
# Windows: https://mosquitto.org/download/
```

### Additional Dependencies

```bash
pip install kamio[gpio]           # GPIO (gpiod)
pip install kamio[serial]          # Serial (pyserial)
pip install kamio[http]            # HTTP (aiohttp)
pip install kamio[all-drivers]   # all external driver dependencies (gpiod, pyserial, aiohttp)
pip install kamio[dev]            # formatting, type checking
pip install kamio[test]           # pytest, pytest-asyncio
```

> `UDPDriver`, `TelnetDriver`, and `ModbusTCPDriver` use only the Python standard library and are available without extras.

## Quick Start

```python
import asyncio
from kamio import KamioApp, Device, command, state, telemetry

app = KamioApp(mqtt_broker="mqtt://localhost:1883")


class SmartLight(Device):
    power:      bool  = state(default=False, writable=True)
    brightness: int   = state(default=100, min=0, max=255, writable=True)
    energy_wh:  float = telemetry(default=0.0, unit="Wh")

    @command
    async def toggle(self):
        self.power = not self.power
        return {"power": self.power}


class MotionSensor(Device):
    motion_detected: bool = state(default=False, writable=True)


@app.rule(device=MotionSensor, fields=["motion_detected"])
async def on_motion(event, app):
    if event.data["motion_detected"]:
        light = app.devices.get("living_room")
        if light:
            await light.handle_state({"power": True, "brightness": 200})


async def main():
    # You can pass a driver: Serial/Telnet/HTTP/UDP/Modbus TCP
    # await app.add_device("living_room", SmartLight, driver=TelnetDriver("10.0.0.10"))
    await app.add_device("living_room", SmartLight)
    await app.add_device("hall_sensor", MotionSensor)
    await app.start()


if __name__ == "__main__":
    asyncio.run(main())
    # OR blocking startup with SIGINT/SIGTERM handling:
    # app.run()
```

## Usage Examples

### Plugins

```python
from kamio.plugins.builtin.metrics_plugin import MetricsPlugin

metrics = await app.load_plugin(MetricsPlugin)
print(metrics.get_counter("device_state_changed"))
```

### Hot-reload rules

```python
app.watch_directory("rules/", "*.py", app.hot_reload.make_rules_handler())
app.enable_hot_reload()
```

### Home Assistant Discovery

```python
app.enable_ha_discovery(prefix="homeassistant")
# HADiscovery is created only here, not during KamioApp initialization
```

### Custom MQTT node

```python
from kamio.core.custom_nodes import CustomNode

class BridgeNode(CustomNode):
    async def handle_message(self, topic: str, payload: bytes):
        print(f"Bridge received: {topic}")

app.register_custom_node("bridge", BridgeNode(app.mqtt_client, "bridge"))
```

### Configuration via file

```python
app = KamioApp(config_path="config.json")
```

```json
{
  "mqtt_broker": "mqtt://broker.local:1883",
  "log_level": "INFO"
}
```

Or via environment variables:
```bash
Kamio_MQTT_BROKER=mqtt://broker.local:1883
Kamio_LOG_LEVEL=DEBUG
```

## Test Structure

```
tests/
  unit/         # isolated component tests (751 tests, 94% coverage)
  stress/       # load tests (16 tests)
```

```bash
pytest                          # all tests
pytest tests/unit/              # unit only
pytest tests/stress/            # stress only
pytest --cov=kamio              # with coverage report
```

## Documentation

- [API](docs/api.md) — reference for all classes and methods v1.0.0b2
- [Architecture](docs/architecture.md) — detailed architecture overview v1.0.0b2
- [CHANGELOG](CHANGELOG.md) — changelog

## License

Apache-2.0 — see the [LICENSE](LICENSE) file
