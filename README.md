# Kamio v1.0.0a1

![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)
![Version](https://img.shields.io/badge/version-1.0.0a1--alpha-blue.svg)
![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)
![MQTT](https://img.shields.io/badge/MQTT-v5-orange.svg)
![Tests](https://github.com/Peim0n/kamio/actions/workflows/ci.yml/badge.svg)

**Kamio** — декларативный асинхронный IoT-фреймворк для Python на базе MQTT. Описывайте устройства классами, подключайте оборудование драйверами, автоматизируйте правилами.

## Возможности

- **Декларативные устройства** — поля `state`, `telemetry`, `event`, `config` через аннотации Python
- **Драйверы** — GPIO, Serial, Telnet, HTTP, UDP, Modbus TCP, Mock (latency/failure simulation)
- **MQTT v5** — надёжная коммуникация, авто-реконнект, обратная совместимость с legacy-топиками
- **Асинхронность** — полностью `asyncio`, никаких блокирующих вызовов в event loop
- **Правила автоматизации** — реакция на изменения полей и периодические интервалы
- **Plugin System** — изолированные плагины с автоматическим cleanup через `PluginContext`
- **Hot-Reload** — перезагрузка правил и устройств без остановки приложения
- **Custom MQTT Nodes** — произвольные MQTT-узлы с маршрутизацией сообщений
- **Home Assistant Discovery** — lazy-init, активируется только при вызове `enable_ha_discovery()`
- **Конфигурация** — JSON-файлы + переменные окружения `Kamio_*`

## Установка

```bash
pip install kamio
```

### Требования

- Python 3.9+
- MQTT брокер (рекомендуется Mosquitto)

```bash
# Ubuntu/Debian
sudo apt-get install mosquitto mosquitto-clients
# macOS
brew install mosquitto
# Windows: https://mosquitto.org/download/
```

### Дополнительные зависимости

```bash
pip install kamio[gpio]           # GPIO (gpiod)
pip install kamio[serial]          # Serial (pyserial)
pip install kamio[http]            # HTTP (aiohttp)
pip install kamio[all-drivers]   # все внешние зависимости драйверов (gpiod, pyserial, aiohttp)
pip install kamio[dev]            # форматирование, типизация
pip install kamio[test]           # pytest, pytest-asyncio
```

> `UDPDriver`, `TelnetDriver` и `ModbusTCPDriver` используют только стандартную библиотеку Python и доступны без extras.

## Быстрый старт

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
    # Можно передать драйвер: Serial/Telnet/HTTP/UDP/Modbus TCP
    # await app.add_device("living_room", SmartLight, driver=TelnetDriver("10.0.0.10"))
    await app.add_device("living_room", SmartLight)
    await app.add_device("hall_sensor", MotionSensor)
    await app.start()


if __name__ == "__main__":
    asyncio.run(main())
    # ИЛИ блокирующий запуск с обработкой SIGINT/SIGTERM:
    # app.run()
```

## Примеры использования

### Плагины

```python
from kamio.plugins.builtin.metrics_plugin import MetricsPlugin

metrics = await app.load_plugin(MetricsPlugin)
print(metrics.get_counter("device_state_changed"))
```

### Hot-reload правил

```python
app.watch_directory("rules/", "*.py", app.hot_reload.make_rules_handler())
app.enable_hot_reload()
```

### Home Assistant Discovery

```python
app.enable_ha_discovery(prefix="homeassistant")
# HADiscovery создаётся только здесь, не при инициализации KamioApp
```

### Кастомный MQTT-узел

```python
from kamio.core.custom_nodes import CustomNode

class BridgeNode(CustomNode):
    async def handle_message(self, topic: str, payload: bytes):
        print(f"Bridge received: {topic}")

app.register_custom_node("bridge", BridgeNode(app.mqtt_client, "bridge"))
```

### Конфигурация через файл

```python
app = KamioApp(config_path="config.json")
```

```json
{
  "mqtt_broker": "mqtt://broker.local:1883",
  "log_level": "INFO"
}
```

Или через переменные окружения:
```bash
Kamio_MQTT_BROKER=mqtt://broker.local:1883
Kamio_LOG_LEVEL=DEBUG
```

## Структура тестов

```
tests/
  unit/         # изолированные тесты компонентов
  stress/       # тесты под нагрузкой
```

```bash
pytest                          # все тесты
pytest tests/unit/              # только unit
pytest tests/stress/            # только stress
```

## Документация

- [API](docs/api.md) — справочник по всем классам и методам
- [Архитектура](docs/architecture.md) — детальный обзор архитектуры v1.0.0a1

## Лицензия

Apache-2.0 — см. файл [LICENSE](LICENSE)

