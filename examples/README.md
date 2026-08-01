# Kamio — Примеры

Папка содержит 30 примеров, покрывающих весь функционал библиотеки Kamio.
Каждый пример — самостоятельный Python-скрипт с подробными комментариями на русском языке.

Примеры делятся на две группы:

- **01–14** — базовые туториалы: пошаговое знакомство с возможностями фреймворка.
- **15–30** — deep-dive для разработчиков фреймворка: внутренности, подводные камни, краевые случаи и production-чеклист.

## Требования

- Python 3.10+
- Установленная библиотека Kamio (`pip install -e .` из корня проекта)
- MQTT-брокер на `localhost:1883` (например, [Mosquitto](https://mosquitto.org/))

```bash
# Установка Kamio в режиме разработки
pip install -e .

# Запуск MQTT-брокера (варианты)
docker run -it -p 1883:1883 eclipse-mosquitto
# или
mosquitto
```

## Список примеров

### Базовые туториалы (01–14)

| # | Файл | Тема | Уровень |
|---|------|------|---------|
| 01 | `01_minimal_device.py` | Минимальное устройство: поля, команды, регистрация | Начальный |
| 02 | `02_smart_home.py` | Умный дом: несколько устройств, правила, события, хуки | Средний |
| 03 | `03_drivers.py` | Все драйверы (Mock, Telnet, UDP, Modbus, Serial, HTTP, GPIO) | Средний |
| 04 | `04_rules_automation.py` | Правила автоматизации: поля, интервалы, cross-device | Средний |
| 05 | `05_plugins.py` | Плагины: кастомные, зависимости, встроенные, загрузка из директории | Продвинутый |
| 06 | `06_custom_nodes.py` | Кастомные MQTT-ноды: логгер, мост, конвертер, счётчик | Продвинутый |
| 07 | `07_event_bus.py` | Event Bus: подписки, фильтры, приоритеты, кастомные события | Средний |
| 08 | `08_telemetry.py` | Телеметрия: freq, unit, валидация, ручная публикация, драйверы | Средний |
| 09 | `09_hot_reload.py` | Hot Reload: правила, устройства, конфиг без перезапуска | Продвинутый |
| 10 | `10_ha_discovery.py` | Home Assistant MQTT Discovery: автоматическое обнаружение | Средний |
| 11 | `11_modbus_device.py` | Modbus TCP: чтение регистров, запись coils, телеметрия | Продвинутый |
| 12 | `12_lifecycle_hooks.py` | Хуки жизненного цикла: все события, приоритеты, sync/async | Средний |
| 13 | `13_config_env.py` | Конфигурация: JSON, env vars, вложенные параметры, Config.get() | Начальный |
| 14 | `14_device_interaction.py` | Взаимодействие устройств: send_command, cross-device rules | Продвинутый |

### Deep-dive для разработчиков фреймворка (15–30)

| # | Файл | Тема | Уровень |
|---|------|------|---------|
| 15 | `15_validation_pitfalls.py` | Подводные камни валидации полей (min/max/choices) | Продвинутый |
| 16 | `16_lifecycle_order.py` | Порядок вызовов жизненного цикла (start/stop/run) | Продвинутый |
| 17 | `17_echo_suppression.py` | Подавление эха MQTT (own-state correlation IDs) | Продвинутый |
| 18 | `18_threading_async.py` | Потоки против асинхронности, `_run_coro_threadsafe` | Продвинутый |
| 19 | `19_driver_edge_cases.py` | Краевые случаи драйверов (таймауты, ошибки, reconnect) | Продвинутый |
| 20 | `20_event_bus_internals.py` | Внутренности EventBus и HooksManager, подводные камни | Продвинутый |
| 21 | `21_hot_reload_gotchas.py` | Подводные камни hot-reload (watchdog, polling, rollback) | Продвинутый |
| 22 | `22_plugin_lifecycle.py` | Подводные камни жизненного цикла плагинов (leaks, deps) | Продвинутый |
| 23 | `23_mqtt_internals.py` | Внутренности MQTT-слоя (ACKs, subscriptions, connection) | Продвинутый |
| 24 | `24_rules_engine_internals.py` | Внутренности движка правил (priority, intervals, removal) | Продвинутый |
| 25 | `25_config_gotchas.py` | Подводные камни конфигурации (env overlay, defaults) | Продвинутый |
| 26 | `26_resource_cleanup.py` | Паттерны очистки ресурсов (задачи, драйверы, подписки) | Продвинутый |
| 27 | `27_silent_failures.py` | Все «тихие» отказы фреймворка и их последствия | Продвинутый |
| 28 | `28_custom_node_gotchas.py` | Подводные камни CustomNode и CustomNodeManager | Продвинутый |
| 29 | `29_testing_patterns.py` | Паттерны тестирования устройств, правил, плагинов | Продвинутый |
| 30 | `30_production_checklist.py` | Production-чеклист перед запуском в эксплуатацию | Продвинутый |

## Рекомендуемый порядок изучения

### Базовый путь (01–14)

1. **Начните с `01_minimal_device.py`** — основы: класс устройства, поля, команды.
2. **`13_config_env.py`** — конфигурация приложения.
3. **`02_smart_home.py`** — комплексный пример с несколькими устройствами.
4. **`04_rules_automation.py`** — автоматизация через правила.
5. **`07_event_bus.py`** — событийная шина.
6. **`08_telemetry.py`** — телеметрия.
7. **`12_lifecycle_hooks.py`** — хуки жизненного цикла.
8. **`03_drivers.py`** — драйверы оборудования.
9. **`11_modbus_device.py`** — Modbus TCP (реальный протокол).
10. **`10_ha_discovery.py`** — интеграция с Home Assistant.
11. **`14_device_interaction.py`** — взаимодействие между устройствами.
12. **`05_plugins.py`** — плагины (расширение фреймворка).
13. **`06_custom_nodes.py`** — кастомные MQTT-ноды.
14. **`09_hot_reload.py`** — hot reload (продвинутая тема).

### Deep-dive путь (15–30)

После освоения базовых примеров переходите к deep-dive — они раскрывают
внутреннее устройство и неочевидное поведение фреймворка:

15. **`15_validation_pitfalls.py`** → `16_lifecycle_order.py` → `17_echo_suppression.py`
16. **`18_threading_async.py`** → `19_driver_edge_cases.py`
17. **`20_event_bus_internals.py`** → `21_hot_reload_gotchas.py` → `22_plugin_lifecycle.py`
18. **`23_mqtt_internals.py`** → `24_rules_engine_internals.py` → `25_config_gotchas.py`
19. **`26_resource_cleanup.py`** → `27_silent_failures.py` → `28_custom_node_gotchas.py`
20. **`29_testing_patterns.py`** → `30_production_checklist.py`

## Запуск

```bash
# Из корня проекта
python examples/01_minimal_device.py

# Или с указанием MQTT-брокера (если не localhost:1883)
# Отредактируйте строку KamioApp(mqtt_broker=...) в начале main()
```

## Что дальше

- [API документация](../docs/api.md) — полный справочник по всем классам и методам
- [Архитектура](../docs/architecture.md) — внутреннее устройство библиотеки
- [README](../README.md) — обзор проекта и Quick Start

## Структура примера

Каждый пример следует единой структуре:

```python
"""
NN — Название
================

Описание что демонстрирует.

Запуск::
    python examples/NN_name.py

Что демонстрирует:
    - Пункт 1
    - Пункт 2
"""
from __future__ import annotations

import asyncio
import logging

from kamio import KamioApp, Device, ...

# 1. Определение классов устройств
class MyDevice(Device):
    ...

# 2. Хуки, правила, плагины (если нужно)
async def on_start_hook():
    ...

# 3. Главный цикл
async def main():
    app = KamioApp(...)
    app.register(MyDevice)
    await app.start()
    await app.add_device("id", MyDevice)
    # ... демонстрация ...
    await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
```
