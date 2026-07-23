# CHANGELOG

All notable changes to Kamio will be documented in this file.

## [1.0.0a1] - 2026-07-23

### Added
- **Декларативные устройства** — поля `state`, `telemetry`, `event`, `config` через аннотации Python
- **Драйверы** — GPIO, Serial, Telnet, HTTP, UDP, Modbus TCP, Mock (latency/failure simulation)
- **MQTT v5** — надёжная коммуникация, авто-реконнект
- **Асинхронность** — полностью `asyncio`, никаких блокирующих вызовов в event loop
- **Правила автоматизации** — реакция на изменения полей и периодические интервалы
- **Plugin System** — изолированные плагины с автоматическим cleanup через `PluginContext`
- **Hot-Reload** — перезагрузка правил и устройств без остановки приложения
- **Custom MQTT Nodes** — произвольные MQTT-узлы с маршрутизацией сообщений
- **Home Assistant Discovery** — lazy-init, активируется только при вызове `enable_ha_discovery()`
- **Конфигурация** — JSON-файлы + переменные окружения `Kamio_*`
- **EventBus** — публичная шина событий pub/sub с поддержкой фильтров и приоритетов
- **HooksManager** — lifecycle-хуки для app, device, rule событий
- **UDPDriver** — асинхронный UDP-драйвер для request/response и plain-send протоколов
- **ModbusTCPDriver** — чистая реализация Modbus TCP на `asyncio` + `struct`, без внешних зависимостей
