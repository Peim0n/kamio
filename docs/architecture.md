# Архитектура Kamio Core v1.0.0a1

## Структура модулей

```
Kamio/
  app/                        # KamioApp — пакет (с v1.0.0a1)
    __init__.py               #   реэкспортирует KamioApp + все миксины
    _application.py           #   класс KamioApp
    mixins/
      lifecycle.py            #   LifecycleMixin   — start/stop/run/signal
      mqtt.py                 #   MqttDispatchMixin — MQTT callbacks, dispatch
      devices.py              #   DeviceRegistryMixin — add/remove/register device
      rules.py                #   RuleRegistryMixin — @rule, add_rule, remove_rule
      plugins.py              #   PluginFacadeMixin — load/unload/list plugins
      hot_reload.py           #   HotReloadFacadeMixin — watch_file/watch_directory
      custom_nodes.py         #   CustomNodeFacadeMixin — register/list custom nodes
      hooks_events.py         #   HookEventFacadeMixin — hooks + event_bus shortcuts
  app_mixins.py               # backward-compat shim (реэкспортирует из app/mixins/)
  device.py                   # Device + @command
  data_fields.py              # Field, state(), telemetry(), event(), config(), parse_freq()
  config.py                   # Config — JSON-файл + Kamio_* env-переменные
  discovery.py                # HADiscovery — Home Assistant MQTT Discovery (lazy)
  core/
    transport/                # namespace-пакет транспортного слоя (с v1.0.0a1)
      __init__.py             #   реэкспорт: MqttConnection, nodes, topics, Envelope
    automation/               # namespace-пакет слоя автоматизации (с v1.0.0a1)
      __init__.py             #   реэкспорт: Rule, RuleEngine, RuleEvent, EventBus, HooksManager
    device_meta.py            # DeviceMeta — metaclass: Kamio_FIELDS/COMMANDS/EVENTS
    envelope.py               # Envelope — JSON-обёртка MQTT-сообщения
    event_bus.py              # EventBus — pub/sub с filter_fn и приоритетами
    hot_reload.py             # HotReloadManager — asyncio polling + debounce + watchdog
    custom_nodes.py           # CustomNode ABC + CustomNodeManager
    handlers.py               # DeviceHandler — диспетчер Envelope → Device
    hooks.py                  # HooksManager — lifecycle-хуки
    mixins.py                 # TelemetryMixin, TaskManagerMixin
    mqtt_connection.py        # MqttConnection — gmqtt клиент с авто-реконнектом
    mqtt_nodes.py             # BaseNode, ServerNode, DeviceNode
    registry.py               # DeviceRegistry — реестр классов и экземпляров
    rules.py                  # Rule, RuleEngine, RuleEvent
    state.py                  # StateManager — централизованное состояние
    subscription.py           # PriorityRegistry, AsyncPriorityDispatcher
    topics.py                 # MQTT topic helpers + parse()
    correlation.py            # CommandManager — связь запрос/ответ по cind
  plugins/
    base.py                   # Plugin — ABC
    loader.py                 # PluginLoader + PluginContext (scoped cleanup)
    builtin/
      logging_plugin.py       # LoggingPlugin — события → rotating log-файл
      metrics_plugin.py       # MetricsPlugin — in-memory счётчики событий
  drivers/
    base.py                   # BaseDriver — 4 абстрактных метода
    mock.py                   # MockHardwareDriver — симуляция latency/failure
    gpio.py                   # GPIOChipDriver
    serial.py                 # SerialDriver
    telnet.py                 # TelnetDriver
    http.py                   # HTTPDeviceDriver
    udp.py                    # UDPDriver
    modbus.py                 # ModbusTCPDriver (pure asyncio)

tests/
  unit/          # изолированные тесты компонентов (config, fields, rules, hooks…)
  stress/        # тесты под нагрузкой (load_stability, stress)
  conftest.py    # корневые фикстуры (mock_mqtt, app)
```

## Компоненты

| Компонент | Файл | Роль |
|---|---|---|
| `KamioApp` | `app/_application.py` | Главный оркестратор. Собирается из 8 миксинов |
| `Device` | `device.py` | Базовый класс IoT-устройства. Расширяется пользователем |
| `DeviceMeta` | `core/device_meta.py` | Metaclass — собирает Kamio_FIELDS / COMMANDS / EVENTS при определении класса |
| `DeviceNode` | `core/mqtt_nodes.py` | MQTT-узел одного устройства: subscribe/publish, фильтрация по `device_id` |
| `ServerNode` | `core/mqtt_nodes.py` | MQTT-узел приложения: принимает состояния и телеметрию от всех устройств |
| `DeviceHandler` | `core/handlers.py` | Диспетчер: принимает `Envelope`, вызывает `handle_state`/`handle_command`; инжектирует callbacks в `Device` |
| `RuleEngine` | `core/rules.py` | Запускает правила по событию устройства или по интервалу; индекс всегда синхронизирован |
| `StateManager` | `core/state.py` | Хранит актуальное состояние всех устройств в памяти |
| `DeviceRegistry` | `core/registry.py` | Хранит классы (`_classes`) и экземпляры (`_instances`) устройств |
| `Envelope` | `core/envelope.py` | JSON-обёртка MQTT-сообщения: source, target, type, data, cind |
| `BaseDriver` | `drivers/base.py` | Интерфейс драйвера: `connect`, `disconnect`, `execute`, `read` |
| `TelemetryMixin` | `core/mixins.py` | Периодическая публикация телеметрии по расписанию (`freq`) |
| `TaskManagerMixin` | `core/mixins.py` | Создание, отслеживание и отмена `asyncio.Task` |
| `Config` | `config.py` | Конфигурация из JSON-файла и переменных окружения `Kamio_*` |
| `HADiscovery` | `discovery.py` | Lazy-init: создаётся только при вызове `enable_ha_discovery()` |
| `HooksManager` | `core/hooks.py` | Lifecycle-хуки: app, device, rule events. Sync/async, приоритеты |
| `EventBus` | `core/event_bus.py` | Публичный pub/sub: системные + пользовательские события, filter_fn, priority |
| `PluginContext` | `plugins/loader.py` | Scoped-контекст плагина: отслеживает все подписки/хуки для чистого unload |
| `Plugin` | `plugins/base.py` | ABC для плагинов: on_load/unload, subscribe_events, register_hooks, dependencies |
| `PluginLoader` | `plugins/loader.py` | Загрузка по классу/модулю/директории, разрешение зависимостей |
| `HotReloadManager` | `core/hot_reload.py` | asyncio polling + debounce; reload rules/devices/config at runtime |
| `CustomNode` | `core/custom_nodes.py` | ABC для кастомных MQTT-узлов: start/stop/handle_message, publish/subscribe helpers |
| `CustomNodeManager` | `core/custom_nodes.py` | Регистрация, запуск, остановка и маршрутизация сообщений к узлам |
| `MqttConnection` | `core/mqtt_connection.py` | gmqtt клиент с автоматическим реконнектом (exponential backoff) |

## Жизненный цикл приложения

```
KamioApp.__init__
  └── StateManager, CommandManager, RuleEngine, DeviceRegistry, HooksManager, EventBus
  └── HADiscovery = None  (lazy — создаётся только в enable_ha_discovery())
  └── MqttConnection создаётся если mqtt_broker — строка URI
      иначе используется готовый gmqtt.Client
  └── Callbacks: on_message → _on_mqtt_message
                 on_connect → _on_mqtt_connect → event_bus("mqtt_connected")
                 on_disconnect → _on_mqtt_disconnect → event_bus("mqtt_disconnected")

app.add_device("id", DeviceClass)  ─── или create_device("id", "type")
  └── DeviceClass авто-регистрируется в DeviceRegistry
  └── Device.__init__ → _apply_defaults() → дефолты из Kamio_FIELDS
  └── device._app = self  (KamioApp)
  └── Device.on_init() → driver.connect()
  └── DeviceNode создан, привязан к Device
  └── DeviceHandler создан:
        ├── инжектирует device._on_state_changed  → app.event_bus.publish(...)
        └── инжектирует device._on_rules_trigger  → app.rules.handle_device_update(...)
  └── DeviceRegistry.register_instance()
  └── hooks.trigger('on_device_added', device)
  └── event_bus.publish('device_added', {...})
  └── [если HA enabled] ha_discovery.announce(device)

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

Входящее MQTT-сообщение (_on_mqtt_message — вызывается из MQTT-потока)
  └── custom_nodes.route_message()           [первым — через asyncio threadsafe]
  └── event_bus("mqtt_message_received")     [threadsafe]
  └── server_node.dispatch()
  └── topics.parse(topic) → device_id
      ├── device_id найден → device_nodes[device_id].dispatch()
      └── device_id == "all" → dispatch всем нодам  [только broadcast]

DeviceNode.dispatch → Envelope.parse → DeviceHandler(envelope)
  ├── DEVICE_STATE   → Device.handle_state()
  │     └── validate → driver.execute (если есть) → setattr
  │         └── _on_state_changed(id, field, old, new)  [callback от DeviceHandler]
  │         └── _on_rules_trigger(id, changes)          [callback от DeviceHandler]
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

## MQTT топики

| Топик | Направление | Тип конверта |
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

Обратная совместимость: `topics.parse()` понимает как `Kamio/v1/{id}/{type}`, так и legacy `Kamio/{id}/{type}`.

## Поля устройства

```python
state(default, writable, min, max, choices)  # читаемое и управляемое состояние
telemetry(unit, freq)                        # периодически отправляемые данные (read-only)
config(default)                              # конфигурационные параметры (always writable)
event(description)                           # события (emit-only, не сохраняются)
```

`DeviceMeta` при создании класса собирает все аннотации типа `Field` в:
- `Kamio_FIELDS` — state + telemetry + config (ключ → `Field`)
- `Kamio_EVENTS` — event (ключ → `Field`)
- `Kamio_COMMANDS` — методы с `@command` (имя → callable)

## Декаплинг Device от KamioApp (v1.0.0a1)

До v1.0.0a1 `Device.handle_state()` напрямую вызывал `self.app.event_bus` и `self.app.rules`, что создавало жёсткую связность.

Начиная с v1.0.0a1 `Device` использует инжектируемые callbacks:

```python
# Инжектируется DeviceHandler при создании:
device._on_state_changed = async (device_id, field, old, new) → event_bus.publish(...)
device._on_rules_trigger = async (device_id, changes) → rules.handle_device_update(...)

# Fallback (если нет DeviceHandler): app.event_bus / app.rules напрямую (обратная совместимость)
```

Это позволяет тестировать `Device` без `KamioApp`.

## Правила автоматизации

```python
# По событию устройства + фильтр полей (app-level)
@app.rule(device=Thermostat, fields=["temperature"])
async def on_temp(event: RuleEvent, app: KamioApp): ...

# По интервалу (с немедленным запуском)
@app.rule(interval=60.0, run_on_start=True)
async def periodic(event: RuleEvent, app: KamioApp): ...

# Device-level правила (внутри класса устройства)
class SmartLight(Device):
    power: bool = state(default=False, writable=True)

    @rule(fields=["power"])
    async def on_power_change(self, event: RuleEvent, app: KamioApp):
        # Автоматически регистрируется при регистрации класса устройства
        pass

# Явная регистрация (без декоратора)
app.add_rule(func, device=Sensor, fields=["motion"])
```

`RuleEngine` — индекс `_event_rules_by_type` всегда актуален (обновляется в `add_rule`/`remove_rule`).
Ошибка в одном правиле логируется, остальные продолжают работу (`gather(return_exceptions=True)`).

Device-level правила (декоратор `@rule` в классе устройства) автоматически добавляются в `RuleEngine` при регистрации класса устройства через `app.register()` или `@app.device`.

## Плагины

```python
from kamio.plugins.base import Plugin

class MyPlugin(Plugin):
    name = "my_plugin"

    async def on_load(self, app, context):
        context.subscribe("device_state_changed", self._on_state)

    async def on_unload(self, app):
        pass  # cleanup автоматически через PluginContext
```

`PluginContext` отслеживает все подписки/хуки плагина → чистый unload без утечек.

## Драйверы

Реализованы драйверы: `MockHardwareDriver`, `GPIOChipDriver`, `SerialDriver`, `TelnetDriver`, `HTTPDeviceDriver`, `UDPDriver`, `ModbusTCPDriver`.

`BaseDriver` определяет контракт из 4 методов:

```python
async def connect()                                  # подключение к оборудованию
async def disconnect()                               # отключение
async def execute(command_name, params)              # выполнение команды
async def read(field_name, params=None)              # чтение значения
```

Если у устройства есть `driver`, `handle_command` и `handle_state` сначала пробуют выполнить через него.
Драйвер получает `command_name` и `params` (включая `value`), что позволяет реализовать унифицированный протокол `set_<field>` или передать команду через `params["command"]`.
`NotImplementedError` в `execute` — fallback к методу устройства.

## Асинхронность

- Весь I/O — `async/await`, никаких блокирующих вызовов в event loop
- MQTT callbacks выполняются внутри asyncio event loop gmqtt; дополнительная потокобезопасность не нужна
- Телеметрия — отдельные `asyncio.Task` на группу полей с одинаковым `freq`
- Interval rules — отдельные `asyncio.Task` в `RuleEngine`
- Остановка — `asyncio.wait_for(..., timeout=5.0)` на каждый этап
- `TaskManagerMixin.cancel_all_tasks()` — безопасная отмена всех задач устройства

## Roadmap

| Версия | Фича | Статус |
|---|---|---|
| v1.3.0 | `add_device`, `add_rule`, `MockHardwareDriver` | ✅ Готово |
| v1.4.0 | Lifecycle Hooks (`HooksManager`) | ✅ Готово |
| v1.5.0 | Event Bus (`EventBus`) | ✅ Готово |
| v1.6.0 | Plugin System (`Plugin`, `PluginLoader`) | ✅ Готово |
| v1.7.0 | Hot-Reload (`HotReloadManager`) | ✅ Готово |
| v1.8.0 | Custom MQTT Nodes (`CustomNode`, `CustomNodeManager`) | ✅ Готово |
| v1.0.0a1 | Архитектурный рефакторинг: декаплинг, новая структура пакетов | ✅ Готово |
