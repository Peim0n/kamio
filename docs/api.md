# API Документация Kamio Core v1.0.0a1

Эта документация описывает все публичные классы и функции фреймворка Kamio Core v1.0.0a1.

## Содержание

- [KamioApp](#KamioApp)
- [HooksManager](#hooksmanager)
- [EventBus](#eventbus)
- [Plugin / PluginLoader](#plugin--pluginloader)
- [HotReloadManager](#hotreloadmanager)
- [CustomNode / CustomNodeManager](#customnode--customnodemanager)
- [Device](#device)
- [Функции для определения полей](#функции-для-определения-полей)
- [Декоратор command](#декоратор-command)
- [Config](#config)
- [HADiscovery](#hadiscovery)
- [Драйверы](#драйверы-Kamiodrivers)
- [Внутренние компоненты](#внутренние-компоненты-Kamiocore)
- [Namespace-пакеты v1.0.0a1](#namespace-пакеты-v190)

## `KamioApp`

Основной класс приложения, оркестрирующий устройства, правила и MQTT-коммуникацию.
Расположен в `Kamio.app` (пакет `Kamio/app/`, класс в `_application.py`).

Собирается из 8 миксинов: `LifecycleMixin`, `MqttDispatchMixin`, `DeviceRegistryMixin`,
`RuleRegistryMixin`, `PluginFacadeMixin`, `HotReloadFacadeMixin`, `CustomNodeFacadeMixin`, `HookEventFacadeMixin`.

### Инициализация

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
        Инициализация приложения Kamio.

        Параметры:
            mqtt_broker:   URI брокера ('mqtt://host:port') или готовый gmqtt.Client.
                           Если не указан — берётся из Config.
            client_id:     ID клиента MQTT. Авто-генерируется если не задан.
            keepalive:     Интервал keep-alive в секундах (по умолчанию 60).
            clean_session: Очищать сессию при подключении (по умолчанию True).
            protocol:      Версия протокола MQTT (по умолчанию MQTTv5).
            log_level:     Уровень логирования Python. None = из Config или без изменений.
            config_path:   Путь к JSON-файлу конфигурации.
            **kwargs:      Дополнительные параметры для MqttConnection
                           (transport, tls, reconnect_min_delay, reconnect_max_delay).

        Примечание v1.0.0a1:
            HADiscovery НЕ создаётся при инициализации. Вызовите enable_ha_discovery()
            чтобы активировать интеграцию с Home Assistant.
        """
```

### Свойства

```python
@property
def logger(self) -> logging.Logger:
    """Возвращает логгер приложения."""

@property
def is_running(self) -> bool:
    """Возвращает True, если приложение запущено."""

@property
def devices(self) -> Dict[str, Device]:
    """Возвращает словарь всех активных экземпляров устройств."""

@property
def registered_types(self) -> List[str]:
    """Возвращает список зарегистрированных типов устройств."""
```

### Методы

```python
def device(self, cls: Optional[Type[Device]] = None) -> Callable[[Type[Device]], Type[Device]]:
    """
    Декоратор для регистрации классов устройств.
    
    Использование:
        @app.device
        class MyDevice(Device): ...
        
        ИЛИ
        
        @app.device()
        class MyDevice(Device): ...
    """

def rule(self, device: Optional[Type[Device]] = None, *, interval: Optional[float] = None,
         fields: Optional[List[str]] = None, enabled: bool = True, run_on_start: bool = False,
         description: Optional[str] = None) -> Callable:
    """
    Декоратор для регистрации правил автоматизации.
    
    Параметры:
        device: Класс устройства для отслеживания изменений
        interval: Интервал выполнения в секундах (для периодических правил)
        fields: Список полей для отслеживания изменений
        enabled: Включено ли правило
        run_on_start: Запустить interval-правило сразу при старте движка
        description: Описание правила
    
    Использование:
        @app.rule(device=MyDevice, fields=["temperature"])
        async def on_temp_change(snapshot, app): ...
        
        @app.rule(interval=60.0)
        async def periodic_task(snapshot, app): ...
    """

def add_rule(self, func: Callable[[RuleEvent, KamioApp], Any], device: Optional[Type[Device]] = None,
             *, interval: Optional[float] = None, fields: Optional[List[str]] = None,
             enabled: bool = True, run_on_start: bool = False, description: Optional[str] = None) -> Callable:
    """
    Явная регистрация правила функции.
    
    Альтернатива декоратору @app.rule для динамической регистрации правил.
    
    Параметры:
        func: Функция правила для регистрации
        device: Класс устройства для отслеживания изменений
        interval: Интервал выполнения в секундах (для периодических правил)
        fields: Список полей для отслеживания изменений
        enabled: Включено ли правило
        run_on_start: Запустить interval-правило сразу при старте движка
        description: Описание правила

    Возвращает:
        Зарегистрированную функцию
    
    Пример:
        async def on_motion(snapshot, app): ...
        app.add_rule(on_motion, device=MotionSensor, fields=["motion"])
    """

def register(self, device_class: Type[Device]):
    """
    Регистрирует класс устройства без использования декоратора.
    
    Параметры:
        device_class: Класс устройства для регистрации
    """

async def create_device(self, device_id: str, device_type: str, **kwargs) -> Device:
    """
    Создает и запускает экземпляр устройства.
    
    Параметры:
        device_id: Уникальный идентификатор устройства
        device_type: Тип устройства (имя зарегистрированного класса)
        **kwargs: Дополнительные параметры для конструктора устройства
    
    Возвращает:
        Экземпляр созданного устройства
    
    Пример:
        device = await app.create_device("my_sensor", "thermostat", driver=my_driver)
    """

async def add_device(self, device_id: str, device_class: Type[Device], **kwargs) -> Device:
    """
    Упрощенный метод создания устройства с автоматической регистрацией класса.
    
    **Новое в v1.3.0**: Рекомендуемый способ создания устройств.
    Автоматически регистрирует класс устройства, если он еще не зарегистрирован.
    
    Параметры:
        device_id: Уникальный идентификатор устройства
        device_class: Класс устройства (автоматически регистрируется при необходимости)
        **kwargs: Дополнительные параметры для конструктора устройства
    
    Возвращает:
        Экземпляр созданного устройства
    
    Пример:
        device = await app.add_device("my_sensor", Thermostat, driver=my_driver)
    """

def run(self):
    """
    Блокирующий метод запуска приложения.
    Рекомендуемый способ запуска в продакшене.
    Обрабатывает сигналы SIGINT и SIGTERM для корректного завершения.
    """

async def start(self):
    """
    Асинхронный запуск приложения.
    Подключается к MQTT брокеру и запускает все узлы устройств.
    """

async def stop(self):
    """
    Асинхронная остановка приложения.
    Корректно останавливает все устройства и отключается от MQTT.
    """

async def remove_device(self, device_id: str) -> None:
    """
    Останавливает и удаляет устройство из реестра.

    Вызывает хук 'on_device_removed' перед удалением.
    Безопасно при отсутствии устройства (логирует предупреждение).

    Параметры:
        device_id: Идентификатор устройства для удаления
    """

async def remove_rule(self, func: Callable) -> None:
    """
    Удаляет зарегистрированное правило по функции.

    Отменяет фоновую задачу interval-правила (если есть).
    Вызывает хук 'on_rule_removed' перед удалением.
    Безопасно при отсутствии правила (логирует предупреждение).

    Параметры:
        func: Функция правила, переданная в @app.rule или app.add_rule
    """

def register_hook(self, event_type: str, hook: Callable, priority: int = 0) -> None:
    """
    Регистрирует lifecycle-хук.

    Удобный псевдоним для app.hooks.register().

    Параметры:
        event_type: Имя события ('on_before_start', 'on_device_added' и др.)
        hook: Sync или async callable
        priority: Приоритет выполнения (выше = раньше, по умолчанию 0)
    """

def unregister_hook(self, event_type: str, hook: Callable) -> None:
    """
    Удаляет ранее зарегистрированный хук.

    Параметры:
        event_type: Имя события
        hook: Функция хука для удаления
    """
```

## `HooksManager`

**Новое в v1.4.0.** Управляет lifecycle-хуками приложения, устройств и правил.
Доступен через `app.hooks`.

### Инициализация

```python
class HooksManager:
    def __init__(self): ...
```

### Методы

```python
def register(self, event_type: str, hook: Callable, priority: int = 0) -> None:
    """
    Регистрирует хук для события.

    Параметры:
        event_type: Имя события
        hook: Sync или async callable. Вызывается с аргументами, переданными в trigger()
        priority: Хуки с большим значением выполняются первыми (по умолчанию 0)
    """

def unregister(self, event_type: str, hook: Callable) -> None:
    """Удаляет хук для события."""

def list_hooks(self, event_type: str) -> List[Callable]:
    """Возвращает список зарегистрированных хуков в порядке приоритета."""

def clear(self, event_type: str = None) -> None:
    """
    Очищает хуки.
    Если event_type не указан — очищает все события.
    """

async def trigger(self, event_type: str, *args, **kwargs) -> None:
    """
    Вызывает все хуки события в порядке приоритета.

    Поддерживает sync и async хуки.
    Ошибки в хуках логируются и не прерывают выполнение остальных.
    """
```

### События приложения

| Событие | Когда вызывается | Аргументы |
|---|---|---|
| `on_before_start` | До подключения к MQTT | — |
| `on_after_start` | После успешного запуска | — |
| `on_before_stop` | До начала остановки | — |
| `on_after_stop` | После полной остановки | — |

### События устройств

| Событие | Когда вызывается | Аргументы |
|---|---|---|
| `on_device_added` | После создания устройства | `device: Device` |
| `on_device_removed` | До удаления устройства | `device: Device` |
| `on_device_started` | После запуска `DeviceNode` | `device: Device` |
| `on_device_stopped` | После остановки `DeviceNode` | `device: Device` |

### События правил

| Событие | Когда вызывается | Аргументы |
|---|---|---|
| `on_rule_added` | При регистрации правила через `@app.rule` | `rule: Rule` |
| `on_rule_removed` | До удаления правила через `remove_rule` | `rule: Rule` |
| `on_rule_triggered` | После успешного выполнения правила | `rule: Rule, snapshot: dict` |
| `on_rule_failed` | После ошибки в правиле | `rule: Rule, error: Exception` |

### Пример

```python
async def on_new_device(device):
    print(f"Новое устройство: {device.device_type()}")

app.register_hook('on_device_added', on_new_device)
app.register_hook('on_rule_failed', lambda rule, err: logger.error(f"{getattr(rule, 'func', rule).__name__}: {err}"))
```

## `EventBus`

**Новое в v1.5.0.** Публичная шина событий для пользовательской логики pub/sub. Доступен через `app.event_bus`.

> **Отличие от `HooksManager`:** `HooksManager` — внутренние перехватчики жизненного цикла. `EventBus` — публичный API для подписки на системные и пользовательские события.

### Методы

```python
def subscribe(
    self,
    event_type: str,
    callback: Callable,
    filter_fn: Optional[Callable[[dict], bool]] = None,
    priority: int = 0,
) -> None:
    """
    Подписка на событие.

    Параметры:
        event_type: Имя события
        callback: Sync или async callable, получает словарь data
        filter_fn: Опциональный предикат (data) -> bool. callback пропускается при False
        priority: Выше = раньше (по умолчанию 0)
    """

def unsubscribe(self, event_type: str, callback: Callable) -> None:
    """Удалить подписку по идентичности callback."""

def list_subscribers(self, event_type: str) -> List[Callable]:
    """Список callback в порядке приоритета."""

def event_types(self) -> List[str]:
    """Список типов событий, имеющих подписчиков."""

def clear(self, event_type: str = None) -> None:
    """Очистить подписчиков. Без аргумента — все события."""

async def publish(self, event_type: str, data: dict) -> None:
    """
    Опубликовать событие.

    Автоматически добавляет 'timestamp' в data (если в ней нет).
    Читает filter_fn перед вызовом callback.
    Ошибки логируются, остальные callback продолжают работу.
    """
```

### События приложения

| Событие | Когда | Поля data |
|---|---|---|
| `app_start` | После запуска | `timestamp` |
| `app_stop` | После остановки | `timestamp` |
| `mqtt_connected` | Подключение MQTT | `broker, port, rc, timestamp` |
| `mqtt_disconnected` | Отключение MQTT | `rc, timestamp` |
| `mqtt_message_received` | Входящее сообщение | `topic, payload, qos, timestamp` |
| `device_added` | Создание устройства | `device_id, device_type, device, timestamp` |
| `device_removed` | Удаление устройства | `device_id, device_type, timestamp` |
| `device_state_changed` | Изменение state | `device_id, field, old_value, new_value, timestamp` |
| `device_command_executed` | Выполнение команды | `device_id, command, params, result, timestamp` |
| `rule_added` | Регистрация правила | `rule, timestamp` |
| `rule_removed` | Удаление правила | `rule, timestamp` |
| `rule_triggered` | Успешное выполнение | `rule, snapshot, timestamp` |
| `rule_failed` | Ошибка в правиле | `rule, error, timestamp` |

### Методы `KamioApp`

```python
def subscribe_event(self, event_type: str, callback: Callable, filter_fn=None, priority: int = 0) -> None:
    """Псевдоним app.event_bus.subscribe()."""

def unsubscribe_event(self, event_type: str, callback: Callable) -> None:
    """Псевдоним app.event_bus.unsubscribe()."""

async def publish_event(self, event_type: str, data: dict) -> None:
    """Публикация пользовательского события."""
```

### Пример

```python
# Подписка с фильтром
app.subscribe_event(
    "device_state_changed",
    lambda d: print(f"{d['device_id']}.{d['field']} = {d['new_value']}"),
    filter_fn=lambda d: d.get("field") == "temperature",
)

# Пользовательское событие
await app.publish_event("sensor_alert", {"level": "critical", "sensor": "co2"})
```

## `Plugin` / `PluginLoader`

**Новое в v1.6.0.** Плагин-система для расширения фреймворка без изменения ядра.

### `Plugin` (ABC)

```python
from typing import Any, Optional
from kamio.plugins.loader import PluginContext

class Plugin(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...         # Уникальное имя плагина

    @property
    @abstractmethod
    def version(self) -> str: ...      # Строка версии

    @property
    def description(self) -> str: ...  # Опциональное описание

    @property
    def dependencies(self) -> List[str]: ...  # Имена плагинов-предшественников

    def configure(self, config: Dict[str, Any]) -> None: ...
    async def on_load(self, app: KamioApp, context: Optional[PluginContext] = None) -> None: ...
    async def on_unload(self, app: KamioApp) -> None: ...
    def subscribe_events(self, event_bus: Any) -> None: ...
    def register_hooks(self, hooks: Any) -> None: ...
```

`PluginContext` (из `Kamio.plugins.loader`) используется для scoped-регистрации подписок и хуков.

### `PluginLoader`

```python
async def load_plugin(self, plugin_class: Type[Plugin], config: dict = None) -> Plugin:
    """
    Загрузить плагин по классу.
    Вызывает configure() → on_load() → subscribe_events() → register_hooks().
    Выбрасывает ValueError если плагин уже загружен или зависимость не загружена.
    """

async def unload_plugin(self, plugin_name: str) -> None:
    """Вызывает on_unload() и удаляет из реестра."""

async def load_from_module(self, module_name: str, config: dict = None) -> Plugin:
    """Загрузить плагин из Python-модуля по его доттед пути."""

async def load_plugins_from_directory(self, directory: str) -> List[Plugin]:
    """Загрузить все плагины из директории (*.py, не ___). Ошибки логируются."""

def get_plugin(self, plugin_name: str) -> Optional[Plugin]: ...
def list_plugins(self) -> List[str]: ...

async def unload_all(self) -> None:
    """Выгружает все плагины в обратном порядке загрузки."""

@property
def load_order(self) -> List[str]:
    """Список имён плагинов в порядке их загрузки."""
```

### События plugin_loaded / plugin_unloaded

Публикуются в `EventBus` автоматически:

| Событие | Поля |
|---|---|
| `plugin_loaded` | `plugin_name, plugin_version, timestamp` |
| `plugin_unloaded` | `plugin_name, timestamp` |

### Методы `KamioApp`

```python
await app.load_plugin(PluginClass, config={...})
await app.unload_plugin("plugin_name")
await app.load_from_module("my_module.MyPlugin", config={...})
await app.load_plugins_from_directory("/path/to/plugins")
app.get_plugin("plugin_name")   # -> Plugin | None
app.list_plugins()              # -> List[str]
```

### Встроенные плагины

| Класс | Модуль | Описание |
|---|---|---|
| `LoggingPlugin` | `Kamio.plugins.builtin.logging_plugin` | События → rotating log-файл |
| `MetricsPlugin` | `Kamio.plugins.builtin.metrics_plugin` | In-memory счётчики событий |

### Пример

```python
from kamio.plugins.builtin import MetricsPlugin, LoggingPlugin

await app.load_plugin(LoggingPlugin, config={"file": "app.log", "level": "INFO"})
await app.load_plugin(MetricsPlugin)

metrics = app.get_plugin("metrics")
print(metrics.get_metrics())
```

## `HotReloadManager`

**Новое в v1.7.0.** Горячая перезагрузка правил, девайсов и конфигурации без остановки приложения.
Доступен через `app.hot_reload`.

### Методы

```python
def watch_file(self, path: str, handler: Callable) -> None:
    """Отслеживать файл. handler(file_path) вызывается при изменении."""

def watch_directory(self, directory: str, pattern: str, handler: Callable) -> None:
    """Отслеживать директорию по шаблону (e.g. '*.py')."""

def enable(self) -> None:
    """Запустить asyncio polling loop (внутренний метод)."""

def disable(self) -> None:
    """Остановить polling loop (внутренний метод)."""

def list_watched(self) -> List[str]:
    """Список отслеживаемых путей."""

@property
def is_enabled(self) -> bool:
    """Возвращает True, если HotReloadManager активен."""

# Готовые handler-фабрики:
def make_rules_handler(self) -> Callable: ...
def make_devices_handler(self) -> Callable: ...
def make_config_handler(self) -> Callable: ...
```

**Примечание:** Для включения/отключения hot reload используйте методы фасада `KamioApp`:
- `app.enable_hot_reload()` - включить polling
- `app.disable_hot_reload()` - остановить polling

### Самостоятельные функции

```python
from kamio.core.hot_reload import (
    reload_rules_from_file,    # (file_path, app) -> bool
    reload_devices_from_file,  # (file_path, app) -> bool
    reload_config_from_file,   # (file_path, app) -> bool
)
```

### События EventBus

| Событие | Поля |
|---|---|
| `hot_reload_rules` | `file_path, replaced, timestamp` |
| `hot_reload_devices` | `file_path, updated_classes, timestamp` |
| `hot_reload_config` | `file_path, config, timestamp` |
| `hot_reload_error` | `file_path, error, timestamp` |

### Методы `KamioApp`

```python
app.enable_hot_reload()                            # включить polling
app.disable_hot_reload()                           # остановить polling
app.watch_file(path, handler)                      # отслеживать файл
app.watch_directory(directory, pattern, handler)   # отслеживать директорию
```

### Пример

```python
# Горячая перезагрузка правил из директории
app.watch_directory("rules/", "*.py", app.hot_reload.make_rules_handler())
app.enable_hot_reload()

# Горячая перезагрузка конфиг
app.watch_file("config.json", app.hot_reload.make_config_handler())
```

## `CustomNode` / `CustomNodeManager`

**Новое в v1.8.0.** Расширяемая система MQTT-узлов для специфичных протоколов и кастомной логики.

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

    async def on_connect(self) -> None: ...    # опционально
    async def on_disconnect(self) -> None: ... # опционально

    def subscribe(self, topic: str, qos: int = 0) -> None:
        """Подписка на topic относительно topic_prefix."""

    def subscribe_absolute(self, topic: str, qos: int = 0) -> None:
        """Подписка на абсолютный топик."""

    def publish(self, topic: str, payload, qos: int = 0, retain: bool = False) -> None:
        """Публикация относительно topic_prefix."""

    def publish_absolute(self, topic: str, payload, qos: int = 0, retain: bool = False) -> None:
        """Публикация на абсолютный топик."""

    async def publish_async(self, topic: str, payload, qos: int = 0, retain: bool = False) -> None:
        """Асинхронная (non-blocking) публикация относительно topic_prefix."""

    def matches(self, topic: str) -> bool:
        """Возвращает True если topic начинается с topic_prefix."""
```

### `CustomNodeManager`

```python
def register_node(self, name: str, node: CustomNode) -> None:
    """Регистрация. ValueError если имя уже занято."""

def unregister_node(self, name: str) -> None:
    """Безопасное удаление."""

async def start_all(self) -> None:
    """Запуск всех узлов. Ошибка одного узла не останавливает другие."""

async def stop_all(self) -> None:
    """Остановка в обратном порядке."""

async def route_message(self, topic: str, payload: bytes) -> bool:
    """Маршрутизация сообщения. True если хотя бы один узел обработал."""

def get_node(self, name: str) -> Optional[CustomNode]: ...
def list_nodes(self) -> List[str]: ...
```

### События EventBus

| Событие | Поля |
|---|---|
| `custom_node_started` | `node_name, topic_prefix, timestamp` |
| `custom_node_stopped` | `node_name, timestamp` |
| `custom_node_error` | `node_name, error, phase, timestamp` |

### Методы `KamioApp`

```python
app.register_custom_node(name, node)  # регистрация
app.unregister_custom_node(name)      # удаление
app.get_custom_node(name)             # -> CustomNode | None
app.list_custom_nodes()               # -> List[str]
```

### Пример

```python
from kamio.core.custom_nodes import CustomNode

class MySensorBridge(CustomNode):
    async def start(self):
        self.subscribe("#")  # подписка на <prefix>/#

    async def stop(self):
        pass

    async def handle_message(self, topic, payload):
        print(f"{topic}: {payload.decode()}")
        self.publish("ack", b"ok")

app.register_custom_node("sensors", MySensorBridge(app.mqtt_client, "sensors"))
```

## `Device`

Базовый класс для всех устройств Kamio. Поддерживает декларативное описание полей (телеметрия, состояние) и команд.

### Классовые переменные

```python
Kamio_FIELDS: ClassVar[Dict[str, Field]]
    """Словарь всех полей устройства (telemetry, state, config)."""

Kamio_COMMANDS: ClassVar[Dict[str, Any]]
    """Словарь всех команд устройства (методов с декоратором @command)."""

Kamio_EVENTS: ClassVar[Dict[str, Field]]
    """Словарь всех событий устройства."""

Kamio_RULES: ClassVar[Dict[str, Any]]
    """Словарь автоматических правил устройства, созданных декоратором @rule."""
```

### Инициализация

```python
def __init__(self, driver: Optional[BaseDriver] = None, keepalive_interval: float = 30.0, **kwargs):
    """
    Инициализация устройства.
    
    Параметры:
        driver: Экземпляр драйвера для взаимодействия с оборудованием
        keepalive_interval: Интервал отправки keepalive-сообщений в секундах (0 — отключить)
        **kwargs: Дополнительные параметры
    """
```

### Свойства

```python
@property
def app(self) -> KamioApp:
    """Возвращает экземпляр KamioApp, к которому привязано устройство."""
```

### Атрибуты

```python
node: Optional[DeviceNode]
    """Узел устройства для MQTT коммуникации. Устанавливается KamioApp при регистрации."""

driver: Optional[BaseDriver]
    """Драйвер устройства для взаимодействия с оборудованием."""
```

### Классовые методы

```python
@classmethod
def device_type(cls) -> str:
    """
    Возвращает строковое представление типа устройства (имя класса в нижнем регистре).
    
    Пример:
        MyDevice.device_type() -> "mydevice"
    """

@classmethod
def get_schema(cls) -> Dict[str, Any]:
    """
    Возвращает схему устройства с описанием всех полей, команд и событий.
    
    Возвращает:
        Словарь с метаданными устройства
    """

@classmethod
def get_fields(cls, kind: str | None = None, writable: bool | None = None) -> Dict[str, Field]:
    """
    Возвращает поля устройства с фильтрацией.
    
    Параметры:
        kind: Тип поля ("telemetry", "state", "config")
        writable: Фильтр по возможности записи
    
    Возвращает:
        Словарь полей
    """

@classmethod
def get_telemetry(cls) -> Dict[str, Field]:
    """Возвращает все поля телеметрии."""

@classmethod
def get_states(cls, writable: bool | None = None) -> Dict[str, Field]:
    """Возвращает все поля состояния."""

@classmethod
def get_commands(cls) -> Dict[str, Any]:
    """Возвращает все команды устройства."""
```

### Методы жизненного цикла

```python
async def on_init(self, **kwargs):
    """
    Асинхронный хук инициализации, вызывается при создании устройства.
    Подходит для подключения драйвера и начальной настройки.
    """

async def on_start(self, node: DeviceNode):
    """
    Хук, вызываемый при запуске узла устройства.
    Подходит для запуска фоновых задач и начала публикации телеметрии.
    """

async def on_stop(self, node: DeviceNode):
    """
    Хук, вызываемый при остановке узла устройства.
    Подходит для корректного завершения задач и отключения драйвера.
    """

async def shutdown(self):
    """
    Полное завершение работы устройства.
    Отключает драйвер и отменяет все фоновые задачи.
    """

async def reinitialize(self):
    """
    Реинициализация устройства (остановка и повторный запуск).
    """
```

### Методы работы с состоянием

```python
def get_state_snapshot(self) -> Dict[str, Any]:
    """Возвращает снимок всех полей состояния."""

def get_config_snapshot(self) -> Dict[str, Any]:
    """Возвращает снимок всех полей конфигурации."""

def get_telemetry_snapshot(self) -> Dict[str, Any]:
    """Возвращает снимок всех полей телеметрии."""

def get_full_snapshot(self) -> Dict[str, Any]:
    """Возвращает полный снимок всех полей устройства."""

async def request_state_sync(self):
    """
    Запрашивает синхронизацию текущего состояния устройства с брокером.
    Публикует сообщение типа DEVICE_STATE с текущими значениями полей state.
    """

async def request_full_sync(self):
    """
    Запрашивает полную синхронизацию всех полей устройства.
    """
```

### Методы работы с событиями

```python
async def emit(self, event_name: str, payload: dict):
    """
    Публикует событие от устройства.
    
    Параметры:
        event_name: Имя события
        payload: Данные события
    """

async def handle_event(self, event_name: str, payload: dict):
    """
    Обработчик входящих событий. Переопределяется в подклассах.
    """
```

### Методы обработки команд и состояния

```python
async def handle_state(self, data: dict) -> Dict[str, Any]:
    """
    Обрабатывает входящие изменения состояния.
    Валидирует и применяет изменения к полям state.
    
    Параметры:
        data: Словарь с изменениями состояния
    
    Возвращает:
        Словарь примененных изменений
    """

async def handle_config(self, data: dict) -> Dict[str, Any]:
    """
    Обрабатывает входящие изменения конфигурации.
    
    Параметры:
        data: Словарь с изменениями конфигурации
    
    Возвращает:
        Словарь примененных изменений
    """

async def handle_command(self, method_name: str, params: dict) -> Any:
    """
    Обрабатывает входящую команду.
    Сначала пытается выполнить через драйвер, затем через внутренние методы.
    
    Параметры:
        method_name: Имя команды
        params: Параметры команды
    
    Возвращает:
        Результат выполнения команды
    """
```

### Методы управления задачами (из TaskManagerMixin)

```python
def create_task(self, coro, name: str = None):
    """
    Создает фоновую задачу, которая будет автоматически отменена при остановке.
    
    Параметры:
        coro: Корутина для выполнения
        name: Имя задачи для логирования
    """

async def cancel_all_tasks(self):
    """Отменяет все фоновые задачи устройства."""
```

### Методы телеметрии, публикации и асинхронных callback

```python
enable_telemetry: bool = True
    """Флаг автоматической публикации телеметрии. Можно переопределить в подклассе."""

async def send_command(self, target_device_id: str, method: str, params: dict, timeout: float = 10.0) -> None:
    """Отправляет команду на другое устройство через MQTT."""

async def publish_telemetry(self, data: dict) -> None:
    """Публикует пакет телеметрии."""

async def start_telemetry(self) -> None:
    """Запускает циклы публикации телеметрии по полям с freq."""

async def read_telemetry_value(self, field_name: str) -> Any:
    """Считывает значение telemetry-поля из драйвера."""

async def handle_telemetry_update(self, field_names: list[str]) -> Optional[dict[str, Any]]:
    """Собирает текущие значения указанных telemetry-полей."""

def register_async_callback(self, topic: str, callback) -> None:
    """Регистрирует async callback для произвольного MQTT-топика."""

def unregister_async_callback(self, topic: str) -> None:
    """Убирает ранее зарегистрированный async callback."""
```

## Функции для определения полей

Эти функции используются для декларативного определения полей в классах `Device`.

### `telemetry`

Определяет поле телеметрии - данные, которые устройство периодически отправляет.

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
    Определяет поле телеметрии (данные, отправляемые устройством).
    
    Параметры:
        default: Значение по умолчанию
        unit: Единица измерения (например, "°C", "%", "V")
        freq: Частота публикации (например, "5s", "1m", "100ms")
        description: Описание поля
        min: Минимальное значение для валидации
        max: Максимальное значение для валидации
        required: Обязательное поле
        **metadata: Дополнительные метаданные
    
    Пример:
        temperature: float = telemetry(unit="°C", freq="5s", description="Температура")
    """
```

### `state`

Определяет поле состояния - данные, которые можно читать и изменять извне.

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
    Определяет поле состояния (данные, которые можно читать и изменять).
    
    Параметры:
        default: Значение по умолчанию
        writable: Можно ли изменять извне
        description: Описание поля
        min: Минимальное значение для валидации
        max: Максимальное значение для валидации
        choices: Список допустимых значений
        required: Обязательное поле
        **metadata: Дополнительные метаданные
    
    Пример:
        power: bool = state(default=False, writable=True, description="Состояние питания")
        mode: str = state(default="auto", choices=("auto", "manual", "off"))
    """
```

### `event`

Определяет поле события - однократные уведомления.

```python
def event(description: str = "", **metadata: Any) -> Any:
    """
    Определяет поле события (например, нажатие кнопки, оповещение).
    
    Параметры:
        description: Описание события
        **metadata: Дополнительные метаданные
    
    Пример:
        button_pressed = event(description="Нажатие кнопки")
    
    Использование:
        await self.emit("button_pressed", {"button": "power"})
    """
```

### `config`

Определяет поле конфигурации - параметры, задаваемые при инициализации.

```python
def config(default: Any = None, **metadata: Any) -> Any:
    """
    Определяет поле конфигурации.
    
    Параметры:
        default: Значение по умолчанию
        **metadata: Дополнительные метаданные
    
    Пример:
        host: str = config(default="localhost", description="Адрес хоста")
        port: int = config(default=8080)
    """
```

## Декоратор `command`

```python
def command(func: Any) -> Any:
    """
    Декоратор для методов класса устройства, которые должны быть доступны как команды RPC.
    
    Команды могут быть вызваны:
    - Через MQTT (топик Kamio/v1/{device_id}/sc)
    - Напрямую из Python кода
    - Из других устройств через app.devices
    
    Пример:
        @command
        async def set_brightness(self, value: int):
            self.brightness = value
            await self.request_state_sync()
            return {"brightness": self.brightness}
    
    Команды могут быть синхронными или асинхронными.
    """
```

## Декоратор `rule`

```python
def rule(func: Any = None, *, fields: Optional[list] = None, description: Optional[str] = None) -> Any:
    """
    Декоратор для методов класса устройства, которые автоматически регистрируются как правила.

    Правила привязываются к устройству и реагируют на изменения указанных полей.
    При регистрации класса устройства правила из Kamio_RULES добавляются в RuleEngine.

    Параметры:
        fields: Список имён полей, при изменении которых вызывается правило
        description: Описание правила

    Пример:
        class SmartLight(Device):
            power: bool = state(default=False, writable=True)

            @rule(fields=["power"])
            async def on_power_change(self, event: RuleEvent, app):
                if event.data.get("power"):
                    print("Light turned on")
    """
```

## `Config`

Класс для управления конфигурацией приложения Kamio Core, поддерживающий загрузку из JSON-файлов и переопределение через переменные окружения.

### Инициализация

```python
class Config:
    def __init__(self, config_path: Optional[str] = None):
        """
        Инициализация конфигурации.
        
        Параметры:
            config_path: Путь к JSON файлу конфигурации
        
        Приоритет значений:
            1. Переменные окружения (префикс Kamio_)
            2. Файл конфигурации
            3. Значения по умолчанию
        """
```

### Методы

```python
def get(self, key: str, default: Any = None, cast: Optional[Callable] = None) -> Any:
    """
    Получает значение конфигурации по ключу.

    Параметры:
        key: Ключ конфигурации
        default: Значение по умолчанию
        cast: Функция для преобразования значения (int, float, bool и т.д.)

    Возвращает:
        Значение конфигурации

    Пример:
        broker = config.get("mqtt_broker", "mqtt://localhost:1883")
    """
```

### Свойства

```python
@property
def mqtt_broker(self) -> str:
    """Возвращает адрес MQTT-брокера."""

@property
def log_level(self) -> int:
    """Возвращает уровень логирования."""

@property
def settings(self) -> Settings:
    """Возвращает типизированный объект Settings (mqtt_broker, log_level)."""
```

### Дополнительные параметры конфигурации

```python
# Параметры для телеметрии (используются через config.get()):
telemetry_min_freq: float = 0.1  # Минимальная частота публикации телеметрии в секундах
                                  # Значения ниже этого будут ограничены до указанного минимума
                                  # По умолчанию: 0.1 секунды (100 мс)
                                  # Пример: config.get("telemetry_min_freq", 0.1, cast=float)
```

### Переменные окружения

```python
# Поддерживаемые переменные окружения:
Kamio_MQTT_BROKER      # Адрес MQTT брокера
Kamio_LOG_LEVEL         # Уровень логирования

# Вложенные ключи поддерживаются через двойное подчёркивание:
# Kamio_MQTT__TLS__CAFILE соответствует config.get("mqtt.tls.cafile")
```

## `HADiscovery`

Класс для автоматического обнаружения устройств в Home Assistant через MQTT Discovery.

> **Примечание:** текущая реализация `announce` упрощена и не покрывает полное
> отображение Kamio-полей на компоненты Home Assistant (sensor, switch и т.д.).
> Полная поддержка discovery находится в разработке.

**Новое в v1.0.0a1:** `HADiscovery` создаётся **lazily** — только при вызове `app.enable_ha_discovery()`.
До этого `app.ha_discovery is None`.

Используйте методы `KamioApp`:
```python
app.enable_ha_discovery(prefix="homeassistant")  # lazy-init + активация
app.disable_ha_discovery()                         # отключить (экземпляр не удаляется)
```

До вызова `enable_ha_discovery()` свойство `app.ha_discovery` равно `None`.

### Инициализация (внутренняя)

```python
class HADiscovery:
    def __init__(self, discovery_prefix: str = "homeassistant"):
        """
        Параметры:
            discovery_prefix: Префикс топиков для HA Discovery (по умолчанию "homeassistant")
        """
```

### Методы

```python
async def announce(self, device: 'Device'):
    """
    Объявляет устройство в Home Assistant через MQTT.
    
    Автоматически маппит поля Kamio на сущности Home Assistant:
    - telemetry -> sensor
    - state (bool, writable=True) -> switch
    - state (bool, writable=False) -> binary_sensor
    - state (другие типы) -> sensor
    
    Параметры:
        device: Экземпляр устройства для объявления
    
    Пример:
        ha_discovery = HADiscovery()
        await ha_discovery.announce(my_device)
    """

def _map_to_ha_component(self, field) -> str:
    """
    Маппит поле Kamio на компонент Home Assistant.
    
    Возвращает:
        Имя компонента HA ("sensor", "switch", "binary_sensor")
    """
```

## Драйверы (`Kamio.drivers`)

Модуль `Kamio.drivers` содержит базовый класс для аппаратных драйверов и различные реализации драйверов для взаимодействия с реальным оборудованием.

### `BaseDriver`

Абстрактный базовый класс, от которого должны наследоваться все драйверы.

```python
class BaseDriver(ABC):
    """
    Базовый класс для всех драйверов Kamio.
    Определяет интерфейс для взаимодействия с оборудованием.
    """
    
    def __init__(self):
        """Инициализация драйвера."""
        self.logger = logging.getLogger(f"Kamio.driver.{self.__class__.__name__}")
    
    @abstractmethod
    async def connect(self) -> None:
        """
        Устанавливает соединение с аппаратным обеспечением или сервисом.
        Должен быть переопределен в подклассах.
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """
        Разрывает соединение с аппаратным обеспечением или сервисом.
        Должен быть переопределен в подклассах.
        """
        pass

    @abstractmethod
    async def execute(self, command_name: str, params: Dict[str, Any]) -> Any:
        """
        Выполняет команду на аппаратном обеспечении или сервисе.
        
        Параметры:
            command_name: Имя команды
            params: Параметры команды
        
        Возвращает:
            Результат выполнения команды
        """
        pass

    @abstractmethod
    async def read(self, field_name: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Считывает значение поля с аппаратного обеспечения или сервиса.

        Параметры:
            field_name: Имя поля для чтения
            params:     Дополнительные параметры, специфичные для драйвера

        Возвращает:
            Значение поля
        """
        pass
    
    async def __aenter__(self) -> BaseDriver:
        """Поддержка контекстного менеджера."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Поддержка контекстного менеджера."""
        await self.disconnect()
```

### Реализованные драйверы:

#### `MockHardwareDriver`
Имитационный драйвер для тестирования и разработки.

```python
class MockHardwareDriver(BaseDriver):
    def __init__(self, latency_range: tuple = (0.01, 0.1),
                 failure_rate: float = 0.0,
                 initial_state: Optional[Dict[str, Any]] = None):
        """
        Параметры:
            latency_range: Диапазон задержки в секундах (min, max)
            failure_rate:  Вероятность случайного сбоя (0.0 - 1.0)
            initial_state: Начальное состояние для чтения
        """
```

#### `GPIOChipDriver`
Драйвер для работы с GPIO-чипами (требуется `gpiod`).

```python
class GPIOChipDriver(BaseDriver):
    def __init__(self, chip_path: str = "/dev/gpiochip4"):
        """
        Параметры:
            chip_path: Путь к GPIO чипу
        """
```

#### `TelnetDriver`
Драйвер для взаимодействия с устройствами по Telnet.

```python
class TelnetDriver(BaseDriver):
    def __init__(self, host: str, port: int = 23, timeout: float = 5.0,
                 max_reconnect_attempts: int = 3):
        """
        Параметры:
            host: Адрес хоста
            port: Порт (по умолчанию 23)
            timeout: Таймаут операций
            max_reconnect_attempts: Количество попыток переподключения
        """
```

#### `SerialDriver`
Драйвер для работы с последовательными портами (требуется `pyserial`).

```python
class SerialDriver(BaseDriver):
    def __init__(self, port: str, baudrate: int = 9600, timeout: float = 1.0):
        """
        Параметры:
            port: Порт (например, "/dev/ttyUSB0" или "COM3")
            baudrate: Скорость передачи
            timeout: Таймаут чтения/записи в секундах
        """
```

#### `HTTPDeviceDriver`
Драйвер для взаимодействия с HTTP/RESTful API (требуется `aiohttp`).

```python
class HTTPDeviceDriver(BaseDriver):
    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None,
                 timeout: float = 10.0):
        """
        Параметры:
            base_url: Базовый URL API
            headers: Заголовки по умолчанию
            timeout: Таймаут запросов
        """
```

#### `UDPDriver`
Драйвер для UDP-протоколов (request/response или plain send).

```python
class UDPDriver(BaseDriver):
    def __init__(self, host: str, port: int, timeout: float = 1.0, local_port: int = 0):
        """
        Параметры:
            host:        Целевой хост
            port:        Целевой порт
            timeout:     Таймаут ожидания ответа
            local_port:  Локальный порт (0 — выбор ОС)
        """
```

`execute(command_name, params)` отправляет `command_name` (или `params["command"]`, или `params["payload"]`).
Если `params["wait_response"] == True`, возвращает принятые байты.
`read(field_name, params)` отправляет `params["command"]` (или `field_name`) и возвращает ответ.

#### `ModbusTCPDriver`
Драйвер для Modbus TCP (pure asyncio, без внешних зависимостей).

```python
class ModbusTCPDriver(BaseDriver):
    def __init__(self, host: str, port: int = 502, unit_id: int = 1, timeout: float = 1.0):
        """
        Параметры:
            host:     Адрес Modbus-шлюза
            port:     Порт (по умолчанию 502)
            unit_id:  ID slave (по умолчанию 1)
            timeout:  Таймаут ответа
        """
```

Поддерживаемые команды `execute`:
- `write_coil` / `coil` — `params["address"]`, `params["value"]` (bool)
- `write_register` / `register` — `params["address"]`, `params["value"]` (int)
- `write_registers` / `registers` — `params["address"]`, `params["values"]`

`read(field_name, params)` использует `params["command"]`: `coil`, `discrete`, `holding`, `input` и `address`, `count`.

## Внутренние компоненты (`Kamio.core`)

Модуль `Kamio.core` содержит внутренние компоненты фреймворка. Хотя они не предназначены для прямого использования конечными пользователями, их понимание может быть полезно для расширенной разработки.

### `DeviceMeta`
Метакласс, отвечающий за сбор метаданных полей и команд из классов устройств.

### `StateManager`
Управляет состоянием всех зарегистрированных устройств.

```python
class StateManager:
    def get_state(self, device_id: str, field: Optional[str] = None) -> Any:
        """Возвращает состояние устройства или отдельное поле."""
    
    def update_state(self, device_id: str, data: Dict[str, Any]) -> None:
        """Обновляет состояние устройства (в т.ч. из телеметрии)."""
    
    async def handle_incoming(self, envelope: Envelope):
        """Обрабатывает входящие сообщения состояния."""
```

### `CommandManager`
Обрабатывает корреляцию команд и ответов.

### `RuleEngine`
Движок для выполнения правил автоматизации.

**Новое в v1.0.0a1:** Индекс `_event_rules_by_type` поддерживается актуальным в `add_rule`/`remove_rule`
в любой момент — избыточный `_rebuild_index()` при `start()` удалён. `remove_rule` защищён от двойного удаления.

```python
class RuleEngine:
    def add_rule(self, rule: Rule):
        """Добавляет правило. Индекс обновляется немедленно."""

    def remove_rule(self, rule: Rule):
        """Удаляет правило. Безопасно если правило уже удалено."""

    async def handle_device_update(self, device_id: str, data: Dict[str, Any]):
        """Обрабатывает обновление устройства и запускает совпадающие правила."""

    async def start(self):
        """Запускает движок: стартует interval-правила как asyncio.Task."""

    async def stop(self):
        """Останавливает движок: отменяет все interval-задачи."""
```

### `DeviceRegistry`
Хранит зарегистрированные классы и экземпляры устройств.

```python
class DeviceRegistry:
    def register_class(self, device_class: Type[Device]):
        """Регистрирует класс устройства."""
    
    def register_instance(self, device_id: str, instance: Device):
        """Регистрирует экземпляр устройства."""
    
    def get_class(self, device_type: str) -> Type[Device]:
        """Возвращает класс устройства по типу."""
    
    @property
    def classes(self) -> Dict[str, Type[Device]]:
        """Все зарегистрированные классы."""
    
    @property
    def instances(self) -> Dict[str, Device]:
        """Все зарегистрированные экземпляры."""
```

### `ServerNode`, `DeviceNode`
Абстракции для взаимодействия с MQTT-брокером на стороне сервера и устройства соответственно.

```python
class ServerNode:
    async def call(self, device_id: str, method: str, params: dict, timeout: float) -> Envelope:
        """Вызывает команду на устройстве и ждет ответа."""
    
    async def set_state(self, device_id: str, state: dict, timeout: float) -> Any:
        """Устанавливает состояние устройства."""

class DeviceNode:
    async def publish(self, envelope: Envelope):
        """Публикует сообщение."""
    
    async def emit_event(self, event_name: str, payload: dict):
        """Публикует событие."""
```

### `Envelope`, `EnvelopeType`
Определяют формат сообщений, используемых для внутренней коммуникации.

```python
class Envelope:
    @staticmethod
    def state(source: str, data: dict) -> 'Envelope':
        """Создает сообщение состояния."""
    
    @staticmethod
    def telemetry(source: str, data: dict) -> 'Envelope':
        """Создает сообщение телеметрии."""
    
    @staticmethod
    def command(source: str, target: str, method: str, params: dict) -> 'Envelope':
        """Создает команду."""
    
    @staticmethod
    def event(source: str, event_name: str, payload: dict) -> 'Envelope':
        """Создает событие."""
    
    @staticmethod
    def keepalive(source: str) -> 'Envelope':
        """Создает keep-alive сообщение."""

class EnvelopeType(Enum):
    DEVICE_STATE = "ds"      # Состояние устройства
    DEVICE_TELEMETRY = "dt"  # Телеметрия
    DEVICE_EVENT = "de"      # Событие
    SERVER_COMMAND = "sc"     # Команда сервера
    COMMAND_ACK = "ca"       # Подтверждение команды
    STATE_ACK = "sa"         # Подтверждение состояния
    KEEPALIVE = "k"          # Keep-alive
    DEVICE_CONFIG = "conf"   # Конфигурация
```

### `topics`
Модуль для управления MQTT-топиками.

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
    """Парсит Kamio/v1/{id}/{type} или legacy Kamio/{id}/{type}."""

def get_topic_func(msg_type: EnvelopeType) -> Optional[Callable[[str], str]]:
    """Возвращает функцию-строитель топика по EnvelopeType."""

PREFIX: str = "Kamio"
VERSION: str = "v1"
BASE: str = "Kamio/v1"
ALL: str = "Kamio/v1/#"
TOPIC_MAP: Dict[EnvelopeType, Callable[[str], str]]
```

### `mixins`
Содержит `TelemetryMixin` для периодической публикации телеметрии и `TaskManagerMixin` для управления фоновыми задачами.

### `handlers`
Содержит `DeviceHandler`, который диспетчеризирует входящие сообщения MQTT для конкретного устройства.

**Новое в v1.0.0a1:** При создании `DeviceHandler` инжектирует в `Device` два callback-а:
- `device._on_state_changed(device_id, field, old, new)` → `app.event_bus.publish(...)`
- `device._on_rules_trigger(device_id, changes)` → `app.rules.handle_device_update(...)`

Это позволяет `Device` не зависеть напрямую от `KamioApp`.

```python
class DeviceHandler:
    async def __call__(self, envelope: Envelope):
        """Обрабатывает входящее сообщение."""

    async def _handle_command(self, envelope: Envelope):
        """Обрабатывает команду → вызывает Device.handle_command() → публикует COMMAND_ACK."""

    async def _handle_state(self, envelope: Envelope):
        """Обрабатывает изменение состояния → Device.handle_state() → callbacks."""

    async def _handle_telemetry(self, envelope: Envelope):
        """Обрабатывает телеметрию → StateManager.update()."""

    async def _handle_event(self, envelope: Envelope):
        """Обрабатывает событие → Device.handle_event()."""
```

---

## Namespace-пакеты v1.0.0a1

С версии v1.0.0a1 введены два namespace-пакета для логической группировки:

### `Kamio.core.transport`

Транспортный слой. Реэкспортирует:

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

Слой автоматизации. Реэкспортирует:

```python
from kamio.core.automation import (
    Rule, RuleEngine, RuleEvent,
    EventBus,
    HooksManager,
    PriorityRegistry, AsyncPriorityDispatcher,
)
```

> Физические файлы остаются в `Kamio/core/` — импорты через `Kamio.core.*` продолжают работать.

---

*Обновлено для Kamio Core v1.0.0a1*

---

## Дополнения и уточнения API v1.0.0a1

### `RuleEvent`

```python
from typing import Any, Dict, Optional

class RuleEvent:
    def __init__(self, data: Dict[str, Any], device_id: Optional[str], kind: str) -> None: ...
    def get(self, key: str, default: Any = None) -> Any: ...
```

### Декоратор `command`

```python
from kamio import command

class MyDevice(Device):
    @command
    async def set_brightness(self, value: int) -> Dict[str, Any]:
        """Команда устройства."""
```

### `Device` — инициализация, телеметрия и вспомогательные методы

```python
from typing import Any, Callable, Dict, List, Optional
from kamio.drivers.base import BaseDriver

class Device:
    def __init__(self, driver: Optional[BaseDriver] = None, keepalive_interval: float = 30.0, **kwargs) -> None: ...

    # Телеметрия
    async def start_telemetry(self) -> None: ...
    async def publish_telemetry(self, data: Dict[str, Any]) -> None: ...
    async def handle_telemetry_update(self, field_names: List[str]) -> Optional[Dict[str, Any]]: ...
    async def read_telemetry_value(self, field_name: str) -> Any: ...

    # Вспомогательные RPC/utility методы
    async def send_command(self, target_device_id: str, method: str, params: dict, timeout: float = 10.0) -> None: ...
    async def shutdown(self) -> None: ...
    def register_async_callback(self, topic: str, callback: Callable) -> None: ...
    def unregister_async_callback(self, topic: str) -> None: ...
```

### `HotReloadManager.__init__`

```python
def __init__(self, app: KamioApp, poll_interval: float = 1.0, debounce: float = 0.3) -> None: ...
```

### `CustomNode.publish_async`

```python
async def publish_async(self, topic: str, payload: Any, qos: int = 0, retain: bool = False) -> None: ...
```

