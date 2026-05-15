# API Документация Synapse Core v43

Эта документация описывает основные классы и функции, доступные в фреймворке Synapse Core v43.

## `SynapseApp`

Основной класс приложения, оркестрирующий устройства, правила и MQTT-коммуникацию.

```python
class SynapseApp:
    def __init__(
        self,
        mqtt_broker: Union[str, mqtt.Client] = "mqtt://localhost:1883",
        client_id: Optional[str] = None,
        keepalive: int = 60,
        clean_session: bool = True,
        protocol: int = mqtt.MQTTv5,
        log_level: Optional[int] = logging.INFO,
        **kwargs
    ):
        # ...

    def device(self, cls: Optional[Type[Device]] = None) -> Callable[[Type[Device]], Type[Device]]:
        """Декоратор для регистрации классов устройств."""
        # ...

    def rule(self, device: Optional[Type[Device]] = None, interval: Optional[float] = None, fields: Optional[List[str]] = None, enabled: bool = True, description: Optional[str] = None) -> Callable[[Callable[[dict, SynapseApp], Any]], Callable[[dict, SynapseApp], Any]]:
        """Декоратор для регистрации правил автоматизации."""
        # ...

    async def create_device(self, device_id: str, device_type: str, **kwargs) -> Device:
        """Создает и запускает экземпляр устройства."""
        # ...

    def run(self):
        """Блокирующий метод запуска приложения."""
        # ...

    async def start(self):
        """Асинхронный запуск приложения."""
        # ...

    async def stop(self):
        """Асинхронная остановка приложения."""
        # ...

    @property
    def devices(self) -> Dict[str, Device]:
        """Возвращает все активные экземпляры устройств."""
        # ...

    @property
    def registered_types(self) -> List[str]:
        """Возвращает список зарегистрированных типов устройств."""
        # ...
```

## `Device`

Базовый класс для всех устройств Synapse. Поддерживает декларативное описание полей (телеметрия, состояние) и команд.

```python
class Device(TelemetryMixin, metaclass=DeviceMeta):
    # Class variables populated by DeviceMeta
    SYNAPSE_FIELDS: ClassVar[Dict[str, Field]]
    SYNAPSE_COMMANDS: ClassVar[Dict[str, Any]]
    SYNAPSE_EVENTS: ClassVar[Dict[str, Field]]

    def __init__(self, driver: Optional[BaseDriver] = None, **kwargs):
        # ...

    @classmethod
    def device_type(cls) -> str:
        """Возвращает строковое представление типа устройства (имя класса в нижнем регистре)."""
        # ...

    async def request_state_sync(self):
        """Запрашивает синхронизацию текущего состояния устройства с брокером."""
        # ...

    async def emit(self, event_name: str, payload: dict):
        """Публикует событие от устройства."""
        # ...

    # Hooks for device lifecycle
    async def on_init(self, **kwargs):
        """Асинхронный хук инициализации, вызывается при создании устройства."""
        # ...

    async def on_start(self, node: DeviceNode):
        """Хук, вызываемый при запуске узла устройства."""
        # ...

    async def on_stop(self, node: DeviceNode):
        """Хук, вызываемый при остановке узла устройства."""
        # ...

    # ... (другие методы, такие как get_state_snapshot, handle_command и т.д.)
```

## Функции для определения полей

Эти функции используются для декларативного определения полей в классах `Device`.

### `telemetry`

```python
def telemetry(
    *,
    unit: str = "",
    freq: str = "",
    description: str = "",
    min: float | None = None,
    max: float | None = None,
    required: bool = False,
    **metadata: Any,
) -> Any:
    """Определяет поле телеметрии (данные, отправляемые устройством)."""
```

### `state`

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
    """Определяет поле состояния (данные, которые можно читать и изменять)."""
```

### `event`

```python
def event(description: str = "", **metadata: Any) -> Any:
    """Определяет поле события (например, нажатие кнопки, оповещение)."""
```

### `config`

```python
def config(default: Any = None, **metadata: Any) -> Any:
    """Определяет поле конфигурации."""
```

## Декоратор `command`

```python
def command(func: Any) -> Any:
    """Декоратор для методов класса устройства, которые должны быть доступны как команды RPC."""
```

## `Config`

Класс для управления конфигурацией приложения Synapse Core, поддерживающий загрузку из JSON-файлов и переопределение через переменные окружения.

```python
class Config:
    def __init__(self, config_path: Optional[str] = None):
        # ...

    def get(self, key: str, default: Any = None) -> Any:
        """Получает значение конфигурации по ключу. Приоритет: переменная окружения > файл конфигурации > значение по умолчанию."""
        # ...

    @property
    def mqtt_broker(self) -> str:
        """Возвращает адрес MQTT-брокера."""
        # ...

    @property
    def log_level(self) -> int:
        """Возвращает уровень логирования."""
        # ...
```

## `HADiscovery`

Класс для поддержки автоматического обнаружения устройств в Home Assistant через MQTT Discovery.

```python
class HADiscovery:
    def __init__(self, discovery_prefix: str = "homeassistant"):
        # ...

    async def announce(self, device: 'Device'):
        """Объявляет устройство в Home Assistant через MQTT."""
        # ...
```

## Драйверы (`synapse.drivers`)

Модуль `synapse.drivers` содержит базовый класс для аппаратных драйверов и различные реализации драйверов для взаимодействия с реальным оборудованием.

### `BaseDriver`

Абстрактный базовый класс, от которого должны наследоваться все драйверы.

```python
class BaseDriver(ABC):
    async def connect(self):
        """Устанавливает соединение с аппаратным обеспечением или сервисом."""
        pass

    async def disconnect(self):
        """Разрывает соединение с аппаратным обеспечением или сервисом."""
        pass

    async def execute(self, command_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Выполняет команду на аппаратном обеспечении или сервисе."""
        pass

    async def read(self, field_name: str) -> Any:
        """Считывает значение поля с аппаратного обеспечения или сервиса."""
        pass
```

### Реализованные драйверы:

*   **`MockHardwareDriver`**: Имитационный драйвер для тестирования и разработки.
*   **`GPIOChipDriver`**: Драйвер для работы с GPIO-чипами (требуется `gpiod`).
*   **`TelnetDriver`**: Драйвер для взаимодействия с устройствами по Telnet.
*   **`SerialDriver`**: Драйвер для работы с последовательными портами (требуется `pyserial`).
*   **`HTTPDeviceDriver`**: Драйвер для взаимодействия с HTTP/RESTful API (требуется `aiohttp`).

## Внутренние компоненты (`synapse.core`)

Модуль `synapse.core` содержит внутренние компоненты фреймворка. Хотя они не предназначены для прямого использования конечными пользователями, их понимание может быть полезно для расширенной разработки.

*   **`DeviceMeta`**: Метакласс, отвечающий за сбор метаданных полей и команд из классов устройств.
*   **`StateManager`**: Управляет состоянием всех зарегистрированных устройств.
*   **`CommandManager`**: Обрабатывает корреляцию команд и ответов.
*   **`RuleEngine`**: Движок для выполнения правил автоматизации.
*   **`DeviceRegistry`**: Хранит зарегистрированные классы и экземпляры устройств.
*   **`ServerNode`, `DeviceNode`**: Абстракции для взаимодействия с MQTT-брокером на стороне сервера и устройства соответственно.
*   **`Envelope`, `EnvelopeType`**: Определяют формат сообщений, используемых для внутренней коммуникации.
*   **`topics`**: Модуль для управления MQTT-топиками.
*   **`mixins`**: Содержит `TelemetryMixin` для периодической публикации телеметрии и `TaskManagerMixin` для управления фоновыми задачами.
*   **`handlers`**: Содержит `DeviceHandler`, который диспетчеризирует входящие сообщения MQTT для конкретного устройства.

---

*Автоматически сгенерировано Manus AI.*
