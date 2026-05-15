# Полное руководство по возможностям Synapse Core v43

Это руководство подробно описывает все типы сообщений, способы создания устройств, работу с драйверами и конфигурацией.

## 1. Типы сообщений в MQTT

Synapse Core использует структурированную систему сообщений (Envelope), передаваемых через MQTT. Каждый тип сообщения имеет свое назначение:

| Суффикс топика | Тип сообщения | Описание |
| :--- | :--- | :--- |
| `/ds` | **Device State** | Обновление состояния устройства (state). Отправляется при изменении полей `state`. |
| `/dt` | **Device Telemetry** | Периодические данные (telemetry). Отправляется автоматически согласно `freq`. |
| `/de` | **Device Event** | Однократные события (event). Например, нажатие кнопки или срабатывание датчика. |
| `/sc` | **Server Command** | Команда от сервера к устройству. Инициирует вызов метода `@command`. |
| `/ca` | **Command Ack** | Ответ устройства на команду. Содержит результат выполнения или ошибку. |
| `/sa` | **State Ack** | Подтверждение изменения состояния от устройства серверу. |
| `/k` | **Keep-Alive** | Системное сообщение для проверки доступности устройства. |
| `/conf` | **Device Config** | Сообщение с конфигурационными параметрами устройства. |
| `/batch` | **Batch Message** | Пакетная передача нескольких сообщений (используется шлюзами). |

### Пример структуры сообщения (JSON):
```json
{
  "source": "device_id",
  "type": "ds",
  "payload": {
    "power": true,
    "brightness": 80
  },
  "timestamp": 1715683200.0
}
```

---

## 2. Создание устройств

### 2.1. Физические устройства (с драйвером)
Такие устройства напрямую взаимодействуют с оборудованием через драйвер.

```python
@app.device
class Thermostat(Device):
    temp: float = telemetry(freq="10s")
    target: float = state(default=22.0)
    
    @command
    async def set_target(self, value: float):
        self.target = value
        await self.request_state_sync()
        return {"status": "ok"}

# Создание с драйвером
dev = await app.create_device("living_room_th", "thermostat", driver=GPIOChipDriver())
```

### 2.2. Логические устройства (без драйвера)
Используются для агрегации данных, виртуальных выключателей или сложных сценариев.

```python
@app.device
class SecuritySystem(Device):
    armed: bool = state(default=False)
    alarm: bool = state(default=False)
```

---

## 3. Работа с полями данных

### `telemetry`
Используется для данных, которые меняются часто и должны отправляться периодически.
- `freq`: Интервал отправки (например, "1s", "5m", "1h").
- `unit`: Единица измерения.

### `state`
Используется для параметров, определяющих текущее состояние.
- `writable`: Можно ли менять это поле извне (через команды).
- `choices`: Список допустимых значений.

### `event`
Используется для мгновенных уведомлений.
- Вызывается через `await self.emit("event_name", payload)`.
- Топик: `synapse/v1/{device_id}/de`.

### `keepalive`
Системный механизм для мониторинга доступности устройств.
- Устройства периодически отправляют пустые сообщения в топик `synapse/v1/{device_id}/k`.
- Сервер отслеживает время последнего сообщения. Если сообщений нет дольше заданного порога, устройство считается `offline`.
- В `SynapseApp` интервал keepalive настраивается при инициализации.

### `config`
Параметры, которые задаются при инициализации и не меняются в процессе работы (например, калибровочные коэффициенты).

---

## 4. Конфигурация приложения

Библиотека поддерживает загрузку настроек из `config.json` и переменных окружения.

### Приоритет настроек:
1. Переменные окружения (например, `SYNAPSE_MQTT_BROKER`).
2. Файл `config.json`.
3. Значения по умолчанию в коде.

```python
from synapse import Config
cfg = Config("my_config.json")
app = SynapseApp(mqtt_broker=cfg.mqtt_broker)
```

---

## 5. Разработка собственных драйверов

Для создания драйвера нужно унаследовать `BaseDriver` и реализовать методы:

```python
from synapse.drivers.base import BaseDriver

class MyDriver(BaseDriver):
    async def connect(self):
        # Логика открытия порта/соединения
        pass

    async def read(self, field_name: str):
        # Логика чтения данных из железа
        pass

    async def execute(self, command_name: str, params: dict):
        # Логика выполнения физической команды
        pass
```

---

## 6. Продвинутая конфигурация и правила

### 6.1. Сложные правила (Rules)
Правила могут реагировать на несколько полей или работать по таймеру.

```python
@app.rule(device=AdvancedDevice, fields=["temperature", "humidity"])
async def climate_control(snapshot: dict, app: SynapseApp):
    # snapshot содержит текущие значения всех полей устройства
    temp = snapshot["state"]["temperature"]
    hum = snapshot["state"]["humidity"]
    
    if temp > 30 and hum > 70:
        print("Слишком жарко и влажно!")

@app.rule(interval=3600.0)
async def hourly_report(snapshot: dict, app: SynapseApp):
    # Выполняется каждый час
    print("Генерация отчета...")
```

### 6.2. Использование TaskManagerMixin
Если вашему устройству нужно выполнять фоновые задачи (например, опрос датчика в цикле), используйте `TaskManagerMixin`.

```python
class PollingDevice(Device):
    async def on_start(self, node):
        self.create_task(self._poll_loop())

    async def _poll_loop(self):
        while True:
            # Логика опроса
            await asyncio.sleep(1)
```

## 7. Продвинутый пример: Система управления Telnet-камерами

Этот пример демонстрирует, как создать драйвер для устройства, взаимодействующего по Telnet, как конфигурировать его через JSON, и как реализовать логическое устройство-менеджер для управления группой таких устройств.

### 7.1. Telnet-драйвер для камеры (`TelnetCameraDriver`)

Драйвер `TelnetCameraDriver` наследуется от `BaseDriver` и реализует логику подключения к Telnet-серверу камеры и отправки команд. Адрес и порт Telnet-соединения передаются драйверу при его инициализации.

```python
class TelnetCameraDriver(BaseDriver):
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        # ... логика подключения и отправки команд ...

    async def connect(self):
        # Имитация подключения к Telnet
        print(f"[TelnetDriver] Подключение к {self.host}:{self.port}...")
        await asyncio.sleep(0.5)
        print(f"[TelnetDriver] Соединение установлено.")

    async def execute(self, command_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        # Имитация отправки команды по Telnet
        cmd_str = f"{command_name} " + " ".join([f"{k}={v}" for k, v in params.items()])
        print(f"[TelnetDriver] Отправка в Telnet: {cmd_str}")
        await asyncio.sleep(0.1)
        return {"status": "ok", "raw_response": "ACK"}
```

### 7.2. Устройство Камера (`TelnetCamera`)

Устройство `TelnetCamera` представляет собой физическую камеру. Оно использует поля `config` для получения адреса и порта Telnet, а также поля `state` для отслеживания текущего пресета и статуса онлайн. Команда `move_to_preset` вызывает метод `execute` у привязанного драйвера.

```python
@SynapseApp.device
class TelnetCamera(Device):
    host: str = config(default="127.0.0.1")
    port: int = config(default=23)
    preset: int = state(default=1, writable=True)

    @command
    async def move_to_preset(self, preset_id: int):
        result = await self.driver.execute("GOTO_PRESET", {"id": preset_id})
        if result["status"] == "ok":
            self.preset = preset_id
            await self.request_state_sync()
            return {"status": "moved", "preset": self.preset}
        return {"status": "error"}
```

### 7.3. Менеджер Камер (`CameraManager`)

`CameraManager` — это логическое устройство, которое не имеет собственного драйвера, но управляет группой `TelnetCamera` устройств. Оно позволяет определять "сцены", где каждая сцена соответствует определенному набору пресетов для всех камер. Менеджер получает доступ к другим устройствам через `self.node.app.devices`.

```python
@SynapseApp.device
class CameraManager(Device):
    active_scene: str = state(default="idle", writable=True)

    @command
    async def set_scene(self, scene_name: str):
        self.active_scene = scene_name
        await self.request_state_sync()
        
        # Пример: Сцена "Конференция" - направляем все камеры на стол
        if scene_name == "conference":
            for dev_id, device in self.node.app.devices.items():
                if isinstance(device, TelnetCamera):
                    await device.move_to_preset(1)
        # ... другие сцены ...
        return {"scene": scene_name, "status": "applied"}
```

### 7.4. Конфигурация через JSON

Для удобства развертывания и управления, параметры камер и сцены могут быть определены в JSON-файле (например, `camera_config.json`). Приложение `SynapseApp` может загружать этот файл и динамически создавать устройства и настраивать их.

Пример `camera_config.json`:

```json
{
  "mqtt_broker": "mqtt://localhost:1883",
  "log_level": "INFO",
  "cameras": [
    {"id": "cam_hallway", "host": "10.0.0.50", "port": 23},
    {"id": "cam_meeting_room", "host": "10.0.0.51", "port": 23}
  ],
  "scenes": {
    "all_home": {"cam_hallway": 1, "cam_meeting_room": 1},
    "meeting": {"cam_hallway": 2, "cam_meeting_room": 5}
  }
}
```

В основном скрипте приложения (`telnet_camera_system.py`) этот JSON-файл читается, и на его основе создаются экземпляры `TelnetCamera` с соответствующими `TelnetCameraDriver`.

```python
# Загрузка конфига (имитация)
config_data = {
    "cameras": [
        {"id": "cam_north", "host": "192.168.1.10", "port": 2323},
        {"id": "cam_south", "host": "192.168.1.11", "port": 2323}
    ]
}

app = SynapseApp(mqtt_broker="mqtt://localhost:1883")

for cam_cfg in config_data["cameras"]:
    driver = TelnetCameraDriver(host=cam_cfg["host"], port=cam_cfg["port"])
    await app.create_device(
        device_id=cam_cfg["id"],
        device_type="telnetcamera",
        driver=driver,
        host=cam_cfg["host"],
        port=cam_cfg["port"]
    )

manager = await app.create_device("global_cam_manager", "cameramanager")
```

Этот пример демонстрирует мощь Synapse Core в разделении логики устройств и их физической реализации, а также гибкость в управлении сложными системами через логические менеджеры и внешнюю конфигурацию.

## 8. Продвинутые возможности: Keep-Alive, Авторизация, Наследование Команд и Валидация Ответов (Реализация через Наследование)

Как было отмечено, Synapse Core спроектирован как минималистичный и универсальный фреймворк. Это означает, что такие продвинутые механизмы, как настраиваемый Keep-Alive, автоматическое переподключение драйверов с авторизацией, расширение наборов команд и продвинутая валидация ответов, должны реализовываться не в ядре библиотеки, а через **наследование и расширение существующих классов `Device` и `BaseDriver`**.

Такой подход сохраняет чистоту и гибкость ядра, позволяя разработчикам адаптировать поведение под свои специфические нужды, не "загрязняя" базовые абстракции.

Рассмотрим, как реализовать эти возможности, используя пример `examples/custom_logic_via_inheritance.py`.

### 8.1. Расширенный Драйвер с Авторизацией и Переподключением (`RobustDriver`)

Вместо того чтобы добавлять логику переподключения и авторизации в `BaseDriver`, мы создаем специализированный драйвер `RobustDriver`, который наследуется от `BaseDriver` и инкапсулирует эту логику.

```python
class RobustDriver(BaseDriver):
    def __init__(self, host: str, token: str):
        super().__init__()
        self.host = host
        self.token = token
        self.is_connected = False
        self.is_authenticated = False

    async def connect(self):
        self.logger.info(f"Подключение к {self.host}...")
        await asyncio.sleep(0.5) # Имитация сети
        self.is_connected = True
        
        if self.token == "secret123":
            self.is_authenticated = True
            self.logger.info("Авторизация успешна")
        else:
            self.is_authenticated = False
            self.logger.error("Ошибка авторизации")

    async def disconnect(self):
        self.is_connected = False
        self.is_authenticated = False
        self.logger.info("Отключено")

    async def execute(self, command_name: str, params: Dict[str, Any]) -> Any:
        if not self.is_connected or not self.is_authenticated:
            self.logger.warning("Нет связи или авторизации, попытка быстрого переподключения...")
            await self.connect() # Попытка переподключения
            if not self.is_authenticated:
                raise ConnectionError("Драйвер не авторизован")
        
        self.logger.info(f"Выполнение: {command_name}")
        return {"status": "ok", "data": "ACK"}

    async def read(self) -> Dict[str, Any]:
        return {"status": "ok"}
```

Здесь `RobustDriver` самостоятельно управляет своим состоянием `is_connected` и `is_authenticated`, а также реализует логику переподключения при попытке выполнения команды, если соединение отсутствует.

### 8.2. Настраиваемый Keep-Alive на Уровне Устройства (`SmartDevice`)

Для реализации настраиваемого Keep-Alive мы создаем промежуточный класс `SmartDevice`, который наследуется от `Device` и использует `TaskManagerMixin` (уже включенный в `Device`) для запуска фоновой задачи отправки Keep-Alive сообщений.

```python
class SmartDevice(Device):
    def __init__(self, ka_interval: float = 10.0, **kwargs):
        super().__init__(**kwargs)
        self.ka_interval = ka_interval

    async def on_start(self, node):
        await super().on_start(node)
        if self.ka_interval > 0:
            self.create_task(self._custom_keepalive_loop())

    async def _custom_keepalive_loop(self):
        from synapse.core.envelope import Envelope
        while self.node and self.node.is_running:
            try:
                env = Envelope.keepalive(source=self.node.device_id)
                await self.node.publish(env)
                self.logger.debug("Custom Keep-Alive sent")
                await asyncio.sleep(self.ka_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Keep-Alive error: {e}")
                await asyncio.sleep(5)
```

Теперь любое устройство, наследующее `SmartDevice`, может задать свой интервал Keep-Alive через параметр `ka_interval` в конструкторе.

### 8.3. Наследование Команд и Продвинутая Валидация Ответов (`AdvancedMotor`)

Класс `AdvancedMotor` наследует `SmartDevice` и демонстрирует, как можно расширять набор команд и выполнять валидацию ответов от драйвера перед обновлением состояния устройства.

```python
@SynapseApp.device
class AdvancedMotor(SmartDevice):
    speed: int = state(default=0, writable=True)

    @command
    async def set_speed(self, value: int):
        res = await self.driver.execute("set_speed", {"v": value})
        if res.get("status") == "ok":
            self.speed = value
            await self.request_state_sync() # Отправка подтверждения (ACK) состояния
            return {"result": "success"}
        return {"result": "fail"}
```

Здесь `set_speed` вызывает `execute` драйвера, валидирует его ответ и только в случае успеха обновляет `state` и отправляет `state_sync`.

### 8.4. Логическое Устройство-Менеджер для Группы Устройств

Пример `LineController` остается актуальным и демонстрирует, как логическое устройство может управлять группой физических устройств, вызывая их команды.

```python
@SynapseApp.device
class LineController(Device):
    line_status: str = state(default="stopped")

    @command
    async def emergency_stop(self):
        self.line_status = "emergency"
        await self.request_state_sync()
        
        for dev_id, device in self.node.app.devices.items():
            if isinstance(device, AdvancedMotor):
                await device.set_speed(0) # Вызов команды другого устройства
        
        return {"result": "all_stopped"}
```

Этот подход позволяет создавать мощные и гибкие IoT-решения, сохраняя при этом ядро Synapse Core чистым и универсальным. Вся сложная логика инкапсулируется в пользовательских классах, что упрощает поддержку и расширение системы.

### 8.5. Продвинутое управление параметрами и обратная связь (Пример Коммутатора ATEN VP2420)

Рассмотрим пример реализации устройства **ATEN VP2420** (`examples/aten_vp2420_system.py`), который демонстрирует полный цикл взаимодействия с реальным оборудованием, включая команды, состояние, события, гибкий Keep-Alive и конфигурацию.

#### 8.5.1. Объявление и инициализация драйвера

Драйвер (`ATEN_VP2420_Driver`) объявляется и инициализируется внутри метода `on_init` устройства `ATEN_VP2420`. Это позволяет использовать параметры конфигурации устройства (например, `host`, `port`, `auth_token`) для создания экземпляра драйвера.

```python
# В классе устройства ATEN_VP2420
class ATEN_VP2420(Device):
    host: str = config(default="192.168.1.100")
    port: int = config(default=23)
    auth_token: Optional[str] = config(default=None)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.driver: Optional[ATEN_VP2420_Driver] = None

    async def on_init(self, **kwargs):
        self.logger.info(f"Инициализация драйвера для {self.host}:{self.port}")
        self.driver = ATEN_VP2420_Driver(host=self.host, port=self.port, auth_token=self.auth_token)
        try:
            await self.driver.connect()
            self.logger.info(f"Драйвер {self.device_id} успешно подключен.")
        except Exception as e:
            self.logger.error(f"Ошибка подключения драйвера {self.device_id}: {e}")
            await self.emit("device_error", f"Connection failed: {e}")
```

#### 8.5.2. Гибкий Keep-Alive с опросом железа

Вместо того чтобы просто отправлять системное сообщение Keep-Alive, устройство `ATEN_VP2420` реализует метод `perform_keepalive_check`, который активно опрашивает реальное оборудование через драйвер. Это позволяет не только подтвердить доступность устройства, но и получить актуальное состояние, а также попытаться переподключиться в случае потери связи.

```python
# В классе устройства ATEN_VP2420
async def _vp2420_keepalive_loop(self):
    while self.node and self.node.is_running:
        try:
            await self.perform_keepalive_check() # Вызов метода устройства
            await asyncio.sleep(self.keepalive_interval)
        except Exception as e:
            self.logger.error(f"Keep-Alive Loop Error: {e}")
            await asyncio.sleep(5)

async def perform_keepalive_check(self):
    from synapse.core.envelope import Envelope
    if not self.driver or not self.driver.is_connected:
        self.logger.warning(f"Драйвер {self.device_id} не подключен. Попытка переподключения...")
        try:
            await self.driver.connect()
        except Exception as e:
            self.logger.error(f"Не удалось переподключиться к драйверу {self.device_id}: {e}")
            await self.emit("device_error", f"Reconnect failed: {e}")
            return

    try:
        status = await self.driver.read_status()
        if status:
            # Обновляем локальное состояние на основе ответа от железа
            self.active_input = status.get("video_input", self.active_input)
            self.mute_state = status.get("audio_mute", self.mute_state)
            self.display_mode = status.get("display_mode", self.display_mode)
            
            # Отправляем системный Keep-Alive (тип 'k')
            env = Envelope.keepalive(source=self.node.device_id)
            await self.node.publish(env)
            
            # Синхронизируем состояние с MQTT (чтобы сервер видел актуальные данные)
            await self.request_state_sync()
            self.logger.debug(f"Keep-Alive для {self.device_id} успешно выполнен. Статус: {status}")
        else:
            self.logger.warning(f"Keep-Alive для {self.device_id}: Драйвер не вернул статус.")
            await self.emit("device_error", "Keep-Alive: No status from driver")
    except Exception as e:
        self.logger.error(f"Ошибка при выполнении Keep-Alive для {self.device_id}: {e}")
        await self.emit("device_error", f"Keep-Alive check failed: {e}")
```

#### 8.5.3. Команды с валидацией и подтверждением (ACK)

Каждая команда устройства (`@command`) взаимодействует с драйвером, обрабатывает его ответ, обновляет внутреннее состояние устройства и отправляет подтверждение (ACK) или событие ошибки. Например, команда `switch_input`:

```python
# В классе устройства ATEN_VP2420
@command
async def switch_input(self, input_source: str, output_port: str = VP2420Commands.Ports.VIDEO_OUTPUT_1):
    if input_source not in VP2420Commands.Ports.VIDEO_INPUTS:
        await self.emit("device_error", f"Invalid input source: {input_source}")
        return {"status": "error", "message": f"Неверный вход: {input_source}"}

    res = await self.driver.execute("switch", {"input_source": input_source, "output_port": output_port})
    if res.get("status") == "success" and res.get("response") == "OK":
        self.active_input = input_source
        await self.request_state_sync() # Отправляем State Ack
        await self.emit("input_switched", {"input": input_source, "output": output_port})
        return {"status": "success", "input": input_source, "ack": True}
    
    await self.emit("device_error", f"Switch command failed: {res.get("response")}")
    return {"status": "error", "message": res.get("response"), "ack": False}
```

#### 8.5.4. Различные типы событий

Устройство может генерировать различные события для уведомления о важных изменениях или ошибках. Например, `device_error` для проблем с подключением или `input_switched` для успешного переключения входа.

```python
# В классе устройства ATEN_VP2420
device_error = event(python_type=str) # Событие ошибки
input_switched = event(python_type=dict) # Событие переключения входа
```

#### 8.5.5. Прямое взаимодействие между устройствами (D2D)

Логическое устройство `PresentationManager` демонстрирует, как можно управлять физическими коммутаторами `ATEN_VP2420` напрямую, вызывая их команды. Это позволяет создавать сложные сценарии автоматизации без лишнего трафика через MQTT.

```python
# В логическом устройстве PresentationManager
@command
async def start_presentation(self, presentation_name: str, main_switcher_id: str):
    switcher: ATEN_VP2420 = self.node.app.get_device(main_switcher_id) # Получаем объект устройства
    if switcher:
        if presentation_name == "meeting_room_A":
            await switcher.switch_input(input_source=VP2420Commands.Ports.VIDEO_INPUT_1)
            await switcher.set_mute(state="off")
        # ... другие сценарии ...
```

#### 8.5.6. Пример конфигурации (JSON)

Конфигурация для ATEN VP2420 и менеджера презентаций может выглядеть следующим образом (`examples/vp2420_config.json`):

```json
{
    "mqtt_broker": "mqtt://localhost:1883",
    "app_name": "VP2420_Controller",
    "log_level": "INFO",
    "devices": {
        "switcher_hall_1": {
            "type": "aten_vp2420",
            "config": {
                "host": "192.168.10.50",
                "port": 23,
                "keepalive_interval": 15.0,
                "auth_token": "my_secret_token_1"
            }
        },
        "presentation_manager_main": {
            "type": "presentationmanager"
        }
    }
}
```

Этот пример демонстрирует, как все эти концепции объединяются для создания мощного и гибкого решения на базе Synapse Core.

## 9. Распределенная архитектура: Несколько приложений на одной шине

Synapse Core позволяет запускать несколько независимых приложений (`SynapseApp`), которые могут взаимодействовать друг с другом через общую шину MQTT. Это мощный механизм для создания распределенных систем, где каждое приложение отвечает за свой набор устройств или логику, но при этом может обмениваться данными и командами с другими.

### 9.1. Принцип работы

Каждый экземпляр `SynapseApp` подключается к одному и тому же MQTT-брокеру. Устройства, принадлежащие разным приложениям, публикуют свои состояния, телеметрию и события в стандартные топики Synapse Core (`synapse/v1/{device_id}/...`). Таким образом, одно приложение может:

*   **Отправлять команды** (`/sc`) устройствам, которые управляются другим приложением.
*   **Подписываться на состояния** (`/ds`), **телеметрию** (`/dt`) или **события** (`/de`) устройств из других приложений.

Ключевым моментом является использование уникальных `client_id` для каждого `SynapseApp` при подключении к MQTT-брокеру, чтобы избежать конфликтов.

### 9.2. Единая конфигурация для нескольких приложений

Вы можете использовать единый JSON-файл конфигурации для определения всех приложений и их устройств. Это упрощает управление и развертывание системы.

Пример `unified_system_config.json` (`examples/unified_system_config.json`):

```json
{
    "mqtt_broker": "mqtt://localhost:1883",
    "apps": {
        "video_service": {
            "app_name": "VideoService",
            "devices": {
                "cam_hall": {
                    "type": "advancedcamera",
                    "config": {"model": "Pro-X"}
                },
                "switcher_main": {
                    "type": "aten_vp2420",
                    "config": {"host": "192.168.1.50"}
                }
            }
        },
        "security_service": {
            "app_name": "SecurityService",
            "devices": {
                "alarm_panel": {
                    "type": "securitycontroller"
                }
            }
        }
    }
}
```

В этом примере определены два логических приложения: `video_service` (с камерой и коммутатором) и `security_service` (с панелью сигнализации). Оба приложения будут использовать один и тот же MQTT-брокер.

### 9.3. Пример взаимодействия между приложениями (`examples/multi_app_system.py`)

Скрипт `multi_app_system.py` демонстрирует, как запустить два `SynapseApp` в одном процессе и как устройство из одного приложения может отправлять команды устройству из другого приложения.

```python
# Устройство в одном приложении, которое управляет устройством в ДРУГОМ приложении
# через общую шину MQTT.
@SynapseApp.device
class MasterController(Device):
    @command
    async def trigger_remote_action(self, target_device_id: str, action: bool):
        print(f"[{self.device_id}] Отправка команды на удаленное устройство {target_device_id}...")
        
        from synapse.core.envelope import Envelope
        
        cmd_payload = {"method": "set_power", "params": {"value": action}}
        env = Envelope(
            source=self.node.device_id,
            type="sc", # Server Command
            payload=cmd_payload
        )
        
        # Публикуем в топик целевого устройства
        target_topic = f"synapse/v1/{target_device_id}/sc"
        await self.node.publish_raw(target_topic, env.to_json())
        
        return {"status": "command_sent_to_mqtt"}

# ... (код запуска двух приложений)

# Демонстрация взаимодействия
master = app_control.get_device("master_ctrl")
await master.trigger_remote_action(target_device_id="light_01", action=True)
```

В этом примере `MasterController` (из `ControlApp`) отправляет команду `set_power` устройству `RemoteControlledLight` (из `VideoApp`). Команда формируется как обычное сообщение `Server Command` (`/sc`) и публикуется в соответствующий топик MQTT. `VideoApp` получает это сообщение, и его `RemoteControlledLight` обрабатывает команду.

Этот подход позволяет строить масштабируемые и отказоустойчивые системы, где каждое приложение может быть развернуто независимо, но при этом эффективно взаимодействовать с другими компонентами системы через MQTT.

## 10. Универсальные устройства и динамическая загрузка драйверов (Фабрика Драйверов)

Часто возникает необходимость управлять различными моделями устройств одного типа (например, разные модели камер) через единый интерфейс. Synapse Core позволяет реализовать это с помощью **фабрики драйверов** и универсального класса устройства.

### 10.1. Фабрика Драйверов (`DriverFactory`)

`DriverFactory` — это вспомогательный класс, который динамически выбирает и создает экземпляр нужного драйвера на основе переданного идентификатора (например, строки `model`). Это позволяет абстрагировать логику выбора драйвера от самого устройства.

Пример `DriverFactory` (`examples/universal_device_demo.py`):

```python
# Вспомогательный класс для динамической загрузки драйверов
class DriverFactory:
    _drivers: Dict[str, Type[BaseDriver]] = {
        "mock": MockHardwareDriver,
        "aten_vp2420": ATEN_VP2420_Driver,
        # Здесь можно добавить другие драйверы
    }

    @classmethod
    def create(cls, model: str, **kwargs) -> BaseDriver:
        driver_class = cls._drivers.get(model.lower())
        if not driver_class:
            raise ValueError(f"Драйвер для модели \'{model}\' не найден.")
        return driver_class(**kwargs)
```

### 10.2. Универсальное Устройство (`CleverCam`)

Класс `CleverCam` представляет собой универсальное устройство, которое в своем методе `on_init` использует `DriverFactory` для создания соответствующего драйвера на основе значения поля `model` из своей конфигурации. Таким образом, одно и то же устройство `CleverCam` может работать с разными физическими камерами, просто меняя параметр `model` в конфиге.

Пример `CleverCam` (`examples/universal_device_demo.py`):

```python
@SynapseApp.device
class CleverCam(Device):
    model: str = config(default="mock")
    host: str = config(default="127.0.0.1")
    status: str = state(default="idle")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.driver: Optional[BaseDriver] = None

    async def on_init(self, **kwargs):
        self.logger.info(f"Загрузка драйвера для модели: {self.model}")
        try:
            self.driver = DriverFactory.create(self.model, host=self.host)
            await self.driver.connect()
        except Exception as e:
            self.logger.error(f"Ошибка инициализации драйвера: {e}")

    @command
    async def capture(self):
        if self.driver:
            res = await self.driver.execute("read", {"field_name": "image"})
            self.status = "captured"
            await self.request_state_sync()
            return {"status": "success", "data": res}
        return {"status": "error", "message": "Драйвер не инициализирован"}
```

### 10.3. Использование в приложении

В основном приложении вы просто создаете экземпляры `CleverCam`, указывая нужную модель в их конфигурации:

```python
# В основном приложении
app = SynapseApp(mqtt_broker="mqtt://localhost:1883")

# Камера с Mock драйвером
cam1 = await app.create_device("cam_01", "clevercam")
await cam1.handle_config({"model": "mock", "host": "localhost"})

# Камера с драйвером ATEN
cam2 = await app.create_device("cam_02", "clevercam")
await cam2.handle_config({"model": "aten_vp2420", "host": "192.168.1.100"})
```

Этот подход значительно упрощает управление разнородным оборудованием и позволяет легко добавлять поддержку новых моделей без изменения логики самого устройства.

## 11. Архитектура шлюзов (Gateway) и сбор данных с датчиков

Для систем с большим количеством локальных датчиков или устройств, которые не могут напрямую подключаться к MQTT-брокеру, эффективно использовать архитектуру шлюзов. **Шлюз (Gateway)** — это отдельное приложение Synapse Core, которое работает на локальном контроллере (например, Raspberry Pi) и собирает данные с подключенных к нему датчиков, а затем передает их в основную систему через MQTT.

### 11.1. Датчики (`TemperatureSensor`)

Простые устройства-датчики, которые могут быть подключены к шлюзу. В примере `examples/sensor_gateway_system.py` используется `TemperatureSensor`, который имитирует генерацию данных.

```python
@SynapseApp.device
class TemperatureSensor(Device):
    value: float = telemetry(freq="5s", unit="C")
    
    async def on_start(self, node):
        self.create_task(self._simulate_data())
        
    async def _simulate_data(self):
        while True:
            self.value = round(random.uniform(20.0, 25.0), 2)
            await asyncio.sleep(5)
```

### 11.2. Устройство-шлюз (`SensorGateway`)

`SensorGateway` — это логическое устройство, которое работает в приложении-шлюзе. Оно не имеет собственного физического драйвера, но его задача — отслеживать и агрегировать данные с других устройств (датчиков), запущенных в том же приложении. Шлюз может публиковать агрегированные данные или выполнять локальную логику.

Пример `SensorGateway` (`examples/sensor_gateway_system.py`):

```python
@SynapseApp.device
class SensorGateway(Device):
    active_sensors_count: int = state(default=0)
    last_aggregated_data: dict = state(default={})

    async def on_start(self, node):
        self.create_task(self._aggregation_loop())

    async def _aggregation_loop(self):
        while True:
            sensors_data = {}
            count = 0
            
            for dev_id, device in self.node.app.devices.items():
                if isinstance(device, TemperatureSensor):
                    sensors_data[dev_id] = device.value
                    count += 1
            
            self.active_sensors_count = count
            self.last_aggregated_data = sensors_data
            
            if count > 0:
                self.logger.info(f"[Gateway] Собраны данные с {count} датчиков: {sensors_data}")
                await self.request_state_sync()
            
            await asyncio.sleep(10)
```

### 11.3. Запуск системы шлюза

Приложение-шлюз запускается как обычное приложение Synapse Core, но его фокус — на локальном сборе данных.

```python
# В основном приложении шлюза
app = SynapseApp(mqtt_broker="mqtt://localhost:1883", app_name="SensorGatewayNode")

# Создаем группу датчиков
for i in range(1, 4):
    await app.create_device(f"temp_sensor_{i}", "temperaturesensor")

# Создаем шлюз
await app.create_device("main_gateway", "sensorgateway")

await app.start()
```

Эта архитектура позволяет эффективно управлять большим количеством датчиков, снижать нагрузку на центральный MQTT-брокер за счет локальной агрегации и обработки, а также обеспечивать отказоустойчивость на уровне локального узла.

## 12. Home Assistant Discovery

Для автоматического добавления в Home Assistant:

```python
from synapse.discovery import HADiscovery
ha = HADiscovery()
await ha.announce(my_device)
```
Это создаст необходимые конфигурационные топики в MQTT, и устройство появится в интерфейсе HA автоматически.
