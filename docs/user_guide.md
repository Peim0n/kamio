# Руководство пользователя Synapse Core v43

Это руководство поможет вам начать работу с Synapse Core v43, создавать собственные устройства, правила и приложения.

## 1. Основы Synapse Core

Synapse Core — это фреймворк, который упрощает разработку IoT-приложений, используя декларативный подход и протокол MQTT. Основные концепции:

*   **`SynapseApp`**: Главный класс вашего приложения. Он управляет подключением к MQTT-брокеру, регистрирует устройства и правила.
*   **`Device`**: Базовый класс для всех ваших IoT-устройств. Вы расширяете его, чтобы определить специфическое поведение и данные вашего устройства.
*   **Поля данных**: `telemetry`, `state`, `event`, `config` — это специальные функции, которые вы используете для декларативного определения характеристик вашего устройства.
    *   `telemetry`: Данные, которые устройство отправляет (например, температура, влажность).
    *   `state`: Данные, которые описывают текущее состояние устройства и могут быть изменены (например, включено/выключено, целевая температура).
    *   `event`: Одноразовые события, происходящие на устройстве (например, нажатие кнопки).
    *   `config`: Параметры конфигурации устройства.
*   **Команды (`@command`)**: Методы класса `Device`, которые могут быть вызваны удаленно через MQTT.
*   **Правила (`@app.rule`)**: Функции, которые автоматически выполняются в ответ на изменения телеметрии или состояния устройств, или по заданному интервалу.

## 2. Создание вашего первого приложения

Давайте создадим простое приложение, которое управляет умной лампочкой.

### Шаг 1: Установка

Убедитесь, что у вас установлен Synapse Core и MQTT-брокер (например, Mosquitto).

```bash
pip install synapse-core
sudo apt-get update
sudo apt-get install -y mosquitto mosquitto-clients
sudo service mosquitto start
```

### Шаг 2: Определение умной лампочки

Создайте файл `smart_light_app.py`:

```python
import asyncio
import logging
from synapse import SynapseApp, Device, command, state

# Инициализация приложения
app = SynapseApp(
    mqtt_broker="mqtt://localhost:1883",
    client_id="smart_light_controller",
    log_level=logging.INFO
)

@app.device
class SmartLight(Device):
    """Простая умная лампочка с управлением питанием и яркостью."""
    power: bool = state(default=False, writable=True, description="Состояние питания лампочки")
    brightness: int = state(default=100, min=0, max=100, writable=True, description="Яркость лампочки (0-100%)")

    @command
    async def toggle(self):
        """Переключает состояние питания лампочки."""
        self.power = not self.power
        self.logger.info(f"Лампочка переключена в состояние: {self.power}")
        await self.request_state_sync() # Отправляем обновление состояния на брокер
        return {"power": self.power}

    @command
    async def set_brightness(self, value: int):
        """Устанавливает яркость лампочки."""
        if not 0 <= value <= 100:
            raise ValueError("Яркость должна быть в диапазоне от 0 до 100")
        self.brightness = value
        self.logger.info(f"Яркость установлена на: {self.brightness}%")
        await self.request_state_sync()
        return {"brightness": self.brightness}

# Запуск приложения
if __name__ == "__main__":
    async def main():
        # Создаем экземпляр лампочки с ID "my_light"
        light_device = await app.create_device("my_light", "smartlight")
        await app.start()
        app.logger.info("Приложение Smart Light запущено. Ожидание команд...")

        # Пример вызова команды через приложение (обычно это делается извне)
        # await asyncio.sleep(5)
        # app.logger.info("Отправка команды: toggle")
        # await light_device.toggle()
        # await asyncio.sleep(2)
        # app.logger.info("Отправка команды: set_brightness(50)")
        # await light_device.set_brightness(50)

        while True:
            await asyncio.sleep(3600) # Работаем час

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        app.logger.info("Приложение остановлено пользователем.")
    finally:
        asyncio.run(app.stop())
```

### Шаг 3: Запуск приложения

```bash
python smart_light_app.py
```

Теперь ваше приложение запущено и готово принимать команды MQTT для `my_light`.

## 3. Использование правил автоматизации

Правила позволяют вашему приложению реагировать на изменения в IoT-системе. Давайте добавим правило, которое будет выключать лампочку, если она была включена более 10 минут.

Добавьте следующий код в `smart_light_app.py` после определения класса `SmartLight`:

```python
import time

# Словарь для отслеживания времени включения лампочек
light_on_times = {}

@app.rule(device=SmartLight, fields=["power"], description="Выключение лампочки после 10 минут")
async def auto_off_light(snapshot: dict, app_instance: SynapseApp):
    device_id = snapshot["device_id"]
    current_power_state = snapshot["update"].get("power")

    if current_power_state is True:
        light_on_times[device_id] = time.time()
        app_instance.logger.info(f"Лампочка {device_id} включена. Запущено отслеживание времени.")
    elif current_power_state is False and device_id in light_on_times:
        del light_on_times[device_id]
        app_instance.logger.info(f"Лампочка {device_id} выключена. Отслеживание остановлено.")

@app.rule(interval=60.0, description="Проверка времени работы лампочек") # Проверяем каждую минуту
async def check_long_on_lights(snapshot: dict, app_instance: SynapseApp):
    current_time = time.time()
    for device_id, on_time in list(light_on_times.items()): # Используем list() для безопасной итерации при удалении элементов
        if current_time - on_time > 600: # 600 секунд = 10 минут
            app_instance.logger.warning(f"Лампочка {device_id} включена более 10 минут. Выключаем.")
            # Находим экземпляр лампочки и выключаем ее
            light_instance = app_instance.devices.get(device_id)
            if light_instance and isinstance(light_instance, SmartLight):
                light_instance.power = False
                await light_instance.request_state_sync()
            del light_on_times[device_id]
```

Теперь, если лампочка `my_light` будет включена более 10 минут, правило `check_long_on_lights` автоматически выключит ее.

## 4. Расширенные возможности

### Драйверы устройств

Synapse Core поддерживает интеграцию с внешними драйверами для взаимодействия с реальным оборудованием. Вы можете определить свой драйвер, унаследовав его от `synapse.drivers.base.BaseDriver`.

```python
# Пример фиктивного драйвера
from synapse.drivers.base import BaseDriver

class MyHardwareDriver(BaseDriver):
    async def connect(self):
        self.logger.info("Подключение к оборудованию...")
        await asyncio.sleep(0.1) # Имитация асинхронного подключения
        self.logger.info("Оборудование подключено.")

    async def disconnect(self):
        self.logger.info("Отключение от оборудования...")
        await asyncio.sleep(0.1)
        self.logger.info("Оборудование отключено.")

    async def execute(self, command_name: str, params: dict) -> dict:
        self.logger.info(f"Выполнение команды {command_name} с параметрами {params}")
        if command_name == "set_power":
            # Логика управления реальным оборудованием
            return {"status": "ok", "power_set": params.get("value")}
        raise NotImplementedError(f"Команда {command_name} не поддерживается драйвером")

    async def read(self, field_name: str) -> Any:
        self.logger.info(f"Чтение значения поля {field_name}")
        if field_name == "temperature":
            return 25.5 # Имитация чтения с датчика
        return None
```

Вы можете передать экземпляр драйвера при создании устройства:

```python
# В вашем main-функции:
# ...
# from my_driver_module import MyHardwareDriver
# hardware_driver = MyHardwareDriver()
# my_device = await app.create_device("my_sensor", "sensor", driver=hardware_driver)
# ...
```

### Работа с MQTT-топиками

Synapse Core использует стандартизированные MQTT-топики для внутренней коммуникации. Все топики начинаются с `synapse/v1/{device_id}/{type}`. Например:

*   `synapse/v1/my_light/ds`: Для отправки состояния устройства `my_light`.
*   `synapse/v1/my_light/dt`: Для отправки телеметрии устройства `my_light`.
*   `synapse/v1/my_light/sc`: Для отправки команд на устройство `my_light`.

Вы можете использовать любой стандартный MQTT-клиент (например, `mosquitto_pub` или `mosquitto_sub`) для взаимодействия с вашим Synapse-приложением.

Пример отправки команды на `SmartLight` через `mosquitto_pub`:

```bash
mosquitto_pub -h localhost -t "synapse/v1/my_light/sc" -m '{"source": "external", "method": "toggle", "params": {}}'
```

Это вызовет метод `toggle` на экземпляре `SmartLight` с `device_id="my_light"`.

## 5. Управление конфигурацией и Home Assistant Discovery

### 5.1. Управление конфигурацией с помощью `Config`

Класс `Config` позволяет централизованно управлять настройками вашего приложения, используя JSON-файлы и переменные окружения. Это удобно для развертывания в различных средах (разработка, тестирование, продакшн).

**Пример использования `Config`:**

Создайте файл `config.json` в корне вашего проекта:

```json
{
    "mqtt_broker": "mqtt://my-production-broker:1883",
    "log_level": "DEBUG"
}
```

Затем в вашем приложении:

```python
from synapse import SynapseApp, Config
import logging

# Загрузка конфигурации из файла (если он существует)
app_config = Config(config_path="config.json")

app = SynapseApp(
    mqtt_broker=app_config.mqtt_broker,
    client_id="my_app",
    log_level=app_config.log_level
)

# Вы также можете переопределить настройки через переменные окружения:
# export SYNAPSE_MQTT_BROKER="mqtt://another-broker:1883"
# export SYNAPSE_LOG_LEVEL="WARNING"
# Эти переменные будут иметь приоритет над значениями из config.json.
```

### 5.2. Интеграция с Home Assistant Discovery

Synapse Core упрощает интеграцию с Home Assistant (HA) благодаря поддержке MQTT Discovery. Это позволяет вашим устройствам автоматически появляться в HA без ручной настройки.

**Пример использования `HADiscovery`:**

```python
import asyncio
from synapse import SynapseApp, Device, state
from synapse.discovery import HADiscovery

app = SynapseApp(
    mqtt_broker="mqtt://localhost:1883",
    client_id="ha_discovery_app"
)

# Инициализация Home Assistant Discovery
ha_discovery = HADiscovery(discovery_prefix="homeassistant")

@app.device
class SimpleSwitch(Device):
    power: bool = state(default=False, writable=True)

async def main():
    switch_device = await app.create_device("my_ha_switch", "simpleswitch")
    await app.start()
    
    # Объявление устройства в Home Assistant
    await ha_discovery.announce(switch_device)
    
    print("Устройство объявлено в Home Assistant. Проверьте интеграцию MQTT Discovery.")
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
```

После запуска этого примера, ваш `my_ha_switch` должен появиться в Home Assistant, если у вас настроена интеграция MQTT Discovery.

## 6. Устранение неполадок

*   **`ModuleNotFoundError: No module named 'synapse'`**: Убедитесь, что вы установили `synapse-core` (`pip install synapse-core`) и запускаете скрипт из корневой директории проекта или добавили ее в `PYTHONPATH`.
*   **`ConnectionRefusedError: [Errno 111] Connection refused`**: Убедитесь, что MQTT-брокер (Mosquitto) запущен и доступен по адресу `localhost:1883`.
*   **Команды не выполняются**: Проверьте логи вашего приложения. Убедитесь, что топик команды (`synapse/v1/{device_id}/sc`) указан правильно и формат сообщения соответствует ожидаемому (`{"source": "external", "method": "your_command", "params": {}}`).
*   **Правила не срабатывают**: Убедитесь, что `device` и `fields` в декораторе `@app.rule` указаны корректно, и что данные, на которые должно реагировать правило, действительно обновляются.

---

*Автоматически сгенерировано Manus AI.*
