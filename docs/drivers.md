# Драйверы в Synapse Core v43

Synapse Core v43 предоставляет гибкий механизм для интеграции с различными аппаратными устройствами и внешними сервисами через систему драйверов. Драйверы позволяют абстрагироваться от низкоуровневых деталей взаимодействия с оборудованием, предоставляя унифицированный интерфейс для устройств Synapse.

## 1. Базовый класс драйвера: `BaseDriver`

Все драйверы должны наследоваться от абстрактного класса `BaseDriver`, определенного в `synapse.drivers.base`. Этот класс определяет основные методы, которые должен реализовать каждый драйвер.

```python
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

class BaseDriver(ABC):
    """Абстрактный базовый класс для всех драйверов Synapse."""
    def __init__(self):
        self.logger = logging.getLogger(f"synapse.driver.{self.__class__.__name__}")

    @abstractmethod
    async def connect(self):
        """Устанавливает соединение с аппаратным обеспечением или сервисом."""
        pass

    @abstractmethod
    async def disconnect(self):
        """Разрывает соединение с аппаратным обеспечением или сервисом."""
        pass

    @abstractmethod
    async def execute(self, command_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Выполняет команду на аппаратном обеспечении или сервисе."""
        pass

    @abstractmethod
    async def read(self, field_name: str) -> Any:
        """Считывает значение поля с аппаратного обеспечения или сервиса."""
        pass
```

## 2. Реализованные драйверы

Synapse Core v43 поставляется с несколькими готовыми драйверами для различных сценариев использования.

### 2.1. `MockHardwareDriver`

**Назначение**: Идеально подходит для тестирования и разработки, когда реальное оборудование недоступно. Позволяет имитировать поведение аппаратного обеспечения, включая задержки и сбои.

**Использование**:

```python
from synapse.drivers import MockHardwareDriver

driver = MockHardwareDriver(
    latency_range=(0.05, 0.2), # Имитация задержки от 50 до 200 мс
    failure_rate=0.1,          # 10% вероятность сбоя операций
    initial_state={"temperature": 25.0, "pressure": 1012.5}
)
# ... передать драйвер в Device
```

### 2.2. `GPIOChipDriver`

**Назначение**: Взаимодействие с GPIO-чипами на одноплатных компьютерах (например, Raspberry Pi, Orange Pi) с использованием современной библиотеки `gpiod`.

**Зависимости**: `pip install gpiod`

**Использование**:

```python
from synapse.drivers import GPIOChipDriver

driver = GPIOChipDriver(chip_path="/dev/gpiochip0") # Путь к GPIO-чипу
# ...

# Пример команды: установить выходной пин 17 в HIGH
await device.driver.execute("set_output", {"pin": 17, "value": True})

# Пример чтения: прочитать состояние входного пина 23
value = await device.driver.read("pin_23")
```

### 2.3. `TelnetDriver`

**Назначение**: Взаимодействие с устаревшим промышленным оборудованием (ПЛК, контроллеры, светодиодные дисплеи), которое поддерживает управление через Telnet.

**Использование**:

```python
from synapse.drivers import TelnetDriver

driver = TelnetDriver(host="192.168.1.100", port=23, timeout=5.0)
# ...

# Пример команды: отправить команду и дождаться ответа
response = await device.driver.execute("send_command", {"command": "STATUS\n", "wait_response": True})

# Пример чтения (через команду GET)
status = await device.driver.read("STATUS")
```

### 2.4. `SerialDriver`

**Назначение**: Связь через последовательные порты (RS-232/RS-485) с использованием библиотеки `pyserial`. Подходит для широкого спектра промышленных датчиков и исполнительных механизмов.

**Зависимости**: `pip install pyserial`

**Использование**:

```python
from synapse.drivers import SerialDriver

driver = SerialDriver(port="/dev/ttyUSB0", baudrate=9600, timeout=1.0)
# ...

# Пример команды: отправить данные и дождаться ответа
response = await device.driver.execute("send_data", {"data": "READ_TEMP\r\n", "wait_response": True})

# Пример чтения (через команду READ)
temp_value = await device.driver.read("TEMP")
```

### 2.5. `HTTPDeviceDriver`

**Назначение**: Взаимодействие с IP-камерами, умными дисплеями, а также любыми устройствами или сервисами, предоставляющими RESTful API. Использует `aiohttp` для асинхронных HTTP-запросов.

**Зависимости**: `pip install aiohttp`

**Использование**:

```python
from synapse.drivers import HTTPDeviceDriver

driver = HTTPDeviceDriver(base_url="http://192.168.1.10/api/v1", headers={"X-API-Key": "your_key"})
# ...

# Пример команды: отправить POST-запрос для включения света
result = await device.driver.execute("turn_on_light", {"method": "POST", "path": "lights/1/on"})

# Пример чтения: получить статус устройства
status_data = await device.driver.read("status")
```

## 3. Создание собственного драйвера

Для создания собственного драйвера необходимо:

1.  Создать новый класс, наследующийся от `BaseDriver`.
2.  Реализовать все абстрактные методы: `connect`, `disconnect`, `execute`, `read`.
3.  Инкапсулировать в драйвере всю специфическую логику взаимодействия с вашим оборудованием или сервисом.

Пример структуры:

```python
import asyncio
from synapse.drivers.base import BaseDriver

class MyCustomDriver(BaseDriver):
    def __init__(self, param1: str, param2: int):
        super().__init__()
        self.param1 = param1
        self.param2 = param2
        # Инициализация специфических для драйвера ресурсов

    async def connect(self):
        self.logger.info(f"Подключение к MyCustomDevice с {self.param1}")
        # Логика подключения
        await asyncio.sleep(0.5)
        self.logger.info("MyCustomDevice подключено.")

    async def disconnect(self):
        self.logger.info("Отключение от MyCustomDevice")
        # Логика отключения
        await asyncio.sleep(0.1)
        self.logger.info("MyCustomDevice отключено.")

    async def execute(self, command_name: str, params: dict) -> dict:
        self.logger.info(f"Выполнение команды {command_name} с {params}")
        # Логика выполнения команды
        if command_name == "set_value":
            value = params.get("value")
            # ... отправить команду на устройство
            return {"status": "ok", "set_value": value}
        raise NotImplementedError(f"Команда {command_name} не поддерживается")

    async def read(self, field_name: str) -> Any:
        self.logger.info(f"Чтение поля {field_name}")
        # Логика чтения значения поля
        if field_name == "sensor_data":
            return 123.45 # Имитация чтения данных
        return None
```

После реализации драйвера его можно использовать при создании экземпляра `Device`:

```python
from synapse import SynapseApp, Device
from my_drivers_module import MyCustomDriver

app = SynapseApp(...)

@app.device
class MySensorDevice(Device):
    data: float = telemetry()

# ...

async def main():
    custom_driver = MyCustomDriver(param1="abc", param2=123)
    sensor_device = await app.create_device("my_sensor_id", "mysensordevice", driver=custom_driver)
    await app.start()
    # ...
```

Использование драйверов позволяет создавать мощные и гибкие IoT-решения, легко адаптируемые к различным аппаратным платформам и протоколам. 
