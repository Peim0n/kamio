import asyncio
import logging
import time
from typing import Any, Dict, Optional
from synapse import SynapseApp, Device, command, state, config, telemetry
from synapse.drivers.base import BaseDriver

# 1. Продвинутый Драйвер с Авторизацией и Валидацией
class IndustrialDriver(BaseDriver):
    def __init__(self, host: str, token: str):
        super().__init__(auth_token=token)
        self.host = host
        self.is_connected = False
        self.is_authenticated = False

    async def connect(self):
        print(f"[Driver] Подключение к {self.host}...")
        await asyncio.sleep(0.5)
        self.is_connected = True
        print(f"[Driver] Соединение установлено.")

    async def authenticate(self) -> bool:
        print(f"[Driver] Авторизация с токеном {self.auth_token[:4]}***")
        await asyncio.sleep(0.3)
        if self.auth_token == "secret123":
            self.is_authenticated = True
            print(f"[Driver] Авторизация успешна.")
            return True
        print(f"[Driver] Ошибка авторизации!")
        return False

    async def execute(self, command_name: str, params: Dict[str, Any]) -> Any:
        if not self.is_connected or not self.is_authenticated:
            raise ConnectionError("Драйвер не готов (нет связи или авторизации)")
        
        print(f"[Driver] Выполнение: {command_name}({params})")
        await asyncio.sleep(0.1)
        
        # Имитация ответа от железа
        if command_name == "set_speed":
            return {"code": 200, "msg": "OK", "new_speed": params.get("value")}
        return {"code": 404, "msg": "Unknown Command"}

    async def read(self) -> Dict[str, Any]:
        return {"load": 45.5, "temp": 60.2}

# 2. Базовый класс промышленного устройства
class BaseIndustrialDevice(Device):
    """Базовый класс с общими параметрами для всех пром. устройств."""
    
    # Настройка Keep-Alive для всех наследников
    keepalive_interval: float = 15.0 
    
    # Общие поля
    firmware: str = config(default="v1.0.0")
    error_count: int = state(default=0)

    @command
    async def reset_errors(self):
        self.error_count = 0
        await self.request_state_sync()
        return {"status": "reset"}

# 3. Наследник: Конкретное устройство (Мотор)
@SynapseApp.device
class IndustrialMotor(BaseIndustrialDevice):
    """Устройство 'Мотор', наследующее базовые пром. функции."""
    
    # Переопределяем интервал Keep-Alive для мотора
    keepalive_interval: float = 5.0 
    
    # Специфичные поля
    speed: int = state(default=0, writable=True, min=0, max=3000)
    load: float = telemetry(freq="2s")

    @command
    async def set_speed(self, value: int):
        """Команда с валидацией ответа от драйвера."""
        # 1. Вызов драйвера
        raw_response = await self.driver.execute("set_speed", {"value": value})
        
        # 2. Валидация ответа (бизнес-логика)
        if raw_response.get("code") == 200:
            # 3. Подтверждение (ACK) - обновляем состояние только после успеха
            self.speed = raw_response["new_speed"]
            await self.request_state_sync()
            print(f"[Motor] Скорость подтверждена: {self.speed}")
            return {"status": "success", "confirmed_speed": self.speed}
        else:
            self.error_count += 1
            await self.request_state_sync()
            return {"status": "error", "reason": raw_response.get("msg")}

# 4. Логическое устройство: Контроллер Линии
@SynapseApp.device
class LineController(Device):
    """Логический контроллер, управляющий группой моторов."""
    
    line_status: str = state(default="stopped")

    @command
    async def emergency_stop(self):
        """Вызывает команды у всех моторов на линии."""
        print("[Controller] ЭКСТРЕННАЯ ОСТАНОВКА ЛИНИИ!")
        self.line_status = "emergency"
        await self.request_state_sync()
        
        # Поиск всех моторов в приложении
        for dev_id, device in self.node.app.devices.items():
            if isinstance(device, IndustrialMotor):
                print(f"[Controller] Останавливаем мотор: {dev_id}")
                # Вызов команды другого устройства
                await device.set_speed(0)
        
        return {"result": "all_stopped"}

# 5. Запуск демонстрации
async def main():
    app = SynapseApp(mqtt_broker="mqtt://localhost:1883")

    # Создаем драйвер с авторизацией
    drv = IndustrialDriver(host="192.168.1.50", token="secret123")
    
    # Создаем мотор
    motor = await app.create_device("motor_main", "industrialmotor", driver=drv)
    
    # Создаем контроллер линии
    controller = await app.create_device("line_ctrl", "linecontroller")

    await app.start()
    print("--- Промышленная система запущена ---")

    # Имитация работы
    await asyncio.sleep(2)
    print("\n[User] Установка скорости мотора...")
    await motor.set_speed(1500)

    await asyncio.sleep(5)
    print("\n[User] Экстренная остановка через контроллер...")
    await controller.emergency_stop()

    await asyncio.sleep(10)
    await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
