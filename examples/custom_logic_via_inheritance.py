import asyncio
import logging
import time
from typing import Any, Dict, Optional
from synapse import SynapseApp, Device, command, state, config, telemetry
from synapse.drivers.base import BaseDriver

# 1. Расширенный Драйвер с логикой переподключения и авторизации
class RobustDriver(BaseDriver):
    """
    Драйвер, который сам управляет своим состоянием, 
    не требуя изменений в ядре библиотеки.
    """
    def __init__(self, host: str, token: str):
        super().__init__()
        self.host = host
        self.token = token
        self.is_connected = False
        self.is_authenticated = False

    async def connect(self):
        """Метод подключения, который также выполняет авторизацию."""
        self.logger.info(f"Подключение к {self.host}...")
        await asyncio.sleep(0.5) # Имитация сети
        self.is_connected = True
        
        # Сразу выполняем авторизацию
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
            # Если связи нет, пробуем переподключиться прямо во время команды
            self.logger.warning("Нет связи, попытка быстрого переподключения...")
            await self.connect()
            if not self.is_authenticated:
                raise ConnectionError("Драйвер не авторизован")
        
        self.logger.info(f"Выполнение: {command_name}")
        return {"status": "ok", "data": "ACK"}

    async def read(self) -> Dict[str, Any]:
        return {"status": "ok"}

# 2. Расширенный класс Устройства с логикой Keep-Alive
class SmartDevice(Device):
    """
    Устройство, которое реализует Keep-Alive через TaskManagerMixin,
    который уже есть в базовом классе Device.
    """
    def __init__(self, ka_interval: float = 10.0, **kwargs):
        super().__init__(**kwargs)
        self.ka_interval = ka_interval

    async def on_start(self, node):
        """Переопределяем старт, чтобы запустить свои задачи."""
        await super().on_start(node)
        if self.ka_interval > 0:
            self.create_task(self._custom_keepalive_loop())

    async def _custom_keepalive_loop(self):
        """Собственная реализация Keep-Alive."""
        from synapse.core.envelope import Envelope
        while self.node and self.node.is_running:
            try:
                # Отправляем системное сообщение 'k'
                env = Envelope.keepalive(source=self.node.device_id)
                await self.node.publish(env)
                self.logger.debug("Custom Keep-Alive sent")
                await asyncio.sleep(self.ka_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Keep-Alive error: {e}")
                await asyncio.sleep(5)

# 3. Конкретная реализация
@SynapseApp.device
class AdvancedMotor(SmartDevice):
    speed: int = state(default=0, writable=True)

    @command
    async def set_speed(self, value: int):
        # Валидация ответа драйвера перед обновлением состояния
        res = await self.driver.execute("set_speed", {"v": value})
        if res.get("status") == "ok":
            self.speed = value
            await self.request_state_sync()
            return {"result": "success"}
        return {"result": "fail"}

# 4. Демонстрация
async def main():
    app = SynapseApp(mqtt_broker="mqtt://localhost:1883")
    
    # Создаем драйвер и устройство
    drv = RobustDriver(host="10.0.0.1", token="secret123")
    motor = await app.create_device(
        "motor_01", 
        "advancedmotor", 
        driver=drv, 
        ka_interval=5.0 # Задаем интервал Keep-Alive через конструктор
    )

    await app.start()
    print("--- Система запущена (логика в наследниках) ---")
    
    await asyncio.sleep(15)
    await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
