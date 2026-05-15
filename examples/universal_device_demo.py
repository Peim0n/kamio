import asyncio
import logging
from typing import Type, Dict, Optional
from synapse import SynapseApp, Device, command, state, config
from synapse.drivers.base import BaseDriver
from synapse.drivers.mock import MockHardwareDriver
from synapse.drivers.aten_vp2420 import ATEN_VP2420_Driver

# --- Фабрика Драйверов ---

class DriverFactory:
    """
    Простая фабрика для динамического создания драйверов на основе модели.
    """
    _drivers: Dict[str, Type[BaseDriver]] = {
        "mock": MockHardwareDriver,
        "aten_vp2420": ATEN_VP2420_Driver,
        # Здесь можно добавить другие драйверы
    }

    @classmethod
    def create(cls, model: str, **kwargs) -> BaseDriver:
        driver_class = cls._drivers.get(model.lower())
        if not driver_class:
            raise ValueError(f"Драйвер для модели '{model}' не найден.")
        return driver_class(**kwargs)

# --- Универсальное Устройство ---

@SynapseApp.device
class CleverCam(Device):
    """
    Универсальное устройство, которое подгружает драйвер в зависимости от конфига.
    """
    model: str = config(default="mock")
    host: str = config(default="127.0.0.1")
    
    status: str = state(default="idle")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.driver: Optional[BaseDriver] = None

    async def on_init(self, **kwargs):
        # Динамическая загрузка драйвера на основе модели из конфига
        self.logger.info(f"Загрузка драйвера для модели: {self.model}")
        try:
            self.driver = DriverFactory.create(self.model, host=self.host)
            await self.driver.connect()
        except Exception as e:
            self.logger.error(f"Ошибка инициализации драйвера: {e}")

    @command
    async def capture(self):
        if self.driver:
            # Вызов метода драйвера (интерфейс должен быть унифицирован в драйверах)
            res = await self.driver.execute("read", {"field_name": "image"})
            self.status = "captured"
            await self.request_state_sync()
            return {"status": "success", "data": res}
        return {"status": "error", "message": "Драйвер не инициализирован"}

# --- Демонстрация ---

async def main():
    app = SynapseApp(mqtt_broker="mqtt://localhost:1883")
    
    # 1. Создаем камеру с Mock драйвером
    cam1 = await app.create_device("cam_01", "clevercam")
    await cam1.handle_config({"model": "mock", "host": "localhost"})
    
    # 2. Создаем камеру с драйвером ATEN (для примера)
    cam2 = await app.create_device("cam_02", "clevercam")
    await cam2.handle_config({"model": "aten_vp2420", "host": "192.168.1.100"})

    await app.start()
    
    print("\n[User] Выполнение захвата на cam_01 (Mock)...")
    await cam1.capture()
    
    print("\n[User] Выполнение захвата на cam_02 (ATEN)...")
    await cam2.capture()

    await asyncio.sleep(2)
    await app.stop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
