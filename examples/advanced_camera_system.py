import asyncio
import logging
import json
from typing import Any, Dict, Optional
from synapse import SynapseApp, Device, command, state, config, telemetry, event

# 1. Продвинутый Драйвер Камеры с обратной связью
class AdvancedCameraDriver:
    def __init__(self, name: str):
        self.name = name
        self.is_dimmed = False
        self.position = {"x": 0, "y": 0}

    async def move(self, direction: str, steps: int) -> bool:
        # Имитация движения с шансом ошибки (обратная связь)
        print(f"[Driver {self.name}] Движение {direction} на {steps} шагов...")
        await asyncio.sleep(0.2)
        if direction == "up": self.position["y"] += steps
        elif direction == "down": self.position["y"] -= steps
        return True # Успешно

    async def set_dimming(self, active: bool) -> bool:
        print(f"[Driver {self.name}] Режим затемнения: {'ВКЛ' if active else 'ВЫКЛ'}")
        self.is_dimmed = active
        return True

    async def check_health(self) -> str:
        # Кастомная проверка для Keep-Alive
        return "OK"

# 2. Устройство Камера с разными типами полей и событий
@SynapseApp.device
class AdvancedCamera(Device):
    # Конфигурация
    model: str = config(default="PTZ-9000")
    sensitivity: int = config(default=5, min=1, max=10)
    
    # Состояние с обратной связью
    is_dimmed: bool = state(default=False, writable=True)
    pos_x: int = state(default=0)
    pos_y: int = state(default=0)
    
    # События разных типов
    motion_detected = event(python_type=dict) # Информационное событие
    system_alert = event(python_type=str)    # Критическое событие

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.driver = AdvancedCameraDriver(self.device_type())
        self.ka_interval = 10.0

    async def on_start(self, node):
        await super().on_start(node)
        self.create_task(self._keepalive_loop())

    # --- Гибкий Keep-Alive ---
    async def _keepalive_loop(self):
        from synapse.core.envelope import Envelope
        while self.node and self.node.is_running:
            # Вызываем внутреннюю функцию устройства для кастомной логики
            await self.perform_keepalive()
            await asyncio.sleep(self.ka_interval)

    async def perform_keepalive(self):
        """Кастомная логика Keep-Alive: проверка драйвера перед отправкой."""
        health = await self.driver.check_health()
        if health == "OK":
            from synapse.core.envelope import Envelope
            env = Envelope.keepalive(source=self.node.device_id)
            await self.node.publish(env)
            # self.logger.info("Keep-Alive sent after health check")

    # --- Команды с параметрами и обратной связью ---
    @command
    async def move(self, direction: str, steps: int = 1):
        success = await self.driver.move(direction, steps)
        if success:
            self.pos_x = self.driver.position["x"]
            self.pos_y = self.driver.position["y"]
            await self.request_state_sync() # Подтверждаем новое положение
            
            # Генерируем событие движения
            await self.emit("motion_detected", {"dir": direction, "x": self.pos_x, "y": self.pos_y})
            return {"status": "moved", "pos": self.driver.position}
        return {"status": "error"}

    @command
    async def toggle_dimming(self, active: bool):
        if await self.driver.set_dimming(active):
            self.is_dimmed = active
            await self.request_state_sync()
            return {"status": "ok"}
        return {"status": "fail"}

    # --- Прямая задача от другого устройства ---
    async def direct_task_handler(self, task_name: str, data: dict):
        """Метод для обработки прямых задач от других устройств."""
        print(f"[Camera] Получена прямая задача: {task_name} с данными {data}")
        if task_name == "focus_on":
            await self.move(direction="up", steps=data.get("offset", 0))

# 3. Логическое устройство: Контроллер Безопасности (D2D связь)
@SynapseApp.device
class SecurityController(Device):
    alarm_active: bool = state(default=False, writable=True)

    @command
    async def trigger_alarm(self):
        self.alarm_active = True
        await self.request_state_sync()
        
        # Прямая передача задачи другому устройству (D2D имитация)
        print("[Security] Тревога! Даем команду камерам...")
        for dev_id, device in self.node.app.devices.items():
            if isinstance(device, AdvancedCamera):
                # Вызываем метод напрямую у объекта устройства
                await device.direct_task_handler("focus_on", {"offset": 10})
                # Или через систему событий
                await self.emit("system_alert", f"Alarm triggered by {self.node.device_id}")
        
        return {"result": "alarm_broadcasted"}

# 4. Пример конфигурации (JSON)
CONFIG_JSON = {
    "mqtt_broker": "mqtt://localhost:1883",
    "devices": {
        "cam_01": {
            "type": "advancedcamera",
            "config": {
                "sensitivity": 8,
                "model": "Pro-Vision-X"
            }
        },
        "sec_ctrl": {
            "type": "securitycontroller"
        }
    }
}

async def main():
    # В реальном приложении: app = SynapseApp(config_file="config.json")
    app = SynapseApp(mqtt_broker="mqtt://localhost:1883")
    
    # Создаем устройства на основе "конфига"
    cam = await app.create_device("cam_01", "advancedcamera")
    # Применяем конфиг вручную для демо
    await cam.handle_config(CONFIG_JSON["devices"]["cam_01"]["config"])
    
    ctrl = await app.create_device("sec_ctrl", "securitycontroller")

    await app.start()
    print("--- Продвинутая система камер запущена ---")

    await asyncio.sleep(2)
    print("\n[User] Тест движения камеры...")
    await cam.move(direction="up", steps=5)

    await asyncio.sleep(2)
    print("\n[User] Тест тревоги (D2D связь)...")
    await ctrl.trigger_alarm()

    await asyncio.sleep(5)
    await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
