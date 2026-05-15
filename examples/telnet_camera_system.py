import asyncio
import logging
from typing import Any, Dict, Optional
from synapse import SynapseApp, Device, command, state, config, Config
from synapse.drivers.base import BaseDriver

# 1. Реализация Telnet Драйвера
class TelnetCameraDriver(BaseDriver):
    """Драйвер для управления камерой по Telnet."""
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None

    async def connect(self):
        """Установка соединения при старте устройства."""
        print(f"[TelnetDriver] Подключение к {self.host}:{self.port}...")
        # В реальном сценарии:
        # self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        await asyncio.sleep(0.5) # Имитация задержки
        print(f"[TelnetDriver] Соединение установлено.")

    async def disconnect(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    async def execute(self, command_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Отправка команд в Telnet сессию."""
        cmd_str = f"{command_name} " + " ".join([f"{k}={v}" for k, v in params.items()])
        print(f"[TelnetDriver] Отправка в Telnet: {cmd_str}")
        
        # Имитация отправки и получения ответа
        # self.writer.write(f"{cmd_str}\n".encode())
        # await self.writer.drain()
        # response = await self.reader.readline()
        
        await asyncio.sleep(0.1)
        return {"status": "ok", "raw_response": "ACK"}

# 2. Устройство "Камера"
@SynapseApp.device
class TelnetCamera(Device):
    """Физическое устройство камеры, использующее Telnet драйвер."""
    
    # Конфигурация (адрес и порт берутся из конфига при создании)
    host: str = config(default="127.0.0.1")
    port: int = config(default=23)
    
    # Состояние
    preset: int = state(default=1, writable=True, description="Текущий пресет (1-10)")
    is_online: bool = state(default=True)

    @command
    async def move_to_preset(self, preset_id: int):
        """Команда перемещения камеры."""
        if not 1 <= preset_id <= 10:
            raise ValueError("Пресет должен быть от 1 до 10")
        
        # Вызов драйвера
        result = await self.driver.execute("GOTO_PRESET", {"id": preset_id})
        
        if result["status"] == "ok":
            self.preset = preset_id
            await self.request_state_sync()
            return {"status": "moved", "preset": self.preset}
        return {"status": "error"}

# 3. Логическое устройство "Менеджер Камер"
@SynapseApp.device
class CameraManager(Device):
    """Логическое устройство для управления группой камер."""
    
    active_scene: str = state(default="idle", writable=True)
    
    @command
    async def set_scene(self, scene_name: str):
        """Управляет сразу несколькими камерами для создания 'сцены'."""
        self.active_scene = scene_name
        await self.request_state_sync()
        
        # Логика управления другими устройствами через SynapseApp
        # Мы можем получить доступ к приложению через self.node.app (если проброшено)
        # Или использовать глобальный реестр. В SynapseApp экземпляры доступны в app.devices.
        
        # Пример: Сцена "Конференция" - направляем все камеры на стол
        if scene_name == "conference":
            for dev_id, device in self.node.app.devices.items():
                if isinstance(device, TelnetCamera):
                    print(f"[Manager] Направляем камеру {dev_id} на пресет 1")
                    await device.move_to_preset(1)
        
        elif scene_name == "security":
            for dev_id, device in self.node.app.devices.items():
                if isinstance(device, TelnetCamera):
                    print(f"[Manager] Направляем камеру {dev_id} на пресет 5 (патруль)")
                    await device.move_to_preset(5)
                    
        return {"scene": scene_name, "status": "applied"}

# 4. Пример запуска и конфигурации
async def main():
    # Имитация загрузки конфига из JSON
    # В реальности это может быть файл:
    # {
    #   "cameras": [
    #     {"id": "cam_north", "host": "192.168.1.10", "port": 2323},
    #     {"id": "cam_south", "host": "192.168.1.11", "port": 2323}
    #   ]
    # }
    config_data = {
        "cameras": [
            {"id": "cam_north", "host": "192.168.1.10", "port": 2323},
            {"id": "cam_south", "host": "192.168.1.11", "port": 2323}
        ]
    }

    app = SynapseApp(mqtt_broker="mqtt://localhost:1883")

    # Создаем камеры на основе конфига
    for cam_cfg in config_data["cameras"]:
        driver = TelnetCameraDriver(host=cam_cfg["host"], port=cam_cfg["port"])
        await app.create_device(
            device_id=cam_cfg["id"],
            device_type="telnetcamera",
            driver=driver,
            host=cam_cfg["host"],
            port=cam_cfg["port"]
        )

    # Создаем менеджер
    manager = await app.create_device("global_cam_manager", "cameramanager")

    await app.start()
    print("--- Система управления камерами запущена ---")

    # Пример: Через 5 секунд менеджер включает сцену "conference"
    await asyncio.sleep(5)
    print("\n[User] Активация сцены: conference")
    await manager.set_scene("conference")

    await asyncio.sleep(5)
    await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
