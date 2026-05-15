import asyncio
import logging
import json
import os
from typing import Any, Dict, List, Optional
from synapse import (
    SynapseApp, Device, command, state, telemetry, event, config,
    Config, HADiscovery
)
from synapse.drivers.base import BaseDriver
from synapse.drivers.mock import MockHardwareDriver

# 1. Создание пользовательского драйвера
class CustomSensorDriver(BaseDriver):
    """Пример драйвера для специфического оборудования."""
    async def connect(self):
        print("[Driver] Подключение к кастомному датчику...")
        
    async def read(self, field_name: str) -> Any:
        if field_name == "raw_value":
            return 42.0
        return None

    async def execute(self, command_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        print(f"[Driver] Выполнение команды драйвера: {command_name}")
        return {"status": "success"}

# 2. Определение устройства со всеми типами полей
@SynapseApp.device
class AdvancedDevice(Device):
    """Устройство, демонстрирующее все типы полей и сообщений."""
    
    # Поле конфигурации (загружается при старте)
    threshold: float = config(default=10.0, description="Порог срабатывания")
    
    # Поле состояния (чтение/запись, синхронизируется через 'ds')
    mode: str = state(default="auto", choices=("auto", "manual", "off"), description="Режим работы")
    is_active: bool = state(default=False, description="Активность устройства")
    
    # Поле телеметрии (периодическая отправка через 'dt')
    temperature: float = telemetry(unit="°C", freq="5s", description="Температура")
    humidity: float = telemetry(unit="%", freq="10s", description="Влажность")
    
    # Поле события (отправка через 'ev')
    motion_detected: bool = event(description="Обнаружено движение")

    @command
    async def set_mode(self, new_mode: str):
        """Команда для изменения режима (вызов через 'sc', ответ через 'ca')."""
        if new_mode in ["auto", "manual", "off"]:
            self.mode = new_mode
            await self.request_state_sync()
            return {"status": "mode_updated", "new_mode": self.mode}
        raise ValueError("Неверный режим")

    @command
    async def trigger_event(self):
        """Имитация генерации события."""
        await self.emit("motion_detected", {"timestamp": "2026-05-14T12:00:00Z", "zone": "A1"})
        return {"event": "sent"}

# 3. Логическое устройство (агрегатор)
@SynapseApp.device
class LogicAggregator(Device):
    """Устройство, которое не имеет драйвера, а работает с данными других устройств."""
    average_temp: float = state(default=0.0)
    alert: bool = state(default=False)

# 4. Основная логика приложения
async def main():
    # Настройка логирования
    logging.basicConfig(level=logging.INFO)
    
    # Загрузка конфигурации
    # Можно создать файл config.json: {"mqtt_broker": "mqtt://localhost:1883"}
    app_config = Config() 
    
    app = SynapseApp(
        mqtt_broker=app_config.mqtt_broker,
        client_id="full_demo_app"
    )

    # Регистрация правил (Automation)
    @app.rule(device=AdvancedDevice, fields=["temperature"])
    async def temp_monitor(snapshot: dict, app_instance: SynapseApp):
        """Правило, реагирующее на изменение температуры."""
        temp = snapshot["update"].get("temperature")
        dev_id = snapshot["device_id"]
        print(f"[Rule] Устройство {dev_id} прислало температуру: {temp}")
        
        # Логика управления другим устройством
        aggregator = app_instance.devices.get("main_aggregator")
        if aggregator and temp > 25:
            aggregator.alert = True
            await aggregator.request_state_sync()

    # Создание экземпляров
    # Устройство с кастомным драйвером
    dev1 = await app.create_device(
        "sensor_01", 
        "advanceddevice", 
        driver=CustomSensorDriver(),
        threshold=15.5 # Переопределение конфига
    )
    
    # Устройство с мок-драйвером
    dev2 = await app.create_device(
        "sensor_02", 
        "advanceddevice", 
        driver=MockHardwareDriver()
    )
    
    # Логическое устройство
    aggregator = await app.create_device("main_aggregator", "logicaggregator")

    # Home Assistant Discovery
    ha = HADiscovery()
    await ha.announce(dev1)
    await ha.announce(dev2)

    # Запуск
    await app.start()
    print("--- Synapse Core Full Demo Started ---")
    
    try:
        # Имитация работы
        while True:
            # В реальности данные приходят от драйверов или MQTT
            # Здесь мы просто показываем, как программно менять значения
            dev1.temperature = 26.4
            await dev1.request_state_sync() # Это отправит 'ds' сообщение
            
            await asyncio.sleep(10)
    except KeyboardInterrupt:
        await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
