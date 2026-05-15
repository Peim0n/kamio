import asyncio
import logging
import random
from synapse import SynapseApp, Device, telemetry, state

# --- Датчики (простые устройства) ---

@SynapseApp.device
class TemperatureSensor(Device):
    value: float = telemetry(freq="5s", unit="C")
    
    async def on_start(self, node):
        self.create_task(self._simulate_data())
        
    async def _simulate_data(self):
        while True:
            self.value = round(random.uniform(20.0, 25.0), 2)
            await asyncio.sleep(5)

# --- Узел Транспортировки (Gateway) ---

@SynapseApp.device
class SensorGateway(Device):
    """
    Устройство-шлюз, которое собирает данные с локальных датчиков
    и может отправлять агрегированный отчет или управлять ими.
    """
    active_sensors_count: int = state(default=0)
    last_aggregated_data: dict = state(default={})

    async def on_start(self, node):
        # Шлюз следит за всеми датчиками в своем приложении
        self.create_task(self._aggregation_loop())

    async def _aggregation_loop(self):
        while True:
            sensors_data = {}
            count = 0
            
            # Собираем данные со всех устройств типа TemperatureSensor
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

# --- Запуск Системы ---

async def main():
    # Это приложение может работать на отдельном контроллере (например, Raspberry Pi)
    # и собирать данные с локально подключенных датчиков.
    app = SynapseApp(mqtt_broker="mqtt://localhost:1883", app_name="SensorGatewayNode")
    
    # Создаем группу датчиков
    for i in range(1, 4):
        await app.create_device(f"temp_sensor_{i}", "temperaturesensor")
    
    # Создаем шлюз
    await app.create_device("main_gateway", "sensorgateway")

    print("--- Запуск Узла Транспортировки Датчиков ---")
    await app.start()
    
    await asyncio.sleep(30) # Работаем 30 секунд
    await app.stop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
