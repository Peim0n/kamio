# Примеры использования Synapse Core v43

В этом разделе представлены различные примеры использования фреймворка Synapse Core v43, демонстрирующие его основные возможности и подходы к разработке IoT-приложений.

## 1. Базовое приложение с термостатом

Этот пример демонстрирует создание простого устройства `SmartThermostat` с телеметрией, состоянием и командой, а также правило, реагирующее на изменение температуры. Он также показывает интеграцию с `MockHardwareDriver`.

**Файл**: `my_app.py` (из `README.md`)

```python
import asyncio
import logging
from synapse import SynapseApp, Device, command, telemetry, state
from synapse.drivers import MockHardwareDriver

# 1. Инициализация приложения
app = SynapseApp(
    mqtt_broker="mqtt://localhost:1883",
    client_id="my_iot_app",
    log_level=logging.INFO
)

# 2. Определение модели устройства с драйвером
@app.device
class SmartThermostat(Device):
    """Умный термостат с телеметрией температуры и целевым состоянием, использующий MockHardwareDriver."""
    temp: float = telemetry(unit="°C", freq="5s")
    target: float = state(default=22.0, writable=True)

    def __init__(self, **kwargs):
        super().__init__(driver=MockHardwareDriver(initial_state={"temp": 20.0}), **kwargs)

    @command
    async def set_target(self, value: float):
        self.logger.info(f"Обновление целевой температуры до {value}°C")
        self.target = value
        await self.request_state_sync()
        return {"status": "ok", "target": self.target}

    async def on_start(self, node):
        await super().on_start(node)
        # Имитация чтения температуры с драйвера
        self.start_task("read_temp", self._read_temperature_periodically())

    async def _read_temperature_periodically(self):
        while True:
            if self.driver:
                read_temp = await self.driver.read("temp")
                if read_temp is not None:
                    self.temp = float(read_temp)
            await asyncio.sleep(5) # Читаем каждые 5 секунд

# 3. Правила автоматизации
@app.rule(device=SmartThermostat, fields=["temp"], description="Контроль климата")
async def on_temp_change(snapshot: dict, app_instance: SynapseApp):
    """Реагирование на изменения температуры."""
    device_id = snapshot["device_id"]
    current_temp = snapshot.get("update", {}).get("temp")
    if current_temp is not None:
        thermostat = app_instance.devices.get(device_id)
        if thermostat and isinstance(thermostat, SmartThermostat):
            if current_temp > thermostat.target + 2.0:
                app_instance.logger.warning(f"[{device_id}] Высокая температура: {current_temp}°C, целевая: {thermostat.target}°C. Включаем охлаждение.")
                # Здесь можно отправить команду на устройство, например, включить кондиционер
            elif current_temp < thermostat.target - 2.0:
                app_instance.logger.info(f"[{device_id}] Низкая температура: {current_temp}°C, целевая: {thermostat.target}°C. Включаем обогрев.")
                # Здесь можно отправить команду на устройство, например, включить обогрев

# 4. Запуск приложения
if __name__ == "__main__":
    async def main():
        # Создание экземпляра устройства
        await app.create_device("room_thermostat", "smartthermostat")
        await app.start()
        app.logger.info("Приложение Smart Thermostat запущено. Press Ctrl+C to stop.")

        # Пример вызова команды через приложение (обычно это делается извне)
        await asyncio.sleep(10)
        app.logger.info("Отправка команды: set_target(24.0)")
        thermostat_instance = app.devices["room_thermostat"]
        await thermostat_instance.set_target(24.0)

        while True:
            await asyncio.sleep(3600) # Работаем час

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        app.logger.info("Приложение остановлено пользователем.")
    finally:
        asyncio.run(app.stop())
```

## 2. Умный дом с несколькими устройствами и правилами

Этот пример демонстрирует более сложную систему умного дома с датчиком движения и умной лампой, а также правила, которые автоматизируют их взаимодействие.

**Файл**: `examples/smart_home.py`

```python
import asyncio
import logging
from synapse import SynapseApp, Device, command, telemetry, state

# Initialize Application
app = SynapseApp(
    mqtt_broker="mqtt://localhost:1883",
    client_id="smart_home_controller",
    log_level=logging.INFO
)

@app.device
class MotionSensor(Device):
    """Simple motion sensor."""
    motion: bool = telemetry(description="Motion detected")

@app.device
class SmartLamp(Device):
    """Dimmable smart lamp."""
    power: bool = state(default=False)
    brightness: int = state(default=100, min=0, max=100)

    @command
    async def set_brightness(self, value: int):
        self.brightness = value
        await self.request_state_sync()
        return {"brightness": self.brightness}

# Automation: Turn on light when motion is detected
@app.rule(device=MotionSensor, fields=["motion"])
async def on_motion(snapshot, app_inst):
    if snapshot["update"].get("motion") is True:
        app_inst.logger.info("Motion detected! Turning on lights.")
        # In a real app, you would find the lamp instance and call it
        for device in app_inst.devices.values():
            if isinstance(device, SmartLamp):
                device.power = True
                await device.request_state_sync()

# Automation: Turn off light after 5 seconds of no motion (simplified)
@app.rule(interval=5.0)
async def periodic_check(snapshot, app_inst):
    app_inst.logger.debug("Periodic system check...")

if __name__ == "__main__":
    # In a real scenario, devices would be created dynamically or via config
    async def setup():
        await app.create_device("hallway_motion", "motionsensor")
        await app.create_device("hallway_lamp", "smartlamp")
        await app.start()
        
    loop = asyncio.get_event_loop()
    loop.run_until_complete(setup())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(app.stop())
```

## 3. Примеры производственных устройств с драйверами

Этот файл содержит определения нескольких реалистичных устройств, демонстрирующих использование различных драйверов и более сложную логику.

**Файл**: `examples/production_devices.py`

```python
import asyncio
import logging
from typing import Optional
from synapse import Device, command, telemetry, state, event
from synapse.drivers.mock import MockHardwareDriver

# 1. MultiSensor (Temperature, Humidity, CO2, Door Contact)
class MultiSensor(Device):
    """Advanced environmental sensor with multiple telemetry fields."""
    temp: float = telemetry(unit="°C", freq="10s")
    humidity: float = telemetry(unit="%", freq="10s")
    co2: int = telemetry(unit="ppm", freq="30s")
    door_open: bool = state(default=False, description="Door contact status")
    
    @event
    def tamper_alert(self, reason: str):
        """Emitted when the sensor casing is opened."""
        pass

    async def on_start(self, node):
        # Simulate periodic telemetry updates if no real driver is doing it
        self.start_task("telemetry_sim", self._sim_telemetry())

    async def _sim_telemetry(self):
        while True:
            self.temp = 22.0 + (asyncio.get_event_loop().time() % 5)
            self.humidity = 45.0 + (asyncio.get_event_loop().time() % 10)
            self.co2 = 400 + int(asyncio.get_event_loop().time() % 100)
            await asyncio.sleep(10)

# 2. Turnstile / AccessControlGate
class AccessControlGate(Device):
    """Industrial turnstile with driver integration."""
    locked: bool = state(default=True)
    passage_count: int = telemetry(default=0)
    
    @command
    async def unlock(self, duration: int = 5):
        """Temporarily unlock the gate for passage."""
        self.logger.info(f"Unlocking gate for {duration}s")
        if self.driver:
            await self.driver.execute("set_lock", {"value": False})
        self.locked = False
        await self.request_state_sync()
        
        # Auto-relock after duration
        async def relock():
            await asyncio.sleep(duration)
            if self.driver:
                await self.driver.execute("set_lock", {"value": True})
            self.locked = True
            await self.request_state_sync()
            self.logger.info("Gate relocked")
            
        self.start_task("relock_task", relock())
        return {"status": "unlocked", "duration": duration}

# 3. IPCamera (Motion, PTZ, Snapshot)
class IPCamera(Device):
    """IP Camera with PTZ and motion detection."""
    motion_detected: bool = telemetry(default=False)
    recording: bool = state(default=False)
    pan: int = state(default=0, min=-180, max=180)
    tilt: int = state(default=0, min=-90, max=90)

    @command
    async def take_snapshot(self):
        """Capture a still image."""
        if self.driver:
            result = await self.driver.execute("snapshot", {})
            return result
        return {"status": "error", "message": "No driver"}

    @command
    async def move_to(self, pan: int, tilt: int):
        """Move camera to specific coordinates."""
        self.pan = pan
        self.tilt = tilt
        if self.driver:
            await self.driver.execute("ptz_move", {"pan": pan, "tilt": tilt})
        await self.request_state_sync()
        return {"pan": self.pan, "tilt": self.tilt}

# 4. RelayModule (Multi-channel)
class RelayModule(Device):
    """8-channel relay module for power control."""
    ch1: bool = state(default=False)
    ch2: bool = state(default=False)
    ch3: bool = state(default=False)
    ch4: bool = state(default=False)

    @command
    async def set_channel(self, channel: int, value: bool):
        """Set specific relay channel state."""
        attr = f"ch{channel}"
        if hasattr(self, attr):
            setattr(self, attr, value)
            if self.driver:
                await self.driver.execute("set_relay", {"channel": channel, "value": value})
            await self.request_state_sync()
            return {attr: value}
        raise ValueError(f"Invalid channel {channel}")

if __name__ == "__main__":
    # This file is intended to be imported, but we can add a small demo
    from synapse import SynapseApp
    
    async def run_demo():
        app = SynapseApp(mqtt_broker="mqtt://localhost:1883")
        app.register(MultiSensor)
        app.register(AccessControlGate)
        
        # Create devices with mock drivers
        sensor = await app.create_device("office_sensor", "multisensor", 
                                       driver=MockHardwareDriver(latency_range=(0.05, 0.2)))
        gate = await app.create_device("main_entrance", "accesscontrolgate",
                                     driver=MockHardwareDriver())
        
        await app.start()
        print("Production-like devices started. Press Ctrl+C to stop.")
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            await app.stop()

    asyncio.run(run_demo())
```

## 4. Работа с командами из внешних MQTT-клиентов

Вы можете отправлять команды на устройства Synapse Core из любого стандартного MQTT-клиента. Для этого используется топик `synapse/v1/{device_id}/sc` и JSON-сообщение следующего формата:

```json
{
    "source": "external",
    "method": "your_command_name",
    "params": {
        "param1": "value1",
        "param2": 123
    },
    "cind": "correlation_id_optional"
}
```

Например, чтобы переключить лампочку `my_light` из примера 2:

```bash
mosquitto_pub -h localhost -t "synapse/v1/my_light/sc" -m '{"source": "external", "method": "toggle", "params": {}}'
```

Или установить яркость:

```bash
mosquitto_pub -h localhost -t "synapse/v1/my_light/sc" -m '{"source": "external", "method": "set_brightness", "params": {"value": 50}}'
```

## 5. Использование Home Assistant Discovery

Synapse Core поддерживает автоматическое обнаружение устройств в Home Assistant. Для этого необходимо включить `HADiscovery` при инициализации `SynapseApp` и вызвать метод `announce` для каждого устройства.

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

---

*Автоматически сгенерировано Manus AI.*
