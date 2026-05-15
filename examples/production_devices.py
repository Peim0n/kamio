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
