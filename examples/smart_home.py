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
