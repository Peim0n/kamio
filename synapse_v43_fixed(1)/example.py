"""
Synapse Core v43 — Reference Example.

This script demonstrates the recommended way to build IoT applications 
using Synapse Core. It covers device definitions, automation rules, 
and the standard application lifecycle.
"""

import asyncio
import logging
from synapse import SynapseApp, Device, command, telemetry, state

# 1. Initialize Application
app = SynapseApp(
    mqtt_broker="mqtt://localhost:1883",
    client_id="synapse_v43_demo",
    log_level=logging.INFO
)

# 2. Define Device Models
@app.device
class Thermostat(Device):
    """Room thermostat with temperature telemetry and target state."""
    temp: float = telemetry(unit="°C", freq="1s")
    target: float = state(default=22.0, writable=True)

    @command
    async def set_target(self, value: float):
        self.logger.info(f"Updating target to {value}°C")
        self.target = value
        await self.request_state_sync()
        return {"status": "ok", "target": self.target}

@app.device
class SmartLight(Device):
    """Simple smart light with power and brightness control."""
    power: bool = state(default=False, writable=True)
    brightness: int = state(default=100, min=0, max=100, writable=True)

    @command
    async def toggle(self):
        self.power = not self.power
        await self.request_state_sync()
        return {"power": self.power}

# 3. Automation Rules
@app.rule(device=Thermostat, fields=["temp"], description="Climate Control")
async def on_temp_change(snapshot: dict, app: SynapseApp):
    """React to temperature changes."""
    temp = snapshot.get("update", {}).get("temp")
    if temp and temp > 25.0:
        app.logger.warning(f"High temperature: {temp}°C")

# 4. Main Execution
if __name__ == "__main__":
    app.run()
