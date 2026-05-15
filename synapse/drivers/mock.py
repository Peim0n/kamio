import asyncio
import random
import logging
from typing import Any, Dict, Optional
from .base import BaseDriver

class MockHardwareDriver(BaseDriver):
    """
    Advanced mock driver with configurable latency, failure rates, and realistic behavior.
    """
    def __init__(
        self, 
        latency_range: tuple = (0.01, 0.1), 
        failure_rate: float = 0.0,
        initial_state: Optional[Dict[str, Any]] = None
    ):
        super().__init__()
        self.latency_range = latency_range
        self.failure_rate = failure_rate
        self.state = initial_state or {}
        self.connected = False
        self.logger = logging.getLogger(f"synapse.driver.mock")

    async def connect(self):
        await self._simulate_latency()
        if random.random() < self.failure_rate:
            raise ConnectionError("Mock connection failed")
        self.connected = True
        self.logger.info("Mock driver connected")

    async def disconnect(self):
        await self._simulate_latency()
        self.connected = False
        self.logger.info("Mock driver disconnected")

    async def execute(self, command_name: str, params: dict) -> dict:
        if not self.connected:
            raise RuntimeError("Driver not connected")
        
        await self._simulate_latency()
        if random.random() < self.failure_rate:
            raise RuntimeError(f"Mock execution of {command_name} failed")

        self.logger.debug(f"Mock executing {command_name} with {params}")
        
        # Simple logic for mock state updates
        if command_name.startswith("set_"):
            field = command_name[4:]
            self.state[field] = params.get("value")
            return {"status": "ok", "field": field, "value": self.state[field]}
        
        return {"status": "ok", "result": "mock_success"}

    async def read(self, field_name: str) -> Any:
        if not self.connected:
            raise RuntimeError("Driver not connected")
        
        await self._simulate_latency()
        if random.random() < self.failure_rate:
            raise RuntimeError(f"Mock read of {field_name} failed")

        value = self.state.get(field_name)
        self.logger.debug(f"Mock read {field_name}: {value}")
        return value

    async def _simulate_latency(self):
        delay = random.uniform(*self.latency_range)
        await asyncio.sleep(delay)
