from __future__ import annotations

import asyncio
import random
from typing import Any, Dict, Optional

from .base import BaseDriver


class MockHardwareDriver(BaseDriver):
    """
    Advanced mock driver with configurable latency, failure rates, and realistic behavior.

    Args:
        latency_range:  ``(min, max)`` tuple of seconds; each operation sleeps
                        a random duration within this range to simulate I/O.
        failure_rate:   Probability ``[0.0, 1.0]`` that an operation raises
                        ``ConnectionError``. ``0.0`` = never fail.
        initial_state:  Pre-seeded state dict returned by ``read()`` and mutated
                        by ``execute("set_<field>", ...)``.
    """

    def __init__(
        self,
        latency_range: tuple = (0.01, 0.1),
        failure_rate: float = 0.0,
        initial_state: Optional[Dict[str, Any]] = None,
    ):
        super().__init__()
        self.latency_range = latency_range
        self.failure_rate = failure_rate
        self.state = initial_state or {}
        self.connected = False

    async def connect(self):
        """Simulate a connection with optional latency and failure injection."""
        await self._simulate_latency()
        if random.random() < self.failure_rate:
            raise ConnectionError("Mock connection failed")
        self.connected = True
        self.logger.info("Mock driver connected")

    async def disconnect(self):
        """Simulate disconnection."""
        await self._simulate_latency()
        self.connected = False
        self.logger.info("Mock driver disconnected")

    async def execute(self, command_name: str, params: dict) -> dict:
        """Simulate command execution, returning canned data or raising injected errors."""
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

    async def read(self, field_name: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Simulate a read operation, returning canned data."""
        if not self.connected:
            raise RuntimeError("Driver not connected")
        await self._simulate_latency()
        if random.random() < self.failure_rate:
            raise RuntimeError(f"Mock read of {field_name} failed")
        return self.state.get(field_name)

    async def _simulate_latency(self):
        delay = random.uniform(*self.latency_range)
        await asyncio.sleep(delay)
