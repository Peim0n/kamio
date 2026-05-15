from __future__ import annotations
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

class BaseDriver(ABC):
    """
    Base driver class for Synapse Core.
    Handles low-level hardware interaction.
    """
    def __init__(self):
        self.logger = logging.getLogger(f"synapse.driver.{self.__class__.__name__}")

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection with hardware."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection."""
        pass

    @abstractmethod
    async def execute(self, command_name: str, params: Dict[str, Any]) -> Any:
        """Execute command on hardware."""
        pass

    @abstractmethod
    async def read(self) -> Dict[str, Any]:
        """Read current data (for telemetry/state)."""
        pass

    async def __aenter__(self) -> BaseDriver:
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>"
