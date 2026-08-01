from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseDriver(ABC):
    """
    Base driver class for Kamio Core.
    Handles low-level hardware interaction.
    """

    def __init__(self):
        self.logger = logging.getLogger(f"Kamio.driver.{self.__class__.__name__}")

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
    async def read(self, field_name: str, params: Dict[str, Any] | None = None) -> Any:
        """Read the current value of a hardware field or sensor."""
        pass

    async def __aenter__(self) -> BaseDriver:
        """Async context manager entry — calls connect()."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit — calls disconnect()."""
        await self.disconnect()

    def __repr__(self) -> str:
        """Return a developer-friendly representation."""
        return f"<{self.__class__.__name__}>"
