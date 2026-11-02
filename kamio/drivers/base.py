from __future__ import annotations

import functools
import logging
from abc import ABC, abstractmethod
from fnmatch import fnmatchcase
from typing import Any, Dict, Iterable, Optional, Tuple


class BaseDriver(ABC):
    """
    Base driver class for Kamio Core.
    Handles low-level hardware interaction.

    Dispatch contract: ``Device.handle_command`` / ``Device.handle_state`` call
    :meth:`execute` first and fall back to the device's ``@command`` method or
    in-memory state **only** when it raises :class:`NotImplementedError`.
    Every subclass ``execute`` is wrapped so that a command rejected by
    :meth:`supports_command` raises ``NotImplementedError`` before the
    subclass body runs.

    Args:
        commands: Optional allowlist of command names (``fnmatch`` patterns
            such as ``"set_*"`` are accepted).  ``None`` means the driver
            accepts every command its own :meth:`supports_command` accepts.
    """

    def __init__(self, commands: Optional[Iterable[str]] = None):
        self.logger = logging.getLogger(f"Kamio.driver.{self.__class__.__name__}")
        if isinstance(commands, str):
            commands = (commands,)
        self.commands: Optional[Tuple[str, ...]] = tuple(commands) if commands is not None else None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        execute = cls.__dict__.get("execute")
        if execute is None or getattr(execute, "__isabstractmethod__", False):
            return
        if getattr(execute, "_kamio_contract", False):
            return

        @functools.wraps(execute)
        async def _contract_execute(self: BaseDriver, command_name: str, params: Any) -> Any:
            if not self.supports_command(command_name):
                raise NotImplementedError(
                    f"{type(self).__name__} does not support command {command_name!r}"
                )
            return await execute(self, command_name, params)

        _contract_execute._kamio_contract = True  # type: ignore[attr-defined]
        setattr(cls, "execute", _contract_execute)

    def supports_command(self, command_name: str) -> bool:
        """Return True if :meth:`execute` handles ``command_name``.

        The base implementation checks the ``commands`` allowlist; subclasses
        with a fixed command set should extend it (and call ``super()``).
        """
        if self.commands is None:
            return True
        return any(fnmatchcase(command_name, pattern) for pattern in self.commands)

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
        """Execute command on hardware.

        Must raise :class:`NotImplementedError` for commands the driver does
        not handle (enforced automatically via :meth:`supports_command`).
        Other exceptions mean the command is the driver's but failed.
        """
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
