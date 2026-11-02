from __future__ import annotations

import functools
import logging
from abc import ABC, abstractmethod
from fnmatch import fnmatchcase
from typing import Any, ClassVar, Dict, Iterable, Optional, Tuple

#: Transport commands shared by byte-stream drivers (Telnet, Serial, UDP).
BYTE_TRANSPORT_COMMANDS: Tuple[str, ...] = ("send", "request")


def require_payload(params: Optional[Dict[str, Any]], command_name: str) -> Any:
    """Return ``params["payload"]`` or raise ``ValueError`` naming the command."""
    payload = (params or {}).get("payload")
    if payload is None:
        raise ValueError(f"{command_name!r} requires params['payload'] (str or bytes)")
    return payload


class BaseDriver(ABC):
    """
    Base driver class for Kamio Core.
    Handles low-level hardware interaction.

    Dispatch contract (identical for every driver): ``Device.handle_command`` /
    ``Device.handle_state`` call :meth:`execute` first and fall back to the
    device's ``@command`` method or in-memory state **only** when it raises
    :class:`NotImplementedError`.  A driver owns a fixed set of transport
    commands, declared in :attr:`COMMANDS`; device semantics (what to send,
    how to interpret the reply) belong to the device.  Every subclass
    ``execute`` is wrapped so a command rejected by :meth:`supports_command`
    raises ``NotImplementedError`` before the subclass body runs.

    Likewise :meth:`read` raises ``NotImplementedError`` when called without
    the addressing parameters the driver needs (e.g. a bare telemetry read).

    Args:
        commands: Optional allowlist (``fnmatch`` patterns) that further
            narrows :attr:`COMMANDS` for this instance.
    """

    #: ``fnmatch`` patterns of the commands this driver owns.
    COMMANDS: ClassVar[Tuple[str, ...]] = ("*",)

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
        """Return True if ``command_name`` matches :attr:`COMMANDS` and ``commands``."""
        if not any(fnmatchcase(command_name, pattern) for pattern in self.COMMANDS):
            return False
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
