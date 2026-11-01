from __future__ import annotations

"""Typed error hierarchy for Kamio.

Every public error inherits from :class:`KamioError` *and* from the builtin
exception it replaces (``ValueError``/``AttributeError``/``RuntimeError``),
so existing ``except`` clauses keep working while callers — including
programmatic agents — can match a specific, stable error type.
"""

from typing import Any, Optional


class KamioError(Exception):
    """Base class for all Kamio errors."""


class ValidationError(KamioError, ValueError):
    """A field value failed validation (type, min/max, choices)."""

    def __init__(self, message: str, *, field: Optional[str] = None, value: Any = None):
        super().__init__(message)
        self.field = field
        self.value = value


class DeviceNotFoundError(KamioError, ValueError):
    """Requested device id or device type is not registered."""


class CommandNotFoundError(KamioError, AttributeError):
    """A command name is not registered on the device."""


class DriverNotAllowedError(KamioError, RuntimeError):
    """A device was created with a driver class the app policy forbids."""


class PolicyError(KamioError, RuntimeError):
    """An operation was rejected by an application policy (guardrail)."""


class KamioTimeoutError(KamioError, TimeoutError):
    """An operation exceeded its deadline (ACK wait, connection, RPC)."""
