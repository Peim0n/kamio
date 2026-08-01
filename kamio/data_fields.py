from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, FrozenSet, Iterable, Literal, Optional

_logger = logging.getLogger("Kamio.fields")


@dataclass(frozen=True)
class Field:
    """
    Immutable descriptor for a single device field.

    Created by the helper functions :func:`state`, :func:`telemetry`,
    :func:`event`, and :func:`config` — never instantiated directly.

    Attributes:
        name:        Field name (filled automatically by ``DeviceMeta``).
        kind:        One of ``"state"``, ``"telemetry"``, ``"event"``, ``"config"``.
        python_type: Inferred Python type (set by metaclass).
        description: Human-readable description shown in schemas.
        unit:        Physical unit string, e.g. ``"°C"``, ``"W"`` (telemetry only).
        freq:        Sampling frequency string, e.g. ``"5s"``, ``"500ms"`` (telemetry only).
        default:     Default value applied on device creation.
        writable:    Whether the field can be set via ``handle_state``.
        min:         Minimum allowed numeric value (inclusive).
        max:         Maximum allowed numeric value (inclusive).
        choices:     Allowed discrete values; raises ``ValueError`` if violated.
        required:    Whether the field must be provided explicitly.
        metadata:    Arbitrary extra key-value pairs forwarded from the helper call.
    """

    name: str = ""
    kind: Literal["telemetry", "state", "event", "config"] = "state"
    python_type: Any = None
    description: str = ""

    # Telemetry specific
    unit: str = ""
    freq: str = ""

    # State specific
    default: Any = None
    writable: bool = True

    # Validation
    min: Optional[float] = None
    max: Optional[float] = None
    choices: Optional[FrozenSet[Any]] = None
    required: bool = False

    metadata: dict[str, Any] = field(default_factory=dict)

    def __set_name__(self, owner, name):
        """Set the field name automatically when the descriptor is assigned to a class attribute."""
        object.__setattr__(self, "name", name)

    def __get__(self, obj, objtype=None):
        """Return the field value from the instance, or the Field itself when accessed on the class."""
        if obj is None:
            return self
        return obj.__dict__.get(self.name, self.default)


def telemetry(
    default: Any = None,
    *,
    unit: str = "",
    freq: str = "",
    description: str = "",
    min: Optional[float] = None,
    max: Optional[float] = None,
    required: bool = False,
    **metadata: Any,
) -> Any:
    """
    Declare a read-only telemetry field on a :class:`Device`.

    Telemetry represents data *produced* by the device (sensor readings,
    counters, etc.).  It is published to MQTT automatically at the
    specified frequency and cannot be set via ``handle_state``.

    Args:
        default:     Initial value before the first real reading.
        unit:        Physical unit string, e.g. ``"°C"``, ``"lux"``.
        freq:        Publish frequency, e.g. ``"5s"``, ``"500ms"``, ``"1m"``.
        description: Human-readable field description.
        min:         Lower bound for validation warnings.
        max:         Upper bound for validation warnings.
        required:    If ``True``, the field must receive a value before publish.
        **metadata:  Extra key-value pairs stored in :attr:`Field.metadata`.

    Example::

        class EnvSensor(Device):
            temperature: float = telemetry(default=0.0, unit="°C", freq="10s")
            humidity:    float = telemetry(default=0.0, unit="%",  freq="10s")
    """
    return Field(
        kind="telemetry",
        default=default,
        writable=False,
        unit=unit,
        freq=freq,
        description=description,
        min=min,
        max=max,
        required=required,
        metadata=metadata,
    )


def state(
    default: Any = None,
    *,
    writable: bool = True,
    description: str = "",
    min: Optional[float] = None,
    max: Optional[float] = None,
    choices: Optional[Iterable[Any]] = None,
    required: bool = False,
    **metadata: Any,
) -> Any:
    """
    Declare a state field on a :class:`Device`.

    State represents a value that can be read and, when ``writable=True``,
    changed remotely via MQTT or ``device.handle_state()``.  Changes
    automatically trigger matching automation rules.

    Args:
        default:     Initial value on device creation.
        writable:    Allow remote writes via ``handle_state`` (default ``True``).
        description: Human-readable field description.
        min:         Minimum allowed numeric value (raises ``ValueError`` if violated).
        max:         Maximum allowed numeric value (raises ``ValueError`` if violated).
        choices:     Allowed values as any iterable; stored as ``frozenset``.
        required:    Reserved for schema documentation; not enforced at runtime.
        **metadata:  Extra key-value pairs stored in :attr:`Field.metadata`.

    Example::

        class SmartLight(Device):
            power:      bool = state(default=False, writable=True)
            brightness: int  = state(default=100, min=0, max=255, writable=True)
            mode:       str  = state(default="auto", choices=("auto", "manual"))
    """
    return Field(
        kind="state",
        default=default,
        writable=writable,
        description=description,
        min=min,
        max=max,
        choices=frozenset(choices) if choices is not None else None,
        required=required,
        metadata=metadata,
    )


def event(description: str = "", **metadata: Any) -> Any:
    """
    Declare an event field on a :class:`Device`.

    Events are one-shot signals emitted by the device (e.g. button press,
    alarm trigger).  They are not stored as persistent state.

    Args:
        description: Human-readable description of when this event fires.
        **metadata:  Extra key-value pairs stored in :attr:`Field.metadata`.

    Example::

        class DoorSensor(Device):
            door_opened: None = event(description="Fires when door opens")
    """
    return Field(kind="event", description=description, metadata=metadata)


def config(default: Any = None, **metadata: Any) -> Any:
    """
    Declare a configuration field on a :class:`Device`.

    Config fields hold persistent settings (e.g. thresholds, identifiers)
    that are applied once and rarely change.  They are set via
    ``device.handle_config()`` and are always writable.

    Args:
        default:    Default value on device creation.
        **metadata: Extra key-value pairs stored in :attr:`Field.metadata`.

    Example::

        class Thermostat(Device):
            setpoint: float = config(default=22.0)
            unit:     str   = config(default="C")
    """
    return Field(kind="config", default=default, writable=True, metadata=metadata)


def parse_freq(freq: Any) -> float:
    """
    Parse a human-readable frequency string into seconds (float).

    Supported suffixes: ``ms`` (milliseconds), ``s`` (seconds),
    ``m`` (minutes), ``h`` (hours).  Plain numbers are treated as seconds.
    ``None`` and empty string return ``0.0`` (treated as "telemetry disabled").

    Args:
        freq: Frequency value — a string like ``"5s"``, ``"500ms"``,
              ``"1m"``, an ``int``/``float`` (already in seconds), or ``None``.

    Returns:
        Duration in seconds as a non-negative ``float``. ``0.0`` disables
        telemetry for the field.

    Raises:
        ValueError: If ``freq`` is a non-empty string that cannot be parsed,
                    or if the resulting duration is negative.

    Examples::

        parse_freq("500ms")  # → 0.5
        parse_freq("5s")     # → 5.0
        parse_freq("2m")     # → 120.0
        parse_freq(10)       # → 10.0
        parse_freq(None)     # → 0.0
    """
    if freq is None:
        return 0.0

    if isinstance(freq, (int, float)):
        seconds = float(freq)
        if seconds < 0:
            raise ValueError(f"Frequency cannot be negative, got {freq!r}")
        return seconds

    if not isinstance(freq, str):
        raise ValueError(f"Cannot parse frequency from {type(freq).__name__}: {freq!r}")

    freq = freq.lower().strip()
    if not freq:
        return 0.0

    multipliers = {
        "ms": 1.0 / 1000.0,
        "s": 1.0,
        "m": 60.0,
        "h": 3600.0,
    }
    for suffix, multiplier in multipliers.items():
        if freq.endswith(suffix):
            try:
                seconds = float(freq[: -len(suffix)]) * multiplier
            except ValueError:
                raise ValueError(
                    f"Failed to parse frequency {freq!r}: invalid numeric value before suffix {suffix!r}"
                )
            if seconds < 0:
                raise ValueError(f"Frequency cannot be negative, got {freq!r}")
            return seconds
    try:
        seconds = float(freq)
    except ValueError:
        raise ValueError(
            f"Failed to parse frequency {freq!r}; expected numeric value with "
            f"unit suffix 'ms', 's', 'm', 'h' (e.g. '500ms', '5s', '2m', '1h')"
        )
    if seconds < 0:
        raise ValueError(f"Frequency cannot be negative, got {freq!r}")
    return seconds
