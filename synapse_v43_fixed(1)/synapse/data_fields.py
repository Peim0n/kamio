from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Literal

@dataclass(frozen=True)
class Field:
    """Universal field for any device data type."""
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
    min: float | None = None
    max: float | None = None
    choices: tuple[Any, ...] | None = None
    required: bool = False

    metadata: dict[str, Any] = field(default_factory=dict)

def telemetry(
    *,
    unit: str = "",
    freq: str = "",
    description: str = "",
    min: float | None = None,
    max: float | None = None,
    required: bool = False,
    **metadata: Any,
) -> Any:
    """Telemetry - data sent by the device."""
    return Field(
        kind="telemetry",
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
    min: float | None = None,
    max: float | None = None,
    choices: tuple | None = None,
    required: bool = False,
    **metadata: Any,
) -> Any:
    """State - data that can be read and modified."""
    return Field(
        kind="state",
        default=default,
        writable=writable,
        description=description,
        min=min,
        max=max,
        choices=choices,
        required=required,
        metadata=metadata,
    )

def event(description: str = "", **metadata: Any) -> Any:
    """Events (e.g., button_pressed, alert)."""
    return Field(kind="event", description=description, metadata=metadata)

def config(default: Any = None, **metadata: Any) -> Any:
    """Configuration parameters."""
    return Field(kind="config", default=default, writable=True, metadata=metadata)

def parse_freq(freq: Any) -> float:
    """Parses frequency string (e.g., '5s', '1m', '100ms') into seconds."""
    if freq is None:
        return 0.0

    if isinstance(freq, (int, float)):
        return float(freq)

    if not isinstance(freq, str):
        return 0.0

    freq = freq.lower().strip()
    if not freq:
        return 0.0

    try:
        if freq.endswith("ms"):
            return float(freq[:-2]) / 1000.0
        elif freq.endswith("s"):
            return float(freq[:-1])
        elif freq.endswith("m"):
            return float(freq[:-1]) * 60.0
        elif freq.endswith("h"):
            return float(freq[:-1]) * 3600.0

        return float(freq)
    except (ValueError, IndexError):
        import logging
        logging.getLogger("synapse.fields").warning(f"Failed to parse freq: {freq}")
        return 0.0
