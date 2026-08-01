from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Union

logger = logging.getLogger("Kamio.envelope")

SERVER_ID = "0"


class EnvelopeType(str, Enum):
    """MQTT message envelope types."""

    DEVICE_TELEMETRY = "dt"
    DEVICE_STATE = "ds"
    STATE_ACK = "sa"
    SERVER_COMMAND = "sc"
    DEVICE_COMMAND = "dc"
    COMMAND_ACK = "ca"
    DEVICE_EVENT = "de"
    SERVER_EVENT = "se"
    KEEPALIVE = "k"
    DEVICE_CONFIG = "conf"
    UNKNOWN = "unknown"


@dataclass(slots=True)
class Envelope:
    """Standard message envelope (DTO)."""

    source: str
    type: EnvelopeType
    data: Dict[str, Any]
    target: Optional[str] = None
    cind: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    ts: float = field(default_factory=time.time)
    meta: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def telemetry(cls, source: str, data: dict) -> Envelope:
        """Create a DEVICE_TELEMETRY envelope."""
        return cls(source=source, type=EnvelopeType.DEVICE_TELEMETRY, data=data)

    @classmethod
    def state(cls, source: str, data: dict) -> Envelope:
        """Create a DEVICE_STATE envelope."""
        return cls(source=source, type=EnvelopeType.DEVICE_STATE, data=data)

    @classmethod
    def state_ack(cls, source: str, target: str, data: dict, cind: str) -> Envelope:
        """Create a STATE_ACK envelope."""
        return cls(source=source, target=target, type=EnvelopeType.STATE_ACK, data=data, cind=cind)

    @classmethod
    def event(
        cls,
        source: str,
        event_name: str,
        payload: Optional[dict] = None,
        data: Optional[dict] = None,
    ) -> Envelope:
        """Create a DEVICE_EVENT envelope with event name and payload."""
        effective_payload = payload if payload is not None else (data if data is not None else {})
        return cls(
            source=source,
            type=EnvelopeType.DEVICE_EVENT,
            data={"event": event_name, "payload": effective_payload},
        )

    @classmethod
    def command(
        cls,
        source: str,
        target: str,
        method: str,
        params: Optional[dict] = None,
        cind: Optional[str] = None,
        meta: Optional[dict] = None,
    ) -> Envelope:
        """Create a SERVER_COMMAND envelope."""
        kwargs: Dict[str, Any] = {
            "source": source,
            "target": target,
            "type": EnvelopeType.SERVER_COMMAND,
            "data": {"method": method, "params": params or {}},
            "meta": meta or {},
        }
        if cind:
            kwargs["cind"] = cind
        return cls(**kwargs)

    @classmethod
    def command_ack(cls, source: str, target: str, data: dict, cind: str) -> Envelope:
        """Create a COMMAND_ACK envelope."""
        return cls(
            source=source, target=target, type=EnvelopeType.COMMAND_ACK, data=data, cind=cind
        )

    @classmethod
    def keepalive(cls, source: str) -> Envelope:
        """Create a KEEPALIVE envelope."""
        return cls(source=source, target=source, type=EnvelopeType.KEEPALIVE, data={})

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the envelope to a dict."""
        return {
            "source": self.source,
            "target": self.target,
            "type": self.type.value,
            "cind": self.cind,
            "ts": self.ts,
            "data": self.data,
            "meta": self.meta,
        }

    def to_json(self) -> str:
        """Serialize the envelope to a JSON string."""
        try:
            return json.dumps(self.to_dict(), default=str)
        except (TypeError, ValueError) as e:
            logger.error(f"Serialization error: {e}")
            raise ValueError(f"Failed to serialize Envelope to JSON: {e}")

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Optional[Envelope]:
        """Parse a dict into an Envelope. Returns None on error."""
        try:
            raw_type = d.get("type", "unknown")
            env_type = EnvelopeType(raw_type)
        except ValueError:
            env_type = EnvelopeType.UNKNOWN

        try:
            target = d.get("target")
            raw_data = d.get("data")
            raw_meta = d.get("meta")
            return cls(
                source=str(d.get("source", "")),
                target=str(target) if target is not None else None,
                type=env_type,
                cind=str(d.get("cind") if d.get("cind") is not None else uuid.uuid4().hex[:8]),
                ts=float(d.get("ts", time.time())),
                data=raw_data if isinstance(raw_data, dict) else {},
                meta=raw_meta if isinstance(raw_meta, dict) else {},
            )
        except Exception:
            logger.exception("Dict parsing error")
            return None

    @classmethod
    def from_json(cls, s: Union[str, bytes]) -> Optional[Envelope]:
        """Parse a JSON string/bytes into an Envelope. Returns None on error."""
        try:
            if isinstance(s, bytes):
                s = s.decode("utf-8")
            return cls.from_dict(json.loads(s))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"JSON/Encoding error: {e}")
            return None
        except (TypeError, ValueError, KeyError, AttributeError) as e:
            logger.error(f"Envelope parsing error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected parsing error: {e}")
            return None
