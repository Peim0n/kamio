from __future__ import annotations
import json
import time
import logging
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional, Dict, Union

logger = logging.getLogger("synapse.envelope")

SERVER_ID = "0"

class EnvelopeType(str, Enum):
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
    BATCH = "batch"
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
        return cls(source=source, type=EnvelopeType.DEVICE_TELEMETRY, data=data)

    @classmethod
    def state(cls, source: str, data: dict) -> Envelope:
        return cls(source=source, type=EnvelopeType.DEVICE_STATE, data=data)

    @classmethod
    def state_ack(cls, source: str, target: str, data: dict, cind: str) -> Envelope:
        return cls(source=source, target=target, type=EnvelopeType.STATE_ACK, data=data, cind=cind)

    @classmethod
    def event(cls, source: str, event_name: str, data: dict) -> Envelope:
        return cls(source=source, type=EnvelopeType.DEVICE_EVENT, data={"event": event_name, "payload": data})

    @classmethod
    def command(cls, source: str, target: str, method: str, params: Optional[dict] = None, cind: Optional[str] = None, meta: Optional[dict] = None) -> Envelope:
        kwargs = {
            "source": source,
            "target": target,
            "type": EnvelopeType.SERVER_COMMAND,
            "data": {"method": method, "params": params or {}},
            "meta": meta or {}
        }
        if cind:
            kwargs["cind"] = cind
        return cls(**kwargs)

    @classmethod
    def command_ack(cls, source: str, target: str, data: dict, cind: str) -> Envelope:
        return cls(source=source, target=target, type=EnvelopeType.COMMAND_ACK, data=data, cind=cind)

    @classmethod
    def keepalive(cls, source: str) -> Envelope:
        return cls(source=source, type=EnvelopeType.KEEPALIVE, data={})

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target,
            "type": self.type.value,
            "cind": self.cind,
            "ts": self.ts,
            "data": self.data,
            "meta": self.meta
        }

    def to_json(self) -> str:
        try:
            return json.dumps(self.to_dict())
        except Exception as e:
            logger.error(f"Serialization error: {e}")
            raise ValueError("Failed to serialize Envelope to JSON")

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Optional[Envelope]:
        try:
            raw_type = d.get("type", "unknown")
            try:
                env_type = EnvelopeType(raw_type)
            except ValueError:
                env_type = EnvelopeType.UNKNOWN

            target = d.get("target")
            return cls(
                source=str(d.get("source", "")),
                target=str(target) if target is not None else None,
                type=env_type,
                cind=str(d.get("cind", "")),
                ts=float(d.get("ts", time.time())),
                data=d.get("data") if isinstance(d.get("data"), dict) else {},
                meta=d.get("meta") if isinstance(d.get("meta"), dict) else {}
            )
        except Exception as e:
            logger.error(f"Dict parsing error: {e}")
            return None

    @classmethod
    def from_json(cls, s: Union[str, bytes]) -> Optional[Envelope]:
        try:
            if isinstance(s, bytes):
                s = s.decode('utf-8')
            return cls.from_dict(json.loads(s))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"JSON/Encoding error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected parsing error: {e}")
            return None
