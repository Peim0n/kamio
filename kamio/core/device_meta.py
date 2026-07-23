from __future__ import annotations

import dataclasses
import logging
from typing import Any, Dict, Optional, get_type_hints
from kamio.data_fields import Field

logger = logging.getLogger("Kamio.device_meta")


def _merge_from_bases(bases: tuple, attr: str, own: dict) -> dict:
    """Collect `attr` from all base classes (MRO order) then overlay `own` entries."""
    result: dict = {}
    for base in bases:
        result.update(getattr(base, attr, {}))
    result.update(own)
    return result


class DeviceMeta(type):
    """
    Metaclass for declarative device description.
    Collects fields, commands, events, and rules into ClassVar during class creation.
    Supports both annotation and assignment styles.
    """

    Kamio_FIELDS: Dict[str, Field]
    Kamio_COMMANDS: Dict[str, Any]
    Kamio_EVENTS: Dict[str, Field]
    Kamio_RULES: Dict[str, Any]
    _cached_schema: Optional[Dict[str, Any]]

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> DeviceMeta:

        annotations: dict[str, Any] = namespace.get("__annotations__", {})

        # Temporary storage for current class
        raw_fields: dict[str, Field] = {}
        raw_events: dict[str, Field] = {}
        raw_commands: dict[str, Any] = {}
        raw_rules: dict[str, Any] = {}

        # 1. Collect metadata from namespace (assignment style)
        for attr_name, value in list(namespace.items()):
            # Check for @command
            if getattr(value, "_is_command", False):
                cmd_name = getattr(value, "_command_name", attr_name)
                raw_commands[cmd_name] = value
            # Check for @rule
            elif getattr(value, "_is_rule", False):
                raw_rules[attr_name] = value
            # Check for Field instances (telemetry, state, etc.)
            elif isinstance(value, Field):
                if value.kind == "event":
                    raw_events[attr_name] = value
                else:
                    raw_fields[attr_name] = value
                # Remove from namespace to avoid shadowing instance values
                del namespace[attr_name]

        # 2. Collect metadata from annotations (annotation style)
        # Note: If style is `temp: float = telemetry(...)`, the Field instance is in namespace,
        # and the type is in annotations. We already collected Fields from namespace above.
        # This loop handles cases where Field might be the annotation itself (rare but possible).
        for attr_name, attr_type in annotations.items():
            if isinstance(attr_type, Field):
                if attr_name not in raw_fields and attr_name not in raw_events:
                    if attr_type.kind == "event":
                        raw_events[attr_name] = attr_type
                    else:
                        raw_fields[attr_name] = attr_type

        # Create the class
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)

        # Initialize base collections for the root Device class
        if not any(isinstance(b, DeviceMeta) for b in bases):
            cls.Kamio_FIELDS = {}
            cls.Kamio_COMMANDS = {}
            cls.Kamio_EVENTS = {}
            cls.Kamio_RULES = {}
            cls._cached_schema = None
            return cls

        # Resolve type hints to get actual types for fields
        try:
            # We use get_type_hints to resolve string forward references
            resolved: dict[str, Any] = get_type_hints(cls)
        except Exception:
            resolved = annotations

        # Process and type fields
        fields: dict[str, Field] = {}
        for field_name, raw in raw_fields.items():
            # Try to get the type from annotations, if available
            field_type = resolved.get(field_name)

            # If the annotation itself was the Field object, try to extract the inner type if possible
            # or just use the Field's own type knowledge.
            if isinstance(field_type, Field):
                field_type = None  # Will fall back to Field's internal type if any

            fields[field_name] = dataclasses.replace(
                raw,
                name=field_name,
                python_type=field_type or raw.python_type,
            )

        cls.Kamio_FIELDS = _merge_from_bases(bases, "Kamio_FIELDS", fields)
        cls.Kamio_COMMANDS = _merge_from_bases(bases, "Kamio_COMMANDS", raw_commands)
        cls.Kamio_EVENTS = _merge_from_bases(bases, "Kamio_EVENTS", raw_events)
        cls.Kamio_RULES = _merge_from_bases(bases, "Kamio_RULES", raw_rules)

        # Reset schema cache
        cls._cached_schema = None

        return cls
