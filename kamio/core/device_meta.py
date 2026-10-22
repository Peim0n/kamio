from __future__ import annotations

import dataclasses
import logging
import sys
from typing import Any, Dict, Optional, get_type_hints

from kamio.data_fields import Field

logger = logging.getLogger("Kamio.device_meta")


def _framework_localns() -> Dict[str, Any]:
    """Names referenced by annotations in Device's framework base classes.

    ``KamioApp``, ``BaseDriver``, and ``DeviceNode`` are imported under
    ``TYPE_CHECKING`` in their respective modules, so ``get_type_hints``
    cannot resolve them from module globals alone.  They are imported lazily
    here because ``device_meta`` itself is imported while ``kamio.app`` is
    still being initialized — a top-level import would be circular.  Missing
    names are fine: non-field annotations that fail to resolve are skipped by
    the per-field fallback in :func:`_resolve_field_hints`.
    """
    ns: Dict[str, Any] = {}
    try:
        from kamio.app import KamioApp

        ns["KamioApp"] = KamioApp
    except Exception:
        pass
    try:
        from kamio.drivers.base import BaseDriver

        ns["BaseDriver"] = BaseDriver
    except Exception:
        pass
    try:
        from kamio.core.mqtt_nodes import DeviceNode

        ns["DeviceNode"] = DeviceNode
    except Exception:
        pass
    return ns


def _resolve_field_hints(cls: type, field_names: set) -> Dict[str, Any]:
    """Resolve annotations only for field/event descriptors of ``cls``.

    Walks the MRO (base classes first, like ``get_type_hints``) and evaluates
    each field annotation with the declaring module's globals merged with the
    declaring class's namespace and Kamio framework names.  Non-field
    annotations (e.g. ``_app`` on mixins) are ignored entirely.

    Raises:
        TypeError: if a declared field annotation cannot be resolved — a raw
            string must never end up in ``Field.python_type`` silently.
    """
    resolved: Dict[str, Any] = {}
    errors: list = []
    for base in reversed(cls.__mro__):
        annotations = base.__dict__.get("__annotations__", {})
        if not annotations:
            continue
        module = sys.modules.get(base.__module__)
        globalns: Dict[str, Any] = dict(vars(module)) if module else {}
        localns = dict(vars(base))
        localns.update(_framework_localns())
        for name, value in annotations.items():
            if name not in field_names:
                continue
            try:
                # A quoted annotation under ``from __future__ import
                # annotations`` is stored as a string containing quotes
                # (e.g. ``'"SomeType"'``), so evaluating it can yield another
                # string — evaluate until the result is not a string.
                while isinstance(value, str):
                    value = eval(value, globalns, localns)
                resolved[name] = value
            except Exception as e:
                errors.append(f"{base.__name__}.{name}: {e}")
    if errors:
        raise TypeError(
            f"Could not resolve field type annotations for {cls.__name__!r}: " + "; ".join(errors)
        )
    return resolved


def _merge_from_bases(bases: tuple, attr: str, own: dict) -> dict:
    """Collect `attr` from all base classes (MRO order) then overlay `own` entries.

    Emits a warning when a child class shadows a parent's entry with the same
    name but a different type/value, so accidental overrides (e.g. changing
    ``power: bool`` to ``power: int``) are surfaced instead of silently applied.
    """
    result: dict = {}
    for base in bases:
        result.update(getattr(base, attr, {}))
    for key, value in own.items():
        if key in result:
            prev = result[key]
            # Compare by type for Field instances, by identity otherwise.
            if isinstance(prev, Field) and isinstance(value, Field):
                changed = prev.python_type != value.python_type or prev.kind != value.kind
            else:
                changed = prev is not value
            if changed:
                logger.warning(
                    f"Device field/command '{key}' in {attr} is overridden by a subclass "
                    f"(was {prev!r}, now {value!r}). This may break compatibility."
                )
        result[key] = value
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
        """Create a Device subclass, collecting field descriptors and command/event methods."""

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
        field_names = set(raw_fields) | set(raw_events)
        try:
            # We use get_type_hints to resolve string forward references.
            # localns supplies names (KamioApp, BaseDriver, DeviceNode) that
            # framework base classes annotate but only import under
            # TYPE_CHECKING — otherwise resolution fails for every subclass.
            resolved: dict[str, Any] = get_type_hints(
                cls, include_extras=True, localns=_framework_localns()
            )
        except Exception as e:
            # Whole-class resolution failed (e.g. a non-field annotation in a
            # user-defined mixin cannot be resolved).  Resolve only the field
            # annotations instead; _resolve_field_hints raises TypeError if a
            # field annotation itself is unresolvable, so a raw string can
            # never slip into Field.python_type silently.
            logger.debug(
                f"Could not resolve all type hints for {name!r}: {e}. "
                f"Resolving field annotations individually."
            )
            resolved = _resolve_field_hints(cls, field_names)

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
