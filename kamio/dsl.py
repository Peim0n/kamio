from __future__ import annotations

"""High-level, agent-friendly primitives.

Everything here produces ordinary Kamio objects (Device subclasses, Rule
instances) through explicit, serializable specifications — no hidden state,
no metaclass magic beyond what Device already does.

- :func:`make_device_class` — build a Device subclass from a dict spec.
- :func:`when` — declarative condition rule: ``when(Sensor, "temp", ">", 28)``
  ``.then_set(app, "fan", "power", True)`` registers a rule that writes a
  state field on another device when the condition holds.
"""

import operator as _op
from typing import Any, Callable, Dict, Optional, Type

from kamio.data_fields import Field
from kamio.device import Device

_OPS: Dict[str, Callable[[Any, Any], bool]] = {
    ">": _op.gt,
    "<": _op.lt,
    ">=": _op.ge,
    "<=": _op.le,
    "==": _op.eq,
    "!=": _op.ne,
}


def make_device_class(type_name: str, fields: Dict[str, Field]) -> Type[Device]:
    """Create a Device subclass from a ``{name: Field}`` spec.

    Example::

        Lamp = make_device_class("lamp", {"power": state(default=False)})
        await app.add_device("lamp1", Lamp)
    """
    if not type_name or not type_name.isidentifier():
        raise ValueError(f"make_device_class: invalid type name {type_name!r}")
    for name, fld in fields.items():
        if not isinstance(fld, Field):
            raise TypeError(
                f"make_device_class: field {name!r} must be created with "
                f"state()/telemetry()/config()/event(), got {type(fld).__name__}"
            )
    return type(type_name, (Device,), dict(fields))


class _WhenBuilder:
    """Fluent builder returned by :func:`when`."""

    def __init__(self, device: Type[Device], field: str, op: str, value: Any):
        self._spec = {"device": device, "field": field, "op": op, "value": value}

    def then_set(self, app, target_id: str, field: str, value: Any):
        """Register a rule on ``app`` that sets ``target.field = value``.

        The rule fires when the watched field's new value satisfies the
        comparison. The full spec is recorded on the rule function as
        ``_kamio_dsl`` so ``app.get_manifest()`` can describe it.
        """
        device_cls = self._spec["device"]
        field_w = self._spec["field"]
        op = self._spec["op"]
        threshold = self._spec["value"]
        cmp_fn = _OPS[op]

        async def _dsl_rule(event, app):
            current = event.data.get(field_w)
            if current is None or not cmp_fn(current, threshold):
                return
            target = app.devices.get(target_id)
            if target is not None:
                await target.handle_state({field: value})

        _dsl_rule.__name__ = f"when_{field_w}{op}{threshold}_set_{target_id}.{field}"
        _dsl_rule._kamio_dsl = {  # type: ignore[attr-defined]
            "when": {"device": device_cls.__name__, "field": field_w, "op": op, "value": threshold},
            "then": {"device_id": target_id, "field": field, "value": value},
        }
        app.add_rule(_dsl_rule, device=device_cls, fields=[field_w])
        return _dsl_rule


def when(device: Type[Device], field: str, op: str, value: Any) -> _WhenBuilder:
    """Start a declarative condition rule.

    ``op`` is one of ``">"``, ``"<"``, ``">="``, ``"<="``, ``"=="``, ``"!="``.
    """
    if op not in _OPS:
        raise ValueError(f"when: unsupported operator {op!r}; use one of {sorted(_OPS)}")
    if not (isinstance(device, type) and issubclass(device, Device)):
        raise TypeError(f"when: 'device' must be a Device subclass, got {device!r}")
    if field not in device.Kamio_FIELDS:
        raise ValueError(f"when: {device.__name__} has no field {field!r}")
    return _WhenBuilder(device, field, op, value)
