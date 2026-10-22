from __future__ import annotations

import logging

import pytest

from kamio import Device, config, state, telemetry
from kamio.discovery import HADiscovery


class FutureAnnotationsDevice(Device):
    """Device defined in a module with ``from __future__ import annotations``.

    Field annotations arrive at DeviceMeta as strings; the metaclass must
    resolve them to real types even though framework bases annotate
    TYPE_CHECKING-only names (LOC-01 regression test).
    """

    power: bool = state(default=False)
    alarm: bool = state(default=False, writable=False)
    brightness: int = state(default=100, min=0, max=255)
    temp: float = telemetry(default=0.0, freq="5s")
    setpoint: float = config(default=22.0)


def test_future_annotations_resolve_to_real_types():
    """python_type must be the actual type, not the annotation string."""
    assert FutureAnnotationsDevice.Kamio_FIELDS["power"].python_type is bool
    assert FutureAnnotationsDevice.Kamio_FIELDS["alarm"].python_type is bool
    assert FutureAnnotationsDevice.Kamio_FIELDS["brightness"].python_type is int
    assert FutureAnnotationsDevice.Kamio_FIELDS["temp"].python_type is float
    assert FutureAnnotationsDevice.Kamio_FIELDS["setpoint"].python_type is float


def test_no_fallback_warning_for_future_annotations(caplog):
    """Defining a Device must not emit the 'Could not resolve type hints' warning."""
    with caplog.at_level(logging.DEBUG, logger="Kamio.device_meta"):

        class AnotherDevice(Device):
            speed: float = telemetry(freq="10s")

        assert AnotherDevice.Kamio_FIELDS["speed"].python_type is float
    assert "Could not resolve" not in caplog.text


def test_ha_discovery_mapping_on_future_annotations():
    """HA component mapping must work on fields declared in a future-annotations module."""
    discovery = HADiscovery()
    fields = FutureAnnotationsDevice.Kamio_FIELDS
    assert discovery._map_to_ha_component(fields["power"]) == "switch"
    assert discovery._map_to_ha_component(fields["alarm"]) == "binary_sensor"
    assert discovery._map_to_ha_component(fields["brightness"]) == "number"
    assert discovery._map_to_ha_component(fields["temp"]) == "sensor"


def test_unresolvable_field_annotation_fails_loudly():
    """A field annotation that cannot be resolved must raise, not leave a string."""
    with pytest.raises(TypeError, match="Could not resolve field type annotations"):

        class BrokenDevice(Device):
            bad: NotARealType = state(default=0)  # noqa: F821
