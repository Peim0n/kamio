"""Tests for envelope, base driver, config, discovery, data_fields."""

from __future__ import annotations

import json
import logging

import pytest

from kamio.config import Config
from kamio.core.envelope import SERVER_ID, Envelope, EnvelopeType
from kamio.data_fields import (
    Field,
    config,
    event,
    parse_freq,
    state,
    telemetry,
)
from kamio.discovery import HADiscovery
from kamio.drivers.base import BaseDriver


# ---------------------------------------------------------------------------
# Envelope
# ---------------------------------------------------------------------------
def test_envelope_factory_methods():
    e = Envelope.telemetry("dev1", {"temp": 21})
    assert e.type == EnvelopeType.DEVICE_TELEMETRY
    assert e.source == "dev1"

    e = Envelope.state("dev1", {"power": True})
    assert e.type == EnvelopeType.DEVICE_STATE

    e = Envelope.state_ack("dev1", "server", {"result": {}}, "cind123")
    assert e.type == EnvelopeType.STATE_ACK
    assert e.cind == "cind123"

    e = Envelope.event("dev1", "motion", {"zone": 1})
    assert e.type == EnvelopeType.DEVICE_EVENT
    assert e.data["event"] == "motion"
    assert e.data["payload"] == {"zone": 1}

    e = Envelope.event("dev1", "motion", data={"zone": 2})
    assert e.data["payload"] == {"zone": 2}

    e = Envelope.event("dev1", "motion")
    assert e.data["payload"] == {}

    e = Envelope.command("server", "dev1", "activate", {"mode": "on"})
    assert e.type == EnvelopeType.SERVER_COMMAND
    assert e.data["method"] == "activate"
    assert e.data["params"] == {"mode": "on"}

    e = Envelope.command("server", "dev1", "activate", cind="abc")
    assert e.cind == "abc"

    e = Envelope.command("server", "dev1", "activate", meta={"priority": 1})
    assert e.meta == {"priority": 1}

    e = Envelope.command_ack("dev1", "server", {"result": "ok"}, "abc")
    assert e.type == EnvelopeType.COMMAND_ACK

    e = Envelope.keepalive("dev1")
    assert e.type == EnvelopeType.KEEPALIVE
    assert e.target == "dev1"


def test_envelope_to_dict_and_json():
    e = Envelope.telemetry("dev1", {"temp": 21})
    d = e.to_dict()
    assert d["source"] == "dev1"
    assert d["type"] == "dt"
    assert d["data"] == {"temp": 21}

    j = e.to_json()
    assert isinstance(j, str)
    parsed = json.loads(j)
    assert parsed["source"] == "dev1"


def test_envelope_from_dict_valid():
    d = {"source": "dev1", "type": "ds", "data": {"power": True}, "cind": "abc"}
    e = Envelope.from_dict(d)
    assert e is not None
    assert e.source == "dev1"
    assert e.type == EnvelopeType.DEVICE_STATE
    assert e.cind == "abc"


def test_envelope_from_dict_unknown_type():
    d = {"source": "dev1", "type": "xyz", "data": {}}
    e = Envelope.from_dict(d)
    assert e is not None
    assert e.type == EnvelopeType.UNKNOWN


def test_envelope_from_dict_missing_data():
    d = {"source": "dev1", "type": "ds"}
    e = Envelope.from_dict(d)
    assert e is not None
    assert e.data == {}
    assert e.meta == {}


def test_envelope_from_dict_invalid():
    # Missing source should still work (defaults to "")
    e = Envelope.from_dict({"type": "ds"})
    assert e is not None
    assert e.source == ""


def test_envelope_from_json_valid():
    j = '{"source": "dev1", "type": "ds", "data": {"x": 1}, "cind": "c"}'
    e = Envelope.from_json(j)
    assert e is not None
    assert e.source == "dev1"


def test_envelope_from_json_bytes():
    j = b'{"source": "dev1", "type": "ds", "data": {}}'
    e = Envelope.from_json(j)
    assert e is not None


def test_envelope_from_json_invalid():
    assert Envelope.from_json("not json") is None
    assert Envelope.from_json(b"\xff\xfe") is None


def test_envelope_to_json_with_non_serializable():
    # to_json uses default=str so even non-standard types get serialized.
    e = Envelope("dev1", EnvelopeType.DEVICE_STATE, {"obj": {1, 2, 3}})
    j = e.to_json()
    assert isinstance(j, str)
    assert "dev1" in j


# ---------------------------------------------------------------------------
# BaseDriver
# ---------------------------------------------------------------------------
def test_base_driver_is_abstract():
    with pytest.raises(TypeError):
        BaseDriver()  # type: ignore[abstract]


def test_base_driver_repr():
    class MyDriver(BaseDriver):
        async def connect(self):
            pass

        async def disconnect(self):
            pass

        async def execute(self, command_name, params):
            return {}

        async def read(self, field_name, params=None):
            return {}

    d = MyDriver()
    assert "MyDriver" in repr(d)
    assert d.logger is not None


@pytest.mark.asyncio
async def test_base_driver_context_manager():
    class MyDriver(BaseDriver):
        def __init__(self):
            super().__init__()
            self.connected = False

        async def connect(self):
            self.connected = True

        async def disconnect(self):
            self.connected = False

        async def execute(self, command_name, params):
            return {}

        async def read(self, field_name, params=None):
            return {}

    d = MyDriver()
    async with d as driver:
        assert driver.connected is True
    assert d.connected is False


# ---------------------------------------------------------------------------
# Config additional coverage
# ---------------------------------------------------------------------------
def test_config_file_not_found(tmp_path):
    config = Config(config_path=str(tmp_path / "nonexistent.json"))
    assert config.mqtt_broker == "mqtt://localhost:1883"


def test_config_invalid_json(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text("{invalid json", encoding="utf-8")
    with pytest.raises(ValueError, match="invalid JSON"):
        Config(config_path=str(p))


def test_config_not_a_dict(tmp_path):
    p = tmp_path / "list.json"
    p.write_text("[1, 2, 3]", encoding="utf-8")
    with pytest.raises(ValueError, match="must contain a JSON object"):
        Config(config_path=str(p))


def test_config_os_error(tmp_path):
    # Create a directory with the same name to cause open() to fail
    p = tmp_path / "dir.json"
    p.mkdir()
    with pytest.raises(OSError):
        Config(config_path=str(p))


def test_config_mqtt_broker_not_string():
    import os

    # Set env to a non-string by injecting directly into the raw dict
    config = Config()
    config._extra["mqtt_broker"] = 12345  # type: ignore
    # _validate_settings should warn and fall back to default
    config._settings = config._validate_settings({"mqtt_broker": 12345})
    assert config.mqtt_broker == "mqtt://localhost:1883"


def test_config_invalid_log_level():
    import os

    os.environ["Kamio_LOG_LEVEL"] = "BOGUS"
    try:
        config = Config()
        assert config.log_level == logging.INFO
    finally:
        del os.environ["Kamio_LOG_LEVEL"]


def test_config_get_with_cast():
    config = Config()
    assert config.get("nonexistent", default="5", cast=int) == 5


def test_config_get_cast_bool():
    config = Config()
    assert config.get("nonexistent", default="true", cast=bool) is True
    assert config.get("nonexistent", default="false", cast=bool) is False


def test_config_get_cast_failure():
    config = Config()
    assert config.get("nonexistent", default="abc", cast=int) == "abc"


def test_config_get_cast_none_value():
    config = Config()
    assert config.get("nonexistent", default=None, cast=int) is None


def test_config_nested_get():
    config = Config()
    # _extra is empty by default
    assert config.get("a.b.c", default="fallback") == "fallback"


def test_config_repr():
    config = Config()
    r = repr(config)
    assert "Config" in r
    assert "mqtt://localhost:1883" in r


# ---------------------------------------------------------------------------
# Discovery additional coverage
# ---------------------------------------------------------------------------
def test_ha_discovery_init():
    d = HADiscovery(discovery_prefix="custom_prefix")
    assert d.discovery_prefix == "custom_prefix"


def test_ha_discovery_map_telemetry():
    d = HADiscovery()
    f = Field(name="temp", kind="telemetry", python_type=float, unit="C")
    assert d._map_to_ha_component(f) == "sensor"


def test_ha_discovery_map_state_bool_writable():
    d = HADiscovery()
    f = Field(name="power", kind="state", python_type=bool, writable=True)
    assert d._map_to_ha_component(f) == "switch"


def test_ha_discovery_map_state_bool_readonly():
    d = HADiscovery()
    f = Field(name="motion", kind="state", python_type=bool, writable=False)
    assert d._map_to_ha_component(f) == "binary_sensor"


def test_ha_discovery_map_state_number_writable():
    d = HADiscovery()
    f = Field(name="level", kind="state", python_type=int, writable=True)
    assert d._map_to_ha_component(f) == "number"


def test_ha_discovery_map_state_select():
    d = HADiscovery()
    f = Field(name="mode", kind="state", python_type=str, writable=True, choices=["a", "b"])
    assert d._map_to_ha_component(f) == "select"


def test_ha_discovery_map_state_text_writable():
    d = HADiscovery()
    f = Field(name="label", kind="state", python_type=str, writable=True)
    assert d._map_to_ha_component(f) == "text"


def test_ha_discovery_map_state_sensor_readonly():
    d = HADiscovery()
    f = Field(name="count", kind="state", python_type=int, writable=False)
    assert d._map_to_ha_component(f) == "sensor"


def test_ha_discovery_map_unknown_kind():
    d = HADiscovery()
    f = Field(name="x", kind="config", python_type=str)
    assert d._map_to_ha_component(f) == ""


def test_ha_discovery_map_with_default_type():
    d = HADiscovery()
    f = Field(name="x", kind="telemetry", python_type=None, default=42)
    assert d._map_to_ha_component(f) == "sensor"


# ---------------------------------------------------------------------------
# data_fields additional coverage
# ---------------------------------------------------------------------------
def test_field_set_name():
    f = Field(name="", kind="state", default=0)

    class Owner:
        x = f

    assert f.name == "x"


def test_field_get_from_instance():
    f = Field(name="x", kind="state", default=42)

    class Owner:
        x = f

    obj = Owner()
    assert f.__get__(obj) == 42
    # Accessing from class returns the Field itself
    assert f.__get__(None) is f


def test_telemetry_factory():
    f = telemetry(default=21.0, unit="C", description="Temperature")
    assert f.kind == "telemetry"
    assert f.default == 21.0
    assert f.unit == "C"
    assert f.description == "Temperature"


def test_state_factory():
    f = state(default=False, writable=True, min=0, max=100, choices=None)
    assert f.kind == "state"
    assert f.default is False
    assert f.writable is True
    assert f.min == 0
    assert f.max == 100


def test_event_factory():
    f = event(description="Motion detected", zone="living")
    assert f.kind == "event"
    assert f.description == "Motion detected"
    assert f.metadata == {"zone": "living"}


def test_config_factory():
    f = config(default="mqtt://localhost")
    assert f.kind == "config"
    assert f.default == "mqtt://localhost"
    assert f.writable is True


def test_parse_freq_float():
    assert parse_freq(3.14) == 3.14


def test_parse_freq_int():
    assert parse_freq(42) == 42.0
