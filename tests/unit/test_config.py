from __future__ import annotations

import json
import logging
import os

import pytest

from kamio.config import Config


@pytest.fixture
def sample_config(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "mqtt_broker": "mqtt://broker.local:1883",
                "log_level": "INFO",
                "nested": {"key": 42, "inner": {"value": 7}},
            }
        ),
        encoding="utf-8",
    )
    return path


def test_config_reads_json_file(sample_config):
    config = Config(config_path=str(sample_config))
    assert config.get("mqtt_broker") == "mqtt://broker.local:1883"
    assert config.get("log_level") == "INFO"


def test_config_environment_variable_overrides_file(monkeypatch, sample_config):
    monkeypatch.setenv("Kamio_MQTT_BROKER", "mqtt://env.local:1883")
    config = Config(config_path=str(sample_config))
    assert config.get("mqtt_broker") == "mqtt://env.local:1883"


def test_config_get_with_cast(monkeypatch, sample_config):
    monkeypatch.setenv("Kamio_NESTED__KEY", "100")
    config = Config(config_path=str(sample_config))
    assert config.get("nested.key", cast=int) == 100


def test_config_get_returns_default_when_missing():
    config = Config()
    assert config.get("missing_key", "default") == "default"
    assert config.get("missing_number", 0, cast=int) == 0


def test_config_nested_keys_dot_notation(sample_config):
    config = Config(config_path=str(sample_config))
    assert config.get("nested.key") == 42
    assert config.get("nested.inner.value") == 7


def test_config_underscore_env_maps_to_nested_key(monkeypatch, sample_config):
    monkeypatch.setenv("Kamio_NESTED__INNER__VALUE", "99")
    config = Config(config_path=str(sample_config))
    assert config.get("nested.inner.value", cast=int) == 99


def test_config_mqtt_broker_property(sample_config):
    config = Config(config_path=str(sample_config))
    assert config.mqtt_broker == "mqtt://broker.local:1883"


def test_config_log_level_property(sample_config):
    config = Config(config_path=str(sample_config))
    assert config.log_level == logging.INFO
