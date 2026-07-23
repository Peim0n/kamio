from __future__ import annotations
import os
import json
import logging
from dataclasses import dataclass, fields
from typing import Any, Callable, Dict, Optional

# Valid logging level names accepted in config files / env vars.
_LOG_LEVEL_NAMES = {"DEBUG", "INFO", "WARNING", "WARN", "ERROR", "CRITICAL", "FATAL", "NOTSET"}


@dataclass(frozen=True)
class Settings:
    """Typed, validated settings for Kamio Core."""

    mqtt_broker: str = "mqtt://localhost:1883"
    log_level: str = "INFO"


class Config:
    """
    Configuration management for Kamio Core.

    Supports loading configuration from JSON files and overriding with
    environment variables. Priority:
        Environment Variable > Config File > Default.

    Environment variables use the ``Kamio_`` prefix. Nested keys can be
    expressed with a double underscore, e.g. ``Kamio_MQTT__TLS__CAFILE``
    maps to ``mqtt.tls.cafile``.

    Args:
        config_path: Optional path to a JSON configuration file.
    """

    def __init__(self, config_path: Optional[str] = None) -> None:
        self.logger = logging.getLogger("Kamio.config")
        raw: Dict[str, Any] = {}

        if config_path:
            if not os.path.exists(config_path):
                self.logger.warning(
                    f"Config file not found: '{config_path}'. "
                    f"Running with defaults. Check the path or set Kamio_* env vars."
                )
            else:
                try:
                    with open(config_path, "r", encoding="utf-8") as f:
                        raw = json.load(f)
                    if not isinstance(raw, dict):
                        raise ValueError(f"Config file '{config_path}' must contain a JSON object.")
                    self.logger.info(
                        f"Loaded config from '{config_path}' ({len(raw)} top-level keys)"
                    )
                except json.JSONDecodeError as e:
                    raise ValueError(
                        f"Config file '{config_path}' contains invalid JSON "
                        f"at line {e.lineno}, col {e.colno}: {e.msg}"
                    ) from e
                except OSError as e:
                    raise OSError(f"Cannot read config file '{config_path}': {e}") from e

        # Overlay environment variables on top of file values.
        raw = self._overlay_env(raw)

        # Build typed settings from known keys.
        settings_kwargs: Dict[str, Any] = {}
        known_field_names = {f.name for f in fields(Settings)}
        for key, value in list(raw.items()):
            if key in known_field_names:
                settings_kwargs[key] = value
                del raw[key]

        self._settings = self._validate_settings(settings_kwargs)
        # Remaining keys are arbitrary user-defined values.
        self._extra: Dict[str, Any] = raw

    @property
    def settings(self) -> Settings:
        """Return the typed, validated settings object."""
        return self._settings

    @property
    def mqtt_broker(self) -> str:
        """MQTT broker URL."""
        return self._settings.mqtt_broker

    @property
    def log_level(self) -> int:
        """Logging level as a Python logging constant."""
        level_str = self._settings.log_level.upper()
        return getattr(logging, level_str, logging.INFO)

    def get(
        self,
        key: str,
        default: Any = None,
        cast: Optional[Callable] = None,
    ) -> Any:
        """
        Get a configuration value.

        Priority: Environment Variable (Kamio_KEY) > Config File > Default.
        Nested keys can be requested using dot notation (e.g. ``mqtt.tls.cafile``).

        Args:
            key: Configuration key, possibly with dot notation.
            default: Default value if key not found.
            cast: Optional type to coerce the value (int, float, bool).

        Returns:
            The configuration value.
        """
        # Known flat keys are served from typed settings.
        if "." not in key and hasattr(self._settings, key):
            value = getattr(self._settings, key)
        else:
            value = self._get_nested(self._extra, key)
            if value is None and "." not in key:
                # Fall back to environment for keys not present anywhere else.
                env_key = f"Kamio_{key.upper()}"
                # Case-insensitive lookup for Windows compatibility
                value = next((v for k, v in os.environ.items() if k.upper() == env_key.upper()), None)
            if value is None:
                value = default

        if cast is not None:
            if value is None:
                return default
            if cast is bool and isinstance(value, str):
                return value.lower() in ("true", "1", "yes", "on")
            try:
                return cast(value)
            except (ValueError, TypeError) as e:
                self.logger.warning(
                    f"Config key '{key}': cannot cast {value!r} to {cast.__name__}: {e}"
                )
                return default

        return value

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_nested(data: Dict[str, Any], key: str) -> Any:
        """Look up a dot-separated key in a nested dict. Returns None if missing."""
        parts = key.split(".")
        current: Any = data
        for part in parts:
            if not isinstance(current, dict) or part not in current:
                return None
            current = current[part]
        return current

    @staticmethod
    def _set_nested(data: Dict[str, Any], key: str, value: Any) -> None:
        """Set a value in a nested dict using dot-separated key."""
        parts = key.split(".")
        current = data
        for part in parts[:-1]:
            if part not in current or not isinstance(current[part], dict):
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value

    def _overlay_env(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Overlay Kamio_* environment variables onto the configuration."""
        result: Dict[str, Any] = dict(data)
        prefix = "Kamio_"
        for env_key, env_value in os.environ.items():
            # Case-insensitive prefix matching for Windows compatibility
            if not env_key.upper().startswith(prefix.upper()):
                continue
            relative = env_key[len(prefix) :]
            if "__" in relative:
                # Nested key: Kamio_MQTT__TLS__CAFILE -> mqtt.tls.cafile
                dotted = relative.replace("__", ".").lower()
                self._set_nested(result, dotted, env_value)
            else:
                # Flat key: Kamio_MQTT_BROKER -> mqtt_broker
                config_key = relative.lower()
                result[config_key] = env_value
        return result

    def _validate_settings(self, kwargs: Dict[str, Any]) -> Settings:
        """Validate and return a Settings dataclass instance."""
        broker = kwargs.get("mqtt_broker", Settings.mqtt_broker)
        if not isinstance(broker, str):
            self.logger.warning(
                f"mqtt_broker must be a string, got {type(broker).__name__}; using default"
            )
            broker = Settings.mqtt_broker

        level = str(kwargs.get("log_level", Settings.log_level)).upper()
        if level not in _LOG_LEVEL_NAMES:
            self.logger.warning(f"Invalid log_level '{level}'; falling back to INFO")
            level = "INFO"

        return Settings(mqtt_broker=broker, log_level=level)

    def __repr__(self) -> str:
        return f"<Config broker={self.mqtt_broker!r} log_level={self._settings.log_level!r}>"
