from __future__ import annotations
import logging
import logging.handlers
from typing import Any, Dict, Optional, TYPE_CHECKING

from kamio.plugins.base import Plugin
from kamio.plugins.loader import PluginContext

if TYPE_CHECKING:
    from kamio.app import KamioApp
    from kamio.core.event_bus import EventBus


class LoggingPlugin(Plugin):
    """
    Built-in plugin that logs all Kamio events to a rotating file.

    Configuration keys:
        file     (str)  : Log file path. Default: 'Kamio.log'.
        level    (str)  : Log level name. Default: 'INFO'.
        max_bytes (int) : Max log file size before rotation. Default: 10 MB.
        backup_count (int): Number of rotated files to keep. Default: 3.
    """

    @property
    def name(self) -> str:
        return "logging"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Logs all Kamio framework events to a rotating file."

    def configure(self, config: Dict[str, Any]) -> None:
        super().configure(config)
        self._file: str = config.get("file", "Kamio.log")
        self._level: int = getattr(logging, config.get("level", "INFO").upper(), logging.INFO)
        self._event_level: int = getattr(
            logging, config.get("event_level", "INFO").upper(), logging.INFO
        )
        self._max_bytes: int = config.get("max_bytes", 10 * 1024 * 1024)
        self._backup_count: int = config.get("backup_count", 3)
        self._handler: logging.Handler | None = None

    async def on_load(self, app: "KamioApp", context: Optional["PluginContext"] = None) -> None:
        handler = logging.handlers.RotatingFileHandler(
            self._file,
            maxBytes=self._max_bytes,
            backupCount=self._backup_count,
            encoding="utf-8",
        )
        handler.setLevel(self._level)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        self._handler = handler
        logging.getLogger("Kamio").addHandler(handler)
        self.logger.info(
            f"LoggingPlugin active — file={self._file} level={logging.getLevelName(self._level)}"
        )

    async def on_unload(self, app: "KamioApp") -> None:
        if self._handler:
            logging.getLogger("Kamio").removeHandler(self._handler)
            self._handler.close()
            self._handler = None

    def subscribe_events(self, ctx: "PluginContext") -> None:
        _EVENTS = [
            "app_start",
            "app_stop",
            "device_added",
            "device_removed",
            "device_state_changed",
            "device_command_executed",
            "rule_triggered",
            "rule_failed",
            "plugin_loaded",
            "plugin_unloaded",
        ]
        for event_type in _EVENTS:
            ctx.subscribe(event_type, self._log_event)

    def _log_event(self, data: dict) -> None:
        self.logger.log(
            self._event_level,
            "event received: %s",
            {k: v for k, v in data.items() if k != "device"},
        )
