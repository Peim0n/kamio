from __future__ import annotations
from collections import defaultdict
from typing import Any, Dict, Optional, TYPE_CHECKING

from kamio.plugins.base import Plugin
from kamio.plugins.loader import PluginContext

if TYPE_CHECKING:
    from kamio.app import KamioApp
    from kamio.core.event_bus import EventBus


class MetricsPlugin(Plugin):
    """
    Built-in plugin that collects lightweight in-memory counters.

    Exposes collected metrics via get_metrics() / get_counter().

    Configuration keys:
        (none required)
    """

    @property
    def name(self) -> str:
        return "metrics"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Collects in-memory event counters for all Kamio events."

    def __init__(self) -> None:
        super().__init__()
        self._counters: Dict[str, int] = defaultdict(int)

    async def on_load(self, app: "KamioApp", context: Optional["PluginContext"] = None) -> None:
        self.logger.info("MetricsPlugin active")

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
            ctx.subscribe(event_type, self._count_event(event_type))

    def _count_event(self, event_type: str):
        def handler(data: dict) -> None:
            self._counters[event_type] += 1

        return handler

    def get_counter(self, event_type: str) -> int:
        """Return the count for a specific event type."""
        return self._counters.get(event_type, 0)

    def get_metrics(self) -> Dict[str, int]:
        """Return a copy of all collected counters."""
        return dict(self._counters)

    def reset(self) -> None:
        """Reset all counters to zero."""
        self._counters.clear()
