from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, Optional

from kamio.plugins.base import Plugin
from kamio.plugins.loader import PluginContext

if TYPE_CHECKING:
    from kamio.app import KamioApp
    from kamio.core.event_bus import EventBus


class MetricsPlugin(Plugin):
    """
    Built-in plugin that collects lightweight in-memory counters.

    Exposes collected metrics via get_metrics() / get_counter().

    All counter mutations and reads are guarded by an :class:`asyncio.Lock`
    so the plugin is safe to use within the event loop without blocking it.

    Configuration keys:
        (none required)
    """

    @property
    def name(self) -> str:
        """Return the plugin name 'metrics'."""
        return "metrics"

    @property
    def version(self) -> str:
        """Return the plugin version."""
        return "1.0.0"

    @property
    def description(self) -> str:
        """Return a human-readable description."""
        return "Collects in-memory event counters for all Kamio events."

    def __init__(self) -> None:
        """Initialize the metrics plugin with empty counters."""
        super().__init__()
        self._counters: Dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()

    async def on_load(self, app: "KamioApp", context: Optional["PluginContext"] = None) -> None:
        """Initialize the event counters dict."""
        self.logger.info("MetricsPlugin active")

    async def on_unload(self, app: "KamioApp") -> None:
        """Reset all counters when the plugin is unloaded to avoid stale state."""
        await self.reset()
        self.logger.info("MetricsPlugin unloaded, counters reset")

    def subscribe_events(self, ctx: "PluginContext") -> None:
        """Subscribe to all system events for counting."""
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
        async def handler(data: dict) -> None:
            async with self._lock:
                self._counters[event_type] += 1

        return handler

    async def get_counter(self, event_type: str) -> int:
        """Return the count for a specific event type."""
        async with self._lock:
            return self._counters.get(event_type, 0)

    async def get_metrics(self) -> Dict[str, int]:
        """Return a copy of all collected counters."""
        async with self._lock:
            return dict(self._counters)

    async def reset(self) -> None:
        """Reset all counters to zero."""
        async with self._lock:
            self._counters.clear()
