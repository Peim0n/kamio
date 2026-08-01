from __future__ import annotations

import inspect
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from .subscription import AsyncPriorityDispatcher


def _now() -> datetime:
    return datetime.now(timezone.utc)


class EventBus(AsyncPriorityDispatcher):
    """
    Public event bus for application-level pub/sub messaging.

    Differs from HooksManager:
    - HooksManager handles internal lifecycle interception (framework internals).
    - EventBus is a public API for user-defined event-driven logic.

    Features:
    - subscribe/unsubscribe with per-subscriber filter and priority
    - Sync and async callbacks (auto-detected)
    - Sequential execution in priority order (highest first)
    - Errors in callbacks are logged and do not stop other subscribers
    """

    def __init__(self) -> None:
        super().__init__("Kamio.event_bus")

    def subscribe(
        self,
        event_type: str,
        callback: Callable,
        filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
        priority: int = 0,
    ) -> None:
        """
        Subscribe to an event type.

        Args:
            event_type: Event name (e.g. 'device_state_changed').
            callback: Sync or async callable receiving the event data dict.
            filter_fn: Optional predicate(data) -> bool. Callback is skipped when False.
            priority: Subscribers with higher value execute first (default 0).
        """
        self._register(event_type, (callback, filter_fn), priority=priority)

    def unsubscribe(self, event_type: str, callback: Callable) -> None:
        """Remove a subscriber by callback identity."""
        self._unregister(
            event_type,
            callback,
            predicate=lambda stored, ref: stored[0] is not ref,
        )

    def list_subscribers(self, event_type: str) -> List[Callable]:
        """Return all callbacks subscribed to event_type, in priority order."""
        return [cb for cb, _ in self._list(event_type)]

    def event_types(self) -> List[str]:
        """Return all event types that have at least one subscriber."""
        return self._event_types()

    def clear(self, event_type: Optional[str] = None) -> None:
        """Clear subscribers for a specific event type, or all if None."""
        self._clear(event_type)

    async def publish(self, event_type: str, data: Dict[str, Any]) -> None:
        """
        Publish an event to all matching subscribers.

        Automatically adds 'timestamp' to data if not present (or if explicitly
        set to ``None``).  Applies per-subscriber filter before calling.
        Errors in callbacks are caught, logged, and do not stop others.
        """
        if not data.get("timestamp"):
            data = {"timestamp": _now(), **data}
        await self._dispatch(event_type, data)

    async def _invoke(self, item: Any, data: Dict[str, Any]) -> None:
        """Invoke a subscriber after applying its optional filter."""
        callback, filter_fn = item
        if filter_fn is not None:
            try:
                passes = filter_fn(data)
            except Exception as fe:
                self.logger.error(f"Error in filter for subscriber '{callback.__name__}': {fe}")
                return
            if not passes:
                return

        if inspect.iscoroutinefunction(callback):
            await callback(data)
        else:
            callback(data)
