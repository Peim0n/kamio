from __future__ import annotations

from typing import Any, Callable, List, Optional

from .subscription import AsyncPriorityDispatcher


class HooksManager(AsyncPriorityDispatcher):
    """
    Manages lifecycle hooks with support for sync/async callbacks and priorities.

    Higher priority value = executes first.
    Errors in hooks are logged and do not interrupt the main flow.
    """

    def __init__(self) -> None:
        super().__init__("Kamio.hooks")

    def register(self, event_type: str, hook: Callable, priority: int = 0) -> None:
        """Register a hook for the given event type."""
        self._register(event_type, hook, priority=priority)

    def unregister(self, event_type: str, hook: Callable) -> None:
        """Remove a specific hook for the given event type."""
        self._unregister(event_type, hook)

    def list_hooks(self, event_type: str) -> List[Callable]:
        """Return all registered hooks for an event type, ordered by priority."""
        return self._list(event_type)

    def clear(self, event_type: Optional[str] = None) -> None:
        """Clear hooks for a specific event type, or all hooks if None."""
        self._clear(event_type)

    async def trigger(self, event_type: str, *args: Any, **kwargs: Any) -> None:
        """
        Invoke all hooks registered for event_type in priority order.

        Supports both sync and async hook functions.
        Exceptions are caught, logged, and do not stop subsequent hooks.
        """
        await self._dispatch(event_type, *args, **kwargs)
