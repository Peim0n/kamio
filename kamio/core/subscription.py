from __future__ import annotations

import asyncio
import inspect
import logging
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple


class PriorityRegistry:
    """
    Generic priority-ordered registry for callbacks/hooks.

    Items are stored as tuples ``(priority, value)`` and returned in descending
    priority order.  Supports registration, removal by identity, listing,
    and clearing by key or globally.

    All mutations and reads are guarded by an :class:`threading.RLock` so the
    registry is safe to use from multiple threads (e.g. MQTT callbacks plus a
    metrics scrape thread).  Iteration during dispatch always operates on a
    snapshot copy returned by :meth:`list`.
    """

    def __init__(self) -> None:
        """Initialize the priority registry with an optional logger."""
        # {key: [(priority, value), ...]}
        self._items: Dict[str, List[Tuple[int, Any]]] = {}
        self._lock = threading.RLock()

    def add(self, key: str, value: Any, priority: int = 0) -> None:
        """Register a value under key with the given priority.

        Uses binary search for O(n) insertion instead of re-sorting the
        entire list on every registration.

        Items with equal priority are inserted **after** existing items
        (LIFO order for equal priorities).  This means that if two callbacks
        share the same priority, the most recently registered one executes
        last among that priority group.
        """
        with self._lock:
            if key not in self._items:
                self._items[key] = []
            items = self._items[key]
            # Find insertion point for descending priority order.
            lo, hi = 0, len(items)
            while lo < hi:
                mid = (lo + hi) // 2
                if items[mid][0] >= priority:
                    lo = mid + 1
                else:
                    hi = mid
            items.insert(lo, (priority, value))

    def remove(
        self, key: str, value: Any, predicate: Optional[Callable[[Any, Any], bool]] = None
    ) -> None:
        """
        Remove values under key.

        ``predicate(stored_value, value)`` should return ``True`` for items that
        should be *kept* (default: keep items whose identity differs from ``value``).
        """
        with self._lock:
            if key not in self._items:
                return
            if predicate is None:
                predicate = lambda stored, ref: stored is not ref
            self._items[key] = [(p, v) for p, v in self._items[key] if predicate(v, value)]

    def list(self, key: str) -> List[Any]:
        """Return all values registered under key in priority order (snapshot)."""
        with self._lock:
            return [v for _, v in self._items.get(key, [])]

    def keys(self) -> List[str]:
        """Return all keys that have at least one registered value."""
        with self._lock:
            return [k for k, items in self._items.items() if items]

    def clear(self, key: Optional[str] = None) -> None:
        """Clear values for a specific key, or all keys if None."""
        with self._lock:
            if key is None:
                self._items.clear()
            else:
                self._items.pop(key, None)


class AsyncPriorityDispatcher:
    """
    Shared base for async callback dispatchers.

    Subclasses provide public naming (subscribe/register) and optional
    per-callback filtering/wrapping by overriding ``_prepare_callback``
    and ``_invoke``.
    """

    def __init__(self, logger_name: str) -> None:
        """Initialize the async dispatcher with a logger name."""
        self._registry = PriorityRegistry()
        self.logger = logging.getLogger(logger_name)

    def _register(self, event_type: str, callback: Any, priority: int = 0) -> None:
        """Register an item with priority under an event type."""
        self._registry.add(event_type, callback, priority=priority)

    def _unregister(
        self,
        event_type: str,
        callback: Any,
        predicate: Optional[Callable[[Any, Any], bool]] = None,
    ) -> None:
        """Remove an item by identity using a predicate."""
        self._registry.remove(event_type, callback, predicate=predicate)

    def _list(self, event_type: str) -> List[Any]:
        """List items for an event type in priority order."""
        return self._registry.list(event_type)

    def _event_types(self) -> List[str]:
        """Return all event types with at least one item."""
        return self._registry.keys()

    def _clear(self, event_type: Optional[str] = None) -> None:
        """Clear items for an event type or all."""
        self._registry.clear(event_type)

    async def _dispatch(
        self,
        event_type: str,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Invoke all callbacks for event_type in priority order."""
        for callback in self._registry.list(event_type):
            try:
                prepared = self._prepare_callback(callback)
                await self._invoke(prepared, *args, **kwargs)
            except Exception as e:
                self.logger.error(
                    f"Error in callback for event '{event_type}': {e}",
                    exc_info=True,
                )

    def _prepare_callback(self, callback: Any) -> Any:
        """Optional hook for wrapping/filtering callbacks."""
        return callback

    async def _invoke(self, callback: Any, *args: Any, **kwargs: Any) -> None:
        """Invoke a single callback, supporting sync and async callables."""
        if inspect.iscoroutinefunction(callback):
            await callback(*args, **kwargs)
        else:
            callback(*args, **kwargs)
