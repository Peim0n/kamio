from __future__ import annotations
from typing import Any, Callable, Dict


class HookEventFacadeMixin:
    """Convenience methods for lifecycle hooks and the public event bus."""

    def register_hook(self: Any, event_type: str, hook: Callable, priority: int = 0) -> None:
        """Register a lifecycle hook."""
        self.hooks.register(event_type, hook, priority)

    def unregister_hook(self: Any, event_type: str, hook: Callable) -> None:
        """Remove a previously registered lifecycle hook by callable identity."""
        self.hooks.unregister(event_type, hook)

    def subscribe_event(
        self: Any,
        event_type: str,
        callback: Callable,
        filter_fn=None,
        priority: int = 0,
    ) -> None:
        """Subscribe to an application event."""
        self.event_bus.subscribe(event_type, callback, filter_fn=filter_fn, priority=priority)

    def unsubscribe_event(self: Any, event_type: str, callback: Callable) -> None:
        """Unsubscribe a previously registered event callback by callable identity."""
        self.event_bus.unsubscribe(event_type, callback)

    async def publish_event(self: Any, event_type: str, data: Dict[str, Any]) -> None:
        """Publish a custom event to the application event bus."""
        await self.event_bus.publish(event_type, data)
