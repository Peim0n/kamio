from __future__ import annotations

from typing import Any, Callable, Dict


class HookEventFacadeMixin:
    """Convenience methods for lifecycle hooks and the public event bus."""

    def register_hook(self: Any, event_type: str, hook: Callable, priority: int = 0) -> None:
        """Register a lifecycle hook.

        Args:
            event_type: The lifecycle event type to hook into.
            hook: Callable invoked when the event fires.
            priority: Hook execution priority (default 0; lower runs first).

        Returns:
            None
        """
        self.hooks.register(event_type, hook, priority)

    def unregister_hook(self: Any, event_type: str, hook: Callable) -> None:
        """Remove a previously registered lifecycle hook by callable identity.

        Args:
            event_type: The lifecycle event type the hook was registered for.
            hook: The callable to remove.

        Returns:
            None
        """
        self.hooks.unregister(event_type, hook)

    def subscribe_event(
        self: Any,
        event_type: str,
        callback: Callable,
        filter_fn=None,
        priority: int = 0,
    ) -> None:
        """Subscribe to an application event.

        Args:
            event_type: The event type to subscribe to.
            callback: Callable invoked when the event is published.
            filter_fn: Optional filter callable; only events passing it reach the callback.
            priority: Subscription priority (default 0; lower runs first).

        Returns:
            None
        """
        self.event_bus.subscribe(event_type, callback, filter_fn=filter_fn, priority=priority)

    def unsubscribe_event(self: Any, event_type: str, callback: Callable) -> None:
        """Unsubscribe a previously registered event callback by callable identity.

        Args:
            event_type: The event type the callback was subscribed to.
            callback: The callable to remove.

        Returns:
            None
        """
        self.event_bus.unsubscribe(event_type, callback)

    async def publish_event(self: Any, event_type: str, data: Dict[str, Any]) -> None:
        """Publish a custom event to the application event bus.

        Args:
            event_type: The event type to publish.
            data: Dictionary payload delivered to subscribers.

        Returns:
            None
        """
        await self.event_bus.publish(event_type, data)
