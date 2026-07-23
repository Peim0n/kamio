from __future__ import annotations
"""
Automation layer — rules, event bus, hooks, and subscriptions.

Re-exports the relevant modules from ``Kamio.core`` for logical grouping.
Physical files remain in ``Kamio/core/`` for import compatibility.
"""
from kamio.core.rules import Rule, RuleEngine, RuleEvent
from kamio.core.event_bus import EventBus
from kamio.core.hooks import HooksManager
from kamio.core.subscription import PriorityRegistry, AsyncPriorityDispatcher

__all__ = [
    "Rule", "RuleEngine", "RuleEvent",
    "EventBus",
    "HooksManager",
    "PriorityRegistry", "AsyncPriorityDispatcher",
]
