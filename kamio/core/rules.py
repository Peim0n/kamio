from __future__ import annotations

import asyncio
import inspect
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Dict, List, Optional, Tuple, Type

if TYPE_CHECKING:
    from ..app import KamioApp
    from ..device import Device

logger = logging.getLogger("Kamio.rules")


class RuleEvent:
    """
    Passed as the first argument to every rule function.

    Attributes:
        data:       Changed field values  ``{"power": True, ...}``
        device_id:  ID of the device that triggered the rule, or ``None`` for interval rules.
        kind:       ``"event"`` for device-triggered rules, ``"interval"`` for timer rules.

    Example rule function signature::

        async def on_motion(event: RuleEvent, app: KamioApp):
            if event.data.get("motion_detected"):
                ...
    """

    __slots__ = ("data", "device_id", "kind")

    def __init__(self, data: Dict[str, Any], device_id: Optional[str], kind: str) -> None:
        self.data = data
        self.device_id = device_id
        self.kind = kind

    def get(self, key: str, default: Any = None) -> Any:
        """Return the value for a key from the event data, or default."""
        return self.data.get(key, default)

    def __repr__(self) -> str:
        """Return a developer-friendly representation."""
        return f"RuleEvent(device_id={self.device_id!r}, kind={self.kind!r}, data={self.data!r})"


class Rule:
    """
    An automation rule executed when matching device fields change or on a timer.

    Rules are created via :meth:`KamioApp.add_rule` or the
    ``@app.rule(...)`` decorator — not instantiated directly.

    Args:
        func:         Async function ``async def fn(event: RuleEvent, app: KamioApp)``.
                      May also be an unbound device method (decorated with
                      ``@rule`` inside a ``Device`` subclass) or a bound method;
                      the engine detects the calling convention and passes
                      ``self`` for device-level rules automatically.
        device_class: :class:`Device` subclass to filter by; ``None`` = all devices.
        interval:     Seconds between periodic invocations (timer rule).
                      Mutually exclusive with ``fields``-based triggering.
        fields:       Field names that must appear in the update to trigger the rule.
                      Ignored for interval rules.
        enabled:      Start enabled (default ``True``).  Can be toggled at runtime
                      via ``rule.enabled = False``.
        run_on_start: If ``True`` and ``interval`` is set, execute the rule once
                      immediately when the engine starts (default ``False``).
        description:  Human-readable label shown in logs and events.
    """

    def __init__(
        self,
        func: Callable[[RuleEvent, "KamioApp"], Coroutine[Any, Any, None]],
        device_class: Optional[type] = None,
        interval: Optional[float] = None,
        fields: Optional[List[str]] = None,
        enabled: bool = True,
        run_on_start: bool = False,
        description: Optional[str] = None,
    ):
        self.func = func
        self.device_class = device_class
        self.interval = interval
        self.fields = fields
        self.enabled = enabled
        self.run_on_start = run_on_start
        self.description = description or (func.__doc__ if func.__doc__ else "")
        self.last_run = 0.0
        self.task: Optional[asyncio.Task] = None

    @property
    def device_type(self) -> Optional[str]:
        """Return the string device type this rule filters by, or ``None``."""
        if self.device_class is None:
            return None
        return self.device_class.__name__.lower()

    async def run(
        self, event: "RuleEvent", app: "KamioApp", device_instance: Optional["Device"] = None
    ) -> None:
        """
        Execute the rule function.

        Skips execution when :attr:`enabled` is ``False``.
        Fires ``on_rule_triggered`` hook and ``rule_triggered`` event on success.
        Fires ``on_rule_failed`` hook and ``rule_failed`` event on error.
        Errors are logged and never propagate to the caller.

        Args:
            event: :class:`RuleEvent` carrying the triggering data.
            app:   The :class:`KamioApp` instance.
            device_instance: The device instance (for instance method rules).
        """
        if not self.enabled:
            return
        import time

        try:
            # Check if this is a device-level rule (unbound method from device class).
            # Device-level rules are registered via @rule decorator on device methods.
            # They have _is_rule attribute and belong to a Device subclass.
            # We detect this by checking that the function has _is_rule AND its
            # __qualname__ contains a "." (indicating it's a method, not a plain function).
            is_device_level_rule = (
                device_instance is not None
                and getattr(self.func, "_is_rule", False)
                and "." in getattr(self.func, "__qualname__", "")
            )

            if is_device_level_rule:
                # Bind unbound method to device instance for device-level rules
                func_to_call = self.func.__get__(device_instance, type(device_instance))
                sig = inspect.signature(func_to_call)
                param_count = len(sig.parameters)
            elif hasattr(self.func, "__self__"):
                # Already a bound method
                func_to_call = self.func
                sig = inspect.signature(func_to_call)
                param_count = len(sig.parameters)
            else:
                # Regular function (app-level rule)
                func_to_call = self.func
                sig = inspect.signature(func_to_call)
                param_count = len(sig.parameters)

            # Call with appropriate arguments based on parameter count
            if param_count >= 2:
                await func_to_call(event, app)
            elif param_count == 1:
                await func_to_call(event)
            else:
                await func_to_call()

            self.last_run = time.time()
            snapshot = dict(event.data)
            await app.hooks.trigger("on_rule_triggered", self, snapshot)
            await app.event_bus.publish("rule_triggered", {"rule": self, "snapshot": snapshot})
        except Exception as e:
            logger.error(f"Error executing rule '{self.func.__name__}': {e}", exc_info=True)
            await app.hooks.trigger("on_rule_failed", self, e)
            await app.event_bus.publish("rule_failed", {"rule": self, "error": e})


class RuleEngine:
    """
    Manages and executes automation rules for the Kamio application.

    The RuleEngine is responsible for:
    - Registering and storing rules
    - Starting and stopping interval-based rules
    - Dispatching device updates to relevant rules
    - Managing background tasks for periodic rule execution

    Args:
        app: The KamioApp instance this engine belongs to.
    """

    def __init__(self, app: KamioApp):
        self.app = app
        self.rules: List[Rule] = []
        self._bg_tasks: set[asyncio.Task] = set()
        self._is_running = False
        self._event_rules_by_type: Dict[Optional[str], List[Rule]] = defaultdict(list)
        # Lock to protect rules list and index from concurrent modification
        # (e.g. hot-reload set_rules while handle_device_update is iterating).
        self._lock = asyncio.Lock()

    def add_rule(self, rule: Rule):
        """
        Register a new rule with the engine.

        The event-rules index is updated immediately regardless of whether the
        engine has been started yet.  If the engine is already running and the
        rule has an interval, the interval task is started immediately.

        Args:
            rule: The Rule instance to register.
        """
        self.rules.append(rule)
        if rule.interval is None:
            self._event_rules_by_type[rule.device_type].append(rule)
        if self._is_running and rule.interval is not None:
            self._start_interval_rule(rule)

    def remove_rule(self, rule: Rule) -> None:
        """
        Remove a rule from the engine and cancel its interval task if any.

        Args:
            rule: The Rule instance to remove.
        """
        if rule.task and not rule.task.done():
            rule.task.cancel()
        if rule in self.rules:
            self.rules.remove(rule)
        if rule.interval is None:
            indexed = self._event_rules_by_type.get(rule.device_type, [])
            if rule in indexed:
                indexed.remove(rule)

    async def set_rules(self, rules: list) -> None:
        """Replace the entire rule set atomically and rebuild the index.

        Intended for rollback scenarios (e.g. hot-reload failure).  Interval
        tasks of removed rules are cancelled and awaited so they can clean
        up before the new rule set takes effect.  The engine should be
        stopped before calling this if it is running.

        Acquires the engine lock to prevent ``handle_device_update`` from
        reading a partially-replaced rule list.
        """
        async with self._lock:
            cancelled = []
            for rule in list(self.rules):
                if rule.task and not rule.task.done():
                    rule.task.cancel()
                    cancelled.append(rule.task)
            if cancelled:
                await asyncio.gather(*cancelled, return_exceptions=True)
            self.rules[:] = list(rules)
            self._rebuild_index()

    async def start(self):
        """
        Start the rule engine.

        This will start background tasks for all interval-based rules.
        Event-based rules will be triggered as device updates occur.
        """
        self._is_running = True
        for rule in self.rules:
            if rule.interval is not None:
                self._start_interval_rule(rule)

    async def stop(self):
        """
        Stop the rule engine gracefully.

        Cancels all background interval tasks and waits for them to complete.
        """
        self._is_running = False
        for task in self._bg_tasks:
            task.cancel()
        if self._bg_tasks:
            await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        self._bg_tasks.clear()

    def _start_interval_rule(self, rule: Rule):
        """
        Start a background task for an interval-based rule.

        Args:
            rule: The rule to run periodically.
        """

        async def _loop():
            if rule.run_on_start and self._is_running and rule.enabled:
                snapshot = RuleEvent(
                    data=self.app.state.get_all_states(),
                    device_id=None,
                    kind="interval",
                )
                await rule.run(snapshot, self.app)

            while self._is_running:
                try:
                    await asyncio.sleep(rule.interval)
                    if not rule.enabled:
                        continue

                    snapshot = RuleEvent(
                        data=self.app.state.get_all_states(),
                        device_id=None,
                        kind="interval",
                    )
                    await rule.run(snapshot, self.app)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in interval rule '{rule.description}': {e}")

        task = asyncio.create_task(_loop())
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)
        rule.task = task

    def _rebuild_index(self) -> None:
        """Rebuild the event-rule index from scratch.

        .. deprecated::
            Since v1.0.0b1 the index is kept in sync by :meth:`add_rule` /
            :meth:`remove_rule`. This method is retained for external callers
            and for rollback by :class:`HotReloadManager`.
        """
        self._event_rules_by_type.clear()
        for rule in self.rules:
            if rule.interval is not None:
                continue
            self._event_rules_by_type[rule.device_type].append(rule)

    async def handle_device_update(self, device_id: str, snapshot: dict):
        """
        Trigger all matching event-based rules for a device update.

        Matches global rules (``device=None``), rules for the exact device type,
        and rules for any base class in the MRO — de-duplicated in registration
        order.  Interval rules are never triggered here.

        Args:
            device_id: The ID of the device that was updated.
            snapshot:  Dict of changed field names → new values.
        """
        device_instance = self.app.registry.get_instance(device_id)
        device_type = device_instance.device_type() if device_instance else None

        wrapped_snapshot = RuleEvent(
            data=snapshot,
            device_id=device_id,
            kind="event",
        )

        # Snapshot the candidate rules under the lock so that a concurrent
        # set_rules() does not mutate the list/index while we iterate.
        async with self._lock:
            candidates: List[Rule] = []
            candidates.extend(self._event_rules_by_type.get(None, []))
            if device_type:
                candidates.extend(self._event_rules_by_type.get(device_type, []))
                if device_instance:
                    for base in type(device_instance).__mro__:
                        if base is type(device_instance):
                            continue
                        base_type = getattr(base, "device_type", lambda: None)()
                        if base_type:
                            candidates.extend(self._event_rules_by_type.get(base_type, []))

            seen = set()
            scheduled: List[Tuple[Rule, Coroutine[Any, Any, Any]]] = []
            for rule in candidates:
                if rule in seen:
                    continue
                seen.add(rule)
                if not rule.enabled or rule.interval is not None:
                    continue
                if rule.fields and not any(field in snapshot for field in rule.fields):
                    continue
                scheduled.append((rule, rule.run(wrapped_snapshot, self.app, device_instance)))

        if scheduled:
            results = await asyncio.gather(*(coro for _, coro in scheduled), return_exceptions=True)
            # return_exceptions=True keeps one failing rule from aborting the
            # whole batch (important under load), but silently swallowing the
            # errors makes failing rules invisible.  Log each exception so the
            # operator can see which rule misbehaved.
            for rule_item, result in zip(scheduled, results):
                rule_obj, _ = rule_item
                if isinstance(result, BaseException) and not isinstance(
                    result, asyncio.CancelledError
                ):
                    logger.error(
                        f"Rule '{getattr(rule_obj.func, '__name__', rule_obj)}' raised: {result}",
                        exc_info=result,
                    )
