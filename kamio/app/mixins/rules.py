from __future__ import annotations
import asyncio
import logging
from typing import Any, Callable, List, Optional, Type, TYPE_CHECKING

from kamio.core.rules import Rule
from kamio.device import Device

if TYPE_CHECKING:
    from kamio.app import KamioApp

logger = logging.getLogger("Kamio.app")


class RuleRegistryMixin:
    """Automation rule registration and removal."""

    def rule(
        self: Any,
        device: Optional[Type[Device]] = None,
        *,
        interval: Optional[float] = None,
        fields: Optional[List[str]] = None,
        enabled: bool = True,
        run_on_start: bool = False,
        description: Optional[str] = None,
    ):
        """Decorator to register an automation rule."""

        def decorator(func: Callable):
            rule_func: Any = func
            rule_func._Kamio_rule_kwargs = {
                "device": device,
                "interval": interval,
                "fields": fields,
                "enabled": enabled,
                "run_on_start": run_on_start,
                "description": description,
            }
            self.add_rule(
                rule_func,
                device=device,
                interval=interval,
                fields=fields,
                enabled=enabled,
                run_on_start=run_on_start,
                description=description,
            )
            return func

        return decorator

    def add_rule(
        self: Any,
        func: Callable,
        device: Optional[Type[Device]] = None,
        *,
        interval: Optional[float] = None,
        fields: Optional[List[str]] = None,
        enabled: bool = True,
        run_on_start: bool = False,
        description: Optional[str] = None,
    ) -> Callable:
        """Register a rule function."""
        if not callable(func):
            raise TypeError(
                f"add_rule: 'func' must be a callable, got {type(func).__name__!r}.\n"
                f"Expected: async def my_rule(event: RuleEvent, app: KamioApp): ..."
            )
        if not asyncio.iscoroutinefunction(func):
            raise TypeError(
                f"add_rule: rule function '{getattr(func, '__name__', func)}' must be async.\n"
                f"Did you forget 'async def'?"
            )
        if interval is not None and fields is not None:
            raise ValueError(
                f"add_rule: rule '{getattr(func, '__name__', func)}' cannot have "
                f"both 'interval' and 'fields' — they are mutually exclusive."
            )
        if device is not None and fields is not None:
            known = set(device.Kamio_FIELDS.keys())
            unknown = [f for f in fields if f not in known]
            if unknown:
                logger.warning(
                    f"add_rule: rule '{getattr(func, '__name__', func)}' watches fields "
                    f"{unknown!r} which do not exist on {device.__name__}. "
                    f"Known fields: {sorted(known)}"
                )
        rule_obj = Rule(
            func,
            device_class=device,
            interval=interval,
            fields=fields,
            enabled=enabled,
            run_on_start=run_on_start,
            description=description,
        )
        self.rules.add_rule(rule_obj)
        self._schedule_when_running(self.hooks.trigger("on_rule_added", rule_obj))
        self._schedule_when_running(self.event_bus.publish("rule_added", {"rule": rule_obj}))
        return func

    async def remove_rule(self: Any, func: Callable) -> None:
        """Remove a registered rule by its function reference."""
        target = None
        for rule_obj in list(self.rules.rules):
            if rule_obj.func is func:
                target = rule_obj
                break
        if target is None:
            logger.warning(f"remove_rule: rule '{getattr(func, '__name__', func)}' not found")
            return
        await self.hooks.trigger("on_rule_removed", target)
        self.rules.remove_rule(target)
        self._schedule_when_running(self.event_bus.publish("rule_removed", {"rule": target}))
