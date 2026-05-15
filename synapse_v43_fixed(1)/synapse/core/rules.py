from __future__ import annotations
import asyncio
import logging
from typing import Any, Callable, Coroutine, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..app import SynapseApp

logger = logging.getLogger("synapse.rules")

class Rule:
    def __init__(
        self,
        func: Callable[[dict, SynapseApp], Coroutine[Any, Any, None]],
        device_class: Optional[type] = None,
        interval: Optional[float] = None,
        fields: Optional[List[str]] = None,
        enabled: bool = True,
        description: Optional[str] = None
    ):
        self.func = func
        self.device_class = device_class
        self.interval = interval
        self.fields = fields
        self.enabled = enabled
        self.description = description or (func.__doc__ if func.__doc__ else "")
        self.last_run = 0.0
        self.task: Optional[asyncio.Task] = None

    async def run(self, snapshot: dict, app: SynapseApp):
        if not self.enabled:
            return
        try:
            await self.func(snapshot, app)
        except Exception as e:
            logger.error(f"Error executing rule '{self.func.__name__}': {e}", exc_info=True)

class RuleEngine:
    def __init__(self, app: SynapseApp):
        self.app = app
        self.rules: List[Rule] = []
        self._bg_tasks: List[asyncio.Task] = []
        self._is_running = False

    def add_rule(self, rule: Rule):
        self.rules.append(rule)
        if self._is_running and rule.interval:
            self._start_interval_rule(rule)

    async def start(self):
        self._is_running = True
        for rule in self.rules:
            if rule.interval:
                self._start_interval_rule(rule)

    async def stop(self):
        self._is_running = False
        for task in self._bg_tasks:
            task.cancel()
        if self._bg_tasks:
            await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        self._bg_tasks.clear()

    def _start_interval_rule(self, rule: Rule):
        async def _loop():
            while self._is_running:
                try:
                    await asyncio.sleep(rule.interval)
                    if not rule.enabled:
                        continue

                    snapshot = {
                        "update": self.app.state.get_all_states(),
                        "timestamp": asyncio.get_running_loop().time(),
                        "type": "interval"
                    }
                    await rule.run(snapshot, self.app)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in interval rule '{rule.description}': {e}")

        task = asyncio.create_task(_loop())
        self._bg_tasks.append(task)
        rule.task = task

    async def handle_device_update(self, device_id: str, snapshot: dict):
        device_instance = self.app.registry.get_instance(device_id)
        device_class = type(device_instance) if device_instance else None

        wrapped_snapshot = {
            "update": snapshot,
            "device_id": device_id,
            "type": "event"
        }

        for rule in self.rules:
            if not rule.enabled or rule.interval is not None:
                continue

            if rule.fields:
                if not any(field in snapshot for field in rule.fields):
                    continue

            if rule.device_class and device_class and issubclass(device_class, rule.device_class):
                await rule.run(wrapped_snapshot, self.app)
            elif rule.device_class is None:
                await rule.run(wrapped_snapshot, self.app)
