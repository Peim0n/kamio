from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional, TYPE_CHECKING

from synapse.data_fields import parse_freq

if TYPE_CHECKING:
    from .mqtt_nodes import DeviceNode
    from .envelope import Envelope

class TaskManagerMixin:
    """Mixin for reliable background task management."""
    def __init__(self, logger_name: str):
        self._bg_tasks: set[asyncio.Task] = set()
        self.logger = logging.getLogger(logger_name)

    def create_task(self, coro, name: str = None) -> asyncio.Task:
        """Creates a task and registers it for tracking."""
        task = asyncio.create_task(coro, name=name)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)
        return task

    async def cancel_all_tasks(self):
        """Guaranteed cancellation of all active tasks."""
        if not self._bg_tasks:
            return

        self.logger.debug(f"Cancelling {len(self._bg_tasks)} tasks")
        for task in list(self._bg_tasks):
            task.cancel()

        results = await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, Exception) and not isinstance(res, asyncio.CancelledError):
                self.logger.error(f"Task finished with error: {res}")

        self._bg_tasks.clear()

class TelemetryMixin(TaskManagerMixin):
    """Mixin for periodic telemetry publication."""
    MIN_FREQ = 0.1  # Minimum frequency threshold in seconds
    enable_telemetry: bool = True

    def __init__(self, logger_name: str):
        super().__init__(logger_name=logger_name)
        self.node: Optional[DeviceNode] = None

    async def start_telemetry(self):
        """Starts telemetry publication loops based on SYNAPSE_FIELDS."""
        if not self.enable_telemetry:
            self.logger.debug("Telemetry is disabled for this device")
            return

        if not self.node or not self.node.is_running:
            return

        if any(t.get_name().startswith("telemetry_") for t in self._bg_tasks):
            return

        freq_groups: dict[float, list[str]] = {}
        fields_source = getattr(self, "SYNAPSE_FIELDS", {})

        for field_name, field in fields_source.items():
            if field.kind == "telemetry":
                seconds = parse_freq(field.freq)
                if seconds > 0:
                    if seconds < self.MIN_FREQ:
                        self.logger.warning(f"Frequency {seconds}s for {field_name} is too high. Capping to {self.MIN_FREQ}s")
                        seconds = self.MIN_FREQ
                    freq_groups.setdefault(seconds, []).append(field_name)

        for seconds, fields in freq_groups.items():
            self.create_task(
                self._telemetry_scheduler(fields, seconds),
                name=f"telemetry_{seconds}"
            )

    async def _telemetry_scheduler(self, field_names: list[str], freq: float):
        """Internal scheduler loop."""
        while self.node and self.node.is_running:
            try:
                await asyncio.sleep(freq)
                data = await self.handle_telemetry_update(field_names)
                if data and isinstance(data, dict):
                    await self.publish_telemetry(data)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Telemetry scheduler ({freq}s) error: {e}")

    async def handle_telemetry_update(self, field_names: list[str]) -> Optional[dict[str, Any]]:
        """Hook for updating telemetry values. Should be overridden if needed."""
        # By default, try to get values from attributes
        data = {}
        for name in field_names:
            val = getattr(self, name, None)
            if val is not None:
                data[name] = val
        return data if data else None

    async def publish_telemetry(self, data: dict):
        """Publishes telemetry via abstract _safe_publish method."""
        if self.node:
            from .envelope import Envelope
            env = Envelope.telemetry(source=self.node.device_id, data=data)
            await self._safe_publish(env)

    async def _safe_publish(self, env: Envelope):
        """
        Abstract publication method.
        Must be implemented in Device to connect with transport.
        """
        pass
