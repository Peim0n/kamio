from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Optional

from kamio.data_fields import parse_freq

if TYPE_CHECKING:
    from .envelope import Envelope
    from .mqtt_nodes import DeviceNode


class TaskManagerMixin:
    """Mixin for reliable background task management."""

    def __init__(self, logger_name: str):
        """Initialize the task manager with an empty background task set."""
        self._bg_tasks: set[asyncio.Task] = set()
        self.logger = logging.getLogger(logger_name)

    def create_task(self, coro, name: Optional[str] = None) -> asyncio.Task:
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

    enable_telemetry: bool = True

    def __init__(self, logger_name: str):
        """Initialize telemetry-related attributes."""
        super().__init__(logger_name=logger_name)
        self.node: Optional[DeviceNode] = None

    def _get_min_freq(self) -> float:
        """Return the minimum telemetry frequency, configurable via app.config."""
        app = getattr(self, "_app", None)
        config = getattr(app, "config", None) if app else None
        if config is not None:
            result: float = config.get("telemetry_min_freq", 0.1, cast=float)
            return result
        return 0.1

    async def start_telemetry(self):
        """Starts telemetry publication loops based on Kamio_FIELDS."""
        if not self.enable_telemetry:
            self.logger.debug("Telemetry is disabled for this device")
            return

        if not self.node or not self.node.is_running:
            return

        if any(t.get_name().startswith("telemetry_") for t in self._bg_tasks):
            return

        min_freq = self._get_min_freq()
        freq_groups: dict[float, list[str]] = {}
        fields_source = getattr(self, "Kamio_FIELDS", {})

        device_id = self.node.device_id if self.node else "?"
        for field_name, field in fields_source.items():
            if field.kind == "telemetry":
                raw_freq = field.freq
                seconds = parse_freq(raw_freq)
                self.logger.debug(
                    f"[{device_id}] Telemetry field '{field_name}' freq='{raw_freq}' => {seconds}s"
                )
                if seconds > 0:
                    if seconds < min_freq:
                        self.logger.warning(
                            f"[{device_id}] Frequency {seconds}s for {field_name} is too high. Capping to {min_freq}s"
                        )
                        seconds = min_freq
                    freq_groups.setdefault(seconds, []).append(field_name)

        for seconds, fields in freq_groups.items():
            self.logger.debug(
                f"[{device_id}] Starting telemetry scheduler: fields={fields} every {seconds}s"
            )
            self.create_task(
                self._telemetry_scheduler(fields, seconds), name=f"telemetry_{seconds}"
            )

    async def _telemetry_scheduler(self, field_names: list[str], freq: float):
        """Internal scheduler loop."""
        device_id = self.node.device_id if self.node else "?"
        self.logger.debug(
            f"[{device_id}] Telemetry scheduler started: freq={freq}s, fields={field_names}"
        )
        while self.node and self.node.is_running:
            try:
                await asyncio.sleep(freq)
                data = await self.handle_telemetry_update(field_names)
                if data and isinstance(data, dict):
                    self.logger.debug(
                        f"[{device_id}] Publishing telemetry: {list(data.keys())} (freq={freq}s)"
                    )
                    await self.publish_telemetry(data)
                else:
                    self.logger.debug(
                        f"[{device_id}] Telemetry skipped: no data from fields={field_names}"
                    )
            except asyncio.CancelledError:
                self.logger.debug(f"[{device_id}] Telemetry scheduler cancelled")
                break
            except Exception as e:
                self.logger.error(
                    f"[{device_id}] Telemetry scheduler ({freq}s) error: {e}", exc_info=True
                )

    async def read_telemetry_value(self, field_name: str) -> Any:
        """
        Read a single telemetry value from the device driver if available.

        Drivers return a dict (e.g. ``{"status": "ok", "field": ..., "data": ...}``).
        This method extracts the ``data`` key from the response.  If the driver
        returns a plain value (not a dict), it is returned as-is.

        Subclasses may override to apply custom parsing logic.
        """
        driver = getattr(self, "driver", None)
        if driver is None:
            return None
        result = await driver.read(field_name)
        # Unwrap the standard driver response envelope.
        if isinstance(result, dict) and "data" in result:
            return result["data"]
        return result

    async def handle_telemetry_update(self, field_names: list[str]) -> Optional[dict[str, Any]]:
        """Collect current values for the given telemetry fields.

        Default implementation reads from instance attributes.  If an attribute
        is ``None`` and a driver is attached, asks the driver via
        :meth:`read_telemetry_value`.  Falsy-but-valid values (``0``, ``False``,
        ``""``) are included; ``None`` and NaN floats are skipped.

        Override in subclasses to customise telemetry collection.
        """
        data = {}
        for name in field_names:
            val = None
            driver = getattr(self, "driver", None)
            if driver is not None:
                try:
                    val = await self.read_telemetry_value(name)
                except Exception as e:
                    self.logger.debug(f"Telemetry driver read for '{name}' failed: {e}")
            if val is None and hasattr(self, name):
                val = getattr(self, name)
            if isinstance(val, float) and val != val:  # NaN
                continue
            if val is None:
                continue
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
