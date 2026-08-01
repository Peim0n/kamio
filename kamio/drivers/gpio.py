from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional

try:
    import gpiod
except ImportError:
    gpiod = None

from .base import BaseDriver


class GPIOChipDriver(BaseDriver):
    """
    Modern GPIO driver using gpiod library.

    All gpiod calls are synchronous and may perform sysfs/ioctl I/O, so every
    operation is offloaded to a worker thread via :func:`asyncio.to_thread` to
    avoid blocking the event loop.
    """

    def __init__(self, chip_path: str = "/dev/gpiochip4"):
        super().__init__()
        self.chip_path = chip_path
        self.chip: Optional[Any] = None
        self.lines: Dict[int, Any] = {}
        self._line_directions: Dict[int, str] = {}

    async def connect(self):
        """Open the GPIO chip and request configured lines."""
        if gpiod is None:
            raise ImportError("gpiod library not installed")
        try:
            self.chip = await asyncio.to_thread(gpiod.Chip, self.chip_path)
            self.logger.info(f"Connected to GPIO chip: {self.chip_path}")
        except Exception as e:
            self.logger.error(f"Failed to connect to GPIO chip {self.chip_path}: {e}")
            raise

    async def disconnect(self):
        """Release all requested lines and close the chip."""
        if self.chip:
            # Release lines one by one so a failure on one line does not leak the rest.
            for line in list(self.lines.values()):
                try:
                    await asyncio.to_thread(line.release)
                except Exception as e:
                    self.logger.warning(f"Failed to release GPIO line: {e}")
            self.lines.clear()
            self._line_directions.clear()
            try:
                await asyncio.to_thread(self.chip.close)
            except Exception as e:
                self.logger.warning(f"Failed to close GPIO chip: {e}")
            self.chip = None
            self.logger.info("GPIO chip disconnected")

    async def read(self, field_name: str, params: dict | None = None) -> Any:
        """Read the current value of a GPIO line by field name."""
        params = params or {}
        pin = params.get("pin")
        if not isinstance(pin, int):
            raise ValueError("'pin' is required for GPIO read and must be an int")
        if not self.chip:
            raise RuntimeError("GPIO chip not connected")
        line = await self._get_line(pin, direction="input")
        value = await asyncio.to_thread(line.get_value)
        return {"status": "ok", "pin": pin, "value": value}

    async def execute(self, command_name: str, params: dict) -> dict:
        """Execute a write command on a GPIO line (set high/low)."""
        if not self.chip:
            raise RuntimeError("GPIO chip not connected")

        pin = params.get("pin")
        if not isinstance(pin, int):
            raise ValueError("'pin' is required for GPIO execute and must be an int")
        value = params.get("value")

        if command_name == "set_output":
            line = await self._get_line(pin, direction="output")
            await asyncio.to_thread(line.set_value, 1 if value else 0)
            return {"status": "ok", "pin": pin, "value": value}

        raise NotImplementedError(f"Command {command_name} not supported")

    async def _get_line(self, pin: int, direction: str = "input"):
        """Resolve (and cache) a GPIO line, offloading sysfs/ioctl calls to a thread."""
        cached_dir = self._line_directions.get(pin)
        if pin in self.lines and cached_dir != direction:
            try:
                await asyncio.to_thread(self.lines[pin].release)
            except Exception as e:
                self.logger.warning(f"Failed to release GPIO line {pin}: {e}")
            del self.lines[pin]

        if pin not in self.lines:
            if self.chip is None:
                raise RuntimeError("GPIO chip not connected")
            line = await asyncio.to_thread(self.chip.get_line, pin)
            req_type = gpiod.LINE_REQ_DIR_OUT if direction == "output" else gpiod.LINE_REQ_DIR_IN
            await asyncio.to_thread(line.request, consumer="Kamio", type=req_type)
            self.lines[pin] = line
            self._line_directions[pin] = direction
        return self.lines[pin]
