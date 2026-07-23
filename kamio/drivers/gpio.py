from __future__ import annotations
from typing import Any, Dict, Optional

try:
    import gpiod
except ImportError:
    gpiod = None

from .base import BaseDriver


class GPIOChipDriver(BaseDriver):
    """
    Modern GPIO driver using gpiod library.
    """

    def __init__(self, chip_path: str = "/dev/gpiochip4"):
        super().__init__()
        self.chip_path = chip_path
        self.chip: Optional[Any] = None
        self.lines: Dict[int, Any] = {}
        self._line_directions: Dict[int, str] = {}

    async def connect(self):
        if gpiod is None:
            raise ImportError("gpiod library not installed")
        try:
            self.chip = gpiod.Chip(self.chip_path)
            self.logger.info(f"Connected to GPIO chip: {self.chip_path}")
        except Exception as e:
            self.logger.error(f"Failed to connect to GPIO chip {self.chip_path}: {e}")
            raise

    async def disconnect(self):
        if self.chip:
            for line in self.lines.values():
                line.release()
            self.chip.close()
            self.chip = None
            self.logger.info("GPIO chip disconnected")

    async def read(self, field_name: str, params: dict | None = None) -> Any:
        params = params or {}
        pin = params.get("pin")
        if not isinstance(pin, int):
            raise ValueError("'pin' is required for GPIO read and must be an int")
        if not self.chip:
            raise RuntimeError("GPIO chip not connected")
        line = self._get_line(pin, direction="input")
        value = line.get_value()
        return {"status": "ok", "pin": pin, "value": value}

    async def execute(self, command_name: str, params: dict) -> dict:
        if not self.chip:
            raise RuntimeError("GPIO chip not connected")

        pin = params.get("pin")
        if not isinstance(pin, int):
            raise ValueError("'pin' is required for GPIO execute and must be an int")
        value = params.get("value")

        if command_name == "set_output":
            line = self._get_line(pin, direction="output")
            line.set_value(1 if value else 0)
            return {"status": "ok", "pin": pin, "value": value}

        raise NotImplementedError(f"Command {command_name} not supported")

    def _get_line(self, pin: int, direction: str = "input"):
        cached_dir = self._line_directions.get(pin)
        if pin in self.lines and cached_dir != direction:
            self.lines[pin].release()
            del self.lines[pin]

        if pin not in self.lines:
            if self.chip is None:
                raise RuntimeError("GPIO chip not connected")
            line = self.chip.get_line(pin)
            if direction == "output":
                line.request(consumer="Kamio", type=gpiod.LINE_REQ_DIR_OUT)
            else:
                line.request(consumer="Kamio", type=gpiod.LINE_REQ_DIR_IN)
            self.lines[pin] = line
            self._line_directions[pin] = direction
        return self.lines[pin]
