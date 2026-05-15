import logging
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
        self.chip = None
        self.lines = {}
        self.logger = logging.getLogger("synapse.driver.gpio")

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

    async def execute(self, command_name: str, params: dict) -> dict:
        if not self.chip:
            raise RuntimeError("GPIO chip not connected")
        
        pin = params.get("pin")
        value = params.get("value")

        if command_name == "set_output":
            line = self._get_line(pin, direction="output")
            line.set_value(1 if value else 0)
            return {"status": "ok", "pin": pin, "value": value}
        
        raise NotImplementedError(f"Command {command_name} not supported")

    async def read(self, field_name: str) -> Any:
        # Assuming field_name might be mapped to a pin in config
        # For simplicity, we expect field_name to be "pin_{number}"
        if field_name.startswith("pin_"):
            try:
                pin = int(field_name.split("_")[1])
                line = self._get_line(pin, direction="input")
                return line.get_value()
            except (ValueError, IndexError):
                return None
        return None

    def _get_line(self, pin: int, direction: str = "input"):
        if pin not in self.lines:
            line = self.chip.get_line(pin)
            if direction == "output":
                line.request(consumer="synapse", type=gpiod.LINE_REQ_DIR_OUT)
            else:
                line.request(consumer="synapse", type=gpiod.LINE_REQ_DIR_IN)
            self.lines[pin] = line
        return self.lines[pin]
