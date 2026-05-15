import logging
import asyncio
from typing import Any, Optional
try:
    import serial
except ImportError:
    serial = None

from .base import BaseDriver

class SerialDriver(BaseDriver):
    """
    Serial driver (RS-232/RS-485) using pyserial.
    Note: pyserial is synchronous, so we wrap it in threads or use non-blocking mode if possible.
    For simplicity here, we use a basic synchronous approach wrapped in a thread-safe way.
    """
    def __init__(self, port: str, baudrate: int = 9600, timeout: float = 1.0):
        super().__init__()
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.ser: Optional[serial.Serial] = None
        self.logger = logging.getLogger("synapse.driver.serial")

    async def connect(self):
        if serial is None:
            raise ImportError("pyserial library not installed")
        
        def _connect():
            return serial.Serial(self.port, self.baudrate, timeout=self.timeout)

        try:
            self.ser = await asyncio.to_thread(_connect)
            self.logger.info(f"Connected to Serial port {self.port} at {self.baudrate}")
        except Exception as e:
            self.logger.error(f"Serial connection failed to {self.port}: {e}")
            raise

    async def disconnect(self):
        if self.ser:
            await asyncio.to_thread(self.ser.close)
            self.ser = None
            self.logger.info("Serial port closed")

    async def execute(self, command_name: str, params: dict) -> dict:
        if not self.ser:
            raise RuntimeError("Serial port not connected")
        
        data = params.get("data", "")
        if isinstance(data, str):
            data = data.encode()

        def _write_read():
            self.ser.write(data)
            if params.get("wait_response", True):
                return self.ser.readline().decode().strip()
            return ""

        response = await asyncio.to_thread(_write_read)
        return {"status": "ok", "response": response}

    async def read(self, field_name: str) -> Any:
        # Similar to telnet, reading usually involves a query
        result = await self.execute("query", {"data": f"READ {field_name}\n", "wait_response": True})
        return result.get("response")
