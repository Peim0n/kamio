from __future__ import annotations
import asyncio
from typing import Any, Dict, Optional
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

    async def read(self, field_name: str, params: Optional[Dict[str, Any]] = None) -> Any:
        if not self.ser:
            raise RuntimeError("Serial port not connected")

        params = params or {}
        data = params.get("command", field_name)
        value = params.get("value")
        if data and value is not None:
            data = f"{data} {value}"
        if data and isinstance(data, str):
            data = data.encode()

        def _write_read():
            if data:
                self.ser.write(data)
            if params.get("wait_response", True):
                return self.ser.readline().decode().strip()
            return ""

        response = await asyncio.to_thread(_write_read)
        return {"status": "ok", "field": field_name, "response": response}

    async def execute(self, command_name: str, params: dict) -> dict:
        if not self.ser:
            raise RuntimeError("Serial port not connected")

        data = params.get("command", command_name)
        value = params.get("value")
        if value is not None:
            data = f"{data} {value}"
        if isinstance(data, str):
            data = data.encode()

        def _write_read():
            self.ser.write(data)
            if params.get("wait_response", True):
                return self.ser.readline().decode().strip()
            return ""

        response = await asyncio.to_thread(_write_read)
        return {"status": "ok", "command": command_name, "response": response}
