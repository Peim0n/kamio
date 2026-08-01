from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional

try:
    import serial
except ImportError:
    serial = None

from .base import BaseDriver


def _readline_bounded(ser, limit: int) -> bytes:
    """Read up to a newline or ``limit`` bytes, whichever comes first.

    pyserial's ``readline()`` has no upper bound on buffer growth: a misbehaving
    device that streams bytes without a newline can exhaust memory. This helper
    reads in small chunks and stops at ``\\n`` or when ``limit`` bytes have been
    accumulated, preventing unbounded allocation.
    """
    buf = bytearray()
    chunk_size = min(64, limit)
    while len(buf) < limit:
        remaining = limit - len(buf)
        chunk = ser.read(min(chunk_size, remaining))
        if not chunk:
            break
        nl = chunk.find(b"\n")
        if nl >= 0:
            buf.extend(chunk[: nl + 1])
            break
        buf.extend(chunk)
        # Truncate to the limit in case the chunk overshot the remaining space.
        if len(buf) > limit:
            del buf[limit:]
            break
    return bytes(buf)


class SerialDriver(BaseDriver):
    """
    Serial driver (RS-232/RS-485) using pyserial.

    pyserial is synchronous, so every I/O call is offloaded to a worker thread
    via :func:`asyncio.to_thread`. Reads are bounded by ``read_limit`` bytes to
    avoid unbounded memory growth when a device streams without a newline.
    All operations are guarded by an :class:`asyncio.Lock` to prevent
    concurrent write/read interleaving on the same serial port.
    """

    #: Maximum number of bytes a single readline-style read may accumulate.
    READ_LIMIT = 4096

    def __init__(
        self, port: str, baudrate: int = 9600, timeout: float = 1.0, read_limit: int = READ_LIMIT
    ):
        super().__init__()
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.ser: Optional[serial.Serial] = None
        self.read_limit = max(1, int(read_limit))
        self._lock = asyncio.Lock()

    async def connect(self):
        """Open the serial port with configured parameters."""
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
        """Close the serial port if open."""
        if self.ser:
            try:
                await asyncio.to_thread(self.ser.close)
            except Exception as e:
                self.logger.warning(f"Error closing Serial port: {e}")
            finally:
                self.ser = None
                self.logger.info("Serial port closed")

    async def read(self, field_name: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Send a query command and read the response from the serial port."""
        if not self.ser:
            raise RuntimeError("Serial port not connected")

        params = params or {}
        data = params.get("command", field_name)
        value = params.get("value")
        if data and value is not None:
            data = f"{data} {value}"
        if data and isinstance(data, str):
            data = data.encode()

        async with self._lock:

            def _write_read():
                if data:
                    self.ser.write(data)
                if params.get("wait_response", True):
                    return (
                        _readline_bounded(self.ser, self.read_limit)
                        .decode(errors="replace")
                        .strip()
                    )
                return ""

            response = await asyncio.to_thread(_write_read)
        return {"status": "ok", "field": field_name, "response": response}

    async def execute(self, command_name: str, params: dict) -> dict:
        """Send a command to the serial device and optionally read a response."""
        if not self.ser:
            raise RuntimeError("Serial port not connected")

        data = params.get("command", command_name)
        value = params.get("value")
        if value is not None:
            data = f"{data} {value}"
        if isinstance(data, str):
            data = data.encode()

        async with self._lock:

            def _write_read():
                self.ser.write(data)
                if params.get("wait_response", True):
                    return (
                        _readline_bounded(self.ser, self.read_limit)
                        .decode(errors="replace")
                        .strip()
                    )
                return ""

            response = await asyncio.to_thread(_write_read)
        return {"status": "ok", "command": command_name, "response": response}
