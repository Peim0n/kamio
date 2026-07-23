from __future__ import annotations
import asyncio
from typing import Any, Dict, Optional
from .base import BaseDriver


class TelnetDriver(BaseDriver):
    """
    Telnet driver for legacy industrial equipment.
    """

    def __init__(
        self, host: str, port: int = 23, timeout: float = 5.0, max_reconnect_attempts: int = 3
    ):
        super().__init__()
        self.host = host
        self.port = port
        self.timeout = timeout
        self.max_reconnect_attempts = max_reconnect_attempts
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self._reconnect_delay_base: float = 1.0
        self._lock = asyncio.Lock()  # Lock to prevent concurrent commands

    async def connect(self):
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), timeout=self.timeout
            )
            self.logger.info(f"Connected to Telnet {self.host}:{self.port}")
        except Exception as e:
            self.logger.error(f"Telnet connection failed to {self.host}:{self.port}: {e}")
            raise

    async def disconnect(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            self.reader = None
            self.writer = None
            self.logger.info("Telnet disconnected")

    async def _ensure_connected(self) -> None:
        """Reconnect if the stream is closed or missing."""
        if self.writer and not self.writer.is_closing():
            return
        await self.disconnect()
        for attempt in range(1, self.max_reconnect_attempts + 1):
            try:
                await self.connect()
                return
            except Exception as e:
                self.logger.warning(f"Reconnect attempt {attempt} failed: {e}")
                if attempt == self.max_reconnect_attempts:
                    raise
                await asyncio.sleep(self._reconnect_delay_base * (2 ** (attempt - 1)))

    async def execute(self, command_name: str, params: dict) -> dict:
        async with self._lock:  # Prevent concurrent commands
            await self._ensure_connected()
            assert self.writer is not None and self.reader is not None

            cmd = params.get("command", command_name)
            value = params.get("value")
            if value is not None:
                cmd = f"{cmd} {value}"
            if not cmd.endswith("\n"):
                cmd += "\n"

            try:
                self.writer.write(cmd.encode())
                await self.writer.drain()
            except Exception as e:
                self.logger.warning(f"Telnet write failed, attempting reconnect: {e}")
                await self._ensure_connected()
                assert self.writer is not None and self.reader is not None
                self.writer.write(cmd.encode())
                await self.writer.drain()

            # Read response if expected
            response = ""
            if params.get("wait_response", True):
                try:
                    line = await asyncio.wait_for(self.reader.readline(), timeout=self.timeout)
                    response = line.decode().strip()
                except asyncio.TimeoutError:
                    self.logger.warning("Telnet read timeout")

            return {"status": "ok", "command": command_name, "response": response}

    async def read(self, field_name: str, params: Optional[Dict[str, Any]] = None) -> Any:
        params = params or {}
        await self._ensure_connected()
        assert self.reader is not None

        cmd = params.get("command", field_name)
        if cmd:
            value = params.get("value")
            if value is not None:
                cmd = f"{cmd} {value}"
            if not cmd.endswith("\n"):
                cmd += "\n"
            self.writer.write(cmd.encode())
            await self.writer.drain()

        try:
            line = await asyncio.wait_for(self.reader.readline(), timeout=self.timeout)
            return {"status": "ok", "field": field_name, "response": line.decode().strip()}
        except asyncio.TimeoutError:
            self.logger.warning("Telnet read timeout")
            return {"status": "ok", "field": field_name, "response": ""}
