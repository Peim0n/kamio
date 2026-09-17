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
        """Establish a Telnet connection to the configured host and port."""
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), timeout=self.timeout
            )
            self.logger.info(f"Connected to Telnet {self.host}:{self.port}")
        except Exception as e:
            self.logger.error(f"Telnet connection failed to {self.host}:{self.port}: {e}")
            raise

    async def disconnect(self):
        """Close the Telnet connection if open."""
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception as e:
                self.logger.warning(f"Error closing Telnet connection: {e}")
            finally:
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
        """Send a command over Telnet and read the response."""
        params = params or {}
        response = await self._transact(self._build_command(command_name, params), params)
        return {"status": "ok", "command": command_name, "response": response}

    async def read(self, field_name: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Send a query command and read the response over Telnet."""
        params = params or {}
        response = await self._transact(self._build_command(field_name, params), params)
        return {"status": "ok", "field": field_name, "response": response}

    @staticmethod
    def _build_command(name: str, params: Dict[str, Any]) -> bytes:
        """Resolve ``params["command"]`` (default ``name``) plus optional ``value`` to a line.

        An empty command yields ``b""`` and is never written, so callers can
        perform a pure read by passing ``{"command": ""}``.
        """
        cmd = str(params.get("command", name) or "")
        if not cmd:
            return b""
        value = params.get("value")
        if value is not None:
            cmd = f"{cmd} {value}"
        if not cmd.endswith("\n"):
            cmd += "\n"
        return cmd.encode()

    async def _transact(self, payload: bytes, params: Dict[str, Any]) -> str:
        """Under the lock: write ``payload`` (if non-empty) and optionally read one line."""
        async with self._lock:  # Prevent concurrent commands
            await self._ensure_connected()
            if payload:
                await self._write(payload)
            if params.get("wait_response", True):
                return await self._read_line()
            return ""

    async def _write(self, payload: bytes) -> None:
        """Write ``payload``, reconnecting and retrying once if the write fails."""
        assert self.writer is not None
        try:
            self.writer.write(payload)
            await self.writer.drain()
        except Exception as e:
            self.logger.warning(f"Telnet write failed, attempting reconnect: {e}")
            await self._ensure_connected()
            assert self.writer is not None
            self.writer.write(payload)
            await self.writer.drain()

    async def _read_line(self) -> str:
        """Read one line, returning ``""`` on timeout."""
        assert self.reader is not None
        try:
            line = await asyncio.wait_for(self.reader.readline(), timeout=self.timeout)
            return line.decode().strip()
        except asyncio.TimeoutError:
            self.logger.warning("Telnet read timeout")
            return ""
