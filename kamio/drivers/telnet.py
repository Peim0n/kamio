from __future__ import annotations

import asyncio
from typing import Any, Dict, Iterable, Optional, Union

from .base import BYTE_TRANSPORT_COMMANDS, BaseDriver, require_payload


def _frame(payload: Union[str, bytes]) -> bytes:
    """Encode ``payload`` as one newline-terminated line (``b""`` stays empty)."""
    data = payload.encode() if isinstance(payload, str) else bytes(payload)
    if data and not data.endswith(b"\n"):
        data += b"\n"
    return data


class TelnetDriver(BaseDriver):
    """
    Telnet driver for legacy industrial equipment.

    Transport commands: ``send`` / ``request`` with ``params["payload"]``.
    """

    COMMANDS = BYTE_TRANSPORT_COMMANDS

    def __init__(
        self,
        host: str,
        port: int = 23,
        timeout: float = 5.0,
        max_reconnect_attempts: int = 3,
        *,
        commands: Optional[Iterable[str]] = None,
    ):
        super().__init__(commands=commands)
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
        """Transport commands: ``send`` (write a line) / ``request`` (write + read a line).

        ``params["payload"]`` (str/bytes) is the line to write; the driver adds
        the trailing newline and strips the reply.
        """
        payload = _frame(require_payload(params, command_name))
        if command_name == "send":
            await self._transact(payload, wait_response=False)
            return {"status": "ok", "command": "send"}
        response = await self._transact(payload, wait_response=True)
        return {"status": "ok", "command": "request", "response": response}

    async def read(self, field_name: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Write ``params["payload"]`` (``""`` = pure read) and read one line."""
        payload = (params or {}).get("payload")
        if payload is None:
            raise NotImplementedError("TelnetDriver.read requires params['payload']")
        response = await self._transact(_frame(payload), wait_response=True)
        return {"status": "ok", "field": field_name, "response": response}

    async def _transact(self, payload: bytes, wait_response: bool) -> str:
        """Under the lock: write ``payload`` (if non-empty) and optionally read one line."""
        async with self._lock:  # Prevent concurrent commands
            await self._ensure_connected()
            if payload:
                await self._write(payload)
            if wait_response:
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
            # Drop the broken stream first — a failed drain may leave the
            # writer half-open, and _ensure_connected() early-returns on a
            # non-closing writer.
            await self.disconnect()
            await self._ensure_connected()
            assert self.writer is not None
            self.writer.write(payload)
            await self.writer.drain()

    async def _read_line(self) -> str:
        """Read one line; TimeoutError propagates after the connection is closed.

        A timed-out read must not return success: the late response would
        stay buffered in the reader and be consumed by the *next*
        transaction, shifting every subsequent reply by one.  Closing the
        connection discards the buffered data entirely.
        """
        assert self.reader is not None
        try:
            line = await asyncio.wait_for(self.reader.readline(), timeout=self.timeout)
            return line.decode().strip()
        except asyncio.TimeoutError:
            self.logger.warning("Telnet read timeout — closing connection")
            await self.disconnect()
            raise TimeoutError(f"Telnet read timed out after {self.timeout}s")
