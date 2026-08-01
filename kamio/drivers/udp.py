from __future__ import annotations

import asyncio
from typing import Any, Optional

from .base import BaseDriver


class UDPDriver(BaseDriver):
    """
    Asynchronous UDP driver for request/response protocols.

    Supports plain send/receive and simple command mapping:
        execute("set_power", {"value": True, "command": "PWR ON"})
        read("temperature", {"command": "GET TEMP", "read_bytes": 1024})

    Args:
        host: Target host.
        port: Target port.
        timeout: Read timeout in seconds.
        local_port: Optional local port to bind; 0 lets the OS choose.
    """

    def __init__(
        self,
        host: str,
        port: int,
        timeout: float = 1.0,
        local_port: int = 0,
    ):
        super().__init__()
        self.host = host
        self.port = port
        self.timeout = timeout
        self.local_port = local_port
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._protocol: Optional[_UDPProtocol] = None

    async def connect(self) -> None:
        """Create a UDP endpoint and store the transport and protocol."""
        loop = asyncio.get_running_loop()
        self._transport, self._protocol = await loop.create_datagram_endpoint(
            lambda: _UDPProtocol(),
            local_addr=("0.0.0.0", self.local_port),
        )
        self.logger.info(f"UDP bound to port {self.local_port}")

    async def disconnect(self) -> None:
        """Close the UDP transport if open."""
        if self._transport:
            self._transport.close()
            self._transport = None
            self._protocol = None
        self.logger.info("UDP disconnected")

    async def execute(self, command_name: str, params: dict) -> Any:
        """
        Send a UDP command.

        payload resolution order:
            1. params["payload"] / params["command"] (bytes or str)
            2. command_name + params["value"]
        """
        payload = self._build_payload(command_name, params)
        await self._send(payload)
        if params.get("wait_response", False):
            return await self._recv(params.get("read_bytes", 1024))
        return {"status": "ok", "sent": len(payload)}

    async def read(self, field_name: str, params: Optional[dict] = None) -> Any:
        """Send a query datagram and await the response."""
        params = params or {}
        payload = self._build_payload(field_name, params)
        if payload:
            await self._send(payload)
        data = await self._recv(params.get("read_bytes", 1024))
        return {"status": "ok", "field": field_name, "data": data}

    def _build_payload(self, command_name: str, params: dict) -> bytes:
        raw = params.get("payload") or params.get("command") or command_name
        if not isinstance(raw, bytes):
            raw = str(raw)
            value = params.get("value")
            if value is not None:
                raw += f" {value}"
            raw = raw.encode("utf-8")
        else:
            value = params.get("value")
            if value is not None:
                raw += f" {value}".encode("utf-8")
        return raw

    async def _send(self, payload: bytes) -> None:
        if self._transport is None:
            raise RuntimeError("UDP driver not connected")
        self._transport.sendto(payload, (self.host, self.port))
        self.logger.debug(f"UDP sent {payload!r} to {self.host}:{self.port}")

    async def _recv(self, read_bytes: int = 1024) -> bytes:
        if self._protocol is None:
            raise RuntimeError("UDP driver not connected")
        try:
            return await asyncio.wait_for(self._protocol.recv(read_bytes), timeout=self.timeout)
        except asyncio.TimeoutError:
            self.logger.warning("UDP receive timeout")
            return b""


class _UDPProtocol(asyncio.DatagramProtocol):
    """Asyncio datagram protocol with a bounded receive queue.

    Network errors are logged and re-raised into the queue as an exception
    so that ``recv()`` callers see the real error instead of an empty
    bytes that silently masks a failure.
    """

    def __init__(self) -> None:
        self._buffer: asyncio.Queue = asyncio.Queue()

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        self._buffer.put_nowait(data)

    def error_received(self, exc: Exception) -> None:
        import logging

        logging.getLogger("Kamio.drivers.udp").warning(f"UDP protocol error: {exc}")
        # Put the exception in the queue so recv() callers see it.
        self._buffer.put_nowait(exc)

    async def recv(self, max_bytes: int) -> bytes:
        data = await self._buffer.get()
        if isinstance(data, Exception):
            raise data
        result: bytes = data[:max_bytes] if max_bytes > 0 else data
        return result
