from __future__ import annotations

import asyncio
from typing import Any, Iterable, Optional, Union

from .base import BYTE_TRANSPORT_COMMANDS, BaseDriver, require_payload


def _encode(payload: Union[str, bytes]) -> bytes:
    return payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)


class UDPDriver(BaseDriver):
    """
    Asynchronous UDP driver for request/response protocols.

    Supports plain send/receive and simple command mapping:
        execute("send", {"payload": "PWR ON"})
        read("temperature", {"payload": "GET TEMP", "read_bytes": 1024})

    Args:
        host: Target host.
        port: Target port.
        timeout: Read timeout in seconds.
        local_port: Optional local port to bind; 0 lets the OS choose.
    """

    COMMANDS = BYTE_TRANSPORT_COMMANDS

    def __init__(
        self,
        host: str,
        port: int,
        timeout: float = 1.0,
        local_port: int = 0,
        *,
        commands: Optional[Iterable[str]] = None,
    ):
        super().__init__(commands=commands)
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
        """Transport commands: ``send`` (one datagram) / ``request`` (send + await reply).

        ``params["payload"]`` (str/bytes) is the datagram; ``request`` reads up
        to ``params["read_bytes"]`` (default 1024) bytes.
        """
        payload = _encode(require_payload(params, command_name))
        await self._send(payload)
        if command_name == "send":
            return {"status": "ok", "command": "send", "sent": len(payload)}
        response = await self._recv(params.get("read_bytes", 1024))
        return {"status": "ok", "command": "request", "response": response}

    async def read(self, field_name: str, params: Optional[dict] = None) -> Any:
        """Send ``params["payload"]`` (``""`` = just listen) and await a datagram."""
        params = params or {}
        payload = params.get("payload")
        if payload is None:
            raise NotImplementedError("UDPDriver.read requires params['payload']")
        data = _encode(payload)
        if data:
            await self._send(data)
        response = await self._recv(params.get("read_bytes", 1024))
        return {"status": "ok", "field": field_name, "data": response}

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

    #: Upper bound for the receive queue — a datagram flood must not grow
    #: memory without limit; oldest items are dropped when it fills.
    _QUEUE_LIMIT = 256

    def __init__(self) -> None:
        self._buffer: asyncio.Queue = asyncio.Queue(maxsize=self._QUEUE_LIMIT)

    def _enqueue(self, item) -> None:
        try:
            self._buffer.put_nowait(item)
        except asyncio.QueueFull:
            self._buffer.get_nowait()  # drop oldest
            self._buffer.put_nowait(item)

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        self._enqueue(data)

    def error_received(self, exc: Exception) -> None:
        import logging

        logging.getLogger("Kamio.drivers.udp").warning(f"UDP protocol error: {exc}")
        # Put the exception in the queue so recv() callers see it.
        self._enqueue(exc)

    async def recv(self, max_bytes: int) -> bytes:
        data = await self._buffer.get()
        if isinstance(data, Exception):
            raise data
        result: bytes = data[:max_bytes] if max_bytes > 0 else data
        return result
