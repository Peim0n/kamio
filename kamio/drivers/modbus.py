from __future__ import annotations

import asyncio
import struct
from typing import Any, List, Optional, Union

from .base import BaseDriver

# Modbus function codes
_READ_COILS = 0x01
_READ_DISCRETE_INPUTS = 0x02
_READ_HOLDING_REGISTERS = 0x03
_READ_INPUT_REGISTERS = 0x04
_WRITE_SINGLE_COIL = 0x05
_WRITE_SINGLE_REGISTER = 0x06
_WRITE_MULTIPLE_REGISTERS = 0x10

# Maximum Modbus PDU size per the spec is 253 bytes.  The MBAP length field
# counts the unit id byte too, so the on-the-wire length is at most 254.
# A server advertising a larger length is either broken or malicious; reading
# that many bytes would block until the timeout (or allocate unbounded memory).
_MAX_MBAP_LENGTH = 260  # 253 PDU + 1 unit id + small safety margin

# Per the Modbus specification, function 0x10 (Write Multiple Registers)
# accepts at most 123 registers per request.
_MAX_WRITE_REGISTERS = 123


class ModbusTCPDriver(BaseDriver):
    """
    Pure-stdlib Modbus TCP driver.

    No external dependencies: uses asyncio TCP sockets.
    Supports reading/writing coils and registers.

    Args:
        host: Modbus gateway host.
        port: Modbus TCP port (default 502).
        unit_id: Modbus slave/unit id (default 1).
        timeout: Connection/response timeout.
    """

    def __init__(
        self,
        host: str,
        port: int = 502,
        unit_id: int = 1,
        timeout: float = 1.0,
        reconnect_attempts: int = 1,
    ):
        super().__init__()
        self.host = host
        self.port = port
        self.unit_id = unit_id
        self.timeout = timeout
        self.reconnect_attempts = max(0, int(reconnect_attempts))
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._lock = asyncio.Lock()
        self._transaction = 0

    async def connect(self) -> None:
        """Open a TCP connection to the Modbus device."""
        await self._open_connection()
        self.logger.info(f"Modbus TCP connected to {self.host}:{self.port} unit={self.unit_id}")

    async def _open_connection(self) -> None:
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port),
            timeout=self.timeout,
        )

    async def _ensure_connected(self) -> None:
        """Reconnect once if the underlying stream is gone or broken."""
        if self._writer is None or self._writer.is_closing() or self._reader is None:
            await self._reconnect()

    async def _reconnect(self) -> None:
        """Close any lingering socket and re-establish the TCP connection."""
        await self._close_writer()
        await self._open_connection()
        self.logger.info(f"Modbus TCP reconnected to {self.host}:{self.port}")

    async def _close_writer(self) -> None:
        if self._writer is not None:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
        self._writer = None
        self._reader = None

    async def disconnect(self) -> None:
        """Close the TCP writer if open."""
        await self._close_writer()
        self.logger.info("Modbus TCP disconnected")

    async def read(self, field_name: str, params: Optional[dict] = None) -> Any:
        """
        Read a Modbus register or coil.

        params keys:
            - command / type: "coil", "discrete", "holding", "input" (default "holding")
            - address: register/coil address (int)
            - count: number of registers/coils (int, default 1)
        """
        params = params or {}
        command_type = params.get("command") or params.get("type") or field_name
        address = int(params.get("address", 0))
        count = int(params.get("count", 1))

        result = await self._read_type(command_type, address, count)
        return {"status": "ok", "field": field_name, "address": address, "data": result}

    async def execute(self, command_name: str, params: dict) -> Any:
        """
        Write a Modbus coil or register.

        command_name: write_coil, write_register, write_registers (or set_*).
        params keys:
            - address: int
            - value: bool/int for single coil/register
            - values: List[int] for write_registers
        """
        command = command_name.replace("set_", "")
        address = int(params.get("address", 0))
        value = params.get("value")
        values = params.get("values")

        if command in ("write_coil", "coil"):
            if value is None:
                raise ValueError("write_coil requires a 'value' parameter")
            await self._write_single_coil(address, bool(value))
        elif command in ("write_register", "register", "holding"):
            if value is None:
                raise ValueError("write_register requires a 'value' parameter")
            await self._write_single_register(address, int(value))
        elif command in ("write_registers", "registers"):
            payload = [int(v) for v in (values or [value])]
            await self._write_multiple_registers(address, payload)
        else:
            raise ValueError(f"Unsupported Modbus command: {command_name}")

        return {"status": "ok", "address": address, "value": value}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _read_type(self, command_type: str, address: int, count: int) -> Any:
        command_type = str(command_type).lower()
        if command_type in ("coil", "coils"):
            data = await self._read(_READ_COILS, address, count)
            bits = _unpack_bits(data, count)
            return bits if count > 1 else bits[0]
        elif command_type in ("discrete", "discrete_input", "discrete_inputs"):
            data = await self._read(_READ_DISCRETE_INPUTS, address, count)
            bits = _unpack_bits(data, count)
            return bits if count > 1 else bits[0]
        elif command_type in (
            "holding",
            "holding_register",
            "holding_registers",
            "register",
            "registers",
        ):
            data = await self._read(_READ_HOLDING_REGISTERS, address, count)
            regs = _unpack_registers(data)
            return regs if count > 1 else regs[0]
        elif command_type in ("input", "input_register", "input_registers"):
            data = await self._read(_READ_INPUT_REGISTERS, address, count)
            regs = _unpack_registers(data)
            return regs if count > 1 else regs[0]
        else:
            raise ValueError(f"Unsupported Modbus read type: {command_type}")

    async def _read(self, func: int, address: int, count: int) -> bytes:
        pdu = struct.pack(">BHH", func, address, count)
        response = await self._transaction_exchange(pdu)
        (return_code,) = struct.unpack(">B", response[:1])
        if return_code & 0x80:
            raise RuntimeError(f"Modbus exception response: {response[1:2].hex()}")
        if return_code != func:
            raise RuntimeError(f"Unexpected Modbus function code: {return_code}")
        (byte_count,) = struct.unpack(">B", response[1:2])
        data = response[2 : 2 + byte_count]
        if len(data) != byte_count:
            raise RuntimeError("Modbus response incomplete")
        return data

    async def _write_single_coil(self, address: int, value: bool) -> None:
        data = 0xFF00 if value else 0x0000
        pdu = struct.pack(">BHH", _WRITE_SINGLE_COIL, address, data)
        response = await self._transaction_exchange(pdu)
        (return_code,) = struct.unpack(">B", response[:1])
        if return_code & 0x80:
            raise RuntimeError("Modbus exception response")
        # echo of request
        if response != pdu:
            raise RuntimeError("Modbus write coil response mismatch")

    async def _write_single_register(self, address: int, value: int) -> None:
        pdu = struct.pack(">BHH", _WRITE_SINGLE_REGISTER, address, int(value))
        response = await self._transaction_exchange(pdu)
        (return_code,) = struct.unpack(">B", response[:1])
        if return_code & 0x80:
            raise RuntimeError("Modbus exception response")
        if response != pdu:
            raise RuntimeError("Modbus write register response mismatch")

    async def _write_multiple_registers(self, address: int, values: List[int]) -> None:
        count = len(values)
        if count > _MAX_WRITE_REGISTERS:
            raise ValueError(
                f"Cannot write {count} registers; Modbus maximum is {_MAX_WRITE_REGISTERS}"
            )
        byte_count = count * 2
        payload = b"".join(struct.pack(">H", int(v)) for v in values)
        pdu = struct.pack(">BHHB", _WRITE_MULTIPLE_REGISTERS, address, count, byte_count) + payload
        response = await self._transaction_exchange(pdu)
        (return_code,) = struct.unpack(">B", response[:1])
        if return_code & 0x80:
            raise RuntimeError("Modbus exception response")
        expected = struct.pack(">BHH", _WRITE_MULTIPLE_REGISTERS, address, count)
        if response[:5] != expected:
            raise RuntimeError("Modbus write registers response mismatch")

    async def _transaction_exchange(self, pdu: bytes) -> bytes:
        async with self._lock:
            last_exc: Optional[BaseException] = None
            for attempt in range(self.reconnect_attempts + 1):
                try:
                    return await self._exchange_once(pdu)
                except (
                    asyncio.IncompleteReadError,
                    ConnectionResetError,
                    BrokenPipeError,
                    RuntimeError,
                ) as e:
                    last_exc = e
                    # Connection-level errors → try to reconnect and retry.
                    if attempt < self.reconnect_attempts:
                        self.logger.warning(f"Modbus TCP exchange failed, reconnecting: {e}")
                        try:
                            await self._reconnect()
                        except Exception as re_err:
                            self.logger.error(f"Modbus TCP reconnect failed: {re_err}")
                            raise
                        continue
                    raise
            # Should be unreachable, but keep mypy happy.
            raise RuntimeError("Modbus TCP exchange failed") from last_exc

    async def _exchange_once(self, pdu: bytes) -> bytes:
        await self._ensure_connected()
        if self._writer is None or self._reader is None:
            raise RuntimeError("Modbus TCP not connected")

        self._transaction = (self._transaction + 1) & 0xFFFF
        tid = self._transaction
        mbap = struct.pack(">HHHB", tid, 0, len(pdu) + 1, self.unit_id)
        frame = mbap + pdu

        self._writer.write(frame)
        await self._writer.drain()

        try:
            header = await asyncio.wait_for(self._reader.readexactly(7), timeout=self.timeout)
        except asyncio.IncompleteReadError as e:
            raise RuntimeError("Modbus TCP connection closed before response") from e

        recv_tid, proto, length, unit = struct.unpack(">HHHB", header)
        if proto != 0:
            raise RuntimeError("Modbus TCP invalid protocol ID")
        if recv_tid != tid:
            raise RuntimeError("Modbus TCP transaction ID mismatch")
        if length < 2 or length > _MAX_MBAP_LENGTH:
            # length includes the unit id byte; a sane PDU is 1..253 bytes,
            # so length must be 2..254.  Reject broken/malicious servers that
            # advertise a huge read, which would otherwise block until timeout.
            raise RuntimeError(
                f"Modbus TCP invalid MBAP length {length} (expected 2..{_MAX_MBAP_LENGTH})"
            )

        payload = await asyncio.wait_for(
            self._reader.readexactly(length - 1),
            timeout=self.timeout,
        )
        return payload


def _unpack_bits(data: bytes, count: int) -> List[bool]:
    """Unpack a raw bytes payload into a list of boolean coil values."""
    bits: List[bool] = []
    for byte in data:
        for i in range(8):
            if len(bits) >= count:
                break
            bits.append(bool(byte & (1 << i)))
    return bits


def _unpack_registers(data: bytes) -> List[int]:
    """Unpack a raw bytes payload into a list of 16-bit register values."""
    if len(data) % 2 != 0:
        raise RuntimeError("Modbus register data length must be even")
    return [struct.unpack(">H", data[i : i + 2])[0] for i in range(0, len(data), 2)]
