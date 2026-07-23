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
    ):
        super().__init__()
        self.host = host
        self.port = port
        self.unit_id = unit_id
        self.timeout = timeout
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._lock = asyncio.Lock()
        self._transaction = 0

    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port),
            timeout=self.timeout,
        )
        self.logger.info(f"Modbus TCP connected to {self.host}:{self.port} unit={self.unit_id}")

    async def disconnect(self) -> None:
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
            self._writer = None
            self._reader = None
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
            await self._write_single_coil(address, bool(value))
        elif command in ("write_register", "register", "holding"):
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
        elif command_type in ("holding", "holding_register", "holding_registers", "register", "registers"):
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
        return_code, = struct.unpack(">B", response[:1])
        if return_code & 0x80:
            raise RuntimeError(f"Modbus exception response: {response[1:2].hex()}")
        if return_code != func:
            raise RuntimeError(f"Unexpected Modbus function code: {return_code}")
        byte_count, = struct.unpack(">B", response[1:2])
        data = response[2:2 + byte_count]
        if len(data) != byte_count:
            raise RuntimeError("Modbus response incomplete")
        return data

    async def _write_single_coil(self, address: int, value: bool) -> None:
        data = 0xFF00 if value else 0x0000
        pdu = struct.pack(">BHH", _WRITE_SINGLE_COIL, address, data)
        response = await self._transaction_exchange(pdu)
        return_code, = struct.unpack(">B", response[:1])
        if return_code & 0x80:
            raise RuntimeError("Modbus exception response")
        # echo of request
        if response != pdu:
            raise RuntimeError("Modbus write coil response mismatch")

    async def _write_single_register(self, address: int, value: int) -> None:
        pdu = struct.pack(">BHH", _WRITE_SINGLE_REGISTER, address, int(value))
        response = await self._transaction_exchange(pdu)
        return_code, = struct.unpack(">B", response[:1])
        if return_code & 0x80:
            raise RuntimeError("Modbus exception response")
        if response != pdu:
            raise RuntimeError("Modbus write register response mismatch")

    async def _write_multiple_registers(self, address: int, values: List[int]) -> None:
        count = len(values)
        byte_count = count * 2
        payload = b"".join(struct.pack(">H", int(v)) for v in values)
        pdu = (
            struct.pack(">BHHB", _WRITE_MULTIPLE_REGISTERS, address, count, byte_count)
            + payload
        )
        response = await self._transaction_exchange(pdu)
        return_code, = struct.unpack(">B", response[:1])
        if return_code & 0x80:
            raise RuntimeError("Modbus exception response")
        expected = struct.pack(">BHH", _WRITE_MULTIPLE_REGISTERS, address, count)
        if response[:5] != expected:
            raise RuntimeError("Modbus write registers response mismatch")

    async def _transaction_exchange(self, pdu: bytes) -> bytes:
        if self._writer is None or self._reader is None:
            raise RuntimeError("Modbus TCP not connected")

        async with self._lock:
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

            payload = await asyncio.wait_for(
                self._reader.readexactly(length - 1),
                timeout=self.timeout,
            )
            return payload


def _unpack_bits(data: bytes, count: int) -> List[bool]:
    bits = []
    for byte in data:
        for i in range(8):
            if len(bits) >= count:
                break
            bits.append(bool(byte & (1 << i)))
    return bits


def _unpack_registers(data: bytes) -> List[int]:
    if len(data) % 2 != 0:
        raise RuntimeError("Modbus register data length must be even")
    return [struct.unpack(">H", data[i:i + 2])[0] for i in range(0, len(data), 2)]
