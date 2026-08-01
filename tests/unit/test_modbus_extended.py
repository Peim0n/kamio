"""Comprehensive unit tests for kamio.drivers.modbus.

Covers the pure-stdlib Modbus TCP driver (ModbusTCPDriver) and its helper
functions. All network I/O is mocked via unittest.mock so no real sockets
are opened.
"""

from __future__ import annotations

import asyncio
import struct
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kamio.drivers.modbus import (
    _READ_COILS,
    _READ_DISCRETE_INPUTS,
    _READ_HOLDING_REGISTERS,
    _READ_INPUT_REGISTERS,
    _WRITE_MULTIPLE_REGISTERS,
    _WRITE_SINGLE_COIL,
    _WRITE_SINGLE_REGISTER,
    ModbusTCPDriver,
    _unpack_bits,
    _unpack_registers,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _make_driver(**kwargs) -> ModbusTCPDriver:
    defaults = dict(host="1.2.3.4", port=502, unit_id=2, timeout=0.5, reconnect_attempts=1)
    defaults.update(kwargs)
    return ModbusTCPDriver(**defaults)


def _make_connected_driver(**kwargs) -> ModbusTCPDriver:
    """Driver with mocked reader/writer already "connected"."""
    drv = _make_driver(**kwargs)
    reader = MagicMock(name="StreamReader")
    writer = MagicMock(name="StreamWriter")
    writer.is_closing.return_value = False
    writer.write = MagicMock()
    writer.drain = AsyncMock()
    writer.close = MagicMock()
    writer.wait_closed = AsyncMock()
    reader.readexactly = AsyncMock()
    drv._reader = reader
    drv._writer = writer
    return drv


def _mbap(tid: int, length: int, unit_id: int = 2) -> bytes:
    return struct.pack(">HHHB", tid, 0, length, unit_id)


# ---------------------------------------------------------------------------
# 1. __init__
# ---------------------------------------------------------------------------


class TestInit:
    def test_defaults(self):
        drv = ModbusTCPDriver("host")
        assert drv.host == "host"
        assert drv.port == 502
        assert drv.unit_id == 1
        assert drv.timeout == 1.0
        assert drv.reconnect_attempts == 1
        assert drv._reader is None
        assert drv._writer is None
        assert drv._transaction == 0
        assert isinstance(drv._lock, asyncio.Lock)

    def test_custom_params(self):
        drv = ModbusTCPDriver("h", port=1502, unit_id=7, timeout=2.5, reconnect_attempts=3)
        assert drv.host == "h"
        assert drv.port == 1502
        assert drv.unit_id == 7
        assert drv.timeout == 2.5
        assert drv.reconnect_attempts == 3

    def test_reconnect_attempts_clamped_to_zero(self):
        drv = ModbusTCPDriver("h", reconnect_attempts=-5)
        assert drv.reconnect_attempts == 0

    def test_logger_name(self):
        drv = ModbusTCPDriver("h")
        assert drv.logger.name == "Kamio.driver.ModbusTCPDriver"


# ---------------------------------------------------------------------------
# 2. connect()
# ---------------------------------------------------------------------------


class TestConnect:
    @pytest.mark.asyncio
    async def test_connect_calls_open_connection_and_logs(self, caplog):
        drv = _make_driver()
        drv._open_connection = AsyncMock()
        with caplog.at_level("INFO", logger="Kamio.driver.ModbusTCPDriver"):
            await drv.connect()
        drv._open_connection.assert_awaited_once()
        assert any("connected to 1.2.3.4:502 unit=2" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# 3. disconnect()
# ---------------------------------------------------------------------------


class TestDisconnect:
    @pytest.mark.asyncio
    async def test_disconnect_calls_close_writer_and_logs(self, caplog):
        drv = _make_driver()
        drv._close_writer = AsyncMock()
        with caplog.at_level("INFO", logger="Kamio.driver.ModbusTCPDriver"):
            await drv.disconnect()
        drv._close_writer.assert_awaited_once()
        assert any("disconnected" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# 4. _open_connection()
# ---------------------------------------------------------------------------


class TestOpenConnection:
    @pytest.mark.asyncio
    async def test_open_connection_uses_asyncio_open_connection(self):
        drv = _make_driver(timeout=0.25)
        reader, writer = MagicMock(), MagicMock()
        open_conn = AsyncMock(return_value=(reader, writer))
        with patch("kamio.drivers.modbus.asyncio.open_connection", open_conn):
            await drv._open_connection()
        open_conn.assert_awaited_once_with("1.2.3.4", 502)
        assert drv._reader is reader
        assert drv._writer is writer

    @pytest.mark.asyncio
    async def test_open_connection_timeout_propagates(self):
        drv = _make_driver(timeout=0.01)
        open_conn = AsyncMock(side_effect=asyncio.TimeoutError())
        with patch("kamio.drivers.modbus.asyncio.open_connection", open_conn):
            with pytest.raises(asyncio.TimeoutError):
                await drv._open_connection()


# ---------------------------------------------------------------------------
# 5. _ensure_connected()
# ---------------------------------------------------------------------------


class TestEnsureConnected:
    @pytest.mark.asyncio
    async def test_healthy_stream_no_reconnect(self):
        drv = _make_connected_driver()
        drv._reconnect = AsyncMock()
        await drv._ensure_connected()
        drv._reconnect.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_writer_is_closing_triggers_reconnect(self):
        drv = _make_connected_driver()
        drv._writer.is_closing.return_value = True
        drv._reconnect = AsyncMock()
        await drv._ensure_connected()
        drv._reconnect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_none_writer_triggers_reconnect(self):
        drv = _make_connected_driver()
        drv._writer = None
        drv._reconnect = AsyncMock()
        await drv._ensure_connected()
        drv._reconnect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_none_reader_triggers_reconnect(self):
        drv = _make_connected_driver()
        drv._reader = None
        drv._reconnect = AsyncMock()
        await drv._ensure_connected()
        drv._reconnect.assert_awaited_once()


# ---------------------------------------------------------------------------
# 6. _reconnect()
# ---------------------------------------------------------------------------


class TestReconnect:
    @pytest.mark.asyncio
    async def test_reconnect_closes_then_opens_and_logs(self, caplog):
        drv = _make_driver()
        drv._close_writer = AsyncMock()
        drv._open_connection = AsyncMock()
        with caplog.at_level("INFO", logger="Kamio.driver.ModbusTCPDriver"):
            await drv._reconnect()
        drv._close_writer.assert_awaited_once()
        drv._open_connection.assert_awaited_once()
        assert any("reconnected to 1.2.3.4:502" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# 7. _close_writer()
# ---------------------------------------------------------------------------


class TestCloseWriter:
    @pytest.mark.asyncio
    async def test_none_writer_is_noop(self):
        drv = _make_driver()
        drv._writer = None
        drv._reader = None
        await drv._close_writer()
        assert drv._writer is None
        assert drv._reader is None

    @pytest.mark.asyncio
    async def test_valid_writer_closed_and_cleared(self):
        drv = _make_connected_driver()
        writer = drv._writer
        drv._reader = MagicMock()  # set reader to something
        await drv._close_writer()
        writer.close.assert_called_once()
        writer.wait_closed.assert_awaited_once()
        assert drv._writer is None
        assert drv._reader is None

    @pytest.mark.asyncio
    async def test_writer_close_raises_is_swallowed(self):
        drv = _make_connected_driver()
        writer = drv._writer
        writer.close.side_effect = ConnectionResetError("boom")
        # should not raise
        await drv._close_writer()
        assert drv._writer is None
        assert drv._reader is None

    @pytest.mark.asyncio
    async def test_writer_wait_closed_raises_is_swallowed(self):
        drv = _make_connected_driver()
        writer = drv._writer
        writer.wait_closed.side_effect = OSError("boom")
        await drv._close_writer()
        assert drv._writer is None
        assert drv._reader is None


# ---------------------------------------------------------------------------
# 8. read() / _read_type()
# ---------------------------------------------------------------------------


class TestRead:
    @pytest.mark.asyncio
    async def test_read_coil_single(self):
        drv = _make_driver()
        # _read returns 1 byte; bit0 set -> True
        drv._read = AsyncMock(return_value=b"\x01")
        res = await drv.read("coil", {"command": "coil", "address": 10, "count": 1})
        drv._read.assert_awaited_once_with(_READ_COILS, 10, 1)
        assert res == {"status": "ok", "field": "coil", "address": 10, "data": True}

    @pytest.mark.asyncio
    async def test_read_coils_multiple(self):
        drv = _make_driver()
        drv._read = AsyncMock(return_value=b"\x03")
        res = await drv.read("coil", {"type": "coils", "address": 0, "count": 3})
        drv._read.assert_awaited_once_with(_READ_COILS, 0, 3)
        assert res["data"] == [True, True, False]

    @pytest.mark.asyncio
    async def test_read_discrete_single(self):
        drv = _make_driver()
        drv._read = AsyncMock(return_value=b"\x00")
        res = await drv.read("d", {"command": "discrete", "address": 1, "count": 1})
        drv._read.assert_awaited_once_with(_READ_DISCRETE_INPUTS, 1, 1)
        assert res["data"] is False

    @pytest.mark.asyncio
    async def test_read_discrete_multiple(self):
        drv = _make_driver()
        drv._read = AsyncMock(return_value=b"\x05")
        res = await drv.read("d", {"type": "discrete_inputs", "address": 1, "count": 3})
        drv._read.assert_awaited_once_with(_READ_DISCRETE_INPUTS, 1, 3)
        assert res["data"] == [True, False, True]

    @pytest.mark.asyncio
    async def test_read_holding_single(self):
        drv = _make_driver()
        drv._read = AsyncMock(return_value=b"\x00\x2a")
        res = await drv.read("holding", {"command": "holding", "address": 5, "count": 1})
        drv._read.assert_awaited_once_with(_READ_HOLDING_REGISTERS, 5, 1)
        assert res["data"] == 0x2A

    @pytest.mark.asyncio
    async def test_read_holding_multiple(self):
        drv = _make_driver()
        drv._read = AsyncMock(return_value=b"\x00\x01\x00\x02")
        res = await drv.read("holding", {"type": "registers", "address": 5, "count": 2})
        drv._read.assert_awaited_once_with(_READ_HOLDING_REGISTERS, 5, 2)
        assert res["data"] == [1, 2]

    @pytest.mark.asyncio
    async def test_read_input_single(self):
        drv = _make_driver()
        drv._read = AsyncMock(return_value=b"\x01\x00")
        res = await drv.read("input", {"command": "input", "address": 7, "count": 1})
        drv._read.assert_awaited_once_with(_READ_INPUT_REGISTERS, 7, 1)
        assert res["data"] == 0x100

    @pytest.mark.asyncio
    async def test_read_input_multiple(self):
        drv = _make_driver()
        drv._read = AsyncMock(return_value=b"\x00\x03\x00\x04")
        res = await drv.read("input", {"type": "input_registers", "address": 7, "count": 2})
        drv._read.assert_awaited_once_with(_READ_INPUT_REGISTERS, 7, 2)
        assert res["data"] == [3, 4]

    @pytest.mark.asyncio
    async def test_read_defaults_to_field_name_as_command(self):
        drv = _make_driver()
        drv._read = AsyncMock(return_value=b"\x00\x01")
        # field_name "holding" used as command_type when no command/type given
        await drv.read("holding", {"address": 1})
        drv._read.assert_awaited_once_with(_READ_HOLDING_REGISTERS, 1, 1)

    @pytest.mark.asyncio
    async def test_read_default_params(self):
        drv = _make_driver()
        drv._read = AsyncMock(return_value=b"\x00\x00")
        res = await drv.read("holding")
        drv._read.assert_awaited_once_with(_READ_HOLDING_REGISTERS, 0, 1)
        assert res["address"] == 0

    @pytest.mark.asyncio
    async def test_read_invalid_command_type_raises(self):
        drv = _make_driver()
        with pytest.raises(ValueError, match="Unsupported Modbus read type"):
            await drv.read("bogus", {"command": "bogus", "address": 0, "count": 1})


# ---------------------------------------------------------------------------
# 9. execute()
# ---------------------------------------------------------------------------


class TestExecute:
    @pytest.mark.asyncio
    async def test_write_single_coil_true(self):
        drv = _make_driver()
        drv._write_single_coil = AsyncMock()
        res = await drv.execute("write_coil", {"address": 3, "value": True})
        drv._write_single_coil.assert_awaited_once_with(3, True)
        assert res == {"status": "ok", "address": 3, "value": True}

    @pytest.mark.asyncio
    async def test_write_single_coil_false(self):
        drv = _make_driver()
        drv._write_single_coil = AsyncMock()
        await drv.execute("write_coil", {"address": 3, "value": False})
        drv._write_single_coil.assert_awaited_once_with(3, False)

    @pytest.mark.asyncio
    async def test_write_single_coil_alias_coil(self):
        drv = _make_driver()
        drv._write_single_coil = AsyncMock()
        await drv.execute("coil", {"address": 1, "value": 1})
        drv._write_single_coil.assert_awaited_once_with(1, True)

    @pytest.mark.asyncio
    async def test_write_single_register(self):
        drv = _make_driver()
        drv._write_single_register = AsyncMock()
        res = await drv.execute("write_register", {"address": 4, "value": 0x1234})
        drv._write_single_register.assert_awaited_once_with(4, 0x1234)
        assert res == {"status": "ok", "address": 4, "value": 0x1234}

    @pytest.mark.asyncio
    async def test_write_single_register_aliases(self):
        drv = _make_driver()
        drv._write_single_register = AsyncMock()
        await drv.execute("register", {"address": 4, "value": 9})
        drv._write_single_register.assert_awaited_once_with(4, 9)
        drv._write_single_register.reset_mock()
        await drv.execute("holding", {"address": 4, "value": 9})
        drv._write_single_register.assert_awaited_once_with(4, 9)

    @pytest.mark.asyncio
    async def test_write_multiple_registers_with_values(self):
        drv = _make_driver()
        drv._write_multiple_registers = AsyncMock()
        res = await drv.execute("write_registers", {"address": 2, "values": [1, 2, 3]})
        drv._write_multiple_registers.assert_awaited_once_with(2, [1, 2, 3])
        assert res == {"status": "ok", "address": 2, "value": None}

    @pytest.mark.asyncio
    async def test_write_multiple_registers_falls_back_to_value(self):
        drv = _make_driver()
        drv._write_multiple_registers = AsyncMock()
        await drv.execute("registers", {"address": 2, "value": 7})
        drv._write_multiple_registers.assert_awaited_once_with(2, [7])

    @pytest.mark.asyncio
    async def test_set_prefix_stripped(self):
        drv = _make_driver()
        drv._write_single_register = AsyncMock()
        await drv.execute("set_register", {"address": 4, "value": 9})
        drv._write_single_register.assert_awaited_once_with(4, 9)

    @pytest.mark.asyncio
    async def test_execute_invalid_command_raises(self):
        drv = _make_driver()
        with pytest.raises(ValueError, match="Unsupported Modbus command"):
            await drv.execute("bogus", {"address": 0, "value": 0})

    @pytest.mark.asyncio
    async def test_write_coil_missing_value_raises(self):
        drv = _make_driver()
        with pytest.raises(ValueError, match="requires a 'value'"):
            await drv.execute("write_coil", {"address": 0})

    @pytest.mark.asyncio
    async def test_write_register_missing_value_raises(self):
        drv = _make_driver()
        with pytest.raises(ValueError, match="requires a 'value'"):
            await drv.execute("write_register", {"address": 0})


# ---------------------------------------------------------------------------
# 10. _read()
# ---------------------------------------------------------------------------


class TestReadInternal:
    @pytest.mark.asyncio
    async def test_valid_response(self):
        drv = _make_driver()
        # func=0x03, byte_count=2, data=0x0001
        drv._transaction_exchange = AsyncMock(return_value=b"\x03\x02\x00\x01")
        data = await drv._read(_READ_HOLDING_REGISTERS, 0, 1)
        pdu = struct.pack(">BHH", _READ_HOLDING_REGISTERS, 0, 1)
        drv._transaction_exchange.assert_awaited_once_with(pdu)
        assert data == b"\x00\x01"

    @pytest.mark.asyncio
    async def test_exception_response(self):
        drv = _make_driver()
        drv._transaction_exchange = AsyncMock(return_value=b"\x83\x02")
        with pytest.raises(RuntimeError, match="Modbus exception response"):
            await drv._read(_READ_HOLDING_REGISTERS, 0, 1)

    @pytest.mark.asyncio
    async def test_wrong_function_code(self):
        drv = _make_driver()
        # return_code 0x04 (no exception bit) but expected 0x03
        drv._transaction_exchange = AsyncMock(return_value=b"\x04\x00")
        with pytest.raises(RuntimeError, match="Unexpected Modbus function code"):
            await drv._read(_READ_HOLDING_REGISTERS, 0, 1)

    @pytest.mark.asyncio
    async def test_incomplete_data(self):
        drv = _make_driver()
        # byte_count=4 but only 2 bytes of data follow
        drv._transaction_exchange = AsyncMock(return_value=b"\x03\x04\x00\x01")
        with pytest.raises(RuntimeError, match="Modbus response incomplete"):
            await drv._read(_READ_HOLDING_REGISTERS, 0, 2)


# ---------------------------------------------------------------------------
# 11. _write_single_coil()
# ---------------------------------------------------------------------------


class TestWriteSingleCoil:
    @pytest.mark.asyncio
    async def test_success_true(self):
        drv = _make_driver()
        pdu = struct.pack(">BHH", _WRITE_SINGLE_COIL, 5, 0xFF00)
        drv._transaction_exchange = AsyncMock(return_value=pdu)
        await drv._write_single_coil(5, True)
        drv._transaction_exchange.assert_awaited_once_with(pdu)

    @pytest.mark.asyncio
    async def test_success_false(self):
        drv = _make_driver()
        pdu = struct.pack(">BHH", _WRITE_SINGLE_COIL, 5, 0x0000)
        drv._transaction_exchange = AsyncMock(return_value=pdu)
        await drv._write_single_coil(5, False)
        drv._transaction_exchange.assert_awaited_once_with(pdu)

    @pytest.mark.asyncio
    async def test_exception_response(self):
        drv = _make_driver()
        drv._transaction_exchange = AsyncMock(return_value=b"\x85\x02")
        with pytest.raises(RuntimeError, match="Modbus exception response"):
            await drv._write_single_coil(5, True)

    @pytest.mark.asyncio
    async def test_response_mismatch(self):
        drv = _make_driver()
        pdu = struct.pack(">BHH", _WRITE_SINGLE_COIL, 5, 0xFF00)
        # different address in echo
        bad = struct.pack(">BHH", _WRITE_SINGLE_COIL, 6, 0xFF00)
        drv._transaction_exchange = AsyncMock(return_value=bad)
        with pytest.raises(RuntimeError, match="write coil response mismatch"):
            await drv._write_single_coil(5, True)


# ---------------------------------------------------------------------------
# 12. _write_single_register()
# ---------------------------------------------------------------------------


class TestWriteSingleRegister:
    @pytest.mark.asyncio
    async def test_success(self):
        drv = _make_driver()
        pdu = struct.pack(">BHH", _WRITE_SINGLE_REGISTER, 8, 0x1234)
        drv._transaction_exchange = AsyncMock(return_value=pdu)
        await drv._write_single_register(8, 0x1234)
        drv._transaction_exchange.assert_awaited_once_with(pdu)

    @pytest.mark.asyncio
    async def test_exception_response(self):
        drv = _make_driver()
        drv._transaction_exchange = AsyncMock(return_value=b"\x86\x03")
        with pytest.raises(RuntimeError, match="Modbus exception response"):
            await drv._write_single_register(8, 1)

    @pytest.mark.asyncio
    async def test_response_mismatch(self):
        drv = _make_driver()
        pdu = struct.pack(">BHH", _WRITE_SINGLE_REGISTER, 8, 0x1234)
        bad = struct.pack(">BHH", _WRITE_SINGLE_REGISTER, 9, 0x1234)
        drv._transaction_exchange = AsyncMock(return_value=bad)
        with pytest.raises(RuntimeError, match="write register response mismatch"):
            await drv._write_single_register(8, 0x1234)


# ---------------------------------------------------------------------------
# 13. _write_multiple_registers()
# ---------------------------------------------------------------------------


class TestWriteMultipleRegisters:
    @pytest.mark.asyncio
    async def test_success(self):
        drv = _make_driver()
        values = [1, 2, 3]
        count = 3
        byte_count = 6
        payload = b"".join(struct.pack(">H", v) for v in values)
        pdu = struct.pack(">BHHB", _WRITE_MULTIPLE_REGISTERS, 2, count, byte_count) + payload
        # response: func, address, count
        resp = struct.pack(">BHH", _WRITE_MULTIPLE_REGISTERS, 2, count) + b"\x99"
        drv._transaction_exchange = AsyncMock(return_value=resp)
        await drv._write_multiple_registers(2, values)
        drv._transaction_exchange.assert_awaited_once_with(pdu)

    @pytest.mark.asyncio
    async def test_exception_response(self):
        drv = _make_driver()
        drv._transaction_exchange = AsyncMock(return_value=b"\x90\x04")
        with pytest.raises(RuntimeError, match="Modbus exception response"):
            await drv._write_multiple_registers(2, [1, 2])

    @pytest.mark.asyncio
    async def test_response_mismatch(self):
        drv = _make_driver()
        resp = struct.pack(">BHH", _WRITE_MULTIPLE_REGISTERS, 9, 2)
        drv._transaction_exchange = AsyncMock(return_value=resp)
        with pytest.raises(RuntimeError, match="write registers response mismatch"):
            await drv._write_multiple_registers(2, [1, 2])

    @pytest.mark.asyncio
    async def test_exceeds_max_registers(self):
        """Writing more than 123 registers should raise ValueError."""
        drv = _make_driver()
        too_many = list(range(124))
        with pytest.raises(ValueError, match="Modbus maximum is 123"):
            await drv._write_multiple_registers(2, too_many)


# ---------------------------------------------------------------------------
# 14. _transaction_exchange()
# ---------------------------------------------------------------------------


class TestTransactionExchange:
    @pytest.mark.asyncio
    async def test_success_first_try(self):
        drv = _make_driver(reconnect_attempts=2)
        drv._exchange_once = AsyncMock(return_value=b"\x03\x01\x01")
        res = await drv._transaction_exchange(b"PDU")
        drv._exchange_once.assert_awaited_once_with(b"PDU")
        assert res == b"\x03\x01\x01"

    @pytest.mark.asyncio
    async def test_retry_on_runtime_error_then_success(self, caplog):
        drv = _make_driver(reconnect_attempts=1)
        drv._exchange_once = AsyncMock(side_effect=[RuntimeError("boom"), b"\x03\x01\x01"])
        drv._reconnect = AsyncMock()
        with caplog.at_level("WARNING", logger="Kamio.driver.ModbusTCPDriver"):
            res = await drv._transaction_exchange(b"PDU")
        assert drv._exchange_once.await_count == 2
        drv._reconnect.assert_awaited_once()
        assert res == b"\x03\x01\x01"
        assert any("exchange failed, reconnecting" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_retry_on_connection_reset_error(self):
        drv = _make_driver(reconnect_attempts=1)
        drv._exchange_once = AsyncMock(side_effect=[ConnectionResetError("reset"), b"\x03\x01\x01"])
        drv._reconnect = AsyncMock()
        res = await drv._transaction_exchange(b"PDU")
        assert drv._exchange_once.await_count == 2
        assert res == b"\x03\x01\x01"

    @pytest.mark.asyncio
    async def test_retry_on_broken_pipe(self):
        drv = _make_driver(reconnect_attempts=1)
        drv._exchange_once = AsyncMock(side_effect=[BrokenPipeError("pipe"), b"\x03\x01\x01"])
        drv._reconnect = AsyncMock()
        res = await drv._transaction_exchange(b"PDU")
        assert res == b"\x03\x01\x01"

    @pytest.mark.asyncio
    async def test_retry_on_incomplete_read(self):
        drv = _make_driver(reconnect_attempts=1)
        drv._exchange_once = AsyncMock(
            side_effect=[asyncio.IncompleteReadError(b"", 7), b"\x03\x01\x01"]
        )
        drv._reconnect = AsyncMock()
        res = await drv._transaction_exchange(b"PDU")
        assert res == b"\x03\x01\x01"

    @pytest.mark.asyncio
    async def test_exhaustion_of_retries_raises(self, caplog):
        drv = _make_driver(reconnect_attempts=1)
        drv._exchange_once = AsyncMock(side_effect=RuntimeError("boom"))
        drv._reconnect = AsyncMock()
        with caplog.at_level("WARNING", logger="Kamio.driver.ModbusTCPDriver"):
            with pytest.raises(RuntimeError, match="boom"):
                await drv._transaction_exchange(b"PDU")
        assert drv._exchange_once.await_count == 2
        drv._reconnect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_zero_reconnect_attempts_no_retry(self):
        drv = _make_driver(reconnect_attempts=0)
        drv._exchange_once = AsyncMock(side_effect=RuntimeError("boom"))
        drv._reconnect = AsyncMock()
        with pytest.raises(RuntimeError, match="boom"):
            await drv._transaction_exchange(b"PDU")
        drv._exchange_once.assert_awaited_once_with(b"PDU")
        drv._reconnect.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_reconnect_failure_reraises(self, caplog):
        drv = _make_driver(reconnect_attempts=1)
        drv._exchange_once = AsyncMock(side_effect=RuntimeError("boom"))
        drv._reconnect = AsyncMock(side_effect=OSError("no route"))
        with caplog.at_level("ERROR", logger="Kamio.driver.ModbusTCPDriver"):
            with pytest.raises(OSError, match="no route"):
                await drv._transaction_exchange(b"PDU")
        assert any("reconnect failed" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# 15. _exchange_once()
# ---------------------------------------------------------------------------


class TestExchangeOnce:
    @pytest.mark.asyncio
    async def test_success_builds_frame_and_reads_response(self):
        drv = _make_connected_driver(unit_id=2)
        drv._transaction = 4
        pdu = b"\x03\x00\x00\x00\x01"
        # MBAP: tid=5, proto=0, length=len(pdu)+1=6, unit=2
        mbap = _mbap(5, len(pdu) + 1, 2)
        # response header: tid=5, proto=0, length=4, unit=2 -> payload length-1=3
        resp_header = struct.pack(">HHHB", 5, 0, 4, 2)
        resp_payload = b"\x03\x02\x00\x01"[:3]  # 3 bytes
        drv._reader.readexactly = AsyncMock(side_effect=[resp_header, resp_payload])

        result = await drv._exchange_once(pdu)
        # verify frame written
        written = drv._writer.write.call_args[0][0]
        assert written == mbap + pdu
        drv._writer.drain.assert_awaited_once()
        assert result == resp_payload
        assert drv._transaction == 5

    @pytest.mark.asyncio
    async def test_transaction_id_wraps_at_ffff(self):
        drv = _make_connected_driver()
        drv._transaction = 0xFFFF
        pdu = b"\x03\x00\x00\x00\x01"
        resp_header = struct.pack(">HHHB", 0, 0, 4, 2)
        resp_payload = b"\x03\x02\x00"
        drv._reader.readexactly = AsyncMock(side_effect=[resp_header, resp_payload])
        await drv._exchange_once(pdu)
        assert drv._transaction == 0

    @pytest.mark.asyncio
    async def test_not_connected_after_ensure_raises(self):
        drv = _make_connected_driver()
        drv._ensure_connected = AsyncMock()
        drv._writer = None
        drv._reader = None
        with pytest.raises(RuntimeError, match="Modbus TCP not connected"):
            await drv._exchange_once(b"\x03")

    @pytest.mark.asyncio
    async def test_incomplete_read_header_raises_runtime(self):
        drv = _make_connected_driver()
        drv._reader.readexactly = AsyncMock(side_effect=asyncio.IncompleteReadError(b"", 7))
        with pytest.raises(RuntimeError, match="connection closed before response"):
            await drv._exchange_once(b"\x03")

    @pytest.mark.asyncio
    async def test_invalid_protocol_id_raises(self):
        drv = _make_connected_driver()
        drv._transaction = 0
        pdu = b"\x03\x00\x00\x00\x01"
        # proto=1 (invalid)
        resp_header = struct.pack(">HHHB", 1, 1, 4, 2)
        drv._reader.readexactly = AsyncMock(return_value=resp_header)
        with pytest.raises(RuntimeError, match="invalid protocol ID"):
            await drv._exchange_once(pdu)

    @pytest.mark.asyncio
    async def test_transaction_id_mismatch_raises(self):
        drv = _make_connected_driver()
        drv._transaction = 0
        pdu = b"\x03\x00\x00\x00\x01"
        # tid=99 != 1
        resp_header = struct.pack(">HHHB", 99, 0, 4, 2)
        drv._reader.readexactly = AsyncMock(return_value=resp_header)
        with pytest.raises(RuntimeError, match="transaction ID mismatch"):
            await drv._exchange_once(pdu)

    @pytest.mark.asyncio
    async def test_ensure_connected_invoked(self):
        drv = _make_connected_driver()
        drv._ensure_connected = AsyncMock()
        drv._transaction = 0
        pdu = b"\x03\x00\x00\x00\x01"
        resp_header = struct.pack(">HHHB", 1, 0, 4, 2)
        resp_payload = b"\x03\x02\x00"
        drv._reader.readexactly = AsyncMock(side_effect=[resp_header, resp_payload])
        await drv._exchange_once(pdu)
        drv._ensure_connected.assert_awaited_once()


# ---------------------------------------------------------------------------
# 16. Helper functions
# ---------------------------------------------------------------------------


class TestUnpackBits:
    def test_single_byte_single_bit(self):
        assert _unpack_bits(b"\x01", 1) == [True]

    def test_single_byte_all_bits(self):
        # 0x05 -> bit0=True, bit1=False, bit2=True
        assert _unpack_bits(b"\x05", 3) == [True, False, True]

    def test_multiple_bytes(self):
        # byte0=0x01 (bit0=1, bit1=0), count=2 → [True, False]
        assert _unpack_bits(b"\x01\x02", 2) == [True, False]
        # 0x03 = bit0=1, bit1=1 → [True, True]
        assert _unpack_bits(b"\x03", 2) == [True, True]

    def test_count_truncates_within_byte(self):
        # 0xFF but only 3 bits requested
        assert _unpack_bits(b"\xff", 3) == [True, True, True]

    def test_count_truncates_across_bytes(self):
        # 0xFF 0xFF, only 10 bits
        bits = _unpack_bits(b"\xff\xff", 10)
        assert bits == [True] * 10
        assert len(bits) == 10

    def test_empty_data(self):
        assert _unpack_bits(b"", 0) == []

    def test_all_false(self):
        assert _unpack_bits(b"\x00", 4) == [False, False, False, False]


class TestUnpackRegisters:
    def test_single_register(self):
        assert _unpack_registers(b"\x00\x2a") == [0x2A]

    def test_multiple_registers(self):
        assert _unpack_registers(b"\x00\x01\x00\x02\xff\xff") == [1, 2, 0xFFFF]

    def test_empty(self):
        assert _unpack_registers(b"") == []

    def test_odd_length_raises(self):
        with pytest.raises(RuntimeError, match="length must be even"):
            _unpack_registers(b"\x00\x01\x02")


# ---------------------------------------------------------------------------
# Integration-ish: full read() path through mocked _exchange_once
# ---------------------------------------------------------------------------


class TestReadFullPath:
    @pytest.mark.asyncio
    async def test_read_holding_through_exchange_once(self):
        drv = _make_connected_driver()
        drv._transaction = 0
        # request: read holding reg, address=10, count=1
        pdu = struct.pack(">BHH", _READ_HOLDING_REGISTERS, 10, 1)
        resp_payload = b"\x03\x02\x00\x2a"  # func, byte_count=2, data=0x002A
        resp_header = struct.pack(">HHHB", 1, 0, len(resp_payload) + 1, 2)
        drv._reader.readexactly = AsyncMock(side_effect=[resp_header, resp_payload])
        res = await drv.read("holding", {"command": "holding", "address": 10, "count": 1})
        assert res["data"] == 0x2A
