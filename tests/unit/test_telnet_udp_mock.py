"""Comprehensive unit tests for the Telnet, UDP, and Mock drivers.

All network and timing dependencies are mocked so the suite runs quickly
and deterministically without hardware or a live network.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kamio.drivers.mock import MockHardwareDriver
from kamio.drivers.telnet import TelnetDriver
from kamio.drivers.udp import UDPDriver, _UDPProtocol


# ---------------------------------------------------------------------------
# TelnetDriver
# ---------------------------------------------------------------------------
class TestTelnetDriver:
    def test_init_defaults(self):
        drv = TelnetDriver("10.0.0.1")
        assert drv.host == "10.0.0.1"
        assert drv.port == 23
        assert drv.timeout == 5.0
        assert drv.max_reconnect_attempts == 3
        assert drv.reader is None
        assert drv.writer is None

    def test_init_custom(self):
        drv = TelnetDriver("10.0.0.1", port=2323, timeout=2.0, max_reconnect_attempts=5)
        assert drv.port == 2323
        assert drv.timeout == 2.0
        assert drv.max_reconnect_attempts == 5

    @pytest.mark.asyncio
    async def test_connect_success(self):
        drv = TelnetDriver("10.0.0.1")
        reader = MagicMock()
        writer = MagicMock()
        with patch(
            "kamio.drivers.telnet.asyncio.open_connection",
            new=AsyncMock(return_value=(reader, writer)),
        ):
            await drv.connect()
        assert drv.reader is reader
        assert drv.writer is writer

    @pytest.mark.asyncio
    async def test_connect_failure_propagates(self):
        drv = TelnetDriver("10.0.0.1", timeout=0.1)
        with patch(
            "kamio.drivers.telnet.asyncio.open_connection",
            new=AsyncMock(side_effect=ConnectionRefusedError("nope")),
        ):
            with pytest.raises(ConnectionRefusedError):
                await drv.connect()
        assert drv.writer is None

    @pytest.mark.asyncio
    async def test_disconnect(self):
        drv = TelnetDriver("10.0.0.1")
        writer = MagicMock()
        writer.wait_closed = AsyncMock()
        drv.writer = writer
        drv.reader = MagicMock()
        await drv.disconnect()
        writer.close.assert_called_once()
        assert drv.writer is None
        assert drv.reader is None

    @pytest.mark.asyncio
    async def test_disconnect_when_not_connected(self):
        drv = TelnetDriver("10.0.0.1")
        # Should be a no-op without raising.
        await drv.disconnect()
        assert drv.writer is None

    @pytest.mark.asyncio
    async def test_disconnect_handles_close_error(self):
        """disconnect() should not raise if writer.close() fails."""
        drv = TelnetDriver("10.0.0.1")
        writer = MagicMock()
        writer.wait_closed = AsyncMock(side_effect=RuntimeError("close fail"))
        drv.reader, drv.writer = MagicMock(), writer
        await drv.disconnect()  # should not raise
        assert drv.writer is None
        assert drv.reader is None

    @pytest.mark.asyncio
    async def test_execute_basic(self):
        drv = TelnetDriver("10.0.0.1")
        reader = MagicMock()
        reader.readline = AsyncMock(return_value=b"OK\n")
        writer = MagicMock()
        writer.drain = AsyncMock()
        drv.reader, drv.writer = reader, writer
        # Patch _ensure_connected to avoid reconnect logic.
        with patch.object(drv, "_ensure_connected", new=AsyncMock()):
            result = await drv.execute("ping", {})
        writer.write.assert_called_once_with(b"ping\n")
        assert result == {"status": "ok", "command": "ping", "response": "OK"}

    @pytest.mark.asyncio
    async def test_execute_with_value_and_command_param(self):
        drv = TelnetDriver("10.0.0.1")
        reader = MagicMock()
        reader.readline = AsyncMock(return_value=b"DONE\n")
        writer = MagicMock()
        writer.drain = AsyncMock()
        drv.reader, drv.writer = reader, writer
        with patch.object(drv, "_ensure_connected", new=AsyncMock()):
            result = await drv.execute("ignored", {"command": "SET", "value": 42})
        writer.write.assert_called_once_with(b"SET 42\n")
        assert result["response"] == "DONE"

    @pytest.mark.asyncio
    async def test_execute_no_response(self):
        drv = TelnetDriver("10.0.0.1")
        reader = MagicMock()
        reader.readline = AsyncMock(return_value=b"should not be called\n")
        writer = MagicMock()
        writer.drain = AsyncMock()
        drv.reader, drv.writer = reader, writer
        with patch.object(drv, "_ensure_connected", new=AsyncMock()):
            result = await drv.execute("ping", {"wait_response": False})
        reader.readline.assert_not_called()
        assert result == {"status": "ok", "command": "ping", "response": ""}

    @pytest.mark.asyncio
    async def test_execute_read_timeout(self):
        drv = TelnetDriver("10.0.0.1", timeout=0.01)
        reader = MagicMock()
        reader.readline = AsyncMock(side_effect=asyncio.TimeoutError)
        writer = MagicMock()
        writer.drain = AsyncMock()
        drv.reader, drv.writer = reader, writer
        with patch.object(drv, "_ensure_connected", new=AsyncMock()):
            result = await drv.execute("ping", {})
        assert result == {"status": "ok", "command": "ping", "response": ""}

    @pytest.mark.asyncio
    async def test_execute_write_failure_then_reconnect(self):
        drv = TelnetDriver("10.0.0.1")
        reader = MagicMock()
        reader.readline = AsyncMock(return_value=b"OK\n")
        writer = MagicMock()
        writer.drain = AsyncMock(side_effect=[ConnectionError("broken"), None])
        drv.reader, drv.writer = reader, writer
        with patch.object(drv, "_ensure_connected", new=AsyncMock()):
            result = await drv.execute("ping", {})
        # write called twice (retry after failure)
        assert writer.write.call_count == 2
        assert result["response"] == "OK"

    @pytest.mark.asyncio
    async def test_read_basic(self):
        drv = TelnetDriver("10.0.0.1")
        reader = MagicMock()
        reader.readline = AsyncMock(return_value=b"42\n")
        writer = MagicMock()
        writer.drain = AsyncMock()
        drv.reader, drv.writer = reader, writer
        with patch.object(drv, "_ensure_connected", new=AsyncMock()):
            result = await drv.read("temperature", {"command": "GET TEMP"})
        writer.write.assert_called_once_with(b"GET TEMP\n")
        assert result == {"status": "ok", "field": "temperature", "response": "42"}

    @pytest.mark.asyncio
    async def test_read_with_value(self):
        drv = TelnetDriver("10.0.0.1")
        reader = MagicMock()
        reader.readline = AsyncMock(return_value=b"ok\n")
        writer = MagicMock()
        writer.drain = AsyncMock()
        drv.reader, drv.writer = reader, writer
        with patch.object(drv, "_ensure_connected", new=AsyncMock()):
            await drv.read("field", {"command": "CMD", "value": "X"})
        writer.write.assert_called_once_with(b"CMD X\n")

    @pytest.mark.asyncio
    async def test_read_no_command(self):
        drv = TelnetDriver("10.0.0.1")
        reader = MagicMock()
        reader.readline = AsyncMock(return_value=b"data\n")
        writer = MagicMock()
        writer.drain = AsyncMock()
        drv.reader, drv.writer = reader, writer
        with patch.object(drv, "_ensure_connected", new=AsyncMock()):
            result = await drv.read("field", {"command": ""})
        writer.write.assert_not_called()
        assert result["response"] == "data"

    @pytest.mark.asyncio
    async def test_read_timeout(self):
        drv = TelnetDriver("10.0.0.1", timeout=0.01)
        reader = MagicMock()
        reader.readline = AsyncMock(side_effect=asyncio.TimeoutError)
        writer = MagicMock()
        writer.drain = AsyncMock()
        drv.reader, drv.writer = reader, writer
        with patch.object(drv, "_ensure_connected", new=AsyncMock()):
            result = await drv.read("field", {})
        assert result == {"status": "ok", "field": "field", "response": ""}

    @pytest.mark.asyncio
    async def test_ensure_connected_already_connected(self):
        drv = TelnetDriver("10.0.0.1")
        writer = MagicMock()
        writer.is_closing.return_value = False
        drv.writer = writer
        with patch.object(drv, "connect", new=AsyncMock()) as connect_mock:
            await drv._ensure_connected()
        connect_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_ensure_connected_reconnects(self):
        drv = TelnetDriver("10.0.0.1")
        drv.writer = None  # not connected
        with patch.object(drv, "disconnect", new=AsyncMock()) as disconnect_mock:
            with patch.object(drv, "connect", new=AsyncMock()) as connect_mock:
                await drv._ensure_connected()
        disconnect_mock.assert_called_once()
        connect_mock.assert_called_once()

    @pytest.mark.asyncio
    async def test_ensure_connected_retries_then_fails(self):
        drv = TelnetDriver("10.0.0.1", max_reconnect_attempts=2)
        drv.writer = None
        connect_mock = AsyncMock(side_effect=ConnectionError("fail"))
        with patch.object(drv, "disconnect", new=AsyncMock()):
            with patch.object(drv, "connect", new=connect_mock):
                with patch("kamio.drivers.telnet.asyncio.sleep", new=AsyncMock()):
                    with pytest.raises(ConnectionError):
                        await drv._ensure_connected()
        # connect attempted max_reconnect_attempts times
        assert connect_mock.await_count == 2

    @pytest.mark.asyncio
    async def test_ensure_connected_retries_then_succeeds(self):
        drv = TelnetDriver("10.0.0.1", max_reconnect_attempts=3)
        drv.writer = None
        reader = MagicMock()
        writer = MagicMock()
        writer.is_closing.return_value = False
        connect_mock = AsyncMock(side_effect=[ConnectionError("fail"), None])
        with patch.object(drv, "disconnect", new=AsyncMock()):
            with patch("kamio.drivers.telnet.asyncio.sleep", new=AsyncMock()):
                with patch.object(drv, "connect", new=connect_mock):
                    await drv._ensure_connected()
        assert connect_mock.await_count == 2


# ---------------------------------------------------------------------------
# UDPDriver
# ---------------------------------------------------------------------------
class TestUDPDriver:
    def test_init_defaults(self):
        drv = UDPDriver("10.0.0.2", 5000)
        assert drv.host == "10.0.0.2"
        assert drv.port == 5000
        assert drv.timeout == 1.0
        assert drv.local_port == 0
        assert drv._transport is None
        assert drv._protocol is None

    def test_init_custom(self):
        drv = UDPDriver("10.0.0.2", 5000, timeout=3.0, local_port=1234)
        assert drv.timeout == 3.0
        assert drv.local_port == 1234

    @pytest.mark.asyncio
    async def test_connect_success(self):
        drv = UDPDriver("10.0.0.2", 5000, local_port=9999)
        transport = MagicMock()
        protocol = MagicMock()
        loop = MagicMock()
        loop.create_datagram_endpoint = AsyncMock(return_value=(transport, protocol))
        with patch("kamio.drivers.udp.asyncio.get_running_loop", return_value=loop):
            await drv.connect()
        assert drv._transport is transport
        assert drv._protocol is protocol
        # Verify local_addr used
        args, kwargs = loop.create_datagram_endpoint.call_args
        local_addr = kwargs.get("local_addr") or (args[1] if len(args) > 1 else None)
        assert local_addr == ("0.0.0.0", 9999)

    @pytest.mark.asyncio
    async def test_disconnect(self):
        drv = UDPDriver("10.0.0.2", 5000)
        transport = MagicMock()
        drv._transport = transport
        drv._protocol = MagicMock()
        await drv.disconnect()
        transport.close.assert_called_once()
        assert drv._transport is None
        assert drv._protocol is None

    @pytest.mark.asyncio
    async def test_disconnect_when_not_connected(self):
        drv = UDPDriver("10.0.0.2", 5000)
        await drv.disconnect()
        assert drv._transport is None

    @pytest.mark.asyncio
    async def test_send_not_connected_raises(self):
        drv = UDPDriver("10.0.0.2", 5000)
        with pytest.raises(RuntimeError, match="not connected"):
            await drv._send(b"data")

    @pytest.mark.asyncio
    async def test_recv_not_connected_raises(self):
        drv = UDPDriver("10.0.0.2", 5000)
        with pytest.raises(RuntimeError, match="not connected"):
            await drv._recv(1024)

    @pytest.mark.asyncio
    async def test_send_and_receive(self):
        """Exercise the internal _send and _recv helpers together."""
        drv = UDPDriver("10.0.0.2", 5000)
        transport = MagicMock()
        protocol = MagicMock()
        protocol.recv = AsyncMock(return_value=b"response")
        drv._transport = transport
        drv._protocol = protocol
        await drv._send(b"hello")
        transport.sendto.assert_called_with(b"hello", ("10.0.0.2", 5000))
        result = await drv._recv(1024)
        assert result == b"response"

    @pytest.mark.asyncio
    async def test_recv_timeout(self):
        drv = UDPDriver("10.0.0.2", 5000, timeout=0.01)
        protocol = MagicMock()
        protocol.recv = AsyncMock(side_effect=asyncio.TimeoutError)
        drv._protocol = protocol
        result = await drv._recv(1024)
        assert result == b""

    @pytest.mark.asyncio
    async def test_execute_plain_send(self):
        drv = UDPDriver("10.0.0.2", 5000)
        transport = MagicMock()
        drv._transport = transport
        drv._protocol = MagicMock()
        result = await drv.execute("PWR ON", {})
        transport.sendto.assert_called_once_with(b"PWR ON", ("10.0.0.2", 5000))
        assert result == {"status": "ok", "sent": 6}

    @pytest.mark.asyncio
    async def test_execute_with_response(self):
        drv = UDPDriver("10.0.0.2", 5000)
        transport = MagicMock()
        protocol = MagicMock()
        protocol.recv = AsyncMock(return_value=b"OK")
        drv._transport = transport
        drv._protocol = protocol
        result = await drv.execute("cmd", {"wait_response": True})
        assert result == b"OK"

    @pytest.mark.asyncio
    async def test_execute_with_response_custom_bytes(self):
        drv = UDPDriver("10.0.0.2", 5000)
        transport = MagicMock()
        protocol = MagicMock()
        protocol.recv = AsyncMock(return_value=b"DATA")
        drv._transport = transport
        drv._protocol = protocol
        result = await drv.execute("cmd", {"wait_response": True, "read_bytes": 512})
        protocol.recv.assert_awaited_once_with(512)
        assert result == b"DATA"

    @pytest.mark.asyncio
    async def test_execute_not_connected_raises(self):
        drv = UDPDriver("10.0.0.2", 5000)
        with pytest.raises(RuntimeError, match="not connected"):
            await drv.execute("cmd", {})

    @pytest.mark.asyncio
    async def test_read_basic(self):
        drv = UDPDriver("10.0.0.2", 5000)
        transport = MagicMock()
        protocol = MagicMock()
        protocol.recv = AsyncMock(return_value=b"42")
        drv._transport = transport
        drv._protocol = protocol
        result = await drv.read("temperature", {"command": "GET TEMP"})
        transport.sendto.assert_called_once_with(b"GET TEMP", ("10.0.0.2", 5000))
        assert result == {"status": "ok", "field": "temperature", "data": b"42"}

    @pytest.mark.asyncio
    async def test_read_no_payload(self):
        drv = UDPDriver("10.0.0.2", 5000)
        transport = MagicMock()
        protocol = MagicMock()
        protocol.recv = AsyncMock(return_value=b"data")
        drv._transport = transport
        drv._protocol = protocol
        # Empty command_name, command, and payload -> _build_payload returns
        # b"" which is falsy, so _send is skipped.
        result = await drv.read("", {"command": "", "payload": ""})
        transport.sendto.assert_not_called()
        assert result == {"status": "ok", "field": "", "data": b"data"}

    @pytest.mark.asyncio
    async def test_read_not_connected_raises(self):
        drv = UDPDriver("10.0.0.2", 5000)
        with pytest.raises(RuntimeError, match="not connected"):
            await drv.read("field", {})

    # _build_payload variations
    def test_build_payload_from_command_name(self):
        drv = UDPDriver("h", 1)
        assert drv._build_payload("CMD", {}) == b"CMD"

    def test_build_payload_with_value(self):
        drv = UDPDriver("h", 1)
        assert drv._build_payload("CMD", {"value": True}) == b"CMD True"

    def test_build_payload_from_command_param(self):
        drv = UDPDriver("h", 1)
        assert drv._build_payload("ignored", {"command": "PWR ON"}) == b"PWR ON"

    def test_build_payload_from_payload_param_bytes(self):
        drv = UDPDriver("h", 1)
        assert drv._build_payload("ignored", {"payload": b"\x01\x02"}) == b"\x01\x02"

    def test_build_payload_from_payload_param_str(self):
        drv = UDPDriver("h", 1)
        assert drv._build_payload("ignored", {"payload": "hello"}) == b"hello"

    def test_build_payload_bytes_with_value(self):
        drv = UDPDriver("h", 1)
        result = drv._build_payload("ignored", {"payload": b"CMD", "value": 5})
        assert result == b"CMD 5"

    def test_build_payload_str_with_value(self):
        drv = UDPDriver("h", 1)
        result = drv._build_payload("ignored", {"command": "CMD", "value": 7})
        assert result == b"CMD 7"


# ---------------------------------------------------------------------------
# _UDPProtocol
# ---------------------------------------------------------------------------
class TestUDPProtocol:
    @pytest.mark.asyncio
    async def test_datagram_received_and_recv(self):
        proto = _UDPProtocol()
        proto.datagram_received(b"hello", ("1.2.3.4", 5))
        data = await proto.recv(1024)
        assert data == b"hello"

    @pytest.mark.asyncio
    async def test_error_received(self):
        proto = _UDPProtocol()
        proto.error_received(ConnectionError("boom"))
        # error_received now propagates the exception through the queue so
        # callers see the real error instead of an empty bytes.
        with pytest.raises(ConnectionError, match="boom"):
            await proto.recv(1024)


# ---------------------------------------------------------------------------
# MockHardwareDriver
# ---------------------------------------------------------------------------
class TestMockDriver:
    def test_init_defaults(self):
        drv = MockHardwareDriver()
        assert drv.latency_range == (0.01, 0.1)
        assert drv.failure_rate == 0.0
        assert drv.state == {}
        assert drv.connected is False

    def test_init_custom(self):
        drv = MockHardwareDriver(latency_range=(0.0, 0.0), failure_rate=0.5, initial_state={"x": 1})
        assert drv.latency_range == (0.0, 0.0)
        assert drv.failure_rate == 0.5
        assert drv.state == {"x": 1}

    @pytest.mark.asyncio
    async def test_connect(self):
        drv = MockHardwareDriver(latency_range=(0.0, 0.0))
        await drv.connect()
        assert drv.connected is True

    @pytest.mark.asyncio
    async def test_connect_failure(self):
        drv = MockHardwareDriver(latency_range=(0.0, 0.0), failure_rate=1.0)
        with pytest.raises(ConnectionError, match="connection failed"):
            await drv.connect()
        assert drv.connected is False

    @pytest.mark.asyncio
    async def test_disconnect(self):
        drv = MockHardwareDriver(latency_range=(0.0, 0.0))
        drv.connected = True
        await drv.disconnect()
        assert drv.connected is False

    @pytest.mark.asyncio
    async def test_execute_not_connected(self):
        drv = MockHardwareDriver(latency_range=(0.0, 0.0))
        with pytest.raises(RuntimeError, match="not connected"):
            await drv.execute("cmd", {})

    @pytest.mark.asyncio
    async def test_execute_set_command(self):
        drv = MockHardwareDriver(latency_range=(0.0, 0.0))
        await drv.connect()
        result = await drv.execute("set_power", {"value": True})
        assert result == {"status": "ok", "field": "power", "value": True}
        assert drv.state["power"] is True

    @pytest.mark.asyncio
    async def test_execute_generic_command(self):
        drv = MockHardwareDriver(latency_range=(0.0, 0.0))
        await drv.connect()
        result = await drv.execute("do_something", {})
        assert result == {"status": "ok", "result": "mock_success"}

    @pytest.mark.asyncio
    async def test_execute_failure(self):
        drv = MockHardwareDriver(latency_range=(0.0, 0.0), failure_rate=0.5)
        # Connect: random=0.9 -> 0.9 < 0.5 is False -> succeeds.
        # Execute: random=0.1 -> 0.1 < 0.5 is True -> raises.
        with patch("kamio.drivers.mock.random.random", side_effect=[0.9, 0.1]):
            await drv.connect()
            with pytest.raises(RuntimeError, match="execution of boom failed"):
                await drv.execute("boom", {})

    @pytest.mark.asyncio
    async def test_read_not_connected(self):
        drv = MockHardwareDriver(latency_range=(0.0, 0.0))
        with pytest.raises(RuntimeError, match="not connected"):
            await drv.read("field")

    @pytest.mark.asyncio
    async def test_read_returns_state(self):
        drv = MockHardwareDriver(latency_range=(0.0, 0.0), initial_state={"temp": 42.5})
        await drv.connect()
        result = await drv.read("temp")
        assert result == 42.5

    @pytest.mark.asyncio
    async def test_read_missing_field(self):
        drv = MockHardwareDriver(latency_range=(0.0, 0.0))
        await drv.connect()
        result = await drv.read("nope")
        assert result is None

    @pytest.mark.asyncio
    async def test_read_failure(self):
        drv = MockHardwareDriver(latency_range=(0.0, 0.0), failure_rate=0.5)
        # Connect: random=0.9 -> succeeds. Read: random=0.1 -> raises.
        with patch("kamio.drivers.mock.random.random", side_effect=[0.9, 0.1]):
            await drv.connect()
            with pytest.raises(RuntimeError, match="read of field failed"):
                await drv.read("field")

    @pytest.mark.asyncio
    async def test_latency_simulated(self):
        # Use a noticeable delay and verify it actually happens.
        drv = MockHardwareDriver(latency_range=(0.05, 0.05))
        loop = asyncio.get_running_loop()
        start = loop.time()
        await drv.connect()
        elapsed = loop.time() - start
        assert elapsed >= 0.04  # allow small scheduling slack

    @pytest.mark.asyncio
    async def test_simulate_latency_directly(self):
        drv = MockHardwareDriver(latency_range=(0.02, 0.03))
        loop = asyncio.get_running_loop()
        start = loop.time()
        await drv._simulate_latency()
        elapsed = loop.time() - start
        assert elapsed >= 0.015

    @pytest.mark.asyncio
    async def test_context_manager(self):
        drv = MockHardwareDriver(latency_range=(0.0, 0.0))
        async with drv as d:
            assert d.connected is True
        assert drv.connected is False
