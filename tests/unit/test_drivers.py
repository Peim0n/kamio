"""Unit tests for driver hardening: bounded serial reads, HTTP error propagation,
GPIO thread offloading, and modbus auto-reconnect.

All driver I/O is mocked so the tests run without hardware or a live network.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kamio.drivers.http import HTTPDeviceDriver
from kamio.drivers.serial import SerialDriver, _readline_bounded


# ---------------------------------------------------------------------------
# Serial: bounded readline
# ---------------------------------------------------------------------------
def test_readline_bounded_stops_at_newline():
    ser = MagicMock()
    # First chunk has the newline mid-stream.
    ser.read.side_effect = [b"hello\nworld"]
    assert _readline_bounded(ser, 64) == b"hello\n"


def test_readline_bounded_stops_at_limit():
    ser = MagicMock()
    # Stream bytes without a newline; should stop at the limit.
    ser.read.side_effect = [b"x" * 64] * 10
    out = _readline_bounded(ser, 100)
    assert len(out) <= 100


def test_readline_bounded_stops_on_eof():
    ser = MagicMock()
    ser.read.side_effect = [b"partial", b""]
    assert _readline_bounded(ser, 64) == b"partial"


@pytest.mark.asyncio
async def test_serial_read_returns_dict():
    driver = SerialDriver(port="/dev/null", read_limit=64)
    fake_ser = MagicMock()
    fake_ser.read.return_value = b"ok\n"
    driver.ser = fake_ser

    result = await driver.read("temp", {"command": "GET TEMP", "wait_response": True})
    assert result["status"] == "ok"
    assert result["field"] == "temp"
    assert result["response"] == "ok"
    fake_ser.write.assert_called_once_with(b"GET TEMP")


@pytest.mark.asyncio
async def test_serial_read_without_response():
    driver = SerialDriver(port="/dev/null")
    fake_ser = MagicMock()
    driver.ser = fake_ser

    result = await driver.read("temp", {"wait_response": False})
    assert result["response"] == ""
    fake_ser.read.assert_not_called()


@pytest.mark.asyncio
async def test_serial_read_raises_when_not_connected():
    driver = SerialDriver(port="/dev/null")
    with pytest.raises(RuntimeError):
        await driver.read("temp")


@pytest.mark.asyncio
async def test_serial_disconnect_handles_close_error():
    """disconnect() should not raise if ser.close() fails."""
    driver = SerialDriver(port="/dev/null")
    fake_ser = MagicMock()
    fake_ser.close = MagicMock(side_effect=OSError("port gone"))
    driver.ser = fake_ser
    await driver.disconnect()  # should not raise
    assert driver.ser is None


@pytest.mark.asyncio
async def test_http_disconnect_handles_close_error():
    """disconnect() should not raise if session.close() fails."""
    driver = HTTPDeviceDriver(base_url="http://example.com")
    fake_session = MagicMock()

    async def _failing_close():
        raise RuntimeError("close failed")

    fake_session.close = _failing_close
    driver.session = fake_session
    await driver.disconnect()  # should not raise
    assert driver.session is None


# ---------------------------------------------------------------------------
# HTTP: error propagation
# ---------------------------------------------------------------------------
class _FakeResponse:
    def __init__(self, status=200, content_type="application/json", payload=None, raise_exc=None):
        self.status = status
        self.content_type = content_type
        self._payload = (
            payload if payload is not None else ({} if content_type == "application/json" else "")
        )
        self._raise_exc = raise_exc

    async def __aenter__(self):
        if self._raise_exc:
            raise self._raise_exc
        return self

    async def __aexit__(self, *args):
        return False

    def raise_for_status(self):
        if self.status >= 400:
            raise AssertionError(f"HTTP {self.status}")

    async def json(self):
        return self._payload

    async def text(self):
        return self._payload


class _FakeSession:
    def __init__(self, response):
        self._response = response
        self.requests = []

    def request(self, method, url, **kwargs):
        self.requests.append((method, url, kwargs))
        return self._response

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_http_read_returns_data():
    driver = HTTPDeviceDriver(base_url="http://example.com")
    resp = _FakeResponse(status=200, content_type="application/json", payload={"temp": 21})
    driver.session = _FakeSession(resp)

    result = await driver.read("temp", {"path": "/sensor"})
    assert result["status"] == "ok"
    assert result["data"] == {"temp": 21}


@pytest.mark.asyncio
async def test_http_read_raises_on_http_error():
    driver = HTTPDeviceDriver(base_url="http://example.com")
    resp = _FakeResponse(status=500)
    driver.session = _FakeSession(resp)
    with pytest.raises(AssertionError):
        await driver.read("temp", {"path": "/sensor"})


@pytest.mark.asyncio
async def test_http_read_raises_when_not_connected():
    driver = HTTPDeviceDriver(base_url="http://example.com")
    with pytest.raises(RuntimeError):
        await driver.read("temp")


@pytest.mark.asyncio
async def test_http_execute_post():
    driver = HTTPDeviceDriver(base_url="http://example.com")
    resp = _FakeResponse(status=200, content_type="application/json", payload={"ok": True})
    session = _FakeSession(resp)
    driver.session = session

    result = await driver.execute(
        "set", {"path": "/actuator", "method": "POST", "value": {"on": True}}
    )
    assert result["status"] == "ok"
    assert session.requests[0][0] == "POST"
    assert session.requests[0][2]["json"] == {"on": True}


# ---------------------------------------------------------------------------
# GPIO: thread offloading
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_gpio_connect_uses_to_thread():
    """connect() must offload gpiod.Chip construction to a worker thread."""
    from kamio.drivers import gpio as gpio_mod

    if gpio_mod.gpiod is None:
        pytest.skip("gpiod not installed")

    fake_chip = MagicMock()
    with patch.object(gpio_mod.gpiod, "Chip", return_value=fake_chip) as chip_ctor:
        driver = gpio_mod.GPIOChipDriver(chip_path="/dev/gpiochip0")
        await driver.connect()
        chip_ctor.assert_called_once_with("/dev/gpiochip0")
        assert driver.chip is fake_chip


@pytest.mark.asyncio
async def test_gpio_connect_raises_import_error_when_missing():
    from kamio.drivers import gpio as gpio_mod

    with patch.object(gpio_mod, "gpiod", None):
        driver = gpio_mod.GPIOChipDriver()
        with pytest.raises(ImportError):
            await driver.connect()


# ---------------------------------------------------------------------------
# Modbus: auto-reconnect
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_modbus_reconnect_on_broken_stream():
    from kamio.drivers import modbus as modbus_mod

    driver = modbus_mod.ModbusTCPDriver(host="localhost", port=502)

    # Simulate a broken writer (is_closing() -> True) and patch _open_connection
    # so we don't actually open a socket.
    fake_writer = MagicMock()
    fake_writer.is_closing.return_value = True
    fake_writer.close = MagicMock()
    fake_writer.wait_closed = AsyncMock()
    driver._writer = fake_writer
    driver._reader = None

    opened = {"count": 0}

    async def fake_open():
        opened["count"] += 1
        driver._reader = MagicMock()
        driver._writer = MagicMock()
        driver._writer.is_closing.return_value = False

    with patch.object(driver, "_open_connection", side_effect=fake_open):
        await driver._ensure_connected()

    assert opened["count"] == 1


@pytest.mark.asyncio
async def test_modbus_no_reconnect_when_healthy():
    from kamio.drivers import modbus as modbus_mod

    driver = modbus_mod.ModbusTCPDriver(host="localhost", port=502)
    fake_writer = MagicMock()
    fake_writer.is_closing.return_value = False
    driver._writer = fake_writer
    driver._reader = MagicMock()

    with patch.object(driver, "_reconnect", new=AsyncMock()) as reconnect:
        await driver._ensure_connected()
        reconnect.assert_not_called()
