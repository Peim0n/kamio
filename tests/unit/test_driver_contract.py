"""BaseDriver dispatch contract: foreign commands raise NotImplementedError."""

import asyncio

import pytest

from kamio import Device, command, state
from kamio.drivers import (
    BaseDriver,
    GPIOChipDriver,
    HTTPDeviceDriver,
    MockHardwareDriver,
    ModbusTCPDriver,
    SerialDriver,
    TelnetDriver,
    UDPDriver,
)


class _Dev(Device):
    power: bool = state(default=False, writable=True)

    @command
    async def reset(self):
        return "device-reset"


class _Custom(BaseDriver):
    def __init__(self, **kw):
        super().__init__(**kw)
        self.calls = []

    async def connect(self):
        pass

    async def disconnect(self):
        pass

    async def execute(self, command_name, params):
        self.calls.append(command_name)
        return "driver"

    async def read(self, field_name, params=None):
        return None


class _Sub(_Custom):
    def supports_command(self, command_name):
        return command_name == "only" and super().supports_command(command_name)


def test_execute_is_wrapped_once_and_keeps_metadata():
    assert getattr(_Custom.execute, "_kamio_contract", False)
    assert _Custom.execute.__name__ == "execute"
    assert getattr(BaseDriver.execute, "__isabstractmethod__", False)
    # Subclass without its own execute inherits the wrapper unchanged.
    assert _Sub.execute is _Custom.execute


@pytest.mark.asyncio
async def test_no_allowlist_accepts_everything():
    drv = _Custom()
    assert await drv.execute("anything", {}) == "driver"


@pytest.mark.asyncio
async def test_allowlist_patterns_and_string():
    drv = _Custom(commands=["set_*", "ping"])
    assert await drv.execute("set_power", {}) == "driver"
    assert await drv.execute("ping", {}) == "driver"
    with pytest.raises(NotImplementedError, match="'reset'"):
        await drv.execute("reset", {})
    assert drv.calls == ["set_power", "ping"]
    assert _Custom(commands="ping").commands == ("ping",)


@pytest.mark.asyncio
async def test_subclass_supports_command_combines_with_allowlist():
    assert await _Sub().execute("only", {}) == "driver"
    with pytest.raises(NotImplementedError):
        await _Sub().execute("other", {})
    with pytest.raises(NotImplementedError):
        await _Sub(commands=[]).execute("only", {})


@pytest.mark.asyncio
async def test_device_falls_back_when_driver_rejects():
    drv = _Custom(commands=["set_*"])
    dev = _Dev(driver=drv)
    assert await dev.handle_command("reset", {}) == "device-reset"
    assert await dev.handle_state({"power": True}) == {"power": True}
    assert drv.calls == ["set_power"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drv",
    [
        ModbusTCPDriver("127.0.0.1", 1),
        GPIOChipDriver(),
    ],
)
async def test_fixed_command_drivers_reject_foreign_commands_before_io(drv):
    # Not connected and no pin/address: the contract check must come first.
    dev = _Dev(driver=drv)
    assert await dev.handle_command("reset", {}) == "device-reset"
    assert await dev.handle_state({"power": True}) == {"power": True}
    assert dev.power is True


def test_modbus_supports_command():
    drv = ModbusTCPDriver("127.0.0.1", 1)
    for name in ("write_coil", "set_coil", "registers", "set_holding"):
        assert drv.supports_command(name)
    for name in ("set_pump", "reset_alarm", "bogus"):
        assert not drv.supports_command(name)
    assert not ModbusTCPDriver("h", commands=["write_coil"]).supports_command("write_register")


def test_gpio_supports_command():
    assert GPIOChipDriver().supports_command("set_output")
    assert not GPIOChipDriver().supports_command("set_power")
    assert not GPIOChipDriver(commands=[]).supports_command("set_output")


@pytest.mark.asyncio
async def test_gpio_own_command_still_validates():
    drv = GPIOChipDriver()
    with pytest.raises(RuntimeError, match="not connected"):
        await drv.execute("set_output", {"pin": 1})


_STOCK = [
    (lambda **kw: TelnetDriver("127.0.0.1", **kw), ("send", "request")),
    (lambda **kw: SerialDriver("/dev/null", **kw), ("send", "request")),
    (lambda **kw: UDPDriver("127.0.0.1", 1, **kw), ("send", "request")),
    (lambda **kw: HTTPDeviceDriver("http://localhost", **kw), ("request", "get", "post")),
    (lambda **kw: MockHardwareDriver(**kw), ("set_power",)),
    (lambda **kw: ModbusTCPDriver("127.0.0.1", 1, **kw), ("write_coil", "set_coil")),
    (lambda **kw: GPIOChipDriver(**kw), ("set_output",)),
]


@pytest.mark.parametrize("factory, owned", _STOCK)
def test_every_stock_driver_owns_only_its_transport_commands(factory, owned):
    drv = factory()
    for name in owned:
        assert drv.supports_command(name)
    for name in ("reset", "emergency_stop", "PWR ON"):
        assert not drv.supports_command(name)
    assert not factory(commands=[]).supports_command(owned[0])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drv",
    [
        TelnetDriver("127.0.0.1", 1),
        SerialDriver("/dev/null"),
        UDPDriver("127.0.0.1", 1),
        HTTPDeviceDriver("http://localhost"),
        MockHardwareDriver(latency_range=(0, 0)),
    ],
)
async def test_device_command_never_reaches_the_wire(drv):
    # Not connected: any attempt to talk to the transport would raise.
    dev = _Dev(driver=drv)
    assert await dev.handle_command("reset", {}) == "device-reset"
    if not isinstance(drv, MockHardwareDriver):
        assert await dev.handle_state({"power": True}) == {"power": True}
        assert dev.power is True


class _TelnetDev(Device):
    power: bool = state(default=False, writable=True)

    @command
    async def set_power(self, value):
        reply = await self.driver.execute("request", {"payload": f"PWR {'ON' if value else 'OFF'}"})
        self.power = bool(value)
        return reply["response"]


@pytest.mark.asyncio
async def test_device_builds_payload_transport_frames_it():
    lines = []

    async def handler(reader, writer):
        while line := await reader.readline():
            lines.append(line)
            writer.write(b"OK\r\n")
            await writer.drain()
        writer.close()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    drv = TelnetDriver("127.0.0.1", port)
    await drv.connect()
    try:
        dev = _TelnetDev(driver=drv)
        assert await dev.handle_command("set_power", {"value": True}) == "OK"
        assert dev.power is True
        assert lines == [b"PWR ON\n"]
    finally:
        await drv.disconnect()
        server.close()
        await server.wait_closed()
