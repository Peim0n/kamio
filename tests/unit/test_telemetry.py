from __future__ import annotations

import pytest

from kamio import Device, KamioApp, telemetry
from kamio.data_fields import parse_freq


class Weather(Device):
    temperature: float = telemetry(default=20.0, unit="°C", freq="0.01s")
    humidity: float = telemetry(default=50.0, unit="%", freq="0.01s")


@pytest.mark.parametrize(
    "value,expected",
    [
        ("5s", 5.0),
        ("1m", 60.0),
        ("100ms", 0.1),
        ("2h", 7200.0),
    ],
)
def test_parse_freq(value, expected):
    assert parse_freq(value) == expected


@pytest.mark.asyncio
async def test_telemetry_methods_are_callable():
    app = KamioApp()
    device = await app.add_device("w", Weather)
    assert hasattr(device, "start_telemetry")
    await device.start_telemetry()


@pytest.mark.asyncio
async def test_telemetry_fields_have_freq_metadata():
    fields = Weather.get_telemetry()
    assert "temperature" in fields
    assert "humidity" in fields
    assert fields["temperature"].freq == "0.01s"
    assert fields["temperature"].unit == "°C"


@pytest.mark.asyncio
async def test_request_full_sync_exists():
    app = KamioApp()
    device = await app.add_device("w", Weather)
    # Should not raise and should sync all fields.
    await device.request_full_sync()
    assert device.temperature == 20.0
