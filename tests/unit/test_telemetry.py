from __future__ import annotations

import asyncio

import pytest
from conftest import wait_until

from kamio import Device, KamioApp, telemetry
from kamio.core.envelope import Envelope, EnvelopeType
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


@pytest.mark.asyncio
async def test_publish_telemetry_publishes_dt_envelope(mock_mqtt):
    """publish_telemetry delivers a DEVICE_TELEMETRY envelope to Kamio/v1/<id>/dt (SCAN-22)."""
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-telemetry")
    await app.start()
    device = await app.add_device("w", Weather)
    try:
        await device.publish_telemetry({"temperature": 21.5, "humidity": 55.0})
        assert mock_mqtt.published
        topic, payload, _, _ = mock_mqtt.published[-1]
        assert topic == "Kamio/v1/w/dt"
        env = Envelope.from_json(payload)
        assert env is not None
        assert env.type == EnvelopeType.DEVICE_TELEMETRY
        assert env.data == {"temperature": 21.5, "humidity": 55.0}
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_telemetry_scheduler_publishes_periodically(mock_mqtt):
    """The telemetry scheduler publishes Kamio/v1/<id>/dt envelopes (SCAN-22)."""
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-telemetry-sched")
    await app.start()
    device = await app.add_device("w", Weather)  # node start auto-starts telemetry
    try:
        assert await wait_until(
            lambda: any(t == "Kamio/v1/w/dt" for t, *_ in mock_mqtt.published),
            timeout=5.0,
        ), "telemetry scheduler never published"
        _, payload, _, _ = next(p for p in mock_mqtt.published if p[0] == "Kamio/v1/w/dt")
        env = Envelope.from_json(payload)
        assert env is not None
        assert env.type == EnvelopeType.DEVICE_TELEMETRY
        assert {"temperature", "humidity"} <= set(env.data)
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_telemetry_min_freq_caps_fast_fields(mock_mqtt, monkeypatch, caplog):
    """Fields faster than telemetry_min_freq are capped to the minimum (SCAN-22)."""
    import logging

    monkeypatch.setenv("KAMIO_TELEMETRY_MIN_FREQ", "5")
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-min-freq")
    await app.start()
    try:
        with caplog.at_level(logging.WARNING):
            device = await app.add_device("w", Weather)

        # The 0.01s fields are capped to 5s -> one scheduler task named
        # "telemetry_5.0", and the cap is logged.
        telem_tasks = [t for t in device._bg_tasks if t.get_name().startswith("telemetry_")]
        assert [t.get_name() for t in telem_tasks] == ["telemetry_5.0"]
        assert "Capping to 5.0s" in caplog.text

        # Nothing may be published yet — the capped interval has not elapsed.
        for _ in range(10):
            await asyncio.sleep(0)
        assert not any(t == "Kamio/v1/w/dt" for t, *_ in mock_mqtt.published)
    finally:
        await app.stop()


@pytest.mark.asyncio
async def test_telemetry_tasks_cancelled_on_stop(mock_mqtt):
    """Stopping the app cancels the telemetry_* scheduler tasks (SCAN-22)."""
    mock_mqtt.simulate_connect()
    app = KamioApp(mqtt_broker=mock_mqtt, client_id="test-telemetry-stop")
    await app.start()
    device = await app.add_device("w", Weather)

    telem_tasks = [t for t in device._bg_tasks if t.get_name().startswith("telemetry_")]
    assert telem_tasks, "telemetry scheduler tasks were not started"

    await app.stop()

    assert all(t.done() for t in telem_tasks)
    assert not [t for t in device._bg_tasks if t.get_name().startswith("telemetry_")]
