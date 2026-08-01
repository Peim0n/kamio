import asyncio

import gmqtt
import pytest


class MockGmqttClient(gmqtt.Client):
    """gmqtt.Client subclass for testing KamioApp without a broker."""

    def __init__(self, client_id="mock"):
        super().__init__(client_id, clean_session=True)
        self._connected_flag = False
        self.published = []
        self.subscribed = []
        self.unsubscribed = []

    @property
    def is_connected(self):
        return self._connected_flag

    async def connect(
        self, host=None, port=1883, ssl=False, keepalive=60, version=5, raise_exc=True
    ):
        self._connected_flag = True
        if self.on_connect:
            self.on_connect(self, 0, 0, {})

    async def disconnect(self, reason_code=None, **kwargs):
        self._connected_flag = False
        if self.on_disconnect:
            self.on_disconnect(self, None)

    def publish(self, topic, payload=None, qos=0, retain=False, **kwargs):
        self.published.append((topic, payload, qos, retain))

    def subscribe(self, topic, qos=0, **kwargs):
        self.subscribed.append((topic, qos))
        return 1

    def unsubscribe(self, topic, **kwargs):
        self.unsubscribed.append((topic,))
        return 1

    async def _kamio_wait_for_suback(self, mid, timeout=10.0):
        return

    async def _kamio_wait_for_unsuback(self, mid, timeout=10.0):
        return

    def simulate_connect(self):
        self._connected_flag = True

    def simulate_message(self, topic, payload):
        if self.on_message:
            self.on_message(self, topic, payload, 0, {})


@pytest.fixture
def mock_mqtt():
    return MockGmqttClient()
