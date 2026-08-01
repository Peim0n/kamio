import asyncio
import logging
from typing import Any, Dict, List, Set, Tuple

import gmqtt
import pytest

logger = logging.getLogger("Kamio.test_broker")


class InMemoryBroker:
    """A minimal in-memory MQTT broker used by the test-suite.

    On Linux ``gmqtt.Client.connect`` opens a TCP socket synchronously inside
    the asyncio event loop and raises ``ConnectionRefusedError`` when no broker
    is listening on ``127.0.0.1:1883``.  On Windows the selector implementation
    defers the connect attempt so the error surfaces later (or not at all),
    which is why the suite passed locally on Windows but failed on the Ubuntu
    CI runners.

    This broker replaces the real network layer with an in-memory pub/sub mesh
    so that tests creating ``KamioApp(mqtt_broker="mqtt://localhost:1883")``
    work identically on every platform without a running Mosquitto instance.

    The broker is a process-wide singleton shared by all ``KamioApp`` instances
    created during a test, mirroring a real broker that all clients connect to.
    """

    _instance: "InMemoryBroker | None" = None

    def __init__(self) -> None:
        self._clients: Dict[str, "_BrokerClient"] = {}
        # topic -> set of client_ids
        self._subscriptions: Dict[str, Set[str]] = {}
        self._next_mid = 1

    @classmethod
    def get(cls) -> "InMemoryBroker":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        cls._instance = None

    def register(self, client: "_BrokerClient") -> None:
        self._clients[client._client_id] = client

    def unregister(self, client_id: str) -> None:
        self._clients.pop(client_id, None)
        for subs in self._subscriptions.values():
            subs.discard(client_id)

    def subscribe(self, client_id: str, topic: str) -> int:
        self._subscriptions.setdefault(topic, set()).add(client_id)
        mid = self._next_mid
        self._next_mid += 1
        # Fire on_subscribe callback asynchronously so the caller can await
        # the SUBACK via _kamio_wait_for_suback.
        client = self._clients.get(client_id)
        if client and client.on_subscribe:
            client.on_subscribe(client, mid, [0], {})
        return mid

    def unsubscribe(self, client_id: str, topic: str) -> int:
        subs = self._subscriptions.get(topic)
        if subs:
            subs.discard(client_id)
        mid = self._next_mid
        self._next_mid += 1
        client = self._clients.get(client_id)
        if client and client.on_unsubscribe:
            client.on_unsubscribe(client, mid, {})
        return mid

    def publish(self, source_id: str, topic: str, payload: Any, qos: int = 0) -> None:
        for sub_topic, subscriber_ids in list(self._subscriptions.items()):
            if self._topic_matches(sub_topic, topic):
                for sub_id in list(subscriber_ids):
                    sub = self._clients.get(sub_id)
                    if sub and sub.on_message:
                        # Schedule the callback asynchronously so it runs on the
                        # event loop, mirroring real gmqtt which dispatches from
                        # a network task.  Calling on_message synchronously here
                        # would re-enter the event loop's call stack and cause
                        # ordering issues with _run_coro_threadsafe.
                        #
                        # Note: we deliberately do NOT skip the publisher
                        # (sub_id == source_id).  In real MQTT, a client
                        # receives its own published messages if it is
                        # subscribed to the topic (unless no_local is set,
                        # which Kamio does not use).  Skipping self-delivery
                        # breaks the telemetry → rule → command cascade that
                        # stress tests rely on.
                        try:
                            loop = asyncio.get_running_loop()
                            loop.call_soon(sub.on_message, sub, topic, payload, qos, {})
                        except RuntimeError:
                            # No running loop — call directly.
                            sub.on_message(sub, topic, payload, qos, {})

    @staticmethod
    def _topic_matches(subscription: str, topic: str) -> bool:
        """MQTT topic filter matching (+ and # wildcards).

        - ``+`` matches exactly one topic level (no slashes).
        - ``#`` matches zero or more topic levels (must be the last level).
        """
        if subscription == topic:
            return True
        sub_parts = subscription.split("/")
        topic_parts = topic.split("/")
        for i, sp in enumerate(sub_parts):
            if sp == "#":
                return True  # # matches everything remaining
            if i >= len(topic_parts):
                return False
            if sp == "+":
                continue  # + matches exactly one level
            if sp != topic_parts[i]:
                return False
        return len(sub_parts) == len(topic_parts)


class _BrokerClient(gmqtt.Client):
    """A gmqtt.Client backed by :class:`InMemoryBroker` instead of a socket.

    Every ``KamioApp`` that is created with a ``mqtt://`` URI gets one of these
    via the patched ``gmqtt.Client.__init__`` / ``connect``.
    """

    def __init__(self, client_id="", **kwargs):
        super().__init__(client_id, **kwargs)
        self._connected_flag = False
        self._broker = InMemoryBroker.get()
        self._broker.register(self)

    @property
    def is_connected(self):  # type: ignore[override]
        return self._connected_flag

    async def connect(
        self, host=None, port=1883, ssl=False, keepalive=60, version=5, raise_exc=True
    ):
        self._connected_flag = True
        if self.on_connect:
            self.on_connect(self, 0, 0, {})

    async def disconnect(self, reason_code=None, **kwargs):
        self._connected_flag = False
        self._broker.unregister(self._client_id)
        if self.on_disconnect:
            self.on_disconnect(self, None)

    def publish(self, topic, payload=None, qos=0, retain=False, **kwargs):
        self._broker.publish(self._client_id, topic, payload, qos)

    def subscribe(self, topic, qos=0, **kwargs):
        return self._broker.subscribe(self._client_id, topic)

    def unsubscribe(self, topic, **kwargs):
        return self._broker.unsubscribe(self._client_id, topic)

    async def _kamio_wait_for_suback(self, mid, timeout=10.0):
        return

    async def _kamio_wait_for_unsuback(self, mid, timeout=10.0):
        return

    # Legacy helpers kept for backward compatibility with tests that use
    # MockGmqttClient directly.
    def simulate_connect(self):
        self._connected_flag = True

    def simulate_message(self, topic, payload):
        if self.on_message:
            self.on_message(self, topic, payload, 0, {})


class MockGmqttClient(_BrokerClient):
    """gmqtt.Client subclass for testing KamioApp without a broker.

    Kept for backward compatibility — tests that pass ``mqtt_broker=mock_mqtt``
    get the same in-memory broker behaviour as tests that use a URI.
    """

    def __init__(self, client_id="mock"):
        super().__init__(client_id, clean_session=True)
        self.published: List[Tuple[str, Any, int, bool]] = []
        self.subscribed: List[Tuple[str, int]] = []
        self.unsubscribed: List[Tuple[str]] = []

    def publish(self, topic, payload=None, qos=0, retain=False, **kwargs):  # type: ignore[override]
        self.published.append((topic, payload, qos, retain))
        super().publish(topic, payload, qos, retain)

    def subscribe(self, topic, qos=0, **kwargs):  # type: ignore[override]
        self.subscribed.append((topic, qos))
        return super().subscribe(topic, qos)

    def unsubscribe(self, topic, **kwargs):  # type: ignore[override]
        self.unsubscribed.append((topic,))
        return super().unsubscribe(topic)


@pytest.fixture(autouse=True)
def _in_memory_broker(monkeypatch):
    """Replace gmqtt.Client with an in-memory broker for every test.

    This autouse fixture patches ``gmqtt.Client`` so that any ``KamioApp``
    created during a test — whether with ``mqtt_broker="mqtt://localhost:1883"``
    or with ``mqtt_broker=mock_mqtt`` — uses the in-memory broker instead of
    opening a real TCP connection.  Without this, tests fail on Linux with
    ``ConnectionRefusedError`` because no Mosquitto broker is running in CI.
    """
    InMemoryBroker.reset()
    monkeypatch.setattr(gmqtt, "Client", _BrokerClient)
    yield
    InMemoryBroker.reset()


@pytest.fixture
def mock_mqtt():
    return MockGmqttClient()
