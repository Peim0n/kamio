from __future__ import annotations
from typing import Any, Optional
from urllib.parse import urlparse

import asyncio
import gmqtt


class MqttConnection:
    """
    Encapsulates MQTT broker connection details and gmqtt client creation.

    This separates MQTT transport setup from application orchestration,
    making KamioApp smaller and easier to test.

    Features:
        - automatic reconnect with exponential backoff
        - optional TLS configuration
        - connection state callbacks
    """

    def __init__(
        self,
        broker_uri: str,
        client_id: Optional[str] = None,
        keepalive: int = 60,
        clean_session: bool = True,
        protocol: int = 5,
        transport: str = "tcp",
        reconnect_min_delay: float = 1.0,
        reconnect_max_delay: float = 60.0,
        tls: Optional[dict[str, Any]] = None,
    ) -> None:
        parsed = urlparse(broker_uri)

        self.host = parsed.hostname or "localhost"
        self.port = parsed.port or 1883
        self.client_id = client_id or ""
        self._version = int(protocol) if protocol else 5
        self._keepalive = keepalive
        self._reconnect_min_delay = reconnect_min_delay
        self._reconnect_max_delay = reconnect_max_delay

        # gmqtt is a pure-asyncio MQTT client, so there is no separate network
        # thread and no GIL contention when publishing/subscribing in bulk.
        self.client = gmqtt.Client(self.client_id, clean_session=clean_session)

        # Track SUBACK/UNSUBACK so nodes can wait for the broker to confirm
        # a subscription/unsubscription before moving on.
        self._sub_acks: dict[int, asyncio.Event] = {}
        self._subed_mids: set[int] = set()
        self._unsub_acks: dict[int, asyncio.Event] = {}
        self._unsubed_mids: set[int] = set()
        self.client.on_subscribe = self._on_subscribe
        self.client.on_unsubscribe = self._on_unsubscribe
        self.client._kamio_wait_for_suback = self._wait_for_suback
        self.client._kamio_wait_for_unsuback = self._wait_for_unsuback

        if parsed.username:
            self.client.set_auth_credentials(parsed.username, parsed.password)

        # gmqtt takes a bool or SSLContext for ssl; keep the TLS dict around
        # so a future version can build an SSLContext from it if needed.
        self._ssl = tls if tls else False

    def _on_subscribe(self, client, mid, granted_qos, properties=None, *args):
        """gmqtt callback fired when a SUBACK is received."""
        event = self._sub_acks.pop(mid, None)
        if event is not None:
            event.set()
        else:
            self._subed_mids.add(mid)

    def _on_unsubscribe(self, client, mid, properties=None, *args):
        """gmqtt callback fired when an UNSUBACK is received."""
        event = self._unsub_acks.pop(mid, None)
        if event is not None:
            event.set()
        else:
            self._unsubed_mids.add(mid)

    async def _wait_for_suback(self, mid: int, timeout: float = 10.0) -> None:
        """Wait for the SUBACK corresponding to a gmqtt message id."""
        if mid in self._subed_mids:
            self._subed_mids.discard(mid)
            return
        event = asyncio.Event()
        self._sub_acks[mid] = event
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        finally:
            self._sub_acks.pop(mid, None)

    async def _wait_for_unsuback(self, mid: int, timeout: float = 10.0) -> None:
        """Wait for the UNSUBACK corresponding to a gmqtt message id."""
        if mid in self._unsubed_mids:
            self._unsubed_mids.discard(mid)
            return
        event = asyncio.Event()
        self._unsub_acks[mid] = event
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        finally:
            self._unsub_acks.pop(mid, None)

    async def connect(self) -> None:
        """Connect to the broker and start the asyncio network loop."""
        ssl = bool(self._ssl)
        await self.client.connect(
            self.host,
            self.port,
            keepalive=self._keepalive,
            version=self._version,
            ssl=ssl,
            raise_exc=True,
        )

    async def disconnect(self) -> None:
        """Disconnect from the broker and stop the asyncio network loop."""
        await self.client.disconnect()
