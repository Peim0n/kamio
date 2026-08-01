from __future__ import annotations

import asyncio
import ssl
from typing import Any, Optional
from urllib.parse import urlparse

import gmqtt

#: Upper bound for the "early SUBACK/UNSUBACK" caches to prevent unbounded
#: growth if many subscriptions are confirmed before their waiter is set up.
_ACK_CACHE_LIMIT = 1024


class MqttConnection:
    """
    Encapsulates MQTT broker connection details and gmqtt client creation.

    This separates MQTT transport setup from application orchestration,
    making KamioApp smaller and easier to test.

    Features:
        - automatic reconnect with exponential backoff (via gmqtt built-in)
        - optional TLS configuration (cafile, certfile, keyfile, etc.)
        - connection state callbacks
        - SUBACK/UNSUBACK correlation so nodes can await broker confirmation
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
        """Initialize the MQTT connection wrapper.

        Args:
            broker_uri: MQTT broker URL (e.g. ``mqtt://host:port``).
            client_id: Optional MQTT client id; empty string if None.
            keepalive: Keepalive interval in seconds.
            clean_session: Whether to request a clean session.
            protocol: MQTT protocol version (default 5).
            transport: Transport type (``"tcp"`` or ``"websockets"``).
            reconnect_min_delay: Minimum reconnect delay in seconds.
            reconnect_max_delay: Maximum reconnect delay in seconds.
            tls: Optional dict of TLS configuration keys.
        """
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

        # Configure gmqtt's built-in reconnect with exponential backoff.
        self.client._reconnect_delay = reconnect_min_delay
        self.client._reconnect_retries = 0  # 0 = unlimited retries

        # Track SUBACK/UNSUBACK so nodes can wait for the broker to confirm
        # a subscription/unsubscription before moving on.
        self._sub_acks: dict[int, asyncio.Event] = {}
        # Ordered dicts used as ordered sets for early-ACK caching.  Using a
        # dict (instead of a plain set) lets us evict only the oldest entries
        # when the cache grows too large, rather than discarding ALL pending
        # ACKs via clear() which could drop legitimate waiters.
        self._subed_mids: dict[int, None] = {}
        self._unsub_acks: dict[int, asyncio.Event] = {}
        self._unsubed_mids: dict[int, None] = {}
        self.client.on_subscribe = self._on_subscribe
        self.client.on_unsubscribe = self._on_unsubscribe
        # Expose the wait helpers on the client so BaseNode can await them via
        # ``self.mqtt._kamio_wait_for_suback(mid)``.  This is an explicit
        # adapter contract rather than opaque monkey-patching: the attributes
        # are set unconditionally and documented.
        self.client._kamio_wait_for_suback = self._wait_for_suback  # type: ignore[attr-defined]
        self.client._kamio_wait_for_unsuback = self._wait_for_unsuback  # type: ignore[attr-defined]

        if parsed.username:
            self.client.set_auth_credentials(parsed.username, parsed.password)

        # Build an SSLContext from the TLS dict so CA certs, client certs,
        # and keys are actually applied — not just a bool flag.
        self._ssl = self._build_ssl_context(tls) if tls else False

    # ------------------------------------------------------------------
    # TLS
    # ------------------------------------------------------------------
    @staticmethod
    def _build_ssl_context(tls: dict[str, Any]) -> ssl.SSLContext:
        """Build an SSLContext from a TLS configuration dict.

        Recognised keys (all optional):
            cafile:       Path to CA certificate bundle file.
            capath:       Path to a directory of CA certificates.
            certfile:     Path to the client certificate file.
            keyfile:      Path to the client private key file.
            cert_reqs:    Whether to require a server certificate.
                          Accepts ssl.CERT_* constants or string names
                          ("REQUIRED", "OPTIONAL", "NONE").
            verify_mode:  Alias for cert_reqs.
            check_hostname: Whether to verify the server hostname (bool).
            tls_version:  ssl.PROTOCOL_* constant or string name.
        """
        ctx = ssl.create_default_context()

        cafile = tls.get("cafile")
        capath = tls.get("capath")
        if cafile or capath:
            ctx.load_verify_locations(cafile=cafile, capath=capath)

        certfile = tls.get("certfile")
        keyfile = tls.get("keyfile")
        if certfile:
            ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)

        cert_reqs = tls.get("cert_reqs", tls.get("verify_mode"))
        if cert_reqs is not None:
            if isinstance(cert_reqs, str):
                cert_reqs = getattr(ssl, f"CERT_{cert_reqs.upper()}", ssl.CERT_REQUIRED)
            # check_hostname cannot be True when verify_mode is CERT_NONE.
            # Disable it first to avoid ValueError from the ssl module.
            if cert_reqs == ssl.CERT_NONE:
                ctx.check_hostname = False
            ctx.verify_mode = cert_reqs  # type: ignore[assignment]

        check_hostname = tls.get("check_hostname")
        if check_hostname is not None:
            ctx.check_hostname = bool(check_hostname)

        tls_version = tls.get("tls_version")
        if tls_version is not None:
            if isinstance(tls_version, str):
                tls_version = getattr(ssl, f"PROTOCOL_{tls_version.upper()}")
            # ssl.SSLContext() does NOT inherit the secure defaults of
            # create_default_context() (verify_mode defaults to CERT_NONE,
            # check_hostname to False).  Re-apply every previously-set option
            # so that specifying tls_version does not silently disable TLS
            # verification — a security-critical regression.
            ctx = ssl.SSLContext(tls_version)
            if cafile or capath:
                ctx.load_verify_locations(cafile=cafile, capath=capath)
            if certfile:
                ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
            # Determine the effective verify_mode and check_hostname.
            # check_hostname cannot be True when verify_mode is CERT_NONE,
            # so set check_hostname=False first when going to CERT_NONE.
            effective_reqs = cert_reqs if cert_reqs is not None else ssl.CERT_REQUIRED
            effective_check = (
                bool(check_hostname)
                if check_hostname is not None
                else effective_reqs != ssl.CERT_NONE
            )
            if not effective_check:
                # Must disable check_hostname before setting CERT_NONE, and
                # also before setting CERT_REQUIRED when check_hostname is
                # explicitly False.
                ctx.check_hostname = False
            ctx.verify_mode = effective_reqs  # type: ignore[assignment]
            if effective_check:
                ctx.check_hostname = True

        return ctx

    # ------------------------------------------------------------------
    # gmqtt callbacks
    # ------------------------------------------------------------------
    def _on_subscribe(self, client, mid, granted_qos, properties=None, *args):
        """gmqtt callback fired when a SUBACK is received."""
        self._resolve_ack(mid, self._sub_acks, self._subed_mids)

    def _on_unsubscribe(self, client, mid, properties=None, *args):
        """gmqtt callback fired when an UNSUBACK is received."""
        self._resolve_ack(mid, self._unsub_acks, self._unsubed_mids)

    @staticmethod
    def _resolve_ack(mid: int, acks: dict, early: dict) -> None:
        """Resolve a pending ACK waiter or record the mid as "early"."""
        event = acks.pop(mid, None)
        if event is not None:
            event.set()
        else:
            # SUBACK arrived before a waiter was registered; remember it.
            early[mid] = None
            # Bound the cache so it cannot grow without limit if waiters never
            # show up (e.g. dropped subscriptions).  Evict only the oldest
            # entries rather than clearing everything, so legitimate waiters
            # that are about to check the cache are not dropped.
            while len(early) > _ACK_CACHE_LIMIT:
                # dict.popitem(last=False) removes the oldest inserted key.
                early.pop(next(iter(early)))

    # ------------------------------------------------------------------
    # ACK waiters
    # ------------------------------------------------------------------
    async def _wait_for_suback(self, mid: int, timeout: float = 10.0) -> None:
        """Wait for the SUBACK corresponding to a gmqtt message id."""
        await self._wait_for_ack(mid, timeout, self._sub_acks, self._subed_mids, "SUBACK")

    async def _wait_for_unsuback(self, mid: int, timeout: float = 10.0) -> None:
        """Wait for the UNSUBACK corresponding to a gmqtt message id."""
        await self._wait_for_ack(mid, timeout, self._unsub_acks, self._unsubed_mids, "UNSUBACK")

    @staticmethod
    async def _wait_for_ack(
        mid: int,
        timeout: float,
        acks: dict[int, asyncio.Event],
        early: dict[int, None],
        label: str,
    ) -> None:
        """Shared implementation for SUBACK/UNSUBACK waiting."""
        if mid in early:
            early.pop(mid, None)
            return
        event = asyncio.Event()
        acks[mid] = event
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(f"Timed out waiting for {label} (mid={mid})")
        finally:
            acks.pop(mid, None)

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------
    async def connect(self) -> None:
        """Connect to the broker and start the asyncio network loop."""
        ssl_ctx = self._ssl
        await self.client.connect(
            self.host,
            self.port,
            keepalive=self._keepalive,
            version=self._version,
            ssl=ssl_ctx,
            raise_exc=True,
        )

    async def disconnect(self) -> None:
        """Disconnect from the broker and stop the asyncio network loop."""
        await self.client.disconnect()
