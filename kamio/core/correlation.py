from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any, Callable, Coroutine, Dict, Tuple

from kamio.core.envelope import Envelope, EnvelopeType

logger = logging.getLogger("Kamio.base_manager")


class BaseCorrelationManager:
    """
    Base class for managers requiring request-response correlation.

    The pending-response table is guarded by an :class:`threading.RLock` so
    that resolution from a callback thread and registration from the event
    loop do not race on the same ``(source_id, cind)`` key.
    """

    def __init__(self, max_pending: int = 1000):
        """Initialize the correlation manager.

        Args:
            max_pending: Maximum number of pending requests (default 1000).
        """
        self._max_pending = max_pending
        # Pending responses: {(source_id, cind): Future}
        self._pending: Dict[Tuple[str, str], asyncio.Future[Envelope]] = {}
        self._lock = threading.RLock()

    async def _wait_for_ack(
        self,
        target_id: str,
        cind: str,
        publish_coro: Coroutine[Any, Any, None],
        timeout: float = 10.0,
    ) -> Envelope:
        """
        Common logic for waiting for an acknowledgment.
        """
        with self._lock:
            if len(self._pending) >= self._max_pending:
                raise RuntimeError(
                    f"{self.__class__.__name__} overloaded: too many pending requests"
                )

            key = (target_id, cind)
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            self._pending[key] = fut

        try:
            await publish_coro
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            with self._lock:
                # Only remove the entry if it is still *our* future, so a
                # concurrently-registered request with a reused key is not
                # accidentally dropped.
                current = self._pending.get(key)
                if current is fut:
                    self._pending.pop(key, None)

    def _resolve_ack(self, source_id: str, cind: str, env: Envelope) -> bool:
        """
        Resolves a pending Future by (source_id, cind).
        """
        key = (source_id, cind)
        with self._lock:
            fut = self._pending.pop(key, None)
        if fut is not None and not fut.done():
            fut.set_result(env)
            return True
        return False


class CommandManager(BaseCorrelationManager):
    """
    Management of RPC commands from server to devices.
    """

    async def send_command(
        self,
        target: str,
        method: str,
        params: Dict[str, Any],
        publish_func: Callable[[Envelope], Coroutine[Any, Any, None]],
        source_id: str,
        timeout: float = 10.0,
    ) -> Envelope:
        """Send a command envelope and wait for the ACK."""
        env = Envelope.command(source=source_id, target=target, method=method, params=params)

        return await self._wait_for_ack(
            target_id=target, cind=env.cind, publish_coro=publish_func(env), timeout=timeout
        )

    def handle_ack(self, env: Envelope) -> bool:
        """Handle an incoming COMMAND_ACK envelope."""
        if env.type != EnvelopeType.COMMAND_ACK:
            return False
        return self._resolve_ack(env.source, env.cind, env)
