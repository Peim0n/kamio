from __future__ import annotations
import logging
from typing import Any, Dict, Optional, Callable, Coroutine
from .envelope import Envelope, EnvelopeType
from .correlation import BaseCorrelationManager

logger = logging.getLogger("Kamio.state")

class StateManager(BaseCorrelationManager):
    """
    Centralized management of device states.

    Maintains a per-device state dictionary and provides methods for querying,
    updating, and synchronizing state across the application.
    Extends :class:`BaseCorrelationManager` to resolve state-change
    acknowledgments via correlation IDs.

    Args:
        max_pending: Maximum number of pending state-change requests (default 1000).
    """
    def __init__(self, max_pending: int = 1000) -> None:
        super().__init__(max_pending=max_pending)
        self._states: Dict[str, Dict[str, Any]] = {}

    def get_state(self, device_id: str, field: Optional[str] = None) -> Any:
        """
        Return state for a device.

        Args:
            device_id: The device identifier.
            field: Optional field name; returns that field's value.
                   If ``None``, returns a copy of all fields for the device.
        """
        device_data = self._states.get(device_id, {})
        if field:
            return device_data.get(field)
        return device_data.copy()

    def get_all_states(self) -> Dict[str, Dict[str, Any]]:
        """Return a shallow copy of all current device state dictionaries."""
        return {k: v.copy() for k, v in self._states.items()}

    def update_state(self, device_id: str, data: Dict[str, Any]) -> None:
        """
        Merge ``data`` into the stored state for ``device_id``.

        Args:
            device_id: The device identifier.
            data: Dict of field values to merge (must be a ``dict``).
        """
        if not isinstance(data, dict):
            logger.warning(f"update_state: expected dict, got {type(data).__name__}")
            return
        if not data:
            return
        if device_id not in self._states:
            self._states[device_id] = {}
        self._states[device_id].update(data)

    async def set_state(
        self,
        device_id: str,
        data: Dict[str, Any],
        publish_func: Callable[[Envelope], Coroutine[Any, Any, None]],
        source_id: str,
        timeout: float = 10.0
    ) -> Dict[str, Any]:
        """
        Send a state-change request to a remote device and await its STATE_ACK.

        Args:
            device_id: Target device identifier.
            data: State fields to set.
            publish_func: Coroutine-returning callable that publishes the envelope.
            source_id: Source device identifier included in the envelope.
            timeout: Seconds to wait before raising :exc:`asyncio.TimeoutError`.

        Returns:
            The ``result`` dict from the STATE_ACK envelope.
        """
        env = Envelope.state(source=source_id, data=data)
        env.target = device_id

        ack_env = await self._wait_for_ack(
            target_id=device_id,
            cind=env.cind,
            publish_coro=publish_func(env),
            timeout=timeout
        )

        result = ack_env.data.get("result", {})
        if ack_env.data.get("status") == "ok":
            self.update_state(device_id, result)
        return result

    async def handle_incoming(self, env: Envelope) -> None:
        """
        Process an incoming DEVICE_STATE or STATE_ACK envelope.

        ``DEVICE_STATE`` updates the local state mirror.
        ``STATE_ACK`` resolves the pending correlation-ID waiter.
        """
        if env.type == EnvelopeType.DEVICE_STATE:
            self.update_state(env.source, env.data)
        elif env.type == EnvelopeType.STATE_ACK:
            self._resolve_ack(env.source, env.cind, env)
