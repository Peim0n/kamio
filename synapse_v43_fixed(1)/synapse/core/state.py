from __future__ import annotations
import logging
from typing import Any, Dict, Optional, Callable, Coroutine
from .envelope import Envelope, EnvelopeType
from .correlation import BaseCorrelationManager

logger = logging.getLogger("synapse.state")

class StateManager(BaseCorrelationManager):
    """
    Centralized management of device states.
    """
    def __init__(self, max_pending: int = 1000):
        super().__init__(max_pending=max_pending)
        self._states: Dict[str, Dict[str, Any]] = {}

    def get_state(self, device_id: str, field: Optional[str] = None) -> Any:
        device_data = self._states.get(device_id, {})
        if field:
            return device_data.get(field)
        return device_data.copy()

    def get_all_states(self) -> Dict[str, Dict[str, Any]]:
        """Returns a copy of all current device states."""
        return {k: v.copy() for k, v in self._states.items()}

    def update_state(self, device_id: str, data: Dict[str, Any]):
        if not data: return
        if device_id not in self._states:
            self._states[device_id] = {}
        self._states[device_id].update(data)

    def update_from_telemetry(self, device_id: str, data: Dict[str, Any]):
        self.update_state(device_id, data)

    async def set_state(
        self,
        device_id: str,
        data: Dict[str, Any],
        publish_func: Callable[[Envelope], Coroutine[Any, Any, None]],
        source_id: str,
        timeout: float = 10.0
    ) -> Dict[str, Any]:
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

    async def handle_incoming(self, env: Envelope):
        if env.type == EnvelopeType.DEVICE_STATE:
            self.update_state(env.source, env.data)
        elif env.type == EnvelopeType.STATE_ACK:
            self._resolve_ack(env.source, env.cind, env)
