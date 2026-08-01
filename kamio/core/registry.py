from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Dict, Optional, Type

if TYPE_CHECKING:
    from kamio.device import Device


class DeviceRegistry:
    """
    Registry for device classes and active instances.

    Maintains two mappings:
    - ``classes``:   device_type name → :class:`Device` subclass
    - ``instances``: device_id → :class:`Device` instance

    All operations are guarded by an :class:`threading.RLock` so the registry
    is safe to mutate from one coroutine while another iterates the snapshots
    returned by the ``classes`` / ``instances`` properties.
    """

    def __init__(self) -> None:
        """Initialize the device registry with empty class and instance maps."""
        self._classes: Dict[str, Type[Device]] = {}
        self._instances: Dict[str, Device] = {}
        self._lock = threading.RLock()

    def register_class(self, cls: Type[Device]) -> None:
        """Register a Device subclass under its ``device_type()`` name."""
        with self._lock:
            self._classes[cls.device_type()] = cls

    def unregister_class(self, device_type: str) -> Optional[Type[Device]]:
        """Remove and return the registered class for ``device_type`` (if any)."""
        with self._lock:
            return self._classes.pop(device_type, None)

    def register_instance(self, device_id: str, instance: Device) -> None:
        """Store a Device instance under ``device_id``."""
        with self._lock:
            self._instances[device_id] = instance

    def unregister_instance(self, device_id: str) -> Optional[Device]:
        """Remove and return the instance registered under ``device_id`` (if any)."""
        with self._lock:
            return self._instances.pop(device_id, None)

    def get_class(self, device_type: str) -> Optional[Type[Device]]:
        """Return the registered class for ``device_type``, or ``None``."""
        with self._lock:
            return self._classes.get(device_type)

    def get_instance(self, device_id: str) -> Optional[Device]:
        """Return the active instance for ``device_id``, or ``None``."""
        with self._lock:
            return self._instances.get(device_id)

    @property
    def classes(self) -> Dict[str, Type[Device]]:
        """Snapshot mapping of device_type names to registered :class:`Device` subclasses."""
        with self._lock:
            return dict(self._classes)

    @property
    def instances(self) -> Dict[str, Device]:
        """Snapshot mapping of device_ids to active :class:`Device` instances."""
        with self._lock:
            return dict(self._instances)
