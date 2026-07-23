from __future__ import annotations
from typing import Dict, Optional, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from kamio.device import Device

class DeviceRegistry:
    """
    Registry for device classes and active instances.

    Maintains two mappings:
    - ``classes``:   device_type name → :class:`Device` subclass
    - ``instances``: device_id → :class:`Device` instance
    """
    def __init__(self) -> None:
        self._classes: Dict[str, Type[Device]] = {}
        self._instances: Dict[str, Device] = {}

    def register_class(self, cls: Type[Device]) -> None:
        """Register a Device subclass under its ``device_type()`` name."""
        self._classes[cls.device_type()] = cls

    def register_instance(self, device_id: str, instance: Device) -> None:
        """Store a Device instance under ``device_id``."""
        self._instances[device_id] = instance

    def get_class(self, device_type: str) -> Optional[Type[Device]]:
        """Return the registered class for ``device_type``, or ``None``."""
        return self._classes.get(device_type)

    def get_instance(self, device_id: str) -> Optional[Device]:
        """Return the active instance for ``device_id``, or ``None``."""
        return self._instances.get(device_id)

    @property
    def classes(self) -> Dict[str, Type[Device]]:
        """Live mapping of device_type names to registered :class:`Device` subclasses."""
        return self._classes

    @property
    def instances(self) -> Dict[str, Device]:
        """Live mapping of device_ids to active :class:`Device` instances."""
        return self._instances
