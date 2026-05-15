from __future__ import annotations
from typing import Dict, Optional, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from synapse.device import Device

class DeviceRegistry:
    def __init__(self):
        self._classes: Dict[str, Type[Device]] = {}
        self._instances: Dict[str, Device] = {}

    def register_class(self, cls: Type[Device]):
        self._classes[cls.device_type()] = cls

    def register_instance(self, device_id: str, instance: Device):
        self._instances[device_id] = instance

    def get_class(self, device_type: str) -> Optional[Type[Device]]:
        return self._classes.get(device_type)

    def get_instance(self, device_id: str) -> Optional[Device]:
        return self._instances.get(device_id)

    @property
    def classes(self) -> Dict[str, Type[Device]]:
        return self._classes

    @property
    def instances(self) -> Dict[str, Device]:
        return self._instances
