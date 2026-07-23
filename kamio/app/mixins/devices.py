from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional, Type, TYPE_CHECKING

from kamio.core.handlers import DeviceHandler
from kamio.core.mqtt_nodes import DeviceNode
from kamio.device import Device

if TYPE_CHECKING:
    from kamio.app import KamioApp

logger = logging.getLogger("Kamio.app")


class DeviceRegistryMixin:
    """Device class registration and instance lifecycle."""

    @property
    def devices(self: Any) -> Dict[str, Device]:
        """Returns all active device instances."""
        return self.registry.instances

    @property
    def registered_types(self: Any) -> List[str]:
        """Returns list of registered device type names."""
        return list(self.registry.classes.keys())

    def device(self: Any, cls: Optional[Type[Device]] = None):
        """Decorator to register a device class."""

        def decorator(device_cls: Type[Device]):
            if not issubclass(device_cls, Device):
                raise TypeError(f"{device_cls.__name__} must inherit from Device")
            self.register(device_cls)
            return device_cls

        if cls is None:
            return decorator
        return decorator(cls)

    def register(self: Any, device_class: Type[Device]) -> None:
        """Register a device class so it can be instantiated by type name."""
        self.registry.register_class(device_class)
        
        # Auto-register device rules
        if hasattr(device_class, 'Kamio_RULES') and device_class.Kamio_RULES:
            for rule_name, rule_func in device_class.Kamio_RULES.items():
                fields = getattr(rule_func, '_rule_fields', None)
                description = getattr(rule_func, '_rule_description', None)
                self.add_rule(rule_func, device=device_class, fields=fields, description=description)
        
        logger.info(f"Registered device: {device_class.__name__}")

    async def create_device(self: Any, device_id: str, device_type: str, **kwargs) -> Device:
        """Create a device by string type name."""
        cls = self.registry.get_class(device_type)
        if not cls:
            raise ValueError(f"Device type '{device_type}' not registered")

        device_instance = cls(**kwargs)
        device_instance.app = self
        await device_instance.on_init(**kwargs)

        node = DeviceNode(device_id=device_id, mqtt_client=self.mqtt_client)
        device_instance.node = node

        handler = DeviceHandler(device_instance, node, state_manager=self.state)
        node.set_handler(handler)

        self.registry.register_instance(device_id, device_instance)
        self._device_nodes[device_id] = node

        if self._is_running:
            await node.start()

        await self.hooks.trigger("on_device_added", device_instance)
        await self.event_bus.publish(
            "device_added",
            {
                "device_id": device_id,
                "device_type": device_instance.device_type(),
                "device": device_instance,
            },
        )
        if getattr(self, "_ha_discovery_enabled", False):
            try:
                await self.ha_discovery.announce(device_instance)
            except Exception as e:
                logger.warning(f"Failed to announce device '{device_id}' to HA: {e}")
        return device_instance

    async def remove_device(self: Any, device_id: str) -> None:
        """Stop and remove a device instance."""
        device_instance = self.registry.get_instance(device_id)
        if device_instance is None:
            logger.warning(f"remove_device: device '{device_id}' not found")
            return
        await self.hooks.trigger("on_device_removed", device_instance)
        await self.event_bus.publish(
            "device_removed",
            {
                "device_id": device_id,
                "device_type": device_instance.device_type(),
            },
        )
        node = self._device_nodes.get(device_id)
        if node:
            await node.stop()
            del self._device_nodes[device_id]
        self.registry.instances.pop(device_id, None)

    async def add_device(self: Any, device_id: str, device_class: Type[Device], **kwargs) -> Device:
        """Create and register a device instance."""
        if not (isinstance(device_class, type) and issubclass(device_class, Device)):
            raise TypeError(
                f"add_device: 'device_class' must be a Device subclass, "
                f"got {device_class!r}. Did you pass an instance instead of a class?"
            )
        if device_id in self.registry.instances:
            raise ValueError(
                f"add_device: device with id '{device_id}' is already registered. "
                f"Use a unique device_id or call remove_device('{device_id}') first."
            )
        device_type = device_class.device_type()
        if device_type not in self.registered_types:
            self.register(device_class)

        return await self.create_device(device_id, device_type, **kwargs)
