from __future__ import annotations
import asyncio
import logging
import signal
from typing import Any, Callable, Dict, List, Optional, Type, Union
from urllib.parse import urlparse

import paho.mqtt.client as mqtt

from synapse.core import (
    ServerNode,
    DeviceNode,
    StateManager,
    CommandManager,
    RuleEngine,
    DeviceRegistry
)
from synapse.core.rules import Rule
from synapse.core.handlers import DeviceHandler
from synapse.device import Device

logger = logging.getLogger("synapse.app")

class SynapseApp:
    """
    Main Application class for Synapse Core.

    Handles the orchestration of devices, rules, and MQTT communication.
    Provides high-level API for interacting with the IoT system.
    """
    def __init__(
        self,
        mqtt_broker: Union[str, mqtt.Client] = "mqtt://localhost:1883",
        client_id: Optional[str] = None,
        keepalive: int = 60,
        clean_session: bool = True,
        protocol: int = mqtt.MQTTv5,
        log_level: Optional[int] = logging.INFO,
        **kwargs
    ):
        if log_level is not None:
            logging.basicConfig(
                level=log_level,
                format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            )

        # Core components
        self.state = StateManager()
        self.commands = CommandManager()
        self.rules = RuleEngine(self)
        self.registry = DeviceRegistry()

        # MQTT Client initialization
        if isinstance(mqtt_broker, mqtt.Client):
            self.mqtt_client = mqtt_broker
            self._mqtt_host = None
            self._mqtt_port = None
        else:
            if client_id:
                kwargs["client_id"] = client_id
            if "keepalive" not in kwargs:
                kwargs["keepalive"] = keepalive
            self.mqtt_client = self._init_mqtt_client(mqtt_broker, **kwargs)

        self.server_node = ServerNode(
            mqtt_client=self.mqtt_client,
            state_manager=self.state,
            command_manager=self.commands
        )

        # Setup MQTT callbacks
        self.mqtt_client.on_message = self._on_mqtt_message
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_disconnect = self._on_mqtt_disconnect

        self._device_nodes: Dict[str, DeviceNode] = {}
        self._is_running = False

    @property
    def logger(self) -> logging.Logger:
        """Returns the application logger."""
        return logger

    @property
    def is_running(self) -> bool:
        """Returns True if the application is currently running."""
        return self._is_running

    def _init_mqtt_client(self, broker_uri: str, **kwargs) -> mqtt.Client:
        parsed = urlparse(broker_uri)

        client_id = kwargs.get("client_id", "")
        protocol = kwargs.get("protocol", mqtt.MQTTv5)
        transport = kwargs.get("transport", "tcp")
        clean_session = kwargs.get("clean_session", True)

        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=protocol,
            transport=transport,
            clean_session=clean_session if protocol != mqtt.MQTTv5 else None
        )

        self._mqtt_clean_start = clean_session if protocol == mqtt.MQTTv5 else None

        if "keepalive" in kwargs:
            client._keepalive = kwargs["keepalive"]

        if parsed.username:
            client.username_pw_set(parsed.username, parsed.password)

        self._mqtt_host = parsed.hostname or "localhost"
        self._mqtt_port = parsed.port or 1883

        return client

    def _on_mqtt_connect(self, client, userdata, flags, rc, properties=None):
        logger.info(f"MQTT connected (rc={rc})")

    def _on_mqtt_disconnect(self, client, userdata, disconnect_flags, rc, properties=None):
        logger.warning(f"MQTT disconnected (rc={rc})")

    def _on_mqtt_message(self, client, userdata, msg):
        """Global MQTT message dispatcher."""
        self.server_node._on_mqtt_message_callback(client, userdata, msg)
        for node in self._device_nodes.values():
            node._on_mqtt_message_callback(client, userdata, msg)

    @property
    def devices(self) -> Dict[str, Device]:
        """Returns all active device instances."""
        return self.registry.instances

    @property
    def registered_types(self) -> List[str]:
        """Returns list of registered device type names."""
        return list(self.registry.classes.keys())

    def device(self, cls: Optional[Type[Device]] = None):
        """Decorator to register device classes.
        Usage:
            @app.device
            class MyDevice(Device): ...
            
            OR
            
            @app.device()
            class MyDevice(Device): ...
        """
        def decorator(device_cls: Type[Device]):
            self.register(device_cls)
            return device_cls

        if cls is None:
            return decorator
        return decorator(cls)

    async def _run_async(self):
        """Internal async runner with signal handling."""
        loop = asyncio.get_running_loop()
        
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))
            except (NotImplementedError, ValueError):
                pass

        await self.start()
        while self._is_running:
            await asyncio.sleep(1)

    def run(self):
        """
        Blocking run method. Recommended way to start a production application.
        """
        try:
            asyncio.run(self._run_async())
        except (KeyboardInterrupt, SystemExit):
            logger.info("Application interrupted")
        except Exception as e:
            logger.exception(f"Application crashed: {e}")
        finally:
            if self._is_running:
                # Use a new loop for cleanup if the main one is closed
                try:
                    asyncio.run(self.stop())
                except Exception:
                    pass

    def rule(self, device: Optional[Type[Device]] = None, interval: Optional[float] = None, fields: Optional[List[str]] = None, enabled: bool = True, description: Optional[str] = None):
        """Decorator to register a rule."""
        def decorator(func: Callable[[dict, SynapseApp], Any]):
            rule_obj = Rule(func, device_class=device, interval=interval, fields=fields, enabled=enabled, description=description)
            self.rules.add_rule(rule_obj)
            return func
        return decorator

    def register(self, device_class: Type[Device]):
        """Register a device class."""
        self.registry.register_class(device_class)
        logger.info(f"Registered device: {device_class.__name__}")

    async def create_device(self, device_id: str, device_type: str, **kwargs) -> Device:
        """Create and start a device instance."""
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

        return device_instance

    async def start(self):
        """Start the application: connect to MQTT broker and initialize all nodes."""
        if self._is_running:
            return

        logger.info("SynapseApp starting...")

        if not self.mqtt_client.is_connected():
            if not self._mqtt_host:
                raise RuntimeError("MQTT broker host not configured")

            connect_kwargs = {}
            if self._mqtt_clean_start is not None:
                connect_kwargs["clean_start"] = self._mqtt_clean_start

            self.mqtt_client.connect(self._mqtt_host, self._mqtt_port, **connect_kwargs)
            self.mqtt_client.loop_start()

        self._is_running = True
        await self.server_node.start()

        for node in self._device_nodes.values():
            await node.start()

        await self.rules.start()
        logger.info("SynapseApp started")

    async def stop(self):
        """Stop the application gracefully."""
        if not self._is_running:
            return

        logger.info("SynapseApp stopping...")
        self._is_running = False

        try:
            await asyncio.wait_for(self.rules.stop(), timeout=5.0)

            if self._device_nodes:
                stop_tasks = [node.stop() for node in self._device_nodes.values()]
                await asyncio.wait_for(asyncio.gather(*stop_tasks, return_exceptions=True), timeout=5.0)

            await asyncio.wait_for(self.server_node.stop(), timeout=5.0)

            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()

            logger.info("SynapseApp stopped")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
