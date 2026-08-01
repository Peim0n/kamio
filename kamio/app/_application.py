from __future__ import annotations

import asyncio
import logging
import threading
from typing import Dict, List, Optional, Union

import gmqtt as mqtt

from kamio.app.mixins.custom_nodes import CustomNodeFacadeMixin
from kamio.app.mixins.devices import DeviceRegistryMixin
from kamio.app.mixins.hooks_events import HookEventFacadeMixin
from kamio.app.mixins.hot_reload import HotReloadFacadeMixin
from kamio.app.mixins.lifecycle import LifecycleMixin
from kamio.app.mixins.mqtt import MqttDispatchMixin
from kamio.app.mixins.plugins import PluginFacadeMixin
from kamio.app.mixins.rules import RuleRegistryMixin
from kamio.config import Config
from kamio.core import (
    CommandManager,
    DeviceNode,
    DeviceRegistry,
    RuleEngine,
    ServerNode,
    StateManager,
)
from kamio.core.custom_nodes import CustomNodeManager
from kamio.core.event_bus import EventBus
from kamio.core.hooks import HooksManager
from kamio.core.hot_reload import HotReloadManager
from kamio.core.mqtt_connection import MqttConnection
from kamio.device import Device
from kamio.discovery import HADiscovery
from kamio.plugins.loader import PluginLoader

logger = logging.getLogger("Kamio.app")


class KamioApp(
    LifecycleMixin,
    MqttDispatchMixin,
    DeviceRegistryMixin,
    RuleRegistryMixin,
    PluginFacadeMixin,
    HotReloadFacadeMixin,
    CustomNodeFacadeMixin,
    HookEventFacadeMixin,
):
    """
    Central application class for Kamio Core.

    Orchestrates devices, automation rules, MQTT communication, plugins,
    custom nodes, and hot-reload.  The recommended entry point for any
    Kamio IoT application.

    Public attributes (read-only after init):
        state:        :class:`StateManager`      — shared device state store.
        rules:        :class:`RuleEngine`         — automation rule engine.
        registry:     :class:`DeviceRegistry`    — registered device classes and instances.
        hooks:        :class:`HooksManager`      — internal lifecycle hooks.
        event_bus:    :class:`EventBus`          — application-level pub/sub.
        plugin_loader: :class:`PluginLoader`      — plugin management.
        hot_reload:   :class:`HotReloadManager`  — file-watch based code reload.
        custom_nodes: :class:`CustomNodeManager` — custom MQTT node registry.
        mqtt_client:  ``gmqtt.Client`` — underlying MQTT client.

    Minimal usage::

        app = KamioApp(mqtt_broker="mqtt://localhost:1883")

        class MyLight(Device):
            power: bool = state(default=False, writable=True)

        @app.rule(device=MyLight, fields=["power"])
        async def on_power(event, app):
            print(event.data["power"])

        async def main():
            await app.add_device("light_1", MyLight)
            await app.start()

        asyncio.run(main())
    """

    def __init__(
        self,
        mqtt_broker: Union[str, mqtt.Client, None] = None,
        client_id: Optional[str] = None,
        keepalive: int = 60,
        clean_session: bool = True,
        protocol: int = 5,
        log_level: Optional[int] = None,
        config_path: Optional[str] = None,
        shutdown_timeout: float = 5.0,
        **kwargs,
    ):
        """
        Args:
            mqtt_broker:   MQTT broker URI (``"mqtt://host:port"``) **or** a
                           pre-configured ``gmqtt.Client`` instance.  If omitted,
                           the value is read from ``Config``.
            client_id:     MQTT client identifier.  Auto-generated if omitted.
            keepalive:     Broker keepalive interval in seconds (default 60).
            clean_session: Start with a clean MQTT session (default ``True``).
            protocol:      MQTT protocol version (default ``5``, i.e. MQTTv5).
            log_level:     Python logging level applied globally.  Pass ``None``
                           to use the value from ``Config`` or leave logging
                           configuration untouched.
            config_path:   Optional path to a JSON configuration file.  Values
                           from the file (or ``Kamio_*`` env vars) fill in
                           ``mqtt_broker`` and ``log_level`` when not passed
                           explicitly.
            shutdown_timeout: Maximum time in seconds to wait for graceful shutdown (default 5.0).
            **kwargs:      Extra keyword arguments forwarded to ``MqttConnection``
                           (e.g. ``transport``, ``tls``, ``reconnect_min_delay``,
                           ``reconnect_max_delay``).
        """
        self.config = Config(config_path)
        resolved_log_level = log_level if log_level is not None else self.config.log_level
        resolved_broker = mqtt_broker if mqtt_broker is not None else self.config.mqtt_broker
        self._keepalive = keepalive
        self.shutdown_timeout = shutdown_timeout

        if resolved_log_level is not None:
            # Configure only the Kamio logger — do not call basicConfig()
            # which would override the host application's logging setup.
            logging.getLogger("Kamio").setLevel(resolved_log_level)

        self.state = StateManager()
        self.commands = CommandManager()
        self.rules = RuleEngine(self)
        self.registry = DeviceRegistry()
        self.hooks = HooksManager()
        self.event_bus = EventBus()
        self.plugin_loader = PluginLoader(self)
        self.hot_reload = HotReloadManager(self)
        self.custom_nodes = CustomNodeManager(self)
        self.ha_discovery: Optional[HADiscovery] = None
        self._ha_discovery_enabled = False

        if isinstance(resolved_broker, mqtt.Client):
            self.mqtt_client = resolved_broker
            self._mqtt_conn: Optional[MqttConnection] = None
            if kwargs:
                raise TypeError(
                    f"KamioApp() got unexpected keyword argument(s): {sorted(kwargs)!r}. "
                    f"These are only forwarded to MqttConnection when 'mqtt_broker' is a URI."
                )
        else:
            transport = kwargs.pop("transport", "tcp")
            reconnect_min_delay = kwargs.pop("reconnect_min_delay", 1.0)
            reconnect_max_delay = kwargs.pop("reconnect_max_delay", 60.0)
            tls = kwargs.pop("tls", None)
            if kwargs:
                raise TypeError(
                    f"KamioApp() got unexpected keyword argument(s): {sorted(kwargs)!r}. "
                    f"Accepted extras: transport, reconnect_min_delay, reconnect_max_delay, tls."
                )
            self._mqtt_conn = MqttConnection(
                broker_uri=resolved_broker,
                client_id=client_id,
                keepalive=keepalive,
                clean_session=clean_session,
                protocol=protocol,
                transport=transport,
                reconnect_min_delay=reconnect_min_delay,
                reconnect_max_delay=reconnect_max_delay,
                tls=tls,
            )
            self.mqtt_client = self._mqtt_conn.client

        if self._mqtt_conn is not None:
            server_id = self._mqtt_conn.client_id
        else:
            raw_id = getattr(self.mqtt_client, "_client_id", "") or ""
            server_id = raw_id.decode() if isinstance(raw_id, (bytes, bytearray)) else raw_id
        self.server_node = ServerNode(
            mqtt_client=self.mqtt_client,
            state_manager=self.state,
            command_manager=self.commands,
            device_id=server_id or None,
        )

        self.mqtt_client.on_message = self._on_mqtt_message
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_disconnect = self._on_mqtt_disconnect

        self._device_nodes: Dict[str, DeviceNode] = {}
        self._device_nodes_lock = threading.RLock()
        self._is_running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    @property
    def logger(self) -> logging.Logger:
        """Returns the application logger."""
        return logger

    def enable_ha_discovery(self, prefix: str = "homeassistant") -> None:
        """Enable Home Assistant MQTT Discovery announcements for new devices.

        Also announces all currently registered device instances so that
        pre-existing devices are discovered by Home Assistant.
        """
        if self.ha_discovery is None or self.ha_discovery.discovery_prefix != prefix:
            self.ha_discovery = HADiscovery(discovery_prefix=prefix)
        self._ha_discovery_enabled = True
        logger.info(f"Home Assistant discovery enabled with prefix '{prefix}'")

        # Announce all existing devices that have a running node.
        for device_id, device in self._device_nodes.items():
            instance = self.registry.get_instance(device_id)
            if instance is None:
                continue
            # Schedule the announcement on the event loop if running.
            self._schedule_when_running(self.ha_discovery.announce(instance))

    def disable_ha_discovery(self) -> None:
        """Disable Home Assistant MQTT Discovery announcements."""
        self._ha_discovery_enabled = False
        logger.info("Home Assistant discovery disabled")
