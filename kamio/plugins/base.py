from __future__ import annotations
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from kamio.app import KamioApp
    from kamio.core.event_bus import EventBus
    from kamio.core.hooks import HooksManager
    from kamio.plugins.loader import PluginContext


class Plugin(ABC):
    """
    Base class for all Kamio plugins.

    A plugin bundles event subscriptions, lifecycle hooks, and arbitrary
    logic into a single portable unit that can be loaded/unloaded at runtime.

    Subclass and implement at minimum `name`, `version`, and `on_load`.
    """

    def __init__(self) -> None:
        self._config: Dict[str, Any] = {}
        self.logger = logging.getLogger(f"Kamio.plugin.{self.name}")

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique plugin name (used as identifier)."""

    @property
    @abstractmethod
    def version(self) -> str:
        """Plugin version string (semver recommended)."""

    @property
    def description(self) -> str:
        """Short human-readable description."""
        return ""

    @property
    def dependencies(self) -> List[str]:
        """Names of other plugins this plugin requires to be loaded first."""
        return []

    def configure(self, config: Dict[str, Any]) -> None:
        """Apply plugin configuration. Called before on_load."""
        self._config = config

    @abstractmethod
    async def on_load(self, app: "KamioApp", context: Optional["PluginContext"] = None) -> None:
        """Called when the plugin is loaded. Override to set up resources.

        The optional ``context`` argument provides a scoped registration API
        for events, hooks, rules, and background tasks.  Plugins that only
        accept ``app`` continue to work via backward-compatible introspection.
        """

    async def on_unload(self, app: "KamioApp") -> None:
        """Called when the plugin is unloaded. Override to clean up resources."""

    def subscribe_events(self, event_bus: Any) -> None:
        """Subscribe to EventBus events. Called automatically after on_load.

        The ``event_bus`` argument is usually a :class:`PluginContext` duck-typed
        to expose ``subscribe``/``unsubscribe``.
        """

    def register_hooks(self, hooks: Any) -> None:
        """Register HooksManager hooks. Called automatically after on_load.

        The ``hooks`` argument is usually a :class:`PluginContext` duck-typed to
        expose ``register_hook``/``unregister_hook``.
        """

    def __repr__(self) -> str:
        return f"<Plugin name={self.name!r} version={self.version!r}>"
