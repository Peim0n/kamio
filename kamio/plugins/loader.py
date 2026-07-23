from __future__ import annotations
import asyncio
import importlib
import importlib.util
import inspect
import logging
import os
from typing import Any, Callable, Dict, List, Optional, Set, Type, TYPE_CHECKING

from kamio.plugins.base import Plugin

if TYPE_CHECKING:
    from kamio.app import KamioApp
    from kamio.core.event_bus import EventBus
    from kamio.core.hooks import HooksManager

logger = logging.getLogger("Kamio.plugin_loader")


class PluginContext:
    """
    Scoped context given to a plugin during load.

    Tracks all event subscriptions, hooks, rules, and tasks registered by the
    plugin so they can be cleanly removed on unload.  Exposes a duck-typed
    subset of :class:`EventBus` and :class:`HooksManager`, so legacy plugins
    that call ``event_bus.subscribe(...)`` / ``hooks.register(...)`` continue
    to work without monkey-patching application objects.
    """

    def __init__(self, app: "KamioApp", plugin_name: str) -> None:
        self.app = app
        self.plugin_name = plugin_name
        self._events: List[tuple] = []
        self._hooks: List[tuple] = []
        self._tasks: Set[asyncio.Task] = set()
        self._logger = logging.getLogger(f"Kamio.plugin.{plugin_name}")

    @property
    def logger(self) -> logging.Logger:
        """Logger scoped to the plugin."""
        return self._logger

    # ---- EventBus-compatible API ----
    def subscribe(self, event_type: str, callback: Callable, **kwargs: Any) -> None:
        """Subscribe to an EventBus event and record the registration."""
        self._events.append((event_type, callback))
        self.app.event_bus.subscribe(event_type, callback, **kwargs)

    def unsubscribe(self, event_type: str, callback: Callable) -> None:
        """Unsubscribe from an EventBus event."""
        self.app.event_bus.unsubscribe(event_type, callback)

    async def publish_event(self, event_type: str, data: Dict[str, Any]) -> None:
        """Publish an event on the application EventBus."""
        await self.app.event_bus.publish(event_type, data)

    # ---- HooksManager-compatible API ----
    def register_hook(self, event_type: str, hook: Callable, **kwargs: Any) -> None:
        """Register a lifecycle hook and record the registration."""
        self._hooks.append((event_type, hook))
        self.app.hooks.register(event_type, hook, **kwargs)

    # HooksManager-compatible alias
    register = register_hook

    def unregister_hook(self, event_type: str, hook: Callable) -> None:
        """Unregister a lifecycle hook."""
        self.app.hooks.unregister(event_type, hook)

    # ---- Convenience helpers ----
    def add_rule(self, func: Callable, **kwargs: Any):
        """Register an automation rule through the app facade."""
        return self.app.add_rule(func, **kwargs)

    def create_task(self, coro, name: Optional[str] = None) -> asyncio.Task:
        """Schedule a background task and keep a reference for cleanup."""
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def cancel_tasks(self) -> None:
        """Cancel all background tasks started by this plugin."""
        for task in list(self._tasks):
            if not task.done():
                task.cancel()

    @property
    def registrations(self) -> Dict[str, List[tuple]]:
        return {"events": list(self._events), "hooks": list(self._hooks)}


class PluginLoader:
    """
    Manages loading, configuration, and unloading of plugins.

    Plugins are loaded in dependency order.
    Each plugin's subscribe_events() and register_hooks() are called
    automatically after on_load().
    """

    def __init__(self, app: "KamioApp") -> None:
        self.app = app
        self._loaded: Dict[str, Plugin] = {}
        self._load_order: List[str] = []
        # {plugin_name: PluginContext}
        self._contexts: Dict[str, PluginContext] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def load_plugin(
        self,
        plugin_class: Type[Plugin],
        config: Optional[Dict] = None,
    ) -> Plugin:
        """
        Instantiate, configure, and load a plugin class.

        Args:
            plugin_class: A subclass of Plugin.
            config: Optional configuration dict passed to plugin.configure().

        Returns:
            The loaded Plugin instance.

        Raises:
            TypeError: If plugin_class does not subclass Plugin.
            ValueError: If a plugin with the same name is already loaded,
                        or a declared dependency is not yet loaded.
        """
        if not (isinstance(plugin_class, type) and issubclass(plugin_class, Plugin)):
            raise TypeError(f"{plugin_class!r} is not a Plugin subclass")

        instance = plugin_class()

        if instance.name in self._loaded:
            raise ValueError(f"Plugin '{instance.name}' is already loaded")

        for dep in instance.dependencies:
            if dep not in self._loaded:
                raise ValueError(f"Plugin '{instance.name}' requires '{dep}' to be loaded first")

        if config:
            instance.configure(config)

        context = PluginContext(self.app, instance.name)

        # Call on_load with context if the plugin accepts it, otherwise legacy signature.
        sig = inspect.signature(instance.on_load)
        if len(sig.parameters) >= 2:
            await instance.on_load(self.app, context)
        else:
            await instance.on_load(self.app)

        # Pass PluginContext to subscribe_events/register_hooks.  Duck typing lets
        # legacy plugins keep calling event_bus.subscribe/hooks.register while the
        # loader tracks registrations for clean unload.
        instance.subscribe_events(context)
        instance.register_hooks(context)

        self._contexts[instance.name] = context
        self._loaded[instance.name] = instance
        self._load_order.append(instance.name)
        logger.info(f"Loaded plugin: {instance.name} v{instance.version}")

        await self.app.event_bus.publish(
            "plugin_loaded",
            {
                "plugin_name": instance.name,
                "plugin_version": instance.version,
            },
        )
        return instance

    async def unload_plugin(self, plugin_name: str) -> None:
        """
        Unload a plugin by name.

        Calls plugin.on_unload() and removes it from the registry.
        Raises ValueError if another loaded plugin depends on this one.
        Safe if the plugin is not found (logs a warning).
        """
        plugin = self._loaded.get(plugin_name)
        if plugin is None:
            logger.warning(f"unload_plugin: '{plugin_name}' not found")
            return

        dependents = [
            name
            for name, p in self._loaded.items()
            if plugin_name in p.dependencies and name != plugin_name
        ]
        if dependents:
            raise ValueError(f"Cannot unload plugin '{plugin_name}': " f"required by {dependents}")

        await plugin.on_unload(self.app)

        # Clean up event subscriptions, hooks, and background tasks.
        context = self._contexts.pop(plugin_name, None)
        if context is not None:
            context.cancel_tasks()
            for event_type, callback in context.registrations.get("events", []):
                self.app.event_bus.unsubscribe(event_type, callback)
            for hook_type, hook in context.registrations.get("hooks", []):
                self.app.hooks.unregister(hook_type, hook)

        del self._loaded[plugin_name]
        if plugin_name in self._load_order:
            self._load_order.remove(plugin_name)
        logger.info(f"Unloaded plugin: {plugin_name}")

        await self.app.event_bus.publish(
            "plugin_unloaded",
            {
                "plugin_name": plugin_name,
            },
        )

    async def load_from_module(
        self,
        module_name: str,
        config: Optional[Dict] = None,
    ) -> Plugin:
        """
        Import a module by dotted name and load the first Plugin subclass found.

        Args:
            module_name: Dotted Python module path (e.g. 'mypackage.my_plugin').
            config: Optional configuration dict.
        """
        mod = importlib.import_module(module_name)
        cls = self._find_plugin_class(mod)
        if cls is None:
            raise ImportError(f"No Plugin subclass found in module '{module_name}'")
        return await self.load_plugin(cls, config=config)

    async def load_plugins_from_directory(self, directory: str) -> List[Plugin]:
        """
        Scan a directory for .py files and load all Plugin subclasses found.

        Files are processed in alphabetical order.
        Plugins that fail to load are logged and skipped.
        """
        loaded: List[Plugin] = []
        if not os.path.isdir(directory):
            logger.warning(f"load_plugins_from_directory: '{directory}' is not a directory")
            return loaded

        for filename in sorted(os.listdir(directory)):
            if not filename.endswith(".py") or filename.startswith("_"):
                continue
            filepath = os.path.join(directory, filename)
            module_name = filename[:-3]
            try:
                spec = importlib.util.spec_from_file_location(module_name, filepath)
                if spec is None or spec.loader is None:
                    logger.warning(f"Could not create module spec for '{filepath}'")
                    continue
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                cls = self._find_plugin_class(mod)
                if cls is None:
                    continue
                instance = await self.load_plugin(cls)
                loaded.append(instance)
            except Exception as e:
                logger.error(f"Failed to load plugin from '{filepath}': {e}", exc_info=True)

        return loaded

    def get_plugin(self, plugin_name: str) -> Optional[Plugin]:
        """Return a loaded plugin instance by name, or None."""
        return self._loaded.get(plugin_name)

    def list_plugins(self) -> List[str]:
        """Return names of all currently loaded plugins in load order."""
        return [name for name in self._load_order if name in self._loaded]

    @property
    def load_order(self) -> List[str]:
        """Return the ordered list of successfully loaded plugin names."""
        return self.list_plugins()

    async def unload_all(self) -> None:
        """Unload all plugins in reverse load order."""
        for name in reversed(self._load_order.copy()):
            await self.unload_plugin(name)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _find_plugin_class(module) -> Optional[Type[Plugin]]:
        """Return the first concrete Plugin subclass found in a module."""
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                isinstance(attr, type)
                and issubclass(attr, Plugin)
                and attr is not Plugin
                and not getattr(attr, "__abstractmethods__", None)
            ):
                return attr
        return None
