from __future__ import annotations

import asyncio
import importlib
import importlib.util
import logging
import os
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Type

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
    subset of :class:`EventBus` and :class:`HooksManager` so plugins can call
    ``context.subscribe(...)`` / ``context.register_hook(...)`` directly.
    """

    def __init__(self, app: "KamioApp", plugin_name: str) -> None:
        """Create a scoped context for a plugin.

        Tracks all subscriptions, hooks, rules, and tasks for cleanup on unload.
        """
        self.app = app
        self.plugin_name = plugin_name
        self._events: List[tuple] = []
        self._hooks: List[tuple] = []
        self._rules: List[Callable] = []
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
    def register(self, event_type: str, hook: Callable, **kwargs: Any) -> None:
        """Alias for register_hook()."""
        self.register_hook(event_type, hook, **kwargs)

    def unregister_hook(self, event_type: str, hook: Callable) -> None:
        """Unregister a lifecycle hook."""
        self.app.hooks.unregister(event_type, hook)

    # ---- Convenience helpers ----
    def add_rule(self, func: Callable, **kwargs: Any):
        """Register an automation rule through the app facade and track it for cleanup."""
        registered = self.app.add_rule(func, **kwargs)
        # add_rule returns the original function; keep a reference so the rule
        # can be removed cleanly when the plugin is unloaded.
        self._rules.append(registered)
        return registered

    def create_task(self, coro, name: Optional[str] = None) -> asyncio.Task:
        """Schedule a background task and keep a reference for cleanup."""
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def cancel_tasks(self) -> None:
        """Cancel all background tasks started by this plugin and await them.

        Awaiting ensures cancelled tasks have a chance to run their
        ``finally`` blocks and release resources before the caller proceeds.
        """
        pending = [t for t in self._tasks if not t.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    @property
    def registrations(self) -> Dict[str, List[tuple]]:
        """Return a dict of recorded event and hook registrations."""
        return {"events": list(self._events), "hooks": list(self._hooks)}


class PluginLoader:
    """
    Manages loading, configuration, and unloading of plugins.

    Plugins are loaded in dependency order.
    Each plugin's subscribe_events() and register_hooks() are called
    automatically after on_load().
    """

    def __init__(self, app: "KamioApp") -> None:
        """Create a PluginLoader bound to the given KamioApp instance."""
        self.app = app
        self._loaded: Dict[str, Plugin] = {}
        self._load_order: List[str] = []
        # {plugin_name: PluginContext}
        self._contexts: Dict[str, PluginContext] = {}
        # Tracks plugins currently being loaded (for cycle detection across
        # recursive load_plugin calls).
        self._loading: set[str] = set()

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

        if not instance.name:
            raise ValueError(f"Plugin class {plugin_class!r} has an empty name")
        if instance.name in self._loaded:
            raise ValueError(f"Plugin '{instance.name}' is already loaded")

        # Auto-load missing dependencies (transitive) with cycle detection.
        await self._ensure_dependencies(instance)

        if config:
            instance.configure(config)

        context = PluginContext(self.app, instance.name)

        try:
            await instance.on_load(self.app, context)
            # Pass PluginContext to subscribe_events/register_hooks.
            instance.subscribe_events(context)
            instance.register_hooks(context)
        except Exception:
            # on_load() or the post-load hooks failed: the plugin is not
            # added to the loaded registry, so unload_plugin() will never
            # run for it.  Tear down whatever the context has already
            # recorded (subscriptions, hooks, rules, tasks) so resources
            # allocated before the failure do not leak.
            await self._cleanup_context(context)
            raise

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

    async def _cleanup_context(self, context: PluginContext) -> None:
        """Tear down everything a PluginContext has registered.

        Cancels background tasks, unsubscribes events/hooks, and removes
        rules.  Used both by :meth:`unload_plugin` (after on_unload) and by
        :meth:`load_plugin` when ``on_load`` fails so a half-loaded plugin
        never leaks resources.
        """
        await context.cancel_tasks()
        for event_type, callback in context.registrations.get("events", []):
            self.app.event_bus.unsubscribe(event_type, callback)
        for hook_type, hook in context.registrations.get("hooks", []):
            self.app.hooks.unregister(hook_type, hook)
        for rule_func in list(getattr(context, "_rules", [])):
            try:
                await self.app.remove_rule(rule_func)
            except Exception as e:
                logger.warning(f"Failed to remove plugin rule: {e}")

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

        try:
            await plugin.on_unload(self.app)
        finally:
            # Clean up event subscriptions, hooks, rules, and background tasks
            # even if on_unload raised, so resources never leak.
            context = self._contexts.pop(plugin_name, None)
            if context is not None:
                await self._cleanup_context(context)

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

    async def _ensure_dependencies(
        self,
        instance: Plugin,
    ) -> None:
        """Recursively load missing dependencies of ``instance``.

        Dependencies are looked up from plugins already registered on the app's
        plugin loader by class. If a dependency name is not loaded and cannot
        be resolved to a registered class, a :class:`ValueError` is raised.
        Cycles are detected via the loader-level ``_loading`` set so that
        recursive ``load_plugin`` calls share the same visiting state.
        """
        if instance.name in self._loading:
            raise ValueError(f"Circular plugin dependency detected involving '{instance.name}'")
        self._loading.add(instance.name)
        try:
            for dep in instance.dependencies:
                if not dep:
                    raise ValueError(f"Plugin '{instance.name}' declared an empty dependency")
                if dep in self._loaded:
                    continue
                # Try to resolve the dependency from the app's registry of plugin
                # classes (if any), otherwise we cannot satisfy it.
                dep_class = self._resolve_dependency_class(dep)
                if dep_class is None:
                    raise ValueError(
                        f"Plugin '{instance.name}' requires '{dep}' which is not loaded "
                        f"and could not be resolved automatically"
                    )
                await self.load_plugin(dep_class)
        finally:
            self._loading.discard(instance.name)

    def _resolve_dependency_class(self, name: str) -> Optional[Type[Plugin]]:
        """Resolve a dependency name to a Plugin subclass, if registered.

        By default Kamio does not keep a global plugin registry, so this returns
        ``None`` unless the application has registered one via
        ``app.plugin_loader.register_class(name, cls)``. Subclasses may override
        this to provide custom resolution.
        """
        registry = getattr(self, "_class_registry", {})
        return registry.get(name)

    def register_class(self, name: str, plugin_class: Type[Plugin]) -> None:
        """Register a Plugin subclass so dependencies can be auto-loaded by name."""
        if not hasattr(self, "_class_registry"):
            self._class_registry: Dict[str, Type[Plugin]] = {}
        self._class_registry[name] = plugin_class

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
