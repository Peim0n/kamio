from __future__ import annotations
from typing import Any, Dict, List, Optional


class PluginFacadeMixin:
    """Shortcuts for plugin management."""

    async def load_plugin(self: Any, plugin_class, config: Optional[Dict[str, Any]] = None):
        """Instantiate and load a plugin."""
        return await self.plugin_loader.load_plugin(plugin_class, config=config)

    async def unload_plugin(self: Any, plugin_name: str) -> None:
        """Unload a plugin and clean up its event subscriptions and hooks."""
        await self.plugin_loader.unload_plugin(plugin_name)

    async def load_plugins_from_directory(self: Any, directory: str):
        """Scan a directory and load all discoverable plugins."""
        return await self.plugin_loader.load_plugins_from_directory(directory)

    async def load_from_module(self: Any, module_name: str, config: Optional[Dict[str, Any]] = None):
        """Load a plugin from a Python module by dotted path."""
        return await self.plugin_loader.load_from_module(module_name, config=config)

    def get_plugin(self: Any, plugin_name: str):
        """Return a loaded plugin instance by name, or ``None`` if not found."""
        return self.plugin_loader.get_plugin(plugin_name)

    def list_plugins(self: Any) -> List[str]:
        """Return names of all currently loaded plugins."""
        return self.plugin_loader.list_plugins()
