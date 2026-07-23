from __future__ import annotations
from typing import Any, Callable


class HotReloadFacadeMixin:
    """Shortcuts for the hot-reload manager."""

    def enable_hot_reload(self: Any) -> None:
        """Start the hot-reload file-watch polling loop."""
        self.hot_reload.enable()

    async def disable_hot_reload(self: Any) -> None:
        """Stop hot-reload polling loop."""
        await self.hot_reload.disable()

    def watch_file(self: Any, path: str, handler: Callable) -> None:
        """Watch a single file and invoke ``handler(file_path)`` when it changes."""
        self.hot_reload.watch_file(path, handler)

    def watch_directory(self: Any, directory: str, pattern: str, handler: Callable) -> None:
        """Watch a directory for matching file changes."""
        self.hot_reload.watch_directory(directory, pattern, handler)
