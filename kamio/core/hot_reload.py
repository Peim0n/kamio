from __future__ import annotations
import asyncio
import importlib
import importlib.util
import inspect
import json
import logging
import os
import sys
from fnmatch import fnmatch
from typing import Any, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from kamio.app import KamioApp

logger = logging.getLogger("Kamio.hot_reload")

# Optional watchdog support: uses OS-level file system events when available.
try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler

    _WATCHDOG_AVAILABLE = True
except Exception:  # pragma: no cover
    Observer = None  # type: ignore
    FileSystemEventHandler = None  # type: ignore
    _WATCHDOG_AVAILABLE = False


class _WatchEntry:
    """Tracks a single watched path with its pattern, handler, and last mtime."""

    __slots__ = ("path", "pattern", "handler", "is_dir", "_mtimes")

    def __init__(self, path: str, pattern: str, handler: Callable, is_dir: bool) -> None:
        self.path = path
        self.pattern = pattern
        self.handler = handler
        self.is_dir = is_dir
        self._mtimes: Dict[str, float] = self._snapshot()

    def _snapshot(self) -> Dict[str, float]:
        result: Dict[str, float] = {}
        try:
            if self.is_dir:
                for fname in os.listdir(self.path):
                    if fnmatch(fname, self.pattern):
                        fpath = os.path.join(self.path, fname)
                        result[fpath] = os.path.getmtime(fpath)
            elif os.path.exists(self.path):
                result[self.path] = os.path.getmtime(self.path)
        except OSError as e:
            logger.debug(f"hot_reload: cannot read path '{self.path}': {e}")
        return result

    def changed_paths(self) -> List[str]:
        """Return file paths whose mtime changed since last check, then update snapshot."""
        new_snap = self._snapshot()
        changed = [
            p for p, mtime in new_snap.items() if p not in self._mtimes or self._mtimes[p] != mtime
        ]
        self._mtimes = new_snap
        return changed

    def matches(self, file_path: str) -> bool:
        """Return True if ``file_path`` matches this watch entry."""
        abs_path = os.path.abspath(file_path)
        if self.is_dir:
            dir_path = os.path.dirname(abs_path)
            if os.path.abspath(dir_path) != self.path:
                return False
            return fnmatch(os.path.basename(abs_path), self.pattern)
        return abs_path == self.path


class _WatchdogHandler:
    """Adapter translating watchdog events into HotReloadManager calls."""

    def __init__(self, manager: "HotReloadManager") -> None:
        self.manager = manager

    def dispatch(self, event) -> None:
        if event.is_directory:
            return
        for entry in self.manager._entries:
            if entry.matches(event.src_path):
                self.manager._schedule_call(event.src_path, entry.handler)


class HotReloadManager:
    """
    Watches files and directories for changes and calls registered handlers.

    Uses OS-level file system events via ``watchdog`` when available, falling
    back to asyncio-based polling otherwise. Supports a debounce delay to
    avoid duplicate triggers on rapid saves.
    """

    def __init__(
        self, app: "KamioApp", poll_interval: float = 1.0, debounce: float = 0.3
    ) -> None:
        self.app = app
        self._poll_interval = poll_interval
        self._debounce = debounce
        self._entries: List[_WatchEntry] = []
        self._task: Optional[asyncio.Task] = None
        self._enabled = False
        self._pending: Dict[str, asyncio.TimerHandle] = {}
        self._observer: Optional[Any] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def watch_file(self, path: str, handler: Callable) -> None:
        """
        Watch a single file for changes.

        Args:
            path: Absolute or relative file path.
            handler: Async or sync callable(file_path: str) called on change.
        """
        entry = _WatchEntry(
            path=os.path.abspath(path),
            pattern="*",
            handler=handler,
            is_dir=False,
        )
        self._entries.append(entry)
        logger.debug(f"Watching file: {entry.path}")

    def watch_directory(self, directory: str, pattern: str, handler: Callable) -> None:
        """
        Watch all files matching pattern in a directory.

        Args:
            directory: Directory path to watch.
            pattern: Glob pattern (e.g. '*.py', '*.yaml').
            handler: Async or sync callable(file_path: str) called on change.
        """
        entry = _WatchEntry(
            path=os.path.abspath(directory),
            pattern=pattern,
            handler=handler,
            is_dir=True,
        )
        self._entries.append(entry)
        logger.debug(f"Watching directory: {entry.path} pattern={pattern!r}")

    def enable(self) -> None:
        """Start watching files. Uses watchdog if installed, otherwise polling."""
        if self._enabled:
            return
        self._enabled = True

        # Capture the loop that owns this manager. Watchdog and polling may call
        # back from other threads, so scheduling must be marshalled here.
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.get_event_loop()

        if _WATCHDOG_AVAILABLE and self._entries:
            self._start_watchdog()
        else:
            self._start_polling()
        logger.info("HotReloadManager enabled")

    async def disable(self) -> None:
        """Stop watching files."""
        self._enabled = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            finally:
                self._task = None
        if self._observer is not None:
            self._observer.stop()
            self._observer.join()
            self._observer = None
        logger.info("HotReloadManager disabled")

    def _start_polling(self) -> None:
        """Start the asyncio polling fallback loop."""
        if self._loop is None:
            logger.error("HotReloadManager has no event loop; cannot start polling")
            return
        self._task = self._loop.create_task(self._poll_loop())

    def _start_watchdog(self) -> None:
        """Start an OS-level file system observer via watchdog."""
        if Observer is None:
            return
        self._observer = Observer()
        handler = _WatchdogHandler(self)
        watched_dirs = set()
        for entry in self._entries:
            if entry.is_dir:
                watched_dirs.add(entry.path)
            else:
                watched_dirs.add(os.path.dirname(entry.path))
        for directory in watched_dirs:
            self._observer.schedule(handler, directory, recursive=False)
        self._observer.start()

    @property
    def is_enabled(self) -> bool:
        return self._enabled

    def list_watched(self) -> List[str]:
        """Return all currently watched paths."""
        return [e.path for e in self._entries]

    # ------------------------------------------------------------------
    # Built-in reload handlers (can be used as handler args)
    # ------------------------------------------------------------------

    def _make_handler(self, reload_func: Callable) -> Callable:
        """Return an async handler that calls reload_func(file_path, app)."""
        app = self.app

        async def _handler(file_path: str) -> None:
            await reload_func(file_path, app)

        return _handler

    def make_rules_handler(self) -> Callable:
        """Return a handler that reloads rules from a changed Python file."""
        return self._make_handler(reload_rules_from_file)

    def make_devices_handler(self) -> Callable:
        """Return a handler that reloads device classes from a changed Python file."""
        return self._make_handler(reload_devices_from_file)

    def make_config_handler(self) -> Callable:
        """Return a handler that reloads config from a changed JSON/YAML file."""
        return self._make_handler(reload_config_from_file)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        while self._enabled:
            await asyncio.sleep(self._poll_interval)
            for entry in self._entries:
                for changed_path in entry.changed_paths():
                    self._schedule_call(changed_path, entry.handler)

    def _schedule_call(self, file_path: str, handler: Callable) -> None:
        """Thread-safe debounced scheduler entry point."""
        if self._loop is None:
            logger.error("HotReloadManager has no event loop; skipping schedule")
            return

        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        if current_loop is self._loop:
            self._schedule_call_in_loop(file_path, handler)
        else:
            self._loop.call_soon_threadsafe(self._schedule_call_in_loop, file_path, handler)

    def _schedule_call_in_loop(self, file_path: str, handler: Callable) -> None:
        """Debounce: cancel any pending call for this path and reschedule."""
        key = file_path
        if key in self._pending:
            self._pending[key].cancel()

        if self._loop is None:
            return

        def _fire():
            del self._pending[key]
            self._loop.create_task(self._invoke_handler(handler, file_path))

        self._pending[key] = self._loop.call_later(self._debounce, _fire)

    async def _invoke_handler(self, handler: Callable, file_path: str) -> None:
        try:
            if inspect.iscoroutinefunction(handler):
                await handler(file_path)
            else:
                handler(file_path)
        except Exception as e:
            logger.error(f"Error in hot-reload handler for '{file_path}': {e}", exc_info=True)
            await self.app.event_bus.publish(
                "hot_reload_error",
                {
                    "file_path": file_path,
                    "error": str(e),
                },
            )


# ---------------------------------------------------------------------------
# Standalone reload handlers
# ---------------------------------------------------------------------------


async def reload_rules_from_file(file_path: str, app: "KamioApp") -> bool:
    """
    Reload rules from a Python module file.

    Strategy:
    1. Import / reimport the module.
    2. Find all functions decorated with _is_rule marker OR registered via @app.rule.
    3. Replace matching rules in RuleEngine (same function name = update).
    4. Rollback to original rules on any error.

    Returns True on success, False on error.
    """
    from kamio.core.rules import Rule

    logger.info(f"[hot-reload] Reloading rules from {file_path}")
    old_rules = list(app.rules.rules)
    try:
        module = _load_module_from_file(file_path)
        new_funcs = _find_rule_funcs(module)

        if not new_funcs:
            logger.debug(f"[hot-reload] No rule functions found in {file_path}")
            return True

        old_by_name: Dict[str, Rule] = {r.func.__name__: r for r in app.rules.rules}
        replaced = 0
        for func, rule_kwargs in new_funcs:
            name = func.__name__
            if name in old_by_name:
                old_rule = old_by_name[name]
                app.rules.remove_rule(old_rule)
                new_rule = Rule(func, **rule_kwargs)
                app.rules.add_rule(new_rule)
                replaced += 1
                logger.info(f"[hot-reload] Updated rule: {name}")

        await app.event_bus.publish(
            "hot_reload_rules",
            {
                "file_path": file_path,
                "replaced": replaced,
            },
        )
        return True

    except Exception as e:
        logger.error(f"[hot-reload] Error reloading rules from '{file_path}': {e}", exc_info=True)
        await app.rules.stop()
        app.rules.rules[:] = old_rules
        app.rules._rebuild_index()
        if app._is_running:
            await app.rules.start()
        await _publish_reload_error(app, file_path, e)
        return False


async def reload_devices_from_file(file_path: str, app: "KamioApp") -> bool:
    """
    Reload Device subclasses from a Python module file.

    Updates DeviceRegistry._classes for each Device subclass found.
    Running instances continue with old class until restart.

    Returns True on success, False on error.
    """
    from kamio.device import Device

    logger.info(f"[hot-reload] Reloading device classes from {file_path}")
    try:
        module = _load_module_from_file(file_path)
        found = 0
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if isinstance(attr, type) and issubclass(attr, Device) and attr is not Device:
                type_name = attr.device_type()
                app.registry.classes[type_name] = attr
                found += 1
                logger.info(f"[hot-reload] Updated device class: {type_name}")

        await app.event_bus.publish(
            "hot_reload_devices",
            {
                "file_path": file_path,
                "updated_classes": found,
            },
        )
        return True

    except Exception as e:
        logger.error(f"[hot-reload] Error reloading devices from '{file_path}': {e}", exc_info=True)
        await _publish_reload_error(app, file_path, e)
        return False


async def reload_config_from_file(file_path: str, app: "KamioApp") -> bool:
    """
    Reload a JSON configuration file and publish hot_reload_config event.

    YAML support requires PyYAML. Falls back gracefully if not installed.

    Returns True on success, False on error.
    """
    logger.info(f"[hot-reload] Reloading config from {file_path}")
    try:
        new_config = _load_config_file(file_path)
        await app.event_bus.publish(
            "hot_reload_config",
            {
                "file_path": file_path,
                "config": new_config,
            },
        )
        logger.info(f"[hot-reload] Config reloaded from {file_path}")
        return True

    except Exception as e:
        logger.error(f"[hot-reload] Error reloading config '{file_path}': {e}", exc_info=True)
        await _publish_reload_error(app, file_path, e)
        return False


async def _publish_reload_error(app: "KamioApp", file_path: str, error: Exception) -> None:
    """Publish a hot-reload error event to the application event bus."""
    await app.event_bus.publish(
        "hot_reload_error",
        {
            "file_path": file_path,
            "error": str(error),
        },
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_module_from_file(file_path: str):
    """Import or reimport a module from an absolute file path."""
    abs_path = os.path.abspath(file_path)
    module_name = os.path.splitext(os.path.basename(abs_path))[0]

    existing = None
    for name, mod in list(sys.modules.items()):
        mod_file = getattr(mod, "__file__", None)
        if mod_file and os.path.abspath(mod_file) == abs_path:
            existing = mod
            break

    if existing is not None:
        return importlib.reload(existing)

    spec = importlib.util.spec_from_file_location(module_name, abs_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot create module spec for '{abs_path}'")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    sys.modules[module_name] = mod
    return mod


def _find_rule_funcs(module) -> List[Tuple[Callable, Dict[str, Any]]]:
    """Find functions marked as Kamio rules in a module."""
    results = []
    for attr_name in dir(module):
        func = getattr(module, attr_name)
        if callable(func) and getattr(func, "_Kamio_rule_kwargs", None) is not None:
            results.append((func, func._Kamio_rule_kwargs))
    return results


def _load_config_file(file_path: str) -> Dict[str, Any]:
    ext = os.path.splitext(file_path)[1].lower()
    with open(file_path, "r", encoding="utf-8") as f:
        if ext in (".yaml", ".yml"):
            try:
                import yaml

                return yaml.safe_load(f) or {}
            except ImportError:
                raise ImportError(
                    "PyYAML is required to reload .yaml configs. Install it: pip install pyyaml"
                )
        return json.load(f)
