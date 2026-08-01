"""Comprehensive unit tests for kamio.core.hot_reload."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
import time

import pytest

from kamio import Device, KamioApp, rule, state
from kamio.core.hot_reload import (
    _WATCHDOG_AVAILABLE,
    HotReloadManager,
    _find_rule_funcs,
    _load_config_file,
    _load_module_from_file,
    _publish_reload_error,
    _WatchdogHandler,
    _WatchEntry,
    reload_config_from_file,
    reload_devices_from_file,
    reload_rules_from_file,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _touch(path: str, content: str = "x") -> None:
    """Write content to a file and bump its mtime into the future."""
    with open(path, "w") as f:
        f.write(content)
    # Force mtime to change even on coarse-grained filesystems.
    future = time.time() + 5
    os.utime(path, (future, future))


class _DummyEvent:
    """Minimal stand-in for a watchdog FileSystemEvent."""

    def __init__(self, src_path: str, is_directory: bool = False) -> None:
        self.src_path = src_path
        self.is_directory = is_directory


# ---------------------------------------------------------------------------
# _WatchEntry
# ---------------------------------------------------------------------------


class TestWatchEntry:
    def test_init_file(self, tmp_path):
        f = tmp_path / "a.txt"
        f.write_text("hello")
        entry = _WatchEntry(str(f), "*", lambda p: None, is_dir=False)
        assert entry.path == os.path.abspath(str(f))
        assert entry.pattern == "*"
        assert entry.is_dir is False
        assert entry.path in entry._mtimes

    def test_init_dir(self, tmp_path):
        (tmp_path / "x.py").write_text("1")
        (tmp_path / "y.py").write_text("2")
        (tmp_path / "z.txt").write_text("3")
        entry = _WatchEntry(str(tmp_path), "*.py", lambda p: None, is_dir=True)
        assert entry.path == os.path.abspath(str(tmp_path))
        assert entry.is_dir is True
        # Only .py files should be in the snapshot.
        basenames = {os.path.basename(p) for p in entry._mtimes}
        assert basenames == {"x.py", "y.py"}

    def test_snapshot_nonexistent_file(self, tmp_path):
        entry = _WatchEntry(str(tmp_path / "nope.txt"), "*", lambda p: None, is_dir=False)
        assert entry._mtimes == {}

    def test_snapshot_nonexistent_dir(self, tmp_path):
        entry = _WatchEntry(str(tmp_path / "nope_dir"), "*", lambda p: None, is_dir=True)
        assert entry._mtimes == {}

    def test_changed_paths_file(self, tmp_path):
        f = tmp_path / "watched.txt"
        f.write_text("initial")
        entry = _WatchEntry(str(f), "*", lambda p: None, is_dir=False)
        # No change yet.
        assert entry.changed_paths() == []
        # Modify.
        _touch(str(f), "changed")
        changed = entry.changed_paths()
        assert len(changed) == 1
        assert os.path.abspath(changed[0]) == os.path.abspath(str(f))
        # After snapshot update, no more changes.
        assert entry.changed_paths() == []

    def test_changed_paths_dir_new_file(self, tmp_path):
        entry = _WatchEntry(str(tmp_path), "*.py", lambda p: None, is_dir=True)
        assert entry.changed_paths() == []
        new_file = tmp_path / "new.py"
        new_file.write_text("print(1)")
        changed = entry.changed_paths()
        assert any(os.path.abspath(c) == os.path.abspath(str(new_file)) for c in changed)

    def test_changed_paths_dir_modified(self, tmp_path):
        f = tmp_path / "mod.py"
        f.write_text("a")
        entry = _WatchEntry(str(tmp_path), "*.py", lambda p: None, is_dir=True)
        assert entry.changed_paths() == []
        _touch(str(f), "b")
        changed = entry.changed_paths()
        assert any(os.path.abspath(c) == os.path.abspath(str(f)) for c in changed)

    def test_changed_paths_dir_ignored_pattern(self, tmp_path):
        entry = _WatchEntry(str(tmp_path), "*.py", lambda p: None, is_dir=True)
        (tmp_path / "ignored.txt").write_text("nope")
        # .txt files don't match *.py pattern, so no change.
        assert entry.changed_paths() == []

    def test_matches_file(self, tmp_path):
        f = tmp_path / "match.txt"
        f.write_text("x")
        entry = _WatchEntry(str(f), "*", lambda p: None, is_dir=False)
        assert entry.matches(str(f)) is True
        assert entry.matches(str(tmp_path / "other.txt")) is False

    def test_matches_dir(self, tmp_path):
        (tmp_path / "a.py").write_text("1")
        entry = _WatchEntry(str(tmp_path), "*.py", lambda p: None, is_dir=True)
        assert entry.matches(str(tmp_path / "a.py")) is True
        assert entry.matches(str(tmp_path / "b.txt")) is False
        # File in a different directory.
        other = tmp_path / "sub"
        other.mkdir()
        (other / "a.py").write_text("1")
        assert entry.matches(str(other / "a.py")) is False

    def test_matches_dir_relative_path(self, tmp_path):
        (tmp_path / "rel.py").write_text("1")
        entry = _WatchEntry(str(tmp_path), "*.py", lambda p: None, is_dir=True)
        cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            assert entry.matches("rel.py") is True
            assert entry.matches("nope.txt") is False
        finally:
            os.chdir(cwd)


# ---------------------------------------------------------------------------
# _WatchdogHandler
# ---------------------------------------------------------------------------


class TestWatchdogHandler:
    def test_dispatch_directory_event_skipped(self):
        calls = []

        class FakeManager:
            _entries = []
            _schedule_call = lambda self, path, handler: calls.append((path, handler))

        handler = _WatchdogHandler(FakeManager())
        handler.dispatch(_DummyEvent("/some/dir", is_directory=True))
        assert calls == []

    def test_dispatch_file_event_matching(self, tmp_path):
        f = tmp_path / "evt.py"
        f.write_text("x")
        entry = _WatchEntry(str(f), "*", None, is_dir=False)
        calls = []

        class FakeManager:
            _entries = [entry]

            def _schedule_call(self, path, h):
                calls.append(path)

        handler = _WatchdogHandler(FakeManager())
        handler.dispatch(_DummyEvent(str(f), is_directory=False))
        assert len(calls) == 1
        assert os.path.abspath(calls[0]) == os.path.abspath(str(f))

    def test_dispatch_file_event_not_matching(self, tmp_path):
        f = tmp_path / "evt.py"
        f.write_text("x")
        entry = _WatchEntry(str(f), "*", None, is_dir=False)
        calls = []

        class FakeManager:
            _entries = [entry]

            def _schedule_call(self, path, h):
                calls.append(path)

        handler = _WatchdogHandler(FakeManager())
        handler.dispatch(_DummyEvent(str(tmp_path / "other.py"), is_directory=False))
        assert calls == []

    def test_dispatch_dir_entry_matching(self, tmp_path):
        (tmp_path / "m.py").write_text("x")
        entry = _WatchEntry(str(tmp_path), "*.py", None, is_dir=True)
        calls = []

        class FakeManager:
            _entries = [entry]

            def _schedule_call(self, path, h):
                calls.append(path)

        handler = _WatchdogHandler(FakeManager())
        handler.dispatch(_DummyEvent(str(tmp_path / "m.py"), is_directory=False))
        assert len(calls) == 1


# ---------------------------------------------------------------------------
# HotReloadManager — public API
# ---------------------------------------------------------------------------


class TestHotReloadManagerPublic:
    def test_watch_file(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        f = tmp_path / "w.txt"
        f.write_text("x")
        app.hot_reload.watch_file(str(f), lambda p: None)
        assert len(app.hot_reload.list_watched()) == 1
        assert os.path.abspath(str(f)) in app.hot_reload.list_watched()

    def test_watch_directory(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        app.hot_reload.watch_directory(str(tmp_path), "*.py", lambda p: None)
        assert len(app.hot_reload.list_watched()) == 1
        assert os.path.abspath(str(tmp_path)) in app.hot_reload.list_watched()

    def test_list_watched_empty(self, mock_mqtt):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        assert app.hot_reload.list_watched() == []

    def test_is_enabled_default_false(self, mock_mqtt):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        assert app.hot_reload.is_enabled is False

    @pytest.mark.asyncio
    async def test_enable_disable(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        f = tmp_path / "e.txt"
        f.write_text("x")
        app.hot_reload.watch_file(str(f), lambda p: None)
        app.hot_reload.enable()
        assert app.hot_reload.is_enabled is True
        await app.hot_reload.disable()
        assert app.hot_reload.is_enabled is False

    @pytest.mark.asyncio
    async def test_enable_idempotent(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        f = tmp_path / "i.txt"
        f.write_text("x")
        app.hot_reload.watch_file(str(f), lambda p: None)
        app.hot_reload.enable()
        task1 = app.hot_reload._task
        app.hot_reload.enable()  # second call should be no-op
        assert app.hot_reload._task is task1
        await app.hot_reload.disable()

    @pytest.mark.asyncio
    async def test_enable_no_entries(self, mock_mqtt):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        app.hot_reload.enable()
        assert app.hot_reload.is_enabled is True
        # Polling should still start (with empty entries, nothing happens).
        assert app.hot_reload._task is not None
        await app.hot_reload.disable()

    @pytest.mark.asyncio
    async def test_disable_without_enable(self, mock_mqtt):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        await app.hot_reload.disable()
        assert app.hot_reload.is_enabled is False

    @pytest.mark.asyncio
    async def test_disable_cancels_task(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        f = tmp_path / "c.txt"
        f.write_text("x")
        app.hot_reload.watch_file(str(f), lambda p: None)
        app.hot_reload.enable()
        task = app.hot_reload._task
        assert task is not None
        await app.hot_reload.disable()
        assert app.hot_reload._task is None
        assert task.cancelled() or task.done()

    @pytest.mark.asyncio
    async def test_disable_awaits_handler_tasks(self, mock_mqtt, tmp_path):
        """disable() should await in-flight handler tasks and cancel pending."""
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        mgr = HotReloadManager(app, poll_interval=1.0, debounce=0.01)
        mgr.enable()

        handler_done = []

        async def slow_handler(path):
            await asyncio.sleep(0.05)
            handler_done.append(path)

        # Schedule a handler task directly.
        mgr._loop = asyncio.get_running_loop()
        task = mgr._loop.create_task(mgr._invoke_handler(slow_handler, "/slow.py"))
        mgr._handler_tasks.add(task)
        task.add_done_callback(mgr._handler_tasks.discard)

        # Also add a pending debounced call to verify it gets cancelled.
        pending_handle = mgr._loop.call_later(10.0, lambda: None)
        mgr._pending["/pending.py"] = pending_handle

        await mgr.disable()
        assert handler_done == ["/slow.py"]
        assert len(mgr._handler_tasks) == 0
        assert len(mgr._pending) == 0


# ---------------------------------------------------------------------------
# HotReloadManager — polling loop
# ---------------------------------------------------------------------------


class TestPollLoop:
    @pytest.mark.asyncio
    async def test_poll_detects_file_change(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        f = tmp_path / "poll.txt"
        f.write_text("init")
        calls = []

        def handler(path):
            calls.append(path)

        mgr = HotReloadManager(app, poll_interval=0.05, debounce=0.01)
        mgr.watch_file(str(f), handler)
        mgr.enable()
        try:
            await asyncio.sleep(0.08)
            _touch(str(f), "updated")
            # Wait for poll + debounce + handler.
            await asyncio.sleep(0.25)
        finally:
            await mgr.disable()
        assert len(calls) >= 1
        assert os.path.abspath(calls[0]) == os.path.abspath(str(f))

    @pytest.mark.asyncio
    async def test_poll_detects_dir_change(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        calls = []

        def handler(path):
            calls.append(path)

        mgr = HotReloadManager(app, poll_interval=0.05, debounce=0.01)
        mgr.watch_directory(str(tmp_path), "*.py", handler)
        mgr.enable()
        try:
            new_file = tmp_path / "new_module.py"
            new_file.write_text("print(1)")
            await asyncio.sleep(0.25)
        finally:
            await mgr.disable()
        assert len(calls) >= 1
        assert any(os.path.abspath(c) == os.path.abspath(str(new_file)) for c in calls)


# ---------------------------------------------------------------------------
# HotReloadManager — scheduling
# ---------------------------------------------------------------------------


class TestScheduling:
    @pytest.mark.asyncio
    async def test_schedule_call_same_loop(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        mgr = HotReloadManager(app, poll_interval=1.0, debounce=0.01)
        mgr.enable()
        calls = []

        def handler(path):
            calls.append(path)

        try:
            mgr._schedule_call("/test/same_loop.py", handler)
            await asyncio.sleep(0.1)
        finally:
            await mgr.disable()
        assert calls == ["/test/same_loop.py"]

    @pytest.mark.asyncio
    async def test_schedule_call_different_thread(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        mgr = HotReloadManager(app, poll_interval=1.0, debounce=0.01)
        mgr.enable()
        calls = []
        call_event = threading.Event()

        def handler(path):
            calls.append(path)
            call_event.set()

        try:

            def thread_fn():
                mgr._schedule_call("/test/thread.py", handler)

            t = threading.Thread(target=thread_fn)
            t.start()
            t.join()
            # Wait for the thread-safe call to be processed.
            await asyncio.sleep(0.15)
        finally:
            await mgr.disable()
        assert calls == ["/test/thread.py"]

    @pytest.mark.asyncio
    async def test_schedule_call_no_loop(self, mock_mqtt):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        mgr = HotReloadManager(app, poll_interval=1.0, debounce=0.01)
        mgr._loop = None
        # Should not raise, just log.
        mgr._schedule_call("/no_loop.py", lambda p: None)

    @pytest.mark.asyncio
    async def test_schedule_call_debounce(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        mgr = HotReloadManager(app, poll_interval=1.0, debounce=0.1)
        mgr.enable()
        calls = []

        def handler(path):
            calls.append(path)

        try:
            # Schedule multiple times rapidly; debounce should coalesce.
            mgr._schedule_call("/debounce.py", handler)
            mgr._schedule_call("/debounce.py", handler)
            mgr._schedule_call("/debounce.py", handler)
            await asyncio.sleep(0.2)
        finally:
            await mgr.disable()
        # Only one call should fire after debounce.
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_schedule_call_in_loop_directly(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        mgr = HotReloadManager(app, poll_interval=1.0, debounce=0.01)
        mgr.enable()
        calls = []

        def handler(path):
            calls.append(path)

        try:
            mgr._schedule_call_in_loop("/direct.py", handler)
            await asyncio.sleep(0.1)
        finally:
            await mgr.disable()
        assert calls == ["/direct.py"]

    @pytest.mark.asyncio
    async def test_schedule_call_in_loop_no_loop(self, mock_mqtt):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        mgr = HotReloadManager(app, poll_interval=1.0, debounce=0.01)
        mgr._loop = None
        # Should not raise.
        mgr._schedule_call_in_loop("/nope.py", lambda p: None)


# ---------------------------------------------------------------------------
# HotReloadManager — _invoke_handler
# ---------------------------------------------------------------------------


class TestInvokeHandler:
    @pytest.mark.asyncio
    async def test_invoke_sync_handler(self, mock_mqtt):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        mgr = HotReloadManager(app)
        calls = []

        def sync_handler(path):
            calls.append(path)

        await mgr._invoke_handler(sync_handler, "/sync.py")
        assert calls == ["/sync.py"]

    @pytest.mark.asyncio
    async def test_invoke_async_handler(self, mock_mqtt):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        mgr = HotReloadManager(app)
        calls = []

        async def async_handler(path):
            calls.append(path)

        await mgr._invoke_handler(async_handler, "/async.py")
        assert calls == ["/async.py"]

    @pytest.mark.asyncio
    async def test_invoke_handler_error_publishes_event(self, mock_mqtt):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        mgr = HotReloadManager(app)
        errors = []

        app.event_bus.subscribe("hot_reload_error", lambda data: errors.append(data))

        def bad_handler(path):
            raise ValueError("boom")

        await mgr._invoke_handler(bad_handler, "/bad.py")
        assert len(errors) == 1
        assert errors[0]["file_path"] == "/bad.py"
        assert "boom" in errors[0]["error"]

    @pytest.mark.asyncio
    async def test_invoke_async_handler_error(self, mock_mqtt):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        mgr = HotReloadManager(app)
        errors = []

        app.event_bus.subscribe("hot_reload_error", lambda data: errors.append(data))

        async def bad_async(path):
            raise RuntimeError("async boom")

        await mgr._invoke_handler(bad_async, "/bad_async.py")
        assert len(errors) == 1
        assert errors[0]["file_path"] == "/bad_async.py"


# ---------------------------------------------------------------------------
# HotReloadManager — built-in handler factories
# ---------------------------------------------------------------------------


class TestHandlerFactories:
    @pytest.mark.asyncio
    async def test_make_rules_handler(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        mgr = HotReloadManager(app)
        handler = mgr.make_rules_handler()
        assert asyncio.iscoroutinefunction(handler)

    @pytest.mark.asyncio
    async def test_make_devices_handler(self, mock_mqtt):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        mgr = HotReloadManager(app)
        handler = mgr.make_devices_handler()
        assert asyncio.iscoroutinefunction(handler)

    @pytest.mark.asyncio
    async def test_make_config_handler(self, mock_mqtt):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        mgr = HotReloadManager(app)
        handler = mgr.make_config_handler()
        assert asyncio.iscoroutinefunction(handler)

    @pytest.mark.asyncio
    async def test_make_handler_invokes_reload(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        mgr = HotReloadManager(app)
        handler = mgr.make_config_handler()

        cfg = tmp_path / "cfg.json"
        cfg.write_text(json.dumps({"key": "val"}))

        events = []
        app.event_bus.subscribe("hot_reload_config", lambda data: events.append(data))

        await handler(str(cfg))
        assert len(events) == 1
        assert events[0]["config"] == {"key": "val"}


# ---------------------------------------------------------------------------
# reload_rules_from_file
# ---------------------------------------------------------------------------


def _rules_file_content(func_name: str = "my_rule", use_correct_kwargs: bool = True) -> str:
    """Generate a temp .py module with a rule function carrying _Kamio_rule_kwargs."""
    if use_correct_kwargs:
        kwargs_line = (
            f'{func_name}._Kamio_rule_kwargs = {{"device_class": None, '
            f'"interval": None, "fields": ["power"], "enabled": True, '
            f'"run_on_start": False, "description": "test rule"}}'
        )
    else:
        # Wrong key "device" — will cause TypeError in Rule(**kwargs).
        kwargs_line = (
            f'{func_name}._Kamio_rule_kwargs = {{"device": None, '
            f'"interval": None, "fields": None, "enabled": True, '
            f'"run_on_start": False, "description": "bad rule"}}'
        )
    return "async def " + func_name + "(event, app):\n" "    pass\n" + kwargs_line + "\n"


class TestReloadRules:
    @pytest.mark.asyncio
    async def test_reload_rules_replaces_matching(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")

        # Register an initial rule named my_rule.
        @app.rule(fields=["power"])
        async def my_rule(event, app):
            pass

        assert len(app.rules.rules) == 1
        old_func = app.rules.rules[0].func

        # Write a new module with a function of the same name.
        mod_file = tmp_path / "rules_mod.py"
        mod_file.write_text(_rules_file_content("my_rule"))

        events = []
        app.event_bus.subscribe("hot_reload_rules", lambda data: events.append(data))

        result = await reload_rules_from_file(str(mod_file), app)
        assert result is True
        assert len(events) == 1
        assert events[0]["replaced"] == 1
        # The rule function should have been replaced.
        assert app.rules.rules[0].func is not old_func
        assert app.rules.rules[0].func.__name__ == "my_rule"

    @pytest.mark.asyncio
    async def test_reload_rules_no_rule_funcs(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        mod_file = tmp_path / "empty.py"
        mod_file.write_text("x = 1\n")
        result = await reload_rules_from_file(str(mod_file), app)
        assert result is True

    @pytest.mark.asyncio
    async def test_reload_rules_no_matching_name(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")

        @app.rule(fields=["power"])
        async def existing_rule(event, app):
            pass

        mod_file = tmp_path / "new_rules.py"
        mod_file.write_text(_rules_file_content("different_rule"))

        events = []
        app.event_bus.subscribe("hot_reload_rules", lambda data: events.append(data))

        result = await reload_rules_from_file(str(mod_file), app)
        assert result is True
        assert events[0]["replaced"] == 0
        # Original rule unchanged.
        assert len(app.rules.rules) == 1

    @pytest.mark.asyncio
    async def test_reload_rules_error_rollback(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")

        @app.rule(fields=["power"])
        async def my_rule(event, app):
            pass

        old_rules = list(app.rules.rules)
        mod_file = tmp_path / "bad_rules.py"
        mod_file.write_text(_rules_file_content("my_rule", use_correct_kwargs=False))

        errors = []
        app.event_bus.subscribe("hot_reload_error", lambda data: errors.append(data))

        result = await reload_rules_from_file(str(mod_file), app)
        assert result is False
        assert len(errors) == 1
        # Rules should be rolled back.
        assert app.rules.rules == old_rules

    @pytest.mark.asyncio
    async def test_reload_rules_error_running_app(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")

        @app.rule(fields=["power"])
        async def my_rule(event, app):
            pass

        app._is_running = True
        mod_file = tmp_path / "bad_running.py"
        mod_file.write_text(_rules_file_content("my_rule", use_correct_kwargs=False))

        errors = []
        app.event_bus.subscribe("hot_reload_error", lambda data: errors.append(data))

        result = await reload_rules_from_file(str(mod_file), app)
        assert result is False
        assert len(errors) == 1
        app._is_running = False


# ---------------------------------------------------------------------------
# reload_devices_from_file
# ---------------------------------------------------------------------------


_DEVICES_FILE = """\
from kamio import Device, state

class ReloadedDevice(Device):
    power: bool = state(default=False, writable=True)
"""


class TestReloadDevices:
    @pytest.mark.asyncio
    async def test_reload_devices_success(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        mod_file = tmp_path / "dev_mod.py"
        mod_file.write_text(_DEVICES_FILE)

        events = []
        app.event_bus.subscribe("hot_reload_devices", lambda data: events.append(data))

        result = await reload_devices_from_file(str(mod_file), app)
        assert result is True
        assert len(events) == 1
        assert events[0]["updated_classes"] == 1
        assert app.registry.get_class("reloadeddevice") is not None

    @pytest.mark.asyncio
    async def test_reload_devices_no_classes(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        mod_file = tmp_path / "no_devs.py"
        mod_file.write_text("x = 1\n")

        events = []
        app.event_bus.subscribe("hot_reload_devices", lambda data: events.append(data))

        result = await reload_devices_from_file(str(mod_file), app)
        assert result is True
        assert events[0]["updated_classes"] == 0

    @pytest.mark.asyncio
    async def test_reload_devices_error(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        mod_file = tmp_path / "bad_dev.py"
        mod_file.write_text("raise RuntimeError('import fail')\n")

        errors = []
        app.event_bus.subscribe("hot_reload_error", lambda data: errors.append(data))

        result = await reload_devices_from_file(str(mod_file), app)
        assert result is False
        assert len(errors) == 1
        assert errors[0]["file_path"] == str(mod_file)

    @pytest.mark.asyncio
    async def test_reload_devices_rolls_back_on_register_error(self, mock_mqtt, tmp_path):
        """When registration fails after a successful import, the registry and
        rule engine must be restored to their pre-reload state.
        """
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        # Pre-register an existing device class so rollback has something to restore.
        from kamio import Device, state

        class ExistingDevice(Device):
            power: bool = state(default=False, writable=True)

        app.register(ExistingDevice)
        old_classes = dict(app.registry.classes)

        mod_file = tmp_path / "dev_rollback.py"
        mod_file.write_text(_DEVICES_FILE)

        # Force registry.register_class to fail after the module imports
        # successfully, exercising the rollback branch.
        original_register_class = app.registry.register_class

        def _failing_register_class(cls):
            raise RuntimeError("register boom")

        app.registry.register_class = _failing_register_class  # type: ignore[assignment]
        try:
            result = await reload_devices_from_file(str(mod_file), app)
        finally:
            app.registry.register_class = original_register_class  # type: ignore[assignment]

        assert result is False
        # Rollback should have restored the original class set.
        assert app.registry.classes == old_classes


# ---------------------------------------------------------------------------
# reload_config_from_file
# ---------------------------------------------------------------------------


class TestReloadConfig:
    @pytest.mark.asyncio
    async def test_reload_config_json(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        cfg = tmp_path / "config.json"
        cfg.write_text(json.dumps({"foo": "bar", "num": 42}))

        events = []
        app.event_bus.subscribe("hot_reload_config", lambda data: events.append(data))

        result = await reload_config_from_file(str(cfg), app)
        assert result is True
        assert len(events) == 1
        assert events[0]["config"] == {"foo": "bar", "num": 42}

    @pytest.mark.asyncio
    async def test_reload_config_yaml(self, mock_mqtt, tmp_path):
        pytest.importorskip("yaml")
        import yaml as _yaml

        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        cfg = tmp_path / "config.yaml"
        cfg.write_text(_yaml.dump({"foo": "bar", "list": [1, 2]}))

        events = []
        app.event_bus.subscribe("hot_reload_config", lambda data: events.append(data))

        result = await reload_config_from_file(str(cfg), app)
        assert result is True
        assert events[0]["config"] == {"foo": "bar", "list": [1, 2]}

    @pytest.mark.asyncio
    async def test_reload_config_error(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        cfg = tmp_path / "bad.json"
        cfg.write_text("{invalid json}")

        errors = []
        app.event_bus.subscribe("hot_reload_error", lambda data: errors.append(data))

        result = await reload_config_from_file(str(cfg), app)
        assert result is False
        assert len(errors) == 1


# ---------------------------------------------------------------------------
# _publish_reload_error
# ---------------------------------------------------------------------------


class TestPublishReloadError:
    @pytest.mark.asyncio
    async def test_publish_reload_error(self, mock_mqtt):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        errors = []
        app.event_bus.subscribe("hot_reload_error", lambda data: errors.append(data))

        await _publish_reload_error(app, "/some/path.py", ValueError("test error"))
        assert len(errors) == 1
        assert errors[0]["file_path"] == "/some/path.py"
        assert errors[0]["error"] == "test error"


# ---------------------------------------------------------------------------
# _load_module_from_file
# ---------------------------------------------------------------------------


class TestLoadModuleFromFile:
    def test_load_new_module(self, tmp_path):
        mod_file = tmp_path / "new_mod.py"
        mod_file.write_text("VALUE = 42\n")
        mod = _load_module_from_file(str(mod_file))
        assert mod.VALUE == 42
        # Should be registered in sys.modules.
        assert "new_mod" in sys.modules

    def test_load_module_reload(self, tmp_path):
        mod_file = tmp_path / "reload_mod.py"
        mod_file.write_text("VALUE = 1\n")
        # Add temp dir to sys.path so importlib.reload can find the spec by name.
        sys.path.insert(0, str(tmp_path))
        try:
            mod1 = _load_module_from_file(str(mod_file))
            assert mod1.VALUE == 1

            # Modify and reload.
            mod_file.write_text("VALUE = 2\n")
            future = time.time() + 10
            os.utime(str(mod_file), (future, future))
            mod2 = _load_module_from_file(str(mod_file))
            assert mod2.VALUE == 2
            # Should be the same module object (reloaded).
            assert mod2 is mod1
        finally:
            sys.path.remove(str(tmp_path))

    def test_load_module_invalid_path(self, tmp_path):
        # A directory with no __init__.py won't have a valid spec loader.
        mod_file = tmp_path / "nonexistent_dir_for_spec"
        mod_file.mkdir()
        with pytest.raises(ImportError):
            _load_module_from_file(str(mod_file))


# ---------------------------------------------------------------------------
# _find_rule_funcs
# ---------------------------------------------------------------------------


class TestFindRuleFuncs:
    def test_find_rule_funcs_with_marker(self):
        class FakeModule:
            pass

        mod = FakeModule()

        async def my_rule(event, app):
            pass

        my_rule._Kamio_rule_kwargs = {"device_class": None}
        mod.my_rule = my_rule

        async def not_a_rule(event, app):
            pass

        mod.not_a_rule = not_a_rule

        mod.not_callable = 42

        results = _find_rule_funcs(mod)
        assert len(results) == 1
        assert results[0][0] is my_rule
        assert results[0][1] == {"device_class": None}

    def test_find_rule_funcs_empty(self):
        class FakeModule:
            pass

        mod = FakeModule()
        assert _find_rule_funcs(mod) == []


# ---------------------------------------------------------------------------
# _load_config_file
# ---------------------------------------------------------------------------


class TestLoadConfigFile:
    def test_load_json(self, tmp_path):
        cfg = tmp_path / "data.json"
        cfg.write_text(json.dumps({"a": 1, "b": [2, 3]}))
        result = _load_config_file(str(cfg))
        assert result == {"a": 1, "b": [2, 3]}

    def test_load_yaml(self, tmp_path):
        pytest.importorskip("yaml")
        import yaml as _yaml

        cfg = tmp_path / "data.yaml"
        cfg.write_text(_yaml.dump({"x": "y"}))
        result = _load_config_file(str(cfg))
        assert result == {"x": "y"}

    def test_load_yml_extension(self, tmp_path):
        pytest.importorskip("yaml")
        import yaml as _yaml

        cfg = tmp_path / "data.yml"
        cfg.write_text(_yaml.dump({"x": 1}))
        result = _load_config_file(str(cfg))
        assert result == {"x": 1}

    def test_load_yaml_empty_file(self, tmp_path):
        pytest.importorskip("yaml")

        cfg = tmp_path / "empty.yaml"
        cfg.write_text("")
        result = _load_config_file(str(cfg))
        assert result == {}

    def test_load_json_invalid(self, tmp_path):
        cfg = tmp_path / "bad.json"
        cfg.write_text("{not valid}")
        with pytest.raises(json.JSONDecodeError):
            _load_config_file(str(cfg))


# ---------------------------------------------------------------------------
# Integration: full enable → change → handler flow
# ---------------------------------------------------------------------------


class TestIntegration:
    @pytest.mark.asyncio
    async def test_full_flow_file_watch(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        f = tmp_path / "integration.txt"
        f.write_text("start")
        calls = []

        def handler(path):
            calls.append(path)

        mgr = HotReloadManager(app, poll_interval=0.05, debounce=0.02)
        mgr.watch_file(str(f), handler)
        mgr.enable()
        try:
            await asyncio.sleep(0.08)
            _touch(str(f), "changed")
            await asyncio.sleep(0.25)
        finally:
            await mgr.disable()
        assert len(calls) >= 1

    @pytest.mark.asyncio
    async def test_full_flow_async_handler(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        f = tmp_path / "async_handler.txt"
        f.write_text("start")
        calls = []

        async def handler(path):
            calls.append(path)

        mgr = HotReloadManager(app, poll_interval=0.05, debounce=0.02)
        mgr.watch_file(str(f), handler)
        mgr.enable()
        try:
            await asyncio.sleep(0.08)
            _touch(str(f), "changed")
            await asyncio.sleep(0.25)
        finally:
            await mgr.disable()
        assert len(calls) >= 1

    @pytest.mark.asyncio
    async def test_full_flow_config_handler(self, mock_mqtt, tmp_path):
        app = KamioApp(mqtt_broker=mock_mqtt, client_id="test")
        cfg = tmp_path / "integration_cfg.json"
        cfg.write_text(json.dumps({"v": 1}))

        events = []
        app.event_bus.subscribe("hot_reload_config", lambda data: events.append(data))

        mgr = HotReloadManager(app, poll_interval=0.05, debounce=0.02)
        mgr.watch_file(str(cfg), mgr.make_config_handler())
        mgr.enable()
        try:
            await asyncio.sleep(0.08)
            cfg.write_text(json.dumps({"v": 2}))
            future = time.time() + 10
            os.utime(str(cfg), (future, future))
            await asyncio.sleep(0.3)
        finally:
            await mgr.disable()
        assert len(events) >= 1
        assert events[-1]["config"] == {"v": 2}
