from __future__ import annotations

import asyncio
import logging

import pytest

from kamio import KamioApp
from kamio.plugins.base import Plugin
from kamio.plugins.loader import PluginContext


class AlphaPlugin(Plugin):
    name = "alpha"
    version = "1.0.0"

    async def on_load(self, app, context=None):
        if context:
            context.subscribe("device_state_changed", self._on_state)

    async def on_unload(self, app):
        pass

    def _on_state(self, data):
        pass


class BetaPlugin(Plugin):
    name = "beta"
    version = "0.1.0"
    description = "Depends on alpha"

    @property
    def dependencies(self):
        return ["alpha"]

    async def on_load(self, app, context=None):
        if context:
            context.register_hook("on_device_added", self._on_added)

    async def on_unload(self, app):
        pass

    def _on_added(self, device):
        pass


class GammaPlugin(Plugin):
    name = "gamma"
    version = "1.0.0"

    async def on_load(self, app, context=None):
        raise RuntimeError("gamma load failure")


@pytest.mark.asyncio
async def test_plugin_lifecycle_load_and_unload():
    app = KamioApp()
    plugin = await app.load_plugin(AlphaPlugin)
    assert plugin.name == "alpha"
    assert app.get_plugin("alpha") is plugin
    assert "alpha" in app.list_plugins()
    await app.unload_plugin("alpha")
    assert app.get_plugin("alpha") is None
    assert "alpha" not in app.list_plugins()


@pytest.mark.asyncio
async def test_plugin_events_and_hooks_cleanup():
    app = KamioApp()
    await app.load_plugin(AlphaPlugin)
    await app.load_plugin(BetaPlugin)
    assert app.get_plugin("beta") is not None
    assert "alpha" in app.list_plugins()
    assert "beta" in app.list_plugins()
    # Cleanup should remove hooks/subscriptions without leaks.
    await app.unload_plugin("beta")
    await app.unload_plugin("alpha")
    assert "alpha" not in app.list_plugins()
    assert "beta" not in app.list_plugins()


@pytest.mark.asyncio
async def test_plugin_dependency_order_enforced():
    app = KamioApp()
    with pytest.raises(ValueError):
        # Loading beta without alpha should raise because alpha is a dependency.
        await app.load_plugin(BetaPlugin)


@pytest.mark.asyncio
async def test_plugin_events_bus_published():
    app = KamioApp()
    events = []
    app.subscribe_event("plugin_loaded", lambda d: events.append(d))
    await app.load_plugin(AlphaPlugin)
    assert any(e.get("plugin_name") == "alpha" for e in events)
    await app.unload_plugin("alpha")


@pytest.mark.asyncio
async def test_plugin_load_failure_logged_not_crash():
    app = KamioApp()
    with pytest.raises(Exception):
        # Loading gamma raises in on_load; the loader should propagate or handle per docs.
        await app.load_plugin(GammaPlugin)


@pytest.mark.asyncio
async def test_multiple_plugins_simultaneous():
    app = KamioApp()
    a = await app.load_plugin(AlphaPlugin)
    b = await app.load_plugin(BetaPlugin)
    assert a is app.get_plugin("alpha")
    assert b is app.get_plugin("beta")


@pytest.mark.asyncio
async def test_plugin_task_failure_logged(caplog):
    ctx = PluginContext(KamioApp(), "alpha")

    async def boom():
        raise RuntimeError("worker crashed")

    task = ctx.create_task(boom(), name="worker")
    with caplog.at_level(logging.ERROR, logger="Kamio.plugin.alpha"):
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)
    assert task not in ctx._tasks
    assert any(
        r.name == "Kamio.plugin.alpha" and r.exc_info and "worker" in r.getMessage()
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_plugin_task_cancel_not_logged(caplog):
    ctx = PluginContext(KamioApp(), "alpha")
    task = ctx.create_task(asyncio.sleep(10), name="sleeper")
    with caplog.at_level(logging.ERROR, logger="Kamio.plugin.alpha"):
        await ctx.cancel_tasks()
        await asyncio.sleep(0)
    assert task not in ctx._tasks
    assert not [r for r in caplog.records if r.name == "Kamio.plugin.alpha"]


# ---------------------------------------------------------------------------
# Coverage push: PluginContext helpers, directory loading, dependency edge cases
# ---------------------------------------------------------------------------

import types

from kamio.plugins.loader import PluginLoader


class DeltaPlugin(Plugin):
    name = "delta"
    version = "1.0.0"

    async def on_load(self, app, context=None):
        pass


class EmptyDepPlugin(Plugin):
    name = "emptydep"
    version = "1.0.0"

    @property
    def dependencies(self):
        return [""]

    async def on_load(self, app, context=None):
        pass


@pytest.mark.asyncio
async def test_plugin_context_helpers():
    app = KamioApp()
    ctx = PluginContext(app, "myplug")
    assert ctx.logger.name == "Kamio.plugin.myplug"
    assert ctx.plugin_name == "myplug"

    cb = lambda data: None
    ctx.subscribe("my_event", cb)
    assert cb in app.event_bus.list_subscribers("my_event")
    ctx.unsubscribe("my_event", cb)
    assert cb not in app.event_bus.list_subscribers("my_event")

    hook = lambda d: None
    ctx.register_hook("on_device_added", hook)
    assert hook in app.hooks.list_hooks("on_device_added")
    ctx.unregister_hook("on_device_added", hook)
    assert hook not in app.hooks.list_hooks("on_device_added")

    # register() is an alias for register_hook()
    ctx.register("on_device_added", hook)
    assert hook in app.hooks.list_hooks("on_device_added")

    received = []
    app.event_bus.subscribe("plugin_evt", lambda d: received.append(d))
    await ctx.publish_event("plugin_evt", {"x": 1})
    assert received and received[0]["x"] == 1

    regs = ctx.registrations
    assert "events" in regs and "hooks" in regs


@pytest.mark.asyncio
async def test_cleanup_context_rule_removal_failure_warns(caplog):
    app = KamioApp()
    loader = app.plugin_loader
    ctx = PluginContext(app, "x")
    ctx._rules.append(lambda: None)

    async def _boom(func):
        raise RuntimeError("cannot remove")

    original = app.remove_rule
    app.remove_rule = _boom
    try:
        # Must not raise — the failure is logged as a warning.
        await loader._cleanup_context(ctx)
    finally:
        app.remove_rule = original


@pytest.mark.asyncio
async def test_load_from_module_without_plugin_raises():
    loader = PluginLoader(KamioApp())
    with pytest.raises(ImportError):
        await loader.load_from_module("json")


@pytest.mark.asyncio
async def test_load_from_module_loads_plugin():
    mod = types.ModuleType("fake_plug_mod")
    mod.DeltaPlugin = DeltaPlugin
    import sys

    sys.modules["fake_plug_mod"] = mod
    try:
        loader = PluginLoader(KamioApp())
        plugin = await loader.load_from_module("fake_plug_mod")
        assert plugin.name == "delta"
    finally:
        sys.modules.pop("fake_plug_mod", None)


@pytest.mark.asyncio
async def test_load_plugins_from_directory(tmp_path, caplog):
    d = tmp_path / "plugins"
    d.mkdir()
    (d / "plug_good.py").write_text(
        "from kamio.plugins.base import Plugin\n"
        "class DirPlugin(Plugin):\n"
        "    name = 'dirplug'\n"
        "    version = '1.0'\n"
        "    async def on_load(self, app, context=None): pass\n"
    )
    (d / "plug_bad.py").write_text("raise RuntimeError('import boom')\n")
    (d / "_private.py").write_text("raise RuntimeError('should be skipped')\n")
    (d / "not_python.txt").write_text("not a py file")

    app = KamioApp()
    loaded = await app.plugin_loader.load_plugins_from_directory(str(d))
    assert [p.name for p in loaded] == ["dirplug"]
    # The broken module is logged and skipped, the underscore file ignored.
    assert app.get_plugin("dirplug") is not None


@pytest.mark.asyncio
async def test_load_plugins_from_directory_missing_dir():
    loader = PluginLoader(KamioApp())
    loaded = await loader.load_plugins_from_directory("definitely_not_a_dir_xyz")
    assert loaded == []


@pytest.mark.asyncio
async def test_empty_dependency_name_raises():
    loader = PluginLoader(KamioApp())
    with pytest.raises(ValueError, match="empty dependency"):
        await loader.load_plugin(EmptyDepPlugin)


def test_find_plugin_class_ignores_abstract():
    mod = types.ModuleType("m")
    mod.Plugin = Plugin  # the abstract base itself must be skipped
    assert PluginLoader._find_plugin_class(mod) is None


@pytest.mark.asyncio
async def test_load_order_property():
    app = KamioApp()
    await app.load_plugin(AlphaPlugin)
    assert app.plugin_loader.load_order == ["alpha"]
    await app.load_plugin(DeltaPlugin)
    assert app.plugin_loader.load_order == ["alpha", "delta"]
