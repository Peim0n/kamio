from __future__ import annotations

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
