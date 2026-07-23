from __future__ import annotations
"""
Backward-compatibility shim.

All mixin classes have been moved to ``Kamio/app/mixins/``.
This module re-exports them so that any existing code importing from
``Kamio.app_mixins`` continues to work without changes.
"""
from kamio.app.mixins.lifecycle import LifecycleMixin
from kamio.app.mixins.mqtt import MqttDispatchMixin
from kamio.app.mixins.hooks_events import HookEventFacadeMixin
from kamio.app.mixins.plugins import PluginFacadeMixin
from kamio.app.mixins.hot_reload import HotReloadFacadeMixin
from kamio.app.mixins.custom_nodes import CustomNodeFacadeMixin
from kamio.app.mixins.rules import RuleRegistryMixin
from kamio.app.mixins.devices import DeviceRegistryMixin

__all__ = [
    "LifecycleMixin",
    "MqttDispatchMixin",
    "HookEventFacadeMixin",
    "PluginFacadeMixin",
    "HotReloadFacadeMixin",
    "CustomNodeFacadeMixin",
    "RuleRegistryMixin",
    "DeviceRegistryMixin",
]
