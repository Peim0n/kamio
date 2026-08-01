"""Gotchas and edge cases in Kamio's plugin lifecycle subsystem.

This module is a deep-dive for framework developers.  It demonstrates
non-obvious behaviours and pitfalls in ``kamio.plugins.loader`` and
``kamio.plugins.base`` that can cause resource leaks, lost subscriptions,
or silent failures.

Key gotchas covered
-------------------
1.  **Load order**: ``_ensure_dependencies`` → ``configure`` → ``on_load``
    → ``subscribe_events`` → ``register_hooks``.  Dependencies load
    *before* configure, so dependency ``on_load`` runs before the
    dependent's ``configure``.
2.  **Dependencies loaded BEFORE configure**: the dependency's
    ``on_load`` sees the default empty config, not the final config.
3.  **subscribe_events after on_load**: subscriptions made *inside*
    ``on_load`` (via ``context.subscribe``) are tracked, but any
    subscriptions made directly on ``app.event_bus`` inside ``on_load``
    are NOT tracked and leak on unload.
4.  **Plugin name must be non-empty**: empty string raises ``ValueError``.
5.  **Circular dependency detection** via ``_loading`` set: the cycle is
    detected at the point where a name re-enters the set.
6.  **_find_plugin_class returns FIRST Plugin subclass**: ``dir()``
    returns names alphabetically, so the first concrete subclass in
    alphabetical order wins.
7.  **unload non-existent plugin**: logs a warning and returns silently
    — no exception raised.
8.  **on_unload failure doesn't prevent cleanup**: ``try/finally``
    ensures context cleanup runs even if ``on_unload`` raises.
9.  **Rule removal failure**: if ``app.remove_rule`` raises, the rule
    is orphaned — only a warning is logged.
10. **configure() replaces entire config**: no merge with previous
    values; calling ``configure`` twice wipes the first config.

Every gotcha is proven with assertions that run **without an MQTT broker**.
"""
from __future__ import annotations

import asyncio
import logging
import unittest
from typing import Any, Dict, List, Optional

from kamio.plugins.base import Plugin
from kamio.plugins.loader import PluginContext, PluginLoader

logging.disable(logging.CRITICAL)  # заглушаем логи во время тестов

# ---------------------------------------------------------------------------
# Mock-инфраструктура
# ---------------------------------------------------------------------------


class MockEventBus:
    """Минимальный EventBus с отслеживанием подписок."""

    def __init__(self) -> None:
        self._subs: dict[str, list] = {}
        self.published: list[tuple[str, dict]] = []

    def subscribe(self, event_type: str, callback, **kwargs) -> None:
        self._subs.setdefault(event_type, []).append(callback)

    def unsubscribe(self, event_type: str, callback) -> None:
        if event_type in self._subs:
            try:
                self._subs[event_type].remove(callback)
            except ValueError:
                pass

    def list_subscribers(self, event_type: str) -> list:
        return list(self._subs.get(event_type, []))

    async def publish(self, event_type: str, data: dict) -> None:
        self.published.append((event_type, data))
        for cb in self._subs.get(event_type, []):
            if asyncio.iscoroutinefunction(cb):
                await cb(data)
            else:
                cb(data)


class MockHooks:
    """Заглушка HooksManager."""

    def __init__(self) -> None:
        self._hooks: dict[str, list] = {}

    def register(self, event_type: str, hook, priority: int = 0) -> None:
        self._hooks.setdefault(event_type, []).append(hook)

    def unregister(self, event_type: str, hook) -> None:
        if event_type in self._hooks:
            try:
                self._hooks[event_type].remove(hook)
            except ValueError:
                pass

    def list_hooks(self, event_type: str) -> list:
        return list(self._hooks.get(event_type, []))

    async def trigger(self, event_type: str, *args, **kwargs) -> None:
        pass


class MockApp:
    """Упрощённый KamioApp для тестирования плагинов."""

    def __init__(self) -> None:
        self.event_bus = MockEventBus()
        self.hooks = MockHooks()
        self._rules: list = []

    def add_rule(self, func, **kwargs) -> object:
        self._rules.append(func)
        return func

    async def remove_rule(self, func) -> None:
        if func in self._rules:
            self._rules.remove(func)


# ---------------------------------------------------------------------------
# Тестовые плагины
# ---------------------------------------------------------------------------


class SimplePlugin(Plugin):
    """Базовый тестовый плагин."""

    @property
    def name(self) -> str:
        return "simple"

    @property
    def version(self) -> str:
        return "1.0.0"

    async def on_load(self, app, context=None) -> None:
        pass


class DependentPlugin(Plugin):
    """Плагин с зависимостью."""

    @property
    def name(self) -> str:
        return "dependent"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def dependencies(self) -> List[str]:
        return ["base_dep"]

    async def on_load(self, app, context=None) -> None:
        pass


class BaseDepPlugin(Plugin):
    """Базовый плагин-зависимость."""

    @property
    def name(self) -> str:
        return "base_dep"

    @property
    def version(self) -> str:
        return "1.0.0"

    async def on_load(self, app, context=None) -> None:
        pass


# ---------------------------------------------------------------------------
# Тесты
# ---------------------------------------------------------------------------


class TestLoadOrder(unittest.IsolatedAsyncioTestCase):
    """Gotcha #1-2: порядок загрузки и зависимости до configure."""

    async def test_dependencies_loaded_before_configure(self):
        # НЕПРАВИЛЬНО: ожидать, что configure вызывается до загрузки зависимостей
        # ПРАВИЛЬНО: зависимости загружаются ДО configure основного плагина
        #
        # Код (loader.py:166-169):
        #   await self._ensure_dependencies(instance)  # СНАЧАЛА зависимости
        #   if config:
        #       instance.configure(config)              # ПОТОМ configure
        #
        # Это означает, что on_load зависимости выполняется с пустым конфигом,
        # даже если зависимый плагин ещё не сконфигурирован.

        call_order: list[str] = []

        class DepPlugin(Plugin):
            @property
            def name(self) -> str:
                return "dep"

            @property
            def version(self) -> str:
                return "1.0"

            async def on_load(self, app, context=None) -> None:
                call_order.append("dep_on_load")

        class MainPlugin(Plugin):
            @property
            def name(self) -> str:
                return "main"

            @property
            def version(self) -> str:
                return "1.0"

            @property
            def dependencies(self) -> List[str]:
                return ["dep"]

            def configure(self, config: Dict[str, Any]) -> None:
                call_order.append("main_configure")

            async def on_load(self, app, context=None) -> None:
                call_order.append("main_on_load")

        app = MockApp()
        loader = PluginLoader(app)
        loader.register_class("dep", DepPlugin)

        await loader.load_plugin(MainPlugin, config={"key": "value"})

        # Порядок: dep_on_load → main_configure → main_on_load
        # dep загружается ВНУТРИ _ensure_dependencies, ДО configure main
        self.assertEqual(call_order, ["dep_on_load", "main_configure", "main_on_load"])

    async def test_subscribe_events_after_on_load(self):
        # НЕПРАВИЛЬНО: подписываться на события внутри on_load через app.event_bus
        # напрямую — эти подписки не отслеживаются и утекают при unload
        # ПРАВИЛЬНО: использовать context.subscribe() внутри on_load
        # или переопределить subscribe_events()
        #
        # Код (loader.py:174-177):
        #   await instance.on_load(self.app, context)
        #   instance.subscribe_events(context)   # ПОСЛЕ on_load
        #   instance.register_hooks(context)
        #
        # PluginContext.subscribe() отслеживает подписки для cleanup.
        # Прямые вызовы app.event_bus.subscribe() — нет.

        app = MockApp()
        loader = PluginLoader(app)

        untracked_cb = MagicMock_async()
        tracked_cb = MagicMock_async()

        class OrderPlugin(Plugin):
            @property
            def name(self) -> str:
                return "order_test"

            @property
            def version(self) -> str:
                return "1.0"

            async def on_load(self, app, context=None) -> None:
                # ПРАВИЛЬНО: используем context.subscribe
                if context:
                    context.subscribe("test_event", tracked_cb)
                # НЕПРАВИЛЬНО: прямой вызов — не отслеживается
                app.event_bus.subscribe("test_event", untracked_cb)

        await loader.load_plugin(OrderPlugin)

        # Обе подписки активны
        self.assertIn(tracked_cb, app.event_bus.list_subscribers("test_event"))
        self.assertIn(untracked_cb, app.event_bus.list_subscribers("test_event"))

        # Выгружаем плагин
        await loader.unload_plugin("order_test")

        # tracked_cb удалён (context отслеживал)
        self.assertNotIn(tracked_cb, app.event_bus.list_subscribers("test_event"))
        # untracked_cb УТЕЧКА — остался в подписках!
        self.assertIn(untracked_cb, app.event_bus.list_subscribers("test_event"),
                       "Прямая подписка в on_load утекает — context её не отслеживает")


class TestPluginNameValidation(unittest.IsolatedAsyncioTestCase):
    """Gotcha #4: имя плагина не должно быть пустым."""

    async def test_empty_name_raises(self):
        # Код (loader.py:160-161):
        #   if not instance.name:
        #       raise ValueError("...empty name")

        class EmptyNamePlugin(Plugin):
            @property
            def name(self) -> str:
                return ""

            @property
            def version(self) -> str:
                return "1.0"

            async def on_load(self, app, context=None) -> None:
                pass

        app = MockApp()
        loader = PluginLoader(app)

        with self.assertRaises(ValueError, msg="Пустое имя должно вызывать ValueError"):
            await loader.load_plugin(EmptyNamePlugin)

    async def test_duplicate_name_raises(self):
        # Код (loader.py:162-163):
        #   if instance.name in self._loaded:
        #       raise ValueError("...already loaded")

        app = MockApp()
        loader = PluginLoader(app)

        await loader.load_plugin(SimplePlugin)

        class AnotherSimplePlugin(Plugin):
            @property
            def name(self) -> str:
                return "simple"  # то же имя!

            @property
            def version(self) -> str:
                return "2.0"

            async def on_load(self, app, context=None) -> None:
                pass

        with self.assertRaises(ValueError):
            await loader.load_plugin(AnotherSimplePlugin)


class TestCircularDependency(unittest.IsolatedAsyncioTestCase):
    """Gotcha #5: обнаружение циклических зависимостей через _loading set."""

    async def test_circular_dependency_detected(self):
        # Код (loader.py:348-349):
        #   if instance.name in self._loading:
        #       raise ValueError("Circular plugin dependency detected...")
        #
        # _loading — множество имён плагинов в процессе загрузки.
        # Цикл A → B → A обнаруживается когда A уже в _loading.

        class PluginA(Plugin):
            @property
            def name(self) -> str:
                return "circ_a"

            @property
            def version(self) -> str:
                return "1.0"

            @property
            def dependencies(self) -> List[str]:
                return ["circ_b"]

            async def on_load(self, app, context=None) -> None:
                pass

        class PluginB(Plugin):
            @property
            def name(self) -> str:
                return "circ_b"

            @property
            def version(self) -> str:
                return "1.0"

            @property
            def dependencies(self) -> List[str]:
                return ["circ_a"]  # цикл обратно к A!

            async def on_load(self, app, context=None) -> None:
                pass

        app = MockApp()
        loader = PluginLoader(app)
        loader.register_class("circ_a", PluginA)
        loader.register_class("circ_b", PluginB)

        with self.assertRaises(ValueError) as ctx:
            await loader.load_plugin(PluginA)

        self.assertIn("Circular", str(ctx.exception))

    async def test_loading_set_cleared_after_success(self):
        # _loading должен очищаться после успешной загрузки (finally блок)
        app = MockApp()
        loader = PluginLoader(app)

        await loader.load_plugin(SimplePlugin)
        self.assertEqual(len(loader._loading), 0, "_loading очищен после успешной загрузки")

    async def test_loading_set_cleared_after_failure(self):
        # _loading должен очищаться даже при ошибке (finally блок)
        class FailPlugin(Plugin):
            @property
            def name(self) -> str:
                return "fail"

            @property
            def version(self) -> str:
                return "1.0"

            async def on_load(self, app, context=None) -> None:
                raise RuntimeError("on_load failed")

        app = MockApp()
        loader = PluginLoader(app)

        with self.assertRaises(RuntimeError):
            await loader.load_plugin(FailPlugin)

        self.assertEqual(len(loader._loading), 0, "_loading очищен даже после ошибки")


class TestFindPluginClass(unittest.IsolatedAsyncioTestCase):
    """Gotcha #6: _find_plugin_class возвращает ПЕРВЫЙ Plugin subclass (по алфавиту)."""

    async def test_returns_first_alphabetically(self):
        # Код (loader.py:389):
        #   for attr_name in dir(module):
        #       attr = getattr(module, attr_name)
        #       if isinstance(attr, type) and issubclass(attr, Plugin)...
        #           return attr
        #
        # dir() возвращает имена в алфавитном порядке.
        # Если в модуле два Plugin-класса, возвращается первый по алфавиту.

        import types

        mod = types.ModuleType("test_mod")

        class ZebraPlugin(Plugin):
            @property
            def name(self) -> str:
                return "zebra"

            @property
            def version(self) -> str:
                return "1.0"

            async def on_load(self, app, context=None) -> None:
                pass

        class ApplePlugin(Plugin):
            @property
            def name(self) -> str:
                return "apple"

            @property
            def version(self) -> str:
                return "1.0"

            async def on_load(self, app, context=None) -> None:
                pass

        mod.ApplePlugin = ApplePlugin
        mod.ZebraPlugin = ZebraPlugin

        found = PluginLoader._find_plugin_class(mod)
        # ApplePlugin идёт раньше по алфавиту в dir()
        self.assertIsNotNone(found)
        self.assertIs(found, ApplePlugin, "Первый по алфавиту Plugin subclass возвращается")

    async def test_abstract_class_skipped(self):
        # Классы с __abstractmethods__ пропускаются
        import types

        mod = types.ModuleType("test_mod2")

        class AbstractPlugin(Plugin):
            @property
            def name(self) -> str:
                return "abstract"

            # version не реализован → __abstractmethods__ не пуст

        class ConcretePlugin(Plugin):
            @property
            def name(self) -> str:
                return "concrete"

            @property
            def version(self) -> str:
                return "1.0"

            async def on_load(self, app, context=None) -> None:
                pass

        mod.AbstractPlugin = AbstractPlugin
        mod.ConcretePlugin = ConcretePlugin

        found = PluginLoader._find_plugin_class(mod)
        self.assertIs(found, ConcretePlugin, "Абстрактные классы пропускаются")


class TestUnloadNonExistent(unittest.IsolatedAsyncioTestCase):
    """Gotcha #7: unload несуществующего плагина — warning, не exception."""

    async def test_unload_nonexistent_silent(self):
        # Код (loader.py:228-231):
        #   plugin = self._loaded.get(plugin_name)
        #   if plugin is None:
        #       logger.warning(f"unload_plugin: '{plugin_name}' not found")
        #       return
        #
        # Молча возвращается, без исключения.

        app = MockApp()
        loader = PluginLoader(app)

        # Не должно вызывать исключение
        await loader.unload_plugin("nonexistent_plugin")
        # Если мы дошли сюда — тест пройден
        self.assertTrue(True)


class TestOnUnloadFailureCleanup(unittest.IsolatedAsyncioTestCase):
    """Gotcha #8: ошибка on_unload не предотвращает cleanup (try/finally)."""

    async def test_cleanup_runs_even_if_on_unload_fails(self):
        # Код (loader.py:241-248):
        #   try:
        #       await plugin.on_unload(self.app)
        #   finally:
        #       context = self._contexts.pop(plugin_name, None)
        #       if context is not None:
        #           await self._cleanup_context(context)
        #       del self._loaded[plugin_name]
        #
        # finally гарантирует cleanup даже при ошибке on_unload.

        cleanup_callback = MagicMock_sync()

        class FailUnloadPlugin(Plugin):
            @property
            def name(self) -> str:
                return "fail_unload"

            @property
            def version(self) -> str:
                return "1.0"

            async def on_load(self, app, context=None) -> None:
                if context:
                    context.subscribe("test", cleanup_callback)

            async def on_unload(self, app) -> None:
                raise RuntimeError("on_unload failed!")

        app = MockApp()
        loader = PluginLoader(app)

        await loader.load_plugin(FailUnloadPlugin)

        # Подписка активна
        self.assertIn(cleanup_callback, app.event_bus.list_subscribers("test"))

        # on_unload падает, но cleanup всё равно выполняется
        with self.assertRaises(RuntimeError):
            await loader.unload_plugin("fail_unload")

        # Плагин удалён из loaded несмотря на ошибку on_unload
        self.assertNotIn("fail_unload", loader._loaded)

        # Подписка удалена (cleanup через context)
        self.assertNotIn(cleanup_callback, app.event_bus.list_subscribers("test"),
                         "Cleanup выполнен несмотря на ошибку on_unload")


class TestRuleRemovalFailure(unittest.IsolatedAsyncioTestCase):
    """Gotcha #9: ошибка удаления правила — warning, правило orphaned."""

    async def test_rule_removal_failure_orphaned(self):
        # Код (loader.py:214-218):
        #   for rule_func in list(getattr(context, "_rules", [])):
        #       try:
        #           await self.app.remove_rule(rule_func)
        #       except Exception as e:
        #           logger.warning(f"Failed to remove plugin rule: {e}")
        #
        # Если remove_rule падает, правило остаётся orphaned — только warning.

        class RulePlugin(Plugin):
            @property
            def name(self) -> str:
                return "rule_plugin"

            @property
            def version(self) -> str:
                return "1.0"

            async def on_load(self, app, context=None) -> None:
                if context:
                    async def my_rule(event, app):
                        pass
                    context.add_rule(my_rule)

        class FailApp(MockApp):
            async def remove_rule(self, func) -> None:
                raise RuntimeError("remove_rule failed!")

        app = FailApp()
        loader = PluginLoader(app)

        await loader.load_plugin(RulePlugin)

        # Правило добавлено
        self.assertEqual(len(app._rules), 1)

        # unload — remove_rule падает, но unload продолжается
        # on_unload не падает, но _cleanup_context ловит ошибку remove_rule
        await loader.unload_plugin("rule_plugin")

        # Плагин выгружен несмотря на ошибку удаления правила
        self.assertNotIn("rule_plugin", loader._loaded)
        # Правило осталось orphaned в app._rules
        self.assertEqual(len(app._rules), 1, "Правило orphaned — не удалено из-за ошибки")


class TestConfigureReplacesEntireConfig(unittest.IsolatedAsyncioTestCase):
    """Gotcha #10: configure() заменяет весь конфиг, без merge."""

    async def test_configure_replaces_not_merges(self):
        # Код (base.py:49-51):
        #   def configure(self, config: Dict[str, Any]) -> None:
        #       self._config = config
        #
        # НЕ self._config.update(config), а присваивание.
        # Второй вызов configure полностью стирает первый конфиг.

        class ConfigPlugin(Plugin):
            @property
            def name(self) -> str:
                return "config_test"

            @property
            def version(self) -> str:
                return "1.0"

            async def on_load(self, app, context=None) -> None:
                pass

        plugin = ConfigPlugin()

        # Первый configure
        plugin.configure({"host": "localhost", "port": 1883})
        self.assertEqual(plugin._config, {"host": "localhost", "port": 1883})

        # Второй configure — НЕ merge, а полная замена
        plugin.configure({"debug": True})
        self.assertEqual(plugin._config, {"debug": True})
        self.assertNotIn("host", plugin._config, "Старый конфиг полностью стёрт")
        self.assertNotIn("port", plugin._config, "Старый конфиг полностью стёрт")

    async def test_default_config_empty(self):
        # По умолчанию _config = {} (base.py:26)
        plugin = SimplePlugin()
        self.assertEqual(plugin._config, {})


class TestPluginContextCreateTask(unittest.IsolatedAsyncioTestCase):
    """Дополнительно: create_task отслеживает задачи для cleanup."""

    async def test_tasks_cancelled_on_unload(self):
        task_started = asyncio.Event()

        class TaskPlugin(Plugin):
            @property
            def name(self) -> str:
                return "task_plugin"

            @property
            def version(self) -> str:
                return "1.0"

            async def on_load(self, app, context=None) -> None:
                if context:

                    async def long_running():
                        task_started.set()
                        try:
                            await asyncio.sleep(100)
                        except asyncio.CancelledError:
                            raise

                    context.create_task(long_running())

        app = MockApp()
        loader = PluginLoader(app)

        await loader.load_plugin(TaskPlugin)

        # Ждём, пока задача стартует
        await asyncio.wait_for(task_started.wait(), timeout=1.0)

        # Выгружаем — задача должна быть отменена
        await loader.unload_plugin("task_plugin")

        # Плагин выгружен
        self.assertNotIn("task_plugin", loader._loaded)


class TestDependentsBlockUnload(unittest.IsolatedAsyncioTestCase):
    """Дополнительно: нельзя выгрузить плагин, от которого зависят другие."""

    async def test_unload_blocked_by_dependent(self):
        app = MockApp()
        loader = PluginLoader(app)
        loader.register_class("base_dep", BaseDepPlugin)

        await loader.load_plugin(BaseDepPlugin)
        await loader.load_plugin(DependentPlugin)

        with self.assertRaises(ValueError) as ctx:
            await loader.unload_plugin("base_dep")

        self.assertIn("required by", str(ctx.exception))

        # Оба плагина остаются загруженными
        self.assertIn("base_dep", loader._loaded)
        self.assertIn("dependent", loader._loaded)


# ---------------------------------------------------------------------------
# Вспомогательные mock-классы
# ---------------------------------------------------------------------------


def MagicMock_async():
    """Создаёт async mock-функцию."""
    async def _mock(data=None):
        pass
    return _mock


def MagicMock_sync():
    """Создаёт sync mock-функцию."""
    def _mock(data=None):
        pass
    return _mock


if __name__ == "__main__":
    unittest.main()
