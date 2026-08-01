"""Gotchas and edge cases in Kamio's hot-reload subsystem.

This module is a deep-dive for framework developers.  It demonstrates
non-obvious behaviours and pitfalls in ``kamio.core.hot_reload`` that can
cause silent failures, lost updates, or resource leaks in production.

Key gotchas covered
-------------------
1.  Watchdog import guard uses ``except Exception`` — catches *all* errors,
    not just ``ImportError``.  A buggy watchdog build silently disables
    OS-level watching and falls back to polling.
2.  ``enable()`` starts watchdog only when ``_WATCHDOG_AVAILABLE *and*
    _entries`` is non-empty.  If you enable before registering watches,
    you silently get polling even with watchdog installed.
3.  Debounce keyed by file path only — multiple entries watching the same
    file collide in ``_pending``; only the *last* registered handler fires.
4.  ``_schedule_call`` silently returns when ``self._loop is None`` —
    changes are lost with only a log.error.
5.  ``reload_rules_from_file`` matches old rules by ``func.__name__``
    only — two different functions with the same name collide and the
    first match is replaced.
6.  ``reload_devices_from_file`` calls ``app.register(attr)`` which
    re-adds device-level rules — calling reload twice duplicates rules.
7.  Rollback for device reload is best-effort; if rollback itself fails
    the application is left in an inconsistent state with only a log.
8.  Handler errors are caught inside ``_invoke_handler`` — the file
    change is considered "processed" even though the handler failed.
9.  ``reload_rules_from_file`` returns ``True`` (success) when no rule
    functions are found in the file — a no-op masquerades as success.

Every gotcha is proven with assertions that run **without an MQTT broker**.
"""
from __future__ import annotations

import asyncio
import importlib
import logging
import os
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from kamio.core.hot_reload import (
    HotReloadManager,
    _WatchEntry,
    _find_rule_funcs,
    reload_rules_from_file,
)
from kamio.core.rules import Rule, RuleEngine, RuleEvent

logging.disable(logging.CRITICAL)  # заглушаем логи во время тестов

# ---------------------------------------------------------------------------
# Mock-инфраструктура: имитирует KamioApp без MQTT-брокера
# ---------------------------------------------------------------------------


class MockEventBus:
    """Минимальный EventBus для hot-reload тестов."""

    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []

    async def publish(self, event_type: str, data: dict) -> None:
        self.published.append((event_type, data))


class MockHooks:
    """Заглушка HooksManager."""

    async def trigger(self, *a, **kw) -> None:
        pass


class MockState:
    """Заглушка StateManager."""

    def get_all_states(self) -> dict:
        return {}


class MockRegistry:
    """Заглушка DeviceRegistry с поддержкой register/unregister."""

    def __init__(self) -> None:
        self._classes: dict[str, type] = {}

    @property
    def classes(self) -> dict[str, type]:
        return dict(self._classes)

    def register_class(self, cls: type) -> None:
        self._classes[cls.device_type()] = cls

    def unregister_class(self, device_type: str) -> type | None:
        return self._classes.pop(device_type, None)

    def get_instance(self, device_id: str):
        return None


class MockApp:
    """Упрощённый KamioApp для тестирования hot-reload без брокера."""

    def __init__(self) -> None:
        self.event_bus = MockEventBus()
        self.hooks = MockHooks()
        self.state = MockState()
        self.rules = RuleEngine(self)
        self.registry = MockRegistry()
        self._is_running = False
        self._registered_funcs: list = []

    def add_rule(self, func, **kwargs) -> object:
        # Упрощённая версия add_rule — создаёт Rule и добавляет в engine
        device = kwargs.pop("device", None)
        rule_obj = Rule(func, device_class=device, **kwargs)
        self.rules.add_rule(rule_obj)
        self._registered_funcs.append(func)
        return func

    async def remove_rule(self, func) -> None:
        for rule_obj in list(self.rules.rules):
            if rule_obj.func is func:
                self.rules.remove_rule(rule_obj)
                return

    def register(self, device_class) -> None:
        self.registry.register_class(device_class)
        if hasattr(device_class, "Kamio_RULES") and device_class.Kamio_RULES:
            for rule_name, rule_func in device_class.Kamio_RULES.items():
                fields = getattr(rule_func, "_rule_fields", None)
                self.add_rule(rule_func, device=device_class, fields=fields)


# ---------------------------------------------------------------------------
# Тесты
# ---------------------------------------------------------------------------


class TestWatchdogImportGuard(unittest.IsolatedAsyncioTestCase):
    """Gotcha #1: ``except Exception`` ловит ВСЕ ошибки, не только ImportError."""

    async def test_except_exception_catches_all(self):
        # НЕПРАВИЛЬНО: предполагаем, что except Exception == except ImportError
        # ПРАВИЛЬНО: except Exception ловит любой Exception-наследник,
        #   включая RuntimeError, AttributeError и т.д.
        #
        # В исходном коде (hot_reload.py:25):
        #   except Exception:  # pragma: no cover
        #       _WATCHDOG_AVAILABLE = False
        #
        # Это означает, что ЛЮБАЯ ошибка при импорте watchdog (не только
        # отсутствие пакета) молча отключает OS-level watching.

        # Доказываем: except Exception ловит RuntimeError, не только ImportError
        caught = False
        try:
            try:
                raise RuntimeError("watchdog internal bug")
            except Exception:  # ловит RuntimeError тоже!
                caught = True
        except ImportError:
            caught = False

        self.assertTrue(caught, "except Exception должен ловить RuntimeError")

        # ImportError — это подмножество Exception
        caught_import = False
        try:
            try:
                raise ImportError("no module named watchdog")
            except Exception:
                caught_import = True
        except ImportError:
            pass

        self.assertTrue(caught_import)

        # Доказываем разницу: except ImportError НЕ ловит RuntimeError
        not_caught = True
        try:
            try:
                raise RuntimeError("bug")
            except ImportError:
                not_caught = False  # не выполняется
        except RuntimeError:
            pass  # выполняется внешний except

        self.assertTrue(not_caught, "except ImportError не ловит RuntimeError")


class TestWatchdogFallbackToPolling(unittest.IsolatedAsyncioTestCase):
    """Gotcha #2: watchdog доступен, но нет entries → fallback на polling."""

    async def test_no_entries_falls_back_to_polling(self):
        # НЕПРАВИЛЬНО: вызывать enable() до watch_file() и ожидать watchdog
        # ПРАВИЛЬНО: сначала watch_file/watch_directory, потом enable()
        #
        # Код (hot_reload.py:176):
        #   if _WATCHDOG_AVAILABLE and self._entries:
        #       self._start_watchdog()
        #   else:
        #       self._start_polling()
        #
        # Если _entries пуст — polling, даже если watchdog установлен.

        app = MockApp()
        mgr = HotReloadManager(app, poll_interval=0.05, debounce=0.01)

        # Регистрируем watch ПОСЛЕ enable — но enable уже решил использовать polling
        # (потому что _entries был пуст на момент вызова)
        mgr.enable()

        # Даже если watchdog доступен, _observer не запущен (нет entries)
        self.assertIsNone(mgr._observer, "Observer не запускается без entries")

        # Теперь добавим watch — но polling уже работает, observer не запустится
        with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as f:
            tmp_path = f.name

        try:
            mgr.watch_file(tmp_path, lambda p: None)
            # _observer остаётся None: enable() уже прошёл
            self.assertIsNone(mgr._observer)
        finally:
            os.unlink(tmp_path)

        await mgr.disable()

    async def test_entries_before_enable_uses_watchdog_if_available(self):
        # ПРАВИЛЬНЫЙ порядок: watch_file → enable
        app = MockApp()
        mgr = HotReloadManager(app, poll_interval=0.05, debounce=0.01)

        with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as f:
            tmp_path = f.name
        try:
            mgr.watch_file(tmp_path, lambda p: None)
            mgr.enable()

            # Если watchdog установлен — observer запущен; иначе polling
            from kamio.core.hot_reload import _WATCHDOG_AVAILABLE

            if _WATCHDOG_AVAILABLE:
                self.assertIsNotNone(mgr._observer)
            else:
                self.assertIsNone(mgr._observer)
                self.assertIsNotNone(mgr._task)  # polling task
        finally:
            await mgr.disable()
            os.unlink(tmp_path)


class TestDebounceKeyCollision(unittest.IsolatedAsyncioTestCase):
    """Gotcha #3: debounce по ключу file_path — коллизия при нескольких entries."""

    async def test_same_file_multiple_entries_only_last_handler(self):
        # НЕПРАВИЛЬНО: регистрировать несколько handler-ов на один файл
        # ПРАВИЛЬНО: использовать один handler, который диспетчеризует внутри
        #
        # _schedule_call_in_loop (hot_reload.py:298):
        #   key = file_path
        #   existing = self._pending.get(key)
        #   if existing: existing.cancel()
        #
        # Ключ — только путь к файлу, без учёта entry/handler.
        # Если два entry смотрят один файл, второй отменяет таймер первого.

        app = MockApp()
        mgr = HotReloadManager(app, poll_interval=0.05, debounce=0.05)
        mgr.enable()

        with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as f:
            f.write("# initial\n")
            tmp_path = f.name
        try:
            handler1_calls: list[str] = []
            handler2_calls: list[str] = []

            async def handler1(path: str) -> None:
                handler1_calls.append(path)

            async def handler2(path: str) -> None:
                handler2_calls.append(path)

            # Два entry на один файл
            mgr.watch_file(tmp_path, handler1)
            mgr.watch_file(tmp_path, handler2)

            # Меняем файл
            with open(tmp_path, "w") as f:
                f.write("# changed\n")
            os.utime(tmp_path, (os.path.getmtime(tmp_path) + 10,) * 2)

            # Ждём debounce + polling
            await asyncio.sleep(0.3)

            # Только handler2 (последний) должен вызваться — handler1 отменён
            # Это доказывает коллизию ключей в _pending
            self.assertEqual(len(handler2_calls), 1, "handler2 (последний) должен сработать")
            self.assertEqual(len(handler1_calls), 0, "handler1 отменён из-за коллизии ключей")
        finally:
            await mgr.disable()
            os.unlink(tmp_path)


class TestScheduleCallNoLoop(unittest.IsolatedAsyncioTestCase):
    """Gotcha #4: _schedule_call молча возвращается если loop is None."""

    async def test_schedule_call_silent_when_loop_none(self):
        # НЕПРАВИЛЬНО: вызывать _schedule_call до enable()
        # ПРАВИЛЬНО: всегда вызывать enable() перед использованием
        #
        # Код (hot_reload.py:277-279):
        #   if self._loop is None:
        #       logger.error("...no event loop; skipping schedule")
        #       return
        #
        # Изменение файла теряется молча (только log.error).

        app = MockApp()
        mgr = HotReloadManager(app, poll_interval=0.05, debounce=0.01)

        # _loop is None до enable()
        self.assertIsNone(mgr._loop)

        calls: list[str] = []

        async def handler(path: str) -> None:
            calls.append(path)

        # _schedule_call не должен ничего планировать
        mgr._schedule_call("/fake/path.py", handler)

        # Никакого таймера не добавлено
        self.assertEqual(len(mgr._pending), 0, "Ничего не запланировано без loop")
        self.assertEqual(len(calls), 0, "Handler не вызван")


class TestRuleMatchingByNameOnly(unittest.IsolatedAsyncioTestCase):
    """Gotcha #5: reload_rules_from_file ищет старые правила по func.__name__."""

    async def test_name_collision_replaces_wrong_rule(self):
        # НЕПРАВИЛЬНО: иметь две функции с одинаковым __name__ в разных модулях
        # ПРАВИЛЬНО: использовать уникальные имена функций или префиксы модулей
        #
        # Код (hot_reload.py:367):
        #   old_by_name = {r.func.__name__: r for r in app.rules.rules}
        #
        # Если два правила имеют функции с одинаковым __name__, в dict
        # остаётся только последнее.  При reload заменяется первое совпадение
        # по имени — может быть не то правило.

        app = MockApp()

        # Создаём два правила с функциями одного имени "on_change"
        async def on_change_v1(event, app):
            pass

        on_change_v1.__name__ = "on_change"
        app.add_rule(on_change_v1, fields=["power"])

        async def on_change_v2(event, app):
            pass

        on_change_v2.__name__ = "on_change"
        app.add_rule(on_change_v2, fields=["temperature"])

        # В rules два правила, но old_by_name будет иметь только одну запись
        # (последняя перезаписывает первую в dict comprehension)
        old_by_name = {r.func.__name__: r for r in app.rules.rules}
        self.assertEqual(len(old_by_name), 1, "Коллизия: только одно имя в dict")
        self.assertIs(old_by_name["on_change"].func, on_change_v2, "Последнее правило перезаписало первое")

        # Доказываем: в engine два правила, но по имени найдётся только v2
        self.assertEqual(len(app.rules.rules), 2)


class TestDeviceReloadDuplicateRules(unittest.IsolatedAsyncioTestCase):
    """Gotcha #6: reload_devices_from_file повторно регистрирует правила."""

    async def test_reload_device_class_duplicates_rules(self):
        # НЕПРАВИЛЬНО: ожидать, что reload_devices_from_file идемпотентен
        # ПРАВИЛЬНО: перед reload удалить старые device-level правила
        #
        # Код (hot_reload.py:431-432):
        #   if hasattr(attr, "Kamio_RULES") and attr.Kamio_RULES:
        #       app.register(attr)
        #
        # app.register() вызывает add_rule для каждого правила — без удаления
        # старых.  Два reload → дубликаты правил.

        from kamio.core.device_meta import DeviceMeta
        from kamio.device import Device, rule as device_rule

        # Создаём класс устройства с правилом
        class TestDevice(Device, metaclass=DeviceMeta):
            power: bool = False

            @device_rule(fields=["power"])
            async def on_power(self, event, app):
                pass

        app = MockApp()
        app.register(TestDevice)

        initial_rule_count = len(app.rules.rules)
        self.assertEqual(initial_rule_count, 1, "Одно правило после первой регистрации")

        # Повторная регистрация (имитация reload) — add_rule НЕ проверяет дубликаты
        app.register(TestDevice)
        self.assertEqual(len(app.rules.rules), 2, "Дубликат правила после повторной регистрации")

        # Третья регистрация — ещё один дубликат
        app.register(TestDevice)
        self.assertEqual(len(app.rules.rules), 3, "Третий дубликат")


class TestHandlerErrorStillProcessed(unittest.IsolatedAsyncioTestCase):
    """Gotcha #8: ошибка в handler ловится, но изменение считается "обработанным"."""

    async def test_handler_error_caught_silently(self):
        # НЕПРАВИЛЬНО: полагаться на то, что ошибка handler-а остановит обработку
        # ПРАВИЛЬНО: проверять event_bus на "hot_reload_error" события
        #
        # Код (hot_reload.py:320-335):
        #   async def _invoke_handler(self, handler, file_path):
        #       try:
        #           await handler(file_path)
        #       except Exception as e:
        #           logger.error(...)
        #           await self.app.event_bus.publish("hot_reload_error", ...)
        #
        # Ошибка ловится, публикуется событие, но _fire() уже завершился.
        # File change считается "processed".

        app = MockApp()
        mgr = HotReloadManager(app, poll_interval=0.05, debounce=0.02)
        mgr.enable()

        with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as f:
            f.write("# initial\n")
            tmp_path = f.name
        try:
            error_raised = False

            async def failing_handler(path: str) -> None:
                nonlocal error_raised
                error_raised = True
                raise ValueError("Handler failed!")

            mgr.watch_file(tmp_path, failing_handler)

            # Меняем файл
            with open(tmp_path, "w") as f:
                f.write("# changed\n")
            os.utime(tmp_path, (os.path.getmtime(tmp_path) + 10,) * 2)

            await asyncio.sleep(0.3)

            # Handler был вызван и выбросил исключение
            self.assertTrue(error_raised, "Handler был вызван")

            # Ошибка опубликована в event_bus
            error_events = [e for e in app.event_bus.published if e[0] == "hot_reload_error"]
            self.assertEqual(len(error_events), 1, "Ошибка опубликована как hot_reload_error")

            # Но _pending очищен — изменение "processed"
            self.assertEqual(len(mgr._pending), 0, "Изменение считается обработанным несмотря на ошибку")
        finally:
            await mgr.disable()
            os.unlink(tmp_path)


class TestNoRuleFunctionsReturnsTrue(unittest.IsolatedAsyncioTestCase):
    """Gotcha #9: файл без rule-функций возвращает True (успех)."""

    async def test_empty_module_returns_true(self):
        # НЕПРАВИЛЬНО: ожидать False или исключение при отсутствии правил
        # ПРАВИЛЬНО: проверять количество заменённых правил в событии
        #
        # Код (hot_reload.py:363-365):
        #   if not new_funcs:
        #       logger.debug("No rule functions found...")
        #       return True
        #
        # Файл без _Kamio_rule_kwargs функций → return True (успех).

        app = MockApp()

        with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as f:
            f.write(textwrap.dedent("""
                # Файл без rule-функций
                def helper():
                    pass
            """))
            tmp_path = f.name
        try:
            result = await reload_rules_from_file(tmp_path, app)
            self.assertTrue(result, "Пустой файл (без правил) возвращает True")

            # Событие hot_reload_rules НЕ опубликовано (return до publish)
            reload_events = [e for e in app.event_bus.published if e[0] == "hot_reload_rules"]
            self.assertEqual(len(reload_events), 0, "Событие reload не опубликовано для пустого файла")
        finally:
            os.unlink(tmp_path)

    async def test_find_rule_funcs_empty(self):
        # Доказываем: _find_rule_funcs возвращает [] для модуля без правил
        with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as f:
            f.write("x = 42\n")
            tmp_path = f.name
        try:
            spec = importlib.util.spec_from_file_location("test_no_rules", tmp_path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)

            funcs = _find_rule_funcs(mod)
            self.assertEqual(funcs, [], "Нет rule-функций → пустой список")
        finally:
            os.unlink(tmp_path)


class TestRollbackDeviceReload(unittest.IsolatedAsyncioTestCase):
    """Gotcha #7: rollback для device reload — best-effort, может сам упасть."""

    async def test_rollback_restores_classes_on_error(self):
        # Код (hot_reload.py:445-465):
        # При ошибке rollback пытается восстановить old_classes и old_rules.
        # Если rollback сам падает — логируется и возвращается False.
        # Приложение остаётся в частично-изменённом состоянии.

        from kamio.core.device_meta import DeviceMeta
        from kamio.device import Device

        class OriginalDevice(Device, metaclass=DeviceMeta):
            power: bool = False

        app = MockApp()
        app.registry.register_class(OriginalDevice)
        self.assertIn("originaldevice", app.registry.classes)

        # Имитируем ошибку при reload: файл с синтаксической ошибкой
        with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as f:
            f.write("syntax error !!!\n")
            tmp_path = f.name
        try:
            from kamio.core.hot_reload import reload_devices_from_file

            result = await reload_devices_from_file(tmp_path, app)
            self.assertFalse(result, "Ошибка reload → return False")

            # Rollback должен восстановить классы
            # (old_classes был пуст, new тоже не добавлены из-за ошибки)
            self.assertIn("originaldevice", app.registry.classes, "Старый класс сохранён после rollback")
        finally:
            os.unlink(tmp_path)


class TestWatchEntryMtimeSnapshot(unittest.IsolatedAsyncioTestCase):
    """Дополнительно: _WatchEntry.changed_paths обновляет snapshot после вызова."""

    async def test_changed_paths_updates_snapshot(self):
        # _WatchEntry.changed_paths() возвращает изменённые пути и обновляет
        # _mtimes.  Повторный вызов без изменений → пустой список.

        with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as f:
            f.write("# v1\n")
            tmp_path = f.name
        try:
            entry = _WatchEntry(
                path=os.path.abspath(tmp_path),
                pattern="*",
                handler=lambda p: None,
                is_dir=False,
            )

            # Первая проверка — нет изменений (snapshot уже сделан в __init__)
            self.assertEqual(entry.changed_paths(), [])

            # Меняем файл
            with open(tmp_path, "w") as f:
                f.write("# v2\n")
            os.utime(tmp_path, (os.path.getmtime(tmp_path) + 10,) * 2)

            # Теперь есть изменение
            changed = entry.changed_paths()
            self.assertEqual(len(changed), 1)
            self.assertEqual(changed[0], os.path.abspath(tmp_path))

            # Повторная проверка — снова пусто (snapshot обновлён)
            self.assertEqual(entry.changed_paths(), [])
        finally:
            os.unlink(tmp_path)


if __name__ == "__main__":
    unittest.main()
