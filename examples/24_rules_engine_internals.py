"""
24 — Rules Engine Internals (внутренности движка правил)
=========================================================

ГЛУБОКОЕ ПОГРУЖЕНИЕ для разработчиков фреймворка.

RuleEngine — сердце автоматизации kamio.  Но его внутренности полны
скрытых поведений: от обнаружения device-level правил через __qualname__
до гонок при add_rule/remove_rule без блокировки.

ПОДВОХИ И КРАЕВЫЕ СЛУЧАИ:

    Обнаружение device-level правил:
      1. "." в __qualname__ — лямбды (lambda) НЕ обнаруживаются как
         device-level правила (у лямбды __qualname__ = "<lambda>").
      2. Параметры: 0=без аргументов, 1=event, 2+=event+app.
         Диспетчеризация по len(sig.parameters), не по типам.

    Interval rules:
      3. Interval rules запускаются НЕМЕДЛЕННО, если engine уже running.
         add_rule с interval при работающем engine → задача стартует сразу.
      4. Rule с interval И fields: НИКОГДА не срабатывает как event-rule
         (handle_device_update фильтрует по rule.interval is not None).

    Блокировки и гонки:
      5. remove_rule НЕ захватывает _lock — гонка с handle_device_update.
      6. add_rule НЕ захватывает _lock — гонка с set_rules.
      7. start()/stop() НЕ захватывают _lock — гонка с handle_device_update.
      8. _rebuild_index вызывается в set_rules, но НЕ в add_rule/remove_rule
         (add/remove обновляют индекс инкрементально).

    Поведение фильтрации:
      9. Disabled rules молча пропускаются (нет логирования в handle_device_update).
     10. Interval rules молча фильтруются в handle_device_update (interval is not None → continue).
     11. Field filtering: правило срабатывает, если ЛЮБОЕ из watched fields
         есть в snapshot (any(), не all()).
     12. Snapshot — shallow copy (dict(event.data)).  Вложенные mutable объекты
         могут быть изменены через ссылку.

    Base class matching:
     13. handle_device_update проверяет MRO базовых классов.
         base_type может быть None (если base не Device) — пропускается.

Запуск (БЕЗ MQTT-брокера)::

    python examples/24_rules_engine_internals.py
"""
from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

from kamio import Device, state
from kamio.core.rules import Rule, RuleEngine, RuleEvent

logging.basicConfig(level=logging.WARNING, format="%(name)s | %(levelname)s | %(message)s")
logger = logging.getLogger("example.24")


# ============================================================================
# Mock App для RuleEngine
# ============================================================================

class MockApp:
    """Минимальный mock KamioApp для тестирования RuleEngine."""

    def __init__(self):
        self.rules = RuleEngine(self)
        self.hooks = MagicMock()
        self.hooks.trigger = AsyncMock()
        self.event_bus = MagicMock()
        self.event_bus.publish = AsyncMock()
        self.state = MagicMock()
        self.state.get_all_states = MagicMock(return_value={})
        self.registry = MagicMock()
        self.registry.get_instance = MagicMock(return_value=None)


# ============================================================================
# 1. Device-level rule detection: "." in __qualname__ (lambda не работает)
# ============================================================================

async def demo_device_level_detection():
    """Device-level rule обнаруживается через "." в __qualname__ — lambda не работает."""
    print("\n--- 1. Device-level rule detection: '.' in __qualname__ ---")

    class MyDevice(Device):
        power: bool = state(default=False, writable=True)

        async def on_power_change(self, event: RuleEvent, app):
            pass

    # Метод класса имеет __qualname__ = "MyDevice.on_power_change"
    # Содержит "." → обнаруживается как device-level
    method = MyDevice.on_power_change
    assert "." in method.__qualname__, f"__qualname__ должен содержать '.': {method.__qualname__}"

    # Декорируем как rule
    from kamio.device import rule as rule_decorator
    decorated = rule_decorator(fields=["power"])(method)
    assert decorated._is_rule is True
    assert "." in decorated.__qualname__
    print(f"  Метод класса: __qualname__='{decorated.__qualname__}' → содержит '.' → device-level OK")

    # НЕПРАВИЛЬНО: использовать lambda как device-level rule
    # Lambda имеет __qualname__ = "<lambda>" — НЕТ точки!
    lambda_fn = lambda event, app: None
    assert "." not in lambda_fn.__qualname__, f"Lambda __qualname__: {lambda_fn.__qualname__}"
    print(f"  Lambda: __qualname__='{lambda_fn.__qualname__}' → НЕТ '.' → НЕ device-level")

    # Проверка логики обнаружения из Rule.run():
    # is_device_level_rule = (
    #     device_instance is not None
    #     and getattr(self.func, "_is_rule", False)
    #     and "." in getattr(self.func, "__qualname__", "")
    # )
    device_instance = MyDevice()
    is_device_level_method = (
        device_instance is not None
        and getattr(decorated, "_is_rule", False)
        and "." in getattr(decorated, "__qualname__", "")
    )
    assert is_device_level_method is True
    print("  Метод с _is_rule + '.' в __qualname__ → is_device_level_rule=True")

    is_device_level_lambda = (
        device_instance is not None
        and getattr(lambda_fn, "_is_rule", False)  # False для lambda
        and "." in getattr(lambda_fn, "__qualname__", "")
    )
    assert is_device_level_lambda is False
    print("  Lambda без _is_rule → is_device_level_rule=False")

    print("  OK: device-level detection через __qualname__ проверен")


# ============================================================================
# 2. Parameter count dispatch: 0=no args, 1=event, 2=event+app
# ============================================================================

async def demo_parameter_count_dispatch():
    """Диспетчеризация по количеству параметров: 0, 1, или 2+."""
    print("\n--- 2. Parameter count dispatch ---")

    app = MockApp()

    call_log: list[str] = []

    # 0 параметров
    async def rule_zero():
        call_log.append("zero")

    # 1 параметр
    async def rule_one(event: RuleEvent):
        call_log.append(f"one:{event.data}")

    # 2 параметра
    async def rule_two(event: RuleEvent, app):
        call_log.append(f"two:{event.data}")

    rules = [
        Rule(func=rule_zero, description="zero"),
        Rule(func=rule_one, description="one"),
        Rule(func=rule_two, description="two"),
    ]

    event = RuleEvent(data={"power": True}, device_id="dev1", kind="event")

    for rule in rules:
        await rule.run(event, app)

    assert call_log == ["zero", "one:{'power': True}", "two:{'power': True}"], (
        f"Ожидали ['zero', 'one:...', 'two:...'], получили {call_log}"
    )
    print("  0 params -> rule_zero()")
    print("  1 param  -> rule_one(event)")
    print("  2 params -> rule_two(event, app)")

    # ПРОВЕРКА: диспетчеризация по len(sig.parameters)
    for rule, expected_count in [(Rule(rule_zero), 0), (Rule(rule_one), 1), (Rule(rule_two), 2)]:
        sig = inspect.signature(rule.func)
        param_count = len(sig.parameters)
        assert param_count == expected_count, (
            f"Ожидали {expected_count} параметров, получили {param_count}"
        )

    # ПОДВОХ: 3+ параметров тоже вызывается как (event, app) — extra не передаётся
    async def rule_three(event: RuleEvent, app, extra=None):
        call_log.append(f"three:{event.data}")

    call_log.clear()
    rule_3 = Rule(func=rule_three)
    await rule_3.run(event, app)
    # rule_three имеет extra=None по умолчанию -> работает
    assert call_log == [f"three:{event.data}"]
    print("  3 params (extra=None default) -> rule_three(event, app) — extra не передаётся, но default OK")

    print("  OK: parameter count dispatch проверен")


# ============================================================================
# 3. Interval rules start immediately if engine running
# ============================================================================

async def demo_interval_starts_immediately():
    """add_rule с interval при работающем engine -> задача стартует сразу."""
    print("\n--- 3. Interval rules start immediately if engine running ---")

    app = MockApp()
    await app.rules.start()  # engine running

    call_count = 0

    async def my_interval_rule(event: RuleEvent, app):
        nonlocal call_count
        call_count += 1

    # НЕПРАВИЛЬНО: ожидать, что interval rule начнётся только при следующем start()
    # ПРАВИЛЬНО: add_rule проверяет self._is_running и запускает задачу сразу
    rule = Rule(func=my_interval_rule, interval=0.05, description="test interval")
    app.rules.add_rule(rule)

    # Задача должна быть создана немедленно
    assert rule.task is not None, "Interval task должен быть создан сразу"
    assert not rule.task.done(), "Task должен быть активен"
    print("  add_rule с interval при running engine -> task создан НЕМЕДЛЕННО")

    # Ждём немного, чтобы задача выполнилась
    await asyncio.sleep(0.15)
    assert call_count > 0, f"Rule должна была выполниться, call_count={call_count}"
    print(f"  После 0.15с: call_count={call_count} (rule выполнилась)")

    await app.rules.stop()
    print("  OK: interval starts immediately проверен")


# ============================================================================
# 4. Rule with both interval AND fields: never triggers as event rule
# ============================================================================

async def demo_interval_and_fields_never_event():
    """Rule с interval AND fields: НИКОГДА не срабатывает как event-rule."""
    print("\n--- 4. Rule with interval AND fields: never event rule ---")

    app = MockApp()

    event_call_count = 0

    async def hybrid_rule(event: RuleEvent, app):
        nonlocal event_call_count
        event_call_count += 1

    # Rule с ОБОИМИ: interval=10.0 AND fields=["power"]
    # НЕПРАВИЛЬНО: ожидать, что rule сработает и по интервалу, и по изменению power
    # ПРАВИЛЬНО: в handle_device_update: if rule.interval is not None: continue
    rule = Rule(
        func=hybrid_rule,
        interval=10.0,  # interval задан
        fields=["power"],  # fields тоже заданы
        description="hybrid",
    )
    app.rules.add_rule(rule)

    # Пытаемся вызвать через handle_device_update
    # handle_device_update фильтрует: if not rule.enabled or rule.interval is not None: continue
    await app.rules.handle_device_update("dev1", {"power": True})

    assert event_call_count == 0, (
        f"Rule с interval НЕ должна срабатывать как event, call_count={event_call_count}"
    )
    print("  Rule с interval=10.0 AND fields=['power'] -> НЕ сработала как event rule")
    print("  handle_device_update: rule.interval is not None -> continue (пропуск)")

    # ПОДВОХ: fields ИГНОРИРУЮТСЯ, если interval задан
    print("  ВНИМАНИЕ: fields ИГНОРИРУЮТСЯ при наличии interval")

    print("  OK: interval+fields never event проверен")


# ============================================================================
# 5. remove_rule doesn't acquire lock (race)
# ============================================================================

async def demo_remove_rule_no_lock():
    """remove_rule НЕ захватывает _lock — гонка с handle_device_update."""
    print("\n--- 5. remove_rule doesn't acquire lock ---")

    app = MockApp()

    async def my_rule(event: RuleEvent, app):
        pass

    rule = Rule(func=my_rule, fields=["power"], description="test")
    app.rules.add_rule(rule)

    # remove_rule — синхронный метод, НЕ async, НЕ захватывает _lock
    # В отличие от set_rules, который делает: async with self._lock: ...
    assert not inspect.iscoroutinefunction(app.rules.remove_rule), (
        "remove_rule — синхронный метод, НЕ может использовать async lock"
    )
    print("  remove_rule — синхронный метод, НЕ захватывает _lock")

    # Демонстрация: remove_rule можно вызвать без await
    app.rules.remove_rule(rule)
    assert rule not in app.rules.rules
    print("  remove_rule выполнен без lock (синхронно)")

    # ПОДВОХ: если handle_device_update итерирует по candidates,
    # а remove_rule одновременно удаляет rule из списка ->
    # возможен пропуск или IndexError
    print("  ВНИМАНИЕ: гонка с handle_device_update при одновременном remove")

    print("  OK: remove_rule no lock проверен")


# ============================================================================
# 6. add_rule doesn't acquire lock (race with set_rules)
# ============================================================================

async def demo_add_rule_no_lock():
    """add_rule НЕ захватывает _lock — гонка с set_rules."""
    print("\n--- 6. add_rule doesn't acquire lock ---")

    app = MockApp()

    async def rule_a(event: RuleEvent, app):
        pass

    async def rule_b(event: RuleEvent, app):
        pass

    # add_rule — синхронный метод
    assert not inspect.iscoroutinefunction(app.rules.add_rule), (
        "add_rule — синхронный метод, НЕ может использовать async lock"
    )
    print("  add_rule — синхронный метод, НЕ захватывает _lock")

    # set_rules — async, захватывает lock
    assert inspect.iscoroutinefunction(app.rules.set_rules), (
        "set_rules — async метод, захватывает _lock"
    )
    print("  set_rules — async метод, захватывает _lock")

    # Демонстрация гонки
    rule1 = Rule(func=rule_a, fields=["power"])
    rule2 = Rule(func=rule_b, fields=["power"])

    # add_rule добавляет в список без lock
    app.rules.add_rule(rule1)
    assert len(app.rules.rules) == 1

    # Если set_rules одновременно заменяет список (self.rules[:] = list(rules)),
    # а add_rule делает self.rules.append(rule) -> race condition
    # set_rules: self.rules[:] = [rule2]  (под lock)
    # add_rule: self.rules.append(rule1)  (без lock)
    # Результат непредсказуем: rule1 может быть потерян
    print("  ВНИМАНИЕ: add_rule (без lock) + set_rules (под lock) = гонка")

    print("  OK: add_rule no lock проверен")


# ============================================================================
# 7. start()/stop() don't acquire lock
# ============================================================================

async def demo_start_stop_no_lock():
    """start()/stop() НЕ захватывают _lock."""
    print("\n--- 7. start()/stop() don't acquire lock ---")

    app = MockApp()

    async def my_rule(event: RuleEvent, app):
        pass

    rule = Rule(func=my_rule, interval=0.1, description="test")
    app.rules.add_rule(rule)

    # start() — async, но НЕ использует async with self._lock
    # Просто: self._is_running = True + запуск interval tasks
    await app.rules.start()
    assert app.rules._is_running is True
    print("  start() — НЕ захватывает _lock (просто устанавливает _is_running=True)")

    # stop() — async, но НЕ использует async with self._lock
    await app.rules.stop()
    assert app.rules._is_running is False
    print("  stop() — НЕ захватывает _lock (просто устанавливает _is_running=False)")

    # ПОДВОХ: если handle_device_update выполняется под lock,
    # а stop() одновременно меняет _is_running ->
    # interval rule может увидеть _is_running=True в начале цикла,
    # но False в середине
    print("  ВНИМАНИЕ: stop() без lock -> interval rules видят несогласованное состояние")

    print("  OK: start()/stop() no lock проверен")


# ============================================================================
# 8. Disabled rules silently skip (no logging)
# ============================================================================

async def demo_disabled_rules_silent():
    """Disabled rules молча пропускаются в handle_device_update — без логирования."""
    print("\n--- 8. Disabled rules silently skip ---")

    app = MockApp()

    call_count = 0

    async def my_rule(event: RuleEvent, app):
        nonlocal call_count
        call_count += 1

    rule = Rule(func=my_rule, fields=["power"], enabled=False, description="disabled")
    app.rules.add_rule(rule)

    # handle_device_update: if not rule.enabled or rule.interval is not None: continue
    # -> disabled rule пропускается БЕЗ логирования
    await app.rules.handle_device_update("dev1", {"power": True})

    assert call_count == 0, "Disabled rule не должна срабатывать"
    print("  Disabled rule пропущена в handle_device_update — БЕЗ логирования")

    # В Rule.run(): if not self.enabled: return — тоже без логирования
    event = RuleEvent(data={"power": True}, device_id="dev1", kind="event")
    await rule.run(event, app)
    assert call_count == 0, "Disabled rule не должна срабатывать через run()"
    print("  Disabled rule пропущена в Rule.run() — тоже БЕЗ логирования")

    # ПРАВИЛЬНЫЙ ПОДХОД: логировать пропуск disabled rules
    print("  ПРАВИЛЬНО: логировать пропуск disabled rules для отладки")

    print("  OK: disabled rules silent skip проверен")


# ============================================================================
# 9. Field filtering: triggers if ANY watched field in snapshot
# ============================================================================

async def demo_field_filtering_any():
    """Field filtering: правило срабатывает, если ЛЮБОЕ из watched fields в snapshot."""
    print("\n--- 9. Field filtering: ANY watched field ---")

    app = MockApp()

    call_log: list[dict] = []

    async def my_rule(event: RuleEvent, app):
        call_log.append(event.data)

    # Rule следит за ["power", "brightness"]
    rule = Rule(func=my_rule, fields=["power", "brightness"], description="multi-field")
    app.rules.add_rule(rule)

    # Snapshot содержит ТОЛЬКО "power" -> срабатывает (any)
    await app.rules.handle_device_update("dev1", {"power": True})
    assert len(call_log) == 1, f"Должна сработать (power в fields), call_log={call_log}"
    print("  Snapshot={'power': True} -> rule сработала (power in fields)")

    # Snapshot содержит ТОЛЬКО "brightness" -> срабатывает (any)
    call_log.clear()
    await app.rules.handle_device_update("dev1", {"brightness": 50})
    assert len(call_log) == 1, f"Должна сработать (brightness в fields), call_log={call_log}"
    print("  Snapshot={'brightness': 50} -> rule сработала (brightness in fields)")

    # Snapshot содержит поле НЕ из fields -> НЕ срабатывает
    call_log.clear()
    await app.rules.handle_device_update("dev1", {"temperature": 25})
    assert len(call_log) == 0, f"НЕ должна сработать (temperature not in fields), call_log={call_log}"
    print("  Snapshot={'temperature': 25} -> rule НЕ сработала (temperature not in fields)")

    # Snapshot содержит И watched И не-watched -> срабатывает (any)
    call_log.clear()
    await app.rules.handle_device_update("dev1", {"power": False, "temperature": 30})
    assert len(call_log) == 1, f"Должна сработать (power in fields), call_log={call_log}"
    print("  Snapshot={'power': False, 'temperature': 30} -> rule сработала (power in fields)")

    # Код: if rule.fields and not any(field in snapshot for field in rule.fields): continue
    # any() = ЛЮБОЕ, не all()
    print("  Логика: any(field in snapshot for field in rule.fields) — ЛЮБОЕ, не ВСЕ")

    print("  OK: field filtering ANY проверен")


# ============================================================================
# 10. Snapshot is shallow copy (nested mutables can be modified)
# ============================================================================

async def demo_snapshot_shallow_copy():
    """Snapshot — dict(event.data), shallow copy.  Вложенные mutable можно изменить."""
    print("\n--- 10. Snapshot is shallow copy ---")

    app = MockApp()

    received_data: list[dict] = []

    async def my_rule(event: RuleEvent, app):
        # event.data — это shallow copy оригинального snapshot
        received_data.append(event.data)

    rule = Rule(func=my_rule, fields=["config"], description="shallow")
    app.rules.add_rule(rule)

    # Snapshot с вложенным mutable
    original_snapshot = {"config": {"threshold": 10, "list": [1, 2, 3]}}
    await app.rules.handle_device_update("dev1", original_snapshot)

    assert len(received_data) == 1
    event_data = received_data[0]

    # event.data — shallow copy: верхний уровень новый
    assert event_data is not original_snapshot, "dict() создаёт новый dict"
    print("  event.data — новый dict (shallow copy верхнего уровня)")

    # НО вложенные объекты — те же ссылки!
    assert event_data["config"] is original_snapshot["config"], (
        "Вложенный dict — та же ссылка (shallow copy)"
    )
    print("  event.data['config'] — ТАКАЯ ЖЕ ссылка, как в оригинале (shallow!)")

    # ПОДВОХ: изменение вложенного объекта в rule влияет на оригинал
    event_data["config"]["threshold"] = 999
    assert original_snapshot["config"]["threshold"] == 999, (
        "Оригинал изменён через shallow copy!"
    )
    print("  После event.data['config']['threshold']=999 -> оригинал тоже 999!")

    # Также и в Rule.run: snapshot = dict(event.data) — тоже shallow
    print("  ВНИМАНИЕ: Rule.run делает dict(event.data) — тоже shallow copy")

    # ПРАВИЛЬНЫЙ ПОДХОД: deepcopy, если нужно изолировать
    import copy
    safe_copy = copy.deepcopy(original_snapshot)
    safe_copy["config"]["threshold"] = 0
    assert original_snapshot["config"]["threshold"] == 999, "deepcopy изолирует"
    print("  ПРАВИЛЬНО: использовать copy.deepcopy для изоляции вложенных объектов")

    print("  OK: snapshot shallow copy проверен")


# ============================================================================
# 11. Base class matching: base_type could be None
# ============================================================================

async def demo_base_class_matching():
    """handle_device_update проверяет MRO — base_type может быть None."""
    print("\n--- 11. Base class matching: base_type could be None ---")

    app = MockApp()

    call_count = 0

    async def base_rule(event: RuleEvent, app):
        nonlocal call_count
        call_count += 1

    # Rule для базового класса Device
    rule = Rule(func=base_rule, device_class=Device, fields=["power"], description="base")
    app.rules.add_rule(rule)

    # Создаём производный класс
    class SmartLight(Device):
        power: bool = state(default=False, writable=True)

    device = SmartLight()

    # Mock registry возвращает экземпляр SmartLight
    app.registry.get_instance = MagicMock(return_value=device)

    # handle_device_update проверяет MRO:
    # for base in type(device_instance).__mro__:
    #     if base is type(device_instance): continue
    #     base_type = getattr(base, "device_type", lambda: None)()
    #     if base_type:
    #         candidates.extend(self._event_rules_by_type.get(base_type, []))
    #
    # SmartLight.__mro__ = [SmartLight, Device, TelemetryMixin, TaskManagerMixin, object]
    # Device.device_type() = "device" -> base_type = "device"
    # TaskManagerMixin.device_type -> нет -> lambda: None -> base_type = None -> skip
    # object.device_type -> нет -> lambda: None -> base_type = None -> skip

    await app.rules.handle_device_update("smart1", {"power": True})

    # Rule для Device должна сработать, т.к. SmartLight наследует Device
    assert call_count > 0, f"Rule для Device должна сработать для SmartLight, call_count={call_count}"
    print(f"  Rule для Device сработала для SmartLight (MRO matching), call_count={call_count}")

    # Проверка: base_type может быть None для не-Device классов в MRO
    for base in SmartLight.__mro__:
        if base is SmartLight:
            continue
        base_type = getattr(base, "device_type", lambda: None)()
        if base_type:
            print(f"  {base.__name__}.device_type() = '{base_type}' -> candidates добавлены")
        else:
            print(f"  {base.__name__}.device_type() = None -> пропущен")

    print("  OK: base class matching проверен")


# ============================================================================
# 12. _rebuild_index on set_rules but not on add_rule/remove_rule
# ============================================================================

async def demo_rebuild_index_only_set_rules():
    """_rebuild_index вызывается в set_rules, но НЕ в add_rule/remove_rule."""
    print("\n--- 12. _rebuild_index on set_rules but not add/remove ---")

    app = MockApp()

    async def rule_a(event: RuleEvent, app):
        pass

    async def rule_b(event: RuleEvent, app):
        pass

    # add_rule обновляет индекс инкрементально (не через _rebuild_index)
    r1 = Rule(func=rule_a, fields=["power"], description="a")
    r2 = Rule(func=rule_b, fields=["brightness"], description="b")
    app.rules.add_rule(r1)
    app.rules.add_rule(r2)

    # Индекс должен содержать обе rules (добавлены инкрементально)
    assert r1 in app.rules._event_rules_by_type[None]
    assert r2 in app.rules._event_rules_by_type[None]
    print("  add_rule: индекс обновлён инкрементально (без _rebuild_index)")

    # remove_rule тоже обновляет инкрементально
    app.rules.remove_rule(r1)
    assert r1 not in app.rules._event_rules_by_type.get(None, [])
    assert r2 in app.rules._event_rules_by_type[None]
    print("  remove_rule: индекс обновлён инкрементально (без _rebuild_index)")

    # set_rules вызывает _rebuild_index (полная перестройка)
    r3 = Rule(func=rule_a, fields=["voltage"], description="c")
    await app.rules.set_rules([r3])

    # После set_rules индекс полностью перестроен
    assert r2 not in app.rules._event_rules_by_type.get(None, []), "r2 должен быть удалён из индекса"
    assert r3 in app.rules._event_rules_by_type[None], "r3 должен быть в индексе"
    print("  set_rules: _rebuild_index вызван — полная перестройка индекса")

    # ПРОВЕРКА: _rebuild_index фильтрует interval rules
    r_interval = Rule(func=rule_a, interval=5.0, description="interval")
    r_event = Rule(func=rule_b, fields=["power"], description="event")
    await app.rules.set_rules([r_interval, r_event])

    # _rebuild_index: if rule.interval is not None: continue
    # -> interval rule НЕ попадает в _event_rules_by_type
    assert r_interval not in app.rules._event_rules_by_type.get(None, [])
    assert r_event in app.rules._event_rules_by_type[None]
    print("  _rebuild_index: interval rules НЕ попадают в event-индекс")

    print("  OK: _rebuild_index on set_rules проверен")


# ============================================================================
# 13. Interval rules silently filtered in handle_device_update
# ============================================================================

async def demo_interval_filtered_in_handle_update():
    """Interval rules молча фильтруются в handle_device_update."""
    print("\n--- 13. Interval rules silently filtered in handle_device_update ---")

    app = MockApp()

    event_count = 0

    async def interval_rule(event: RuleEvent, app):
        nonlocal event_count
        event_count += 1

    # Rule с interval (без fields)
    rule = Rule(func=interval_rule, interval=5.0, description="interval only")
    app.rules.add_rule(rule)

    # handle_device_update: if not rule.enabled or rule.interval is not None: continue
    # -> interval rule пропускается БЕЗ логирования
    await app.rules.handle_device_update("dev1", {"power": True})

    assert event_count == 0, "Interval rule не должна срабатывать через handle_device_update"
    print("  Interval rule пропущена в handle_device_update — БЕЗ логирования")
    print("  Фильтр: rule.interval is not None -> continue")

    # ПРАВИЛЬНЫЙ ПОДХОД: не смешивать interval и event rules
    print("  ПРАВИЛЬНО: не использовать interval rule для обработки событий устройства")

    print("  OK: interval filtered in handle_device_update проверен")


# ============================================================================
# Main
# ============================================================================

async def main():
    print("=" * 70)
    print("24 — Rules Engine Internals (внутренности движка правил)")
    print("=" * 70)

    await demo_device_level_detection()
    await demo_parameter_count_dispatch()
    await demo_interval_starts_immediately()
    await demo_interval_and_fields_never_event()
    await demo_remove_rule_no_lock()
    await demo_add_rule_no_lock()
    await demo_start_stop_no_lock()
    await demo_disabled_rules_silent()
    await demo_field_filtering_any()
    await demo_snapshot_shallow_copy()
    await demo_base_class_matching()
    await demo_rebuild_index_only_set_rules()
    await demo_interval_filtered_in_handle_update()

    print("\n" + "=" * 70)
    print("ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
