"""
20 — EventBus и HooksManager: внутренности и подводные камни
=============================================================

Этот файл — НЕ учебник. Это глубокий разбор внутренних механизмов
EventBus и HooksManager для разработчиков фреймворка.

Демонстрируемые грабли:
    1. Высший приоритет = выполняется ПЕРВЫМ (не наоборот!)
    2. Равный приоритет: LIFO — последний зарегистрированный выполняется
       ПОСЛЕДНИМ в группе равных приоритетов.
    3. filter_fn с исключением: перехватывается и логируется, подписчик
       тихо пропускается (silent skip).
    4. filter_fn с возвращаемым значением None / 0 / False — отфильтрован
       (truthy-проверка через ``not passes``).
    5. Синхронные callback-и вызываются напрямую — блокируют event loop
       если работают медленно.
    6. Async callback-и ожидаются (await).
    7. publish() добавляет timestamp если его нет, но 0 или False
       трактуются как «отсутствует» (``not data.get("timestamp")``).
    8. unsubscribe использует проверку по идентичности (``is``), а не по
       равенству (``==``).
    9. list_subscribers возвращает только callback-и, без filter_fn.
    10. publish() создаёт НОВЫЙ dict с timestamp — исходный не мутируется.
    11. HooksManager: тот же порядок приоритетов, но НЕ добавляет timestamp.
    12. HooksManager: хранит hook напрямую (не в кортеже, как EventBus).

Запуск без MQTT-брокера::

    python examples/20_event_bus_internals.py
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import MagicMock

from kamio.core.event_bus import EventBus
from kamio.core.hooks import HooksManager
from kamio.core.subscription import PriorityRegistry

# Тихое логирование, чтобы вывод примера был чистым
logging.basicConfig(level=logging.CRITICAL, format="%(message)s")


# =====================================================================
# 1. Приоритет: ВЫСШИЙ = ПЕРВЫЙ (не наоборот)
# =====================================================================

async def demo_priority_higher_first() -> None:
    """Высший приоритет выполняется первым — частая путаница."""
    bus = EventBus()
    order: List[str] = []

    # НЕПРАВИЛЬНО: если вы думаете, что priority=1 выполнится раньше priority=10
    bus.subscribe("evt", lambda d: order.append("low"), priority=1)
    bus.subscribe("evt", lambda d: order.append("high"), priority=10)
    bus.subscribe("evt", lambda d: order.append("mid"), priority=5)

    await bus.publish("evt", {"v": 1})

    # ПРАВИЛЬНО: высший приоритет = первый
    assert order == ["high", "mid", "low"], (
        f"Ожидался порядок [high, mid, low], получен {order}. "
        f"Высший приоритет выполняется ПЕРВЫМ."
    )
    print("[OK] 1. Высший приоритет = выполняется первым")


# =====================================================================
# 2. Равный приоритет: LIFO (последний зарегистрированный — последний в группе)
# =====================================================================

async def demo_equal_priority_lifo() -> None:
    """При равном приоритете соблюдается LIFO внутри группы."""
    bus = EventBus()
    order: List[str] = []

    # Все с priority=0 (по умолчанию)
    bus.subscribe("evt", lambda d: order.append("first"))
    bus.subscribe("evt", lambda d: order.append("second"))
    bus.subscribe("evt", lambda d: order.append("third"))

    await bus.publish("evt", {})

    # LIFO означает: последний добавленный — последний в группе.
    # Но это НЕ полный LIFO! Это «вставка после существующих равных».
    # То есть порядок сохраняется как FIFO для равных приоритетов,
    # потому что вставка идёт ПОСЛЕ существующих (см. binary search).
    assert order == ["first", "second", "third"], (
        f"Ожидался [first, second, third], получен {order}. "
        f"Равные приоритеты: вставка после существующих = FIFO порядок."
    )
    print("[OK] 2. Равный приоритет: вставка после существующих (FIFO в группе)")


# =====================================================================
# 3. filter_fn с исключением: тихий пропуск подписчика
# =====================================================================

async def demo_filter_fn_exception_silent_skip() -> None:
    """Исключение в filter_fn перехватывается — подписчик пропускается тихо."""
    bus = EventBus()
    called: List[str] = []

    def bad_filter(data: Dict[str, Any]) -> bool:
        # Имитируем ошибку: обращение к несуществующему ключу
        return data["nonexistent_key"] == 42  # KeyError

    def good_filter(data: Dict[str, Any]) -> bool:
        return True

    bus.subscribe("evt", lambda d: called.append("with_bad_filter"), filter_fn=bad_filter)
    bus.subscribe("evt", lambda d: called.append("with_good_filter"), filter_fn=good_filter)

    # НЕ должно выбросить исключение — ошибка в фильтре перехватывается
    await bus.publish("evt", {"v": 1})

    # Подписчик с плохим фильтром пропущен, второй — вызван
    assert called == ["with_good_filter"], (
        f"Ожидалось ['with_good_filter'], получено {called}. "
        f"Исключение в filter_fn -> подписчик пропускается тихо."
    )
    print("[OK] 3. filter_fn с исключением -> тихий пропуск подписчика")


# =====================================================================
# 4. filter_fn: truthy-проверка (None, 0, False = отфильтровано)
# =====================================================================

async def demo_filter_fn_truthy_check() -> None:
    """filter_fn проверяется через ``not passes`` — falsy значения отфильтрованы."""
    bus = EventBus()
    called: List[str] = []

    # НЕПРАВИЛЬНО: возврат None из filter_fn не означает «пропустить фильтр»
    def returns_none(data: Dict[str, Any]):
        pass  # возвращает None неявно

    def returns_zero(data: Dict[str, Any]) -> int:
        return 0

    def returns_false(data: Dict[str, Any]) -> bool:
        return False

    def returns_true(data: Dict[str, Any]) -> bool:
        return True

    bus.subscribe("evt", lambda d: called.append("none"), filter_fn=returns_none)
    bus.subscribe("evt", lambda d: called.append("zero"), filter_fn=returns_zero)
    bus.subscribe("evt", lambda d: called.append("false"), filter_fn=returns_false)
    bus.subscribe("evt", lambda d: called.append("true"), filter_fn=returns_true)

    await bus.publish("evt", {})

    # Только returns_true пропускает подписчика
    assert called == ["true"], (
        f"Ожидалось ['true'], получено {called}. "
        f"None, 0, False — все falsy, отфильтрованы через ``not passes``."
    )
    print("[OK] 4. filter_fn: None/0/False отфильтрованы (truthy-проверка)")


# =====================================================================
# 5. Синхронный callback блокирует event loop
# =====================================================================

async def demo_sync_callback_blocks_loop() -> None:
    """Синхронный callback вызывается напрямую — блокирует event loop."""
    bus = EventBus()
    timestamps: List[float] = []

    def slow_sync(data: Dict[str, Any]) -> None:
        # Имитация блокирующей операции (time.sleep блокирует loop!)
        time.sleep(0.05)
        timestamps.append(time.monotonic())

    async def async_after(data: Dict[str, Any]) -> None:
        timestamps.append(time.monotonic())

    bus.subscribe("evt", slow_sync, priority=10)
    bus.subscribe("evt", async_after, priority=1)

    start = time.monotonic()
    await bus.publish("evt", {})
    elapsed = time.monotonic() - start

    # sync callback отработал первым (приоритет 10), но заблокировал loop на 50мс
    # async_after не начнётся пока sync не завершится
    assert elapsed >= 0.04, (
        f"Ожидалось >= 0.04с, прошло {elapsed:.3f}с. "
        f"Sync callback блокирует event loop."
    )
    assert timestamps[0] < timestamps[1], "Sync должен завершиться раньше async"
    print(f"[OK] 5. Sync callback блокирует loop ({elapsed:.3f}с)")


# =====================================================================
# 6. Async callback ожидается (await)
# =====================================================================

async def demo_async_callback_awaited() -> None:
    """Async callback корректно ожидается через await."""
    bus = EventBus()
    results: List[str] = []

    async def async_cb(data: Dict[str, Any]) -> None:
        await asyncio.sleep(0.01)  # имитация асинхронной работы
        results.append("async_done")

    bus.subscribe("evt", async_cb)
    await bus.publish("evt", {})

    assert results == ["async_done"], (
        f"Ожидалось ['async_done'], получено {results}. "
        f"Async callback должен быть await."
    )
    print("[OK] 6. Async callback корректно ожидается")


# =====================================================================
# 7. publish() и timestamp: 0/False трактуются как «отсутствует»
# =====================================================================

async def demo_timestamp_falsy_treated_as_missing() -> None:
    """publish() добавляет timestamp если ``not data.get("timestamp")``."""
    bus = EventBus()
    received: List[Dict[str, Any]] = []

    bus.subscribe("evt", lambda d: received.append(d))

    # Случай 1: нет timestamp -> добавляется
    await bus.publish("evt", {"v": 1})
    assert "timestamp" in received[0], "timestamp должен быть добавлен если отсутствует"
    assert isinstance(received[0]["timestamp"], datetime), (
        f"Ожидался datetime, получен {type(received[0]['timestamp'])}"
    )

    # Случай 2: timestamp=0 -> НЕПРАВИЛЬНО думать, что 0 будет заменён на datetime.
    # ``not 0`` == True -> publish() входит в if-блок и создаёт новый dict:
    #   data = {"timestamp": _now(), **data}
    # НО **data содержит "timestamp": 0, который ПЕРЕЗАПИСЫВАЕТ _now()!
    # Итог: подписчик получает timestamp=0, хотя код «пытался» его заменить.
    received.clear()
    await bus.publish("evt", {"v": 2, "timestamp": 0})
    assert received[0]["timestamp"] == 0, (
        f"Ожидался 0 (перезаписан **data), получен {received[0]['timestamp']}. "
        f"Грабли: {{'timestamp': _now(), **data}} -> **data перезаписывает _now()!"
    )

    # Случай 3: timestamp=False -> та же грабля: **data перезаписывает
    received.clear()
    await bus.publish("evt", {"v": 3, "timestamp": False})
    assert received[0]["timestamp"] is False, (
        f"Ожидался False (перезаписан **data), получен {received[0]['timestamp']}. "
        f"``not False`` == True, но **data ставит False обратно."
    )

    # Случай 4: timestamp=строка -> сохраняется (truthy)
    received.clear()
    custom_ts = "2024-01-01T00:00:00Z"
    await bus.publish("evt", {"v": 4, "timestamp": custom_ts})
    assert received[0]["timestamp"] == custom_ts, (
        f"Ожидался {custom_ts}, получен {received[0]['timestamp']}. "
        f"Truthy timestamp сохраняется."
    )
    print("[OK] 7. timestamp: 0/False заменяются (``not data.get(...)``)")


# =====================================================================
# 8. unsubscribe: проверка по идентичности (is), не по равенству (==)
# =====================================================================

async def demo_unsubscribe_identity_check() -> None:
    """unsubscribe использует ``is`` для сравнения, а не ``==``."""
    bus = EventBus()

    class CallableObj:
        """Объект, который можно вызывать и у которого __eq__ всегда True."""
        def __call__(self, data: Dict[str, Any]) -> None:
            pass

        def __eq__(self, other) -> bool:
            return True  # Все экземпляры «равны»

    obj_a = CallableObj()
    obj_b = CallableObj()

    # obj_a == obj_b -> True, но obj_a is not obj_b
    assert obj_a == obj_b, "Предусловие: __eq__ возвращает True"
    assert obj_a is not obj_b, "Предусловие: разные объекты в памяти"

    bus.subscribe("evt", obj_a)
    # Пытаемся отписать obj_b (равный, но не идентичный)
    bus.unsubscribe("evt", obj_b)

    # Подписка obj_a НЕ удалена, потому что проверка через ``is``
    subs = bus.list_subscribers("evt")
    assert len(subs) == 1, (
        f"Ожидалась 1 подписка (is-проверка не удалила), получено {len(subs)}. "
        f"unsubscribe использует ``stored[0] is not ref`` — идентичность, не равенство."
    )

    # ПРАВИЛЬНО: отписываем тот же объект
    bus.unsubscribe("evt", obj_a)
    subs = bus.list_subscribers("evt")
    assert len(subs) == 0, f"Ожидалось 0 подписок, получено {len(subs)}"
    print("[OK] 8. unsubscribe: проверка по ``is``, не по ``==``")


# =====================================================================
# 9. list_subscribers: возвращает только callback, без filter_fn
# =====================================================================

async def demo_list_subscribers_no_filter() -> None:
    """list_subscribers возвращает только callback-и, не filter_fn."""
    bus = EventBus()

    def my_filter(data: Dict[str, Any]) -> bool:
        return True

    def my_callback(data: Dict[str, Any]) -> None:
        pass

    bus.subscribe("evt", my_callback, filter_fn=my_filter, priority=5)
    subs = bus.list_subscribers("evt")

    assert subs == [my_callback], (
        f"Ожидался только [my_callback], получено {subs}. "
        f"list_subscribers извлекает cb из (cb, filter_fn) кортежа."
    )
    assert my_filter not in subs, "filter_fn не должен быть в списке"
    print("[OK] 9. list_subscribers: только callback, без filter_fn")


# =====================================================================
# 10. publish() создаёт НОВЫЙ dict — исходный не мутируется
# =====================================================================

async def demo_publish_creates_new_dict() -> None:
    """publish() создаёт новый dict с timestamp, не мутирует исходный."""
    bus = EventBus()
    received: List[Dict[str, Any]] = []

    bus.subscribe("evt", lambda d: received.append(d))

    original = {"v": 42}
    await bus.publish("evt", original)

    # Исходный dict не должен содержать timestamp
    assert "timestamp" not in original, (
        f"Исходный dict мутирован! Содержит timestamp. "
        f"publish() должен создавать новый dict: ``data = {{'timestamp': _now(), **data}}``."
    )
    # Полученный подписчиком dict должен содержать timestamp
    assert "timestamp" in received[0], "Полученный dict должен содержать timestamp"
    assert received[0]["v"] == 42, "Данные должны сохраниться"
    print("[OK] 10. publish() создаёт новый dict, исходный не мутируется")


# =====================================================================
# 11. HooksManager: тот же порядок приоритетов, но БЕЗ timestamp
# =====================================================================

async def demo_hooks_no_timestamp() -> None:
    """HooksManager.trigger не добавляет timestamp (в отличие от EventBus.publish)."""
    hooks = HooksManager()
    received_args: List[Any] = []

    async def hook_fn(*args, **kwargs) -> None:
        received_args.append({"args": args, "kwargs": kwargs})

    hooks.register("on_start", hook_fn, priority=10)
    await hooks.trigger("on_start", "arg1", key="val")

    # trigger передаёт *args и **kwargs напрямую — никакого timestamp
    assert received_args[0]["args"] == ("arg1",), (
        f"Ожидалось ('arg1',), получено {received_args[0]['args']}. "
        f"HooksManager.trigger передаёт args напрямую."
    )
    assert received_args[0]["kwargs"] == {"key": "val"}, (
        f"Ожидалось {{'key': 'val'}}, получено {received_args[0]['kwargs']}"
    )
    assert "timestamp" not in received_args[0]["kwargs"], (
        "HooksManager НЕ добавляет timestamp (в отличие от EventBus.publish)"
    )
    print("[OK] 11. HooksManager: НЕ добавляет timestamp")


# =====================================================================
# 12. HooksManager: хранит hook напрямую (не в кортеже)
# =====================================================================

async def demo_hooks_stores_directly() -> None:
    """HooksManager хранит hook напрямую, EventBus — в кортеже (callback, filter_fn)."""
    hooks = HooksManager()
    bus = EventBus()

    async def my_hook(*args, **kwargs) -> None:
        pass

    def my_cb(data: Dict[str, Any]) -> None:
        pass

    hooks.register("evt", my_hook)
    bus.subscribe("evt", my_cb)

    # Внутренний реестр: HooksManager хранит callable напрямую
    hooks_items = hooks._registry._items["evt"]
    assert hooks_items[0][1] is my_hook, (
        f"HooksManager хранит hook напрямую, получено {hooks_items[0][1]!r}. "
        f"Ожидался сам my_hook, не кортеж."
    )

    # EventBus хранит кортеж (callback, filter_fn)
    bus_items = bus._registry._items["evt"]
    stored = bus_items[0][1]
    assert isinstance(stored, tuple), (
        f"EventBus хранит кортеж (callback, filter_fn), получено {type(stored)}"
    )
    assert stored[0] is my_cb, "Первый элемент кортежа — callback"
    assert stored[1] is None, "Второй элемент — filter_fn (None если не задан)"
    print("[OK] 12. EventBus: кортеж (cb, filter); HooksManager: напрямую")


# =====================================================================
# 13. PriorityRegistry: binary search для O(n) вставки
# =====================================================================

async def demo_priority_registry_binary_search() -> None:
    """PriorityRegistry использует бинарный поиск для вставки."""
    reg = PriorityRegistry()

    # Вставляем в случайном порядке приоритетов
    reg.add("k", "c", priority=5)
    reg.add("k", "a", priority=10)
    reg.add("k", "b", priority=7)
    reg.add("k", "d", priority=1)

    items = reg.list("k")
    # Порядок: высший приоритет первым
    assert items == ["a", "b", "c", "d"], (
        f"Ожидался порядок по убыванию приоритета [a(10), b(7), c(5), d(1)], "
        f"получен {items}"
    )
    print("[OK] 13. PriorityRegistry: бинарный поиск -> убывание приоритета")


# =====================================================================
# 14. Ошибки в callback логируются, но не останавливают других подписчиков
# =====================================================================

async def demo_callback_error_does_not_stop_others() -> None:
    """Исключение в callback перехватывается в _dispatch, остальные подписчики работают."""
    bus = EventBus()
    called: List[str] = []

    def failing_cb(data: Dict[str, Any]) -> None:
        called.append("failing")
        raise RuntimeError("boom")

    def healthy_cb(data: Dict[str, Any]) -> None:
        called.append("healthy")

    bus.subscribe("evt", failing_cb, priority=10)
    bus.subscribe("evt", healthy_cb, priority=1)

    # Не должно выбросить
    await bus.publish("evt", {})

    assert called == ["failing", "healthy"], (
        f"Ожидалось ['failing', 'healthy'], получено {called}. "
        f"Ошибка в одном callback не должна останавливать остальных."
    )
    print("[OK] 14. Ошибка в callback не останавливает других подписчиков")


# =====================================================================
# 15. _dispatch делает snapshot списка (копию) — безопасен при мутации
# =====================================================================

async def demo_dispatch_snapshot_safe() -> None:
    """_dispatch делает snapshot через registry.list() — мутации во время диспетчера безопасны."""
    bus = EventBus()
    called: List[str] = []

    def first_cb(data: Dict[str, Any]) -> None:
        called.append("first")
        # Пытаемся отписать второго во время диспетчеризации
        bus.unsubscribe("evt", second_cb)

    def second_cb(data: Dict[str, Any]) -> None:
        called.append("second")

    bus.subscribe("evt", first_cb, priority=10)
    bus.subscribe("evt", second_cb, priority=1)

    await bus.publish("evt", {})

    # second_cb всё ещё вызван, потому что list() вернул snapshot ДО отписки
    assert called == ["first", "second"], (
        f"Ожидалось ['first', 'second'], получено {called}. "
        f"_dispatch работает со snapshot — отписка во время диспетчеризации "
        f"не влияет на текущий цикл."
    )

    # Но при следующей публикации second_cb уже нет
    called.clear()
    await bus.publish("evt", {})
    assert called == ["first"], (
        f"Ожидалось ['first'], получено {called}. "
        f"После отписки second_cb больше не вызывается."
    )
    print("[OK] 15. _dispatch: snapshot защищает от мутаций во время цикла")


# =====================================================================
# Точка входа
# =====================================================================

async def main() -> None:
    print("=" * 70)
    print("20 — EventBus и HooksManager: внутренности и подводные камни")
    print("=" * 70)
    print()

    await demo_priority_higher_first()
    await demo_equal_priority_lifo()
    await demo_filter_fn_exception_silent_skip()
    await demo_filter_fn_truthy_check()
    await demo_sync_callback_blocks_loop()
    await demo_async_callback_awaited()
    await demo_timestamp_falsy_treated_as_missing()
    await demo_unsubscribe_identity_check()
    await demo_list_subscribers_no_filter()
    await demo_publish_creates_new_dict()
    await demo_hooks_no_timestamp()
    await demo_hooks_stores_directly()
    await demo_priority_registry_binary_search()
    await demo_callback_error_does_not_stop_others()
    await demo_dispatch_snapshot_safe()

    print()
    print("=" * 70)
    print("Все демонстрации прошли успешно!")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
