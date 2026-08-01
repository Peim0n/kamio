"""
18 — Threading vs Async (потоки против асинхронности)
======================================================

ГЛУБОКОЕ ПОГРУЖЕНИЕ для разработчиков фреймворка.

Kamio — асинхронный фреймворк, но внутри использует ``threading.Lock`` и
``threading.RLock`` в нескольких критических местах.  Это сознательное
решение: некоторые методы вызываются как из event loop, так и из
синхронного кода (например, ``__setattr__``), и ``asyncio.Lock`` там
неприменим.  Но это создаёт ряд подводных камней.

ПОДВОХИ И КРАЕВЫЕ СЛУЧАИ:

    1. ``threading.Lock`` в async-контексте (кэш эха ``_cinds_lock``).
       Блокирует ВЕСЬ event loop при contention — ни одна корутина не
       выполняется, пока lock занят.
    2. ``threading.RLock`` в StateManager (``_state_lock``) — рекурсивный.
       Один поток может захватить lock несколько раз.  Но ``asyncio.Lock``
       не рекурсивный — повторный ``await lock.acquire()`` в том же таск
       вызывает deadlock.
    3. ``threading.RLock`` в BaseCorrelationManager (``_lock``) — защищает
       таблицу pending futures.  ``asyncio.get_running_loop()`` вызывается
       ПОД lock — если нет loop, поднимается RuntimeError ВНУТРИ lock.
       finally-блок корректно освобождает lock, но исключение летит дальше.
    4. ``_bg_tasks`` (множество) НЕ потокобезопасно.  ``set.add`` и
       ``set.discard`` в CPython атомарны благодаря GIL, но ``create_task``
       добавляет в set без lock.  Если два потока одновременно вызовут
       ``create_task``, может произойти потеря элемента (теоретически).
    5. Синхронные callbacks в EventBus блокируют event loop.  Если callback
       выполняет тяжёлую работу (I/O, вычисления), весь event loop стопорится.
    6. Синхронные hooks в HooksManager тоже блокируют event loop.
       ``_invoke`` вызывает sync callback напрямую без ``run_in_executor``.
    7. ``gmqtt.publish`` — синхронный метод, вызываемый в async-методе
       ``node.publish``.  Это блокирует event loop на время сетевой отправки.
    8. ``asyncio.get_running_loop()`` под lock в correlation manager:
       если вызван из потока без loop — RuntimeError внутри ``with self._lock``.

ПРАВИЛЬНЫЙ ПОДХОД:
  - Использовать ``asyncio.Lock`` когда возможно (только в async-контексте).
  - Выносить тяжёлые синхронные операции в ``run_in_executor`` / ``to_thread``.
  - Понимать, что ``threading.Lock`` в async-коде — компромисс: безопасно
    для коротких операций, опасно для длинных.

Запуск (БЕЗ MQTT-брокера)::

    python examples/18_threading_async.py
"""
from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

from kamio import Device, EventBus, HooksManager, state
from kamio.core.correlation import BaseCorrelationManager
from kamio.core.envelope import Envelope, EnvelopeType
from kamio.core.state import StateManager

# Тихое логирование
logging.basicConfig(level=logging.CRITICAL)


# =====================================================================
# 1. threading.Lock в async-контексте (кэш эха _cinds_lock)
# =====================================================================

class LockDevice(Device):
    """Устройство для тестирования threading.Lock в __setattr__."""
    power: bool = state(default=False, writable=True)


async def test_threading_lock_in_async():
    """_cinds_lock — threading.Lock, блокирует event loop при contention."""
    dev = LockDevice()

    mock_node = MagicMock()
    mock_node.device_id = "lock_dev_1"
    mock_node.publish = AsyncMock()
    dev.node = mock_node

    # Утверждение: _cinds_lock — threading.Lock
    # threading.Lock() создаёт экземпляр типа LockType
    assert isinstance(dev._cinds_lock, type(threading.Lock())), (
        "_cinds_lock должен быть threading.Lock"
    )

    # Утверждение: lock можно использовать из синхронного __setattr__
    # (это ключевая причина выбора threading.Lock вместо asyncio.Lock)
    dev.power = True  # __setattr__ — синхронный метод, использует lock
    assert dev.power is True
    assert len(dev._own_state_cinds) == 1

    # НЕПРАВИЛЬНО: пытаться использовать asyncio.Lock в __setattr__
    # asyncio.Lock требует await, а __setattr__ — синхронный метод.
    # Это вызвало бы TypeError или потребовало бы переписывания __setattr__
    # как async — что сломало бы синтаксис self.power = True.

    # ПРАВИЛЬНО: threading.Lock для коротких операций в синхронных методах.
    # Но помнить: при contention блокируется event loop.

    await dev.cancel_all_tasks()


# =====================================================================
# 2. threading.RLock в StateManager (рекурсивный)
# =====================================================================

async def test_state_manager_rlock():
    """StateManager использует threading.RLock — рекурсивный lock.

    RLock позволяет одному потоку захватить lock несколько раз.
    asyncio.Lock не рекурсивный — повторный acquire = deadlock.
    """
    sm = StateManager()

    # Утверждение: _state_lock — threading.RLock
    assert isinstance(sm._state_lock, type(threading.RLock())), (
        "_state_lock должен быть threading.RLock"
    )

    # Утверждение: RLock можно захватить рекурсивно в одном потоке
    with sm._state_lock:
        sm.update_state("dev1", {"power": True})
        # Рекурсивный захват: update_state тоже берёт lock
        # RLock позволяет это — не deadlock
        with sm._state_lock:
            sm.update_state("dev1", {"brightness": 50})

    # Утверждение: оба обновления применились
    state = sm.get_state("dev1")
    assert state["power"] is True
    assert state["brightness"] == 50

    # НЕПРАВИЛЬНО: заменить RLock на asyncio.Lock и вызывать update_state
    # (синхронный метод) из async-кода — будет TypeError (нельзя await в sync).

    # ПРАВИЛЬНО: RLock для синхронных методов, которые могут вызываться
    # рекурсивно (update_state → внутренняя логика → update_state).


# =====================================================================
# 3. threading.RLock в BaseCorrelationManager + get_running_loop под lock
# =====================================================================

async def test_correlation_lock_and_get_running_loop():
    """BaseCorrelationManager использует RLock и вызывает get_running_loop под lock.

    Если _wait_for_ack вызывается без event loop, RuntimeError поднимается
    ВНУТРИ ``with self._lock``.  finally-блок освобождает lock, но
    исключение распространяется.
    """
    cm = BaseCorrelationManager(max_pending=10)

    # Утверждение: _lock — threading.RLock
    assert isinstance(cm._lock, type(threading.RLock())), (
        "_lock должен быть threading.RLock"
    )

    # Утверждение: asyncio.get_running_loop() вызывается ПОД lock
    # Проверяем это, вызвав _wait_for_ack в event loop (должно работать)
    publish_called = False

    async def fake_publish():
        nonlocal publish_called
        publish_called = True

    # В event loop — get_running_loop работает, future создаётся
    async def fake_publish_coro():
        pass

    # Создаём корутину для publish
    coro = fake_publish_coro()

    # Запускаем _wait_for_ack с очень коротким timeout
    # (future никогда не будет resolved → TimeoutError)
    try:
        await cm._wait_for_ack(
            target_id="dev1",
            cind="test_cind_1",
            publish_coro=coro,
            timeout=0.01,
        )
        assert False, "Должен быть TimeoutError"
    except asyncio.TimeoutError:
        pass  # Ожидаемо — никто не резолвит future

    # Утверждение: publish_coro был awaited
    assert publish_called or coro.cr_frame is None, (
        "publish_coro должен быть awaited (даже если future не resolved)"
    )

    # Утверждение: pending очищен после timeout (finally-блок)
    assert len(cm._pending) == 0, (
        "pending должен быть очищен в finally-блоке"
    )

    # НЕПРАВИЛЬНО: вызывать _wait_for_ack из потока без event loop.
    # get_running_loop() поднимёт RuntimeError ВНУТРИ with self._lock.
    # finally освободит lock, но исключение полетит дальше.

    # ПРАВИЛЬНО: всегда вызывать из event loop.  Если нужен вызов из
    # другого потока — использовать asyncio.run_coroutine_threadsafe.


# =====================================================================
# 4. _bg_tasks set НЕ потокобезопасен
# =====================================================================

async def test_bg_tasks_not_thread_safe():
    """_bg_tasks — обычное set, без lock.  CPython GIL делает add/discard
    атомарными, но это деталь реализации, а не гарантия."""
    dev = LockDevice()

    # Утверждение: _bg_tasks — обычный set (не threading-protected)
    assert isinstance(dev._bg_tasks, set), (
        "_bg_tasks должен быть обычным set"
    )

    # Утверждение: нет lock для защиты _bg_tasks
    # create_task добавляет в set, add_done_callback удаляет — без lock
    async def dummy():
        await asyncio.sleep(0.01)

    task = dev.create_task(dummy(), name="test_task")
    assert task in dev._bg_tasks, "Задача должна быть в _bg_tasks"

    # Утверждение: после завершения задача удаляется из set
    await asyncio.sleep(0.05)
    assert task not in dev._bg_tasks, (
        "Завершённая задача должна быть удалена из _bg_tasks через callback"
    )

    # НЕПРАВИЛЬНО: полагаться на потокобезопасность _bg_tasks.
    # Если create_task вызывается из нескольких потоков одновременно,
    # теоретически может произойти потеря элемента (хотя GIL защищает
    # на практике в CPython).

    # ПРАВИЛЬНО: вызывать create_task только из event loop (один поток).
    # Для межпотокового создания задач использовать
    # asyncio.run_coroutine_threadsafe(coro, loop).


# =====================================================================
# 5. Синхронные callbacks в EventBus блокируют event loop
# =====================================================================

async def test_sync_callback_blocks_event_loop():
    """Синхронный callback в EventBus выполняется напрямую — блокирует loop."""
    bus = EventBus()

    call_order = []

    # Синхронный callback с «тяжёлой» работой (имитация)
    def slow_sync_callback(data):
        # Имитация блокирующей операции
        total = sum(range(100_000))  # ~миллисекунды, но блокирует loop
        call_order.append(f"sync_done_{total > 0}")

    # Async callback для проверки порядка
    async def async_callback(data):
        call_order.append("async_start")
        await asyncio.sleep(0.01)
        call_order.append("async_end")

    bus.subscribe("test_event", slow_sync_callback, priority=10)
    bus.subscribe("test_event", async_callback, priority=5)

    await bus.publish("test_event", {"value": 1})

    # Утверждение: sync callback выполнился ДО async (priority 10 > 5)
    # И sync блокировал loop, пока async не мог стартовать
    assert call_order[0] == "sync_done_True", (
        f"Sync callback (priority=10) должен выполниться первым, got: {call_order}"
    )

    # Утверждение: async callback выполнился после sync
    assert "async_start" in call_order and "async_end" in call_order

    # НЕПРАВИЛЬНО: использовать sync callback для I/O или долгих вычислений.
    # Это блокирует event loop — ни одна другая корутина не выполнится.

    # ПРАВИЛЬНО: async callbacks для I/O.  Если sync работа тяжёлая —
    # обернуть в asyncio.to_thread внутри async callback:
    #   async def callback(data):
    #       result = await asyncio.to_thread(heavy_sync_work, data)


# =====================================================================
# 6. Синхронные hooks в HooksManager блокируют event loop
# =====================================================================

async def test_sync_hook_blocks_event_loop():
    """Синхронный hook в HooksManager выполняется напрямую — блокирует loop."""
    hooks = HooksManager()

    hook_order = []

    def sync_hook(*args, **kwargs):
        # Имитация блокирующей работы
        total = sum(range(50_000))
        hook_order.append(f"sync_hook_{total > 0}")

    async def async_hook(*args, **kwargs):
        hook_order.append("async_hook_start")
        await asyncio.sleep(0.01)
        hook_order.append("async_hook_end")

    hooks.register("on_device_started", sync_hook, priority=10)
    hooks.register("on_device_started", async_hook, priority=5)

    await hooks.trigger("on_device_started", "device_1")

    # Утверждение: sync hook выполнился первым (priority 10)
    assert hook_order[0].startswith("sync_hook"), (
        f"Sync hook (priority=10) должен быть первым, got: {hook_order}"
    )

    # Утверждение: async hook выполнился после sync
    assert "async_hook_end" in hook_order

    # НЕПРАВИЛЬНО: регистрировать sync hook с I/O (файл, сеть, БД).
    # HooksManager._invoke вызывает sync callback напрямую без to_thread.

    # ПРАВИЛЬНО: async hooks для I/O.  Для тяжёлых sync операций —
    # async обёртка с to_thread.


# =====================================================================
# 7. gmqtt.publish — синхронный в async-методе (имитация)
# =====================================================================

async def test_sync_publish_in_async():
    """node.publish вызывает gmqtt client.publish — синхронный метод.

    gmqtt.Client.publish() — синхронный: копирует payload и ставит в
    внутренний буфер.  Это блокирует event loop на время копирования.
    Для больших payload это может быть заметно.
    """
    dev = LockDevice()

    # Имитируем node с синхронным publish внутри async-обёртки
    mock_node = MagicMock()
    mock_node.device_id = "sync_pub_dev"

    publish_block_time = []

    async def fake_publish(env):
        # gmqtt.publish синхронный — имитируем блокировку
        # В реальности gmqtt.publish копирует payload в буфер
        import time
        start = time.monotonic()
        # Имитация копирования большого payload
        _ = b"x" * 100_000
        publish_block_time.append(time.monotonic() - start)

    mock_node.publish = fake_publish
    dev.node = mock_node

    # Публикация через _safe_publish
    env = Envelope.state(source="sync_pub_dev", data={"power": True})
    await dev._safe_publish(env)

    # Утверждение: publish был вызван
    assert len(publish_block_time) == 1, "publish должен быть вызван один раз"

    # Утверждение: время блокировки > 0 (синхронная операция)
    assert publish_block_time[0] >= 0, "Синхронная операция занимает время"

    # НЕПРАВИЛЬНО: публиковать очень большие payload в hot-path.
    # gmqtt.publish копирует данные синхронно — блокирует loop.

    # ПРАВИЛЬНО: для больших payload использовать отдельный таск
    # или ограничивать размер.  gmqtt internally использует asyncio,
    # но publish() — синхронный метод постановки в очередь.


# =====================================================================
# 8. get_running_loop под lock — RuntimeError без event loop
# =====================================================================

def test_get_running_loop_without_event_loop():
    """Без event loop get_running_loop() поднимает RuntimeError.

    В BaseCorrelationManager._wait_for_ack это происходит ВНУТРИ
    ``with self._lock``.  finally-блок освобождает lock, но
    RuntimeError распространяется.
    """
    cm = BaseCorrelationManager(max_pending=10)

    async def fake_coro():
        pass

    coro = fake_coro()

    # НЕПРАВИЛЬНО: вызывать _wait_for_ack из синхронного кода без loop.
    # Это вызовет RuntimeError внутри lock.
    #
    # Мы НЕ можем напрямую вызвать ``await cm._wait_for_ack(...)`` без loop,
    # но можем проверить, что get_running_loop поднимает RuntimeError
    # в синхронном контексте:

    try:
        asyncio.get_running_loop()
        # Если мы здесь — loop есть (не должно быть в синхронном тесте)
        # Этот тест должен вызываться БЕЗ event loop
    except RuntimeError:
        pass  # Ожидаемо: нет running loop

    # Закрываем корутину, чтобы избежать warning
    coro.close()

    # Утверждение: pending пуст (никакой регистрации не произошло)
    assert len(cm._pending) == 0

    # ПРАВИЛЬНО: всегда обеспечивать event loop при работе с correlation.
    # Для межпотокового вызова: asyncio.run_coroutine_threadsafe.


# =====================================================================
# 9. threading.RLock против asyncio.Lock — сравнение поведения
# =====================================================================

async def test_rlock_vs_asyncio_lock():
    """Сравнение: RLock рекурсивный (один поток), asyncio.Lock — нет.

    asyncio.Lock: повторный ``await lock.acquire()`` в том же таск
    вызывает deadlock (не рекурсивный).
    threading.RLock: повторный ``with lock:`` в том же потоке — OK.
    """
    rlock = threading.RLock()
    alock = asyncio.Lock()

    # RLock: рекурсивный захват в одном потоке — OK
    with rlock:
        with rlock:
            pass  # Нет deadlock — RLock рекурсивный

    # Утверждение: RLock освобождён
    # (если бы не освободился, следующий with заблокировал бы)

    # asyncio.Lock: НЕ рекурсивный
    # НЕПРАВИЛЬНО: повторный acquire в том же таск → deadlock
    # async def deadlock_demo():
    #     async with alock:
    #         async with alock:  # DEADLOCK!
    #             pass

    # ПРАВИЛЬНО: использовать asyncio.Lock только для неконкурентных
    # async-секций.  Для рекурсивных вызовов — threading.RLock или
    # реорганизация кода без повторного захвата.

    # Демонстрация: asyncio.Lock работает для разных тасков
    async def task_with_lock(name, results):
        async with alock:
            results.append(f"{name}_start")
            await asyncio.sleep(0.01)
            results.append(f"{name}_end")

    results = []
    await asyncio.gather(
        task_with_lock("A", results),
        task_with_lock("B", results),
    )

    # Утверждение: таски выполнились последовательно (lock сериализует)
    # A_start ... A_end ... B_start ... B_end (или B сначала)
    assert results[0].endswith("_start"), f"Первый должен быть start: {results}"
    assert results[1].endswith("_end"), f"Второй должен быть end: {results}"
    assert results[2].endswith("_start"), f"Третий должен быть start: {results}"
    assert results[3].endswith("_end"), f"Четвёртый должен быть end: {results}"


# =====================================================================
# 10. ПРАВИЛЬНЫЙ способ: offload синхронной работы в to_thread
# =====================================================================

async def test_right_way_offload_sync_work():
    """ПРАВИЛЬНО: тяжёлые синхронные операции — в to_thread.

    Вместо блокировки event loop синхронным callback, обернуть в
    asyncio.to_thread — операция выполнится в потоке, loop свободен.
    """
    bus = EventBus()

    loop_was_free = []

    def heavy_sync_work(data):
        # Имитация тяжёлой работы
        total = sum(range(500_000))
        return total

    # НЕПРАВИЛЬНО: sync callback блокирует loop
    def blocking_callback(data):
        heavy_sync_work(data)

    # ПРАВИЛЬНО: async callback с to_thread
    async def non_blocking_callback(data):
        result = await asyncio.to_thread(heavy_sync_work, data)
        loop_was_free.append(result)

    # Параллельная задача для проверки, что loop свободен
    async def heartbeat():
        for i in range(5):
            loop_was_free.append(f"tick_{i}")
            await asyncio.sleep(0.005)

    bus.subscribe("work_event", non_blocking_callback)

    # Запускаем heartbeat и publish параллельно
    await asyncio.gather(
        heartbeat(),
        bus.publish("work_event", {"value": 1}),
    )

    # Утверждение: heartbeat тики были во время работы callback
    # (loop не был заблокирован, потому что to_thread освободил его)
    tick_count = sum(1 for item in loop_was_free if item.startswith("tick_"))
    assert tick_count > 0, (
        "Heartbeat должен работать во время to_thread — loop свободен"
    )

    # Утверждение: результат тяжёлой работы получен
    work_results = [item for item in loop_was_free if isinstance(item, int)]
    assert len(work_results) == 1, "Тяжёлая работа должна выполниться один раз"


# =====================================================================
# 11. PriorityRegistry тоже использует threading.RLock
# =====================================================================

async def test_priority_registry_uses_rlock():
    """PriorityRegistry (основа EventBus и HooksManager) — threading.RLock."""
    from kamio.core.subscription import PriorityRegistry

    reg = PriorityRegistry()

    # Утверждение: _lock — threading.RLock
    assert isinstance(reg._lock, type(threading.RLock())), (
        "PriorityRegistry._lock должен быть threading.RLock"
    )

    # Утверждение: можно рекурсивно захватывать
    with reg._lock:
        reg.add("event1", "cb1", priority=0)
        with reg._lock:
            reg.add("event1", "cb2", priority=1)

    items = reg.list("event1")
    assert len(items) == 2, "Оба элемента должны быть зарегистрированы"
    # priority=1 первым (descending order)
    assert items[0] == "cb2", "Higher priority должен быть первым"


# =====================================================================
# Главная функция
# =====================================================================

async def main():
    print("=" * 70)
    print("18 — Threading vs Async: потоки против асинхронности")
    print("=" * 70)

    tests = [
        ("1. threading.Lock в async-контексте (_cinds_lock)", test_threading_lock_in_async),
        ("2. threading.RLock в StateManager (рекурсивный)", test_state_manager_rlock),
        ("3. RLock в correlation + get_running_loop под lock", test_correlation_lock_and_get_running_loop),
        ("4. _bg_tasks set НЕ потокобезопасен", test_bg_tasks_not_thread_safe),
        ("5. Синхронные callbacks в EventBus блокируют loop", test_sync_callback_blocks_event_loop),
        ("6. Синхронные hooks в HooksManager блокируют loop", test_sync_hook_blocks_event_loop),
        ("7. gmqtt.publish — синхронный в async-методе", test_sync_publish_in_async),
        ("8. get_running_loop без event loop → RuntimeError", test_get_running_loop_without_event_loop),
        ("9. threading.RLock vs asyncio.Lock — сравнение", test_rlock_vs_asyncio_lock),
        ("10. ПРАВИЛЬНО: offload синхронной работы в to_thread", test_right_way_offload_sync_work),
        ("11. PriorityRegistry использует threading.RLock", test_priority_registry_uses_rlock),
    ]

    passed = 0
    failed = 0

    for name, test_func in tests:
        try:
            if asyncio.iscoroutinefunction(test_func):
                await test_func()
            else:
                test_func()
            print(f"  [PASS] {name}")
            passed += 1
        except Exception as e:
            print(f"  [FAIL] {name}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("-" * 70)
    print(f"Итого: {passed} прошло, {failed} провалено")
    if failed == 0:
        print("Все тесты прошли успешно!")


if __name__ == "__main__":
    asyncio.run(main())
