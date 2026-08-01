"""
17 — Echo Suppression (подавление эха MQTT)
============================================

ГЛУБОКОЕ ПОГРУЖЕНИЕ для разработчиков фреймворка.

Когда устройство меняет состояние напрямую (``self.power = True``), Kamio
публикует новое состояние в MQTT.  Многие брокеры (с retain=1 или loopback
подпиской) возвращают это сообщение обратно — «эхо».  Без подавления эха
устройство получило бы своё же сообщение и применило его повторно.

Механизм подавления эха:
  - При ``__setattr__`` для state-поля создаётся Envelope с уникальным ``cind``.
  - ``cind`` добавляется в ``_own_state_cinds`` (множество) ДО публикации.
  - Когда эхо возвращается через ``DeviceHandler._handle_state``, проверяется
    наличие ``cind`` в множестве.  Если найден — сообщение игнорируется.

ПОДВОХИ И КРАЕВЫЕ СЛУЧАИ:

    1. Прямое присваивание state вызывает публикацию MQTT ТОЛЬКО если self.node
       существует (не None).  Без node изменение применяется локально, но не
       публикуется — тихо и без предупреждения.
    2. Если нет event loop (``RuntimeError`` от ``get_running_loop``), корутина
       публикации закрывается через ``coro.close()``.  Изменение применяется
       локально, но НЕ публикуется.  Логируется warning.
    3. Кэш эха ``_own_state_cinds`` ограничен 4096 записями.  При переполнении
       самые старые cinds вытесняются → подавление эха перестаёт работать для
       вытесненных cinds.  Это может произойти при массовом обновлении без
       получения эхо-ответов (например, брокер недоступен).
    4. cind добавляется в кэш ДО публикации (внутри ``with _cinds_lock``).
       Это создаёт окно для race condition: другой таск может обработать эхо
       до того, как публикация реально уйдёт, и cind будет удалён из кэша.
       Последующее «настоящее» эхо не будет подавлено.
    5. Кэш защищён ``threading.Lock`` (НЕ ``asyncio.Lock``).  Это безопасно для
       коротких операций, но блокирует event loop при contention.
    6. Lock НЕ удерживается во время ``await publish``.  Публикация происходит
       вне lock — это правильно (иначе был бы deadlock), но означает, что
       проверка и удаление cind в ``_handle_state`` могут произойти
       concurrently с добавлением нового cind.
    7. ``_set_state`` полностью обходит подавление эха — использует
       ``object.__setattr__`` напрямую.  Изменение применяется без публикации
       и без добавления cind в кэш.
    8. Обнаружить сбой подавления эха можно проверив ``_own_state_cinds``:
       если множество растёт без очистки, эхо не приходит (брокер не
       возвращает сообщения или cinds вытеснены).

Запуск (БЕЗ MQTT-брокера)::

    python examples/17_echo_suppression.py
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

from kamio import Device, state
from kamio.core.envelope import Envelope, EnvelopeType

# Тихое логирование
logging.basicConfig(level=logging.CRITICAL)


# =====================================================================
# Вспомогательный класс устройства
# =====================================================================

class EchoDevice(Device):
    """Простое устройство с одним state-полем для тестирования эха."""
    power: bool = state(default=False, writable=True)
    brightness: int = state(default=0, min=0, max=100)


# =====================================================================
# 1. Прямое присваивание БЕЗ node — изменение локально, публикации нет
# =====================================================================

async def test_assignment_without_node():
    """Без node изменение применяется, но не публикуется и cind не добавляется."""
    dev = EchoDevice()

    # НЕПРАВИЛЬНО: ожидать, что cind попадёт в кэш без node
    dev.power = True

    # Утверждение: значение изменилось локально
    assert dev.power is True, "Значение должно примениться локально"

    # Утверждение: node нет → cind НЕ добавлен в кэш эха
    assert len(dev._own_state_cinds) == 0, (
        "Без node cind не должен добавляться в _own_state_cinds"
    )

    # ПРАВИЛЬНО: понимать, что без node нет публикации
    # Если нужна публикация — сначала подключить node


# =====================================================================
# 2. Прямое присваивание С node — cind добавляется в кэш, публикация идёт
# =====================================================================

async def test_assignment_with_node():
    """С node cind добавляется в кэш и создаётся задача публикации."""
    dev = EchoDevice()

    # Создаём mock node с AsyncMock для publish
    mock_node = MagicMock()
    mock_node.device_id = "echo_dev_1"
    mock_node.publish = AsyncMock()
    dev.node = mock_node

    dev.power = True

    # Утверждение: значение изменилось
    assert dev.power is True

    # Утверждение: cind добавлен в кэш эха
    assert len(dev._own_state_cinds) == 1, (
        "cind должен быть в _own_state_cinds после присваивания с node"
    )

    # Утверждение: _own_state_cinds_order тоже содержит запись
    assert len(dev._own_state_cinds_order) == 1

    # Утверждение: publish был вызван (через create_task)
    # Даём задачам выполниться
    await asyncio.sleep(0.05)
    assert mock_node.publish.called, "publish должен быть вызван"
    # Очищаем фоновые задачи
    await dev.cancel_all_tasks()


# =====================================================================
# 3. Нет event loop → coro.close(), изменение локально, публикации нет
# =====================================================================

def test_no_event_loop():
    """Без event loop корутина закрывается, изменение применяется локально."""
    dev = EchoDevice()

    # Создаём mock node
    mock_node = MagicMock()
    mock_node.device_id = "echo_dev_2"
    mock_node.publish = AsyncMock()
    dev.node = mock_node

    # НЕПРАВИЛЬНО: менять state вне event loop и ожидать публикацию
    # Это вызывается БЕЗ asyncio loop (в синхронном коде)
    dev.power = True

    # Утверждение: значение изменилось локально
    assert dev.power is True, "Значение применяется локально даже без loop"

    # Утверждение: cind добавлен в кэш (добавление происходит до проверки loop)
    assert len(dev._own_state_cinds) == 1, (
        "cind добавляется в кэш ДО проверки event loop"
    )

    # Утверждение: publish НЕ был вызван (корутина закрыта, не выполнена)
    assert not mock_node.publish.called, (
        "publish не должен быть вызван без event loop — coro.close() вместо этого"
    )

    # ПРАВИЛЬНО: для публикации вне loop использовать
    # asyncio.run_coroutine_threadsafe или запустить loop


# =====================================================================
# 4. Кэш ограничен 4096 записями — переполнение вытесняет старые cinds
# =====================================================================

async def test_cache_overflow():
    """При переполнении кэша (4096) старые cinds вытесняются → эхо не подавится."""
    dev = EchoDevice()

    mock_node = MagicMock()
    mock_node.device_id = "echo_dev_3"
    mock_node.publish = AsyncMock()
    dev.node = mock_node

    # Уменьшаем лимит для теста (вместо 4096)
    dev._own_state_cinds_limit = 5

    # Генерируем 7 изменений → кэш переполнится, старые cinds вытеснятся
    cinds_generated = []
    for i in range(7):
        dev.brightness = i
        # Сохраняем последний добавленный cind
        cinds_generated.append(dev._own_state_cinds_order[-1])

    # Утверждение: в кэше осталось только 5 записей (лимит)
    assert len(dev._own_state_cinds) <= 5, (
        f"Кэш должен быть ограничен лимитом, но размер = {len(dev._own_state_cinds)}"
    )
    assert len(dev._own_state_cinds_order) <= 5, (
        f"Order list тоже ограничен, но размер = {len(dev._own_state_cinds_order)}"
    )

    # Утверждение: первые 2 cinds вытеснены (7 - 5 = 2)
    first_cind = cinds_generated[0]
    second_cind = cinds_generated[1]
    assert first_cind not in dev._own_state_cinds, (
        "Самый старый cind должен быть вытеснен при переполнении кэша"
    )
    assert second_cind not in dev._own_state_cinds, (
        "Второй cind тоже должен быть вытеснен"
    )

    # Утверждение: последние 5 cinds на месте
    for cind in cinds_generated[2:]:
        assert cind in dev._own_state_cinds, (
            f"Последние cinds должны остаться в кэше, но {cind} отсутствует"
        )

    # ВАЖНО: если эхо для вытесненного cind придёт — оно НЕ будет подавлено!
    # Устройство применит изменение повторно (двойная запись).
    await dev.cancel_all_tasks()


# =====================================================================
# 5. cind добавляется в кэш ДО публикации (race condition окно)
# =====================================================================

async def test_cind_added_before_publish():
    """cind добавляется в кэш внутри lock, публикация — вне lock.

    Это создаёт окно: эхо может прийти и быть подавленным ДО реальной
    публикации.  Если затем «настоящее» эхо придёт — оно не подавится,
    потому что cind уже удалён из кэша.
    """
    dev = EchoDevice()

    mock_node = MagicMock()
    mock_node.device_id = "echo_dev_4"
    mock_node.publish = AsyncMock()
    dev.node = mock_node

    dev.power = True

    # Утверждение: cind в кэше сразу после присваивания (до await publish)
    # Это доказывает, что добавление происходит синхронно в __setattr__
    assert len(dev._own_state_cinds) == 1

    # Симулируем эхо: вызываем _handle_state с тем же cind
    cind = dev._own_state_cinds_order[-1]
    echo_env = Envelope.state(source="echo_dev_4", data={"power": True})
    echo_env.cind = cind  # Подделываем тот же cind

    # НЕПРАВИЛЬНО: предполагать, что эхо всегда приходит после публикации.
    # В реальности брокер может вернуть сообщение раньше, чем корутина
    # публикации завершится (особенно с retain).

    # Проверяем логику подавления вручную (как делает DeviceHandler._handle_state)
    with dev._cinds_lock:
        is_own_echo = echo_env.cind in dev._own_state_cinds
        if is_own_echo:
            dev._own_state_cinds.discard(echo_env.cind)
            dev._own_state_cinds_order.remove(echo_env.cind)

    # Утверждение: эхо подавлено (cind найден и удалён)
    assert is_own_echo, "Эхо должно быть распознано по cind"
    assert len(dev._own_state_cinds) == 0, "cind должен быть удалён после подавления"

    # ТЕПЕРЬ: если придёт второе эхо с тем же cind (дубликат от брокера),
    # оно НЕ будет подавлено — cind уже удалён!
    with dev._cinds_lock:
        is_second_echo = echo_env.cind in dev._own_state_cinds
    assert not is_second_echo, (
        "Второе эхо с тем же cind НЕ подавится — cind уже удалён из кэша"
    )

    await dev.cancel_all_tasks()


# =====================================================================
# 6. threading.Lock (не asyncio.Lock) защищает кэш
# =====================================================================

def test_threading_lock_not_asyncio():
    """Кэш эха использует threading.Lock, а не asyncio.Lock.

    threading.Lock блокирует поток (и event loop) при contention.
    Для коротких операций (add/discard в set) это приемлемо, но
    при высокой нагрузке может стать узким местом.
    """
    dev = EchoDevice()

    # Утверждение: _cinds_lock — это threading.Lock, не asyncio.Lock
    import threading
    assert isinstance(dev._cinds_lock, type(threading.Lock())), (
        "_cinds_lock должен быть threading.Lock, не asyncio.Lock"
    )

    # Утверждение: lock можно использовать из синхронного кода
    with dev._cinds_lock:
        dev._own_state_cinds.add("test_cind")
        dev._own_state_cinds_order.append("test_cind")

    assert "test_cind" in dev._own_state_cinds

    # НЕПРАВИЛЬНО: использовать asyncio.Lock для этого кэша.
    # asyncio.Lock нельзя использовать из __setattr__ (синхронный метод).
    # __setattr__ вызывается как из async, так и из синхронного кода.

    # ПРАВИЛЬНО: threading.Lock работает везде, но блокирует event loop.
    # Для коротких операций (set add/discard) overhead минимален.


# =====================================================================
# 7. Lock НЕ удерживается во время await publish
# =====================================================================

async def test_lock_not_held_during_publish():
    """Публикация происходит ВНЕ lock — иначе был бы deadlock.

    __setattr__ добавляет cind под lock, затем выходит из lock,
    и только потом создаёт задачу публикации.  Это означает:
    - _handle_state может concurrently удалить cind из кэша
    - Новые cinds могут быть добавлены параллельно
    """
    dev = EchoDevice()

    mock_node = MagicMock()
    mock_node.device_id = "echo_dev_5"
    mock_node.publish = AsyncMock()
    dev.node = mock_node

    # Меняем два поля подряд
    dev.power = True
    dev.brightness = 50

    # Утверждение: оба cind в кэше (lock не блокирует между ними)
    assert len(dev._own_state_cinds) == 2, (
        "Оба cind должны быть в кэше — lock не удерживается между присваиваниями"
    )

    # Утверждение: publish вызван для обоих (через create_task)
    await asyncio.sleep(0.05)
    assert mock_node.publish.call_count == 2, (
        f"publish должен быть вызван 2 раза, вызовов = {mock_node.publish.call_count}"
    )

    await dev.cancel_all_tasks()


# =====================================================================
# 8. _set_state обходит подавление эха полностью
# =====================================================================

async def test_set_state_bypasses_echo_suppression():
    """_set_state использует object.__setattr__ — нет публикации, нет cind."""
    dev = EchoDevice()

    mock_node = MagicMock()
    mock_node.device_id = "echo_dev_6"
    mock_node.publish = AsyncMock()
    dev.node = mock_node

    # _set_state меняет значение без публикации и без cind
    dev._set_state(power=True, brightness=42)

    # Утверждение: значения изменились
    assert dev.power is True
    assert dev.brightness == 42

    # Утверждение: cind НЕ добавлен в кэш
    assert len(dev._own_state_cinds) == 0, (
        "_set_state не должен добавлять cind в кэш эха"
    )

    # Утверждение: publish НЕ вызван
    await asyncio.sleep(0.05)
    assert not mock_node.publish.called, (
        "_set_state не должен публиковать в MQTT"
    )

    # ПРАВИЛЬНО: использовать _set_state для зеркалирования состояния,
    # полученного от другого устройства (чтобы не переиздавать его).
    # НЕПРАВИЛЬНО: использовать _set_state для локальных изменений,
    # которые должны быть опубликованы — публикация не произойдёт.


# =====================================================================
# 9. Обнаружение сбоя подавления эха через проверку _own_state_cinds
# =====================================================================

async def test_detect_echo_suppression_failure():
    """Если _own_state_cinds растёт без очистки — эхо не приходит.

    Это диагностический приём: после N изменений без эхо-ответов
    кэш растёт.  При достижении лимита (4096) старые cinds вытесняются,
    и подавление перестаёт работать для них.
    """
    dev = EchoDevice()

    mock_node = MagicMock()
    mock_node.device_id = "echo_dev_7"
    mock_node.publish = AsyncMock()
    dev.node = mock_node

    # Устанавливаем маленький лимит для демонстрации
    dev._own_state_cinds_limit = 10

    # Меняем состояние 15 раз БЕЗ получения эхо
    for i in range(15):
        dev.brightness = i

    # Утверждение: кэш не превышает лимит
    assert len(dev._own_state_cinds) <= 10, (
        "Кэш не должен превышать лимит даже без эхо-ответов"
    )

    # Утверждение: кэш НЕ пустой — эхо не пришло, cinds не очищены
    assert len(dev._own_state_cinds) > 0, (
        "Без эхо-ответов cinds остаются в кэше (подавление не сработало)"
    )

    # ДИАГНОСТИКА: размер кэша > 0 после изменений = эхо не приходит.
    # В реальном приложении это может означать:
    # - Брокер не возвращает сообщения (нет loopback подписки)
    # - Слишком много изменений без эхо (переполнение кэша)
    # - Ошибка в маршрутизации сообщений

    # ПРАВИЛЬНО: периодически проверять размер кэша как health-check
    cache_size = len(dev._own_state_cinds)
    if cache_size > dev._own_state_cinds_limit * 0.8:
        # Предупреждение: кэш почти полон, подавление эха может не работать
        pass  # В реальном коде: логировать warning или алертить

    await dev.cancel_all_tasks()


# =====================================================================
# 10. Полный цикл: присваивание → эхо → подавление → повторное изменение
# =====================================================================

async def test_full_echo_cycle():
    """Полный цикл подавления эха: изменение → публикация → эхо → подавление."""
    dev = EchoDevice()

    mock_node = MagicMock()
    mock_node.device_id = "echo_dev_8"
    mock_node.publish = AsyncMock()
    dev.node = mock_node

    # Шаг 1: меняем состояние
    dev.power = True
    cind = dev._own_state_cinds_order[-1]

    # Утверждение: cind в кэше
    assert cind in dev._own_state_cinds

    # Шаг 2: симулируем эхо (брокер вернул наше сообщение)
    echo_env = Envelope(
        source="echo_dev_8",
        type=EnvelopeType.DEVICE_STATE,
        data={"power": True},
        cind=cind,
    )

    # Шаг 3: обрабатываем эхо как DeviceHandler._handle_state
    with dev._cinds_lock:
        is_own_echo = echo_env.cind in dev._own_state_cinds
        if is_own_echo:
            dev._own_state_cinds.discard(echo_env.cind)
            try:
                dev._own_state_cinds_order.remove(echo_env.cind)
            except ValueError:
                pass

    # Утверждение: эхо подавлено
    assert is_own_echo, "Эхо должно быть распознано"
    assert len(dev._own_state_cinds) == 0, "Кэш должен быть пуст после подавления"

    # Шаг 4: второе изменение — новый cind
    dev.power = False
    new_cind = dev._own_state_cinds_order[-1]

    # Утверждение: новый cind отличается от старого
    assert new_cind != cind, "Каждое изменение должно генерировать новый cind"
    assert len(dev._own_state_cinds) == 1

    await dev.cancel_all_tasks()


# =====================================================================
# Главная функция
# =====================================================================

async def main():
    print("=" * 70)
    print("17 — Echo Suppression: подавление эха MQTT")
    print("=" * 70)

    tests = [
        ("1. Присваивание БЕЗ node — нет публикации", test_assignment_without_node),
        ("2. Присваивание С node — cind в кэше, публикация идёт", test_assignment_with_node),
        ("3. Нет event loop — coro.close(), локально только", test_no_event_loop),
        ("4. Переполнение кэша (4096) — старые cinds вытесняются", test_cache_overflow),
        ("5. cind добавляется ДО публикации (race condition)", test_cind_added_before_publish),
        ("6. threading.Lock (не asyncio.Lock) защищает кэш", test_threading_lock_not_asyncio),
        ("7. Lock НЕ удерживается во время await publish", test_lock_not_held_during_publish),
        ("8. _set_state обходит подавление эха", test_set_state_bypasses_echo_suppression),
        ("9. Обнаружение сбоя подавления через _own_state_cinds", test_detect_echo_suppression_failure),
        ("10. Полный цикл: изменение → эхо → подавление", test_full_echo_cycle),
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
