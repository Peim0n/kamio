"""
07 — Event Bus (событийная шина)
================================

Демонстрирует систему событийной шины Kamio:
    - app.subscribe_event() с filter_fn и priority
    - app.publish_event() для кастомных событий
    - Встроенные события: device_state_changed, device_command_executed,
      app_start, app_stop
    - Кастомные типы событий
    - unsubscribe_event
    - Несколько подписчиков с разными приоритетами
    - Примеры filter_fn

Запуск::
    python examples/07_event_bus.py

Предварительно запустите MQTT-брокер на localhost:1883
(например, ``docker run -p 1883:1883 eclipse-mosquitto``).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

from kamio import KamioApp, Device, command, state

# Настройка логирования — выводим все сообщения от фреймворка и примера
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("event_bus_demo")


# =====================================================================
# Устройство для демонстрации встроенных событий
# =====================================================================

class SmartSocket(Device):
    """Умная розетка с вкл/выкл и счётчиком энергии."""

    power: bool = state(default=False, writable=True, description="Питание розетки")
    energy_kwh: float = state(default=0.0, writable=False, description="Накопленная энергия")

    @command
    async def toggle(self):
        """Переключить питание розетки."""
        self.power = not self.power
        return {"power": self.power}

    @command
    async def reset_energy(self):
        """Сбросить счётчик энергии."""
        self.energy_kwh = 0.0
        return {"energy_kwh": self.energy_kwh}


# =====================================================================
# Подписчики на встроенные события
# =====================================================================

def on_state_changed(data: Dict[str, Any]) -> None:
    """Синхронный подписчик на device_state_changed.

    Событие публикуется автоматически при изменении любого state-поля.
    Структура data:
        - device_id:  ID устройства
        - field:       имя изменённого поля
        - old_value:   предыдущее значение
        - new_value:   новое значение
        - timestamp:   время события (добавляется автоматически)
    """
    logger.info(
        f"[state_changed] {data['device_id']}.{data['field']}: "
        f"{data['old_value']} -> {data['new_value']}"
    )


async def on_command_executed(data: Dict[str, Any]) -> None:
    """Асинхронный подписчик на device_command_executed.

    Событие публикуется автоматически после выполнения любой команды.
    Структура data:
        - device_id:  ID устройства
        - command:    имя команды
        - params:     параметры вызова
        - result:     результат выполнения
    """
    logger.info(
        f"[command_executed] {data['device_id']}.{data['command']}("
        f"params={data['params']}) -> {data['result']}"
    )


def on_app_start(data: Dict[str, Any]) -> None:
    """Подписчик на встроенное событие app_start (публикуется при запуске)."""
    logger.info("[app_start] Приложение запущено")


def on_app_stop(data: Dict[str, Any]) -> None:
    """Подписчик на встроенное событие app_stop (публикуется при остановке)."""
    logger.info("[app_stop] Приложение остановлено")


# =====================================================================
# Кастомные события и подписчики с приоритетами
# =====================================================================

# Кастомное событие: тревога энергопотребления
ENERGY_ALERT_EVENT = "energy_alert"


async def high_priority_logger(data: Dict[str, Any]) -> None:
    """Подписчик с высоким приоритетом (priority=10) — выполнится первым.

    В EventBus подписчики с бОльшим значением priority выполняются раньше.
    Это полезно для критичных обработчиков (логирование, аудит), которые
    должны получить событие до остальных.
    """
    logger.warning(f"[priority=10] ENERGY ALERT: {data}")


async def medium_priority_handler(data: Dict[str, Any]) -> None:
    """Подписчик со средним приоритетом (priority=5)."""
    logger.info(f"[priority=5] Обработка тревоги: device={data.get('device_id')}")


async def low_priority_notifier(data: Dict[str, Any]) -> None:
    """Подписчик с низким приоритетом (priority=0, по умолчанию).

    Выполнится последним — подходит для уведомлений, которые не зависят
    от других обработчиков.
    """
    logger.info(f"[priority=0] Уведомление: {data.get('message', 'нет сообщения')}")


# =====================================================================
# Пример filter_fn — фильтрация событий перед доставкой подписчику
# =====================================================================

def filter_power_only(data: Dict[str, Any]) -> bool:
    """Пропустить только события изменения поля 'power'.

    filter_fn — это предикат (data) -> bool.
    Если возвращает False, callback не вызывается.
    Это позволяет одному подписчику реагировать только на релевантные
    события, не создавая отдельный подписчик для каждого поля.
    """
    return data.get("field") == "power"


def filter_energy_only(data: Dict[str, Any]) -> bool:
    """Пропустить только события изменения поля 'energy_kwh'."""
    return data.get("field") == "energy_kwh"


def on_power_change(data: Dict[str, Any]) -> None:
    """Подписчик, получающий только изменения поля 'power' (через filter_fn)."""
    logger.info(f"[filtered:power] Розетка {data['device_id']} -> {'ON' if data['new_value'] else 'OFF'}")


def on_energy_change(data: Dict[str, Any]) -> None:
    """Подписчик, получающий только изменения поля 'energy_kwh' (через filter_fn)."""
    logger.info(f"[filtered:energy] Энергия {data['device_id']} = {data['new_value']} kWh")


# =====================================================================
# Демонстрация unsubscribe_event
# =====================================================================

def temporary_subscriber(data: Dict[str, Any]) -> None:
    """Временный подписчик, который будет отписан через unsubscribe_event."""
    logger.info(f"[temporary] Это сообщение больше не появится после отписки: {data}")


# =====================================================================
# Главный цикл
# =====================================================================

async def main():
    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="event_bus_demo")

    # --- Подписка на встроенные события ---

    # device_state_changed: публикуется при изменении любого state-поля
    app.subscribe_event("device_state_changed", on_state_changed)

    # device_command_executed: публикуется после выполнения команды
    app.subscribe_event("device_command_executed", on_command_executed)

    # app_start / app_stop: публикуются при запуске и остановке приложения
    app.subscribe_event("app_start", on_app_start)
    app.subscribe_event("app_stop", on_app_stop)

    # --- Подписка с filter_fn — фильтрация по конкретному полю ---

    # on_power_change получит только события, где field == "power"
    app.subscribe_event(
        "device_state_changed",
        on_power_change,
        filter_fn=filter_power_only,
    )

    # on_energy_change получит только события, где field == "energy_kwh"
    app.subscribe_event(
        "device_state_changed",
        on_energy_change,
        filter_fn=filter_energy_only,
    )

    # --- Подписка на кастомное событие с разными приоритетами ---

    # priority=10 — выполнится первым (высший приоритет)
    app.subscribe_event(ENERGY_ALERT_EVENT, high_priority_logger, priority=10)

    # priority=5 — выполнится вторым
    app.subscribe_event(ENERGY_ALERT_EVENT, medium_priority_handler, priority=5)

    # priority=0 (по умолчанию) — выполнится последним
    app.subscribe_event(ENERGY_ALERT_EVENT, low_priority_notifier)

    # --- Временный подписчик для демонстрации unsubscribe_event ---
    app.subscribe_event(ENERGY_ALERT_EVENT, temporary_subscriber)

    # --- Регистрация и запуск ---
    app.register(SmartSocket)
    await app.start()

    # Создаём устройство
    socket = await app.add_device("living_room_socket", SmartSocket)

    logger.info("=== Демонстрация встроенных событий ===")

    # 1. Изменение state — вызовет device_state_changed
    logger.info("Переключаем розетку (toggle)...")
    await socket.handle_command("toggle", {})
    await asyncio.sleep(0.5)

    # 2. Изменение другого поля — тоже вызовет device_state_changed,
    #    но on_power_change не сработает (filter_fn отфильтрует)
    logger.info("Меняем energy_kwh напрямую...")
    await socket.handle_state({"energy_kwh": 42.5})
    await asyncio.sleep(0.5)

    logger.info("=== Демонстрация кастомного события с приоритетами ===")

    # 3. Публикация кастомного события — подписчики вызовутся по приоритету
    await app.publish_event(ENERGY_ALERT_EVENT, {
        "device_id": "living_room_socket",
        "message": "Потребление превышает норму!",
        "value": 42.5,
    })
    await asyncio.sleep(0.5)

    logger.info("=== Демонстрация unsubscribe_event ===")

    # 4. Отписываем временного подписчика
    app.unsubscribe_event(ENERGY_ALERT_EVENT, temporary_subscriber)
    logger.info("Временный подписчик отписан. Публикуем событие снова...")

    await app.publish_event(ENERGY_ALERT_EVENT, {
        "device_id": "living_room_socket",
        "message": "Повторная тревога (temporary_subscriber не должен появиться)",
        "value": 50.0,
    })
    await asyncio.sleep(0.5)

    # 5. Проверка списка подписчиков через EventBus
    subscribers = app.event_bus.list_subscribers(ENERGY_ALERT_EVENT)
    logger.info(f"Активных подписчиков на '{ENERGY_ALERT_EVENT}': {len(subscribers)}")
    logger.info(f"Типы событий с подписчиками: {app.event_bus.event_types()}")

    # --- Дополнительные демонстрации ---
    await demo_list_subscribers(app)
    await demo_event_types(app)
    await demo_priority_detailed(app)
    await demo_complex_filter(app, socket)
    await demo_sync_vs_async(app)
    await demo_timestamp_auto_add(app)
    await demo_unsubscribe(app)

    logger.info("=== Завершение ===")
    await app.stop()


# =====================================================================
# Демонстрация: list_subscribers — список подписчиков события
# =====================================================================

async def demo_list_subscribers(app):
    """Показывает использование EventBus.list_subscribers().

    list_subscribers(event_type) возвращает список всех callback-функций,
    подписанных на указанный тип события, в порядке приоритета
    (от высшего к низшему).
    """
    logger.info("=== Демонстрация: list_subscribers ===")

    # Получаем список подписчиков на device_state_changed
    state_subs = app.event_bus.list_subscribers("device_state_changed")
    logger.info(f"Подписчики 'device_state_changed': {len(state_subs)} шт.")
    for i, cb in enumerate(state_subs):
        logger.info(f"  {i+1}. {cb.__name__}")

    # Получаем список подписчиков на energy_alert
    alert_subs = app.event_bus.list_subscribers(ENERGY_ALERT_EVENT)
    logger.info(f"Подписчики '{ENERGY_ALERT_EVENT}': {len(alert_subs)} шт.")
    for i, cb in enumerate(alert_subs):
        logger.info(f"  {i+1}. {cb.__name__}")

    # Подписчики на несуществующее событие — пустой список
    no_subs = app.event_bus.list_subscribers("nonexistent_event")
    logger.info(f"Подписчики 'nonexistent_event': {len(no_subs)} шт.")


# =====================================================================
# Демонстрация: event_types — все зарегистрированные типы событий
# =====================================================================

async def demo_event_types(app):
    """Показывает использование EventBus.event_types().

    event_types() возвращает список всех типов событий, на которые
    есть хотя бы один подписчик.
    """
    logger.info("\n=== Демонстрация: event_types ===")

    types = app.event_bus.event_types()
    logger.info(f"Все типы событий с подписчиками ({len(types)} шт.):")
    for t in types:
        count = len(app.event_bus.list_subscribers(t))
        logger.info(f"  • {t} ({count} подписчиков)")


# =====================================================================
# Демонстрация: приоритеты (подробно)
# =====================================================================

# Список для записи порядка вызова подписчиков
_priority_order: list[str] = []


def make_priority_callback(name: str, priority: int):
    """Создаёт callback, который записывает свой порядок вызова."""

    async def callback(data: Dict[str, Any]) -> None:
        _priority_order.append(name)
        logger.info(f"  [priority={priority}] {name} вызван")

    callback.__name__ = f"priority_{priority}_{name}"
    return callback


async def demo_priority_detailed(app):
    """Подробно показывает порядок выполнения подписчиков по приоритету.

    В EventBus подписчики с бОльшим значением priority выполняются раньше.
    При равных приоритетах порядок зависит от порядка регистрации.
    """
    logger.info("\n=== Демонстрация: приоритеты (подробно) ===")

    _priority_order.clear()
    test_event = "priority_test"

    # Регистрируем подписчиков с разными приоритетами
    cb_100 = make_priority_callback("critical_audit", 100)
    cb_50 = make_priority_callback("high_handler", 50)
    cb_10 = make_priority_callback("medium_handler", 10)
    cb_0_a = make_priority_callback("default_a", 0)
    cb_0_b = make_priority_callback("default_b", 0)
    cb_neg = make_priority_callback("cleanup", -10)

    app.subscribe_event(test_event, cb_100, priority=100)
    app.subscribe_event(test_event, cb_50, priority=50)
    app.subscribe_event(test_event, cb_10, priority=10)
    app.subscribe_event(test_event, cb_0_a, priority=0)
    app.subscribe_event(test_event, cb_0_b, priority=0)
    app.subscribe_event(test_event, cb_neg, priority=-10)

    # Публикуем событие — подписчики вызовутся по приоритету
    await app.publish_event(test_event, {"test": True})
    await asyncio.sleep(0.3)

    logger.info(f"Порядок выполнения: {_priority_order}")
    logger.info("(ожидается: critical_audit → high_handler → medium_handler → default_a → default_b → cleanup)")

    # Очистка
    app.event_bus.clear(test_event)


# =====================================================================
# Демонстрация: filter_fn с сложной логикой
# =====================================================================

def complex_filter(data: Dict[str, Any]) -> bool:
    """Сложный фильтр: пропускает только события от устройства 'living_room_socket',
    где поле 'power' изменилось на True, и значение больше 0.

    filter_fn может содержать любую логику — это обычный предикат.
    Доступ ко всем полям data позволяет создавать сложные условия.
    """
    # Проверяем device_id
    if data.get("device_id") != "living_room_socket":
        return False
    # Проверяем имя поля
    if data.get("field") != "power":
        return False
    # Проверяем, что новое значение — True
    if data.get("new_value") is not True:
        return False
    # Проверяем наличие timestamp
    if "timestamp" not in data:
        return False
    return True


def on_complex_filter_match(data: Dict[str, Any]) -> None:
    """Подписчик со сложным фильтром — вызывается только при всех условиях."""
    logger.info(
        f"[complex_filter] Розетка {data['device_id']} включена "
        f"(power=True, timestamp есть)"
    )


async def demo_complex_filter(app, socket):
    """Демонстрирует filter_fn с многоуровневой логикой."""
    logger.info("\n=== Демонстрация: filter_fn с сложной логикой ===")

    app.subscribe_event(
        "device_state_changed",
        on_complex_filter_match,
        filter_fn=complex_filter,
    )

    # Выключаем розетку (power=False) — фильтр не пропустит
    logger.info("Выключаем розетку (power=False) — сложный фильтр не должен пропустить...")
    await socket.handle_state({"power": False})
    await asyncio.sleep(0.3)

    # Включаем розетку (power=True) — фильтр пропустит
    logger.info("Включаем розетку (power=True) — сложный фильтр должен пропустить...")
    await socket.handle_state({"power": True})
    await asyncio.sleep(0.3)


# =====================================================================
# Демонстрация: sync vs async callback
# =====================================================================

def sync_callback_demo(data: Dict[str, Any]) -> None:
    """Синхронный callback — выполняется без await.

    EventBus автоматически определяет, является ли callback
    синхронным или асинхронным (через inspect.iscoroutinefunction),
    и вызывает его соответствующим образом.
    """
    logger.info(f"[sync] Синхронный callback получил: {data.get('test_value')}")


async def async_callback_demo(data: Dict[str, Any]) -> None:
    """Асинхронный callback — может использовать await.

    Асинхронные callback'и могут выполнять I/O операции (запросы к API,
    запись в БД, отправка уведомлений) без блокировки event loop.
    """
    # Имитируем асинхронную операцию
    await asyncio.sleep(0.01)
    logger.info(f"[async] Асинхронный callback получил: {data.get('test_value')}")


async def demo_sync_vs_async(app):
    """Показывает, что EventBus поддерживает оба типа callback."""
    logger.info("\n=== Демонстрация: sync vs async callback ===")

    test_event = "sync_async_test"

    # Регистрируем оба типа callback на одно событие
    app.subscribe_event(test_event, sync_callback_demo)
    app.subscribe_event(test_event, async_callback_demo)

    # Публикуем — оба callback будут вызваны
    await app.publish_event(test_event, {"test_value": 42})
    await asyncio.sleep(0.2)

    logger.info("Оба callback вызваны: sync (без await) и async (с await)")

    # Очистка
    app.event_bus.clear(test_event)


# =====================================================================
# Демонстрация: timestamp auto-add
# =====================================================================

def on_timestamp_check(data: Dict[str, Any]) -> None:
    """Проверяет наличие автоматически добавленного timestamp."""
    ts = data.get("timestamp")
    logger.info(f"[timestamp] Событие содержит timestamp: {ts} (тип: {type(ts).__name__})")


async def demo_timestamp_auto_add(app):
    """Показывает, что EventBus автоматически добавляет timestamp.

    При publish() если в data нет ключа 'timestamp' (или он равен None),
    EventBus автоматически добавляет текущее время в UTC.
    """
    logger.info("\n=== Демонстрация: timestamp auto-add ===")

    test_event = "timestamp_test"
    app.subscribe_event(test_event, on_timestamp_check)

    # Публикуем БЕЗ timestamp — EventBus добавит автоматически
    logger.info("Публикуем событие БЕЗ timestamp (будет добавлен автоматически)...")
    await app.publish_event(test_event, {"value": 1})
    await asyncio.sleep(0.2)

    # Публикуем С timestamp — EventBus не перезапишет
    from datetime import datetime, timezone
    custom_ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    logger.info(f"Публикуем событие С кастомным timestamp ({custom_ts})...")
    await app.publish_event(test_event, {"value": 2, "timestamp": custom_ts})
    await asyncio.sleep(0.2)

    # Очистка
    app.event_bus.clear(test_event)


# =====================================================================
# Демонстрация: unsubscribe — удаление подписчика
# =====================================================================

_unsub_call_count = 0


def on_unsub_test(data: Dict[str, Any]) -> None:
    """Подписчик, который будет удалён через unsubscribe."""
    global _unsub_call_count
    _unsub_call_count += 1
    logger.info(f"[unsub_test] Вызван (count={_unsub_call_count})")


async def demo_unsubscribe(app):
    """Подробно показывает удаление подписчика через unsubscribe.

    unsubscribe(event_type, callback) удаляет конкретный callback
    по его идентичности (identity). Другие подписчики на то же
    событие остаются.
    """
    logger.info("\n=== Демонстрация: unsubscribe ===")

    global _unsub_call_count
    _unsub_call_count = 0

    test_event = "unsub_test"
    app.subscribe_event(test_event, on_unsub_test)

    # Публикуем — подписчик вызовется
    logger.info("Публикация 1 — подписчик активен...")
    await app.publish_event(test_event, {"n": 1})
    await asyncio.sleep(0.2)
    logger.info(f"Вызовов: {_unsub_call_count}")

    # Отписываем
    logger.info("Отписываем on_unsub_test...")
    app.unsubscribe_event(test_event, on_unsub_test)

    # Публикуем снова — подписчик НЕ вызовется
    logger.info("Публикация 2 — подписчик отписан...")
    await app.publish_event(test_event, {"n": 2})
    await asyncio.sleep(0.2)
    logger.info(f"Вызовов после отписки: {_unsub_call_count} (не должно увеличиться)")

    # Проверяем через list_subscribers
    subs = app.event_bus.list_subscribers(test_event)
    logger.info(f"Подписчиков на '{test_event}' после отписки: {len(subs)}")


if __name__ == "__main__":
    asyncio.run(main())
