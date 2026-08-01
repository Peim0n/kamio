"""
12 — Lifecycle Hooks (хуки жизненного цикла)
=============================================

Демонстрирует все хуки жизненного цикла Kamio:
    - on_before_start, on_after_start, on_before_stop, on_after_stop
    - on_device_added, on_device_removed, on_device_started, on_device_stopped
    - on_rule_added, on_rule_removed, on_rule_triggered, on_rule_failed
    - Приоритеты хуков
    - Синхронные и асинхронные хуки

Хуки регистрируются через app.register_hook(event_type, hook, priority).
В отличие от EventBus, хуки предназначены для перехвата внутренних
событий фреймворка (lifecycle interception), а не для пользовательского pub/sub.

Запуск::
    python examples/12_lifecycle_hooks.py

Предварительно запустите MQTT-брокер на localhost:1883
(например, ``docker run -p 1883:1883 eclipse-mosquitto``).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

from kamio import KamioApp, Device, RuleEvent, command, rule, state

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("hooks_demo")


# =====================================================================
# Устройство для демонстрации
# =====================================================================

class SmartLight(Device):
    """Умная лампа с правилом автоматизации."""

    power: bool = state(default=False, writable=True, description="Питание")
    brightness: int = state(
        default=100, min=0, max=255, writable=True, description="Яркость"
    )

    # --- Правило внутри устройства ---
    # @rule регистрирует метод как правило, реагирующее на изменение полей.
    # При срабатывании правила фреймворк вызывает хук on_rule_triggered.
    @rule(fields=["power"])
    async def on_power_change(self, event: RuleEvent, app: KamioApp):
        """Срабатывает при изменении поля power."""
        if event.get("power"):
            logger.info(f"  [device rule] Лампа {self.node.device_id} включена")
        else:
            logger.info(f"  [device rule] Лампа {self.node.device_id} выключена")

    @command
    async def toggle(self):
        """Переключить питание."""
        self.power = not self.power
        return {"power": self.power}

    @command
    async def fail_command(self):
        """Команда, которая всегда вызывает ошибку (для демонстрации on_rule_failed)."""
        raise ValueError("Демонстрационная ошибка команды")


# =====================================================================
# Хуки жизненного цикла приложения
# =====================================================================

# --- on_before_start / on_after_start ---

async def on_before_start_hook():
    """Вызывается перед запуском приложения (до подключения к MQTT).

    Полезно для подготовки ресурсов: открытие файлов, инициализация
    внешних сервисов, проверка окружения.
    """
    logger.info("[HOOK] on_before_start: подготовка к запуску")


async def on_after_start_hook():
    """Вызывается после полного запуска приложения (MQTT подключён, узлы стартовали).

    Полезно для запуска фоновых задач, отправки уведомлений о запуске.
    """
    logger.info("[HOOK] on_after_start: приложение полностью запущено")


# --- on_before_stop / on_after_stop ---

async def on_before_stop_hook():
    """Вызывается перед остановкой приложения.

    Полезно для graceful shutdown: закрытие соединений, сохранение состояния.
    """
    logger.info("[HOOK] on_before_stop: подготовка к остановке")


async def on_after_stop_hook():
    """Вызывается после полной остановки приложения.

    Все ресурсы освобождены, MQTT отключён.
    """
    logger.info("[HOOK] on_after_stop: приложение полностью остановлено")


# =====================================================================
# Хуки жизненного цикла устройств
# =====================================================================

# --- on_device_added / on_device_removed ---

async def on_device_added_hook(device: Device):
    """Вызывается при добавлении устройства в приложение.

    Аргумент: экземпляр устройства (Device).
    """
    device_id = device.node.device_id if device.node else "?"
    logger.info(f"[HOOK] on_device_added: {device_id} (type={device.device_type()})")


async def on_device_removed_hook(device: Device):
    """Вызывается при удалении устройства из приложения.

    Аргумент: экземпляр устройства (Device).
    """
    device_id = device.node.device_id if device.node else "?"
    logger.info(f"[HOOK] on_device_removed: {device_id}")


# --- on_device_started / on_device_stopped ---

async def on_device_started_hook(device: Device):
    """Вызывается когда узел устройства стартовал (on_start завершён).

    К этому моменту телеметрия и keepalive запущены.
    """
    device_id = device.node.device_id if device.node else "?"
    logger.info(f"[HOOK] on_device_started: {device_id} — телеметрия активна")


async def on_device_stopped_hook(device: Device):
    """Вызывается когда узел устройства остановлен (on_stop завершён).

    Драйвер отключён, задачи отменены.
    """
    device_id = device.node.device_id if device.node else "?"
    logger.info(f"[HOOK] on_device_stopped: {device_id} — ресурсы освобождены")


# =====================================================================
# Хуки правил автоматизации
# =====================================================================

# --- on_rule_added / on_rule_removed ---

async def on_rule_added_hook(rule_obj):
    """Вызывается при добавлении правила в RuleEngine.

    Аргумент: объект Rule (содержит .func, .device_class, .fields, и т.д.).
    """
    func_name = rule_obj.func.__name__
    fields = getattr(rule_obj, "fields", None)
    logger.info(f"[HOOK] on_rule_added: {func_name} (fields={fields})")


async def on_rule_removed_hook(rule_obj):
    """Вызывается при удалении правила из RuleEngine."""
    func_name = rule_obj.func.__name__
    logger.info(f"[HOOK] on_rule_removed: {func_name}")


# --- on_rule_triggered / on_rule_failed ---

async def on_rule_triggered_hook(rule_obj, snapshot: Dict[str, Any]):
    """Вызывается при успешном срабатывании правила.

    Аргументы:
        - rule_obj: объект Rule
        - snapshot: слепок данных, на которых сработало правило
    """
    func_name = rule_obj.func.__name__
    logger.info(f"[HOOK] on_rule_triggered: {func_name} (data={snapshot})")


async def on_rule_failed_hook(rule_obj, error: Exception):
    """Вызывается при ошибке выполнения правила.

    Аргументы:
        - rule_obj: объект Rule
        - error:    исключение, вызванное правилом
    """
    func_name = rule_obj.func.__name__
    logger.error(f"[HOOK] on_rule_failed: {func_name} — {error}")


# =====================================================================
# Демонстрация приоритетов хуков
# =====================================================================

async def high_priority_start_hook():
    """Хук с высоким приоритетом (priority=10) — выполнится первым.

    В HooksManager: бОльшее значение priority = выполняется раньше.
    Это позволяет гарантировать, что критичные хуки (например,
    инициализация базы данных) выполняются до остальных.
    """
    logger.info("[HOOK] on_after_start priority=10: критичная инициализация (первый)")


async def low_priority_start_hook():
    """Хук с низким приоритетом (priority=-10) — выполнится последним.

    Подходит для необязательных задач (логирование, метрики),
    которые не должны блокировать остальные хуки.
    """
    logger.info("[HOOK] on_after_start priority=-10: финальная обработка (последний)")


# =====================================================================
# Демонстрация синхронных хуков
# =====================================================================

def sync_hook_on_device_added(device: Device):
    """Синхронный хук — поддерживается наравне с асинхронными.

    HooksManager автоматически определяет, является ли хук
    coroutine function (async def) или обычной функцией (def).
    Синхронные хуки вызываются напрямую, асинхронные — через await.
    """
    device_id = device.node.device_id if device.node else "?"
    logger.info(f"[HOOK] sync on_device_added: {device_id} (синхронный хук)")


# =====================================================================
# Правило на уровне приложения (для демонстрации on_rule_added)
# =====================================================================

# Это правило регистрируется через @app.rule и тоже вызывает on_rule_added
async def app_level_rule(event: RuleEvent, app: KamioApp):
    """Правило на уровне приложения: реагирует на изменение brightness."""
    if event.get("brightness") is not None:
        logger.info(f"  [app rule] Яркость изменена: {event.get('brightness')}")


# =====================================================================
# Главный цикл
# =====================================================================

async def main():
    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="hooks_demo")

    # --- Регистрация хуков жизненного цикла приложения ---
    # priority: бОльшее значение = выполняется раньше (по умолчанию 0)
    app.register_hook("on_before_start", on_before_start_hook)
    app.register_hook("on_after_start", on_after_start_hook)
    app.register_hook("on_before_stop", on_before_stop_hook)
    app.register_hook("on_after_stop", on_after_stop_hook)

    # --- Регистрация хуков устройств ---
    app.register_hook("on_device_added", on_device_added_hook)
    app.register_hook("on_device_removed", on_device_removed_hook)
    app.register_hook("on_device_started", on_device_started_hook)
    app.register_hook("on_device_stopped", on_device_stopped_hook)

    # --- Регистрация хуков правил ---
    app.register_hook("on_rule_added", on_rule_added_hook)
    app.register_hook("on_rule_removed", on_rule_removed_hook)
    app.register_hook("on_rule_triggered", on_rule_triggered_hook)
    app.register_hook("on_rule_failed", on_rule_failed_hook)

    # --- Демонстрация приоритетов ---
    # Регистрируем два хука на on_after_start с разными приоритетами
    app.register_hook("on_after_start", high_priority_start_hook, priority=10)
    app.register_hook("on_after_start", low_priority_start_hook, priority=-10)

    # --- Демонстрация синхронного хука ---
    app.register_hook("on_device_added", sync_hook_on_device_added)

    # --- Демонстрация: несколько hooks на одно событие ---
    # hook_c с высоким приоритетом выполнится первым
    app.register_hook("on_device_added", hook_c_for_device_added, priority=10)
    app.register_hook("on_device_added", hook_a_for_device_added)
    app.register_hook("on_device_added", hook_b_for_device_added)

    # --- Демонстрация: hook с ошибкой ---
    # on_failing_hook вызовет RuntimeError, но on_safe_hook всё равно выполнится
    app.register_hook("on_device_added", on_failing_hook)
    app.register_hook("on_device_added", on_safe_hook_after_failing)

    # --- Демонстрация: hook с *args ---
    app.register_hook("on_after_start", on_flexible_hook)

    # --- Демонстрация: on_rule_triggered подробно ---
    # Заменяем базовый хук на подробную версию
    app.register_hook("on_rule_triggered", on_rule_triggered_detailed, priority=10)

    # --- Регистрация правила на уровне приложения ---
    # @app.rule регистрирует функцию как правило и вызывает on_rule_added
    app.add_rule(
        app_level_rule,
        device=SmartLight,
        fields=["brightness"],
        description="Логирование изменения яркости",
    )

    # --- Регистрация класса устройства ---
    # При регистрации класса с @rule методами, on_rule_added вызывается
    # для каждого правила внутри класса.
    app.register(SmartLight)

    # --- Запуск приложения ---
    # Сработают хуки: on_before_start -> (MQTT connect) -> on_after_start
    logger.info("=== Запуск приложения ===")
    await app.start()

    # --- Создание устройства ---
    # Сработают хуки: on_device_added -> on_device_started
    logger.info("=== Создание устройства ===")
    light = await app.add_device("demo_light", SmartLight)

    # --- Демонстрация: изменение состояния ---
    # Сработает правило on_power_change -> on_rule_triggered
    logger.info("=== Изменение power (сработает правило) ===")
    await light.handle_state({"power": True})
    await asyncio.sleep(0.5)

    # Сработает app_level_rule -> on_rule_triggered
    logger.info("=== Изменение brightness (сработает app rule) ===")
    await light.handle_state({"brightness": 200})
    await asyncio.sleep(0.5)

    # --- Демонстрация: команда ---
    logger.info("=== Вызов команды toggle ===")
    await light.handle_command("toggle", {})
    await asyncio.sleep(0.5)

    # --- Демонстрация: ошибка команды ---
    # on_rule_failed не сработает (это не правило), но ошибка будет
    # перехвачена и залогирована фреймворком.
    logger.info("=== Вызов команды с ошибкой ===")
    try:
        await light.handle_command("fail_command", {})
    except ValueError as e:
        logger.info(f"Ожидаемая ошибка перехвачена: {e}")

    await asyncio.sleep(0.5)

    # --- Демонстрация: удаление устройства ---
    # Сработают хуки: on_device_removed -> on_device_stopped
    logger.info("=== Удаление устройства ===")
    await app.remove_device("demo_light")
    await asyncio.sleep(0.5)

    # --- Демонстрация: unregister_hook ---
    # Можно удалить конкретный хук по ссылке на функцию
    logger.info("=== Демонстрация unregister_hook ===")
    app.unregister_hook("on_after_stop", on_after_stop_hook)
    logger.info("Хук on_after_stop удалён — сообщение не появится при остановке")

    # Удаляем один из нескольких хуков на on_device_added
    app.unregister_hook("on_device_added", on_failing_hook)
    logger.info("Хук on_failing_hook удалён из on_device_added")
    logger.info("(при следующем добавлении устройства failing_hook не вызовется)")

    # --- Остановка ---
    # Сработают хуки: on_before_stop -> (cleanup) -> on_after_stop
    # (on_after_stop_hook удалён, но on_after_stop не вызовет его)
    logger.info("=== Остановка приложения ===")
    await app.stop()


# =====================================================================
# Демонстрация: hook с *args (переменное число аргументов)
# =====================================================================

async def on_flexible_hook(*args, **kwargs):
    """Хук с переменным числом аргументов.

    HooksManager передаёт аргументы хуку в зависимости от типа события.
    Некоторые хуки получают только device, другие — rule_obj и snapshot,
    третьи — вообще без аргументов.

    Использование *args, **kwargs позволяет создать универсальный хук,
    который работает с любым типом события. Однако это менее типобезопасно.
    """
    logger.info(f"[flexible_hook] args={len(args)}, kwargs={list(kwargs.keys())}")
    for i, arg in enumerate(args):
        logger.info(f"  arg[{i}]: {type(arg).__name__} = {arg!r}")


# =====================================================================
# Демонстрация: несколько hooks на одно событие
# =====================================================================

async def hook_a_for_device_added(device: Device):
    """Первый хук на on_device_added."""
    device_id = device.node.device_id if device.node else "?"
    logger.info(f"  [hook_a] on_device_added: {device_id}")


async def hook_b_for_device_added(device: Device):
    """Второй хук на on_device_added.

    На одно событие можно зарегистрировать несколько хуков.
    Они выполняются в порядке приоритета (при равных приоритетах —
    в порядке регистрации).
    """
    device_id = device.node.device_id if device.node else "?"
    logger.info(f"  [hook_b] on_device_added: {device_id} (type={device.device_type()})")


async def hook_c_for_device_added(device: Device):
    """Третий хук на on_device_added (с высоким приоритетом)."""
    device_id = device.node.device_id if device.node else "?"
    logger.info(f"  [hook_c] on_device_added: {device_id} (высокий приоритет, выполнится первым)")


# =====================================================================
# Демонстрация: hook с ошибкой — обработка ошибок в хуках
# =====================================================================

async def on_failing_hook(device: Device):
    """Хук, который вызывает исключение.

    HooksManager перехватывает ошибки в хуках, логирует их
    и продолжает выполнение остальных хуков. Ошибка в одном хуке
    не останавливает выполнение других хуков на то же событие.
    """
    device_id = device.node.device_id if device.node else "?"
    logger.info(f"  [failing_hook] on_device_added: {device_id} — сейчас вызову ошибку...")
    raise RuntimeError("Демонстрационная ошибка в хуке")


async def on_safe_hook_after_failing(device: Device):
    """Хук, который должен выполниться даже после ошибки в предыдущем хуке."""
    device_id = device.node.device_id if device.node else "?"
    logger.info(f"  [safe_hook] on_device_added: {device_id} — выполнен несмотря на ошибку в failing_hook")


# =====================================================================
# Демонстрация: on_rule_triggered подробно
# =====================================================================

async def on_rule_triggered_detailed(rule_obj, snapshot: Dict[str, Any]):
    """Подробная демонстрация хука on_rule_triggered.

    on_rule_triggered вызывается после успешного выполнения правила.
    Аргументы:
    - rule_obj: объект Rule с атрибутами:
        - .func: функция правила (async callable)
        - .device_class: класс устройства (или None для app-level rule)
        - .fields: список полей, на которые реагирует правило
        - .description: описание правила
    - snapshot: слепок данных, на которых сработало правило
        (dict с изменёнными полями и их значениями)
    """
    func_name = rule_obj.func.__name__
    device_class = getattr(rule_obj, "device_class", None)
    device_name = device_class.__name__ if device_class else "app-level"
    fields = getattr(rule_obj, "fields", None)
    description = getattr(rule_obj, "description", None)

    logger.info(f"  [rule_triggered detailed] Правило: {func_name}")
    logger.info(f"    Устройство: {device_name}")
    logger.info(f"    Поля: {fields}")
    logger.info(f"    Описание: {description}")
    logger.info(f"    Snapshot: {snapshot}")


if __name__ == "__main__":
    asyncio.run(main())
