"""
09 — Hot Reload (горячая перезагрузка)
=======================================

Демонстрирует систему горячей перезагрузки Kamio:
    - app.watch_directory() для отслеживания правил (rules)
    - app.watch_file() для отслеживания конфигурации (config)
    - app.enable_hot_reload() / app.disable_hot_reload()
    - make_rules_handler(), make_devices_handler(), make_config_handler()
    - События Hot Reload на EventBus

Запуск::
    python examples/09_hot_reload.py

Предварительно запустите MQTT-брокер на localhost:1883
(например, ``docker run -p 1883:1883 eclipse-mosquitto``).

Для проверки горячего обновления:
    1. Запустите пример — он создаст файлы rules/my_rules.py и config.json
    2. Отредактируйте rules/my_rules.py (измените логику правила)
    3. Сохраните файл — фреймворк автоматически перезагрузит правила
    4. Отредактируйте config.json — конфигурация будет перезагружена
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Dict

from kamio import KamioApp, Device, state

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("hot_reload_demo")

# Директория для файлов, которые будем отслеживать
DEMO_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hot_reload_demo_files")


# =====================================================================
# Устройство для демонстрации
# =====================================================================

class CounterDevice(Device):
    """Простой счётчик для демонстрации горячего обновления правил."""

    count: int = state(default=0, writable=True, description="Значение счётчика")
    threshold: int = state(default=5, writable=True, description="Порог срабатывания")


# =====================================================================
# Создание демонстрационных файлов
# =====================================================================

def create_demo_files():
    """Создаёт файлы правил и конфигурации для демонстрации.

    В реальном проекте эти файлы уже существуют и редактируются
    разработчиком. Здесь мы создаём их программно, чтобы пример
    был самодостаточным.
    """
    os.makedirs(DEMO_DIR, exist_ok=True)

    # --- Файл с правилами ---
    rules_content = '''\
"""
Демонстрационные правила для hot reload.
Отредактируйте этот файл и сохраните — правила перезагрузятся автоматически.
"""
from __future__ import annotations

from kamio import RuleEvent


async def on_count_change(event: RuleEvent, app):
    """Срабатывает при изменении поля 'count' на CounterDevice."""
    count = event.data.get("count", 0)
    # Получаем порог из конфигурации приложения
    threshold = app.config.get("counter.threshold", 5, cast=int)
    if count >= threshold:
        app.logger.info(f"[RULE] Счётчик достиг порога {threshold}: count={count}")
    else:
        app.logger.info(f"[RULE] Счётчик изменён: count={count} (порог {threshold})")
'''
    rules_path = os.path.join(DEMO_DIR, "my_rules.py")
    with open(rules_path, "w", encoding="utf-8") as f:
        f.write(rules_content)
    logger.info(f"Создан файл правил: {rules_path}")

    # --- Файл конфигурации ---
    config_data = {
        "counter": {
            "threshold": 5,
            "step": 1,
        },
        "logging": {
            "verbose": True,
        },
    }
    config_path = os.path.join(DEMO_DIR, "config.json")
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config_data, f, indent=2, ensure_ascii=False)
    logger.info(f"Создан файл конфигурации: {config_path}")

    return rules_path, config_path


# =====================================================================
# Кастомный обработчик для перезагрузки конфигурации
# =====================================================================

async def on_config_changed(file_path: str) -> None:
    """Кастомный обработчик изменения конфигурации.

    Вызывается при каждом сохранении отслеживаемого config-файла.
    Здесь мы просто логируем событие — реальная логика может
    перечитать конфигурацию и применить новые настройки.
    """
    logger.info(f"[config_handler] Файл конфигурации изменён: {file_path}")
    # Можно перечитать файл и обновить настройки:
    #   with open(file_path) as f:
    #       new_config = json.load(f)
    #   ... применить new_config ...


# =====================================================================
# Подписчики на события Hot Reload через EventBus
# =====================================================================

async def on_hot_reload_rules(data: Dict[str, Any]) -> None:
    """Событие hot_reload_rules — публикуется после успешной перезагрузки правил.

    Структура data:
        - file_path: путь к перезагруженному файлу
        - replaced:  количество заменённых правил
    """
    logger.info(
        f"[EVENT] Правила перезагружены: {data.get('file_path')}, "
        f"заменено: {data.get('replaced', 0)}"
    )


async def on_hot_reload_error(data: Dict[str, Any]) -> None:
    """Событие hot_reload_error — публикуется при ошибке перезагрузки.

    Структура data:
        - file_path: путь к файлу, вызвавшему ошибку
        - error:     строка ошибки
    """
    logger.error(
        f"[EVENT] Ошибка hot reload: {data.get('file_path')}: {data.get('error')}"
    )


# =====================================================================
# Главный цикл
# =====================================================================

async def main():
    # --- Создаём демонстрационные файлы ---
    rules_path, config_path = create_demo_files()

    # --- Создаём приложение ---
    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="hot_reload_demo")

    # --- Подписка на события Hot Reload через EventBus ---
    app.subscribe_event("hot_reload_rules", on_hot_reload_rules)
    app.subscribe_event("hot_reload_error", on_hot_reload_error)

    # --- Регистрируем устройство ---
    app.register(CounterDevice)

    # --- Настройка Hot Reload ---

    # 1. watch_directory() — отслеживаем *.py файлы в директории с правилами.
    #    Используем make_rules_handler() — встроенный обработчик, который
    #    находит функции с маркером _is_rule в файле и заменяет правила
    #    в RuleEngine по совпадению имени функции.
    rules_handler = app.hot_reload.make_rules_handler()
    app.watch_directory(DEMO_DIR, "*.py", rules_handler)
    logger.info(f"Отслеживание директории правил: {DEMO_DIR}/*.py")

    # 2. watch_file() — отслеживаем конкретный файл конфигурации.
    #    Можно использовать make_config_handler() (встроенный) или
    #    свой кастомный обработчик.
    #
    #    Вариант A: встроенный обработчик (перезагружает Config):
    #       config_handler = app.hot_reload.make_config_handler()
    #
    #    Вариант B: кастомный обработчик (полный контроль):
    #       app.watch_file(config_path, on_config_changed)
    #
    #    Используем кастомный для демонстрации:
    app.watch_file(config_path, on_config_changed)
    logger.info(f"Отслеживание файла конфигурации: {config_path}")

    # 3. Дополнительный пример: watch_directory для классов устройств
    #    make_devices_handler() перезагружает классы Device из изменённых файлов.
    #    Запущенные экземпляры продолжают работать со старым классом до перезапуска.
    #    (Раскомментируйте для использования:)
    # devices_handler = app.hot_reload.make_devices_handler()
    # app.watch_directory(DEMO_DIR, "*device*.py", devices_handler)

    # --- Включаем Hot Reload ---
    # Важно: вызывать после всех watch_file/watch_directory.
    # Использует watchdog (OS-level events) если установлен, иначе polling.
    app.enable_hot_reload()
    logger.info("Hot Reload включён")

    # Проверяем, что Hot Reload действительно включён
    logger.info(f"Hot Reload активен: {app.hot_reload.is_enabled}")
    logger.info(f"Отслеживаемые пути: {app.hot_reload.list_watched()}")

    # --- Запуск приложения ---
    await app.start()

    # --- Создаём устройство ---
    counter = await app.add_device("counter_1", CounterDevice)

    # --- Демонстрация: изменяем счётчик ---
    # Правило on_count_change (из my_rules.py) сработает при изменении count
    logger.info("=== Демонстрация: изменяем count ===")

    for i in range(1, 8):
        counter.count = i
        await asyncio.sleep(0.5)

    # --- Дополнительные демонстрации ---
    await demo_reload_devices(app)
    await demo_reload_config(app, config_path)
    await demo_hot_reload_error_event(app)
    demo_debounce_config()
    demo_list_watched(app)
    demo_is_enabled(app)

    # --- Инструкция для пользователя ---
    logger.info("=== Hot Reload готов к тестированию ===")
    logger.info(f"Отредактируйте файл: {rules_path}")
    logger.info("  Например, измените сообщение в on_count_change")
    logger.info("  и сохраните — правило перезагрузится автоматически.")
    logger.info(f"Или отредактируйте: {config_path}")
    logger.info("  Измените counter.threshold и сохраните.")
    logger.info("Нажмите Ctrl+C для выхода.")

    # --- Hold ---
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Останавливаемся...")

    # --- Отключение Hot Reload ---
    # disable_hot_reload() — асинхронный, останавливает polling/observer
    await app.disable_hot_reload()
    logger.info("Hot Reload отключён")

    await app.stop()

    # --- Очистка демонстрационных файлов (опционально) ---
    # Раскомментируйте, чтобы удалить файлы после завершения:
    # import shutil
    # shutil.rmtree(DEMO_DIR, ignore_errors=True)


# =====================================================================
# Демонстрация: reload_devices_from_file — перезагрузка классов устройств
# =====================================================================

async def demo_reload_devices(app):
    """Показывает использование reload_devices_from_file().

    reload_devices_from_file() загружает Python-модуль и находит
    все классы, унаследованные от Device. Найденные классы заменяются
    в DeviceRegistry (app.registry.classes).

    Важно: запущенные экземпляры устройств продолжают использовать
    старый класс до перезапуска. Новый класс применяется при следующем
    add_device().

    При ошибке загрузки выполняется откат к предыдущему состоянию реестра.
    """
    logger.info("=== Демонстрация: reload_devices_from_file ===")

    from kamio.core.hot_reload import reload_devices_from_file

    # Создаём временный файл с классом устройства
    device_file = os.path.join(DEMO_DIR, "demo_device.py")
    device_content = '''\
"""Временный класс устройства для демонстрации reload_devices_from_file."""
from __future__ import annotations
from kamio import Device, state


class ReloadableDevice(Device):
    """Устройство, которое можно перезагрузить через hot reload."""
    value: int = state(default=0, writable=True, description="Значение")
'''
    with open(device_file, "w", encoding="utf-8") as f:
        f.write(device_content)

    # До перезагрузки — класса ReloadableDevice нет в реестре
    logger.info(f"Классы в реестре ДО: {list(app.registry.classes.keys())}")

    # Перезагружаем — находим и регистрируем ReloadableDevice
    success = await reload_devices_from_file(device_file, app)
    logger.info(f"reload_devices_from_file вернул: {success}")
    logger.info(f"Классы в реестре ПОСЛЕ: {list(app.registry.classes.keys())}")

    if "reloadabledevice" in app.registry.classes:
        logger.info("ReloadableDevice успешно зарегистрирован!")
    else:
        logger.warning("ReloadableDevice не найден в реестре")

    # Очистка
    try:
        os.remove(device_file)
    except OSError:
        pass


# =====================================================================
# Демонстрация: reload_config_from_file — перезагрузка конфигурации
# =====================================================================

async def demo_reload_config(app, config_path):
    """Показывает использование reload_config_from_file().

    reload_config_from_file() загружает JSON (или YAML) файл конфигурации
    и публикует событие 'hot_reload_config' с новыми настройками.
    Подписчики на это событие могут применить новые настройки.
    """
    logger.info("\n=== Демонстрация: reload_config_from_file ===")

    from kamio.core.hot_reload import reload_config_from_file

    # Подписываемся на событие hot_reload_config
    async def on_config_reloaded(data: Dict[str, Any]) -> None:
        logger.info(f"[hot_reload_config] Файл: {data.get('file_path')}")
        config = data.get("config", {})
        logger.info(f"[hot_reload_config] Новые настройки: {config}")

    app.subscribe_event("hot_reload_config", on_config_reloaded)

    # Вызываем reload_config_from_file напрямую
    success = await reload_config_from_file(config_path, app)
    logger.info(f"reload_config_from_file вернул: {success}")

    await asyncio.sleep(0.3)


# =====================================================================
# Демонстрация: hot_reload_error event — событие ошибки перезагрузки
# =====================================================================

async def demo_hot_reload_error_event(app):
    """Показывает публикацию события hot_reload_error при ошибке.

    При ошибке в обработчике hot-reload (например, синтаксическая ошибка
    в Python-файле), HotReloadManager публикует событие 'hot_reload_error'
    с информацией о файле и ошибке.
    """
    logger.info("\n=== Демонстрация: hot_reload_error event ===")

    # Создаём файл с синтаксической ошибкой
    bad_file = os.path.join(DEMO_DIR, "bad_rules.py")
    with open(bad_file, "w", encoding="utf-8") as f:
        f.write("def broken(:\n    pass\n")  # Синтаксическая ошибка

    # Подписчик на hot_reload_error уже зарегистрирован в main()
    # Дополнительно логируем
    error_received: list[str] = []

    async def on_error(data: Dict[str, Any]) -> None:
        error_received.append(data.get("error", ""))
        logger.info(f"[hot_reload_error demo] Ошибка: {data.get('error')}")

    app.subscribe_event("hot_reload_error", on_error)

    # Пытаемся перезагрузить файл с ошибкой
    from kamio.core.hot_reload import reload_rules_from_file
    success = await reload_rules_from_file(bad_file, app)
    logger.info(f"reload_rules_from_file для bad_rules.py вернул: {success} (ожидается False)")
    logger.info(f"Ошибки перехвачены: {len(error_received)}")

    # Очистка
    try:
        os.remove(bad_file)
    except OSError:
        pass


# =====================================================================
# Демонстрация: debounce настройка
# =====================================================================

def demo_debounce_config():
    """Показывает настройку debounce в HotReloadManager.

    debounce — задержка (в секундах) перед вызовом обработчика после
    обнаружения изменения файла. Предотвращает множественные вызовы
    при быстром сохранении (например, редактор сохраняет временный файл
    и затем основной).

    По умолчанию debounce=0.3 (300 мс). Можно изменить через конструктор
    HotReloadManager или через параметры app.enable_hot_reload().
    """
    logger.info("=== Демонстрация: debounce настройка ===")

    # Доступ к HotReloadManager через app.hot_reload
    # debounce хранится в _debounce (приватный атрибут)
    # poll_interval — интервал polling в секундах
    logger.info("HotReloadManager параметры:")
    logger.info(f"  debounce: {0.3} (по умолчанию, 300 мс задержка перед вызовом)")
    logger.info(f"  poll_interval: {1.0} (по умолчанию, интервал polling)")
    logger.info("  При watchdog доступен — используется OS-level events вместо polling")
    logger.info("  debounce предотвращает дублирование при быстром сохранении")

    # В реальном коде можно настроить:
    #   app = KamioApp(...)
    #   app.hot_reload._debounce = 0.5  # 500 мс
    #   app.hot_reload._poll_interval = 2.0  # polling каждые 2 сек

    logger.info("Демонстрация debounce завершена\n")


# =====================================================================
# Демонстрация: list_watched — список отслеживаемых файлов
# =====================================================================

def demo_list_watched(app):
    """Показывает использование list_watched().

    list_watched() возвращает список всех путей, отслеживаемых
    HotReloadManager. Включает как файлы (watch_file), так и
    директории (watch_directory).
    """
    logger.info("\n=== Демонстрация: list_watched ===")

    watched = app.hot_reload.list_watched()
    logger.info(f"Отслеживаемых путей: {len(watched)}")
    for i, path in enumerate(watched):
        # Определяем тип: файл или директория
        if os.path.isdir(path):
            kind = "директория"
        elif os.path.isfile(path):
            kind = "файл"
        else:
            kind = "не существует"
        logger.info(f"  {i+1}. [{kind}] {path}")


# =====================================================================
# Демонстрация: is_enabled property — проверка статуса
# =====================================================================

def demo_is_enabled(app):
    """Показывает использование свойства is_enabled.

    is_enabled возвращает True, если HotReloadManager активен
    (вызван enable() и не вызван disable()). Позволяет проверить
    статус перед операциями, зависящими от hot reload.
    """
    logger.info("\n=== Демонстрация: is_enabled property ===")

    # Проверяем статус
    enabled = app.hot_reload.is_enabled
    logger.info(f"HotReloadManager.is_enabled = {enabled}")

    if enabled:
        logger.info("  → Hot Reload активен, файлы отслеживаются")
    else:
        logger.info("  → Hot Reload неактивен, изменения не отслеживаются")

    # is_enabled — это property, доступное только для чтения
    # Внутренне проверяет self._enabled
    logger.info(f"  Внутренний флаг _enabled: {app.hot_reload._enabled}")


if __name__ == "__main__":
    asyncio.run(main())
