"""
13 — Configuration (конфигурация из JSON и переменных окружения)
=================================================================

Демонстрирует систему конфигурации Kamio:
    - KamioApp(config_path="config.json") — загрузка из JSON-файла
    - Config из JSON-файла
    - Config из переменных окружения (Kamio_MQTT_BROKER, Kamio_LOG_LEVEL)
    - Вложенные ключи через __ (Kamio_MQTT__TLS__CAFILE)
    - Config.get() и Config.settings
    - Приоритет: переменная окружения > JSON-файл > значение по умолчанию

Запуск::
    python examples/13_config_env.py

Или с переменными окружения::
    # Linux/Mac:
    Kamio_MQTT_BROKER=mqtt://192.168.1.100:1883 \
    Kamio_LOG_LEVEL=DEBUG \
    Kamio_MQTT__TLS__CAFILE=/etc/ssl/certs/ca.pem \
    python examples/13_config_env.py

    # Windows (PowerShell):
    $env:Kamio_MQTT_BROKER = "mqtt://192.168.1.100:1883"
    $env:Kamio_LOG_LEVEL = "DEBUG"
    $env:Kamio_MQTT__TLS__CAFILE = "C:\\certs\\ca.pem"
    python examples/13_config_env.py

Предварительно запустите MQTT-брокер на localhost:1883
(например, ``docker run -p 1883:1883 eclipse-mosquitto``).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from typing import Any

from kamio import KamioApp, Config, Device, state

# Настройка логирования — уровень будет уточнён из конфигурации ниже
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("config_demo")


# =====================================================================
# Устройство для демонстрации
# =====================================================================

class SimpleSensor(Device):
    """Простой датчик с одним полем состояния."""

    value: float = state(default=0.0, writable=True, description="Значение датчика")


# =====================================================================
# Создание демонстрационного config.json
# =====================================================================

def create_demo_config() -> str:
    """Создаёт временный JSON-файл конфигурации и возвращает путь.

    В реальном проекте config.json существует рядом с приложением.
    Здесь мы создаём его программно для самодостаточности примера.
    """
    config_data = {
        # --- Известные ключи (маппятся в Settings) ---
        "mqtt_broker": "mqtt://localhost:1883",
        "log_level": "INFO",

        # --- Пользовательские (произвольные) ключи ---
        "app_name": "config_demo_app",
        "version": "1.0.0",

        # --- Вложенные ключи ---
        "mqtt": {
            "tls": {
                "cafile": None,
                "certfile": None,
                "keyfile": None,
            },
            "keepalive": 60,
        },
        "telemetry": {
            "min_freq": 0.5,
        },
        "thresholds": {
            "temperature": {
                "min": -10.0,
                "max": 50.0,
            },
            "humidity": {
                "min": 0.0,
                "max": 100.0,
            },
        },
    }

    # Создаём временный файл
    config_path = os.path.join(tempfile.gettempdir(), "kamio_config_demo.json")
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config_data, f, indent=2, ensure_ascii=False)

    logger.info(f"Создан config.json: {config_path}")
    return config_path


# =====================================================================
# Демонстрация Config напрямую (без KamioApp)
# =====================================================================

def demonstrate_config_standalone(config_path: str):
    """Демонстрирует использование Config без создания KamioApp.

    Config можно использовать как самостоятельный объект для чтения
    настроек из JSON-файла и переменных окружения.
    """
    logger.info("=== Config как самостоятельный объект ===")

    config = Config(config_path=config_path)

    # --- Config.settings — типизированный объект Settings ---
    # Settings — frozen dataclass с известными полями:
    #   - mqtt_broker: str = "mqtt://localhost:1883"
    #   - log_level: str = "INFO"
    settings = config.settings
    logger.info(f"settings.mqtt_broker = {settings.mqtt_broker!r}")
    logger.info(f"settings.log_level = {settings.log_level!r}")

    # --- Config.mqtt_broker и Config.log_level — короткие свойства ---
    logger.info(f"config.mqtt_broker = {config.mqtt_broker!r}")
    logger.info(f"config.log_level (int) = {config.log_level}")

    # --- Config.get() — универсальный метод получения значений ---
    # Поддерживает dot-нотацию для вложенных ключей
    logger.info("--- Config.get() с dot-нотацией ---")

    # Простые (плоские) ключи
    app_name = config.get("app_name", default="unknown")
    logger.info(f"app_name = {app_name!r}")

    version = config.get("version", default="0.0.0")
    logger.info(f"version = {version!r}")

    # Вложенные ключи через dot-нотацию
    keepalive = config.get("mqtt.keepalive", default=60, cast=int)
    logger.info(f"mqtt.keepalive = {keepalive}")

    min_freq = config.get("telemetry.min_freq", default=0.1, cast=float)
    logger.info(f"telemetry.min_freq = {min_freq}")

    # Глубоко вложенные ключи
    temp_min = config.get("thresholds.temperature.min", default=-999.0, cast=float)
    temp_max = config.get("thresholds.temperature.max", default=999.0, cast=float)
    logger.info(f"thresholds.temperature.min = {temp_min}")
    logger.info(f"thresholds.temperature.max = {temp_max}")

    # Несуществующий ключ — возвращает default
    missing = config.get("nonexistent.key", default="нет значения")
    logger.info(f"nonexistent.key = {missing!r}")

    # --- Config.get() с параметром cast ---
    # cast приводит значение к нужному типу
    logger.info("--- Config.get() с cast ---")
    raw_value = config.get("mqtt.keepalive", default="60")
    logger.info(f"mqtt.keepalive (без cast) = {raw_value!r} (тип: {type(raw_value).__name__})")

    casted_value = config.get("mqtt.keepalive", default=60, cast=int)
    logger.info(f"mqtt.keepalive (cast=int) = {casted_value!r} (тип: {type(casted_value).__name__})")

    # cast=bool для строковых значений
    # "true", "1", "yes", "on" -> True; остальное -> False
    bool_value = config.get("debug_mode", default="false", cast=bool)
    logger.info(f"debug_mode (cast=bool) = {bool_value}")

    # --- Переменные окружения ---
    # Config автоматически накладывает переменные Kamio_* поверх JSON
    logger.info("--- Переменные окружения Kamio_* ---")
    logger.info("Приоритет: Kamio_* env > JSON > default")
    logger.info(f"mqtt_broker (с учётом env) = {config.mqtt_broker!r}")

    # Проверяем, была ли переменная окружения установлена
    env_broker = os.environ.get("Kamio_MQTT_BROKER")
    if env_broker:
        logger.info(f"  (перекрыта env: Kamio_MQTT_BROKER={env_broker!r})")
    else:
        logger.info("  (env Kamio_MQTT_BROKER не установлена — используется JSON/default)")

    return config


# =====================================================================
# Демонстрация переменных окружения
# =====================================================================

def demonstrate_env_vars():
    """Демонстрирует маппинг переменных окружения на конфигурацию.

    Формат переменных:
        Kamio_<KEY>           -> плоский ключ (нижний регистр)
        Kamio_<KEY>__<SUBKEY> -> вложенный ключ (dot-нотация)

    Примеры:
        Kamio_MQTT_BROKER   -> mqtt_broker
        Kamio_LOG_LEVEL     -> log_level
        Kamio_MQTT__TLS__CAFILE -> mqtt.tls.cafile
        Kamio_APP_NAME      -> app_name
    """
    logger.info("=== Маппинг переменных окружения ===")

    # Показываем текущие переменные Kamio_*
    kamio_envs = {
        k: v for k, v in os.environ.items()
        if k.upper().startswith("KAMIO_")
    }

    if kamio_envs:
        logger.info("Найдены переменные окружения Kamio_*:")
        for key, value in sorted(kamio_envs.items()):
            # Показываем маппинг
            relative = key[len("Kamio_"):]
            if "__" in relative:
                dotted = relative.replace("__", ".").lower()
                logger.info(f"  {key} -> {dotted} = {value!r}")
            else:
                config_key = relative.lower()
                logger.info(f"  {key} -> {config_key} = {value!r}")
    else:
        logger.info("Переменные окружения Kamio_* не установлены.")
        logger.info("Попробуйте запустить с переменными:")
        logger.info("  Kamio_MQTT_BROKER=mqtt://broker:1883 python examples/13_config_env.py")
        logger.info("  Kamio_LOG_LEVEL=DEBUG python examples/13_config_env.py")
        logger.info("  Kamio_MQTT__TLS__CAFILE=/path/ca.pem python examples/13_config_env.py")


# =====================================================================
# Демонстрация KamioApp с config_path
# =====================================================================

async def demonstrate_app_with_config(config_path: str):
    """Демонстрирует создание KamioApp с config_path.

    KamioApp загружает Config из указанного файла и использует
    значения для mqtt_broker и log_level, если они не переданы
    явно в конструктор.
    """
    logger.info("=== KamioApp с config_path ===")

    # Создаём приложение с config_path
    # Если mqtt_broker не указан явно, берётся из Config
    # Если log_level не указан явно, берётся из Config
    app = KamioApp(
        config_path=config_path,
        client_id="config_demo",
    )

    # Доступ к конфигурации через app.config
    logger.info(f"app.config.mqtt_broker = {app.config.mqtt_broker!r}")
    logger.info(f"app.config.log_level = {app.config.log_level}")

    # Чтение пользовательских настроек через app.config.get()
    app_name = app.config.get("app_name", default="unnamed")
    logger.info(f"app.config.get('app_name') = {app_name!r}")

    # Вложенные ключи
    min_freq = app.config.get("telemetry.min_freq", default=0.1, cast=float)
    logger.info(f"app.config.get('telemetry.min_freq') = {min_freq}")

    # --- Запуск приложения ---
    app.register(SimpleSensor)
    await app.start()

    # Создаём устройство
    sensor = await app.add_device("sensor_1", SimpleSensor)

    # Используем конфигурацию для установки значения
    threshold = app.config.get("thresholds.temperature.max", default=50.0, cast=float)
    sensor.value = threshold * 0.5
    logger.info(f"sensor.value = {sensor.value} (половина от порога {threshold})")

    await asyncio.sleep(1)

    # --- Демонстрация Config.__repr__ ---
    logger.info(f"Config repr: {repr(app.config)}")

    await app.stop()


# =====================================================================
# Главный цикл
# =====================================================================

async def main():
    logger.info("=== Демонстрация конфигурации Kamio ===\n")

    # 1. Демонстрация переменных окружения
    demonstrate_env_vars()
    logger.info("")

    # 2. Создание демонстрационного config.json
    config_path = create_demo_config()

    # 3. Демонстрация Config как самостоятельного объекта
    demonstrate_config_standalone(config_path)
    logger.info("")

    # 4. Дополнительные демонстрации (без MQTT-брокера)
    demo_config_get_with_cast(config_path)
    demo_nested_keys(config_path)
    demo_settings_dataclass(config_path)
    demo_boolean_casting()
    demo_priority_chain()
    demo_nested_internals()

    # 5. Демонстрация KamioApp с config_path
    await demonstrate_app_with_config(config_path)

    # Очистка
    try:
        os.remove(config_path)
        logger.info(f"\nУдалён временный файл: {config_path}")
    except OSError:
        pass


# =====================================================================
# Демонстрация: Config.get с cast — приведение типов
# =====================================================================

def demo_config_get_with_cast(config_path: str):
    """Подробно показывает параметр cast в Config.get().

    cast — опциональная функция для приведения значения к нужному типу.
    Поддерживается: int, float, bool, str, и любые callable.

    Особенности:
    - cast=bool: строки "true", "1", "yes", "on" → True; остальное → False
    - cast=int/float: стандартное преобразование
    - При ошибке cast: логируется warning, возвращается default
    - Если value is None и cast задан: возвращается default
    """
    logger.info("=== Демонстрация: Config.get с cast ===")

    config = Config(config_path=config_path)

    # cast=int — преобразование строки/float в int
    keepalive = config.get("mqtt.keepalive", default=60, cast=int)
    logger.info(f"mqtt.keepalive (cast=int): {keepalive} (тип: {type(keepalive).__name__})")

    # cast=float — преобразование в float
    min_freq = config.get("telemetry.min_freq", default=0.1, cast=float)
    logger.info(f"telemetry.min_freq (cast=float): {min_freq} (тип: {type(min_freq).__name__})")

    # cast=bool — особая логика для строк
    # "true", "1", "yes", "on" → True; всё остальное → False
    # Если значение уже bool, возвращается как есть
    verbose = config.get("logging.verbose", default=False, cast=bool)
    logger.info(f"logging.verbose (cast=bool): {verbose} (тип: {type(verbose).__name__})")

    # cast с несуществующим ключом — возвращает default
    missing = config.get("nonexistent", default=42, cast=int)
    logger.info(f"nonexistent (cast=int, default=42): {missing}")

    # cast с ошибкой преобразования — возвращает default и логирует warning
    # Создаём конфиг с строковым значением, которое нельзя преобразовать в int
    bad_val = config.get("app_name", default=0, cast=int)
    logger.info(f"app_name (cast=int, будет ошибка): {bad_val} (вернулся default при ошибке cast)")

    # cast=str — преобразование в строку
    broker_str = config.get("mqtt_broker", default="", cast=str)
    logger.info(f"mqtt_broker (cast=str): {broker_str!r}")

    logger.info("Демонстрация Config.get с cast завершена\n")


# =====================================================================
# Демонстрация: вложенные ключи (dot notation)
# =====================================================================

def demo_nested_keys(config_path: str):
    """Подробно показывает доступ к вложенным ключам через dot-нотацию.

    Config.get() поддерживает dot-нотацию для вложенных словарей:
    "mqtt.tls.cafile" → config["mqtt"]["tls"]["cafile"]

    Внутренне используется _get_nested(data, key), которая проходит
    по частям ключа, разделённым точками.
    """
    logger.info("=== Демонстрация: вложенные ключи (dot notation) ===")

    config = Config(config_path=config_path)

    # Одноуровневая вложенность
    mqtt_keepalive = config.get("mqtt.keepalive", default=60)
    logger.info(f"mqtt.keepalive = {mqtt_keepalive}")

    # Двухуровневая вложенность
    tls_cafile = config.get("mqtt.tls.cafile", default=None)
    logger.info(f"mqtt.tls.cafile = {tls_cafile!r}")

    tls_certfile = config.get("mqtt.tls.certfile", default=None)
    logger.info(f"mqtt.tls.certfile = {tls_certfile!r}")

    # Трёхуровневая вложенность
    temp_min = config.get("thresholds.temperature.min", default=-999.0, cast=float)
    logger.info(f"thresholds.temperature.min = {temp_min}")

    temp_max = config.get("thresholds.temperature.max", default=999.0, cast=float)
    logger.info(f"thresholds.temperature.max = {temp_max}")

    hum_min = config.get("thresholds.humidity.min", default=-1.0, cast=float)
    logger.info(f"thresholds.humidity.min = {hum_min}")

    # Несуществующий вложенный ключ — возвращает default
    missing = config.get("mqtt.tls.nonexistent", default="нет значения")
    logger.info(f"mqtt.tls.nonexistent = {missing!r}")

    # Несуществующий корневой ключ — возвращает default
    missing_root = config.get("nonexistent.deep.key", default="нет")
    logger.info(f"nonexistent.deep.key = {missing_root!r}")

    logger.info("Демонстрация вложенных ключей завершена\n")


# =====================================================================
# Демонстрация: settings dataclass — типизированный доступ
# =====================================================================

def demo_settings_dataclass(config_path: str):
    """Подробно показывает доступ к типизированным настройкам через Settings.

    Config.settings возвращает frozen dataclass Settings с известными полями:
    - mqtt_broker: str = "mqtt://localhost:1883"
    - log_level: str = "INFO"

    Settings валидируется при создании Config:
    - mqtt_broker должен быть строкой (иначе — default)
    - log_level должен быть из _LOG_LEVEL_NAMES (иначе — "INFO")
    """
    logger.info("=== Демонстрация: settings dataclass ===")

    config = Config(config_path=config_path)
    settings = config.settings

    # Settings — frozen dataclass (immutable)
    logger.info(f"settings.mqtt_broker = {settings.mqtt_broker!r}")
    logger.info(f"settings.log_level = {settings.log_level!r}")

    # Проверяем, что Settings — frozen (нельзя изменить)
    logger.info(f"Settings is frozen: {settings.__dataclass_params__.frozen}")

    # Короткие свойства Config делегируют в Settings
    logger.info(f"config.mqtt_broker = {config.mqtt_broker!r}")
    logger.info(f"config.log_level (int) = {config.log_level} (logging constant)")

    # log_level как строка vs как int
    logger.info(f"settings.log_level (str) = {settings.log_level!r}")
    logger.info(f"config.log_level (int) = {config.log_level} (например, 20 = INFO)")

    # Проверка валидации: что если log_level невалидный?
    # Config._validate_settings проверяет, что level в _LOG_LEVEL_NAMES
    # Если нет — fallback на "INFO"
    logger.info(f"Допустимые уровни логирования: DEBUG, INFO, WARNING, ERROR, CRITICAL")

    logger.info("Демонстрация settings dataclass завершена\n")


# =====================================================================
# Демонстрация: boolean casting — преобразование строк в bool
# =====================================================================

def demo_boolean_casting():
    """Подробно показывает логику cast=bool в Config.get().

    При cast=bool и строковом значении:
    - "true", "1", "yes", "on" (case-insensitive) → True
    - Всё остальное → False

    При cast=bool и нестроковом значении:
    - bool(value) — стандартное Python преобразование
    """
    logger.info("=== Демонстрация: boolean casting ===")

    # Создаём временный конфиг с тестовыми значениями
    config_data = {
        "flag_true": "true",
        "flag_false": "false",
        "flag_one": "1",
        "flag_zero": "0",
        "flag_yes": "yes",
        "flag_no": "no",
        "flag_on": "on",
        "flag_off": "off",
        "flag_empty": "",
        "flag_bool_true": True,
        "flag_bool_false": False,
        "flag_int_1": 1,
        "flag_int_0": 0,
    }

    config_path = os.path.join(tempfile.gettempdir(), "kamio_bool_test.json")
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config_data, f)

    config = Config(config_path=config_path)

    # Строковые значения → cast=bool
    test_cases = [
        ("flag_true", "true"),
        ("flag_false", "false"),
        ("flag_one", "1"),
        ("flag_zero", "0"),
        ("flag_yes", "yes"),
        ("flag_no", "no"),
        ("flag_on", "on"),
        ("flag_off", "off"),
        ("flag_empty", ""),
    ]

    logger.info("Строковые значения (cast=bool):")
    for key, original in test_cases:
        result = config.get(key, default=False, cast=bool)
        logger.info(f"  '{original}' → {result}")

    # Нестроковые значения → cast=bool (стандартный bool())
    logger.info("Нестроковые значения (cast=bool):")
    for key, label in [("flag_bool_true", "True"), ("flag_bool_false", "False"),
                        ("flag_int_1", "1 (int)"), ("flag_int_0", "0 (int)")]:
        result = config.get(key, default=False, cast=bool)
        logger.info(f"  {label} → {result}")

    # Очистка
    try:
        os.remove(config_path)
    except OSError:
        pass

    logger.info("Демонстрация boolean casting завершена\n")


# =====================================================================
# Демонстрация: приоритет env > file > default
# =====================================================================

def demo_priority_chain():
    """Подробно показывает цепочку приоритета конфигурации.

    Приоритет значений (от высшего к низшему):
    1. Переменная окружения (Kamio_KEY)
    2. Значение из JSON-файла
    3. Значение по умолчанию (default в Config.get() или Settings)

    _overlay_env() накладывает переменные окружения поверх JSON-значений.
    """
    logger.info("=== Демонстрация: приоритет env > file > default ===")

    # Создаём конфиг с известным значением
    config_data = {
        "mqtt_broker": "mqtt://from-file:1883",
        "log_level": "WARNING",
        "custom_key": "file_value",
    }
    config_path = os.path.join(tempfile.gettempdir(), "kamio_priority_test.json")
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config_data, f)

    # Уровень 3: default (нет в файле, нет в env)
    config = Config(config_path=config_path)
    missing = config.get("nonexistent_key", default="default_value")
    logger.info(f"Уровень 3 (default): nonexistent_key = {missing!r}")

    # Уровень 2: file (есть в файле, нет в env)
    file_val = config.get("custom_key", default="default_value")
    logger.info(f"Уровень 2 (file): custom_key = {file_val!r}")

    # Уровень 2: file для mqtt_broker
    broker = config.mqtt_broker
    logger.info(f"Уровень 2 (file): mqtt_broker = {broker!r}")

    # Уровень 1: env (если установлена переменная Kamio_CUSTOM_KEY)
    env_val = os.environ.get("Kamio_CUSTOM_KEY")
    if env_val:
        logger.info(f"Уровень 1 (env): Kamio_CUSTOM_KEY установлена = {env_val!r}")
        # Config.get вернёт значение из env, а не из файла
        config_val = config.get("custom_key", default="default")
        logger.info(f"  Config.get('custom_key') = {config_val!r} (env перекрывает file)")
    else:
        logger.info("Уровень 1 (env): Kamio_CUSTOM_KEY не установлена")
        logger.info("  Попробуйте: Kamio_CUSTOM_KEY=env_value python examples/13_config_env.py")

    # Демонстрация: env перекрывает file для mqtt_broker
    env_broker = os.environ.get("Kamio_MQTT_BROKER")
    if env_broker:
        logger.info(f"Уровень 1 (env): Kamio_MQTT_BROKER = {env_broker!r}")
        logger.info(f"  Config.mqtt_broker = {config.mqtt_broker!r} (env перекрывает file)")
    else:
        logger.info("Уровень 1 (env): Kamio_MQTT_BROKER не установлена — используется file value")

    # Очистка
    try:
        os.remove(config_path)
    except OSError:
        pass

    logger.info("Демонстрация приоритета завершена\n")


# =====================================================================
# Демонстрация: _get_nested vs _set_nested — внутренние методы
# =====================================================================

def demo_nested_internals():
    """Показывает внутренние методы _get_nested и _set_nested.

    Config._get_nested(data, key) — ищет значение по dot-ключу в словаре.
    Возвращает None, если ключ не найден.

    Config._set_nested(data, key, value) — устанавливает значение
    по dot-ключу, создавая промежуточные словари при необходимости.

    Эти методы используются внутри Config для:
    - get(): _get_nested для поиска значения
    - _overlay_env(): _set_nested для установки env-значений
    """
    logger.info("=== Демонстрация: _get_nested vs _set_nested ===")

    # Тестовый словарь
    data = {
        "mqtt": {
            "host": "localhost",
            "port": 1883,
            "tls": {
                "cafile": "/path/ca.pem",
            },
        },
        "app_name": "test",
    }

    # _get_nested — чтение
    logger.info("Тестовый словарь:")
    logger.info(f"  {data}")

    # Чтение существующего ключа
    host = Config._get_nested(data, "mqtt.host")
    logger.info(f"\n_get_nested(data, 'mqtt.host') = {host!r}")

    # Чтение глубоко вложенного ключа
    cafile = Config._get_nested(data, "mqtt.tls.cafile")
    logger.info(f"_get_nested(data, 'mqtt.tls.cafile') = {cafile!r}")

    # Чтение несуществующего ключа → None
    missing = Config._get_nested(data, "mqtt.tls.nonexistent")
    logger.info(f"_get_nested(data, 'mqtt.tls.nonexistent') = {missing!r}")

    # Чтение ключа, где промежуточный элемент не dict → None
    bad = Config._get_nested(data, "app_name.sub")
    logger.info(f"_get_nested(data, 'app_name.sub') = {bad!r} (app_name не dict)")

    # _set_nested — запись
    logger.info("\n_set_nested — запись:")

    # Установка нового значения в существующий путь
    Config._set_nested(data, "mqtt.port", 8883)
    logger.info(f"_set_nested(data, 'mqtt.port', 8883) → data['mqtt']['port'] = {data['mqtt']['port']}")

    # Установка нового вложенного ключа
    Config._set_nested(data, "mqtt.tls.certfile", "/path/cert.pem")
    logger.info(f"_set_nested(data, 'mqtt.tls.certfile', ...) → data['mqtt']['tls']['certfile'] = {data['mqtt']['tls']['certfile']!r}")

    # Установка полностью нового пути (создаёт промежуточные dict)
    Config._set_nested(data, "new.deep.path", "value")
    logger.info(f"_set_nested(data, 'new.deep.path', 'value') → data['new'] = {data['new']}")

    # Перезапись существующего значения
    Config._set_nested(data, "app_name", "updated")
    logger.info(f"_set_nested(data, 'app_name', 'updated') → data['app_name'] = {data['app_name']!r}")

    logger.info(f"\nИтоговый словарь: {data}")
    logger.info("Демонстрация _get_nested / _set_nested завершена\n")


if __name__ == "__main__":
    asyncio.run(main())
