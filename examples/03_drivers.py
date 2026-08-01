"""
03 — Драйверы оборудования
==========================

Демонстрирует все типы драйверов, доступных в kamio, и способы их
использования с устройствами.

Запуск (требуется MQTT-брокер на localhost:1883)::

    python examples/03_drivers.py

Что демонстрирует:
    - MockHardwareDriver с симуляцией задержки и сбоев (рабочий демо-режим)
    - TelnetDriver, UDPDriver, ModbusTCPDriver, SerialDriver,
      HTTPDeviceDriver, GPIOChipDriver (комментированные примеры)
    - Вызов driver.read() и driver.execute() через устройство
    - Прямой вызов driver.read() / driver.execute() вне устройства
    - Асинхронный контекстный менеджер (async with driver:)
    - Передача драйвера в конструктор Device(driver=...)
    - handle_state() и handle_command() с драйвером
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from kamio import KamioApp, Device, command, state, telemetry
from kamio.drivers.mock import MockHardwareDriver

# Остальные драйверы импортируем для документации в комментариях.
# В реальном коде раскомментируйте нужный импорт и используйте его.
# from kamio.drivers.telnet import TelnetDriver
# from kamio.drivers.udp import UDPDriver
# from kamio.drivers.modbus import ModbusTCPDriver
# from kamio.drivers.serial import SerialDriver
# from kamio.drivers.http import HTTPDeviceDriver
# from kamio.drivers.gpio import GPIOChipDriver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("drivers_demo")


# =====================================================================
# Устройство с драйвером
# =====================================================================

class ClimateController(Device):
    """
    Климат-контроллер с датчиком температуры и управляемым реле.

    Драйвер отвечает за чтение сенсора и выполнение команд на оборудовании.
    Поля telemetry читаются из драйвера, поля state — управляемые.
    """

    # --- Телеметрия (читается из драйвера автоматически) ---
    temperature: float = telemetry(default=0.0, unit="°C", freq="5s", description="Температура")
    humidity: float = telemetry(default=0.0, unit="%", freq="5s", description="Влажность")

    # --- Состояние (управляемое, записывается через драйвер) ---
    relay_on: bool = state(default=False, writable=True, description="Состояние реле")
    fan_speed: int = state(default=0, min=0, max=100, writable=True, description="Скорость вентилятора")

    @command
    async def emergency_stop(self):
        """Аварийная остановка: выключить реле и вентилятор."""
        self.relay_on = False
        self.fan_speed = 0
        logger.warning(f"Аварийная остановка на устройстве {self.node.device_id}")
        return {"relay_on": False, "fan_speed": 0}

    async def on_start(self, node):
        """Вызывается после старта узла устройства."""
        await super().on_start(node)
        logger.info(f"ClimateController '{node.device_id}' запущен")


# =====================================================================
# Примеры создания драйверов (комментированные)
# =====================================================================
#
# --- TelnetDriver — для legacy-оборудования по Telnet ---
#
#   driver = TelnetDriver(
#       host="192.168.1.100",
#       port=23,                # стандартный Telnet-порт
#       timeout=5.0,            # таймаут чтения/записи (сек)
#       max_reconnect_attempts=3,
#   )
#   # execute("reboot", {"command": "reboot", "wait_response": True})
#   # read("status", {"command": "show status"})
#
# --- UDPDriver — для датчиков и устройств по UDP ---
#
#   driver = UDPDriver(
#       host="192.168.1.50",
#       port=5000,
#       timeout=1.0,
#       local_port=0,           # 0 = ОС выберет свободный порт
#   )
#   # execute("set_power", {"value": True, "command": "PWR ON", "wait_response": False})
#   # read("temperature", {"command": "GET TEMP", "read_bytes": 1024})
#
# --- ModbusTCPDriver — для ПЛК и промышленных контроллеров ---
#
#   driver = ModbusTCPDriver(
#       host="192.168.1.200",
#       port=502,               # стандартный Modbus TCP-порт
#       unit_id=1,              # адрес slave-устройства
#       timeout=1.0,
#       reconnect_attempts=1,
#   )
#   # read("temp", {"type": "holding", "address": 0, "count": 1})
#   # execute("write_register", {"address": 10, "value": 42})
#   # execute("write_coil", {"address": 5, "value": True})
#
# --- SerialDriver — для RS-232 / RS-485 (требует pyserial) ---
#
#   driver = SerialDriver(
#       port="/dev/ttyUSB0",    # или "COM3" на Windows
#       baudrate=9600,
#       timeout=1.0,
#       read_limit=4096,        # макс. байт на одно чтение
#   )
#   # execute("set_temp", {"command": "SET TEMP", "value": 25, "wait_response": True})
#   # read("pressure", {"command": "GET PRESS"})
#
# --- HTTPDeviceDriver — для REST API и IP-камер (требует aiohttp) ---
#
#   driver = HTTPDeviceDriver(
#       base_url="http://192.168.1.80/api",
#       headers={"Authorization": "Bearer token123"},
#       timeout=10.0,
#   )
#   # read("status", {"path": "/device/status"})
#   # execute("reboot", {"method": "POST", "path": "/device/reboot", "json": {"delay": 5}})
#
# --- GPIOChipDriver — для GPIO через libgpiod (требует gpiod, только Linux) ---
#
#   driver = GPIOChipDriver(
#       chip_path="/dev/gpiochip4",
#   )
#   # read("button", {"pin": 17})
#   # execute("set_output", {"pin": 18, "value": True})


# =====================================================================
# Демонстрация прямого использования драйвера (без устройства)
# =====================================================================

async def demo_standalone_driver():
    """
    Показывает, что драйвер можно использовать напрямую — без Device и KamioApp.

    Это удобно для тестирования оборудования и отладки протокола.
    Здесь используется асинхронный контекстный менеджер (async with),
    который автоматически вызывает connect() и disconnect().
    """
    logger.info("=== Демонстрация: прямой вызов драйвера ===")

    # Создаём mock-драйвер с симуляцией задержки 10-50 мс и без сбоев
    driver = MockHardwareDriver(
        latency_range=(0.01, 0.05),
        failure_rate=0.0,
        initial_state={"temperature": 23.5, "humidity": 45.0, "relay": False},
    )

    # Асинхронный контекстный менеджер: __aenter__ вызывает connect(),
    # __aexit__ — disconnect(). Это гарантирует освобождение ресурсов.
    async with driver:
        # Чтение значения сенсора напрямую через драйвер
        temp = await driver.read("temperature")
        logger.info(f"Прямое чтение temperature: {temp} °C")

        hum = await driver.read("humidity")
        logger.info(f"Прямое чтение humidity: {hum} %")

        # Выполнение команды напрямую через драйвер
        # MockHardwareDriver распознаёт команды вида "set_<field>"
        result = await driver.execute("set_relay", {"value": True})
        logger.info(f"Прямой execute set_relay: {result}")

        # Проверяем, что состояние изменилось
        relay_val = await driver.read("relay")
        logger.info(f"Реле после команды: {relay_val}")

    logger.info("Драйвер автоматически отключён через контекстный менеджер\n")


# =====================================================================
# Демонстрация симуляции сбоев
# =====================================================================

async def demo_failure_simulation():
    """
    Показывает симуляцию сбоев через failure_rate.

    MockHardwareDriver с failure_rate > 0 случайным образом генерирует
    ConnectionError при connect() и RuntimeError при read()/execute().
    Это полезно для тестирования отказоустойчивости вашего кода.
    """
    logger.info("=== Демонстрация: симуляция сбоев ===")

    # failure_rate=0.3 — 30% операций завершатся ошибкой
    driver = MockHardwareDriver(
        latency_range=(0.01, 0.02),
        failure_rate=0.3,
        initial_state={"sensor": 42},
    )

    await driver.connect()
    successes = 0
    failures = 0

    for i in range(10):
        try:
            val = await driver.read("sensor")
            successes += 1
            logger.info(f"  Попытка {i + 1}: успешно, sensor={val}")
        except RuntimeError as e:
            failures += 1
            logger.info(f"  Попытка {i + 1}: сбой — {e}")

    await driver.disconnect()
    logger.info(f"Итого: успехов={successes}, сбоев={failures}\n")


# =====================================================================
# Основная демонстрация: устройство + драйвер + MQTT
# =====================================================================

async def main():
    """
    Полный демо-цикл:
    1. Прямое использование драйвера (без MQTT)
    2. Симуляция сбоев
    3. Устройство с драйвером через KamioApp и MQTT
    """
    # --- Часть 1: прямой вызов драйвера ---
    await demo_standalone_driver()

    # --- Часть 2: симуляция сбоев ---
    await demo_failure_simulation()

    # --- Часть 3: устройство с драйвером через KamioApp ---
    logger.info("=== Демонстрация: устройство + драйвер + MQTT ===")

    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="drivers_demo")

    # Создаём mock-драйвер для климат-контроллера.
    # initial_state задаёт значения, которые драйвер будет возвращать
    # при read() для полей telemetry.
    climate_driver = MockHardwareDriver(
        latency_range=(0.01, 0.05),
        failure_rate=0.0,
        initial_state={
            "temperature": 22.3,
            "humidity": 48.0,
        },
    )

    # Регистрируем устройство, передавая драйвер в конструктор.
    # KamioApp вызовет driver.connect() в on_init() автоматически.
    climate = await app.add_device(
        "climate_1",
        ClimateController,
        driver=climate_driver,
    )

    # Запускаем приложение (подключение к MQTT-брокеру)
    await app.start()

    # --- Чтение через драйвер (вызывается автоматически для telemetry) ---
    # Telemetry-поля публикуются автоматически с заданной частотой (freq="5s").
    # Но мы также можем прочитать значение напрямую:
    temp_raw = await climate_driver.read("temperature")
    logger.info(f"Драйвер вернул temperature: {temp_raw}")

    # --- Запись состояния через handle_state (вызывает driver.execute) ---
    # handle_state проверяет writable-поля, валидирует значения,
    # затем вызывает driver.execute("set_<field>", {"value": ...}).
    # MockHardwareDriver распознаёт префикс "set_" и обновляет свой state.
    applied = await climate.handle_state({"relay_on": True, "fan_speed": 75})
    logger.info(f"handle_state применил: {applied}")
    logger.info(f"Текущее состояние: relay_on={climate.relay_on}, fan_speed={climate.fan_speed}")

    # --- Вызов команды через handle_command ---
    # handle_command сначала пытается выполнить команду через драйвер
    # (driver.execute(method_name, params)), затем через @command-метод.
    result = await climate.handle_command("emergency_stop", {})
    logger.info(f"Команда emergency_stop: {result}")

    # --- Прямой вызов driver.execute() ---
    # Можно вызывать драйвер напрямую из любого места кода:
    exec_result = await climate_driver.execute("set_fan_speed", {"value": 50})
    logger.info(f"Прямой driver.execute('set_fan_speed'): {exec_result}")

    # Ждём немного, чтобы увидеть публикацию телеметрии в MQTT
    logger.info("Ожидание 6 секунд для публикации телеметрии...")
    await asyncio.sleep(6)

    # --- Останавливаем приложение ---
    await app.stop()
    logger.info("Демонстрация завершена")


# =====================================================================
# Демонстрация: driver.read() через устройство (handle_state → driver.read)
# =====================================================================

async def demo_driver_read_via_device():
    """
    Показывает, что handle_state() вызывает driver.execute("set_<field>", ...),
    а TelemetryMixin автоматически вызывает driver.read() для телеметрии.

    Когда устройство имеет драйвер, изменение writable state-поля через
    handle_state() приводит к вызову driver.execute("set_<field>", {"value": ...}).
    Если драйвер не реализует команду (NotImplementedError), изменение
    применяется только в памяти устройства.
    """
    logger.info("=== Демонстрация: driver.read() через устройство ===")

    driver = MockHardwareDriver(
        latency_range=(0.01, 0.03),
        failure_rate=0.0,
        initial_state={"temperature": 21.0, "humidity": 55.0, "relay_on": False},
    )

    # Создаём устройство без KamioApp (для демонстрации прямого вызова)
    device = ClimateController(driver=driver)

    # Подключаем драйвер вручную (обычно это делает KamioApp в on_init)
    await driver.connect()
    logger.info(f"Драйвер подключён: connected={driver.connected}")

    # handle_state вызывает driver.execute("set_relay_on", {"value": True})
    # MockHardwareDriver распознаёт префикс "set_" и обновляет свой state dict.
    # После успешного выполнения драйвером, значение применяется в памяти устройства.
    applied = await device.handle_state({"relay_on": True, "fan_speed": 60})
    logger.info(f"handle_state применил: {applied}")
    logger.info(f"Драйвер state dict: {driver.state}")
    logger.info(f"Устройство: relay_on={device.relay_on}, fan_speed={device.fan_speed}")

    # Проверяем, что драйвер действительно обновил своё состояние
    relay_val = await driver.read("relay_on")
    logger.info(f"driver.read('relay_on') после set_relay_on=True: {relay_val}")

    await driver.disconnect()
    logger.info("Демонстрация driver.read() через устройство завершена\n")


# =====================================================================
# Демонстрация: driver.execute() через handle_command
# =====================================================================

async def demo_driver_execute_via_handle_command():
    """
    Показывает маршрутизацию команд через handle_command к драйверу.

    handle_command(method_name, params) сначала пытается выполнить команду
    через driver.execute(method_name, params). Если драйвер не реализует
    команду (NotImplementedError), выполняется @command-метод на устройстве.
    """
    logger.info("=== Демонстрация: driver.execute() через handle_command ===")

    driver = MockHardwareDriver(
        latency_range=(0.01, 0.03),
        failure_rate=0.0,
        initial_state={"temperature": 20.0},
    )
    device = ClimateController(driver=driver)
    await driver.connect()

    # handle_command("set_fan_speed", {"value": 80})
    # Сначала драйвер получает "set_fan_speed" → обновляет state dict.
    # MockHardwareDriver распознаёт "set_" и сохраняет значение.
    result = await device.handle_command("set_fan_speed", {"value": 80})
    logger.info(f"handle_command('set_fan_speed', {{'value': 80}}): {result}")
    logger.info(f"Драйвер state: fan_speed={driver.state.get('fan_speed')}")

    # handle_command("emergency_stop", {})
    # Драйвер не знает команду "emergency_stop" → NotImplementedError.
    # Но MockHardwareDriver не вызывает NotImplementedError — он возвращает
    # mock_success. В реальном драйвере, если метод не реализован,
    # будет NotImplementedError и фреймворк вызовет @command-метод.
    # Здесь emergency_stop — это @command-метод, который вызывается после драйвера.
    result = await device.handle_command("emergency_stop", {})
    logger.info(f"handle_command('emergency_stop'): {result}")
    logger.info(f"Устройство после emergency_stop: relay_on={device.relay_on}, fan_speed={device.fan_speed}")

    await driver.disconnect()
    logger.info("Демонстрация driver.execute() через handle_command завершена\n")


# =====================================================================
# Демонстрация: async context manager (async with)
# =====================================================================

async def demo_async_context_manager():
    """
    Показывает использование драйвера как асинхронного контекстного менеджера.

    BaseDriver реализует __aenter__ (вызывает connect()) и __aexit__
    (вызывает disconnect()). Это гарантирует освобождение ресурсов
    даже при возникновении исключений внутри блока.
    """
    logger.info("=== Демонстрация: async context manager (async with) ===")

    driver = MockHardwareDriver(
        latency_range=(0.01, 0.03),
        failure_rate=0.0,
        initial_state={"temperature": 25.0, "humidity": 40.0},
    )

    # async with автоматически вызывает connect() при входе
    # и disconnect() при выходе (даже если произошло исключение)
    async with driver:
        logger.info(f"Внутри async with: connected={driver.connected}")
        temp = await driver.read("temperature")
        logger.info(f"Чтение temperature внутри контекста: {temp}")

        # Выполнение команды внутри контекста
        result = await driver.execute("set_temperature", {"value": 26.5})
        logger.info(f"execute('set_temperature') внутри контекста: {result}")

    # После выхода из блока disconnect() уже вызван
    logger.info(f"После async with: connected={driver.connected}")

    # Демонстрация: исключение внутри блока не предотвращает disconnect()
    driver2 = MockHardwareDriver(
        latency_range=(0.01, 0.03),
        failure_rate=0.0,
        initial_state={"val": 1},
    )
    try:
        async with driver2:
            logger.info(f"driver2 внутри контекста: connected={driver2.connected}")
            raise ValueError("Тестовое исключение внутри async with")
    except ValueError as e:
        logger.info(f"Исключение перехвачено: {e}")
    logger.info(f"driver2 после исключения: connected={driver2.connected} (disconnect всё равно вызван)")

    logger.info("Демонстрация async context manager завершена\n")


# =====================================================================
# Демонстрация: обработка NotImplementedError
# =====================================================================

# Драйвер, который не реализует некоторые команды (имитирует partial-драйвер)
from kamio.drivers.base import BaseDriver as _BaseDriver


class PartialDriver(_BaseDriver):
    """Драйвер, который реализует только connect/disconnect, но не execute/read.

    Имитирует ситуацию, когда драйвер оборудования не поддерживает
    все команды. При вызове execute() или read() вызывает NotImplementedError.
    Фреймворк Kamio корректно обрабатывает это: handle_state и handle_command
    перехватывают NotImplementedError и применяют изменения в памяти устройства.
    """

    def __init__(self):
        super().__init__()
        self.connected = False

    async def connect(self):
        self.connected = True
        self.logger.info("PartialDriver подключён (без реального оборудования)")

    async def disconnect(self):
        self.connected = False
        self.logger.info("PartialDriver отключён")

    async def execute(self, command_name: str, params: dict) -> Any:
        # Драйвер не поддерживает никаких команд — имитируем NotImplementedError
        raise NotImplementedError(f"PartialDriver не реализует execute('{command_name}')")

    async def read(self, field_name: str, params=None) -> Any:
        raise NotImplementedError(f"PartialDriver не реализует read('{field_name}')")


async def demo_not_implemented_error():
    """
    Показывает поведение при NotImplementedError от драйвера.

    Когда driver.execute() вызывает NotImplementedError:
    - handle_state() перехватывает исключение и применяет значение в памяти
    - handle_command() перехватывает исключение и вызывает @command-метод

    Это позволяет использовать драйверы, которые реализуют только часть
    функциональности (например, только чтение, но не запись).
    """
    logger.info("=== Демонстрация: обработка NotImplementedError ===")

    driver = PartialDriver()
    device = ClimateController(driver=driver)
    await driver.connect()

    # handle_state вызывает driver.execute("set_relay_on", {"value": True})
    # Драйвер вызывает NotImplementedError → handle_state перехватывает
    # и применяет значение в памяти устройства (fallback).
    logger.info("Вызываем handle_state с PartialDriver (NotImplementedError ожидаем)...")
    applied = await device.handle_state({"relay_on": True, "fan_speed": 50})
    logger.info(f"handle_state применил (fallback в память): {applied}")
    logger.info(f"Устройство: relay_on={device.relay_on}, fan_speed={device.fan_speed}")

    # handle_command("emergency_stop", {}) → driver.execute вызывает NotImplementedError
    # → handle_command вызывает @command-метод emergency_stop на устройстве
    logger.info("Вызываем handle_command('emergency_stop') с PartialDriver...")
    result = await device.handle_command("emergency_stop", {})
    logger.info(f"handle_command результат (через @command-метод): {result}")
    logger.info(f"Устройство после emergency_stop: relay_on={device.relay_on}, fan_speed={device.fan_speed}")

    await driver.disconnect()
    logger.info("Демонстрация NotImplementedError завершена\n")


# =====================================================================
# Демонстрация: driver disconnect с ошибкой
# =====================================================================


class FaultyDisconnectDriver(_BaseDriver):
    """Драйвер, у которого disconnect() всегда вызывает ошибку.

    Имитирует ситуацию, когда оборудование не отвечает на команду
    отключения (например, сетевое соединение уже разорвано).
    """

    def __init__(self):
        super().__init__()
        self.connected = False

    async def connect(self):
        self.connected = True
        self.logger.info("FaultyDisconnectDriver подключён")

    async def disconnect(self):
        # Имитируем ошибку при отключении (например, соединение уже разорвано)
        self.connected = False
        raise ConnectionError("Оборудование не отвечает на disconnect")

    async def execute(self, command_name: str, params: dict) -> Any:
        return {"status": "ok"}

    async def read(self, field_name: str, params=None) -> Any:
        return None


async def demo_driver_disconnect_error():
    """
    Показывает обработку ошибок при disconnect() драйвера.

    Если driver.disconnect() вызывает исключение, оно перехватывается
    в on_stop() и логируется. Устройство корректно останавливается
    даже если драйвер не смог отключиться чисто.
    """
    logger.info("=== Демонстрация: driver disconnect с ошибкой ===")

    driver = FaultyDisconnectDriver()
    device = ClimateController(driver=driver)

    # Подключаем драйвер
    await driver.connect()
    logger.info(f"Драйвер подключён: connected={driver.connected}")

    # Пытаемся отключить — драйвер вызовет ConnectionError
    logger.info("Пытаемся disconnect (ожидается ConnectionError)...")
    try:
        await driver.disconnect()
    except ConnectionError as e:
        logger.warning(f"disconnect вызвал ошибку (ожидаемо): {e}")

    # Даже при ошибке disconnect, connected сбрасывается в False
    logger.info(f"После disconnect с ошибкой: connected={driver.connected}")

    # Демонстрация: async with также перехватывает ошибку disconnect
    driver2 = FaultyDisconnectDriver()
    logger.info("Используем async with с FaultyDisconnectDriver...")
    try:
        async with driver2:
            logger.info(f"Внутри контекста: connected={driver2.connected}")
        # __aexit__ вызовет disconnect() → ConnectionError
    except ConnectionError as e:
        logger.warning(f"async with __aexit__ вызвал ошибку: {e}")

    logger.info("Демонстрация driver disconnect с ошибкой завершена\n")


# =====================================================================
# Демонстрация: MockHardwareDriver state dict (set_ prefix commands)
# =====================================================================

async def demo_mock_state_dict():
    """
    Показывает, как MockHardwareDriver управляет своим state dict
    через команды с префиксом "set_".

    MockHardwareDriver.execute() проверяет префикс "set_" в имени команды:
    - "set_<field>" → обновляет self.state[field] = params["value"]
    - другие команды → возвращает {"status": "ok", "result": "mock_success"}

    Это позволяет использовать MockHardwareDriver для тестирования
    логики handle_state() и handle_command() без реального оборудования.
    """
    logger.info("=== Демонстрация: MockHardwareDriver state dict ===")

    driver = MockHardwareDriver(
        latency_range=(0.0, 0.01),
        failure_rate=0.0,
        initial_state={
            "temperature": 22.0,
            "humidity": 45.0,
            "relay_on": False,
            "fan_speed": 0,
        },
    )

    await driver.connect()
    logger.info(f"Начальный state dict: {driver.state}")

    # Команда "set_relay_on" → обновляет state["relay_on"]
    result = await driver.execute("set_relay_on", {"value": True})
    logger.info(f"execute('set_relay_on', {{'value': True}}): {result}")
    logger.info(f"state['relay_on'] = {driver.state['relay_on']}")

    # Команда "set_fan_speed" → обновляет state["fan_speed"]
    result = await driver.execute("set_fan_speed", {"value": 75})
    logger.info(f"execute('set_fan_speed', {{'value': 75}}): {result}")
    logger.info(f"state['fan_speed'] = {driver.state['fan_speed']}")

    # Команда "set_temperature" → обновляет state["temperature"]
    result = await driver.execute("set_temperature", {"value": 23.5})
    logger.info(f"execute('set_temperature', {{'value': 23.5}}): {result}")
    logger.info(f"state['temperature'] = {driver.state['temperature']}")

    # Команда без префикса "set_" → возвращает mock_success
    result = await driver.execute("reboot", {})
    logger.info(f"execute('reboot', {{}}): {result} (без префикса set_)")

    # Чтение значений из state dict
    temp = await driver.read("temperature")
    logger.info(f"read('temperature'): {temp}")

    hum = await driver.read("humidity")
    logger.info(f"read('humidity'): {hum}")

    # Чтение несуществующего поля → None
    missing = await driver.read("nonexistent")
    logger.info(f"read('nonexistent'): {missing}")

    # Итоговый state dict после всех операций
    logger.info(f"Итоговый state dict: {driver.state}")

    await driver.disconnect()
    logger.info("Демонстрация MockHardwareDriver state dict завершена\n")


# =====================================================================
# Обновлённый главный цикл (вызывает все демонстрации)
# =====================================================================

async def main():
    # --- Автономные демонстрации (без MQTT-брокера) ---
    await demo_driver_read_via_device()
    await demo_driver_execute_via_handle_command()
    await demo_async_context_manager()
    await demo_not_implemented_error()
    await demo_driver_disconnect_error()
    await demo_mock_state_dict()

    # --- Демонстрация с MQTT-брокером (оригинальный код) ---
    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="drivers_demo")

    # Создаём mock-драйвер для климат-контроллера.
    # initial_state задаёт значения, которые драйвер будет возвращать
    # при read() для полей telemetry.
    climate_driver = MockHardwareDriver(
        latency_range=(0.01, 0.05),
        failure_rate=0.0,
        initial_state={
            "temperature": 22.3,
            "humidity": 48.0,
        },
    )

    # Регистрируем устройство, передавая драйвер в конструктор.
    # KamioApp вызовет driver.connect() в on_init() автоматически.
    climate = await app.add_device(
        "climate_1",
        ClimateController,
        driver=climate_driver,
    )

    # Запускаем приложение (подключение к MQTT-брокеру)
    await app.start()

    # --- Чтение через драйвер (вызывается автоматически для telemetry) ---
    # Telemetry-поля публикуются автоматически с заданной частотой (freq="5s").
    # Но мы также можем прочитать значение напрямую:
    temp_raw = await climate_driver.read("temperature")
    logger.info(f"Драйвер вернул temperature: {temp_raw}")

    # --- Запись состояния через handle_state (вызывает driver.execute) ---
    # handle_state проверяет writable-поля, валидирует значения,
    # затем вызывает driver.execute("set_<field>", {"value": ...}).
    # MockHardwareDriver распознаёт префикс "set_" и обновляет свой state.
    applied = await climate.handle_state({"relay_on": True, "fan_speed": 75})
    logger.info(f"handle_state применил: {applied}")
    logger.info(f"Текущее состояние: relay_on={climate.relay_on}, fan_speed={climate.fan_speed}")

    # --- Вызов команды через handle_command ---
    # handle_command сначала пытается выполнить команду через драйвер
    # (driver.execute(method_name, params)), затем через @command-метод.
    result = await climate.handle_command("emergency_stop", {})
    logger.info(f"Команда emergency_stop: {result}")

    # --- Прямой вызов driver.execute() ---
    # Можно вызывать драйвер напрямую из любого места кода:
    exec_result = await climate_driver.execute("set_fan_speed", {"value": 50})
    logger.info(f"Прямой driver.execute('set_fan_speed'): {exec_result}")

    # Ждём немного, чтобы увидеть публикацию телеметрии в MQTT
    logger.info("Ожидание 6 секунд для публикации телеметрии...")
    await asyncio.sleep(6)

    # --- Останавливаем приложение ---
    await app.stop()
    logger.info("Демонстрация завершена")


if __name__ == "__main__":
    asyncio.run(main())
