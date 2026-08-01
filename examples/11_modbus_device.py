"""
11 — Modbus TCP Device (устройство на Modbus TCP)
==================================================

Полный пример Modbus TCP устройства через Kamio:
    - ModbusTCPDriver(host, port, unit_id) — создание драйвера
    - Поля state, отображаемые на Modbus-регистры
    - Поля telemetry, читаемые из holding registers
    - Команды, записывающие в coils и регистры
    - read_holding_registers, read_coils через driver.read()
    - write_coil, write_register, write_multiple_registers через driver.execute()
    - Обработка ошибок подключения

Запуск::
    python examples/11_modbus_device.py

Предварительно:
    1. Запустите MQTT-брокер на localhost:1883
    2. Запустите Modbus TCP симулятор на localhost:502
       Например, ``pip install pymodbus`` и::
           python -m pymodbus.simulator --type tcp --port 502

      Или используйте любой Modbus TCP slave.
      Если Modbus-устройства нет, пример покажет обработку ошибок.

Структура регистров (пример):
    Holding Registers (HR):
        0:   temperature     (float, 2 регистра, big-endian)
        2:   humidity        (float, 2 регистра)
        4:   power_setpoint  (int16)
    Coils:
        0:   pump_on         (bool)
        1:   alarm_reset     (bool)
"""
from __future__ import annotations

import asyncio
import logging
import struct
from typing import Any, Dict, Optional

from kamio import KamioApp, Device, command, state, telemetry
from kamio.drivers.modbus import ModbusTCPDriver

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("modbus_demo")


# =====================================================================
# Устройство: промышленный контроллер на Modbus TCP
# =====================================================================

class IndustrialController(Device):
    """Промышленный контроллер с датчиками и управляемыми выходами.

    Поля state:
        - pump_on (bool, writable)    — отображается на coil 0
        - setpoint (int, writable)    — отображается на HR 4
        - mode (str, writable)        — локальное поле (не в Modbus)

    Поля telemetry:
        - temperature (float)  — читается из HR 0-1 (2 регистра)
        - humidity (float)     — читается из HR 2-3 (2 регистра)
        - pump_status (bool)   — читается из coil 1

    Команды:
        - reset_alarm  — запись в coil 1 (write_coil)
        - write_batch  — запись нескольких регистров (write_multiple_registers)
    """

    # --- State поля (управляемые через MQTT и Modbus) ---
    pump_on: bool = state(
        default=False,
        writable=True,
        description="Состояние насоса (coil 0)",
    )

    setpoint: int = state(
        default=100,
        min=0,
        max=1000,
        writable=True,
        description="Уставка мощности (HR 4)",
    )

    mode: str = state(
        default="auto",
        choices=("auto", "manual", "maintenance"),
        writable=True,
        description="Режим работы (локальное поле, не в Modbus)",
    )

    # --- Telemetry поля (читаются из Modbus периодически) ---
    temperature: float = telemetry(
        default=0.0,
        unit="°C",
        freq="5s",
        min=-20.0,
        max=150.0,
        description="Температура (HR 0-1, 2 регистра, float32)",
    )

    humidity: float = telemetry(
        default=0.0,
        unit="%",
        freq="5s",
        min=0.0,
        max=100.0,
        description="Влажность (HR 2-3, 2 регистра, float32)",
    )

    pump_status: bool = telemetry(
        default=False,
        freq="5s",
        description="Статус насоса (coil 1)",
    )

    # --- Адреса Modbus-регистров ---
    # Храним карту адресов для удобства
    MODBUS_MAP = {
        "temperature": {"type": "holding", "address": 0, "count": 2},
        "humidity": {"type": "holding", "address": 2, "count": 2},
        "pump_status": {"type": "coil", "address": 1, "count": 1},
    }

    # ------------------------------------------------------------------
    # Переопределение handle_telemetry_update для чтения из Modbus
    # ------------------------------------------------------------------

    async def handle_telemetry_update(self, field_names: list[str]) -> Optional[dict[str, Any]]:
        """Кастомный сбор телеметрии из Modbus-регистров.

        По умолчанию handle_telemetry_update читает значения через
        read_telemetry_value(), который вызывает driver.read().
        Однако для float-значений, занимающих 2 регистра, нужен
        дополнительный декодинг (big-endian 32-bit float).

        Здесь мы:
        1. Группируем чтение holding registers для эффективности
        2. Декодируем float32 из пары регистров
        3. Читаем coils отдельно
        """
        if not self.driver:
            return None

        data: dict[str, Any] = {}

        for name in field_names:
            reg_info = self.MODBUS_MAP.get(name)
            if reg_info is None:
                # Если поля нет в карте — пробуем стандартный read
                val = await self.read_telemetry_value(name)
                if val is not None:
                    data[name] = val
                continue

            try:
                # Чтение через driver.read()
                # params передаёт тип (coil/holding), адрес и количество
                result = await self.driver.read(name, params=reg_info)

                if not isinstance(result, dict) or result.get("status") != "ok":
                    logger.warning(f"Modbus read для '{name}' вернул: {result}")
                    continue

                raw = result.get("data")

                # Декодирование значений
                if name == "temperature" and isinstance(raw, list) and len(raw) >= 2:
                    # float32 из двух holding registers (big-endian)
                    data[name] = self._decode_float32(raw[0], raw[1])
                    self.temperature = data[name]

                elif name == "humidity" and isinstance(raw, list) and len(raw) >= 2:
                    data[name] = self._decode_float32(raw[0], raw[1])
                    self.humidity = data[name]

                elif name == "pump_status":
                    # coil возвращает bool или list[bool]
                    if isinstance(raw, bool):
                        data[name] = raw
                    elif isinstance(raw, list) and raw:
                        data[name] = raw[0]
                    self.pump_status = data[name] if name in data else self.pump_status

            except Exception as e:
                logger.error(f"Ошибка чтения Modbus для '{name}': {e}")

        return data if data else None

    @staticmethod
    def _decode_float32(reg_high: int, reg_low: int) -> float:
        """Декодировать float32 из двух 16-битных регистров (big-endian).

        Modbus хранит 32-битные float как пару holding registers.
        Порядок: старший регистр первым (big-endian word order).
        """
        # Упаковываем два 16-битных значения в 32-битный float
        packed = struct.pack(">HH", reg_high & 0xFFFF, reg_low & 0xFFFF)
        return struct.unpack(">f", packed)[0]

    # ------------------------------------------------------------------
    # Команды (запись в Modbus)
    # ------------------------------------------------------------------

    @command
    async def reset_alarm(self):
        """Сброс тревоги — запись True в coil 1.

        Использует driver.execute() с командой "write_coil".
        params:
            - address: адрес coil (int)
            - value:   True/False (bool)
        """
        if not self.driver:
            raise RuntimeError("Драйвер не подключён")

        # driver.execute() вызывает _write_single_coil внутри
        result = await self.driver.execute("write_coil", {
            "address": 1,
            "value": True,
        })
        logger.info(f"Alarm reset: {result}")
        return result

    @command
    async def write_batch(self, values: list):
        """Запись нескольких регистров подряд (write_multiple_registers).

        Использует driver.execute() с командой "write_registers".
        params:
            - address: начальный адрес (int)
            - values:  список значений (list[int])
        """
        if not self.driver:
            raise RuntimeError("Драйвер не подключён")

        # Записываем начиная с регистра 10
        result = await self.driver.execute("write_registers", {
            "address": 10,
            "values": [int(v) for v in values],
        })
        logger.info(f"Batch write: {result}")
        return result

    @command
    async def set_pump(self, value: bool):
        """Включить/выключить насос — запись в coil 0.

        Также обновляет локальное state-поле pump_on.
        """
        if not self.driver:
            raise RuntimeError("Драйвер не подключён")

        # Запись в coil через driver.execute()
        result = await self.driver.execute("write_coil", {
            "address": 0,
            "value": bool(value),
        })

        # Обновляем локальное состояние
        # Используем object.__setattr__ чтобы избежать повторной публикации,
        # т.к. handle_state уже опубликует изменение.
        # Но в данном случае мы хотим публикацию, поэтому просто присваиваем:
        self.pump_on = bool(value)
        logger.info(f"Pump set to {value}: {result}")
        return result

    @command
    async def write_setpoint(self, value: int):
        """Записать уставку в holding register 4.

        Использует driver.execute() с командой "write_register".
        params:
            - address: адрес регистра (int)
            - value:   значение (int)
        """
        if not self.driver:
            raise RuntimeError("Драйвер не подключён")

        result = await self.driver.execute("write_register", {
            "address": 4,
            "value": int(value),
        })
        self.setpoint = int(value)
        logger.info(f"Setpoint written: {result}")
        return result

    # ------------------------------------------------------------------
    # Обработка ошибок подключения
    # ------------------------------------------------------------------

    async def on_init(self, **kwargs):
        """Инициализация устройства.

        on_init вызывается фреймворком после __init__, но до on_start.
        Если драйвер задан, on_init вызывает driver.connect().
        При ошибке подключения — исключение пробрасывается наверх,
        и устройство не регистрируется в приложении.

        Здесь мы добавляем логирование для наглядности.
        """
        if self.driver:
            try:
                await self.driver.connect()
                logger.info("Modbus драйвер успешно подключён")
            except Exception as e:
                logger.error(f"Ошибка подключения Modbus: {e}")
                # Перевыбираем исключение — on_init не должен глотать ошибки.
                # KamioApp.create_device обработает это и не зарегистрирует
                # устройство в сломанном состоянии.
                raise


# =====================================================================
# Подписчики на события для логирования
# =====================================================================

async def on_state_changed(data: Dict[str, Any]) -> None:
    """Логирование изменений состояния."""
    logger.info(
        f"[state] {data['device_id']}.{data['field']}: "
        f"{data['old_value']} -> {data['new_value']}"
    )


async def on_command_executed(data: Dict[str, Any]) -> None:
    """Логирование выполнения команд."""
    logger.info(
        f"[command] {data['device_id']}.{data['command']}("
        f"params={data['params']}) -> {data['result']}"
    )


# =====================================================================
# Главный цикл
# =====================================================================

async def main():
    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="modbus_demo")

    # Подписка на события
    app.subscribe_event("device_state_changed", on_state_changed)
    app.subscribe_event("device_command_executed", on_command_executed)

    # --- Создание Modbus TCP драйвера ---
    # host:    IP-адрес или hostname Modbus-устройства
    # port:    Modbus TCP порт (стандарт 502)
    # unit_id: Modbus slave address (обычно 1)
    # timeout: таймаут соединения/ответа в секундах
    modbus_driver = ModbusTCPDriver(
        host="localhost",
        port=502,
        unit_id=1,
        timeout=3.0,
        reconnect_attempts=1,
    )

    # --- Регистрация класса ---
    app.register(IndustrialController)

    # --- Запуск приложения ---
    await app.start()

    # --- Создание устройства с драйвером ---
    # Драйвер передаётся через kwargs и автоматически сохраняется в device.driver.
    # on_init вызовет driver.connect().
    logger.info("=== Создание Modbus устройства ===")

    try:
        controller = await app.add_device(
            "controller_1",
            IndustrialController,
            driver=modbus_driver,
        )
        logger.info("Modbus устройство успешно создано и подключено")

    except Exception as e:
        logger.error(f"Не удалось создать Modbus устройство: {e}")
        logger.info("Убедитесь, что Modbus TCP симулятор запущен на localhost:502")
        logger.info("Например: python -m pymodbus.simulator --type tcp --port 502")
        await app.stop()
        return

    # --- Демонстрация команд ---
    logger.info("=== Демонстрация команд Modbus ===")

    # 1. Включить насос (write_coil)
    try:
        result = await controller.handle_command("set_pump", {"value": True})
        logger.info(f"set_pump(True): {result}")
    except Exception as e:
        logger.error(f"Ошибка set_pump: {e}")

    await asyncio.sleep(1)

    # 2. Записать уставку (write_register)
    try:
        result = await controller.handle_command("write_setpoint", {"value": 250})
        logger.info(f"write_setpoint(250): {result}")
    except Exception as e:
        logger.error(f"Ошибка write_setpoint: {e}")

    await asyncio.sleep(1)

    # 3. Сброс тревоги (write_coil)
    try:
        result = await controller.handle_command("reset_alarm", {})
        logger.info(f"reset_alarm(): {result}")
    except Exception as e:
        logger.error(f"Ошибка reset_alarm: {e}")

    await asyncio.sleep(1)

    # 4. Пакетная запись регистров (write_multiple_registers)
    try:
        result = await controller.handle_command("write_batch", {"values": [10, 20, 30]})
        logger.info(f"write_batch([10,20,30]): {result}")
    except Exception as e:
        logger.error(f"Ошибка write_batch: {e}")

    # --- Демонстрация чтения через driver.read() напрямую ---
    logger.info("=== Прямое чтение Modbus через driver.read() ===")

    try:
        # Чтение holding registers (temperature)
        temp_result = await modbus_driver.read("temperature", params={
            "type": "holding",
            "address": 0,
            "count": 2,
        })
        logger.info(f"read HR 0-1 (temperature): {temp_result}")

        # Чтение coils (pump_status)
        coil_result = await modbus_driver.read("pump_status", params={
            "type": "coil",
            "address": 1,
            "count": 1,
        })
        logger.info(f"read coil 1 (pump_status): {coil_result}")

    except Exception as e:
        logger.error(f"Ошибка прямого чтения: {e}")

    # --- Ожидание автоматической телеметрии ---
    logger.info("=== Ожидание автоматической телеметрии (5s freq) ===")
    logger.info("Telemetry будет публиковаться каждые 5 секунд...")
    await asyncio.sleep(12)

    # --- Снимки состояния ---
    logger.info("=== Снимки устройства ===")
    logger.info(f"State snapshot: {controller.get_state_snapshot()}")
    logger.info(f"Telemetry snapshot: {controller.get_telemetry_snapshot()}")
    logger.info(f"Full snapshot: {controller.get_full_snapshot()}")

    # --- Демонстрация reinitialize (переподключение драйвера) ---
    logger.info("=== Демонстрация reinitialize() ===")
    try:
        await controller.reinitialize()
        logger.info("Устройство реинициализировано (драйвер переподключён)")
    except Exception as e:
        logger.error(f"Ошибка reinitialize: {e}")

    # --- Дополнительные демонстрации ---
    await demo_read_coils(modbus_driver)
    await demo_read_discrete_inputs(modbus_driver)
    await demo_write_multiple_registers(modbus_driver)
    await demo_reconnect(modbus_driver)
    await demo_transaction_id(modbus_driver)
    await demo_timeout_handling()

    logger.info("=== Завершение ===")
    await app.stop()


# =====================================================================
# Демонстрация: read_coils — чтение coils
# =====================================================================

async def demo_read_coils(modbus_driver):
    """Показывает чтение Modbus coils через driver.read().

    Coils — это однобитовые значения (bool), доступные для чтения и записи.
    Modbus function code 0x01 (Read Coils).

    driver.read() с params={"type": "coil", "address": N, "count": M}
    возвращает список bool при count > 1, или один bool при count == 1.
    """
    logger.info("=== Демонстрация: read_coils ===")

    try:
        # Чтение одного coil (address=0, count=1) → bool
        result = await modbus_driver.read("pump_on", params={
            "type": "coil",
            "address": 0,
            "count": 1,
        })
        logger.info(f"read coil 0 (单个): {result}")

        # Чтение нескольких coils (address=0, count=4) → list[bool]
        result = await modbus_driver.read("coils_batch", params={
            "type": "coil",
            "address": 0,
            "count": 4,
        })
        logger.info(f"read coils 0-3 (пакет): {result}")

    except Exception as e:
        logger.error(f"Ошибка read_coils: {e}")
        logger.info("  (убедитесь, что Modbus симулятор запущен)")


# =====================================================================
# Демонстрация: read_discrete_inputs — чтение discrete inputs
# =====================================================================

async def demo_read_discrete_inputs(modbus_driver):
    """Показывает чтение Modbus discrete inputs через driver.read().

    Discrete Inputs — однобитовые значения (bool), доступные только для чтения.
    Modbus function code 0x02 (Read Discrete Inputs).

    Аналогично coils, но предназначены для входных сигналов (датчики,
    кнопки), которые нельзя перезаписать.
    """
    logger.info("\n=== Демонстрация: read_discrete_inputs ===")

    try:
        # Чтение одного discrete input
        result = await modbus_driver.read("input_0", params={
            "type": "discrete",
            "address": 0,
            "count": 1,
        })
        logger.info(f"read discrete input 0: {result}")

        # Чтение нескольких discrete inputs
        result = await modbus_driver.read("inputs_batch", params={
            "type": "discrete",
            "address": 0,
            "count": 8,
        })
        logger.info(f"read discrete inputs 0-7: {result}")

    except Exception as e:
        logger.error(f"Ошибка read_discrete_inputs: {e}")
        logger.info("  (убедитесь, что Modbus симулятор запущен)")


# =====================================================================
# Демонстрация: write_multiple_registers — запись нескольких регистров
# =====================================================================

async def demo_write_multiple_registers(modbus_driver):
    """Показывает запись нескольких holding registers одной командой.

    Modbus function code 0x10 (Write Multiple Registers).
    Максимум 123 регистра за один запрос (по спецификации Modbus).

    driver.execute("write_registers", {"address": N, "values": [v1, v2, ...]})
    записывает список значений начиная с указанного адреса.
    """
    logger.info("\n=== Демонстрация: write_multiple_registers ===")

    try:
        # Запись 3 регистров начиная с адреса 10
        result = await modbus_driver.execute("write_registers", {
            "address": 10,
            "values": [100, 200, 300],
        })
        logger.info(f"write_registers(10, [100, 200, 300]): {result}")

        # Чтение записанных регистров для проверки
        read_result = await modbus_driver.read("verify", params={
            "type": "holding",
            "address": 10,
            "count": 3,
        })
        logger.info(f"verify read HR 10-12: {read_result}")

        # Попытка записи слишком большого количества регистров
        # (> 123) — ожидается ValueError
        logger.info("Попытка записи 200 регистров (ожидается ValueError)...")
        try:
            await modbus_driver.execute("write_registers", {
                "address": 0,
                "values": list(range(200)),
            })
        except ValueError as e:
            logger.info(f"ValueError перехвачен: {e}")

    except Exception as e:
        logger.error(f"Ошибка write_multiple_registers: {e}")
        logger.info("  (убедитесь, что Modbus симулятор запущен)")


# =====================================================================
# Демонстрация: reconnect при обрыве
# =====================================================================

async def demo_reconnect(modbus_driver):
    """Показывает автоматическое переподключение при обрыве соединения.

    ModbusTCPDriver имеет параметр reconnect_attempts (по умолчанию 1).
    При обрыве соединения (ConnectionResetError, BrokenPipeError, и т.д.)
    драйвер пытается переподключиться до reconnect_attempts раз.

    _ensure_connected() проверяет состояние writer/reader и вызывает
    _reconnect() при необходимости.
    """
    logger.info("\n=== Демонстрация: reconnect при обрыве ===")

    logger.info(f"reconnect_attempts: {modbus_driver.reconnect_attempts}")
    logger.info(f"host: {modbus_driver.host}:{modbus_driver.port}")
    logger.info(f"timeout: {modbus_driver.timeout}s")

    # Проверяем текущее состояние соединения
    writer = modbus_driver._writer
    is_closing = writer.is_closing() if writer else True
    logger.info(f"Текущее состояние: writer={'None' if not writer else 'set'}, is_closing={is_closing}")

    # Имитация обрыва — закрываем writer
    if writer:
        logger.info("Имитация обрыва соединения (закрываем writer)...")
        try:
            writer.close()
        except Exception:
            pass

    # Следующая операция вызовет _ensure_connected → _reconnect
    try:
        result = await modbus_driver.read("test_after_reconnect", params={
            "type": "holding",
            "address": 0,
            "count": 1,
        })
        logger.info(f"Чтение после обрыва (reconnect сработал): {result}")
    except Exception as e:
        logger.error(f"Чтение после обрыва не удалось: {e}")
        logger.info("  (если симулятор не запущен, reconnect не поможет)")


# =====================================================================
# Демонстрация: transaction ID — отслеживание транзакций
# =====================================================================

async def demo_transaction_id(modbus_driver):
    """Показывает отслеживание Modbus transaction ID.

    Каждый Modbus TCP запрос содержит transaction ID в MBAP header.
    Драйвер инкрементирует _transaction для каждого запроса (0..65535,
    затем обнуляется). Ответ должен содержать тот же transaction ID.

    Несовпадение transaction ID вызывает RuntimeError.
    """
    logger.info("\n=== Демонстрация: transaction ID ===")

    # Текущий transaction ID (до запроса)
    initial_tid = modbus_driver._transaction
    logger.info(f"Начальный transaction ID: {initial_tid}")

    # Выполняем несколько запросов — TID инкрементируется
    for i in range(3):
        try:
            await modbus_driver.read(f"tid_test_{i}", params={
                "type": "holding",
                "address": 0,
                "count": 1,
            })
            current_tid = modbus_driver._transaction
            logger.info(f"  Запрос {i+1}: transaction ID после = {current_tid}")
        except Exception as e:
            logger.error(f"  Запрос {i+1} не удался: {e}")
            break

    # TID обнуляется при переполнении (65535 → 0)
    final_tid = modbus_driver._transaction
    logger.info(f"Итоговый transaction ID: {final_tid}")
    logger.info(f"  (инкрементируется с каждым запросом, обнуляется на 65536)")


# =====================================================================
# Демонстрация: timeout handling — обработка таймаута
# =====================================================================

async def demo_timeout_handling():
    """Показывает обработку таймаута при подключении/чтении.

    ModbusTCPDriver использует asyncio.wait_for() с указанным timeout
    для всех операций (connect, read, write). При таймауте вызывается
    asyncio.TimeoutError.

    Короткий timeout → быстрая обработка ошибок, но риск ложных срабатываний.
    Длинный timeout → надёжнее, но медленнее при реальных проблемах.
    """
    logger.info("\n=== Демонстрация: timeout handling ===")

    # Создаём драйвер с очень коротким timeout
    fast_driver = ModbusTCPDriver(
        host="192.0.2.1",  # TEST-NET IP (не отвечает)
        port=502,
        unit_id=1,
        timeout=0.5,  # 500 мс — очень короткий
        reconnect_attempts=0,  # без попыток переподключения
    )

    logger.info(f"Драйвер: host={fast_driver.host}, timeout={fast_driver.timeout}s")
    logger.info("Пытаемся подключиться к несуществующему хосту...")

    try:
        await fast_driver.connect()
    except asyncio.TimeoutError:
        logger.info(f"TimeoutError перехвачен (connect timeout={fast_driver.timeout}s)")
    except Exception as e:
        logger.info(f"Другая ошибка подключения: {type(e).__name__}: {e}")

    # Также таймаут может произойти при чтении, если устройство перестало отвечать
    logger.info("\nТаймаут также применяется к read/write операциям:")
    logger.info("  - asyncio.wait_for(reader.readexactly(7), timeout=self.timeout)")
    logger.info("  - При таймауте → asyncio.TimeoutError")
    logger.info("  - При обрыве соединения → asyncio.IncompleteReadError")

    # Драйвер с нормальным timeout для сравнения
    normal_driver = ModbusTCPDriver(
        host="localhost",
        port=502,
        timeout=3.0,
        reconnect_attempts=2,
    )
    logger.info(f"\nДрайвер с нормальным timeout: {normal_driver.timeout}s, reconnect={normal_driver.reconnect_attempts}")


if __name__ == "__main__":
    asyncio.run(main())
