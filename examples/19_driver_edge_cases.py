"""
19 — Driver Edge Cases (краевые случаи драйверов)
==================================================

ГЛУБОКОЕ ПОГРУЖЕНИЕ для разработчиков фреймворка.

Каждый драйвер в kamio имеет свои скрытые подводные камни.  Этот файл
демонстрирует реальные краевые случаи, которые могут привести к зависанию,
потере данных или тихим ошибкам в production.

ПОДВОХИ И КРАЕВЫЕ СЛУЧАИ:

    MockHardwareDriver:
      1. failure_rate — вероятность сбоя КАЖДОЙ операции (connect/read/execute).
         При failure_rate=1.0 ВСЕГДА падает, даже disconnect не спасёт.
      2. connected=False по умолчанию — read/execute требуют connect() сначала.
      3. read() возвращает RAW значение (self.state.get(field_name)), а НЕ dict.
         В отличие от Modbus/Telnet/Serial, которые возвращают {"status": "ok", ...}.

    ModbusTCPDriver:
      4. reconnect_attempts=max(0, int(reconnect_attempts)) — отрицательные
         значения молча обрезаются до 0 (одна попытка, без ретраев).
      5. _transaction обнуляется на 0xFFFF: (self._transaction + 1) & 0xFFFF.
         После 65535-й транзакции ID сбрасывается в 0 — коллизия возможна.
      6. _close_writer глотает ВСЕ исключения (except Exception: pass).
         Ошибка закрытия сокета невидима — может быть утечка fd.
      7. _reconnect() не имеет backoff — мгновенный retry после сбоя.
         При сетевых проблемах это создаёт шквал попыток.

    TelnetDriver:
      8. assert self.writer is not None в execute() — assert удаляется при
         запуске с python -O.  В production writer может быть None после
         неудачного _ensure_connected, и assert не сработает.
      9. timeout при чтении возвращает {"status": "ok", "response": ""}.
         Пустой ответ неотличим от реального пустого ответа устройства.

    SerialDriver:
     10. asyncio.to_thread БЕЗ asyncio.wait_for — если pyserial зависнет,
         to_thread блокирует поток навсегда.  Event loop не блокируется,
         но операция никогда не завершится.
     11. .decode(errors="replace") — тихо заменяет некорректные байты на U+FFFD.
         Данные искажаются без уведомления.

    BaseDriver:
     12. __aexit__ НЕ подавляет исключения (нет return True).
         Если disconnect() падает, исключение вылетает из async with.
     13. disconnect() во ВСЕХ драйверах глотает или логирует ошибки закрытия.

ПРАВИЛЬНЫЙ ПОДХОД:
  - Оборачивать asyncio.to_thread в asyncio.wait_for.
  - Логировать ошибки disconnect, но не падать.
  - Не использовать assert в production-коде для проверки инвариантов.
  - Проверять возвращаемый тип от driver.read() — может быть dict ИЛИ raw.

Запуск (БЕЗ MQTT-брокера)::

    python examples/19_driver_edge_cases.py
"""
from __future__ import annotations

import asyncio
import logging
import struct
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

from kamio.drivers.base import BaseDriver
from kamio.drivers.mock import MockHardwareDriver
from kamio.drivers.modbus import ModbusTCPDriver
from kamio.drivers.telnet import TelnetDriver

logging.basicConfig(level=logging.DEBUG, format="%(name)s | %(levelname)s | %(message)s")
logger = logging.getLogger("example.19")


# ============================================================================
# 1. MockHardwareDriver: failure_rate и connected state
# ============================================================================

async def demo_mock_failure_rate():
    """failure_rate=1.0 — КАЖДАЯ операция падает, включая connect."""
    print("\n--- 1. MockHardwareDriver: failure_rate=1.0 ---")

    # НЕПРАВИЛЬНО: ожидать, что failure_rate влияет только на read/execute
    driver = MockHardwareDriver(failure_rate=1.0, latency_range=(0, 0.001))
    try:
        await driver.connect()
        assert False, "connect() должен был упасть при failure_rate=1.0"
    except ConnectionError as e:
        # ПРАВИЛЬНО: failure_rate применяется и к connect()
        print(f"  connect() упал как ожидается: {e}")
        assert "Mock connection failed" in str(e)

    # connected остаётся False после неудачного connect
    assert driver.connected is False, "connected должен быть False после неудачного connect"

    # read/execute падают с RuntimeError, если не connected
    driver2 = MockHardwareDriver(failure_rate=0.0, latency_range=(0, 0.001))
    try:
        await driver2.read("temperature")
        assert False, "read() должен упасть без connect()"
    except RuntimeError as e:
        print(f"  read() без connect(): {e}")
        assert "not connected" in str(e)

    await driver2.disconnect()
    print("  OK: failure_rate и connected проверены")


# ============================================================================
# 2. MockHardwareDriver: read() возвращает RAW, а не dict
# ============================================================================

async def demo_mock_raw_return():
    """read() возвращает raw значение, а не dict — отличие от других драйверов."""
    print("\n--- 2. MockHardwareDriver: read() возвращает raw, не dict ---")

    driver = MockHardwareDriver(
        failure_rate=0.0,
        latency_range=(0, 0.001),
        initial_state={"temperature": 23.5, "humidity": 45},
    )
    await driver.connect()

    result = await driver.read("temperature")

    # НЕПРАВИЛЬНО: ожидать {"status": "ok", "field": "temperature", "data": 23.5}
    # assert result["status"] == "ok"  # ← AttributeError: 'float' object has no attribute '__getitem__'

    # ПРАВИЛЬНО: MockHardwareDriver.read() возвращает raw значение
    assert result == 23.5, f"Ожидали 23.5, получили {result!r}"
    assert not isinstance(result, dict), "Mock read() возвращает raw, не dict"
    print(f"  read('temperature') = {result!r} (raw float, не dict)")

    # execute() возвращает dict, а read() — raw
    exec_result = await driver.execute("set_temperature", {"value": 25.0})
    assert isinstance(exec_result, dict), "execute() возвращает dict"
    assert exec_result["status"] == "ok"
    print(f"  execute('set_temperature') = {exec_result} (dict)")

    # Сравнение с Modbus: Modbus.read() возвращает dict
    modbus = ModbusTCPDriver("127.0.0.1", reconnect_attempts=0)
    # Не подключаем — просто проверяем тип возвращаемого значения по коду
    # Modbus.read() всегда возвращает {"status": "ok", "field": ..., "data": ...}
    print("  Modbus.read() возвращает dict (см. исходный код), Mock.read() — raw")

    await driver.disconnect()
    print("  OK: raw vs dict возвращаемые типы проверены")


# ============================================================================
# 3. ModbusTCPDriver: reconnect_attempts обрезается до 0
# ============================================================================

async def demo_modbus_reconnect_clamped():
    """reconnect_attempts=max(0, int(reconnect_attempts)) — отрицательные → 0."""
    print("\n--- 3. ModbusTCPDriver: reconnect_attempts clamped ---")

    # НЕПРАВИЛЬНО: передать отрицательное значение, ожидая "бесконечные ретраи"
    driver = ModbusTCPDriver("127.0.0.1", reconnect_attempts=-5)

    # ПРАВИЛЬНО: отрицательное значение молча обрезается до 0
    assert driver.reconnect_attempts == 0, (
        f"Ожидали 0, получили {driver.reconnect_attempts}"
    )
    print(f"  reconnect_attempts=-5 → clamped to {driver.reconnect_attempts}")

    # reconnect_attempts=0 означает ОДНУ попытку (range(0+1) = [0])
    # Цикл в _transaction_exchange: for attempt in range(self.reconnect_attempts + 1)
    # При reconnect_attempts=0: range(1) = [0] — одна попытка, без ретрая
    driver2 = ModbusTCPDriver("127.0.0.1", reconnect_attempts=0)
    assert driver2.reconnect_attempts == 0
    print(f"  reconnect_attempts=0 → {driver2.reconnect_attempts} (одна попытка, без ретрая)")

    # При reconnect_attempts=2: range(3) = [0, 1, 2] — три попытки
    driver3 = ModbusTCPDriver("127.0.0.1", reconnect_attempts=2)
    assert driver3.reconnect_attempts == 2
    print(f"  reconnect_attempts=2 → {driver3.reconnect_attempts} (три попытки)")

    print("  OK: reconnect_attempts clamping проверен")


# ============================================================================
# 4. ModbusTCPDriver: transaction ID wraps at 0xFFFF
# ============================================================================

async def demo_modbus_transaction_wrap():
    """_transaction обнуляется на 0xFFFF через маску & 0xFFFF."""
    print("\n--- 4. ModbusTCPDriver: transaction ID wraps at 0xFFFF ---")

    driver = ModbusTCPDriver("127.0.0.1", reconnect_attempts=0)

    # Устанавливаем transaction близко к границе
    driver._transaction = 0xFFFE  # 65534

    # НЕПРАВИЛЬНО: ожидать, что transaction монотонно растёт
    # ПРАВИЛЬНО: (self._transaction + 1) & 0xFFFF — обнуляется после 65535

    # Симулируем инкремент (код из _exchange_once)
    driver._transaction = (driver._transaction + 1) & 0xFFFF
    assert driver._transaction == 0xFFFF, f"Ожидали 0xFFFF, получили {driver._transaction:#x}"
    print(f"  После 0xFFFE + 1 = {driver._transaction:#x} (0xFFFF, максимум)")

    # Следующий инкремент — обнуление
    driver._transaction = (driver._transaction + 1) & 0xFFFF
    assert driver._transaction == 0, f"Ожидали 0 (wrap), получили {driver._transaction:#x}"
    print(f"  После 0xFFFF + 1 = {driver._transaction:#x} (WRAP к 0!)")

    # ПОДВОХ: после 65535 транзакций ID сбрасывается в 0.
    # Если старый ответ с tid=0 ещё в буфере, произойдёт ложное совпадение.
    print("  ВНИМАНИЕ: wrap может вызвать коллизию tid при долгой работе")

    print("  OK: transaction ID wrap проверен")


# ============================================================================
# 5. ModbusTCPDriver: _close_writer глотает ВСЕ исключения
# ============================================================================

async def demo_modbus_close_writer_swallows():
    """_close_writer глотает ВСЕ исключения — ошибки закрытия невидимы."""
    print("\n--- 5. ModbusTCPDriver: _close_writer глотает исключения ---")

    driver = ModbusTCPDriver("127.0.0.1", reconnect_attempts=0)

    # Создаём фейковый writer, который падает при close/wait_closed
    fake_writer = MagicMock()
    fake_writer.close.side_effect = OSError("Socket broken")
    fake_writer.wait_closed = AsyncMock(side_effect=RuntimeError("Already closed"))
    driver._writer = fake_writer
    driver._reader = MagicMock()

    # НЕПРАВИЛЬНО: ожидать, что _close_writer сообщит об ошибке
    # ПРАВИЛЬНО: except Exception: pass — все ошибки молча проглатываются
    await driver._close_writer()  # не падает, несмотря на ошибки

    # writer и reader обнуляются в любом случае
    assert driver._writer is None, "writer должен быть None после _close_writer"
    assert driver._reader is None, "reader должен быть None после _close_writer"
    print("  _close_writer() проглотил OSError и RuntimeError без уведомления")
    print("  writer и reader обнулены в любом случае")

    # ПОДВОХ: close() был вызван, но wait_closed() не был (close упал первым)
    # В реальности это может означать утечку файлового дескриптора
    fake_writer.close.assert_called_once()
    print("  ВНИМАНИЕ: close() вызван, но wait_closed() не выполнен — возможна утечка fd")

    print("  OK: _close_writer exception swallowing проверен")


# ============================================================================
# 6. ModbusTCPDriver: _reconnect без backoff
# ============================================================================

async def demo_modbus_no_backoff():
    """_reconnect() не имеет backoff — мгновенный retry после сбоя."""
    print("\n--- 6. ModbusTCPDriver: _reconnect без backoff ---")

    driver = ModbusTCPDriver("127.0.0.1", reconnect_attempts=2)

    # _reconnect просто вызывает _close_writer + _open_connection
    # Никакого asyncio.sleep между попытками нет.
    # В _transaction_exchange цикл ретраев тоже без задержки:
    #   for attempt in range(self.reconnect_attempts + 1):
    #       try: return await self._exchange_once(pdu)
    #       except ...:
    #           if attempt < self.reconnect_attempts:
    #               await self._reconnect()  # ← без sleep!
    #               continue

    # Сравнение с TelnetDriver, который ИМЕЕТ backoff:
    telnet = TelnetDriver("127.0.0.1")
    assert hasattr(telnet, "_reconnect_delay_base"), "TelnetDriver имеет _reconnect_delay_base"
    print(f"  TelnetDriver: _reconnect_delay_base={telnet._reconnect_delay_base} (есть backoff)")

    # ModbusTCPDriver не имеет никакого delay
    assert not hasattr(driver, "_reconnect_delay_base"), "ModbusTCPDriver НЕ имеет backoff"
    print("  ModbusTCPDriver: НЕТ backoff в _reconnect (мгновенный retry)")

    # ПРАВИЛЬНЫЙ ПОДХОД: добавить задержку вручную при использовании
    print("  ПРАВИЛЬНО: добавьте asyncio.sleep перед повторным подключением")

    print("  OK: отсутствие backoff проверено")


# ============================================================================
# 7. TelnetDriver: assert в production (удаляется с -O)
# ============================================================================

async def demo_telnet_assert_in_production():
    """assert в execute() удаляется при python -O — writer может быть None."""
    print("\n--- 7. TelnetDriver: assert удаляется с -O ---")

    # В TelnetDriver.execute():
    #   await self._ensure_connected()
    #   assert self.writer is not None and self.reader is not None
    #
    # Если _ensure_connected() не смог подключиться, но не поднял исключение
    # (например, max_reconnect_attempts=0), assert — единственная защита.
    # С python -O assert удаляется → writer=None → AttributeError в .write()

    driver = TelnetDriver("127.0.0.1", max_reconnect_attempts=0)

    # НЕПРАВИЛЬНО: полагаться на assert для проверки инвариантов в production
    # ПРАВИЛЬНО: использовать явный if-raise
    #
    # Демонстрация: _ensure_connected с max_reconnect_attempts=0
    # Если writer=None и reader=None, _ensure_connected вызывает disconnect
    # (no-op), затем цикл range(1, 0+1)=range(1,1)=[] — НИ ОДНОЙ попытки!
    # → writer остаётся None → assert ловит (БЕЗ -O)

    # Симулируем: writer=None → _ensure_connected → disconnect (no-op) →
    # range(1, 1) = [] → нет попыток → writer всё ещё None
    assert driver.writer is None
    assert driver.reader is None

    # _ensure_connected с max_reconnect_attempts=0 не делает попыток
    try:
        await driver._ensure_connected()
        # Если мы здесь, writer всё ещё None
        # assert self.writer is not None — единственная защита (БЕЗ -O)
        assert driver.writer is None, "writer должен быть None после неудачного _ensure_connected"
        print("  _ensure_connected() с max_reconnect_attempts=0: writer остался None")
        print("  assert self.writer is not None — единственная защита (БЕЗ -O)")
        print("  С python -O: assert удалён → AttributeError в writer.write()")
    except Exception as e:
        print(f"  _ensure_connected() поднял: {e}")

    print("  OK: assert в production проверен")


# ============================================================================
# 8. TelnetDriver: timeout возвращает "ok" с пустым response
# ============================================================================

async def demo_telnet_timeout_returns_ok():
    """timeout при чтении возвращает {'status': 'ok', 'response': ''}."""
    print("\n--- 8. TelnetDriver: timeout → status=ok, response='' ---")

    # В TelnetDriver.execute():
    #   try:
    #       line = await asyncio.wait_for(self.reader.readline(), timeout=self.timeout)
    #       response = line.decode().strip()
    #   except asyncio.TimeoutError:
    #       self.logger.warning("Telnet read timeout")
    #   return {"status": "ok", "command": command_name, "response": response}
    #
    # ПОДВОХ: response="" при timeout неотличим от реального пустого ответа.
    # status="ok" даже когда данных нет.

    # Демонстрация через mock
    driver = TelnetDriver("127.0.0.1", timeout=0.05)

    # Создаём фейковый reader/writer
    fake_reader = MagicMock()
    fake_writer = MagicMock()
    fake_writer.is_closing.return_value = False
    fake_writer.write = MagicMock()
    fake_writer.drain = AsyncMock()

    # readline() будет висеть дольше timeout
    async def slow_readline():
        await asyncio.sleep(10)
        return b"late response\n"

    fake_reader.readline = slow_readline
    driver.reader = fake_reader
    driver.writer = fake_writer

    result = await driver.execute("get_status", {"wait_response": True})

    # НЕПРАВИЛЬНО: проверять result["status"] == "ok" и считать, что всё хорошо
    # ПРАВИЛЬНО: проверять response на пустоту
    assert result["status"] == "ok", "status='ok' даже при timeout!"
    assert result["response"] == "", "response пустой при timeout"
    print(f"  execute() при timeout: {result}")
    print("  ВНИМАНИЕ: status='ok' с пустым response — неотличимо от реального пустого ответа")

    # ПРАВИЛЬНЫЙ ПОДХОД: проверять response
    if not result["response"]:
        print("  ПРАВИЛЬНО: проверять response на пустоту, а не только status")

    print("  OK: timeout → ok проверен")


# ============================================================================
# 9. SerialDriver: asyncio.to_thread БЕЗ wait_for (может зависнуть)
# ============================================================================

async def demo_serial_no_async_timeout():
    """SerialDriver.read/execute используют to_thread без wait_for — зависание."""
    print("\n--- 9. SerialDriver: to_thread без wait_for ---")

    # В SerialDriver.read():
    #   response = await asyncio.to_thread(_write_read)
    #
    # НЕТ asyncio.wait_for вокруг to_thread!
    # Если pyserial зависнет (устройство не отвечает, кабель оборван),
    # to_thread блокирует поток навсегда.  Event loop не блокируется,
    # но операция никогда не завершится.

    # Демонстрация: симулируем зависший to_thread
    print("  SerialDriver.read() → await asyncio.to_thread(_write_read)")
    print("  НЕТ asyncio.wait_for — операция может висеть вечно")
    print("  Event loop не блокируется, но задача никогда не завершится")

    # ПРАВИЛЬНЫЙ ПОДХОД: обернуть в wait_for
    async def simulated_blocking_read():
        # Симулируем зависший pyserial
        await asyncio.sleep(999)
        return "never"

    # НЕПРАВИЛЬНО:
    # result = await asyncio.to_thread(blocking_serial_read)

    # ПРАВИЛЬНО:
    try:
        result = await asyncio.wait_for(simulated_blocking_read(), timeout=0.1)
    except asyncio.TimeoutError:
        print("  ПРАВИЛЬНО: asyncio.wait_for(timeout=0.1) → TimeoutError через 0.1с")
        print("  Это позволяет обработать зависание, а не ждать вечно")

    print("  OK: отсутствие wait_for в SerialDriver проверено")


# ============================================================================
# 10. SerialDriver: errors="replace" тихо искажает данные
# ============================================================================

async def demo_serial_errors_replace():
    """decode(errors='replace') тихо заменяет некорректные байты на U+FFFD."""
    print("\n--- 10. SerialDriver: errors='replace' искажает данные ---")

    # В SerialDriver.read() и execute():
    #   return _readline_bounded(self.ser, self.read_limit).decode(errors="replace").strip()
    #
    # errors="replace" заменяет байты, не образующие валидный UTF-8, на U+FFFD ().
    # Это ТИХОЕ искажение — никаких исключений, просто мусор в данных.

    # Демонстрация
    bad_bytes = b"\xff\xfe\xfd\x00garbage\xff"
    decoded = bad_bytes.decode(errors="replace")

    # НЕПРАВИЛЬНО: ожидать, что decode сообщит об ошибке
    # ПРАВИЛЬНО: errors="replace" тихо заменяет
    assert "\ufffd" in decoded, "Должен содержать U+FFFD (заменяющий символ)"
    print(f"  b'\\xff\\xfe\\xfd\\x00garbage\\xff'.decode(errors='replace') = {decoded!r}")
    print(f"  Содержит U+FFFD: {chr(0xFFFD) in decoded}")

    # Сравнение: errors="strict" (по умолчанию) — поднимает UnicodeDecodeError
    try:
        bad_bytes.decode(errors="strict")
        assert False, "Должен был упасть UnicodeDecodeError"
    except UnicodeDecodeError as e:
        print(f"  decode(errors='strict') → UnicodeDecodeError: {e}")
        print("  ПРАВИЛЬНО: использовать errors='strict' и обрабатывать исключение")

    print("  OK: errors='replace' silent corruption проверен")


# ============================================================================
# 11. BaseDriver: __aexit__ не подавляет исключения disconnect
# ============================================================================

async def demo_base_aexit_no_suppress():
    """__aexit__ не возвращает True — исключения disconnect не подавляются."""
    print("\n--- 11. BaseDriver: __aexit__ не подавляет исключения ---")

    # В BaseDriver.__aexit__:
    #   async def __aexit__(self, exc_type, exc_val, exc_tb):
    #       await self.disconnect()
    #
    # НЕТ return True — исключения из disconnect() вылетают из async with.
    # Если в теле async with было исключение, а disconnect() тоже падает,
    # исключение disconnect ЗАМЕНЯЕТ исходное.

    # Создаём драйвер, у которого disconnect падает
    class BadDisconnectDriver(BaseDriver):
        async def connect(self):
            pass

        async def disconnect(self):
            raise OSError("Disconnect failed!")

        async def execute(self, command_name, params):
            return {}

        async def read(self, field_name, params=None):
            return None

    # НЕПРАВИЛЬНО: ожидать, что async with подавит ошибку disconnect
    driver = BadDisconnectDriver()

    # Случай 1: тело без ошибки, disconnect падает
    try:
        async with driver:
            pass  # нормальное завершение
        assert False, "disconnect() должен был упасть"
    except OSError as e:
        print(f"  async with (нормальное завершение) → disconnect упал: {e}")
        assert "Disconnect failed" in str(e)

    # Случай 2: тело с ошибкой, disconnect тоже падает — исходная ошибка ЗАТЕРТА
    driver2 = BadDisconnectDriver()
    try:
        async with driver2:
            raise ValueError("Original error in body")
        assert False
    except OSError as e:
        # НЕПРАВИЛЬНО: исходная ValueError затёрта OSError от disconnect!
        print(f"  async with (ValueError в теле) → OSError от disconnect затёр исходную ошибку: {e}")
        print("  ВНИМАНИЕ: исходное исключение (ValueError) ПОТЕРЯНО!")
        assert "Disconnect failed" in str(e)

    # ПРАВИЛЬНЫЙ ПОДХОД: обернуть disconnect в try/except
    class SafeDriver(BaseDriver):
        async def connect(self):
            pass

        async def disconnect(self):
            try:
                raise OSError("Disconnect failed!")
            except Exception as exc:
                logger.warning(f"Safe disconnect error: {exc}")

        async def execute(self, command_name, params):
            return {}

        async def read(self, field_name, params=None):
            return None

    driver3 = SafeDriver()
    try:
        async with driver3:
            raise ValueError("Original error in body")
    except ValueError as e:
        print(f"  ПРАВИЛЬНО: SafeDriver — исходная ValueError сохранена: {e}")
        assert "Original error" in str(e)

    print("  OK: __aexit__ no-suppress проверен")


# ============================================================================
# 12. Все драйверы: disconnect глотает ошибки
# ============================================================================

async def demo_all_drivers_disconnect_swallows():
    """disconnect() во всех драйверах глотает или логирует ошибки закрытия."""
    print("\n--- 12. Все драйверы: disconnect глотает ошибки ---")

    # ModbusTCPDriver.disconnect → _close_writer → except Exception: pass
    # TelnetDriver.disconnect → except Exception as e: logger.warning(...)
    # SerialDriver.disconnect → except Exception as e: logger.warning(...)
    # MockHardwareDriver.disconnect → просто self.connected = False (не падает)

    # Modbus
    modbus = ModbusTCPDriver("127.0.0.1", reconnect_attempts=0)
    fake_writer = MagicMock()
    fake_writer.close.side_effect = Exception("Boom")
    fake_writer.wait_closed = AsyncMock(side_effect=Exception("Boom2"))
    modbus._writer = fake_writer
    modbus._reader = MagicMock()
    await modbus.disconnect()  # не падает
    print("  ModbusTCPDriver.disconnect() — глотает через except Exception: pass")

    # Telnet
    telnet = TelnetDriver("127.0.0.1")
    fake_writer2 = MagicMock()
    fake_writer2.close.side_effect = Exception("Boom")
    fake_writer2.wait_closed = AsyncMock(side_effect=Exception("Boom2"))
    telnet.writer = fake_writer2
    telnet.reader = MagicMock()
    await telnet.disconnect()  # не падает, логирует warning
    print("  TelnetDriver.disconnect() — глотает через except + logger.warning")

    # Mock
    mock = MockHardwareDriver(latency_range=(0, 0.001))
    await mock.connect()
    await mock.disconnect()  # просто connected = False
    assert mock.connected is False
    print("  MockHardwareDriver.disconnect() — просто connected = False")

    print("  OK: все драйверы глотают disconnect errors")


# ============================================================================
# 13. ПРАВИЛЬНЫЙ ПОДХОД: обёртка to_thread с wait_for
# ============================================================================

async def demo_right_way_wait_for_to_thread():
    """ПРАВИЛЬНО: оборачивать to_thread в wait_for для таймаута."""
    print("\n--- 13. ПРАВИЛЬНЫЙ ПОДХОД: wait_for вокруг to_thread ---")

    import time

    def blocking_io(duration: float) -> str:
        """Симулирует блокирующий I/O (как pyserial)."""
        time.sleep(duration)
        return "result"

    # НЕПРАВИЛЬНО: без wait_for — зависает если duration слишком большой
    # result = await asyncio.to_thread(blocking_io, 999)

    # ПРАВИЛЬНО: с wait_for
    try:
        result = await asyncio.wait_for(
            asyncio.to_thread(blocking_io, 5.0),
            timeout=0.1,
        )
        assert False, "Должен был timeout"
    except asyncio.TimeoutError:
        print("  asyncio.wait_for(asyncio.to_thread(...), timeout=0.1) → TimeoutError")
        print("  Поток продолжает работать в фоне, но задача отменена")

    # ПРАВИЛЬНО: нормальный случай — завершается вовремя
    result = await asyncio.wait_for(
        asyncio.to_thread(blocking_io, 0.01),
        timeout=1.0,
    )
    assert result == "result"
    print(f"  Нормальный случай: result={result!r}")

    print("  OK: wait_for вокруг to_thread проверен")


# ============================================================================
# 14. ПРАВИЛЬНЫЙ ПОДХОД: обработка ошибок disconnect
# ============================================================================

async def demo_right_way_disconnect_handling():
    """ПРАВИЛЬНО: логировать ошибки disconnect, но не падать."""
    print("\n--- 14. ПРАВИЛЬНЫЙ ПОДХОД: обработка disconnect errors ---")

    class ProductionDriver(BaseDriver):
        """Драйвер с правильной обработкой disconnect."""

        def __init__(self):
            super().__init__()
            self._connected = False

        async def connect(self):
            self._connected = True

        async def disconnect(self):
            # ПРАВИЛЬНО: try/except/finally — логируем, но не падаем
            try:
                if self._connected:
                    # Симулируем ошибку закрытия
                    raise OSError("Connection reset by peer")
            except Exception as e:
                self.logger.warning(f"Disconnect error (non-fatal): {e}")
            finally:
                self._connected = False
                self.logger.info("Disconnected (cleanup done)")

        async def execute(self, command_name, params):
            if not self._connected:
                raise RuntimeError("Not connected")
            return {"status": "ok"}

        async def read(self, field_name, params=None):
            if not self._connected:
                raise RuntimeError("Not connected")
            return None

    driver = ProductionDriver()
    await driver.connect()

    # disconnect не падает, несмотря на внутреннюю ошибку
    await driver.disconnect()
    assert driver._connected is False
    print("  disconnect() залогировал ошибку, но не упал")
    print("  _connected = False в finally — корректная очистка")

    # async with тоже работает
    driver2 = ProductionDriver()
    async with driver2:
        result = await driver2.execute("test", {})
        assert result["status"] == "ok"
    # disconnect не упал → async with завершён корректно
    assert driver2._connected is False
    print("  async with завершён корректно — disconnect не упал")

    print("  OK: правильная обработка disconnect проверена")


# ============================================================================
# Main
# ============================================================================

async def main():
    print("=" * 70)
    print("19 — Driver Edge Cases (краевые случаи драйверов)")
    print("=" * 70)

    await demo_mock_failure_rate()
    await demo_mock_raw_return()
    await demo_modbus_reconnect_clamped()
    await demo_modbus_transaction_wrap()
    await demo_modbus_close_writer_swallows()
    await demo_modbus_no_backoff()
    await demo_telnet_assert_in_production()
    await demo_telnet_timeout_returns_ok()
    await demo_serial_no_async_timeout()
    await demo_serial_errors_replace()
    await demo_base_aexit_no_suppress()
    await demo_all_drivers_disconnect_swallows()
    await demo_right_way_wait_for_to_thread()
    await demo_right_way_disconnect_handling()

    print("\n" + "=" * 70)
    print("ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ ✓")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
