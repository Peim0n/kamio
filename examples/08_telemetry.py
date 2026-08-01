"""
08 — Telemetry (телеметрия)
============================

Демонстрирует систему телеметрии Kamio:
    - Поле telemetry() с параметрами freq, unit, default
    - Автоматическая публикация через freq (периодический опрос)
    - Ручная публикация через publish_telemetry()
    - Чтение значений из драйвера через read_telemetry_value()
    - Переопределение handle_telemetry_update() для кастомной логики сбора
    - Несколько полей телеметрии с разными частотами
    - Валидация min/max для телеметрии
    - enable_telemetry = False для отключения телеметрии

Запуск::
    python examples/08_telemetry.py

Предварительно запустите MQTT-брокер на localhost:1883
(например, ``docker run -p 1883:1883 eclipse-mosquitto``).
"""
from __future__ import annotations

import asyncio
import logging
import random
from typing import Any, Dict, Optional

from kamio import KamioApp, Device, command, state, telemetry
from kamio.drivers.base import BaseDriver

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("telemetry_demo")


# =====================================================================
# Псевдо-драйвер: имитирует аппаратный датчик
# =====================================================================

class FakeSensorDriver(BaseDriver):
    """Драйвер-имитатор, возвращающий случайные значения датчиков.

    Реальный драйвер (например, ModbusTCPDriver) читает значения
    из физического устройства. Здесь мы генерируем случайные числа,
    чтобы пример работал без железа.
    """

    def __init__(self):
        super().__init__()
        self._connected = False
        # Внутренние «регистры» устройства
        self._registers: Dict[str, float] = {
            "temperature": 22.0,
            "humidity": 45.0,
            "pressure": 1013.0,
            "co2": 420.0,
        }

    async def connect(self) -> None:
        """Имитация подключения к устройству."""
        self._connected = True
        self.logger.info("FakeSensorDriver подключён")

    async def disconnect(self) -> None:
        """Имитация отключения."""
        self._connected = False
        self.logger.info("FakeSensorDriver отключён")

    async def read(self, field_name: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Чтение значения датчика по имени поля.

        Возвращает стандартный ответ драйвера:
            {"status": "ok", "field": <имя>, "address": <адрес>, "data": <значение>}

        TelemetryMixin.read_telemetry_value() автоматически извлекает
        ключ "data" из этого ответа.
        """
        if not self._connected:
            raise RuntimeError("Драйвер не подключён")

        # Имитируем небольшое изменение значений при каждом чтении
        if field_name in self._registers:
            base = self._registers[field_name]
            # Случайное отклонение ±2% от базового значения
            noise = random.uniform(-0.02, 0.02) * base
            value = round(base + noise, 2)
            self._registers[field_name] = value
            return {"status": "ok", "field": field_name, "address": 0, "data": value}

        # Неизвестное поле
        return {"status": "ok", "field": field_name, "address": 0, "data": None}

    async def execute(self, command_name: str, params: Dict[str, Any]) -> Any:
        """Выполнение команды на «устройстве» (не используется в примере)."""
        return {"status": "ok", "command": command_name}


# =====================================================================
# Устройство 1: датчик среды с автоматической телеметрией
# =====================================================================

class EnvironmentSensor(Device):
    """Датчик окружающей среды с автоматической телеметрией.

    Поля телеметрии объявляются через telemetry() и публикуются
    автоматически с заданной частотой (freq). Фреймворк сам
    создаёт периодические задачи для каждого поля.
    """

    # --- Поля телеметрии с разными частотами ---
    # freq="5s"  — публикация каждые 5 секунд
    # unit="°C" — единица измерения (передаётся в HA Discovery)
    # min/max — валидация: если значение выходит за пределы,
    #           _validate_value вызовет ValueError
    temperature: float = telemetry(
        default=22.0,
        unit="°C",
        freq="5s",
        min=-40.0,
        max=85.0,
        description="Температура воздуха",
    )

    # freq="10s" — публикация каждые 10 секунд (реже, т.к. влажность
    # меняется медленнее)
    humidity: float = telemetry(
        default=45.0,
        unit="%",
        freq="10s",
        min=0.0,
        max=100.0,
        description="Относительная влажность",
    )

    # freq="30s" — публикация каждые 30 секунд
    pressure: float = telemetry(
        default=1013.0,
        unit="hPa",
        freq="30s",
        min=800.0,
        max=1100.0,
        description="Атмосферное давление",
    )

    # --- Поле состояния (управляемое) ---
    reporting_enabled: bool = state(
        default=True,
        writable=True,
        description="Включена ли передача телеметрии",
    )

    @command
    async def calibrate(self):
        """Калибровка датчика (имитация)."""
        logger.info(f"Калибровка {self.node.device_id}...")
        await asyncio.sleep(0.5)
        return {"status": "calibrated"}


# =====================================================================
# Устройство 2: датчик CO2 с переопределённым handle_telemetry_update
# =====================================================================

class CO2Sensor(Device):
    """Датчик CO2 с кастомной логикой сбора телеметрии.

    Переопределяем handle_telemetry_update(), чтобы добавить
    пользовательскую логику перед публикацией (например, округление
    или фильтрацию выбросов).
    """

    co2: float = telemetry(
        default=420.0,
        unit="ppm",
        freq="5s",
        min=0.0,
        max=5000.0,
        description="Концентрация CO2",
    )

    # Дополнительное вычисляемое поле телеметрии
    air_quality: str = telemetry(
        default="good",
        freq="5s",
        description="Качество воздуха (good/moderate/poor)",
    )

    async def handle_telemetry_update(self, field_names: list[str]) -> Optional[dict[str, Any]]:
        """Кастомный сбор телеметрии.

        По умолчанию handle_telemetry_update читает значения из атрибутов
        или через драйвер. Здесь мы переопределяем его, чтобы:
        1. Читать co2 из драйвера
        2. Вычислять air_quality на основе значения co2
        3. Возвращать оба поля одним словарём

        Возвращаемый словарь публикуется через publish_telemetry().
        Если вернуть None — публикация пропускается.
        """
        data: dict[str, Any] = {}

        for name in field_names:
            if name == "co2":
                # Читаем значение через драйвер
                val = await self.read_telemetry_value("co2")
                if val is not None:
                    # Округляем до целого ppm
                    val = round(float(val))
                    data["co2"] = val
                    # Сохраняем в атрибут для get_telemetry_snapshot()
                    self.co2 = float(val)

            elif name == "air_quality":
                # Вычисляем качество воздуха на основе текущего CO2
                co2_val = self.co2
                if co2_val < 800:
                    quality = "good"
                elif co2_val < 1200:
                    quality = "moderate"
                else:
                    quality = "poor"
                data["air_quality"] = quality
                self.air_quality = quality

        return data if data else None


# =====================================================================
# Устройство 3: телеметрия отключена через enable_telemetry
# =====================================================================

class DisabledTelemetryDevice(Device):
    """Устройство с отключённой телеметрией.

    Установка enable_telemetry = False полностью отключает
    автоматическую публикацию телеметрии. Поля telemetry() всё
    равно объявляются в схеме, но периодические задачи не запускаются.
    Это полезно для экономии трафика или когда публикация управляется
    внешним кодом.
    """

    # Классовый атрибут — отключает телеметрию для всех экземпляров
    enable_telemetry = False

    voltage: float = telemetry(
        default=230.0,
        unit="V",
        freq="5s",
        description="Напряжение сети",
    )

    @command
    async def manual_report(self):
        """Ручная публикация телеметрии, несмотря на enable_telemetry=False.

        Даже когда автоматическая телеметрия отключена, мы можем
        публиковать значения вручную через publish_telemetry().
        """
        self.voltage = round(random.uniform(225.0, 235.0), 1)
        await self.publish_telemetry({"voltage": self.voltage})
        logger.info(f"Ручная публикация: voltage={self.voltage}V")
        return {"voltage": self.voltage}


# =====================================================================
# Подписчик на события телеметрии для логирования
# =====================================================================

async def on_command_executed(data: Dict[str, Any]) -> None:
    """Логирование выполнения команд (встроенное событие)."""
    logger.info(
        f"[command] {data['device_id']}.{data['command']} -> {data['result']}"
    )


# =====================================================================
# Главный цикл
# =====================================================================

async def main():
    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="telemetry_demo")

    # Подписка на выполнение команд для логирования
    app.subscribe_event("device_command_executed", on_command_executed)

    # Регистрируем классы устройств
    app.register(EnvironmentSensor)
    app.register(CO2Sensor)
    app.register(DisabledTelemetryDevice)

    # Запускаем приложение (подключение к MQTT)
    await app.start()

    # --- Создаём устройства ---

    # 1. Датчик среды с драйвером (автоматическая телеметрия)
    driver = FakeSensorDriver()
    env_sensor = await app.add_device("env_sensor_1", EnvironmentSensor, driver=driver)

    # 2. Датчик CO2 с драйвером и кастомным handle_telemetry_update
    co2_driver = FakeSensorDriver()
    co2_sensor = await app.add_device("co2_sensor_1", CO2Sensor, driver=co2_driver)

    # 3. Устройство с отключённой телеметрией
    disabled_dev = await app.add_device("disabled_1", DisabledTelemetryDevice)

    logger.info("=== Устройства созданы. Телеметрия запущена автоматически. ===")
    logger.info("=== EnvironmentSensor: temperature(5s), humidity(10s), pressure(30s) ===")
    logger.info("=== CO2Sensor: co2(5s), air_quality(5s) — кастомный handle_telemetry_update ===")
    logger.info("=== DisabledTelemetryDevice: enable_telemetry=False ===")

    # Ждём, чтобы автоматическая телеметрия успела опубликоваться несколько раз
    logger.info("Ждём 12 секунд для наблюдения за автоматической телеметрией...")
    await asyncio.sleep(12)

    # --- Демонстрация ручной публикации телеметрии ---
    logger.info("=== Ручная публикация через publish_telemetry() ===")

    # Публикуем телеметрию вручную (вне графика автоматической публикации)
    await env_sensor.publish_telemetry({
        "temperature": 25.3,
        "humidity": 50.1,
    })
    logger.info("Ручная публикация env_sensor: temperature=25.3°C, humidity=50.1%")

    await asyncio.sleep(1)

    # --- Демонстрация get_telemetry_snapshot() ---
    logger.info("=== Снимки телеметрии (get_telemetry_snapshot) ===")
    telemetry_snap = env_sensor.get_telemetry_snapshot()
    logger.info(f"EnvironmentSensor телеметрия: {telemetry_snap}")

    full_snap = env_sensor.get_full_snapshot()
    logger.info(f"EnvironmentSensor полный снимок: {full_snap}")

    # --- Демонстрация read_telemetry_value() напрямую ---
    logger.info("=== Прямое чтение через read_telemetry_value() ===")
    temp_value = await env_sensor.read_telemetry_value("temperature")
    logger.info(f"Прямое чтение temperature: {temp_value}")

    # --- Демонстрация устройства с отключённой телеметрией ---
    logger.info("=== Устройство с enable_telemetry=False ===")
    logger.info("Автоматическая телеметрия НЕ публикуется. Вызываем manual_report()...")
    await disabled_dev.handle_command("manual_report", {})
    await asyncio.sleep(1)

    # --- Демонстрация калибровки (команда) ---
    logger.info("=== Команда calibrate на EnvironmentSensor ===")
    result = await env_sensor.handle_command("calibrate", {})
    logger.info(f"Результат калибровки: {result}")

    # Ждём ещё немного для наблюдения
    logger.info("Ждём ещё 6 секунд...")
    await asyncio.sleep(6)

    logger.info("=== Завершение ===")
    await app.stop()


# =====================================================================
# Дополнительные устройства для расширенных демонстраций
# =====================================================================


class ValidatedTelemetryDevice(Device):
    """Устройство с строгой min/max валидацией телеметрии.

    Демонстрирует:
        - min/max валидацию для telemetry-полей
        - ValueError при выходе за пределы
    """

    # Телеметрия с узким диапазоном валидации
    cpu_temp: float = telemetry(
        default=45.0,
        unit="°C",
        freq="5s",
        min=0.0,
        max=100.0,
        description="Температура CPU (0-100°C)",
    )

    fan_speed: int = telemetry(
        default=1500,
        unit="RPM",
        freq="5s",
        min=0,
        max=5000,
        description="Скорость вентилятора (0-5000 RPM)",
    )

    @command
    async def set_bad_temp(self):
        """Попытка установить недопустимое значение температуры."""
        try:
            self.cpu_temp = 150.0  # > max=100.0
            logger.info("ОШИБКА: должно было выбросить ValueError")
        except ValueError as e:
            logger.info(f"✅ cpu_temp=150 отклонено: {e}")

    @command
    async def set_good_temp(self):
        """Установка корректного значения температуры."""
        self.cpu_temp = 65.0
        logger.info(f"cpu_temp=65.0 принято: {self.cpu_temp}")


class CustomCollectionDevice(Device):
    """Устройство с полностью кастомным handle_telemetry_update.

    Демонстрирует:
        - Переопределение handle_telemetry_update для всех полей
        - Вычисляемые значения на основе других полей
        - Возврат None для пропуска публикации
    """

    raw_value: float = telemetry(
        default=0.0,
        unit="count",
        freq="5s",
        description="Сырое значение счётчика",
    )

    smoothed_value: float = telemetry(
        default=0.0,
        unit="count",
        freq="5s",
        description="Сглаженное значение (скользящее среднее)",
    )

    trend: str = telemetry(
        default="stable",
        freq="5s",
        description="Тренд: rising/falling/stable",
    )

    def __init__(self, driver=None, **kwargs):
        super().__init__(driver=driver, **kwargs)
        self._history: list[float] = []
        self._last_raw: float = 0.0

    async def handle_telemetry_update(self, field_names: list[str]) -> Optional[dict[str, Any]]:
        """Полностью кастомный сбор телеметрии.

        Вместо чтения из драйвера, генерируем значения программно:
        1. raw_value — случайное число
        2. smoothed_value — среднее из истории
        3. trend — направление изменения
        """
        data: dict[str, Any] = {}

        if "raw_value" in field_names:
            # Генерируем новое случайное значение
            raw = round(random.uniform(0, 100), 2)
            self._history.append(raw)
            if len(self._history) > 10:
                self._history.pop(0)
            data["raw_value"] = raw
            self.raw_value = raw

        if "smoothed_value" in field_names:
            # Вычисляем скользящее среднее
            if self._history:
                smoothed = round(sum(self._history) / len(self._history), 2)
            else:
                smoothed = 0.0
            data["smoothed_value"] = smoothed
            self.smoothed_value = smoothed

        if "trend" in field_names:
            # Определяем тренд
            if self._history:
                current = self._history[-1]
                if current > self._last_raw + 1:
                    trend = "rising"
                elif current < self._last_raw - 1:
                    trend = "falling"
                else:
                    trend = "stable"
                self._last_raw = current
            else:
                trend = "stable"
            data["trend"] = trend
            self.trend = trend

        return data if data else None


class DriverReadDevice(Device):
    """Устройство, читающее телеметрию через драйвер.

    Демонстрирует:
        - read_telemetry_value() — чтение одного поля из драйвера
        - Автоматическое извлечение ключа 'data' из ответа драйвера
    """

    voltage: float = telemetry(
        default=230.0,
        unit="V",
        freq="5s",
        min=200.0,
        max=250.0,
        description="Напряжение сети",
    )

    current: float = telemetry(
        default=5.0,
        unit="A",
        freq="5s",
        min=0.0,
        max=20.0,
        description="Сила тока",
    )

    @command
    async def read_voltage_manually(self):
        """Ручное чтение напряжения через read_telemetry_value."""
        val = await self.read_telemetry_value("voltage")
        logger.info(f"Ручное чтение voltage через read_telemetry_value: {val}")
        return {"voltage": val}


class MultiFreqDevice(Device):
    """Устройство с полями телеметрии разных частот.

    Демонстрирует:
        - Группировка полей по частоте (freq_groups)
        - Поля с одинаковой частотой публикуются одним сообщением
    """

    # Группа 5s — публикуются вместе
    fast_sensor_1: float = telemetry(default=0.0, unit="°C", freq="5s", description="Быстрый датчик 1")
    fast_sensor_2: float = telemetry(default=0.0, unit="%", freq="5s", description="Быстрый датчик 2")

    # Группа 30s — публикуются вместе
    slow_sensor_1: float = telemetry(default=0.0, unit="hPa", freq="30s", description="Медленный датчик 1")
    slow_sensor_2: float = telemetry(default=0.0, unit="lux", freq="30s", description="Медленный датчик 2")

    # Группа 1m (60s) — публикуются вместе
    rare_sensor: float = telemetry(default=0.0, unit="Wh", freq="1m", description="Редкий датчик")


class NaNFilterDevice(Device):
    """Устройство для демонстрации NaN-фильтрации в телеметрии.

    handle_telemetry_update по умолчанию пропускает значения None
    и NaN (float('nan')). Это предотвращает публикацию невалидных
    данных в MQTT.
    """

    sensor_a: float = telemetry(default=10.0, unit="°C", freq="5s", description="Датчик A")
    sensor_b: float = telemetry(default=20.0, unit="°C", freq="5s", description="Датчик B")
    sensor_c: float = telemetry(default=30.0, unit="°C", freq="5s", description="Датчик C")

    @command
    async def set_nan_values(self):
        """Устанавливает NaN и None для демонстрации фильтрации."""
        # NaN не будет опубликован (val != val → True для NaN)
        self.sensor_b = float('nan')
        logger.info(f"sensor_b установлен в NaN: {self.sensor_b}")

        # None также не будет опубликован
        object.__setattr__(self, "sensor_c", None)
        logger.info(f"sensor_c установлен в None")

        # sensor_a остаётся валидным
        self.sensor_a = 15.0
        logger.info(f"sensor_a установлен в 15.0: {self.sensor_a}")

    async def handle_telemetry_update(self, field_names: list[str]) -> Optional[dict[str, Any]]:
        """Демонстрирует NaN-фильтрацию.

        По умолчанию handle_telemetry_update:
        - Пропускает None значения
        - Пропускает NaN (через проверку val != val)
        - Включает falsy-but-valid значения (0, False, "")
        """
        data = {}
        for name in field_names:
            val = getattr(self, name, None)
            if val is None:
                logger.info(f"  [NaN фильтр] {name}: None → пропущено")
                continue
            if isinstance(val, float) and val != val:  # NaN check
                logger.info(f"  [NaN фильтр] {name}: NaN → пропущено")
                continue
            data[name] = val
            logger.info(f"  [NaN фильтр] {name}: {val} → включено")
        return data if data else None


# =====================================================================
# Демонстрация: телеметрия с min/max валидацией
# =====================================================================

async def demo_telemetry_validation(app: KamioApp):
    """Показывает min/max валидацию для телеметрии."""
    logger.info("=== Демонстрация: телеметрия с min/max валидацией ===")

    app.register(ValidatedTelemetryDevice)
    dev = await app.add_device("validated_telemetry", ValidatedTelemetryDevice)

    # Попытка установить значение выше max
    logger.info("Вызываем set_bad_temp (попытка установить 150°C при max=100)...")
    await dev.handle_command("set_bad_temp", {})
    await asyncio.sleep(0.3)

    # Корректное значение
    logger.info("Вызываем set_good_temp (установка 65°C)...")
    await dev.handle_command("set_good_temp", {})
    await asyncio.sleep(0.3)

    # Прямое присваивание также валидируется
    try:
        dev.fan_speed = 6000  # > max=5000
        logger.info("ОШИБКА: должно было выбросить ValueError")
    except ValueError as e:
        logger.info(f"✅ fan_speed=6000 отклонено: {e}")

    dev.fan_speed = 2500
    logger.info(f"fan_speed=2500 принято: {dev.fan_speed}")

    # Снимок телеметрии
    snap = dev.get_telemetry_snapshot()
    logger.info(f"get_telemetry_snapshot(): {snap}")


# =====================================================================
# Демонстрация: handle_telemetry_update override
# =====================================================================

async def demo_custom_telemetry_update(app: KamioApp):
    """Показывает кастомный сбор телеметрии через handle_telemetry_update."""
    logger.info("=== Демонстрация: handle_telemetry_update override ===")

    app.register(CustomCollectionDevice)
    dev = await app.add_device("custom_telemetry", CustomCollectionDevice)

    logger.info("CustomCollectionDevice собирает телеметрию программно:")
    logger.info("  raw_value — случайное число")
    logger.info("  smoothed_value — скользящее среднее из 10 значений")
    logger.info("  trend — rising/falling/stable")

    # Ждём несколько циклов телеметрии (freq=5s)
    logger.info("Ждём 12 секунд для наблюдения...")
    await asyncio.sleep(12)

    snap = dev.get_telemetry_snapshot()
    logger.info(f"Снимок телеметрии: {snap}")
    logger.info(f"История raw_value: {dev._history}")


# =====================================================================
# Демонстрация: read_telemetry_value из драйвера
# =====================================================================

async def demo_driver_read(app: KamioApp):
    """Показывает чтение телеметрии из драйвера."""
    logger.info("=== Демонстрация: read_telemetry_value из драйвера ===")

    app.register(DriverReadDevice)

    # Создаём драйвер и устройство
    driver = FakeSensorDriver()
    dev = await app.add_device("driver_read_dev", DriverReadDevice, driver=driver)

    # Ручное чтение через read_telemetry_value
    logger.info("Ручное чтение через read_telemetry_value():")
    voltage = await dev.read_telemetry_value("voltage")
    logger.info(f"  voltage: {voltage}")

    current = await dev.read_telemetry_value("current")
    logger.info(f"  current: {current}")

    # read_telemetry_value автоматически извлекает 'data' из ответа драйвера
    # Драйвер возвращает: {"status": "ok", "field": ..., "data": <значение>}
    # Метод возвращает только <значение>
    logger.info("read_telemetry_value() автоматически извлекает ключ 'data' из ответа драйвера")

    # Команда, использующая read_telemetry_value
    logger.info("Вызываем команду read_voltage_manually()...")
    result = await dev.handle_command("read_voltage_manually", {})
    logger.info(f"Результат: {result}")


# =====================================================================
# Демонстрация: группировка полей по частоте
# =====================================================================

async def demo_freq_grouping(app: KamioApp):
    """Показывает группировку полей телеметрии по частоте."""
    logger.info("=== Демонстрация: группировка полей по частоте ===")

    app.register(MultiFreqDevice)
    dev = await app.add_device("multi_freq_dev", MultiFreqDevice)

    logger.info("MultiFreqDevice имеет поля с тремя частотами:")
    logger.info("  5s:  fast_sensor_1, fast_sensor_2")
    logger.info("  30s: slow_sensor_1, slow_sensor_2")
    logger.info("  60s: rare_sensor")

    # TelemetryMixin группирует поля по freq и создаёт один
    # scheduler-цикл на каждую группу. Поля с одинаковой частотой
    # публикуются одним сообщением через publish_telemetry().
    logger.info("")
    logger.info("TelemetryMixin создаёт отдельный scheduler для каждой группы:")
    logger.info("  scheduler #1: [fast_sensor_1, fast_sensor_2] каждые 5s")
    logger.info("  scheduler #2: [slow_sensor_1, slow_sensor_2] каждые 30s")
    logger.info("  scheduler #3: [rare_sensor] каждые 60s")

    # Проверяем поля через get_telemetry()
    telemetry_fields = MultiFreqDevice.get_telemetry()
    for name, field in telemetry_fields.items():
        logger.info(f"  {name}: freq={field.freq!r}, unit={field.unit!r}")


# =====================================================================
# Демонстрация: _get_min_freq из конфига
# =====================================================================

async def demo_min_freq_config(app: KamioApp):
    """Показывает минимальную частоту телеметрии из конфига."""
    logger.info("=== Демонстрация: _get_min_freq из конфига ===")

    dev = app.devices.get("env_sensor_1")
    if not dev:
        dev = app.devices.get("multi_freq_dev")
    if not dev:
        logger.warning("Устройство не найдено")
        return

    # _get_min_freq() читает telemetry_min_freq из конфига приложения
    min_freq = dev._get_min_freq()
    logger.info(f"_get_min_freq() = {min_freq} секунд")

    # Значение по умолчанию — 0.1 сек (100ms)
    # Можно переопределить через Config: telemetry_min_freq = 1.0
    logger.info("По умолчанию telemetry_min_freq = 0.1 (100ms)")
    logger.info("Если freq поля < min_freq, частота повышается до min_freq")

    # Демонстрация: parse_freq для различных форматов
    from kamio.data_fields import parse_freq
    logger.info("\nparse_freq() — парсинг строк частоты:")
    test_freqs = ["500ms", "5s", "10s", "30s", "1m", "2m", 10, 0.5, None, ""]
    for freq in test_freqs:
        seconds = parse_freq(freq)
        logger.info(f"  parse_freq({freq!r:10}) = {seconds}s")


# =====================================================================
# Демонстрация: NaN фильтрация
# =====================================================================

async def demo_nan_filtering(app: KamioApp):
    """Показывает фильтрацию NaN и None в телеметрии."""
    logger.info("=== Демонстрация: NaN фильтрация ===")

    app.register(NaNFilterDevice)
    dev = await app.add_device("nan_filter_dev", NaNFilterDevice)

    logger.info("NaNFilterDevice имеет 3 датчика: sensor_a, sensor_b, sensor_c")
    logger.info("Устанавливаем NaN в sensor_b и None в sensor_c...")

    # Устанавливаем невалидные значения
    await dev.handle_command("set_nan_values", {})
    await asyncio.sleep(0.3)

    # Вызываем handle_telemetry_update вручную для демонстрации фильтрации
    logger.info("Вызываем handle_telemetry_update(['sensor_a', 'sensor_b', 'sensor_c'])...")
    result = await dev.handle_telemetry_update(["sensor_a", "sensor_b", "sensor_c"])
    logger.info(f"Результат (только валидные): {result}")

    logger.info("\nПравила фильтрации handle_telemetry_update:")
    logger.info("  - None → пропускается")
    logger.info("  - NaN (float('nan')) → пропускается (через val != val)")
    logger.info("  - 0, False, '' → включаются (falsy, но валидные)")
    logger.info("  - Пустой результат → публикация пропускается (return None)")


# =====================================================================
# Демонстрация: get_telemetry_snapshot
# =====================================================================

async def demo_telemetry_snapshot(app: KamioApp):
    """Показывает получение снимка телеметрии."""
    logger.info("=== Демонстрация: get_telemetry_snapshot ===")

    env = app.devices.get("env_sensor_1")
    if not env:
        logger.warning("EnvironmentSensor не найден")
        return

    # get_telemetry_snapshot() — только телеметрия
    telemetry_snap = env.get_telemetry_snapshot()
    logger.info(f"get_telemetry_snapshot(): {telemetry_snap}")

    # get_state_snapshot() — только state-поля
    state_snap = env.get_state_snapshot()
    logger.info(f"get_state_snapshot():    {state_snap}")

    # get_config_snapshot() — только config-поля
    config_snap = env.get_config_snapshot()
    logger.info(f"get_config_snapshot():   {config_snap}")

    # get_full_snapshot() — все поля
    full_snap = env.get_full_snapshot()
    logger.info(f"get_full_snapshot():     {full_snap}")

    # Проверяем, что full_snapshot = state + config + telemetry
    merged = {**state_snap, **config_snap, **telemetry_snap}
    assert merged == full_snap, "full_snapshot должен объединять все типы"
    logger.info("✅ get_full_snapshot() = state + config + telemetry")

    # Сравнение типов полей в схеме
    schema = EnvironmentSensor.get_schema()
    for name, info in schema["fields"].items():
        logger.info(f"  {name}: kind={info['kind']}, type={info['type']}, unit={info.get('unit')}")


# =====================================================================
# Расширенная главная функция с дополнительными демонстрациями
# =====================================================================

async def extended_main():
    """Запускает базовую демонстрацию телеметрии плюс все дополнительные секции."""
    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="telemetry_demo")

    # Подписка на выполнение команд для логирования
    app.subscribe_event("device_command_executed", on_command_executed)

    # Регистрируем классы устройств
    app.register(EnvironmentSensor)
    app.register(CO2Sensor)
    app.register(DisabledTelemetryDevice)

    # Запускаем приложение
    await app.start()

    # --- Создаём устройства ---
    driver = FakeSensorDriver()
    env_sensor = await app.add_device("env_sensor_1", EnvironmentSensor, driver=driver)

    co2_driver = FakeSensorDriver()
    co2_sensor = await app.add_device("co2_sensor_1", CO2Sensor, driver=co2_driver)

    disabled_dev = await app.add_device("disabled_1", DisabledTelemetryDevice)

    logger.info("=== Устройства созданы. Телеметрия запущена автоматически. ===")

    # Ждём для наблюдения за автоматической телеметрией
    logger.info("Ждём 12 секунд для наблюдения за автоматической телеметрией...")
    await asyncio.sleep(12)

    # --- Базовая демонстрация (из оригинального main) ---
    logger.info("=== Ручная публикация через publish_telemetry() ===")
    await env_sensor.publish_telemetry({"temperature": 25.3, "humidity": 50.1})
    logger.info("Ручная публикация env_sensor: temperature=25.3°C, humidity=50.1%")
    await asyncio.sleep(1)

    logger.info("=== Снимки телеметрии ===")
    telemetry_snap = env_sensor.get_telemetry_snapshot()
    logger.info(f"EnvironmentSensor телеметрия: {telemetry_snap}")
    full_snap = env_sensor.get_full_snapshot()
    logger.info(f"EnvironmentSensor полный снимок: {full_snap}")

    logger.info("=== Прямое чтение через read_telemetry_value() ===")
    temp_value = await env_sensor.read_telemetry_value("temperature")
    logger.info(f"Прямое чтение temperature: {temp_value}")

    logger.info("=== Устройство с enable_telemetry=False ===")
    await disabled_dev.handle_command("manual_report", {})
    await asyncio.sleep(1)

    logger.info("=== Команда calibrate ===")
    result = await env_sensor.handle_command("calibrate", {})
    logger.info(f"Результат калибровки: {result}")

    # --- Дополнительные демонстрации ---
    await demo_telemetry_validation(app)
    await demo_custom_telemetry_update(app)
    await demo_driver_read(app)
    await demo_freq_grouping(app)
    await demo_min_freq_config(app)
    await demo_nan_filtering(app)
    await demo_telemetry_snapshot(app)

    logger.info("\n=== Завершение ===")
    await app.stop()
    logger.info("Демонстрация завершена")


if __name__ == "__main__":
    asyncio.run(extended_main())
