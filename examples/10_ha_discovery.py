"""
10 — Home Assistant MQTT Discovery
===================================

Демонстрирует интеграцию Kamio с Home Assistant через MQTT Discovery:
    - app.enable_ha_discovery() — включение автоматического обнаружения
    - HADiscovery.announce() — вызывается автоматически при добавлении устройства
    - HADiscovery.clear() — вызывается автоматически при удалении устройства
    - Как HA сопоставляет поля Kamio с компонентами (sensor, switch,
      binary_sensor, number, select, text)

Запуск::
    python examples/10_ha_discovery.py

Предварительно:
    1. Запустите MQTT-брокер на localhost:1883
       (например, ``docker run -p 1883:1883 eclipse-mosquitto``)
    2. (Опционально) Запустите Home Assistant с настроенным MQTT integration.
       HA автоматически обнаружит устройства после запуска примера.

Что произойдёт:
    - При добавлении каждого устройства Kamio опубликует discovery-сообщения
      в топики homeassistant/<component>/<device_id>/<field>/config
    - Home Assistant автоматически создаст сущности (entities) для каждого поля
    - При удалении устройства discovery-записи будут очищены (пустой retained payload)

Маппинг полей Kamio -> компоненты HA:
    - telemetry             -> sensor
    - state (bool, writable) -> switch
    - state (bool, ro)       -> binary_sensor
    - state (int/float, writable, без choices) -> number
    - state (str, writable, с choices) -> select
    - state (str, writable, без choices) -> text
    - state (ro, не bool)    -> sensor
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

from kamio import KamioApp, Device, command, config, event, state, telemetry

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("ha_discovery_demo")


# =====================================================================
# Устройство 1: умная лампа (switch + number)
# =====================================================================

class SmartBulb(Device):
    """Умная лампа с питанием (switch) и яркостью (number).

    HA mapping:
        - power (bool, writable)   -> switch
        - brightness (int, writable) -> number
    """

    power: bool = state(default=False, writable=True, description="Питание лампы")
    brightness: int = state(
        default=100, min=0, max=255, writable=True, description="Яркость 0-255"
    )

    @command
    async def toggle(self):
        """Переключить питание."""
        self.power = not self.power
        return {"power": self.power}


# =====================================================================
# Устройство 2: датчик среды (sensor)
# =====================================================================

class EnvironmentMonitor(Device):
    """Датчик температуры, влажности и качества воздуха.

    HA mapping:
        - temperature (telemetry) -> sensor (с unit_of_measurement)
        - humidity (telemetry)    -> sensor
        - co2 (telemetry)         -> sensor
    """

    temperature: float = telemetry(
        default=22.0, unit="°C", freq="10s", description="Температура"
    )
    humidity: float = telemetry(
        default=45.0, unit="%", freq="10s", description="Влажность"
    )
    co2: float = telemetry(
        default=420.0, unit="ppm", freq="15s", description="CO2"
    )


# =====================================================================
# Устройство 3: термостат (select + number + binary_sensor)
# =====================================================================

class Thermostat(Device):
    """Термостат с режимом (select) и целевой температурой (number).

    HA mapping:
        - mode (str, writable, choices) -> select
        - target_temp (float, writable)  -> number
        - heating (bool, read-only)      -> binary_sensor
        - current_temp (telemetry)       -> sensor
    """

    mode: str = state(
        default="auto",
        choices=("auto", "manual", "off"),
        writable=True,
        description="Режим работы",
    )
    target_temp: float = state(
        default=22.0, min=10.0, max=35.0, writable=True, description="Целевая температура"
    )
    heating: bool = state(
        default=False, writable=False, description="Активен ли нагрев"
    )
    current_temp: float = telemetry(
        default=22.0, unit="°C", freq="10s", description="Текущая температура"
    )

    @command
    async def set_mode(self, value: str):
        """Установить режим (auto/manual/off)."""
        self.mode = value
        return {"mode": self.mode}


# =====================================================================
# Устройство 4: текстовая панель (text)
# =====================================================================

class MessageBoard(Device):
    """Текстовая панель для отображения сообщений.

    HA mapping:
        - message (str, writable, без choices) -> text
    """

    message: str = state(
        default="Привет, Home Assistant!",
        writable=True,
        description="Текст на панели",
    )


# =====================================================================
# Подписчики на события для логирования
# =====================================================================

async def on_device_added(data: Dict[str, Any]) -> None:
    """Логирование добавления устройства (встроенное событие).

    При включённом HA Discovery, announce() вызывается автоматически
    после публикации этого события.
    """
    logger.info(
        f"[device_added] {data['device_id']} (type={data['device_type']}) "
        f"— HA discovery будет опубликован автоматически"
    )


async def on_device_removed(data: Dict[str, Any]) -> None:
    """Логирование удаления устройства.

    При включённом HA Discovery, clear() вызывается автоматически
    перед публикацией этого события, чтобы HA убрал сущности.
    """
    logger.info(
        f"[device_removed] {data['device_id']} — HA discovery очищен"
    )


# =====================================================================
# Главный цикл
# =====================================================================

async def main():
    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="ha_discovery_demo")

    # --- Включаем Home Assistant MQTT Discovery ---
    # prefix="homeassistant" — стандартный префикс HA для discovery-топиков.
    # Discovery-сообщения публикуются с retain=True, поэтому HA подхватит
    # их даже после перезапуска брокера.
    app.enable_ha_discovery(prefix="homeassistant")
    logger.info("HA Discovery включён (prefix='homeassistant')")

    # --- Подписка на события для логирования ---
    app.subscribe_event("device_added", on_device_added)
    app.subscribe_event("device_removed", on_device_removed)

    # --- Регистрируем классы устройств ---
    app.register(SmartBulb)
    app.register(EnvironmentMonitor)
    app.register(Thermostat)
    app.register(MessageBoard)

    # --- Запуск приложения ---
    await app.start()

    # --- Создаём устройства ---
    # При каждом add_device с включённым HA Discovery:
    #   1. Устройство создаётся и запускается
    #   2. Срабатывает хук on_device_added
    #   3. Публикуется событие device_added
    #   4. HADiscovery.announce() публикует discovery-сообщения
    #      для каждого поля устройства в топики:
    #      homeassistant/<component>/<device_id>/<field>/config

    logger.info("=== Добавление устройств (HA Discovery публикуется автоматически) ===")

    bulb = await app.add_device("living_room_bulb", SmartBulb)
    logger.info("SmartBulb: power(switch), brightness(number)")

    monitor = await app.add_device("env_monitor_1", EnvironmentMonitor)
    logger.info("EnvironmentMonitor: temperature(sensor), humidity(sensor), co2(sensor)")

    thermostat = await app.add_device("thermostat_1", Thermostat)
    logger.info("Thermostat: mode(select), target_temp(number), heating(binary_sensor), current_temp(sensor)")

    board = await app.add_device("message_board_1", MessageBoard)
    logger.info("MessageBoard: message(text)")

    # --- Демонстрация: HA может управлять устройствами ---
    # HA отправляет команды в топик Kamio/v1/<device_id>/sc
    # Для bool полей: payload_on / payload_off (JSON с set_<field>)
    # Для других: command_template (JSON шаблон с set_<field>)
    logger.info("=== Демонстрация управления через state changes ===")
    await asyncio.sleep(1)

    # Изменяем состояние — HA увидит обновление в state_topic
    await bulb.handle_state({"power": True, "brightness": 180})
    logger.info(f"Bulb: power={bulb.power}, brightness={bulb.brightness}")

    await thermostat.handle_state({"mode": "manual", "target_temp": 24.0})
    logger.info(f"Thermostat: mode={thermostat.mode}, target_temp={thermostat.target_temp}")

    await asyncio.sleep(1)

    # --- Демонстрация ручного вызова announce ---
    # announce() обычно вызывается автоматически, но можно вызвать и вручную.
    # Например, если поля устройства изменились и нужно обновить discovery.
    logger.info("=== Ручной вызов HADiscovery.announce() ===")
    if app.ha_discovery:
        await app.ha_discovery.announce(bulb)
        logger.info(f"Повторный announce для {bulb.node.device_id}")

    await asyncio.sleep(1)

    # --- Дополнительные демонстрации ---
    await demo_custom_discovery_prefix(app, bulb)
    await demo_clear_for_device(app, thermostat)
    await demo_select_component(app, thermostat)
    await demo_unmapped_field(app)

    # --- Демонстрация удаления устройства (clear) ---
    # При remove_device с включённым HA Discovery:
    #   1. Срабатывает хук on_device_removed
    #   2. HADiscovery.clear() публикует пустой retained payload
    #      в discovery-топики, чтобы HA убрал сущности
    #   3. Публикуется событие device_removed
    #   4. Устройство останавливается и удаляется из реестра
    logger.info("=== Удаление устройства (HA Discovery очищается автоматически) ===")
    await app.remove_device("message_board_1")
    logger.info("MessageBoard удалён — HA уберёт text сущность")

    await asyncio.sleep(1)

    # --- Демонстрация disable_ha_discovery (подробно) ---
    await demo_disable_ha_discovery(app)

    logger.info("=== Завершение ===")
    await asyncio.sleep(2)
    await app.stop()


# =====================================================================
# Демонстрация: custom discovery_prefix
# =====================================================================

async def demo_custom_discovery_prefix(app, bulb):
    """Показывает использование кастомного префикса для HA Discovery.

    По умолчанию discovery_prefix = "homeassistant" (стандартный HA префикс).
    Можно изменить префикс при создании HADiscovery или через app.enable_ha_discovery().

    Нестандартный префикс полезен, когда:
    - На одном брокере работает несколько HA инстансов
    - Используется нестандартная конфигурация HA
    - Нужно отделить discovery-топики разных систем
    """
    logger.info("=== Демонстрация: custom discovery_prefix ===")

    # Текущий префикс
    if app.ha_discovery:
        logger.info(f"Текущий discovery_prefix: {app.ha_discovery.discovery_prefix!r}")
        logger.info(f"Discovery-топики имеют вид: {app.ha_discovery.discovery_prefix}/<component>/<device_id>/<field>/config")

    # Пример создания HADiscovery с кастомным префиксом (без app):
    from kamio.discovery import HADiscovery
    custom_ha = HADiscovery(discovery_prefix="custom_ha")
    logger.info(f"Кастомный HADiscovery prefix: {custom_ha.discovery_prefix!r}")
    logger.info(f"  → топики: custom_ha/<component>/<device_id>/<field>/config")

    # В реальном коде:
    #   app.enable_ha_discovery(prefix="my_homeassistant")
    # или:
    #   app.ha_discovery = HADiscovery(discovery_prefix="my_ha")


# =====================================================================
# Демонстрация: clear() для конкретного устройства
# =====================================================================

async def demo_clear_for_device(app, thermostat):
    """Показывает ручной вызов HADiscovery.clear() для конкретного устройства.

    clear() публикует пустой retained payload в discovery-топики
    устройства, чтобы Home Assistant убрал соответствующие сущности.

    Обычно clear() вызывается автоматически при remove_device(),
    но можно вызвать и вручную — например, если нужно скрыть
    устройство из HA без удаления из приложения.
    """
    logger.info("\n=== Демонстрация: clear() для конкретного устройства ===")

    if not app.ha_discovery:
        logger.warning("HA Discovery не включён")
        return

    device_id = thermostat.node.device_id
    logger.info(f"Устройство: {device_id}")
    logger.info(f"Поля: {list(thermostat.Kamio_FIELDS.keys())}")

    # Ручной вызов clear() — публикует пустой retained payload
    # в каждый discovery-топик устройства
    logger.info("Вызываем app.ha_discovery.clear(thermostat)...")
    await app.ha_discovery.clear(thermostat)
    logger.info(f"Discovery-записи для {device_id} очищены")
    logger.info("HA уберёт сущности (mode, target_temp, heating, current_temp)")

    # Устройство остаётся в приложении и работает — только HA discovery удалён
    logger.info(f"Устройство всё ещё в app: {'thermostat_1' in app.devices}")

    # Повторный announce() восстановит discovery-записи
    logger.info("Восстанавливаем через announce()...")
    await app.ha_discovery.announce(thermostat)
    logger.info(f"Discovery-записи для {device_id} восстановлены")


# =====================================================================
# Демонстрация: field с choices → select
# =====================================================================

async def demo_select_component(app, thermostat):
    """Подробно показывает маппинг поля с choices на HA компонент select.

    Когда state-поле имеет параметр choices (кортеж допустимых значений)
    и writable=True, HADiscovery._map_to_ha_component() возвращает "select".

    HA отображает select как выпадающий список с заданными опциями.
    """
    logger.info("\n=== Демонстрация: field с choices → select ===")

    # Проверяем поле mode у Thermostat
    mode_field = thermostat.Kamio_FIELDS.get("mode")
    if mode_field:
        logger.info(f"Поле 'mode':")
        logger.info(f"  kind: {mode_field.kind}")
        logger.info(f"  python_type: {mode_field.python_type}")
        logger.info(f"  writable: {mode_field.writable}")
        logger.info(f"  choices: {mode_field.choices}")

        # Проверяем маппинг
        if app.ha_discovery:
            component = app.ha_discovery._map_to_ha_component(mode_field)
            logger.info(f"  HA component: {component!r} (ожидается 'select')")

    # Изменяем mode — HA увидит обновление в state_topic
    logger.info("Устанавливаем mode='manual'...")
    await thermostat.handle_state({"mode": "manual"})
    logger.info(f"  thermostat.mode = {thermostat.mode!r}")

    logger.info("Устанавливаем mode='off'...")
    await thermostat.handle_state({"mode": "off"})
    logger.info(f"  thermostat.mode = {thermostat.mode!r}")

    # Попытка установить недопустимое значение — вызовет ValueError
    logger.info("Попытка установить mode='invalid' (ожидается ValueError)...")
    try:
        await thermostat.handle_state({"mode": "invalid"})
    except ValueError as e:
        logger.info(f"  ValueError перехвачен: {e}")


# =====================================================================
# Демонстрация: поле без маппинга (event-поля)
# =====================================================================

async def demo_unmapped_field(app):
    """Показывает поведение для полей, которые не маппятся на HA компоненты.

    HADiscovery._map_to_ha_component() возвращает "" (пустую строку)
    для полей, которые не могут быть отображены на HA компонент.
    Такие поля пропускаются при announce() и clear().

    Например, config-поля и event-поля не маппятся на HA компоненты.
    """
    logger.info("\n=== Демонстрация: поле без маппинга ===")

    from kamio import Device, config, event, state, telemetry

    class DeviceWithUnmappedFields(Device):
        """Устройство с полями, которые не маппятся на HA."""

        # config-поле — не маппится на HA компонент
        poll_interval: float = config(default=5.0, description="Интервал опроса")

        # event-поле — не маппится на HA компонент
        alert: str = event(description="Событие тревоги")

        # state-поле — маппится (switch)
        power: bool = state(default=False, writable=True)

        # telemetry-поле — маппится (sensor)
        voltage: float = telemetry(default=220.0, unit="V", freq="10s")

    # Создаём экземпляр (без MQTT-узла, только для проверки маппинга)
    device = DeviceWithUnmappedFields()

    if app.ha_discovery:
        for name, field in device.Kamio_FIELDS.items():
            component = app.ha_discovery._map_to_ha_component(field)
            if component:
                logger.info(f"  {name} (kind={field.kind}) → HA component: {component!r}")
            else:
                logger.info(f"  {name} (kind={field.kind}) → БЕЗ МАППИНГА (пропускается при announce)")


# =====================================================================
# Демонстрация: disable_ha_discovery — отключение
# =====================================================================

async def demo_disable_ha_discovery(app):
    """Подробно показывает отключение HA Discovery.

    disable_ha_discovery() прекращает автоматические announce/clear
    при добавлении/удалении устройств. Уже опубликованные discovery-
    записи остаются в брокере (retain=True) до явного clear().

    После отключения:
    - Новые устройства не анонсируются в HA
    - Удаление устройств не очищает discovery-записи
    - app.ha_discovery устанавливается в None
    """
    logger.info("\n=== Демонстрация: disable_ha_discovery ===")

    # Проверяем, что HA Discovery включён
    logger.info(f"HA Discovery до отключения: {app.ha_discovery is not None}")

    # Отключаем
    app.disable_ha_discovery()
    logger.info("disable_ha_discovery() вызван")

    # Проверяем, что отключено
    logger.info(f"HA Discovery после отключения: {app.ha_discovery is not None}")
    logger.info("  → новые устройства не будут анонсированы в HA")
    logger.info("  → удаление устройств не очистит discovery-записи")
    logger.info("  → уже опубликованные записи остаются в брокере (retain=True)")

    # Можно включить снова — enable_ha_discovery анонсирует все существующие устройства
    logger.info("Включаем снова для демонстрации...")
    app.enable_ha_discovery(prefix="homeassistant")
    logger.info(f"HA Discovery после повторного включения: {app.ha_discovery is not None}")
    logger.info("  → при следующем start() все устройства будут анонсированы")

    # Окончательно отключаем
    app.disable_ha_discovery()
    logger.info("Окончательно отключено")


if __name__ == "__main__":
    asyncio.run(main())
