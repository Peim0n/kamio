"""
01 — Minimal Device
====================

Самый простой пример: одно устройство с одним полем состояния и одной командой.

Запуск::
    python examples/01_minimal_device.py

Что демонстрирует:
    - Создание класса устройства с декларативными полями
    - Команду (@command)
    - Регистрацию и запуск приложения
    - Подключение к MQTT-брокеру
"""
from __future__ import annotations

import asyncio

from kamio import KamioApp, Device, command, config, event, state


class SmartLight(Device):
    """Простейшее устройство: лампочка с вкл/выкл и яркостью."""

    power: bool = state(default=False, writable=True, description="Включена ли лампа")
    brightness: int = state(default=100, min=0, max=255, writable=True, description="Яркость 0-255")

    # Конфигурационное поле — применяется через handle_config()
    location: str = config(default="living_room", description="Расположение лампы")

    # Событийное поле — объявляется через event(), генерируется через emit()
    bulb_replaced = event(description="Событие замены лампы")

    @command
    async def toggle(self):
        """Переключить питание."""
        self.power = not self.power
        return {"power": self.power}

    @command
    async def set_brightness(self, value: int):
        """Установить яркость (0-255)."""
        self.brightness = max(0, min(255, value))
        return {"brightness": self.brightness}


async def main():
    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="minimal_demo")

    # Регистрируем класс устройства
    app.register(SmartLight)

    # Запускаем приложение (подключение к MQTT)
    await app.start()

    # Создаём экземпляр устройства
    light = await app.add_device("living_room", SmartLight)

    # Демонстрация: переключаем лампу
    await light.handle_state({"power": True, "brightness": 200})
    print(f"Light power: {light.power}, brightness: {light.brightness}")

    # Вызываем команду
    result = await light.handle_command("toggle", {})
    print(f"Toggle result: {result}")

    # Останавливаем
    await app.stop()


# =====================================================================
# Демонстрация: конфигурационные поля
# =====================================================================
# Поля config() хранят постоянные настройки (пороги, идентификаторы).
# Они применяются через handle_config() и всегда writable.
# В отличие от state, config-поля не публикуются как state-изменения
# и не запускают правила автоматизации.

async def demo_config_fields(app: KamioApp, light: SmartLight):
    """Показывает работу с конфигурационными полями."""
    print("\n=== Демонстрация: конфигурационные поля ===")

    # Читаем значение по умолчанию
    print(f"location (по умолчанию): {light.location}")

    # Меняем конфигурацию через handle_config()
    applied = await light.handle_config({"location": "bedroom"})
    print(f"handle_config результат: {applied}")
    print(f"location (после изменения): {light.location}")

    # Получаем снимок только конфигурационных полей
    config_snap = light.get_config_snapshot()
    print(f"get_config_snapshot(): {config_snap}")


# =====================================================================
# Демонстрация: события (event)
# =====================================================================
# Поля event() — одноразовые сигналы от устройства (нажатие кнопки,
# тревога, замена лампы). Они не хранятся как постоянное состояние.
# Генерируются через device.emit("event_name", payload).

async def demo_events(app: KamioApp, light: SmartLight):
    """Показывает работу с событийными полями."""
    print("\n=== Демонстрация: события (event) ===")

    # Подписываемся на событие через Event Bus
    received_events = []

    def on_bulb_replaced(data: dict):
        received_events.append(data)
        print(f"  [подписка] Получено событие bulb_replaced: {data}")

    app.subscribe_event("bulb_replaced", on_bulb_replaced)

    # Генерируем событие через emit()
    print("Вызываем light.emit('bulb_replaced', {...})...")
    await light.emit("bulb_replaced", {"reason": "перегорела", "new_wattage": 9})

    # Небольшая пауза, чтобы событие успело обработаться
    await asyncio.sleep(0.3)
    print(f"Получено событий: {len(received_events)}")

    # События видны в схеме устройства
    schema = SmartLight.get_schema()
    print(f"События в схеме: {list(schema['events'].keys())}")


# =====================================================================
# Демонстрация: снимки состояния
# =====================================================================
# Устройство предоставляет три метода для получения снимков:
#   get_state_snapshot()    — только state-поля
#   get_config_snapshot()   — только config-поля
#   get_telemetry_snapshot() — только telemetry-поля
#   get_full_snapshot()     — все поля (state + config + telemetry)

async def demo_snapshots(app: KamioApp, light: SmartLight):
    """Показывает методы получения снимков состояния."""
    print("\n=== Демонстрация: снимки состояния ===")

    # Устанавливаем известные значения
    await light.handle_state({"power": True, "brightness": 180})
    await light.handle_config({"location": "kitchen"})

    state_snap = light.get_state_snapshot()
    print(f"get_state_snapshot():    {state_snap}")

    config_snap = light.get_config_snapshot()
    print(f"get_config_snapshot():   {config_snap}")

    full_snap = light.get_full_snapshot()
    print(f"get_full_snapshot():     {full_snap}")

    # get_full_snapshot объединяет все типы полей в один словарь
    assert "power" in full_snap      # state
    assert "location" in full_snap   # config


# =====================================================================
# Демонстрация: handle_command с параметрами
# =====================================================================
# Команды могут принимать параметры через params dict.
# handle_command("command_name", {"param": value}) вызывает метод,
# передавая params как **kwargs.

async def demo_command_with_params(app: KamioApp, light: SmartLight):
    """Показывает вызов команд с параметрами."""
    print("\n=== Демонстрация: handle_command с параметрами ===")

    # Команда set_brightness принимает параметр value
    result = await light.handle_command("set_brightness", {"value": 42})
    print(f"set_brightness(42): результат={result}, brightness={light.brightness}")

    # Команда toggle не принимает параметров — передаём пустой dict
    result = await light.handle_command("toggle", {})
    print(f"toggle(): результат={result}, power={light.power}")

    # Авто-маршрутизация: set_<field> для writable state-полей
    # handle_command("set_power", {"value": True}) автоматически
    # вызывает handle_state({"power": True}) — совместимость с HA
    result = await light.handle_command("set_power", {"value": True})
    print(f"set_power(True) [авто-маршрут]: power={light.power}")

    # Список всех зарегистрированных команд
    commands = SmartLight.get_commands()
    print(f"Зарегистрированные команды: {list(commands.keys())}")


# =====================================================================
# Демонстрация: валидация
# =====================================================================
# Поля state() поддерживают валидацию через min, max и choices.
# При нарушении ограничений выбрасывается ValueError.
# Это работает как при прямом присваивании, так и через handle_state().

async def demo_validation(app: KamioApp, light: SmartLight):
    """Показывает валидацию min/max/choices."""
    print("\n=== Демонстрация: валидация ===")

    # --- min/max валидация ---
    # brightness имеет min=0, max=255
    try:
        await light.handle_state({"brightness": 300})  # > max
        print("ОШИБКА: должно было выбросить ValueError")
    except ValueError as e:
        print(f"brightness=300 отклонено: {e}")

    try:
        light.brightness = -10  # < min (прямое присваивание)
        print("ОШИБКА: должно было выбросить ValueError")
    except ValueError as e:
        print(f"brightness=-10 отклонено: {e}")

    # Корректное значение проходит без ошибок
    await light.handle_state({"brightness": 128})
    print(f"brightness=128 принято: {light.brightness}")

    # --- choices валидация ---
    # Добавим устройство с choices для демонстрации
    class ModeDevice(Device):
        mode: str = state(default="auto", choices=("auto", "manual", "off"), writable=True)

    app.register(ModeDevice)
    mode_dev = await app.add_device("mode_demo", ModeDevice)

    try:
        await mode_dev.handle_state({"mode": "invalid"})
        print("ОШИБКА: должно было выбросить ValueError")
    except ValueError as e:
        print(f"mode='invalid' отклонено: {e}")

    await mode_dev.handle_state({"mode": "manual"})
    print(f"mode='manual' принято: {mode_dev.mode}")


# =====================================================================
# Демонстрация: _get_field_value
# =====================================================================
# Внутренний метод _get_field_value(field_name) возвращает текущее
# значение поля по имени. Если поле не найдено — возвращает None.
# Для state/config полей при отсутствии значения возвращается default.

async def demo_get_field_value(app: KamioApp, light: SmartLight):
    """Показывает внутренний метод _get_field_value."""
    print("\n=== Демонстрация: _get_field_value ===")

    # Чтение существующих полей
    power_val = light._get_field_value("power")
    print(f"_get_field_value('power'):     {power_val}")

    brightness_val = light._get_field_value("brightness")
    print(f"_get_field_value('brightness'): {brightness_val}")

    config_val = light._get_field_value("location")
    print(f"_get_field_value('location'):   {config_val}")

    # Несуществующее поле возвращает None
    missing_val = light._get_field_value("nonexistent_field")
    print(f"_get_field_value('nonexistent'): {missing_val}")

    # Метод использует Field.default для state/config, если значение
    # ещё не было установлено
    print(f"Поле 'power' имеет default={SmartLight.Kamio_FIELDS['power'].default}")


# =====================================================================
# Расширенная главная функция с дополнительными демонстрациями
# =====================================================================

async def extended_main():
    """Запускает базовую демонстрацию плюс все дополнительные секции."""
    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="minimal_demo")

    app.register(SmartLight)
    await app.start()

    light = await app.add_device("living_room", SmartLight)

    # Базовая демонстрация (из оригинального main)
    await light.handle_state({"power": True, "brightness": 200})
    print(f"Light power: {light.power}, brightness: {light.brightness}")

    result = await light.handle_command("toggle", {})
    print(f"Toggle result: {result}")

    # --- Дополнительные демонстрации ---
    await demo_config_fields(app, light)
    await demo_events(app, light)
    await demo_snapshots(app, light)
    await demo_command_with_params(app, light)
    await demo_validation(app, light)
    await demo_get_field_value(app, light)

    await app.stop()


if __name__ == "__main__":
    asyncio.run(extended_main())
