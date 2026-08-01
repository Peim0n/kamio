"""
04 — Правила автоматизации
===========================

Демонстрирует все типы правил автоматизации в kamio: от простых реакций
на изменения полей до периодических задач и взаимодействия между
устройствами.

Запуск (требуется MQTT-брокер на localhost:1883)::

    python examples/04_rules_automation.py

Что демонстрирует:
    - Правило на уровне устройства: @rule(fields=[...])
    - Правило на уровне устройства с несколькими полями
    - Правило на уровне приложения: @app.rule(device=..., fields=[...])
    - Периодическое правило: @app.rule(interval=N)
    - Периодическое правило с запуском при старте: run_on_start=True
    - RuleEvent.get() и event.data — доступ к данным события
    - Взаимодействие с другими устройствами через app.devices
    - Включение/выключение правил во время выполнения
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Optional

from kamio import KamioApp, Device, RuleEvent, command, rule, state, telemetry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("rules_demo")


# =====================================================================
# Устройства
# =====================================================================

class SmartLight(Device):
    """Умная лампа с питанием, яркостью и режимом."""

    power: bool = state(default=False, writable=True, description="Питание вкл/выкл")
    brightness: int = state(default=100, min=0, max=255, writable=True, description="Яркость 0-255")
    mode: str = state(default="normal", choices=("normal", "night", "party"), writable=True)

    # --- Правило на уровне устройства: реакция на одно поле ---
    @rule(fields=["power"])
    async def on_power_toggle(self, event: RuleEvent, app: KamioApp):
        """Срабатывает при изменении поля power.

        event.data содержит словарь изменённых полей:
        {"power": True} или {"power": False}.
        event.get("power") — удобный метод доступа к значению.
        """
        is_on = event.get("power", False)
        device_id = self.node.device_id if self.node else "?"
        logger.info(f"💡 [правило устройства] Лампа '{device_id}' "
                    f"{'ВКЛ' if is_on else 'ВЫКЛ'} (event.data={event.data})")

    # --- Правило на уровне устройства: реакция на несколько полей ---
    @rule(fields=["brightness", "mode"])
    async def on_brightness_or_mode_change(self, event: RuleEvent, app: KamioApp):
        """Срабатывает при изменении brightness ИЛИ mode.

        Проверяем event.data, чтобы понять, какое именно поле изменилось.
        Правило с fields=["brightness", "mode"] сработает, если в
        обновлении присутствует хотя бы одно из перечисленных полей.
        """
        device_id = self.node.device_id if self.node else "?"

        if "brightness" in event.data:
            new_brightness = event.get("brightness")
            logger.info(f"💡 [правило устройства] Яркость '{device_id}' "
                        f"изменилась на {new_brightness}")

        if "mode" in event.data:
            new_mode = event.get("mode")
            logger.info(f"💡 [правило устройства] Режим '{device_id}' "
                        f"изменился на '{new_mode}'")

            # Реагируем на режим "night" — приглушаем яркость
            if new_mode == "night":
                self.brightness = 10
                logger.info(f"🌙 [правило устройства] Ночной режим: "
                            f"яркость снижена до {self.brightness}")


class MotionSensor(Device):
    """Датчик движения с телеметрией и событием."""

    motion: bool = state(default=False, writable=False, description="Движение обнаружено")
    battery_level: float = telemetry(default=100.0, unit="%", freq="30s", description="Заряд батареи")
    last_motion_time: Optional[str] = state(default=None, writable=False)

    @command
    async def trigger_motion(self):
        """Имитировать обнаружение движения (для тестирования)."""
        self.motion = True
        self.last_motion_time = datetime.now().isoformat()
        logger.info(f"🚶 Датчик '{self.node.device_id}' обнаружил движение")

        # Автоматический сброс через 3 секунды
        async def _reset():
            await asyncio.sleep(3)
            self.motion = False
            logger.info(f"🚶 Датчик '{self.node.device_id}' движение сброшено")

        self.create_task(_reset(), name=f"motion_reset_{self.node.device_id}")


class EnergyMeter(Device):
    """Счётчик энергии с телеметрией потребления."""

    power_consumption: float = telemetry(
        default=0.0, unit="W", freq="5s", description="Текущее потребление"
    )
    total_energy: float = telemetry(
        default=0.0, unit="Wh", freq="10s", description="Общее потребление"
    )


# =====================================================================
# Правила на уровне приложения
# =====================================================================

# --- Правило: реакция на изменение поля устройства определённого класса ---
# @app.rule(device=DeviceClass, fields=[...]) срабатывает, когда любое
# устройство указанного класса изменяет перечисленные поля.
# Функция получает (event: RuleEvent, app: KamioApp).


def setup_app_rules(app: KamioApp):
    """Регистрирует все правила на уровне приложения.

    Вызывается до app.start(), чтобы правила были активны с момента запуска.
    """

    # --- Правило: включать свет при обнаружении движения ---
    @app.rule(device=MotionSensor, fields=["motion"])
    async def motion_to_light(event: RuleEvent, app: KamioApp):
        """Включает лампу при движении, выключает при отсутствии.

        event.device_id — ID устройства, вызвавшего правило.
        event.data — словарь изменённых полей.
        app.devices — все зарегистрированные устройства {id: Device}.
        """
        motion_detected = event.get("motion", False)
        sensor_id = event.device_id
        logger.info(f"🚶 [правило приложения] Датчик '{sensor_id}' "
                    f"motion={motion_detected}")

        if motion_detected:
            # Ищем все лампы и включаем их
            for dev_id, device in app.devices.items():
                if isinstance(device, SmartLight) and not device.power:
                    await device.handle_state({"power": True})
                    logger.info(f"💡 [правило приложения] "
                                f"Лампа '{dev_id}' включена по движению")

    # --- Периодическое правило: проверка низкого заряда батареи ---
    @app.rule(interval=10, description="Проверка заряда батарей каждые 10 сек")
    async def check_battery_levels(event: RuleEvent, app: KamioApp):
        """Периодическое правило (interval=10 секунд).

        Для interval-правил:
        - event.kind == "interval"
        - event.device_id == None (нет конкретного устройства)
        - event.data содержит снимок всех состояний через app.state
        """
        for dev_id, device in app.devices.items():
            if isinstance(device, MotionSensor):
                battery = device.battery_level
                if battery < 20.0:
                    logger.warning(
                        f"🔋 [периодическое правило] Низкий заряд батареи "
                        f"на '{dev_id}': {battery:.1f}%"
                    )

    # --- Периодическое правило с запуском при старте ---
    @app.rule(
        interval=15,
        run_on_start=True,
        description="Отчёт по энергопотреблению (запускается сразу при старте)",
    )
    async def energy_report(event: RuleEvent, app: KamioApp):
        """Периодическое правило с run_on_start=True.

        Выполнится немедленно при старте приложения, а затем каждые
        15 секунд. Удобно для правил, которые должны сделать первую
        проверку сразу, а не ждать один интервал.
        """
        for dev_id, device in app.devices.items():
            if isinstance(device, EnergyMeter):
                power = device.power_consumption
                total = device.total_energy
                logger.info(
                    f"⚡ [отчёт энергии] '{dev_id}': "
                    f"потребление={power:.1f}W, всего={total:.1f}Wh"
                )

    # --- Правило: мониторинг всех изменений состояния (без фильтра по полям) ---
    @app.rule(device=SmartLight, description="Логирование всех изменений ламп")
    async def log_light_changes(event: RuleEvent, app: KamioApp):
        """Срабатывает при любом изменении состояния SmartLight.

        fields=None (по умолчанию) означает, что правило сработает
        при любом обновлении состояния устройства данного класса.
        """
        if event.data:
            logger.info(
                f"📝 [мониторинг] Лампа '{event.device_id}' "
                f"изменила: {event.data}"
            )


# =====================================================================
# Демонстрация включения/выключения правил
# =====================================================================

async def demo_rule_enable_disable(app: KamioApp):
    """
    Показывает, как включать и выключать правила во время выполнения.

    Каждое правило имеет атрибут .enabled (bool).
    Установка enabled=False приостанавливает срабатывание правила.
    Установка enabled=True возобновляет.
    """
    logger.info("=== Демонстрация: включение/выключение правил ===")

    # Получаем список всех зарегистрированных правил
    all_rules = app.rules.rules
    logger.info(f"Зарегистрировано правил: {len(all_rules)}")

    # Находим правило "log_light_changes" по описанию
    target_rule = None
    for r in all_rules:
        desc = r.description or ""
        if "Логирование всех изменений ламп" in desc:
            target_rule = r
            break

    if target_rule:
        # Выключаем правило
        target_rule.enabled = False
        logger.info("Правило 'log_light_changes' ВЫКЛЮЧЕНО")

        # Изменяем состояние лампы — правило не сработает
        light = app.devices.get("living_room_light")
        if light:
            await light.handle_state({"brightness": 150})
            await asyncio.sleep(0.5)
            logger.info("Изменение внесено, но правило не сработало (выключено)")

        # Включаем правило обратно
        target_rule.enabled = True
        logger.info("Правило 'log_light_changes' ВКЛЮЧЕНО снова")

        # Снова изменяем состояние — теперь правило сработает
        if light:
            await light.handle_state({"brightness": 200})
            await asyncio.sleep(0.5)

    logger.info("")


# =====================================================================
# Основная функция
# =====================================================================

async def main():
    logger.info("=== Демонстрация правил автоматизации kamio ===\n")

    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="rules_demo")

    # Регистрируем правила на уровне приложения ДО запуска
    setup_app_rules(app)

    # Создаём устройства
    light = await app.add_device("living_room_light", SmartLight)
    motion = await app.add_device("hallway_motion", MotionSensor)
    meter = await app.add_device("main_meter", EnergyMeter)

    # Имитируем значения телеметрии для демонстрации
    motion.battery_level = 85.0
    meter.power_consumption = 42.5
    meter.total_energy = 1500.0

    # Запускаем приложение
    await app.start()

    # --- Демонстрация 1: правило на уровне устройства (одно поле) ---
    logger.info("\n--- Тест 1: правило @rule(fields=['power']) ---")
    await light.handle_state({"power": True})
    await asyncio.sleep(0.5)

    await light.handle_state({"power": False})
    await asyncio.sleep(0.5)

    # --- Демонстрация 2: правило на несколько полей ---
    logger.info("\n--- Тест 2: правило @rule(fields=['brightness', 'mode']) ---")
    await light.handle_state({"brightness": 180})
    await asyncio.sleep(0.5)

    await light.handle_state({"mode": "night"})
    await asyncio.sleep(0.5)

    # --- Демонстрация 3: правило приложения — движение → свет ---
    logger.info("\n--- Тест 3: движение включает свет ---")
    await motion.handle_command("trigger_motion", {})
    await asyncio.sleep(1.0)

    # --- Демонстрация 4: включение/выключение правил ---
    logger.info("\n--- Тест 4: управление правилами ---")
    await demo_rule_enable_disable(app)

    # --- Демонстрация 5: периодические правила ---
    logger.info("\n--- Тест 5: периодические правила (ждём 16 сек) ---")
    logger.info("Ожидание срабатывания interval-правил...")
    logger.info("(energy_report с run_on_start=True уже сработал при старте)")
    await asyncio.sleep(16)

    # --- Останавливаем ---
    logger.info("\n--- Завершение ---")
    await app.stop()
    logger.info("Демонстрация завершена")


# =====================================================================
# Демонстрация: правило с 0 параметров
# =====================================================================
# RuleEngine определяет количество параметров функции через
# inspect.signature() и вызывает с нужным числом аргументов:
#   0 параметров → fn()
#   1 параметр   → fn(event)
#   2 параметра  → fn(event, app)
# Это позволяет писать максимально лаконичные правила, когда
# доступ к event или app не нужен.

async def demo_zero_param_rule(app: KamioApp):
    """Показывает правило без параметров."""
    logger.info("=== Демонстрация: правило с 0 параметров ===")

    # Правило без параметров — вызывается как fn()
    @app.rule(interval=5, description="Правило без параметров (каждые 5 сек)")
    async def heartbeat():
        """Периодическое правило, не требующее event или app.

        Вызывается как heartbeat() — без аргументов.
        Удобно для простых периодических задач (heartbeat, очистка).
        """
        logger.info("  [0 параметров] heartbeat() вызван — без аргументов")

    logger.info("Правило 'heartbeat' зарегистрировано (0 параметров)")
    logger.info("Ожидание срабатывания (5 сек)...")
    await asyncio.sleep(6)


# =====================================================================
# Демонстрация: правило с 1 параметром (только event)
# =====================================================================
# Функция с 1 параметром получает только RuleEvent.
# Доступ к app отсутствует — подходит для правил, которым нужны
# только данные события, но не глобальное состояние приложения.

async def demo_one_param_rule(app: KamioApp):
    """Показывает правило с одним параметром (event)."""
    logger.info("=== Демонстрация: правило с 1 параметром (только event) ===")

    light = app.devices.get("living_room_light")
    if not light:
        return

    # Правило с 1 параметром — вызывается как fn(event)
    @app.rule(device=SmartLight, fields=["power"], description="Правило с 1 параметром")
    async def on_power_simple(event: RuleEvent):
        """Правило, получающее только event.

        Вызывается как on_power_simple(event).
        event.data содержит изменённые поля.
        event.get("power") — удобный доступ к значению.
        """
        is_on = event.get("power", False)
        logger.info(f"  [1 параметр] on_power_simple(event): power={is_on}")

    logger.info("Правило 'on_power_simple' зарегистрировано (1 параметр)")

    # Триггерим изменение
    await light.handle_state({"power": True})
    await asyncio.sleep(0.5)
    await light.handle_state({"power": False})
    await asyncio.sleep(0.5)


# =====================================================================
# Демонстрация: правило с 2 параметрами (event, app)
# =====================================================================
# Функция с 2 параметрами получает (RuleEvent, KamioApp).
# Это стандартная сигнатура, используемая в большинстве примеров.
# Доступ к app позволяет взаимодействовать с другими устройствами.

async def demo_two_param_rule(app: KamioApp):
    """Показывает правило с двумя параметрами (event, app)."""
    logger.info("=== Демонстрация: правило с 2 параметрами (event, app) ===")

    light = app.devices.get("living_room_light")
    if not light:
        return

    # Правило с 2 параметрами — вызывается как fn(event, app)
    @app.rule(device=SmartLight, fields=["brightness"], description="Правило с 2 параметрами")
    async def on_brightness_full(event: RuleEvent, app: KamioApp):
        """Правило, получающее event и app.

        Вызывается как on_brightness_full(event, app).
        event.device_id — ID устройства, вызвавшего правило.
        app.devices — доступ ко всем устройствам.
        """
        brightness = event.get("brightness")
        device_id = event.device_id
        device_count = len(app.devices)
        logger.info(
            f"  [2 параметра] on_brightness_full(event, app): "
            f"device={device_id}, brightness={brightness}, "
            f"всего устройств={device_count}"
        )

    logger.info("Правило 'on_brightness_full' зарегистрировано (2 параметра)")

    # Триггерим изменение
    await light.handle_state({"brightness": 99})
    await asyncio.sleep(0.5)


# =====================================================================
# Демонстрация: run_on_start с интервалом
# =====================================================================
# run_on_start=True заставляет interval-правило выполниться
# немедленно при старте RuleEngine, а затем — каждые interval секунд.
# Без run_on_start первое выполнение происходит через interval секунд.

async def demo_run_on_start(app: KamioApp):
    """Показывает немедленное выполнение правила при старте."""
    logger.info("=== Демонстрация: run_on_start с интервалом ===")

    execution_times = []

    @app.rule(
        interval=10,
        run_on_start=True,
        description="Правило с run_on_start=True",
    )
    async def immediate_check(event: RuleEvent, app: KamioApp):
        """Выполнится сразу при старте, затем каждые 10 сек.

        Без run_on_start первое выполнение было бы через 10 секунд.
        С run_on_start=True — немедленно при запуске RuleEngine.
        """
        from datetime import datetime
        execution_times.append(datetime.now())
        logger.info(f"  [run_on_start] Выполнение #{len(execution_times)}")

    logger.info("Правило 'immediate_check' зарегистрировано с run_on_start=True")
    logger.info("Ожидание 2 секунды для проверки немедленного запуска...")
    await asyncio.sleep(2)

    if execution_times:
        logger.info(f"✅ Правило сработало немедленно ({len(execution_times)} раз)")
    else:
        logger.warning("❌ Правило не сработало немедленно")


# =====================================================================
# Демонстрация: отключение правила во время выполнения
# =====================================================================
# Каждое правило имеет атрибут .enabled (bool).
# Установка enabled=False приостанавливает срабатывание.
# Для interval-правил: цикл продолжает работать, но пропускает
# выполнение (continue при not rule.enabled).
# Для event-правил: правило просто не вызывается.

async def demo_disable_during_execution(app: KamioApp):
    """Показывает отключение правила во время выполнения."""
    logger.info("=== Демонстрация: отключение правила во время выполнения ===")

    light = app.devices.get("living_room_light")
    if not light:
        return

    trigger_count = 0

    @app.rule(device=SmartLight, fields=["mode"], description="Правило для отключения")
    async def countable_rule(event: RuleEvent, app: KamioApp):
        nonlocal trigger_count
        trigger_count += 1
        logger.info(f"  [правило] Срабатывание #{trigger_count}")

    # Находим зарегистрированное правило
    target_rule = None
    for r in app.rules.rules:
        if r.description == "Правило для отключения":
            target_rule = r
            break

    if not target_rule:
        return

    # Триггерим — правило сработает
    await light.handle_state({"mode": "night"})
    await asyncio.sleep(0.5)
    logger.info(f"Срабатываний до отключения: {trigger_count}")

    # Отключаем правило
    target_rule.enabled = False
    logger.info("Правило отключено (enabled=False)")

    # Триггерим — правило НЕ сработает
    await light.handle_state({"mode": "party"})
    await asyncio.sleep(0.5)
    logger.info(f"Срабатываний после отключения: {trigger_count} (не изменилось)")

    # Включаем обратно
    target_rule.enabled = True
    logger.info("Правило включено (enabled=True)")

    await light.handle_state({"mode": "normal"})
    await asyncio.sleep(0.5)
    logger.info(f"Срабатываний после включения: {trigger_count}")


# =====================================================================
# Демонстрация: правило на базовый класс устройства
# =====================================================================
# RuleEngine.matching использует MRO (Method Resolution Order)
# для поиска правил. Правило, зарегистрированное на базовый класс
# Device, срабатывает для ВСЕХ устройств, т.к. все они наследуют Device.
# Правило на промежуточный базовый класс срабатывает для всех
# его подклассов.

async def demo_base_class_rule(app: KamioApp):
    """Показывает правило на базовый класс."""
    logger.info("=== Демонстрация: правило на базовый класс устройства ===")

    matched_devices = []

    # Правило на базовый класс Device — срабатывает для ВСЕХ устройств
    @app.rule(device=Device, fields=["power"], description="Правило на базовый Device")
    async def on_any_power_change(event: RuleEvent, app: KamioApp):
        """Срабатывает при изменении поля 'power' на ЛЮБОМ устройстве.

        device=Device означает, что правило матчится через MRO:
        любое устройство является экземпляром Device, поэтому
        правило сработает для всех.
        """
        matched_devices.append(event.device_id)
        logger.info(f"  [базовый класс] power изменён на '{event.device_id}'")

    logger.info("Правило 'on_any_power_change' зарегистрировано на device=Device")

    # Триггерим на разных устройствах
    light = app.devices.get("living_room_light")
    if light:
        await light.handle_state({"power": True})
        await asyncio.sleep(0.5)

    logger.info(f"Устройства, вызвавшие правило: {matched_devices}")
    logger.info("Правило на базовый класс срабатывает для всех подклассов Device")


# =====================================================================
# Демонстрация: снимок данных в RuleEvent
# =====================================================================
# RuleEvent.data содержит словарь изменённых полей.
# RuleEvent.get(key, default) — удобный метод доступа.
# RuleEvent.kind — "event" (устройство) или "interval" (таймер).
# RuleEvent.device_id — ID устройства или None для interval-правил.

async def demo_rule_event_data(app: KamioApp):
    """Показывает доступ к данным в RuleEvent."""
    logger.info("=== Демонстрация: снимок данных в RuleEvent ===")

    light = app.devices.get("living_room_light")
    if not light:
        return

    @app.rule(device=SmartLight, description="Демонстрация RuleEvent.data")
    async def inspect_event(event: RuleEvent, app: KamioApp):
        """Подробный разбор содержимого RuleEvent."""
        logger.info(f"  event.kind:       {event.kind}")
        logger.info(f"  event.device_id:  {event.device_id}")
        logger.info(f"  event.data:       {event.data}")

        # event.get() с значением по умолчанию
        power = event.get("power", "нет поля power")
        brightness = event.get("brightness", "нет поля brightness")
        missing = event.get("nonexistent", "значение по умолчанию")
        logger.info(f"  event.get('power'):       {power}")
        logger.info(f"  event.get('brightness'):  {brightness}")
        logger.info(f"  event.get('nonexistent'): {missing}")

        # event.data — обычный dict, можно итерировать
        for key, value in event.data.items():
            logger.info(f"    {key} = {value}")

    # Триггерим с несколькими полями одновременно
    await light.handle_state({"power": True, "brightness": 150})
    await asyncio.sleep(0.5)


# =====================================================================
# Демонстрация: правило с несколькими полями
# =====================================================================
# @app.rule(fields=["field1", "field2"]) срабатывает, если в
# обновлении присутствует ХОТЯ БЫ ОДНО из перечисленных полей.
# Проверяйте event.data, чтобы понять, какое именно поле изменилось.

async def demo_multi_field_rule(app: KamioApp):
    """Показывает правило, реагирующее на несколько полей."""
    logger.info("=== Демонстрация: правило с несколькими полями ===")

    light = app.devices.get("living_room_light")
    if not light:
        return

    @app.rule(
        device=SmartLight,
        fields=["power", "brightness", "mode"],
        description="Мульти-поле правило",
    )
    async def multi_field_handler(event: RuleEvent, app: KamioApp):
        """Срабатывает при изменении power, brightness ИЛИ mode.

        fields=["power", "brightness", "mode"] означает:
        правило сработает, если в обновлении есть хотя бы одно
        из этих полей. event.data покажет, какие именно изменились.
        """
        changed = list(event.data.keys())
        logger.info(f"  [мульти-поле] Изменены поля: {changed}")

        if "power" in event.data:
            logger.info(f"    power → {event.get('power')}")
        if "brightness" in event.data:
            logger.info(f"    brightness → {event.get('brightness')}")
        if "mode" in event.data:
            logger.info(f"    mode → {event.get('mode')}")

    logger.info("Правило 'multi_field_handler' реагирует на power/brightness/mode")

    # Изменяем только power — сработает
    logger.info("Изменяем только power:")
    await light.handle_state({"power": True})
    await asyncio.sleep(0.5)

    # Изменяем только brightness — сработает
    logger.info("Изменяем только brightness:")
    await light.handle_state({"brightness": 200})
    await asyncio.sleep(0.5)

    # Изменяем только mode — сработает
    logger.info("Изменяем только mode:")
    await light.handle_state({"mode": "party"})
    await asyncio.sleep(0.5)

    # Изменяем несколько полей сразу — сработает один раз
    logger.info("Изменяем power + brightness одновременно:")
    await light.handle_state({"power": False, "brightness": 50})
    await asyncio.sleep(0.5)


# =====================================================================
# Расширенная главная функция с дополнительными демонстрациями
# =====================================================================

async def extended_main():
    """Запускает базовую демонстрацию плюс все дополнительные секции."""
    logger.info("=== Демонстрация правил автоматизации kamio ===\n")

    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="rules_demo")

    # Регистрируем правила на уровне приложения ДО запуска
    setup_app_rules(app)

    # Создаём устройства
    light = await app.add_device("living_room_light", SmartLight)
    motion = await app.add_device("hallway_motion", MotionSensor)
    meter = await app.add_device("main_meter", EnergyMeter)

    # Имитируем значения телеметрии
    motion.battery_level = 85.0
    meter.power_consumption = 42.5
    meter.total_energy = 1500.0

    # Запускаем приложение
    await app.start()

    # --- Базовые демонстрации (из оригинального main) ---
    logger.info("\n--- Тест 1: правило @rule(fields=['power']) ---")
    await light.handle_state({"power": True})
    await asyncio.sleep(0.5)
    await light.handle_state({"power": False})
    await asyncio.sleep(0.5)

    logger.info("\n--- Тест 2: правило @rule(fields=['brightness', 'mode']) ---")
    await light.handle_state({"brightness": 180})
    await asyncio.sleep(0.5)
    await light.handle_state({"mode": "night"})
    await asyncio.sleep(0.5)

    logger.info("\n--- Тест 3: движение включает свет ---")
    await motion.handle_command("trigger_motion", {})
    await asyncio.sleep(1.0)

    logger.info("\n--- Тест 4: управление правилами ---")
    await demo_rule_enable_disable(app)

    # --- Дополнительные демонстрации ---
    await demo_zero_param_rule(app)
    await demo_one_param_rule(app)
    await demo_two_param_rule(app)
    await demo_run_on_start(app)
    await demo_disable_during_execution(app)
    await demo_base_class_rule(app)
    await demo_rule_event_data(app)
    await demo_multi_field_rule(app)

    # --- Завершение ---
    logger.info("\n--- Завершение ---")
    await app.stop()
    logger.info("Демонстрация завершена")


if __name__ == "__main__":
    asyncio.run(extended_main())
