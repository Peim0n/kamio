"""
02 — Smart Home (полный пример)
================================

Умный дом с несколькими устройствами, правилами автоматизации,
событийной шиной и хуками жизненного цикла.

Запуск::
    python examples/02_smart_home.py

Что демонстрирует:
    - Несколько классов устройств (свет, датчик движения, термостат, жалюзи)
    - Правила автоматизации (@rule) внутри классов устройств
    - Правила на уровне приложения (@app.rule)
    - Event Bus: подписка на события, кастомные события
    - Хуки жизненного цикла (on_after_start, on_device_added)
    - Взаимодействие устройств через app.devices
    - Конструктор kwargs (target_light_id, target_motion_sensor_id)
    - Симуляторы (псевдо-устройства для тестирования логики)
"""
from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime
from typing import Any, Dict, Optional

from kamio import KamioApp, Device, RuleEvent, command, event, rule, state, telemetry

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("smart_home")


# =====================================================================
# Устройства
# =====================================================================

class DimmableLight(Device):
    """Диммируемая лампа с яркостью."""

    power: bool = state(default=False, writable=True, description="Питание")
    brightness: int = state(default=100, min=0, max=255, writable=True, description="Яркость")

    @command
    async def turn_on(self):
        self.power = True
        logger.info(f"💡 Light {self.node.device_id} ON")

    @command
    async def turn_off(self):
        self.power = False
        logger.info(f"💡 Light {self.node.device_id} OFF")

    @command
    async def set_brightness(self, value: int):
        self.brightness = max(0, min(255, value))
        logger.info(f"💡 Light {self.node.device_id} brightness={self.brightness}")


class MotionSensor(Device):
    """Датчик движения с событием обнаружения."""

    motion: bool = state(default=False, writable=False, description="Обнаружено движение")
    last_triggered: Optional[datetime] = state(default=None, writable=False)
    motion_detected_event = event(description="Событие обнаружения движения")

    @command
    async def simulate_motion(self):
        """Имитировать обнаружение движения (для тестирования)."""
        self.motion = True
        self.last_triggered = datetime.now()
        await self.emit("motion_detected_event", {"timestamp": datetime.now().isoformat()})
        logger.info(f"🚶 Motion detected on {self.node.device_id}")

        async def reset():
            await asyncio.sleep(5)
            self.motion = False
            logger.info(f"🚶 Motion cleared on {self.node.device_id}")

        self.create_task(reset(), name=f"motion_reset_{self.node.device_id}")


class Thermostat(Device):
    """Термостат с целевой температурой и режимом нагрева."""

    current_temp: float = telemetry(default=22.0, unit="°C", freq="10s", description="Текущая температура")
    target_temp: float = state(default=22.0, min=10.0, max=35.0, writable=True, description="Целевая температура")
    heating: bool = state(default=False, writable=False, description="Активен ли нагрев")
    mode: str = state(default="auto", choices=("auto", "manual", "off"), writable=True)

    @rule(fields=["current_temp", "target_temp"])
    async def regulate(self, event: RuleEvent, app: KamioApp):
        """Автоматическое регулирование температуры."""
        if self.mode == "off":
            self.heating = False
            return
        if self.current_temp < self.target_temp - 0.5:
            if not self.heating:
                self.heating = True
                logger.info(f"🔥 Thermostat {self.node.device_id} heating ON (temp={self.current_temp}°C, target={self.target_temp}°C)")
        elif self.current_temp >= self.target_temp:
            if self.heating:
                self.heating = False
                logger.info(f"🔥 Thermostat {self.node.device_id} heating OFF (temp={self.current_temp}°C)")


class WindowBlinds(Device):
    """Управляемые жалюзи с положением 0-100%."""

    position: int = state(default=0, min=0, max=100, writable=True, description="Положение 0=закрыты, 100=открыты")
    moving_state: str = state(default="idle", choices=("idle", "opening", "closing"), writable=False)

    @command
    async def open_fully(self):
        await self._move_to(100)

    @command
    async def close_fully(self):
        await self._move_to(0)

    @command
    async def set_position(self, value: int):
        await self._move_to(max(0, min(100, value)))

    async def _move_to(self, target: int):
        if self.position == target:
            return
        self.moving_state = "opening" if target > self.position else "closing"
        logger.info(f"🪟 Blinds {self.node.device_id} {self.moving_state} → {target}%")
        await asyncio.sleep(1.0)  # Имитация движения
        self.position = target
        self.moving_state = "idle"
        logger.info(f"🪟 Blinds {self.node.device_id} reached {target}%")


class DoorSensor(Device):
    """Датчик двери с режимом охраны."""

    is_open: bool = state(default=False, writable=False, description="Дверь открыта")
    security_armed: bool = state(default=False, writable=True, description="Режим охраны")
    last_opened: Optional[datetime] = state(default=None, writable=False)
    intrusion_event = event(description="Событие вторжения")

    @rule(fields=["is_open"])
    async def on_door_change(self, event: RuleEvent, app: KamioApp):
        if event.get("is_open") is True:
            self.last_opened = datetime.now()
            if self.security_armed:
                logger.error(f"🚨 ALARM! Door {self.node.device_id} opened while armed!")
                await self.emit("intrusion_event", {"device_id": self.node.device_id, "time": datetime.now().isoformat()})
                await app.publish_event("security_alert", {"device_id": self.node.device_id, "type": "intrusion"})
            else:
                logger.info(f"🚪 Door {self.node.device_id} opened")

    @command
    async def arm(self):
        self.security_armed = True
        logger.info(f"🔒 Door {self.node.device_id} armed")

    @command
    async def disarm(self):
        self.security_armed = False
        logger.info(f"🔓 Door {self.node.device_id} disarmed")

    @command
    async def simulate_open(self):
        """Имитировать открытие/закрытие двери."""
        self.is_open = not self.is_open
        logger.info(f"🚪 Door {self.node.device_id} {'opened' if self.is_open else 'closed'}")


# =====================================================================
# Контроллеры (устройства для глобальной логики)
# =====================================================================

class MotionLightController(Device):
    """Контроллер: включает свет по движению, выключает через задержку."""

    target_light_id: str = state(description="ID лампы")
    target_motion_sensor_id: str = state(description="ID датчика движения")
    auto_off_delay: int = state(default=30, min=1, description="Задержка авто-выключения (сек)")

    async def on_start(self, node):
        self.app.subscribe_event(
            "motion_detected_event",
            self._on_motion,
            filter_fn=lambda d: d.get("device_id") == self.target_motion_sensor_id,
        )
        logger.info(f"[MotionLightCtrl] Watching {self.target_motion_sensor_id} → {self.target_light_id}")

    async def _on_motion(self, data: Dict[str, Any]):
        light = self.app.devices.get(self.target_light_id)
        if isinstance(light, DimmableLight) and not light.power:
            await light.turn_on()
            logger.info(f"[MotionLightCtrl] Motion → light ON")

            async def auto_off():
                await asyncio.sleep(self.auto_off_delay)
                sensor = self.app.devices.get(self.target_motion_sensor_id)
                if isinstance(sensor, MotionSensor) and not sensor.motion:
                    await light.turn_off()
                    logger.info(f"[MotionLightCtrl] No motion for {self.auto_off_delay}s → light OFF")

            self.create_task(auto_off(), name="auto_off")


# =====================================================================
# Симуляторы
# =====================================================================

class EnvironmentSimulator(Device):
    """Симулирует изменения температуры для термостата."""

    async def on_start(self, node):
        self.create_task(self._simulate(), name="env_sim")

    async def _simulate(self):
        while True:
            await asyncio.sleep(10)
            for dev in self.app.devices.values():
                if isinstance(dev, Thermostat):
                    delta = random.uniform(-0.3, 0.3)
                    if dev.heating:
                        delta += 0.2
                    new_temp = round(dev.current_temp + delta, 1)
                    dev.current_temp = new_temp
                    await dev.publish_telemetry({"current_temp": new_temp})


# =====================================================================
# Хуки жизненного цикла
# =====================================================================

async def on_start_hook():
    logger.info("🚀 Smart Home is ONLINE")

async def on_device_added(device: Device):
    logger.info(f"🆕 Device added: {device.node.device_id} ({device.device_type()})")


# =====================================================================
# Главный цикл
# =====================================================================
# Главный цикл
# =====================================================================

async def main():
    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="smart_home_demo")

    # Хуки
    app.register_hook("on_after_start", on_start_hook)
    app.register_hook("on_device_added", on_device_added)

    # Подписка на security_alert через Event Bus
    app.subscribe_event(
        "security_alert",
        lambda d: logger.critical(f"🚨🚨 SECURITY: {d}"),
    )

    # Подписка на изменения состояния для логирования
    app.subscribe_event(
        "device_state_changed",
        lambda d: logger.debug(f"📊 {d['device_id']}.{d['field']}: {d['old_value']} → {d['new_value']}"),
        filter_fn=lambda d: d.get("field") in ("power", "heating", "is_open", "position"),
    )

    # Регистрация классов
    for cls in [DimmableLight, MotionSensor, Thermostat, WindowBlinds,
                DoorSensor, MotionLightController, EnvironmentSimulator]:
        app.register(cls)

    # Запуск
    await app.start()

    # Создание устройств
    await app.add_device("hallway_light", DimmableLight)
    await app.add_device("hallway_motion", MotionSensor)
    await app.add_device("living_thermostat", Thermostat, target_temp=21.0)
    await app.add_device("bedroom_blinds", WindowBlinds)
    await app.add_device("front_door", DoorSensor)

    # Контроллер: движение → свет (kwargs передаются в конструктор)
    await app.add_device(
        "motion_light_ctrl",
        MotionLightController,
        target_light_id="hallway_light",
        target_motion_sensor_id="hallway_motion",
        auto_off_delay=15,
    )

    # Симулятор
    await app.add_device("env_sim", EnvironmentSimulator)

    logger.info("✅ Smart Home initialized. Press Ctrl+C to stop.")

    # Демонстрация: симулируем события
    async def demo():
        await asyncio.sleep(2)

        # 1. Движение → свет включается
        motion = app.devices.get("hallway_motion")
        if motion:
            await motion.simulate_motion()

        await asyncio.sleep(3)

        # 2. Меняем температуру
        thermostat = app.devices.get("living_thermostat")
        if thermostat:
            thermostat.current_temp = 18.0  # искусственно понижаем температуру
            await thermostat.publish_telemetry({"current_temp": 18.0})

        await asyncio.sleep(3)

        # 3. Открываем дверь в режиме охраны
        door = app.devices.get("front_door")
        if door:
            await door.arm()
            await asyncio.sleep(1)
            await door.simulate_open()

        await asyncio.sleep(3)

        # 4. Закрываем дверь, разоружаем
        if door:
            await door.simulate_open()  # закроет
            await door.disarm()

    app.create_task(demo(), name="demo")

    # Hold
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await app.stop()


# =====================================================================
# Демонстрация: send_command между устройствами
# =====================================================================
# device.send_command(target_device_id, method, params) отправляет
# команду другому устройству через MQTT и ожидает ACK.
# Это позволяет устройствам взаимодействовать напрямую, без
# промежуточного контроллера.

async def demo_send_command(app: KamioApp):
    """Показывает отправку команд между устройствами."""
    logger.info("=== Демонстрация: send_command между устройствами ===")

    light = app.devices.get("hallway_light")
    if not light:
        logger.warning("Устройство 'hallway_light' не найдено")
        return

    # Отправляем команду turn_on на лампу через send_command
    # Формат: send_command(target_id, method, params, timeout)
    try:
        ack = await light.send_command(
            target_device_id="hallway_light",
            method="set_brightness",
            params={"value": 200},
            timeout=5.0,
        )
        logger.info(f"send_command ACK получен: {ack}")
    except Exception as e:
        logger.info(f"send_command: {e} (требуется MQTT-брокер для ACK)")

    # send_command можно использовать для управления любым устройством
    blinds = app.devices.get("bedroom_blinds")
    if blinds and light:
        try:
            ack = await blinds.send_command(
                target_device_id="hallway_light",
                method="turn_off",
                params={},
                timeout=5.0,
            )
            logger.info(f"Жалюзи → лампа: ACK={ack}")
        except Exception as e:
            logger.info(f"send_command blinds→light: {e}")


# =====================================================================
# Демонстрация: register_async_callback
# =====================================================================
# device.register_async_callback(topic, callback) создаёт
# lightweight CustomNode, подписанный на указанный MQTT topic.
# Это позволяет устройству слушать произвольные MQTT-топики,
# не только стандартные kamio-топики.

async def demo_async_callback(app: KamioApp):
    """Показывает регистрацию кастомного MQTT-колбэка."""
    logger.info("=== Демонстрация: register_async_callback ===")

    light = app.devices.get("hallway_light")
    if not light:
        return

    # Регистрируем колбэк на кастомный топик
    custom_topic = "home/custom/sensor_data"

    received_messages = []

    async def on_custom_message(topic: str, payload: bytes):
        """Асинхронный колбэк для кастомного MQTT-топика."""
        received_messages.append((topic, payload))
        logger.info(f"  [async_callback] Топик: {topic}, payload: {payload[:50]}")

    light.register_async_callback(custom_topic, on_custom_message)
    logger.info(f"Зарегистрирован колбэк на топик: {custom_topic}")

    # Публикуем тестовое сообщение в этот топик через MQTT-клиент
    app.mqtt_client.publish(custom_topic, b'{"temp": 23.5}', qos=1)
    await asyncio.sleep(1.0)

    logger.info(f"Получено сообщений через кастомный топик: {len(received_messages)}")

    # Отписываемся — unregister_async_callback удаляет CustomNode
    light.unregister_async_callback(custom_topic)
    logger.info("Колбэк удалён через unregister_async_callback")


# =====================================================================
# Демонстрация: request_state_sync / request_full_sync
# =====================================================================
# request_state_sync() немедленно публикует текущие state-поля в MQTT.
# request_full_sync() публикует все поля (state + config + telemetry).
# Это полезно для внеочередной синхронизации (например, после
# ручного изменения атрибутов через _set_state).

async def demo_sync_methods(app: KamioApp):
    """Показывает синхронизацию состояния с MQTT."""
    logger.info("=== Демонстрация: request_state_sync / request_full_sync ===")

    light = app.devices.get("hallway_light")
    thermostat = app.devices.get("living_thermostat")
    if not light:
        return

    # Тихо меняем состояние без публикации (через _set_state)
    light._set_state(power=True, brightness=77)
    logger.info(f"_set_state: power={light.power}, brightness={light.brightness} (без публикации)")

    # request_state_sync публикует текущие state-поля в MQTT
    await light.request_state_sync()
    logger.info("request_state_sync() — state-поля опубликованы")

    # request_full_sync публикует все поля, включая config и telemetry
    if thermostat:
        await thermostat.request_full_sync()
        logger.info(f"thermostat.request_full_sync() — все поля опубликованы")

    # Снимки для сравнения
    logger.info(f"light state snapshot: {light.get_state_snapshot()}")
    if thermostat:
        logger.info(f"thermostat full snapshot: {thermostat.get_full_snapshot()}")


# =====================================================================
# Демонстрация: get_full_snapshot для мониторинга
# =====================================================================
# get_full_snapshot() возвращает словарь всех полей устройства
# (state + config + telemetry). Полезно для мониторинга, логирования
# и создания дашбордов.

async def demo_full_snapshot_monitoring(app: KamioApp):
    """Показывает использование get_full_snapshot для мониторинга."""
    logger.info("=== Демонстрация: get_full_snapshot для мониторинга ===")

    # Собираем снимки всех устройств
    all_snapshots = {}
    for dev_id, device in app.devices.items():
        all_snapshots[dev_id] = device.get_full_snapshot()

    # Выводим сводку
    for dev_id, snap in all_snapshots.items():
        field_count = len(snap)
        logger.info(f"  {dev_id}: {field_count} полей — {list(snap.keys())}")

    # Пример: находим все устройства с включённым питанием
    powered_on = [
        dev_id for dev_id, snap in all_snapshots.items()
        if snap.get("power") is True
    ]
    logger.info(f"Устройства с power=True: {powered_on}")

    # Пример: мониторинг температуры термостата
    thermostat = app.devices.get("living_thermostat")
    if thermostat:
        snap = thermostat.get_full_snapshot()
        current_temp = snap.get("current_temp")
        target_temp = snap.get("target_temp")
        heating = snap.get("heating")
        logger.info(
            f"Термостат: текущая={current_temp}°C, "
            f"целевая={target_temp}°C, нагрев={heating}"
        )


# =====================================================================
# Демонстрация: reinitialize
# =====================================================================
# device.reinitialize() останавливает и перезапускает устройство
# на месте (например, после изменения конфигурации).
# Отключает и заново подключает драйвер, перезапускает телеметрию
# и keepalive-циклы через on_stop / on_start.

async def demo_reinitialize(app: KamioApp):
    """Показывает переинициализацию устройства."""
    logger.info("=== Демонстрация: reinitialize ===")

    thermostat = app.devices.get("living_thermostat")
    if not thermostat:
        return

    # Меняем конфигурацию перед reinitialize
    logger.info(f"До reinitialize: target_temp={thermostat.target_temp}, mode={thermostat.mode}")

    # Меняем целевую температуру
    await thermostat.handle_state({"target_temp": 25.0})
    logger.info(f"После изменения: target_temp={thermostat.target_temp}")

    # Переинициализируем устройство
    # Это вызывает on_stop (отключение драйвера, отмена задач),
    # затем переподключение драйвера и on_start (телеметрия, keepalive)
    try:
        await thermostat.reinitialize()
        logger.info("Устройство успешно переинициализировано")
        logger.info(f"После reinitialize: target_temp={thermostat.target_temp} (значение сохранено)")
    except Exception as e:
        logger.warning(f"Reinitialize не удался (возможно нет драйвера): {e}")


# =====================================================================
# Демонстрация: enable_telemetry = False
# =====================================================================
# Установка enable_telemetry = False на классе или экземпляре
# полностью отключает автоматическую публикацию телеметрии.
# Поля telemetry() всё равно объявляются в схеме, но периодические
# задачи не запускаются.

class SilentThermostat(Thermostat):
    """Термостат с отключённой телеметрией для экономии трафика.

    Классовый атрибут enable_telemetry = False отключает
    автоматическую публикацию для всех экземпляров этого класса.
    """

    enable_telemetry = False


async def demo_disable_telemetry(app: KamioApp):
    """Показывает отключение телеметрии на устройстве."""
    logger.info("=== Демонстрация: enable_telemetry = False ===")

    # Регистрируем и создаём тихий термостат
    app.register(SilentThermostat)
    silent = await app.add_device("silent_thermostat", SilentThermostat)

    # Проверяем, что телеметрия отключена
    logger.info(f"SilentThermostat.enable_telemetry = {SilentThermostat.enable_telemetry}")
    logger.info("Автоматическая телеметрия НЕ запускается (нет периодических задач)")

    # Поля телеметрии всё равно присутствуют в схеме
    schema = SilentThermostat.get_schema()
    telemetry_fields = {
        name: f for name, f in schema["fields"].items() if f["kind"] == "telemetry"
    }
    logger.info(f"Поля телеметрии в схеме: {list(telemetry_fields.keys())}")

    # Можно публиковать телеметрию вручную через publish_telemetry()
    silent.current_temp = 19.5
    await silent.publish_telemetry({"current_temp": 19.5})
    logger.info("Ручная публикация через publish_telemetry() — работает даже с enable_telemetry=False")

    # Снимок телеметрии доступен независимо от enable_telemetry
    snap = silent.get_telemetry_snapshot()
    logger.info(f"get_telemetry_snapshot(): {snap}")


# =====================================================================
# Расширенная главная функция с дополнительными демонстрациями
# =====================================================================

async def extended_main():
    """Запускает базовый smart home плюс все дополнительные секции."""
    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="smart_home_demo")

    # Хуки
    app.register_hook("on_after_start", on_start_hook)
    app.register_hook("on_device_added", on_device_added)

    # Подписка на security_alert через Event Bus
    app.subscribe_event(
        "security_alert",
        lambda d: logger.critical(f"🚨🚨 SECURITY: {d}"),
    )

    # Подписка на изменения состояния для логирования
    app.subscribe_event(
        "device_state_changed",
        lambda d: logger.debug(f"📊 {d['device_id']}.{d['field']}: {d['old_value']} → {d['new_value']}"),
        filter_fn=lambda d: d.get("field") in ("power", "heating", "is_open", "position"),
    )

    # Регистрация классов
    for cls in [DimmableLight, MotionSensor, Thermostat, WindowBlinds,
                DoorSensor, MotionLightController, EnvironmentSimulator]:
        app.register(cls)

    # Запуск
    await app.start()

    # Создание устройств
    await app.add_device("hallway_light", DimmableLight)
    await app.add_device("hallway_motion", MotionSensor)
    await app.add_device("living_thermostat", Thermostat, target_temp=21.0)
    await app.add_device("bedroom_blinds", WindowBlinds)
    await app.add_device("front_door", DoorSensor)

    # Контроллер: движение → свет
    await app.add_device(
        "motion_light_ctrl",
        MotionLightController,
        target_light_id="hallway_light",
        target_motion_sensor_id="hallway_motion",
        auto_off_delay=15,
    )

    # Симулятор
    await app.add_device("env_sim", EnvironmentSimulator)

    logger.info("✅ Smart Home initialized.")

    # --- Дополнительные демонстрации ---
    await asyncio.sleep(2)  # ждём инициализацию

    await demo_send_command(app)
    await demo_async_callback(app)
    await demo_sync_methods(app)
    await demo_full_snapshot_monitoring(app)
    await demo_reinitialize(app)
    await demo_disable_telemetry(app)

    logger.info("Все демонстрации завершены.")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(extended_main())
