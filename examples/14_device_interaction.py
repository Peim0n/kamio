"""
14 — Device Interaction (взаимодействие устройств)
====================================================

Демонстрирует взаимодействие между устройствами в Kamio:
    - send_command() — отправка команды другому устройству и ожидание ACK
    - Cross-device rules (устройство A реагирует на изменение состояния устройства B)
    - app.devices dict — доступ ко всем устройствам
    - get_state_snapshot(), get_full_snapshot() — снимки состояния
    - request_state_sync(), request_full_sync() — внеплановая синхронизация через MQTT
    - reinitialize() — реинициализация устройства (переподключение драйвера)
    - register_async_callback() — подписка на кастомные MQTT-топики

Запуск::
    python examples/14_device_interaction.py

Предварительно запустите MQTT-брокер на localhost:1883
(например, ``docker run -p 1883:1883 eclipse-mosquitto``).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

from kamio import KamioApp, Device, RuleEvent, command, rule, state

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("interaction_demo")


# =====================================================================
# Устройство A: Умная лампа (получатель команд)
# =====================================================================

class SmartLight(Device):
    """Умная лампа — получает команды от контроллера.

    Имеет поля power и brightness, а также команду toggle.
    Другие устройства могут вызывать toggle через send_command().
    """

    power: bool = state(default=False, writable=True, description="Питание")
    brightness: int = state(
        default=100, min=0, max=255, writable=True, description="Яркость"
    )

    @command
    async def toggle(self):
        """Переключить питание. Возвращает новое состояние."""
        self.power = not self.power
        logger.info(f"  [SmartLight] toggle -> power={self.power}")
        return {"power": self.power}

    @command
    async def set_brightness(self, value: int):
        """Установить яркость."""
        self.brightness = max(0, min(255, int(value)))
        logger.info(f"  [SmartLight] set_brightness({value}) -> brightness={self.brightness}")
        return {"brightness": self.brightness}


# =====================================================================
# Устройство B: Датчик присутствия (источник событий)
# =====================================================================

class PresenceSensor(Device):
    """Датчик присутствия — меняет состояние, на которое реагирует контроллер."""

    presence: bool = state(default=False, writable=True, description="Обнаружено присутствие")
    last_motion: str = state(default="", writable=False, description="Время последнего движения")

    @command
    async def simulate_motion(self):
        """Имитировать обнаружение движения."""
        self.presence = True
        self.last_motion = "now"
        logger.info(f"  [PresenceSensor] Движение обнаружено!")

        # Авто-сброс через 5 секунд
        async def auto_reset():
            await asyncio.sleep(5)
            self.presence = False
            logger.info(f"  [PresenceSensor] Движение сброшено")

        self.create_task(auto_reset(), name="presence_reset")

        return {"presence": True}


# =====================================================================
# Устройство C: Контроллер (взаимодействует с A и B)
# =====================================================================

class LightController(Device):
    """Контроллер: включает свет при обнаружении движения.

    Демонстрирует:
    1. Cross-device rule — реагирует на изменение PresenceSensor.presence
    2. send_command() — отправляет команду toggle на SmartLight
    3. app.devices — прямой доступ к другим устройствам
    4. get_state_snapshot() — чтение состояния другого устройства
    """

    # ID целевых устройств (передаются через kwargs в add_device)
    target_light_id: str = state(default="", description="ID управляемой лампы")
    target_sensor_id: str = state(default="", description="ID датчика присутствия")

    # Локальное состояние контроллера
    auto_mode: bool = state(default=True, writable=True, description="Автоматический режим")
    last_action: str = state(default="idle", writable=False, description="Последнее действие")

    # --- Cross-device rule ---
    # Реагирует на изменение поля presence на устройстве PresenceSensor.
    # @rule с fields=["presence"] срабатывает при изменении этого поля.
    # Но это правило внутри класса LightController — оно реагирует на
    # изменения СОБСТВЕННЫХ полей. Для реакции на чужие поля используем
    # app-level rule (см. ниже в main()).
    #
    # Здесь мы используем on_start для подписки на события через EventBus.

    async def on_start(self, node):
        """Подписка на изменения состояния датчика при старте."""
        # Подписываемся на device_state_changed с фильтром по датчику
        self.app.subscribe_event(
            "device_state_changed",
            self._on_sensor_state_changed,
            filter_fn=self._is_target_sensor_presence,
        )
        logger.info(
            f"  [Controller] Подписка на {self.target_sensor_id}.presence -> {self.target_light_id}"
        )

    def _is_target_sensor_presence(self, data: Dict[str, Any]) -> bool:
        """Фильтр: только изменения поля presence на целевом датчике."""
        return (
            data.get("device_id") == self.target_sensor_id
            and data.get("field") == "presence"
        )

    async def _on_sensor_state_changed(self, data: Dict[str, Any]):
        """Обработчик изменения состояния датчика присутствия.

        Вызывается через EventBus при изменении PresenceSensor.presence.
        """
        if not self.auto_mode:
            logger.info("  [Controller] Авто-режим выключен, пропускаем")
            return

        presence = data.get("new_value", False)

        if presence:
            # Движение обнаружено — включаем свет через send_command
            self.last_action = "turning_on"
            logger.info(f"  [Controller] Движение -> отправляем toggle на {self.target_light_id}")

            try:
                # send_command() отправляет команду на другое устройство
                # и ждёт ACK (подтверждение выполнения).
                # Возвращает Envelope с результатом.
                ack = await self.send_command(
                    target_device_id=self.target_light_id,
                    method="toggle",
                    params={},
                    timeout=5.0,
                )
                self.last_action = "sent_toggle"
                logger.info(f"  [Controller] ACK получен: {ack.data if hasattr(ack, 'data') else ack}")

            except asyncio.TimeoutError:
                self.last_action = "timeout"
                logger.error(f"  [Controller] Таймаут: лампа {self.target_light_id} не ответила")
            except Exception as e:
                self.last_action = "error"
                logger.error(f"  [Controller] Ошибка send_command: {e}")

            # Читаем состояние лампы через app.devices
            light = self.app.devices.get(self.target_light_id)
            if light:
                snapshot = light.get_state_snapshot()
                logger.info(f"  [Controller] Состояние лампы: {snapshot}")

        else:
            # Движение пропало — можно выключить свет
            self.last_action = "idle"
            logger.info(f"  [Controller] Движение прекратилось")

    @command
    async def send_brightness(self, value: int):
        """Отправить команду set_brightness на целевую лампу через send_command."""
        try:
            ack = await self.send_command(
                target_device_id=self.target_light_id,
                method="set_brightness",
                params={"value": value},
                timeout=5.0,
            )
            logger.info(f"  [Controller] brightness ACK: {ack.data if hasattr(ack, 'data') else ack}")
            return {"status": "ok", "value": value}
        except Exception as e:
            logger.error(f"  [Controller] Ошибка: {e}")
            return {"status": "error", "error": str(e)}


# =====================================================================
# Устройство D: Монитор (читает снимки и синхронизирует состояние)
# =====================================================================

class StateMonitor(Device):
    """Монитор: периодически читает снимки других устройств.

    Демонстрирует:
    1. get_state_snapshot() — только state-поля
    2. get_full_snapshot() — state + config + telemetry
    3. request_state_sync() — публикация состояния в MQTT
    4. request_full_sync() — публикация всех полей в MQTT
    """

    monitored_device_id: str = state(default="", description="ID отслеживаемого устройства")

    async def on_start(self, node):
        """Запуск периодического мониторинга."""
        self.create_task(self._monitor_loop(), name="monitor_loop")
        logger.info(f"  [Monitor] Запущен мониторинг устройства {self.monitored_device_id}")

    async def _monitor_loop(self):
        """Периодически читает снимки отслеживаемого устройства."""
        while self.node and self.node.is_running:
            await asyncio.sleep(5)

            target = self.app.devices.get(self.monitored_device_id)
            if not target:
                continue

            # --- get_state_snapshot() ---
            # Возвращает словарь {field_name: value} только для state-полей
            state_snap = target.get_state_snapshot()
            logger.info(f"  [Monitor] {self.monitored_device_id} state: {state_snap}")

            # --- get_full_snapshot() ---
            # Возвращает все поля: state + config + telemetry
            full_snap = target.get_full_snapshot()
            logger.info(f"  [Monitor] {self.monitored_device_id} full: {full_snap}")

            # --- request_state_sync() ---
            # Публикует текущие state-поля в MQTT (внеплановая синхронизация).
            # Полезно когда состояние изменилось локально и нужно уведомить
            # других подписчиков MQTT (например, Home Assistant).
            await target.request_state_sync()
            logger.info(f"  [Monitor] request_state_sync() отправлен для {self.monitored_device_id}")

            # --- request_full_sync() ---
            # Публикует ВСЕ поля (state + config + telemetry) в MQTT.
            # Используется для полной синхронизации при восстановлении.
            # await target.request_full_sync()


# =====================================================================
# Устройство E: Custom MQTT subscriber (register_async_callback)
# =====================================================================

class CustomTopicListener(Device):
    """Устройство, подписанное на произвольный MQTT-топик.

    Демонстрирует register_async_callback() — подписку на кастомные
    MQTT-топики, не относящиеся к стандартной шине Kamio.

    Это полезно для интеграции с внешними системами, которые публикуют
    данные в свои собственные топики (например, tele/<device>/SENSOR).
    """

    last_payload: str = state(default="", writable=False, description="Последний payload")
    message_count: int = state(default=0, writable=False, description="Счётчик сообщений")

    # Топик для подписки (можно передать через kwargs)
    target_topic: str = state(default="", description="MQTT топик для подписки")

    async def on_start(self, node):
        """Регистрация callback на кастомный топик."""
        if not self.target_topic:
            logger.warning("  [CustomListener] target_topic не задан")
            return

        # register_async_callback создаёт CustomNode, который подписывается
        # на указанный MQTT-топик и вызывает callback при получении сообщения.
        # Callback может быть sync или async; сигнатура: (topic, payload: bytes)
        self.register_async_callback(self.target_topic, self._on_custom_message)
        logger.info(f"  [CustomListener] Подписка на топик: {self.target_topic}")

    async def _on_custom_message(self, topic: str, payload: bytes):
        """Callback для кастомного MQTT-топика.

        Args:
            topic:   MQTT-топик сообщения (абсолютный).
            payload: Сырые байты сообщения.
        """
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError:
            text = repr(payload)

        self.last_payload = text[:200]  # Ограничиваем длину
        self.message_count += 1
        logger.info(f"  [CustomListener] {topic}: {text[:100]} (всего: {self.message_count})")

    async def on_stop(self, node):
        """Отписка от кастомного топика при остановке."""
        if self.target_topic:
            self.unregister_async_callback(self.target_topic)
            logger.info(f"  [CustomListener] Отписка от топика: {self.target_topic}")


# =====================================================================
# App-level rule: cross-device реакция
# =====================================================================

async def on_presence_change(event: RuleEvent, app: KamioApp):
    """App-level правило: реакция на изменение PresenceSensor.presence.

    Это альтернатива подписке через EventBus (как в LightController).
    App-level правила регистрируются через @app.rule или app.add_rule()
    и привязываются к конкретному классу устройства и полям.

    Преимущество: декларативная привязка к полям устройства.
    Недостаток: меньше контроля (нет filter_fn).
    """
    presence = event.get("presence")
    device_id = event.device_id
    logger.info(f"  [app-rule] on_presence_change: {device_id}.presence={presence}")


# =====================================================================
# Главный цикл
# =====================================================================

async def main():
    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="interaction_demo")

    # --- Регистрация классов ---
    app.register(SmartLight)
    app.register(PresenceSensor)
    app.register(LightController)
    app.register(StateMonitor)
    app.register(CustomTopicListener)

    # --- App-level rule: реакция на изменение presence ---
    # Привязывается к классу PresenceSensor и полю "presence"
    app.add_rule(
        on_presence_change,
        device=PresenceSensor,
        fields=["presence"],
        description="Логирование изменения присутствия",
    )

    # --- Запуск приложения ---
    await app.start()

    # --- Создание устройств ---
    logger.info("=== Создание устройств ===")

    # A: Лампа
    light = await app.add_device("living_light", SmartLight)

    # B: Датчик присутствия
    sensor = await app.add_device("hall_sensor", PresenceSensor)

    # C: Контроллер (kwargs передаются в конструктор и применяются к state-полям)
    controller = await app.add_device(
        "light_ctrl",
        LightController,
        target_light_id="living_light",
        target_sensor_id="hall_sensor",
    )

    # D: Монитор
    monitor = await app.add_device(
        "state_monitor",
        StateMonitor,
        monitored_device_id="living_light",
    )

    # E: Custom listener — подписка на топик Kamio/v1/living_light/ds
    # (state-обновления лампы, которые Kamio публикует автоматически)
    listener = await app.add_device(
        "topic_listener",
        CustomTopicListener,
        target_topic="Kamio/v1/living_light/ds",
    )

    # --- Демонстрация app.devices ---
    logger.info("=== app.devices dict ===")
    for dev_id, dev in app.devices.items():
        logger.info(f"  {dev_id}: {dev.device_type()}")

    # --- Демонстрация: send_command через контроллер ---
    logger.info("=== send_command: контроллер -> лампа ===")
    await asyncio.sleep(1)

    # Имитируем движение — контроллер должен включить свет
    logger.info("Имитация движения на датчике...")
    await sensor.handle_command("simulate_motion", {})

    # Ждём, пока контроллер отреагирует через EventBus
    await asyncio.sleep(2)

    # Проверяем состояние лампы
    logger.info(f"Лампа power={light.power} (должна быть True после toggle)")

    # --- Демонстрация: send_brightness через контроллер ---
    logger.info("=== send_command: set_brightness ===")
    result = await controller.handle_command("send_brightness", {"value": 150})
    logger.info(f"Результат: {result}")

    await asyncio.sleep(1)

    # --- Демонстрация: get_state_snapshot / get_full_snapshot ---
    logger.info("=== Снимки состояния ===")
    logger.info(f"Light state snapshot: {light.get_state_snapshot()}")
    logger.info(f"Light full snapshot: {light.get_full_snapshot()}")
    logger.info(f"Controller state snapshot: {controller.get_state_snapshot()}")

    # --- Демонстрация: request_state_sync ---
    logger.info("=== request_state_sync() ===")
    # Публикует текущие state-поля в MQTT вне графика автоматической публикации
    await light.request_state_sync()
    logger.info("State sync отправлен для living_light")

    await asyncio.sleep(1)

    # --- Демонстрация: request_full_sync ---
    logger.info("=== request_full_sync() ===")
    await light.request_full_sync()
    logger.info("Full sync отправлен для living_light")

    await asyncio.sleep(1)

    # --- Демонстрация: reinitialize ---
    logger.info("=== reinitialize() ===")
    # reinitialize() останавливает и заново запускает устройство:
    #   1. on_stop (disconnect драйвера, отмена задач)
    #   2. driver.connect() (если драйвер задан)
    #   3. on_start (запуск телеметрии, keepalive)
    # Полезно после изменения конфигурации или при восстановлении связи.
    await sensor.reinitialize()
    logger.info("PresenceSensor реинициализирован")

    await asyncio.sleep(1)

    # --- Демонстрация: register_async_callback в действии ---
    logger.info("=== Custom MQTT callback ===")
    # CustomTopicListener подписан на топик Kamio/v1/living_light/ds
    # При изменении состояния лампы, Kamio публикует в этот топик,
    # и listener должен получить сообщение.
    logger.info("Изменяем состояние лампы — listener должен получить сообщение...")
    await light.handle_state({"brightness": 77})
    await asyncio.sleep(1)
    logger.info(f"Listener message_count={listener.message_count}, last_payload={listener.last_payload[:80]}...")

    # --- Демонстрация: прямой доступ через app.devices ---
    logger.info("=== Прямой доступ через app.devices ===")
    target_light = app.devices.get("living_light")
    if target_light:
        # Можно напрямую вызывать методы устройства
        await target_light.handle_state({"power": False})
        logger.info(f"Прямой доступ: light.power = {target_light.power}")

    # Ждём для наблюдения за монитором
    logger.info("=== Ожидание мониторинга (5s) ===")
    await asyncio.sleep(6)

    # =================================================================
    # Новые демонстрации (добавлены для полноты примера)
    # =================================================================

    # 1. send_command с timeout — обработка таймаута
    await demo_send_command_timeout(controller)

    # 2. send_command с ошибкой — сценарий ошибки
    await demo_send_command_error(controller, light)

    # 3. cross-device rule подробно
    await demo_cross_device_rule_detailed(app, sensor, light, controller)

    # 4. app.devices итерация — перебор всех устройств
    demo_devices_iteration(app)

    # 5. unregister_async_callback — удаление callback
    await demo_unregister_async_callback(app, listener)

    # 6. reinitialize с driver — реинициализация устройства
    await demo_reinitialize_with_driver(app, sensor)

    logger.info("=== Завершение ===")
    await app.stop()


# =====================================================================
# Демонстрация: send_command с timeout — обработка таймаута
# =====================================================================

async def demo_send_command_timeout(controller):
    """Показывает обработку таймаута при send_command.

    send_command(target_device_id, method, params, timeout) отправляет
    команду и ждёт ACK. Если ACK не получен за timeout секунд,
    вызывается asyncio.TimeoutError.

    Короткий timeout полезен для быстрого обнаружения недоступности
    устройства. Длинный — для устройств с медленным откликом.
    """
    logger.info("=== Демонстрация: send_command с timeout ===")

    # send_command с очень коротким timeout на несуществующее устройство
    logger.info("Отправляем команду на несуществующее устройство (timeout=0.5s)...")
    try:
        ack = await controller.send_command(
            target_device_id="nonexistent_device",
            method="toggle",
            params={},
            timeout=0.5,
        )
        logger.info(f"ACK получен: {ack}")
    except asyncio.TimeoutError:
        logger.info("TimeoutError перехвачен (устройство не ответило за 0.5s)")
    except Exception as e:
        logger.info(f"Другая ошибка: {type(e).__name__}: {e}")

    # send_command с нормальным timeout на существующее устройство
    logger.info("\nОтправляем команду на существующую лампу (timeout=5.0s)...")
    try:
        ack = await controller.send_command(
            target_device_id="living_light",
            method="toggle",
            params={},
            timeout=5.0,
        )
        logger.info(f"ACK получен: {ack.data if hasattr(ack, 'data') else ack}")
    except asyncio.TimeoutError:
        logger.warning("TimeoutError — лампа не ответила за 5s")
    except Exception as e:
        logger.error(f"Ошибка: {e}")


# =====================================================================
# Демонстрация: send_command с ошибкой — сценарий ошибки
# =====================================================================

async def demo_send_command_error(controller, light):
    """Показывает сценарий, когда целевое устройство вызывает ошибку.

    Если команда на целевом устройстве вызывает исключение,
    ACK содержит информацию об ошибке, или send_command
    может вызвать исключение.
    """
    logger.info("\n=== Демонстрация: send_command с ошибкой ===")

    # Отправляем команду, которая не существует на целевом устройстве
    logger.info("Отправляем несуществующую команду 'nonexistent_cmd'...")
    try:
        ack = await controller.send_command(
            target_device_id="living_light",
            method="nonexistent_cmd",
            params={},
            timeout=5.0,
        )
        logger.info(f"ACK получен: {ack.data if hasattr(ack, 'data') else ack}")
    except asyncio.TimeoutError:
        logger.warning("TimeoutError — возможно, команда не найдена и ACK не отправлен")
    except Exception as e:
        logger.info(f"Ошибка перехвачена: {type(e).__name__}: {e}")

    # Отправляем команду с неверными параметрами
    logger.info("\nОтправляем команду set_brightness с неверным типом параметра...")
    try:
        ack = await controller.send_command(
            target_device_id="living_light",
            method="set_brightness",
            params={"value": "not_a_number"},  # строка вместо int
            timeout=5.0,
        )
        logger.info(f"ACK: {ack.data if hasattr(ack, 'data') else ack}")
    except asyncio.TimeoutError:
        logger.warning("TimeoutError")
    except Exception as e:
        logger.info(f"Ошибка: {type(e).__name__}: {e}")


# =====================================================================
# Демонстрация: cross-device rule подробно
# =====================================================================

async def demo_cross_device_rule_detailed(app, sensor, light, controller):
    """Подробно показывает cross-device взаимодействие.

    Cross-device rule — это правило, которое реагирует на изменение
    состояния одного устройства и выполняет действие на другом.

    В Kamio это реализуется через:
    1. EventBus: подписка на device_state_changed с filter_fn
    2. send_command: отправка команды на целевое устройство
    3. app.devices: прямой доступ к экземпляру устройства

    Здесь показан полный цикл: sensor → EventBus → controller → light.
    """
    logger.info("\n=== Демонстрация: cross-device rule подробно ===")

    # Показываем конфигурацию контроллера
    logger.info(f"Контроллер: target_light_id={controller.target_light_id!r}")
    logger.info(f"Контроллер: target_sensor_id={controller.target_sensor_id!r}")
    logger.info(f"Контроллер: auto_mode={controller.auto_mode}")

    # Шаг 1: датчик обнаруживает движение
    logger.info("\nШаг 1: Датчик обнаруживает движение (presence=True)...")
    await sensor.handle_state({"presence": True})
    await asyncio.sleep(1)

    # Шаг 2: EventBus доставляет событие device_state_changed контроллеру
    logger.info("Шаг 2: EventBus доставляет событие контроллеру...")
    logger.info(f"  (filter_fn проверяет device_id={controller.target_sensor_id!r} и field='presence')")

    # Шаг 3: Контроллер вызывает send_command на лампу
    logger.info("Шаг 3: Контроллер вызывает send_command('living_light', 'toggle')...")
    await asyncio.sleep(2)

    # Шаг 4: Лампа выполняет toggle и отправляет ACK
    logger.info(f"Шаг 4: Лампа получила toggle, power={light.power}")
    logger.info(f"  ACK отправлен обратно контроллеру")

    # Шаг 5: Сброс — датчик сбрасывает presence
    logger.info("\nШаг 5: Сброс presence=False...")
    await sensor.handle_state({"presence": False})
    await asyncio.sleep(1)

    logger.info(f"Итоговое состояние: light.power={light.power}, sensor.presence={sensor.presence}")
    logger.info(f"Контроллер last_action={controller.last_action!r}")


# =====================================================================
# Демонстрация: app.devices итерация — перебор всех устройств
# =====================================================================

def demo_devices_iteration(app):
    """Показывает итерацию по app.devices и доступ к свойствам устройств.

    app.devices — это dict {device_id: Device_instance}.
    Позволяет получить прямой доступ к любому устройству без send_command.
    """
    logger.info("\n=== Демонстрация: app.devices итерация ===")

    logger.info(f"Всего устройств: {len(app.devices)}")

    # Итерация с получением снимков состояния
    for dev_id, dev in app.devices.items():
        state = dev.get_state_snapshot()
        logger.info(f"  {dev_id} ({dev.device_type()}):")
        for field_name, value in state.items():
            logger.info(f"    {field_name} = {value!r}")

    # Доступ к конкретному устройству
    light = app.devices.get("living_light")
    if light:
        logger.info(f"\nПрямой доступ к living_light:")
        logger.info(f"  power = {light.power}")
        logger.info(f"  brightness = {light.brightness}")
        logger.info(f"  device_type = {light.device_type()}")
        logger.info(f"  has driver = {light.driver is not None}")
        logger.info(f"  has node = {light.node is not None}")
        if light.node:
            logger.info(f"  node.device_id = {light.node.device_id}")

    # Проверка наличия устройства
    logger.info(f"\n'living_light' in app.devices: {'living_light' in app.devices}")
    logger.info(f"'nonexistent' in app.devices: {'nonexistent' in app.devices}")

    # Получение списка всех device_id
    all_ids = list(app.devices.keys())
    logger.info(f"Все device_id: {all_ids}")


# =====================================================================
# Демонстрация: unregister_async_callback — удаление callback
# =====================================================================

async def demo_unregister_async_callback(app, listener):
    """Показывает удаление async callback через unregister_async_callback.

    register_async_callback создаёт внутренний CustomNode для подписки
    на MQTT-топик. unregister_async_callback находит и удаляет этот узел.

    После удаления сообщения из указанного топика больше не доставляются
    callback-функции.
    """
    logger.info("\n=== Демонстрация: unregister_async_callback ===")

    target_topic = listener.target_topic
    logger.info(f"Listener подписан на: {target_topic}")
    logger.info(f"  message_count до: {listener.message_count}")

    # Отправляем сообщение — listener должен получить
    logger.info("Отправляем тестовое сообщение...")
    app.mqtt_client.publish(target_topic, b"test_before_unregister", qos=1)
    await asyncio.sleep(1)
    logger.info(f"  message_count после сообщения: {listener.message_count}")

    # Отписываем callback
    logger.info(f"Вызываем listener.unregister_async_callback('{target_topic}')...")
    listener.unregister_async_callback(target_topic)
    logger.info("Callback отписан")

    # Отправляем ещё одно сообщение — listener не должен получить
    logger.info("Отправляем сообщение после отписки...")
    app.mqtt_client.publish(target_topic, b"test_after_unregister", qos=1)
    await asyncio.sleep(1)
    logger.info(f"  message_count после отписки: {listener.message_count} (не должно увеличиться)")


# =====================================================================
# Демонстрация: reinitialize с driver — реинициализация устройства
# =====================================================================

async def demo_reinitialize_with_driver(app, sensor):
    """Подробно показывает reinitialize() для устройства с драйвером.

    reinitialize() выполняет:
    1. on_stop() — останавливает устройство (disconnect драйвера, отмена задач)
    2. driver.connect() — переподключение драйвера (если есть)
    3. on_start() — перезапуск устройства (telemetry, keepalive)

    Если driver.connect() вызывает ошибку, on_start() НЕ вызывается.
    Устройство остаётся в остановленном состоянии, ошибка пробрасывается.
    """
    logger.info("\n=== Демонстрация: reinitialize с driver ===")

    # Проверяем текущее состояние
    logger.info(f"До reinitialize:")
    logger.info(f"  sensor.presence = {sensor.presence}")
    logger.info(f"  sensor.last_motion = {sensor.last_motion!r}")
    logger.info(f"  sensor.driver = {sensor.driver}")

    # Вызываем reinitialize
    logger.info("\nВызываем sensor.reinitialize()...")
    try:
        await sensor.reinitialize()
        logger.info("reinitialize() завершён успешно")
        logger.info("  on_stop → (driver disconnect) → driver.connect → on_start")
    except Exception as e:
        logger.error(f"reinitialize() вызвал ошибку: {e}")
        logger.info("  Устройство осталось в остановленном состоянии")

    # Проверяем состояние после
    logger.info(f"\nПосле reinitialize:")
    logger.info(f"  sensor.presence = {sensor.presence}")
    logger.info(f"  sensor.last_motion = {sensor.last_motion!r}")

    # reinitialize с драйвером, который не может переподключиться
    from kamio.drivers.mock import MockHardwareDriver

    failing_driver = MockHardwareDriver(
        latency_range=(0.0, 0.01),
        failure_rate=1.0,  # 100% сбоев — connect вызовет ConnectionError
    )

    logger.info("\nСоздаём устройство с failing driver (failure_rate=1.0)...")
    try:
        failing_device = await app.add_device(
            "failing_device",
            PresenceSensor,
            driver=failing_driver,
        )
        logger.info("Устройство создано (драйвер мог не подключиться)")

        # Пытаемся reinitialize — driver.connect вызовет ConnectionError
        logger.info("Пытаемся reinitialize с failing driver...")
        try:
            await failing_device.reinitialize()
        except Exception as e:
            logger.info(f"Ожидаемая ошибка reinitialize: {type(e).__name__}: {e}")
            logger.info("  on_start НЕ вызван — устройство остановлено")

        # Очистка
        await app.remove_device("failing_device")
    except Exception as e:
        logger.info(f"Устройство с failing driver не создано: {e}")


if __name__ == "__main__":
    asyncio.run(main())
