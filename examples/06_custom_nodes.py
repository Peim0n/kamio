"""
06 — Кастомные MQTT-узлы
=========================

Демонстрирует создание пользовательских MQTT-узлов (CustomNode) для
обработки сообщений на произвольных топиках, не привязанных к
устройствам kamio.

Запуск (требуется MQTT-брокер на localhost:1883)::

    python examples/06_custom_nodes.py

Что демонстрирует:
    - CustomNode subclass с start(), stop(), handle_message()
    - subscribe() — подписка на топик относительно topic_prefix
    - subscribe_absolute() — подписка на абсолютный топик
    - publish() и publish_absolute() — публикация сообщений
    - register_custom_node / unregister_custom_node
    - Мост (bridge): пересылка сообщений между топиками
    - Логгер входящих сообщений
    - Преобразователь формата JSON → простой текст
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional

from kamio import KamioApp, CustomNode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("custom_nodes_demo")


# =====================================================================
# Пример 1: Логгер сообщений
# =====================================================================

class MessageLoggerNode(CustomNode):
    """
    Простой узел, который подписывается на топик и логирует
    все входящие сообщения.

    Демонстрирует:
        - Минимальная реализация CustomNode (start, stop, handle_message)
        - subscribe() — подписка относительно topic_prefix
        - Логирование payload
    """

    def __init__(self, mqtt_client, topic_prefix: str) -> None:
        super().__init__(mqtt_client, topic_prefix)

    async def start(self) -> None:
        """Вызывается при старте узла. Здесь оформляем подписки.

        subscribe("data/#") подпишется на "<topic_prefix>/data/#",
        то есть на все вложенные топики.
        """
        # Подписываемся на все сообщения внутри нашего префикса
        self.subscribe("#", qos=1)
        self._is_running = True
        self.logger.info(f"MessageLoggerNode запущен, слушает '{self.topic_prefix}/#'")

    async def stop(self) -> None:
        """Вызывается при остановке узла.

        Базовый класс CustomNode.stop() автоматически отписывается
        от всех топиков, зарегистрированных через subscribe().
        Вызываем super().stop() для очистки.
        """
        await super().stop()
        self.logger.info("MessageLoggerNode остановлен")

    async def handle_message(self, topic: str, payload: bytes) -> None:
        """Вызывается для каждого сообщения, топик которого начинается
        с topic_prefix.

        Args:
            topic: Полный MQTT-топик.
            payload: Сырые байты сообщения.
        """
        # Декодируем payload для логирования
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError:
            text = repr(payload)

        self.logger.info(f"📨 [логгер] {topic}: {text}")


# =====================================================================
# Пример 2: Мост (bridge) — пересылка сообщений между топиками
# =====================================================================

class TopicBridgeNode(CustomNode):
    """
    Узел-мост: пересылает сообщения из одного топика в другой.

    Слушает входящие сообщения на source_prefix и публикует их
    в target_prefix, сохраняя или преобразуя payload.

    Демонстрирует:
        - subscribe_absolute() — подписка на абсолютный топик
        - publish_absolute() — публикация в абсолютный топик
        - Пересылка сообщений между разными ветками MQTT
    """

    def __init__(
        self,
        mqtt_client,
        topic_prefix: str,
        source_topic: str,
        target_topic: str,
    ) -> None:
        """
        Args:
            mqtt_client:   gmqtt.Client для MQTT-коммуникации.
            topic_prefix:  Префикс узла (используется для matches()).
            source_topic:  Абсолютный топик для подписки (откуда брать).
            target_topic:  Абсолютный топик для публикации (куда отправлять).
        """
        super().__init__(mqtt_client, topic_prefix)
        self._source_topic = source_topic
        self._target_topic = target_topic

    async def start(self) -> None:
        """Подписываемся на абсолютный топик-источник."""
        # subscribe_absolute() подписывается на топик БЕЗ добавления
        # topic_prefix. Это позволяет слушать топики вне пространства
        # имён узла (например, чужие устройства).
        self.subscribe_absolute(self._source_topic, qos=1)
        self._is_running = True
        self.logger.info(
            f"TopicBridgeNode запущен: {self._source_topic} → {self._target_topic}"
        )

    async def stop(self) -> None:
        """Останавливаем узел и очищаем подписки."""
        await super().stop()
        self.logger.info("TopicBridgeNode остановлен")

    async def handle_message(self, topic: str, payload: bytes) -> None:
        """Пересылаем сообщение из source в target.

        Топик-источник совпадает с self._source_topic, на который
        мы подписались через subscribe_absolute().
        """
        # Публикуем в целевой топик через publish_absolute().
        # publish_absolute() публикует БЕЗ добавления topic_prefix.
        self.publish_absolute(self._target_topic, payload, qos=1)
        self.logger.info(
            f"🌉 [мост] {topic} → {self._target_topic} "
            f"({len(payload)} байт)"
        )


# =====================================================================
# Пример 3: Преобразователь JSON → текст
# =====================================================================

class JsonToTextNode(CustomNode):
    """
    Узел-преобразователь: принимает JSON-сообщения и публикует
    человекочитаемый текст в другой топик.

    Демонстрирует:
        - subscribe() с wildcard-топиком
        - publish() — публикация относительно topic_prefix
        - Обработка и преобразование payload
    """

    def __init__(self, mqtt_client, topic_prefix: str, output_subtopic: str = "text") -> None:
        super().__init__(mqtt_client, topic_prefix)
        self._output_subtopic = output_subtopic

    async def start(self) -> None:
        """Подписываемся на входные JSON-данные."""
        # subscribe("json/#") → "<prefix>/json/#"
        self.subscribe("json/#", qos=1)
        self._is_running = True
        self.logger.info(
            f"JsonToTextNode запущен, слушает '{self.topic_prefix}/json/#', "
            f"публикует в '{self.topic_prefix}/{self._output_subtopic}/#'"
        )

    async def stop(self) -> None:
        """Останавливаем узел."""
        await super().stop()
        self.logger.info("JsonToTextNode остановлен")

    async def handle_message(self, topic: str, payload: bytes) -> None:
        """Разбираем JSON и публикуем текстовое представление.

        Если payload не является валидным JSON, публикуем сообщение
        об ошибке.
        """
        try:
            data = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            # Публикуем ошибку в выходной топик
            error_msg = f"ERROR: invalid JSON — {e}"
            self.publish(self._output_subtopic, error_msg, qos=1)
            self.logger.warning(f"⚠️ [json→text] Ошибка разбора: {e}")
            return

        # Преобразуем JSON в человекочитаемый текст
        lines = []
        if isinstance(data, dict):
            for key, value in data.items():
                lines.append(f"{key}: {value}")
        else:
            lines.append(str(data))

        text_output = "\n".join(lines)

        # Извлекаем последнюю часть топика для построения выходного топика
        # Например, "prefix/json/sensor1" → "prefix/text/sensor1"
        subtopic_part = topic.replace(f"{self.topic_prefix}/json/", "")
        output_topic = f"{self._output_subtopic}/{subtopic_part}"

        # publish() добавляет topic_prefix автоматически:
        # publish("text/sensor1") → "<prefix>/text/sensor1"
        self.publish(output_topic, text_output, qos=1)
        self.logger.info(f"📝 [json→text] {topic} → текст ({len(lines)} строк)")


# =====================================================================
# Пример 4: Счётчик сообщений с периодической публикацией статистики
# =====================================================================

class MessageCounterNode(CustomNode):
    """
    Узел, который считает сообщения и периодически публикует статистику.

    Демонстрирует:
        - Накопление состояния в узле
        - Периодическая публикация через asyncio-задачу
        - publish() для отправки результатов
    """

    def __init__(self, mqtt_client, topic_prefix: str, stats_interval: float = 5.0) -> None:
        super().__init__(mqtt_client, topic_prefix)
        self._stats_interval = stats_interval
        self._count: int = 0
        self._stats_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Подписываемся на сообщения и запускаем задачу статистики."""
        self.subscribe("input/#", qos=0)
        self._is_running = True

        # Запускаем периодическую публикацию статистики
        self._stats_task = asyncio.create_task(self._publish_stats())
        self.logger.info(
            f"MessageCounterNode запущен, статистика каждые {self._stats_interval}с"
        )

    async def stop(self) -> None:
        """Останавливаем задачу статистики и очищаем подписки."""
        if self._stats_task and not self._stats_task.done():
            self._stats_task.cancel()
            try:
                await self._stats_task
            except asyncio.CancelledError:
                pass
        await super().stop()
        self.logger.info(f"MessageCounterNode остановлен (всего сообщений: {self._count})")

    async def handle_message(self, topic: str, payload: bytes) -> None:
        """Увеличиваем счётчик при каждом сообщении."""
        self._count += 1
        self.logger.debug(f"🔢 [счётчик] Сообщение #{self._count} на {topic}")

    async def _publish_stats(self) -> None:
        """Периодически публикуем статистику в топик 'stats'."""
        try:
            while self._is_running:
                await asyncio.sleep(self._stats_interval)
                stats = {
                    "total_messages": self._count,
                    "topic_prefix": self.topic_prefix,
                }
                # publish() публикует относительно topic_prefix:
                # publish("stats", ...) → "<prefix>/stats"
                self.publish("stats", json.dumps(stats), qos=1)
                self.logger.info(f"📊 [счётчик] Статистика: {stats}")
        except asyncio.CancelledError:
            self.logger.debug("Stats task отменена")
            raise


# =====================================================================
# Основная функция
# =====================================================================

async def main():
    logger.info("=== Демонстрация кастомных MQTT-узлов kamio ===\n")

    # --- Демонстрация matches() без MQTT-брокера ---
    demo_matches_logic()

    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="custom_nodes_demo")

    # --- Создаём узлы ---
    # Передаём app.mqtt_client в конструктор каждого узла.

    # 1. Логгер сообщений — слушает всё в "sensors/#"
    logger_node = MessageLoggerNode(
        mqtt_client=app.mqtt_client,
        topic_prefix="sensors",
    )

    # 2. Мост — пересылает из "raw/data" в "processed/data"
    bridge_node = TopicBridgeNode(
        mqtt_client=app.mqtt_client,
        topic_prefix="bridge",
        source_topic="raw/data",
        target_topic="processed/data",
    )

    # 3. JSON → текст — преобразует JSON в читаемый текст
    json_node = JsonToTextNode(
        mqtt_client=app.mqtt_client,
        topic_prefix="converter",
    )

    # 4. Счётчик сообщений — считает и публикует статистику
    counter_node = MessageCounterNode(
        mqtt_client=app.mqtt_client,
        topic_prefix="counter",
        stats_interval=5.0,
    )

    # 5. AsyncPublisher — демонстрация publish_async()
    async_publisher_node = AsyncPublisherNode(
        mqtt_client=app.mqtt_client,
        topic_prefix="async_pub",
    )

    # 6. LifecycleAwareNode — демонстрация on_connect/on_disconnect
    lifecycle_node = LifecycleAwareNode(
        mqtt_client=app.mqtt_client,
        topic_prefix="lifecycle",
    )

    # 7. HeartbeatNode — демонстрация фоновой задачи в узле
    heartbeat_node = HeartbeatNode(
        mqtt_client=app.mqtt_client,
        topic_prefix="heartbeat",
        interval=2.0,
    )

    # --- Регистрируем узлы ---
    # register_custom_node добавляет узел в менеджер.
    # Если приложение уже запущено, узел стартует немедленно.
    # Если нет — узел стартует при вызове app.start().
    app.register_custom_node("logger", logger_node)
    app.register_custom_node("bridge", bridge_node)
    app.register_custom_node("json_converter", json_node)
    app.register_custom_node("counter", counter_node)
    app.register_custom_node("async_publisher", async_publisher_node)
    app.register_custom_node("lifecycle", lifecycle_node)
    app.register_custom_node("heartbeat", heartbeat_node)

    logger.info(f"Зарегистрированные узлы: {app.list_custom_nodes()}")

    # --- Запускаем приложение ---
    # app.start() вызовет start() у всех зарегистрированных узлов.
    await app.start()

    # --- Демонстрация: отправляем тестовые сообщения ---
    logger.info("\n--- Тест 1: логгер сообщений ---")
    # Публикуем напрямую через MQTT-клиент (имитируем внешнее устройство)
    app.mqtt_client.publish("sensors/temperature", b"23.5", qos=1)
    app.mqtt_client.publish("sensors/humidity", b"48.0", qos=1)
    await asyncio.sleep(1.0)

    logger.info("\n--- Тест 2: мост между топиками ---")
    # Отправляем в raw/data — мост должен переслать в processed/data
    app.mqtt_client.publish("raw/data", b"hello_from_bridge", qos=1)
    await asyncio.sleep(1.0)

    logger.info("\n--- Тест 3: JSON → текст ---")
    # Отправляем JSON в converter/json/sensor1
    json_payload = json.dumps({"temperature": 22.5, "humidity": 45, "pressure": 1013}).encode()
    app.mqtt_client.publish("converter/json/sensor1", json_payload, qos=1)
    await asyncio.sleep(1.0)

    # Отправляем невалидный JSON для демонстрации обработки ошибок
    app.mqtt_client.publish("converter/json/sensor2", b"not a json", qos=1)
    await asyncio.sleep(1.0)

    logger.info("\n--- Тест 4: счётчик сообщений ---")
    # Отправляем несколько сообщений в counter/input/*
    for i in range(5):
        app.mqtt_client.publish(f"counter/input/msg{i}", f"message_{i}".encode(), qos=0)
    await asyncio.sleep(0.5)

    # Ждём публикации статистики (интервал 5 сек)
    logger.info("Ожидание статистики счётчика (6 сек)...")
    await asyncio.sleep(6)

    logger.info("\n--- Тест 5: publish_async (неблокирующая публикация) ---")
    # Отправляем триггер в async_pub/trigger — узел ответит через publish_async
    app.mqtt_client.publish("async_pub/trigger", b"test_async", qos=1)
    await asyncio.sleep(1.0)

    logger.info("\n--- Тест 6: lifecycle hooks (on_connect уже сработал) ---")
    lifecycle = app.get_custom_node("lifecycle")
    if lifecycle:
        logger.info(f"LifecycleAwareNode: connects={lifecycle._connect_count}, disconnects={lifecycle._disconnect_count}")

    logger.info("\n--- Тест 7: heartbeat (фоновая задача) ---")
    heartbeat = app.get_custom_node("heartbeat")
    if heartbeat:
        logger.info(f"HeartbeatNode: beat_count={heartbeat._beat_count} (публикует каждые 2s)")
    await asyncio.sleep(3)
    if heartbeat:
        logger.info(f"HeartbeatNode после ожидания: beat_count={heartbeat._beat_count}")

    # --- Демонстрация: доступ к узлу по имени ---
    logger.info("\n--- Доступ к узлам ---")
    node = app.get_custom_node("logger")
    if node:
        logger.info(f"Узел 'logger': {node}")

    # --- Демонстрация: unregister_custom_node ---
    logger.info("\n--- Удаление узла 'json_converter' ---")
    app.unregister_custom_node("json_converter")
    logger.info(f"Оставшиеся узлы: {app.list_custom_nodes()}")

    # --- Останавливаем ---
    logger.info("\n--- Завершение ---")
    await app.stop()
    logger.info("Демонстрация завершена")


# =====================================================================
# Демонстрация: publish_async (non-blocking)
# =====================================================================

class AsyncPublisherNode(CustomNode):
    """Узел, демонстрирующий publish_async() — неблокирующую публикацию.

    publish_async() использует asyncio.to_thread() для вызова
    mqtt_client.publish() в отдельном потоке. Это полезно, когда
    публикация может заблокировать event loop (например, при большом
    payload или медленном брокере).

    В отличие от publish() (синхронный вызов mqtt_client.publish),
    publish_async() не блокирует текущий корутин.
    """

    def __init__(self, mqtt_client, topic_prefix: str) -> None:
        super().__init__(mqtt_client, topic_prefix)
        self._publish_count = 0

    async def start(self) -> None:
        self.subscribe("trigger", qos=1)
        self._is_running = True
        self.logger.info(f"AsyncPublisherNode запущен, слушает '{self.topic_prefix}/trigger'")

    async def stop(self) -> None:
        await super().stop()
        self.logger.info("AsyncPublisherNode остановлен")

    async def handle_message(self, topic: str, payload: bytes) -> None:
        """При получении триггера — публикуем ответ через publish_async."""
        self._publish_count += 1
        response = f"async_response_{self._publish_count}: {payload.decode('utf-8', errors='replace')}"

        # publish_async — неблокирующая публикация (через asyncio.to_thread)
        await self.publish_async("response", response, qos=1)
        self.logger.info(f"📤 [async publish] Опубликовано: {response}")

        # Для сравнения: синхронный publish() (может блокировать event loop)
        # self.publish("response_sync", response, qos=1)


# =====================================================================
# Демонстрация: on_connect / on_disconnect hooks
# =====================================================================

class LifecycleAwareNode(CustomNode):
    """Узел с реализованными on_connect() и on_disconnect() хуками.

    CustomNode предоставляет два опциональных хука:
    - on_connect(): вызывается после установления MQTT-соединения с брокером
    - on_disconnect(): вызывается после разрыва MQTT-соединения

    Эти хуки полезны для:
    - Восстановления подписок после реконнекта
    - Очистки кэша при отключении
    - Уведомления внешних систем о состоянии соединения
    """

    def __init__(self, mqtt_client, topic_prefix: str) -> None:
        super().__init__(mqtt_client, topic_prefix)
        self._connect_count = 0
        self._disconnect_count = 0

    async def start(self) -> None:
        self.subscribe("data", qos=1)
        self._is_running = True
        self.logger.info(f"LifecycleAwareNode запущен, слушает '{self.topic_prefix}/data'")

    async def stop(self) -> None:
        await super().stop()
        self.logger.info("LifecycleAwareNode остановлен")

    async def on_connect(self) -> None:
        """Вызывается после подключения к MQTT-брокеру."""
        self._connect_count += 1
        self.logger.info(
            f"🔗 [on_connect] MQTT-соединение установлено "
            f"(всего подключений: {self._connect_count})"
        )
        # Здесь можно восстановить подписки или отправить статус "online"

    async def on_disconnect(self) -> None:
        """Вызывается после отключения от MQTT-брокера."""
        self._disconnect_count += 1
        self.logger.info(
            f"🔌 [on_disconnect] MQTT-соединение разорвано "
            f"(всего отключений: {self._disconnect_count})"
        )
        # Здесь можно очистить кэш или отправить статус "offline"

    async def handle_message(self, topic: str, payload: bytes) -> None:
        text = payload.decode("utf-8", errors="replace")
        self.logger.info(f"📨 [lifecycle] {topic}: {text} (connects={self._connect_count})")


# =====================================================================
# Демонстрация: node с фоновыми задачами (create_task)
# =====================================================================

class HeartbeatNode(CustomNode):
    """Узел с фоновой задачей — периодически публикует heartbeat.

    Демонстрирует создание и управление фоновой задачей внутри узла.
    Задача запускается в start() и отменяется в stop().

    Важно: все фоновые задачи должны быть отменены в stop(),
    чтобы избежать утечек ресурсов и предупреждений asyncio.
    """

    def __init__(self, mqtt_client, topic_prefix: str, interval: float = 2.0) -> None:
        super().__init__(mqtt_client, topic_prefix)
        self._interval = interval
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._beat_count = 0

    async def start(self) -> None:
        self._is_running = True
        # Запускаем фоновую задачу heartbeat
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self.logger.info(
            f"HeartbeatNode запущен, heartbeat каждые {self._interval}s"
        )

    async def stop(self) -> None:
        # Отменяем фоновую задачу перед остановкой
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
        await super().stop()
        self.logger.info(f"HeartbeatNode остановлен (всего heartbeat: {self._beat_count})")

    async def _heartbeat_loop(self) -> None:
        """Фоновый цикл: публикует heartbeat-сообщение периодически."""
        try:
            while self._is_running:
                self._beat_count += 1
                # Публикуем heartbeat через publish (синхронный)
                self.publish("heartbeat", f"beat_{self._beat_count}", qos=0)
                self.logger.debug(f"💓 heartbeat #{self._beat_count}")
                await asyncio.sleep(self._interval)
        except asyncio.CancelledError:
            self.logger.debug("Heartbeat loop отменена")
            raise

    async def handle_message(self, topic: str, payload: bytes) -> None:
        """HeartbeatNode не обрабатывает входящие сообщения."""
        pass


# =====================================================================
# Демонстрация: маршрутизация сообщений (route_message)
# =====================================================================

# CustomNodeManager.route_message(topic, payload) — внутренний метод,
# который вызывается KamioApp при получении каждого MQTT-сообщения.
# Он проверяет все зарегистрированные узлы через node.matches(topic)
# и вызывает handle_message() для каждого совпавшего узла.
#
# Возвращает True, если хотя бы один узел обработал сообщение.
# Это позволяет KamioApp знать, было ли сообщение обработано
# кастомным узлом, или его нужно передать стандартным device-узлам.
#
# Пример маршрутизации:
#   Узел "logger" с topic_prefix="sensors" → matches("sensors/temp") = True
#   Узел "bridge" с topic_prefix="bridge" → matches("sensors/temp") = False
#   Узел "converter" с topic_prefix="converter" → matches("converter/json/x") = True
#
# Если несколько узлов совпадают с одним топиком, сообщение
# доставляется каждому из них в порядке регистрации.


# =====================================================================
# Демонстрация: matches() логика (prefix matching)
# =====================================================================

def demo_matches_logic():
    """
    Показывает логику работы matches() — проверки соответствия топика.

    CustomNode.matches(topic) возвращает True, если:
    - topic == topic_prefix (точное совпадение)
    - topic начинается с topic_prefix + "/" (префикс с разделителем)

    Это используется CustomNodeManager для маршрутизации сообщений:
    только узлы, у которых matches() вернул True, получают сообщение.
    """
    logger.info("=== Демонстрация: matches() логика ===")

    # Создаём mock MQTT-клиент (не подключаемся к брокеру)
    class MockMqtt:
        def subscribe(self, *a, **kw): pass
        def unsubscribe(self, *a, **kw): pass
        def publish(self, *a, **kw): pass

    mock_mqtt = MockMqtt()

    # Узел с префиксом "sensors"
    node = MessageLoggerNode(mock_mqtt, "sensors")
    logger.info(f"Узел: topic_prefix={node.topic_prefix!r}")

    # Точное совпадение
    result = node.matches("sensors")
    logger.info(f"matches('sensors') → {result} (точное совпадение)")

    # Совпадение с подпунктом
    result = node.matches("sensors/temperature")
    logger.info(f"matches('sensors/temperature') → {result} (префикс + /)")

    result = node.matches("sensors/humidity/room1")
    logger.info(f"matches('sensors/humidity/room1') → {result} (глубокий подпункт)")

    # Не совпадает — другой префикс
    result = node.matches("actuators/pump")
    logger.info(f"matches('actuators/pump') → {result} (другой префикс)")

    # Не совпадает — "sensorsX" начинается с "sensors", но не с "sensors/"
    result = node.matches("sensorsX")
    logger.info(f"matches('sensorsX') → {result} (похожий, но не префикс)")

    # Пустой топик
    result = node.matches("")
    logger.info(f"matches('') → {result} (пустой топик)")

    # __repr__ узла
    logger.info(f"repr: {repr(node)}")

    logger.info("Демонстрация matches() завершена\n")


if __name__ == "__main__":
    asyncio.run(main())
