"""
05 — Плагины
=============

Демонстрирует систему плагинов kamio: от создания собственных плагинов
до загрузки встроенных и управления жизненным циклом.

Запуск (требуется MQTT-брокер на localhost:1883)::

    python examples/05_plugins.py

Что демонстрирует:
    - Создание собственного плагина (Plugin subclass)
    - Свойства name, version, description, dependencies
    - Методы on_load, on_unload, subscribe_events, register_hooks
    - PluginContext: subscribe, register_hook, create_task, add_rule
    - Плагин с зависимостями (автозагрузка зависимостей)
    - Загрузка встроенных плагинов (LoggingPlugin, MetricsPlugin)
    - load_plugin, unload_plugin, get_plugin, list_plugins
    - load_plugins_from_directory
"""
from __future__ import annotations

import asyncio
import logging
import os
import tempfile
from typing import Any, Dict, Optional

from kamio import KamioApp, Device, Plugin, RuleEvent, command, state
from kamio.plugins.loader import PluginContext

# Встроенные плагины
from kamio.plugins.builtin import LoggingPlugin, MetricsPlugin

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("plugins_demo")


# =====================================================================
# Пример 1: Простой плагин — уведомления о событиях устройств
# =====================================================================

class DeviceAlertPlugin(Plugin):
    """
    Плагин уведомлений: логирует добавление/удаление устройств
    и изменения состояния.

    Демонстрирует:
        - name, version, description (обязательные свойства)
        - on_load / on_unload (жизненный цикл)
        - subscribe_events (подписка на события через PluginContext)
        - register_hooks (хуки жизненного цикла через PluginContext)
        - configure() — приём конфигурации
    """

    @property
    def name(self) -> str:
        """Уникальное имя плагина (используется как идентификатор)."""
        return "device_alerts"

    @property
    def version(self) -> str:
        """Версия плагина (рекомендуется semver)."""
        return "1.0.0"

    @property
    def description(self) -> str:
        """Краткое описание для логов и UI."""
        return "Уведомления о событиях устройств: добавление, удаление, изменение состояния."

    def __init__(self) -> None:
        super().__init__()
        self._alert_count: int = 0

    def configure(self, config: Dict[str, Any]) -> None:
        """Применить конфигурацию. Вызывается ДО on_load.

        config передаётся из load_plugin(plugin_class, config={...}).
        """
        super().configure(config)
        # Читаем настройки из конфигурации
        self._log_state_changes: bool = config.get("log_state_changes", True)

    async def on_load(self, app: KamioApp, context: Optional[PluginContext] = None) -> None:
        """Вызывается при загрузке плагина.

        context — PluginContext, предоставляющий scoped API:
            - context.subscribe(event_type, callback) — подписка на события
            - context.register_hook(event_type, hook) — регистрация хука
            - context.add_rule(func, **kwargs) — добавление правила
            - context.create_task(coro) — фоновая задача
        Все регистрации автоматически очищаются при выгрузке плагина.
        """
        self.logger.info("DeviceAlertPlugin загружен")
        self._alert_count = 0

    async def on_unload(self, app: KamioApp) -> None:
        """Вызывается при выгрузке плагина.

        Очищайте ресурсы здесь. PluginContext автоматически отписывает
        события и хуки, отменяет задачи — вручную делать это не нужно.
        """
        self.logger.info(f"DeviceAlertPlugin выгружен (всего уведомлений: {self._alert_count})")

    def subscribe_events(self, context: PluginContext) -> None:
        """Подписка на события EventBus. Вызывается автоматически после on_load.

        context.subscribe регистрирует callback и запоминает его
        для автоматической отписки при unload.
        """
        # Подписываемся на события добавления/удаления устройств
        context.subscribe("device_added", self._on_device_added)
        context.subscribe("device_removed", self._on_device_removed)

        # Условная подписка — только если включено в конфигурации
        if self._log_state_changes:
            context.subscribe("device_state_changed", self._on_state_changed)

    def register_hooks(self, context: PluginContext) -> None:
        """Регистрация хуков жизненного цикла. Вызывается автоматически после on_load.

        context.register_hook регистрирует хук и запоминает его
        для автоматического удаления при unload.
        """
        # Хук срабатывает после старта приложения
        context.register_hook("on_after_start", self._on_app_started)

    # --- Callback-методы ---

    def _on_device_added(self, data: dict) -> None:
        """Callback для события 'device_added'."""
        self._alert_count += 1
        device_id = data.get("device_id", "?")
        device_type = data.get("device_type", "?")
        self.logger.info(f"🔔 [плагин] Устройство добавлено: "
                         f"id='{device_id}', type='{device_type}'")

    def _on_device_removed(self, data: dict) -> None:
        """Callback для события 'device_removed'."""
        self._alert_count += 1
        device_id = data.get("device_id", "?")
        self.logger.info(f"🔔 [плагин] Устройство удалено: id='{device_id}'")

    def _on_state_changed(self, data: dict) -> None:
        """Callback для события 'device_state_changed'."""
        self._alert_count += 1
        device_id = data.get("device_id", "?")
        field = data.get("field", "?")
        old_val = data.get("old_value")
        new_val = data.get("new_value")
        self.logger.info(f"🔔 [плагин] Состояние изменено: "
                         f"'{device_id}.{field}': {old_val} → {new_val}")

    async def _on_app_started(self, *args, **kwargs) -> None:
        """Хук для события 'on_after_start'."""
        self.logger.info("🔔 [плагин] Приложение запущено — "
                         "DeviceAlertPlugin активен")


# =====================================================================
# Пример 2: Плагин с фоновой задачей и правилом через PluginContext
# =====================================================================

class HealthCheckPlugin(Plugin):
    """
    Плагин мониторинга здоровья: периодически проверяет устройства
    и публикует отчёт.

    Демонстрирует:
        - context.create_task() — фоновые задачи
        - context.add_rule() — регистрация правил
        - Взаимодействие с app.devices
    """

    @property
    def name(self) -> str:
        return "health_check"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Периодическая проверка состояния всех устройств."

    def __init__(self) -> None:
        super().__init__()
        self._check_interval: float = 5.0

    def configure(self, config: Dict[str, Any]) -> None:
        super().configure(config)
        self._check_interval = config.get("check_interval", 5.0)

    async def on_load(self, app: KamioApp, context: Optional[PluginContext] = None) -> None:
        self.logger.info(f"HealthCheckPlugin загружен (интервал={self._check_interval}с)")

        # Запускаем фоновую задачу через PluginContext.
        # При выгрузке плагина задача будет автоматически отменена.
        context.create_task(
            self._health_loop(app),
            name="health_check_loop",
        )

        # Регистрируем правило через PluginContext.
        # При выгрузке плагина правило будет автоматически удалено.
        context.add_rule(
            self._on_any_state_change,
            description="HealthCheck: логировать изменения состояния",
        )

    async def on_unload(self, app: KamioApp) -> None:
        self.logger.info("HealthCheckPlugin выгружен")

    async def _health_loop(self, app: KamioApp) -> None:
        """Фоновая задача: периодический опрос всех устройств."""
        try:
            while True:
                await asyncio.sleep(self._check_interval)
                device_count = len(app.devices)
                device_ids = list(app.devices.keys())
                self.logger.info(
                    f"❤️ [health_check] Активных устройств: {device_count} "
                    f"({', '.join(device_ids) if device_ids else 'нет'})"
                )
        except asyncio.CancelledError:
            self.logger.debug("Health check loop отменён")
            raise

    async def _on_any_state_change(self, event: RuleEvent, app: KamioApp) -> None:
        """Правило: срабатывает при любом изменении состояния любого устройства.

        Зарегистрировано через context.add_rule() без device и fields,
        поэтому срабатывает на все обновления.
        """
        if event.kind == "event" and event.data:
            self.logger.debug(
                f"❤️ [health_check] Изменение на '{event.device_id}': {event.data}"
            )


# =====================================================================
# Пример 3: Плагин с зависимостями
# =====================================================================

class NotificationPlugin(Plugin):
    """
    Плагин уведомлений, зависящий от device_alerts.

    Демонстрирует:
        - dependencies — список имён плагинов, которые должны быть загружены
        - Автоматическая загрузка зависимостей через register_class()
    """

    @property
    def name(self) -> str:
        return "notifications"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Отправка уведомлений (зависит от device_alerts)."

    @property
    def dependencies(self) -> list[str]:
        """Имена плагинов, которые должны быть загружены ДО этого.

        PluginLoader автоматически загрузит зависимости, если они
        зарегистрированы через plugin_loader.register_class(name, cls).
        """
        return ["device_alerts"]

    async def on_load(self, app: KamioApp, context: Optional[PluginContext] = None) -> None:
        self.logger.info("NotificationPlugin загружен (зависимость: device_alerts)")

        # Проверяем, что зависимость действительно загружена
        alerts = app.get_plugin("device_alerts")
        if alerts:
            self.logger.info(f"  Зависимость 'device_alerts' найдена: {alerts}")
        else:
            self.logger.warning("  Зависимость 'device_alerts' НЕ найдена!")

    async def on_unload(self, app: KamioApp) -> None:
        self.logger.info("NotificationPlugin выгружен")

    def subscribe_events(self, context: PluginContext) -> None:
        context.subscribe("device_added", self._notify)

    def _notify(self, data: dict) -> None:
        device_id = data.get("device_id", "?")
        self.logger.info(f"📧 [уведомление] Новое устройство: '{device_id}'")


# =====================================================================
# Пример 4: Плагин для загрузки из директории (файл-плагин)
# =====================================================================

# Этот код записывает временный .py-файл с плагином и загружает его
# через load_plugins_from_directory. В реальном приложении плагины
# лежат в отдельной папке (например, plugins/).

PLUGIN_FILE_CONTENT = '''\
"""Плагин из внешнего файла — загружается через load_plugins_from_directory."""
from __future__ import annotations

import logging
from typing import Optional

from kamio import KamioApp, Plugin
from kamio.plugins.loader import PluginContext


class ExternalPlugin(Plugin):
    """Простой плагин, загружаемый из .py-файла."""

    @property
    def name(self) -> str:
        return "external"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def description(self) -> str:
        return "Внешний плагин, загруженный из директории."

    async def on_load(self, app: KamioApp, context: Optional[PluginContext] = None) -> None:
        self.logger.info("ExternalPlugin загружен из файла!")

    async def on_unload(self, app: KamioApp) -> None:
        self.logger.info("ExternalPlugin выгружен")
'''


# =====================================================================
# Устройство для демонстрации
# =====================================================================

class DemoSwitch(Device):
    """Простой переключатель для демонстрации событий плагинов."""

    power: bool = state(default=False, writable=True, description="Питание")

    @command
    async def toggle(self):
        self.power = not self.power
        return {"power": self.power}


# =====================================================================
# Основная функция
# =====================================================================

async def main():
    logger.info("=== Демонстрация системы плагинов kamio ===\n")

    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="plugins_demo")

    # --- Регистрируем классы плагинов для разрешения зависимостей ---
    # register_class позволяет PluginLoader автоматически загружать
    # зависимости по имени при загрузке плагина.
    app.plugin_loader.register_class("device_alerts", DeviceAlertPlugin)

    # --- Загрузка плагина 1: DeviceAlertPlugin (с конфигурацией) ---
    logger.info("--- Загрузка DeviceAlertPlugin ---")
    await app.load_plugin(
        DeviceAlertPlugin,
        config={"log_state_changes": True},
    )

    # --- Загрузка плагина 2: HealthCheckPlugin ---
    logger.info("--- Загрузка HealthCheckPlugin ---")
    await app.load_plugin(
        HealthCheckPlugin,
        config={"check_interval": 5.0},
    )

    # --- Загрузка плагина 3: NotificationPlugin (с автозагрузкой зависимости) ---
    # NotificationPlugin зависит от "device_alerts".
    # Поскольку device_alerts уже загружен, зависимость удовлетворена.
    # Если бы он не был загружен, PluginLoader попытался бы загрузить его
    # через register_class (мы зарегистрировали его выше).
    logger.info("--- Загрузка NotificationPlugin (с зависимостью) ---")
    # Регистрируем и сам класс для полноты демонстрации
    app.plugin_loader.register_class("notifications", NotificationPlugin)
    await app.load_plugin(NotificationPlugin)

    # --- Загрузка встроенных плагинов ---
    logger.info("--- Загрузка встроенных плагинов ---")

    # LoggingPlugin — логирование всех событий в ротируемый файл
    await app.load_plugin(
        LoggingPlugin,
        config={
            "file": "kamio_events.log",
            "level": "INFO",
            "max_bytes": 5 * 1024 * 1024,  # 5 MB
            "backup_count": 2,
        },
    )

    # MetricsPlugin — сбор счётчиков событий в памяти
    await app.load_plugin(MetricsPlugin)

    # --- Проверка списка загруженных плагинов ---
    logger.info(f"\nЗагруженные плагины: {app.list_plugins()}")

    # --- Доступ к плагину по имени ---
    alerts = app.get_plugin("device_alerts")
    if alerts:
        logger.info(f"Плагин 'device_alerts': {alerts} v{alerts.version}")

    metrics = app.get_plugin("metrics")
    if metrics:
        logger.info(f"Плагин 'metrics': {metrics} v{metrics.version}")

    # --- Запуск приложения ---
    logger.info("\n--- Запуск приложения ---")
    await app.start()

    # --- Создаём устройство (вызовет события device_added) ---
    logger.info("\n--- Создание устройства ---")
    switch = await app.add_device("demo_switch", DemoSwitch)

    # --- Изменяем состояние (вызовет device_state_changed) ---
    logger.info("\n--- Изменение состояния ---")
    await switch.handle_state({"power": True})
    await asyncio.sleep(0.5)
    await switch.handle_state({"power": False})
    await asyncio.sleep(0.5)

    # --- Ждём срабатывания HealthCheckPlugin ---
    logger.info("\n--- Ожидание health check (6 сек) ---")
    await asyncio.sleep(6)

    # --- Читаем метрики из MetricsPlugin ---
    logger.info("\n--- Метрики ---")
    if metrics:
        all_metrics = await metrics.get_metrics()
        for event_type, count in all_metrics.items():
            logger.info(f"  {event_type}: {count}")

    # --- Демонстрация выгрузки плагина ---
    logger.info("\n--- Выгрузка плагина 'notifications' ---")
    await app.unload_plugin("notifications")
    logger.info(f"Оставшиеся плагины: {app.list_plugins()}")

    # --- Демонстрация load_plugins_from_directory ---
    logger.info("\n--- Загрузка плагинов из директории ---")
    with tempfile.TemporaryDirectory() as tmpdir:
        # Записываем файл плагина
        plugin_path = os.path.join(tmpdir, "external_plugin.py")
        with open(plugin_path, "w", encoding="utf-8") as f:
            f.write(PLUGIN_FILE_CONTENT)

        # Загружаем все плагины из директории
        loaded = await app.load_plugins_from_directory(tmpdir)
        logger.info(f"Загружено из директории: {[p.name for p in loaded]}")
        logger.info(f"Все плагины: {app.list_plugins()}")

    # --- Останавливаем ---
    logger.info("\n--- Завершение ---")
    await app.stop()
    logger.info("Демонстрация завершена")


# =====================================================================
# Дополнительные плагины для расширенных демонстраций
# =====================================================================

class ValidatedConfigPlugin(Plugin):
    """Плагин с валидацией конфигурации в configure().

    Демонстрирует:
        - configure() — проверка обязательных параметров
        - Генерация ValueError при неверной конфигурации
    """

    @property
    def name(self) -> str:
        return "validated_config"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Плагин с валидацией конфигурации в configure()."

    def __init__(self) -> None:
        super().__init__()
        self._max_devices: int = 10

    def configure(self, config: Dict[str, Any]) -> None:
        """Валидация конфигурации ДО on_load.

        Если обязательные параметры отсутствуют или некорректны,
        выбрасываем ValueError — плагин не будет загружен.
        """
        super().configure(config)

        # Проверяем обязательный параметр
        max_devices = config.get("max_devices")
        if max_devices is None:
            raise ValueError("validated_config: параметр 'max_devices' обязателен")

        if not isinstance(max_devices, int) or max_devices <= 0:
            raise ValueError(
                f"validated_config: 'max_devices' должен быть положительным int, "
                f"получено {max_devices!r}"
            )

        self._max_devices = max_devices
        self.logger.info(f"validated_config: max_devices={self._max_devices}")

    async def on_load(self, app: KamioApp, context: Optional[PluginContext] = None) -> None:
        self.logger.info(f"ValidatedConfigPlugin загружен (max_devices={self._max_devices})")

    async def on_unload(self, app: KamioApp) -> None:
        self.logger.info("ValidatedConfigPlugin выгружен")


class HookRegistrationPlugin(Plugin):
    """Плагин с регистрацией нескольких хуков через register_hooks.

    Демонстрирует:
        - register_hooks() — регистрация нескольких хуков жизненного цикла
        - Хуки: on_after_start, on_device_added, on_device_removed
    """

    @property
    def name(self) -> str:
        return "hook_registration"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Плагин с регистрацией нескольких хуков."

    async def on_load(self, app: KamioApp, context: Optional[PluginContext] = None) -> None:
        self.logger.info("HookRegistrationPlugin загружен")

    async def on_unload(self, app: KamioApp) -> None:
        self.logger.info("HookRegistrationPlugin выгружен")

    def register_hooks(self, context: PluginContext) -> None:
        """Регистрация нескольких хуков через PluginContext.

        context.register_hook регистрирует хук и запоминает его
        для автоматического удаления при unload.
        """
        context.register_hook("on_after_start", self._on_start)
        context.register_hook("on_device_added", self._on_device_added)
        context.register_hook("on_device_removed", self._on_device_removed)

    async def _on_start(self, *args, **kwargs) -> None:
        self.logger.info("🔗 [hook_plugin] Приложение запущено")

    async def _on_device_added(self, device: Device) -> None:
        self.logger.info(f"🔗 [hook_plugin] Устройство добавлено: {device.node.device_id}")

    async def _on_device_removed(self, device: Device) -> None:
        self.logger.info(f"🔗 [hook_plugin] Устройство удалено: {device.node.device_id}")


class RulePlugin(Plugin):
    """Плагин, регистрирующий правило через context.add_rule.

    Демонстрирует:
        - context.add_rule() — регистрация правила автоматизации
        - Правило автоматически удаляется при выгрузке плагина
    """

    @property
    def name(self) -> str:
        return "rule_plugin"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Плагин, регистрирующий правило через PluginContext."

    async def on_load(self, app: KamioApp, context: Optional[PluginContext] = None) -> None:
        self.logger.info("RulePlugin загружен")

        # Регистрируем правило через PluginContext
        # При выгрузке плагина правило будет автоматически удалено
        context.add_rule(
            self._on_state_change,
            device=DemoSwitch,
            fields=["power"],
            description="RulePlugin: реакция на power",
        )
        self.logger.info("  Правило зарегистрировано через context.add_rule()")

    async def on_unload(self, app: KamioApp) -> None:
        self.logger.info("RulePlugin выгружен (правило авто-удалено)")

    async def _on_state_change(self, event: RuleEvent, app: KamioApp) -> None:
        """Правило: срабатывает при изменении power на DemoSwitch."""
        power = event.get("power")
        self.logger.info(f"📋 [rule_plugin] power изменён на '{event.device_id}': {power}")


class BackgroundTaskPlugin(Plugin):
    """Плагин с фоновой задачей через context.create_task.

    Демонстрирует:
        - context.create_task() — запуск фоновой задачи
        - Задача автоматически отменяется при выгрузке плагина
    """

    @property
    def name(self) -> str:
        return "bg_task_plugin"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Плагин с фоновой задачей через PluginContext."

    async def on_load(self, app: KamioApp, context: Optional[PluginContext] = None) -> None:
        self.logger.info("BackgroundTaskPlugin загружен")

        # Запускаем фоновую задачу через PluginContext
        # При выгрузке плагина задача будет автоматически отменена
        context.create_task(
            self._background_loop(),
            name="bg_task_loop",
        )
        self.logger.info("  Фоновая задача запущена через context.create_task()")

    async def on_unload(self, app: KamioApp) -> None:
        self.logger.info("BackgroundTaskPlugin выгружен (задача авто-отменена)")

    async def _background_loop(self) -> None:
        """Периодическая фоновая задача."""
        try:
            count = 0
            while True:
                await asyncio.sleep(2)
                count += 1
                self.logger.info(f"🔄 [bg_task] Тик #{count}")
        except asyncio.CancelledError:
            self.logger.debug("Фоновая задача отменена")
            raise


class PluginLoadedSubscriber(Plugin):
    """Плагин, подписывающийся на событие plugin_loaded.

    Демонстрирует:
        - subscribe_events() — подписка на событие plugin_loaded
        - Реакция на загрузку других плагинов
    """

    @property
    def name(self) -> str:
        return "loaded_subscriber"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Плагин, отслеживающий загрузку других плагинов."

    async def on_load(self, app: KamioApp, context: Optional[PluginContext] = None) -> None:
        self.logger.info("PluginLoadedSubscriber загружен")

    async def on_unload(self, app: KamioApp) -> None:
        self.logger.info("PluginLoadedSubscriber выгружен")

    def subscribe_events(self, context: PluginContext) -> None:
        """Подписка на событие plugin_loaded.

        PluginLoader публикует событие 'plugin_loaded' после
        успешной загрузки каждого плагина.
        """
        context.subscribe("plugin_loaded", self._on_plugin_loaded)
        context.subscribe("plugin_unloaded", self._on_plugin_unloaded)

    def _on_plugin_loaded(self, data: dict) -> None:
        plugin_name = data.get("plugin_name", "?")
        plugin_version = data.get("plugin_version", "?")
        self.logger.info(f"📡 [subscriber] Плагин загружен: {plugin_name} v{plugin_version}")

    def _on_plugin_unloaded(self, data: dict) -> None:
        plugin_name = data.get("plugin_name", "?")
        self.logger.info(f"📡 [subscriber] Плагин выгружен: {plugin_name}")


class TransitiveDepPlugin(Plugin):
    """Плагин с транзитивной зависимостью.

    Зависит от 'notifications', который, в свою очередь, зависит
    от 'device_alerts'. PluginLoader автоматически загружает
    всю цепочку зависимостей.

    Демонстрирует:
        - Транзитивная загрузка зависимостей
        - Цикл A → B → C → A обнаруживается через _loading set
    """

    @property
    def name(self) -> str:
        return "transitive_dep"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Плагин с транзитивной зависимостью (→ notifications → device_alerts)."

    @property
    def dependencies(self) -> list[str]:
        # Зависит от notifications, который зависит от device_alerts
        return ["notifications"]

    async def on_load(self, app: KamioApp, context: Optional[PluginContext] = None) -> None:
        self.logger.info("TransitiveDepPlugin загружен (зависит от notifications)")

        # Проверяем всю цепочку зависимостей
        alerts = app.get_plugin("device_alerts")
        notifications = app.get_plugin("notifications")
        self.logger.info(
            f"  Цепочка зависимостей: "
            f"device_alerts={'✅' if alerts else '❌'}, "
            f"notifications={'✅' if notifications else '❌'}"
        )

    async def on_unload(self, app: KamioApp) -> None:
        self.logger.info("TransitiveDepPlugin выгружен")


# =====================================================================
# Демонстрация: плагин с configure() валидацией
# =====================================================================

async def demo_config_validation(app: KamioApp):
    """Показывает валидацию конфигурации в configure()."""
    logger.info("\n=== Демонстрация: плагин с configure() валидацией ===")

    # --- Успешная загрузка с корректной конфигурацией ---
    logger.info("--- Загрузка с корректной конфигурацией ---")
    try:
        plugin = await app.load_plugin(
            ValidatedConfigPlugin,
            config={"max_devices": 25},
        )
        logger.info(f"Плагин загружен: {plugin.name} v{plugin.version}")
    except ValueError as e:
        logger.error(f"Неожиданная ошибка: {e}")

    # --- Ошибка при неверной конфигурации ---
    logger.info("--- Загрузка с неверной конфигурацией (должна провалиться) ---")
    try:
        await app.load_plugin(
            ValidatedConfigPlugin,  # тот же класс — вызовет "already loaded"
            config={"max_devices": -5},
        )
        logger.error("ОШИБКА: должно было выбросить исключение")
    except (ValueError, Exception) as e:
        logger.info(f"✅ Ожидаемая ошибка: {e}")

    # Выгружаем для последующих демонстраций
    await app.unload_plugin("validated_config")


# =====================================================================
# Демонстрация: плагин с register_hooks
# =====================================================================

async def demo_hook_registration(app: KamioApp):
    """Показывает регистрацию хуков в плагине."""
    logger.info("\n=== Демонстрация: плагин с register_hooks ===")

    await app.load_plugin(HookRegistrationPlugin)
    logger.info("HookRegistrationPlugin загружен с 3 хуками:")
    logger.info("  - on_after_start")
    logger.info("  - on_device_added")
    logger.info("  - on_device_removed")

    # Создаём устройство — должен сработать хук on_device_added
    logger.info("Создаём устройство для триггера хука...")
    switch = await app.add_device("hook_test_switch", DemoSwitch)
    await asyncio.sleep(0.5)

    # Удаляем устройство — должен сработать хук on_device_removed
    logger.info("Удаляем устройство для триггера хука...")
    await app.remove_device("hook_test_switch")
    await asyncio.sleep(0.5)

    # Выгружаем плагин — хуки автоматически удаляются
    await app.unload_plugin("hook_registration")
    logger.info("Плагин выгружен — все хуки автоматически удалены")


# =====================================================================
# Демонстрация: плагин с add_rule через контекст
# =====================================================================

async def demo_rule_through_context(app: KamioApp):
    """Показывает регистрацию правила через PluginContext."""
    logger.info("\n=== Демонстрация: плагин с add_rule через контекст ===")

    await app.load_plugin(RulePlugin)
    logger.info("RulePlugin загружен — правило зарегистрировано через context.add_rule()")

    # Создаём устройство и меняем состояние — правило сработает
    switch = await app.add_device("rule_test_switch", DemoSwitch)
    await asyncio.sleep(0.3)

    logger.info("Изменяем power — правило плагина должно сработать:")
    await switch.handle_state({"power": True})
    await asyncio.sleep(0.5)
    await switch.handle_state({"power": False})
    await asyncio.sleep(0.5)

    # Выгружаем плагин — правило автоматически удаляется
    await app.unload_plugin("rule_plugin")
    logger.info("Плагин выгружен — правило автоматически удалено")

    # Проверяем: теперь правило не сработает
    logger.info("Изменяем power после выгрузки — правило не должно сработать:")
    await switch.handle_state({"power": True})
    await asyncio.sleep(0.5)

    await app.remove_device("rule_test_switch")


# =====================================================================
# Демонстрация: плагин с create_task через контекст
# =====================================================================

async def demo_task_through_context(app: KamioApp):
    """Показывает фоновую задачу через PluginContext."""
    logger.info("\n=== Демонстрация: плагин с create_task через контекст ===")

    await app.load_plugin(BackgroundTaskPlugin)
    logger.info("BackgroundTaskPlugin загружен — фоновая задача запущена")

    # Ждём, чтобы задача успела выполниться несколько раз
    logger.info("Ждём 5 секунд для наблюдения за фоновой задачей...")
    await asyncio.sleep(5)

    # Выгружаем плагин — задача автоматически отменяется
    await app.unload_plugin("bg_task_plugin")
    logger.info("Плагин выгружен — фоновая задача автоматически отменена")


# =====================================================================
# Демонстрация: unload_all
# =====================================================================

async def demo_unload_all(app: KamioApp):
    """Показывает выгрузку всех плагинов через unload_all."""
    logger.info("\n=== Демонстрация: unload_all ===")

    # Загружаем несколько плагинов
    logger.info(f"Текущие плагины: {app.list_plugins()}")

    # Выгружаем все плагины в обратном порядке загрузки
    logger.info("Вызываем app.plugin_loader.unload_all()...")
    await app.plugin_loader.unload_all()

    logger.info(f"Плагины после unload_all: {app.list_plugins()}")
    logger.info("Все плагины выгружены в обратном порядке (зависимости последними)")


# =====================================================================
# Демонстрация: обработка plugin_loaded события
# =====================================================================

async def demo_plugin_loaded_event(app: KamioApp):
    """Показывает подписку на событие plugin_loaded."""
    logger.info("\n=== Демонстрация: обработка plugin_loaded события ===")

    # Загружаем плагин-подписчик ПЕРВЫМ — он будет ловить загрузки остальных
    await app.load_plugin(PluginLoadedSubscriber)
    logger.info("PluginLoadedSubscriber загружен — слушает события plugin_loaded")

    # Теперь загружаем другой плагин — подписчик должен среагировать
    logger.info("Загружаем другой плагин (должно сработать событие)...")
    await app.load_plugin(HealthCheckPlugin, config={"check_interval": 30.0})
    await asyncio.sleep(0.5)

    # Выгружаем — подписчик должен среагировать на plugin_unloaded
    logger.info("Выгружаем health_check (должно сработать событие plugin_unloaded)...")
    await app.unload_plugin("health_check")
    await asyncio.sleep(0.5)

    # Выгружаем подписчика
    await app.unload_plugin("loaded_subscriber")


# =====================================================================
# Демонстрация: зависимости с транзитивной загрузкой
# =====================================================================

async def demo_transitive_deps(app: KamioApp):
    """Показывает транзитивную загрузку зависимостей."""
    logger.info("\n=== Демонстрация: зависимости с транзитивной загрузкой ===")

    # Регистрируем классы для разрешения зависимостей
    app.plugin_loader.register_class("device_alerts", DeviceAlertPlugin)
    app.plugin_loader.register_class("notifications", NotificationPlugin)
    app.plugin_loader.register_class("transitive_dep", TransitiveDepPlugin)

    # Выгружаем существующие плагины для чистоты эксперимента
    # (device_alerts и notifications могут быть уже загружены)
    existing = app.list_plugins()
    logger.info(f"Уже загружено: {existing}")

    # Загружаем transitive_dep — он зависит от notifications,
    # который зависит от device_alerts.
    # PluginLoader автоматически загрузит всю цепочку:
    # device_alerts → notifications → transitive_dep
    logger.info("Загружаем TransitiveDepPlugin (зависит от notifications → device_alerts)...")

    # Если зависимости уже загружены, они просто пропускаются
    try:
        await app.load_plugin(TransitiveDepPlugin)
        logger.info("✅ Транзитивная загрузка успешна!")

        # Проверяем порядок загрузки
        load_order = app.plugin_loader.load_order
        logger.info(f"Порядок загрузки: {load_order}")

        # transitive_dep должен быть загружен ПОСЛЕ notifications
        # notifications должен быть загружен ПОСЛЕ device_alerts
        da_idx = load_order.index("device_alerts") if "device_alerts" in load_order else -1
        n_idx = load_order.index("notifications") if "notifications" in load_order else -1
        t_idx = load_order.index("transitive_dep") if "transitive_dep" in load_order else -1

        if da_idx >= 0 and n_idx >= 0 and t_idx >= 0:
            logger.info(f"  device_alerts #{da_idx} → notifications #{n_idx} → transitive_dep #{t_idx}")
            assert da_idx < n_idx < t_idx, "Порядок зависимостей нарушен!"
            logger.info("✅ Порядок зависимостей корректен")
    except ValueError as e:
        logger.info(f"Зависимости уже загружены или ошибка: {e}")


# =====================================================================
# Расширенная главная функция с дополнительными демонстрациями
# =====================================================================

async def extended_main():
    """Запускает базовую демонстрацию плагинов плюс все дополнительные секции."""
    logger.info("=== Демонстрация системы плагинов kamio ===\n")

    app = KamioApp(mqtt_broker="mqtt://localhost:1883", client_id="plugins_demo")

    # --- Регистрируем классы плагинов для разрешения зависимостей ---
    app.plugin_loader.register_class("device_alerts", DeviceAlertPlugin)
    app.plugin_loader.register_class("notifications", NotificationPlugin)

    # --- Базовая загрузка плагинов (из оригинального main) ---
    logger.info("--- Загрузка DeviceAlertPlugin ---")
    await app.load_plugin(DeviceAlertPlugin, config={"log_state_changes": True})

    logger.info("--- Загрузка HealthCheckPlugin ---")
    await app.load_plugin(HealthCheckPlugin, config={"check_interval": 5.0})

    logger.info("--- Загрузка NotificationPlugin (с зависимостью) ---")
    app.plugin_loader.register_class("notifications", NotificationPlugin)
    await app.load_plugin(NotificationPlugin)

    logger.info("--- Загрузка встроенных плагинов ---")
    await app.load_plugin(LoggingPlugin, config={
        "file": "kamio_events.log", "level": "INFO",
        "max_bytes": 5 * 1024 * 1024, "backup_count": 2,
    })
    await app.load_plugin(MetricsPlugin)

    logger.info(f"\nЗагруженные плагины: {app.list_plugins()}")

    # --- Запуск приложения ---
    logger.info("\n--- Запуск приложения ---")
    await app.start()

    # --- Создаём устройство ---
    switch = await app.add_device("demo_switch", DemoSwitch)
    await switch.handle_state({"power": True})
    await asyncio.sleep(0.5)
    await switch.handle_state({"power": False})
    await asyncio.sleep(0.5)

    # --- Дополнительные демонстрации ---
    await demo_config_validation(app)
    await demo_hook_registration(app)
    await demo_rule_through_context(app)
    await demo_task_through_context(app)
    await demo_plugin_loaded_event(app)
    await demo_transitive_deps(app)

    # --- Демонстрация unload_all ---
    await demo_unload_all(app)

    # --- Завершение ---
    logger.info("\n--- Завершение ---")
    await app.stop()
    logger.info("Демонстрация завершена")


if __name__ == "__main__":
    asyncio.run(extended_main())
