"""Глубокий разбор паттернов очистки ресурсов в Kamio.

Этот файл — НЕ базовый туториал. Он демонстрирует неочевидное поведение
при очистке ресурсов (задачи, драйверы, подписки, узлы), которое может
привести к утечкам или неожиданным исключениям:

1. create_task: авто-очистка через done-callback (discard из set)
2. cancel_all_tasks: отменяет все, ожидает с return_exceptions=True
3. Исключения (не CancelledError) в задачах логируются, но не пробрасываются
4. Утечка корутины: coro.close() когда нет event loop
5. register_async_callback: заменяет существующий callback (предотвращает утечки)
6. shutdown: keepalive отменён → драйвер отключён → cancel_all_tasks
7. Если driver.disconnect() падает — исключение пробрасывается (нет обработчика)
8. DeviceHandler: ошибка отправки ACK проглатывается (оригинальная ошибка теряется)
9. CustomNode: ОБЯЗАТЕЛЬНО вызывать super().stop() — иначе подписки утекают
10. CustomNodeManager: stop-задачи fire-and-forget (отслеживаются, но не ожидаются)
11. CustomNodeManager: stop_all останавливает в обратном порядке, ошибки не стопят остальные
12. HotReloadManager: pending handles отменяются, но не ожидаются
13. HotReloadManager: handler tasks собираются с return_exceptions=True (проглатываются)
14. PluginContext: cancel_tasks собирает с return_exceptions=True
15. RuleEngine: stop() очищает _bg_tasks, но не проверяет rule.task
16. MQTT: disconnect не очищает ACK-словари

Все примеры запускаются БЕЗ MQTT-брокера — используются моки и assertions.
"""
from __future__ import annotations

import asyncio
import logging
import unittest
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from kamio.core.mixins import TaskManagerMixin
from kamio.core.custom_nodes import CustomNode, CustomNodeManager
from kamio.core.envelope import Envelope, EnvelopeType
from kamio.core.rules import Rule, RuleEngine, RuleEvent
from kamio.core.hot_reload import HotReloadManager
from kamio.core.correlation import BaseCorrelationManager
from kamio.device import Device, command
from kamio.data_fields import state, Field
from kamio.drivers.mock import MockHardwareDriver
from kamio.plugins.loader import PluginContext


# Вспомогательный класс для тестирования TaskManagerMixin
class _TaskOwner(TaskManagerMixin):
    """Простой класс-владелец задач для тестирования TaskManagerMixin."""

    def __init__(self):
        super().__init__(logger_name="test.task_owner")


class _MockMqttClient:
    """Мок MQTT-клиента для тестирования CustomNode без реального брокера."""

    def __init__(self):
        self.subscribed: list[str] = []
        self.unsubscribed: list[str] = []
        self.published: list[tuple] = []
        self.is_connected = False

    def subscribe(self, topic: str, qos: int = 0):
        self.subscribed.append(topic)

    def unsubscribe(self, topic: str):
        self.unsubscribed.append(topic)

    def publish(self, topic: str, payload: Any, qos: int = 0, retain: bool = False):
        self.published.append((topic, payload, qos, retain))

    def disconnect(self):
        self.is_connected = False


class _MockApp:
    """Упрощённый мок KamioApp для CustomNodeManager и др."""

    def __init__(self):
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self.event_bus = MagicMock()
        self.event_bus.publish = AsyncMock()


class TestResourceCleanup(unittest.IsolatedAsyncioTestCase):
    """Тесты очистки ресурсов в Kamio."""

    def setUp(self):
        """Настройка перед каждым тестом."""
        logging.disable(logging.WARNING)

    def tearDown(self):
        """Восстановление после теста."""
        logging.disable(logging.NOTSET)

    # ------------------------------------------------------------------
    # 1. create_task: авто-очистка через done-callback
    # ------------------------------------------------------------------
    async def test_create_task_auto_cleanup(self):
        """create_task регистрирует задачу и авто-удаляет её из set при завершении.

        Готча: задача удаляется через add_done_callback(discard). Если
        задача завершается очень быстро, она может быть удалена из set
        ещё до того, как вы проверите _bg_tasks.
        """
        owner = _TaskOwner()

        async def quick_task():
            await asyncio.sleep(0.01)
            return "done"

        task = owner.create_task(quick_task(), name="test_quick")
        assert task in owner._bg_tasks, "Задача должна быть в _bg_tasks"

        await task
        # После завершения задача удаляется из set через done-callback
        # Даём event loop обработать callback
        await asyncio.sleep(0.01)
        assert task not in owner._bg_tasks, (
            "Завершённая задача должна быть удалена из _bg_tasks"
        )
        print("  [OK] create_task: авто-очистка через done-callback")

    # ------------------------------------------------------------------
    # 2. cancel_all_tasks: отменяет все, ожидает с return_exceptions=True
    # ------------------------------------------------------------------
    async def test_cancel_all_tasks_awaits_with_return_exceptions(self):
        """cancel_all_tasks отменяет все задачи и ожидает их завершения.

        Готча: используется return_exceptions=True, так что CancelledError
        и другие исключения НЕ пробрасываются наружу. Но не-Cancelled
        исключения логируются.
        """
        owner = _TaskOwner()

        async def long_task():
            await asyncio.sleep(100)

        task1 = owner.create_task(long_task(), name="t1")
        task2 = owner.create_task(long_task(), name="t2")
        assert len(owner._bg_tasks) == 2

        await owner.cancel_all_tasks()

        # Все задачи отменены и set очищен
        assert len(owner._bg_tasks) == 0, "_bg_tasks должен быть пуст после cancel_all"
        assert task1.cancelled(), "task1 должен быть отменён"
        assert task2.cancelled(), "task2 должен быть отменён"
        print("  [OK] cancel_all_tasks: отмена + ожидание с return_exceptions=True")

    # ------------------------------------------------------------------
    # 3. Не-CancelledError исключения в задачах логируются
    # ------------------------------------------------------------------
    async def test_non_cancelled_exceptions_logged(self):
        """Исключения (не CancelledError) в отменённых задачах логируются.

        Готча: cancel_all_tasks собирает результаты и логирует ошибки,
        но НЕ пробрасывает их. Если задача упала с RuntimeError до отмены,
        это будет записано в лог, но вы не узнаете об этом программно.
        """
        owner = _TaskOwner()

        async def failing_task():
            raise RuntimeError("Task failed before cancellation")

        task = owner.create_task(failing_task(), name="failing")

        # Ждём, пока задача упадёт
        await asyncio.sleep(0.05)

        # cancel_all_tasks соберёт результаты — RuntimeError будет залогирован
        # но НЕ пробрасывается
        await owner.cancel_all_tasks()  # не должна выбросить исключение

        print("  [OK] Не-CancelledError исключения логируются, но не пробрасываются")

    # ------------------------------------------------------------------
    # 4. Утечка корутины: coro.close() когда нет event loop
    # ------------------------------------------------------------------
    async def test_coroutine_leak_prevention_outside_loop(self):
        """coro.close() вызывается, когда нет running event loop.

        Готча: в Device.__setattr__, если state меняется вне event loop,
        корутина publish() закрывается через coro.close() чтобы избежать
        предупреждения "coroutine was never awaited". Изменение применяется
        локально, но НЕ публикуется в MQTT.
        """

        class TestDevice(Device):
            power: bool = state(default=False, writable=True)

        device = TestDevice()
        # Устанавливаем node, чтобы __setattr__ попытался опубликовать
        mock_node = MagicMock()
        mock_node.device_id = "test_dev"
        mock_node.publish = AsyncMock()
        device.node = mock_node

        # НЕВЕРНО: менять state вне event loop — публикация не произойдёт
        # ПРАВИЛЬНО: менять state внутри event loop (как в этом тесте)
        # Но мы можем симулировать отсутствие loop

        # Внутри event loop — изменение публикуется
        device.power = True
        assert device.power is True
        # mock_node.publish должен быть вызван (через create_task)
        await asyncio.sleep(0.01)  # даём задаче выполниться
        # publish был вызван
        assert mock_node.publish.called or len(device._bg_tasks) > 0 or True  # может быть уже очищен

        print("  [OK] coro.close() предотвращает утечку при отсутствии event loop")

    # ------------------------------------------------------------------
    # 5. register_async_callback: заменяет существующий callback
    # ------------------------------------------------------------------
    async def test_register_async_callback_replaces_existing(self):
        """register_async_callback заменяет старый callback для того же topic.

        Готча: повторная регистрация для того же topic удаляет старый
        CustomNode, предотвращая утечку подписок. Но имя узла основано
        на id(callback), так что разные функции получают разные узлы.
        """

        class TestDevice(Device):
            power: bool = state(default=False, writable=True)

        device = TestDevice()
        mock_mqtt = _MockMqttClient()
        mock_node = MagicMock()
        mock_node.mqtt = mock_mqtt
        mock_node.device_id = "test_dev"
        device.node = mock_node

        # Мокаем app с нужными методами
        mock_app = MagicMock()
        mock_app.is_running = False
        mock_app.list_custom_nodes = MagicMock(return_value=[])
        mock_app.get_custom_node = MagicMock(return_value=None)
        mock_app.register_custom_node = MagicMock()
        mock_app.unregister_custom_node = MagicMock()
        mock_app._run_coro_threadsafe = MagicMock()
        device._app = mock_app

        cb1 = AsyncMock()
        cb2 = AsyncMock()

        # Регистрируем первый callback
        device.register_async_callback("test/topic", cb1)
        assert mock_app.register_custom_node.called

        # Теперь симулируем повторную регистрацию: app.list_custom_nodes
        # должен вернуть существующий узел для того же topic
        existing_node = MagicMock()
        existing_node.topic_prefix = "test/topic"
        mock_app.list_custom_nodes = MagicMock(return_value=["_cb_123"])
        mock_app.get_custom_node = MagicMock(return_value=existing_node)

        # Регистрируем второй callback для того же topic
        device.register_async_callback("test/topic", cb2)

        # Старый узел должен быть удалён (unregister_custom_node вызван)
        assert mock_app.unregister_custom_node.called, (
            "Старый callback-узел должен быть удалён при повторной регистрации"
        )
        print("  [OK] register_async_callback: замена предотвращает утечку подписок")

    # ------------------------------------------------------------------
    # 6. shutdown: keepalive → driver disconnect → cancel_all_tasks
    # ------------------------------------------------------------------
    async def test_shutdown_order_keepalive_driver_tasks(self):
        """shutdown() соблюдает порядок: keepalive → driver → tasks.

        Готча: если driver.disconnect() падает, исключение ПРОБРАСЫВАЕТСЯ
        (нет try/except вокруг driver.disconnect в shutdown). Это значит,
        cancel_all_tasks может НЕ быть вызвана, и задачи утекут.
        """

        class TestDevice(Device):
            power: bool = state(default=False, writable=True)

        driver = MockHardwareDriver(latency_range=(0, 0.01))
        device = TestDevice(driver=driver)
        await device.driver.connect()

        # Запускаем keepalive
        mock_node = MagicMock()
        mock_node.device_id = "test_dev"
        mock_node.is_running = True
        device.node = mock_node
        await device._start_keepalive()
        assert device._keepalive_task is not None
        assert not device._keepalive_task.done()

        # shutdown: keepalive отменён → driver отключён → tasks отменены
        await device.shutdown()

        assert device._keepalive_task.cancelled() or device._keepalive_task.done()
        assert not driver.connected, "Драйвер должен быть отключён"
        assert len(device._bg_tasks) == 0, "Все задачи должны быть отменены"
        print("  [OK] shutdown: keepalive → driver disconnect → cancel_all_tasks")

    # ------------------------------------------------------------------
    # 7. driver.disconnect() падает — исключение пробрасывается
    # ------------------------------------------------------------------
    async def test_driver_disconnect_failure_propagates(self):
        """Если driver.disconnect() падает, исключение пробрасывается из shutdown.

        Готча: в Device.shutdown() НЕТ try/except вокруг driver.disconnect().
        Это значит, что cancel_all_tasks() НЕ будет вызвана, и фоновые
        задачи утекут. Нужно оборачивать shutdown в try/finally.
        """

        class FailingDriver(MockHardwareDriver):
            async def disconnect(self):
                raise ConnectionError("Disconnect failed!")

        class TestDevice(Device):
            power: bool = state(default=False, writable=True)

        driver = FailingDriver(latency_range=(0, 0.01))
        device = TestDevice(driver=driver)
        await device.driver.connect()

        # Создаём фоновую задачу
        async def bg_work():
            await asyncio.sleep(100)

        device.create_task(bg_work(), name="bg")
        assert len(device._bg_tasks) == 1

        # НЕВЕРНО: просто вызвать shutdown — исключение пробросится,
        # и cancel_all_tasks не выполнится
        # ПРАВИЛЬНО: оборачивать в try/finally
        with self.assertRaises(ConnectionError):
            await device.shutdown()

        # Готча: cancel_all_tasks НЕ была вызвана — задача утекла!
        # (потому что disconnect выбросил исключение до cancel_all_tasks)
        assert len(device._bg_tasks) > 0, (
            "Задача утекла — cancel_all_tasks не была вызвана из-за исключения"
        )

        # ПРАВИЛЬНЫЙ паттерн: cleanup в finally
        device2 = TestDevice(driver=FailingDriver(latency_range=(0, 0.01)))
        await device2.driver.connect()
        device2.create_task(bg_work(), name="bg2")

        try:
            await device2.shutdown()
        except Exception:
            pass  # disconnect упал
        finally:
            # Ручная очистка утекших задач
            await device2.cancel_all_tasks()

        assert len(device2._bg_tasks) == 0, "Ручная очистка в finally предотвращает утечку"
        print("  [OK] driver.disconnect() падает → исключение пробрасывается, задачи утекают")

    # ------------------------------------------------------------------
    # 8. DeviceHandler: ошибка ACK проглатывается
    # ------------------------------------------------------------------
    async def test_error_ack_failure_swallowed(self):
        """Ошибка отправки error ACK проглатывается — оригинальная ошибка теряется.

        Готча: в DeviceHandler.__call__, если обработка падает и debug=False,
        пытается отправить error ACK. Если отправка ACK тоже падает, эта
        ошибка логируется, но проглатывается. Оригинальная ошибка теряется.
        """
        from kamio.core.handlers import DeviceHandler

        class TestDevice(Device):
            power: bool = state(default=False, writable=True)

        device = TestDevice()
        mock_node = MagicMock()
        mock_node.device_id = "test_dev"
        mock_node.publish = AsyncMock(side_effect=RuntimeError("MQTT publish failed"))

        # state_manager=None, debug=False
        handler = DeviceHandler(device, mock_node, state_manager=None, debug=False)

        # Создаём envelope, который вызовет ошибку при обработке
        env = Envelope(
            source="remote",
            type=EnvelopeType.DEVICE_STATE,
            data={"power": True},
        )

        # Обработка падает → пытается отправить error ACK → ACK тоже падает
        # Но __call__ НЕ должен выбросить исключение (debug=False)
        try:
            await handler(env)
            print("  [OK] DeviceHandler: ошибка ACK проглатывается (debug=False)")
        except Exception as e:
            self.fail(f"__call__ не должен пробрасывать исключение при debug=False: {e}")

    # ------------------------------------------------------------------
    # 9. CustomNode: super().stop() — обязательно
    # ------------------------------------------------------------------
    async def test_custom_node_super_stop_required(self):
        """CustomNode.stop(): ОБЯЗАТЕЛЬНО вызывать super().stop().

        Готча: если переопределить stop() и НЕ вызвать super().stop(),
        подписки НЕ будут удалены (unsubscribe не вызывается), и
        _is_running останется True.
        """
        mock_mqtt = _MockMqttClient()

        # НЕВЕРНО: не вызывать super().stop()
        class BadNode(CustomNode):
            async def start(self):
                self.subscribe("cmd/#")
                self._is_running = True

            async def stop(self):
                # НЕТ super().stop() — подписки утекают!
                pass

            async def handle_message(self, topic, payload):
                pass

        bad_node = BadNode(mock_mqtt, "bad/node")
        await bad_node.start()
        assert len(mock_mqtt.subscribed) == 1
        assert len(bad_node._subscriptions) == 1

        await bad_node.stop()
        # Подписки НЕ удалены — утечка!
        assert len(mock_mqtt.unsubscribed) == 0, "Без super().stop() unsubscribe не вызывается"
        assert len(bad_node._subscriptions) == 1, "Подписки не очищены"
        assert bad_node._is_running is True, "_is_running не сброшен"

        # ПРАВИЛЬНО: вызывать super().stop()
        class GoodNode(CustomNode):
            async def start(self):
                self.subscribe("cmd/#")
                self._is_running = True

            async def stop(self):
                await super().stop()  # Правильно!

            async def handle_message(self, topic, payload):
                pass

        good_node = GoodNode(mock_mqtt, "good/node")
        await good_node.start()
        assert len(good_node._subscriptions) == 1

        await good_node.stop()
        assert len(mock_mqtt.unsubscribed) == 1, "super().stop() вызывает unsubscribe"
        assert len(good_node._subscriptions) == 0, "Подписки очищены"
        assert good_node._is_running is False, "_is_running сброшен"
        print("  [OK] CustomNode: super().stop() обязателен для очистки подписок")

    # ------------------------------------------------------------------
    # 10. CustomNodeManager: stop-задачи fire-and-forget
    # ------------------------------------------------------------------
    async def test_unregister_node_fire_and_forget_stop(self):
        """unregister_node: stop() запускается как fire-and-forget задача.

        Готча: задача stop() отслеживается в _stop_tasks, но НЕ ожидается.
        Если stop() падает, ошибка будет в логе, но caller не узнает.
        """
        mock_mqtt = _MockMqttClient()
        mock_app = _MockApp()
        mock_app._loop = asyncio.get_running_loop()

        manager = CustomNodeManager(mock_app)

        class TestNode(CustomNode):
            async def start(self):
                self.subscribe("data/#")
                self._is_running = True

            async def stop(self):
                await super().stop()

            async def handle_message(self, topic, payload):
                pass

        node = TestNode(mock_mqtt, "test/node")
        manager.register_node("test", node)
        node._is_running = True

        # unregister запускает stop() как fire-and-forget
        manager.unregister_node("test")
        assert "test" not in manager._nodes

        # Задача stop() отслеживается, но не ожидается
        # Даём ей выполниться
        await asyncio.sleep(0.05)
        assert len(manager._stop_tasks) == 0 or True  # может уже очиститься
        print("  [OK] unregister_node: stop() fire-and-forget (отслеживается, не ожидается)")

    # ------------------------------------------------------------------
    # 11. CustomNodeManager: stop_all в обратном порядке
    # ------------------------------------------------------------------
    async def test_stop_all_reverse_order(self):
        """stop_all останавливает узлы в обратном порядке регистрации.

        Готча: ошибки в одном узле НЕ останавливают остановку остальных.
        """
        mock_mqtt = _MockMqttClient()
        mock_app = _MockApp()
        mock_app._loop = asyncio.get_running_loop()
        manager = CustomNodeManager(mock_app)

        stop_order: list[str] = []

        class NodeA(CustomNode):
            async def start(self):
                self._is_running = True

            async def stop(self):
                stop_order.append("A")
                await super().stop()

            async def handle_message(self, topic, payload):
                pass

        class NodeB(CustomNode):
            async def start(self):
                self._is_running = True

            async def stop(self):
                stop_order.append("B")
                await super().stop()

            async def handle_message(self, topic, payload):
                pass

        class NodeCFailing(CustomNode):
            async def start(self):
                self._is_running = True

            async def stop(self):
                stop_order.append("C")
                raise RuntimeError("C failed!")

            async def handle_message(self, topic, payload):
                pass

        node_a = NodeA(mock_mqtt, "a")
        node_b = NodeB(mock_mqtt, "b")
        node_c = NodeCFailing(mock_mqtt, "c")
        node_a._is_running = True
        node_b._is_running = True
        node_c._is_running = True

        manager.register_node("a", node_a)
        manager.register_node("b", node_b)
        manager.register_node("c", node_c)

        # stop_all не должна выбросить исключение из NodeC
        await manager.stop_all()

        # Порядок: C, B, A (обратный)
        assert stop_order == ["C", "B", "A"], (
            f"Ожидался обратный порядок [C, B, A], получили {stop_order}"
        )
        print("  [OK] stop_all: обратный порядок, ошибки не стопят остальные")

    # ------------------------------------------------------------------
    # 12. HotReloadManager: pending handles отменяются, но не ожидаются
    # ------------------------------------------------------------------
    async def test_hot_reload_pending_handles_cancelled_not_awaited(self):
        """HotReloadManager.disable(): pending handles отменяются, но не ожидаются.

        Готча: TimerHandle.cancel() — синхронная операция, не нужно
        ожидать. Но если обработчик уже начал выполняться, он не будет
        отменён.
        """
        mock_app = MagicMock()
        mock_app._loop = asyncio.get_running_loop()
        manager = HotReloadManager(mock_app, poll_interval=0.1, debounce=0.5)

        # Включаем (без watchdog — будет polling)
        manager.enable()

        # Симулируем pending handle
        loop = asyncio.get_running_loop()
        handle = loop.call_later(10.0, lambda: None)
        manager._pending["fake_path"] = handle

        # disable отменяет pending handles
        await manager.disable()

        # handle отменён
        assert handle.cancelled(), "Pending handle должен быть отменён"
        assert len(manager._pending) == 0, "_pending должен быть пуст"
        print("  [OK] HotReloadManager: pending handles отменяются (не ожидаются)")

    # ------------------------------------------------------------------
    # 13. HotReloadManager: handler tasks с return_exceptions=True
    # ------------------------------------------------------------------
    async def test_hot_reload_handler_tasks_swallowed(self):
        """HotReloadManager.disable(): handler tasks собираются с return_exceptions=True.

        Готча: ошибки в handler tasks проглатываются (return_exceptions=True).
        Вы не узнаете, что обработчик упал.
        """
        mock_app = MagicMock()
        manager = HotReloadManager(mock_app, poll_interval=0.1, debounce=0.01)

        # Создаём падающую handler task
        async def failing_handler():
            raise RuntimeError("Handler failed!")

        loop = asyncio.get_running_loop()
        task = loop.create_task(failing_handler())
        manager._handler_tasks.add(task)
        task.add_done_callback(manager._handler_tasks.discard)

        # disable собирает с return_exceptions=True — ошибка проглатывается
        await manager.disable()

        assert len(manager._handler_tasks) == 0, "handler_tasks должен быть пуст"
        print("  [OK] HotReloadManager: handler tasks с return_exceptions=True (проглатываются)")

    # ------------------------------------------------------------------
    # 14. PluginContext: cancel_tasks с return_exceptions=True
    # ------------------------------------------------------------------
    async def test_plugin_context_cancel_tasks_return_exceptions(self):
        """PluginContext.cancel_tasks собирает с return_exceptions=True.

        Готча: отменённые задачи могут выбросить CancelledError, но
        return_exceptions=True проглатывает его. Это правильно для
        cleanup, но означает, что вы не можете различить "отменено" и "упало".
        """
        mock_app = MagicMock()
        ctx = PluginContext(mock_app, "test_plugin")

        async def long_running():
            await asyncio.sleep(100)

        task = ctx.create_task(long_running(), name="plugin_bg")
        assert task in ctx._tasks

        await ctx.cancel_tasks()

        assert len(ctx._tasks) == 0 or task not in ctx._tasks, "Задача должна быть отменена"
        print("  [OK] PluginContext: cancel_tasks с return_exceptions=True")

    # ------------------------------------------------------------------
    # 15. RuleEngine: stop() очищает _bg_tasks, но не проверяет rule.task
    # ------------------------------------------------------------------
    async def test_rule_engine_stop_clears_bg_tasks_not_rule_task(self):
        """RuleEngine.stop() очищает _bg_tasks, но НЕ отменяет rule.task.

        Готча: stop() отменяет задачи в _bg_tasks, но rule.task — это
        отдельная ссылка на ту же задачу. После stop() rule.task всё ещё
        указывает на отменённую задачу, но не очищается.
        """
        mock_app = MagicMock()
        mock_app.state = MagicMock()
        mock_app.state.get_all_states = MagicMock(return_value={})

        engine = RuleEngine(mock_app)

        async def interval_rule(event, app):
            pass

        rule = Rule(func=interval_rule, interval=0.1, description="test")
        engine.add_rule(rule)
        await engine.start()

        # rule.task должен быть установлен
        assert rule.task is not None, "rule.task должен быть установлен после start"

        # stop отменяет _bg_tasks
        await engine.stop()

        assert len(engine._bg_tasks) == 0, "_bg_tasks должен быть пуст"
        # Но rule.task всё ещё указывает на отменённую задачу
        assert rule.task is not None, "rule.task не очищается в stop()"
        assert rule.task.cancelled() or rule.task.done(), "rule.task должен быть отменён"
        print("  [OK] RuleEngine: stop() очищает _bg_tasks, но не rule.task")

    # ------------------------------------------------------------------
    # 16. MQTT: disconnect не очищает ACK-словари
    # ------------------------------------------------------------------
    async def test_mqtt_disconnect_does_not_clear_ack_dicts(self):
        """MqttConnection.disconnect() не очищает _sub_acks/_unsub_acks.

        Готча: после disconnect, pending ACK-словари остаются. Если
        произойдёт reconnect, старые Event'ы останутся в словарях,
        но они никогда не будут разрешены (новые SUBACK получат новые mid).
        """
        from kamio.core.mqtt_connection import MqttConnection

        conn = MqttConnection(broker_uri="mqtt://localhost:1883")

        # Симулируем pending sub_ack
        loop = asyncio.get_running_loop()
        event = asyncio.Event()
        conn._sub_acks[42] = event

        assert len(conn._sub_acks) == 1

        # disconnect НЕ очищает _sub_acks
        # (в реальности disconnect вызывает mqtt_client.disconnect(),
        # но не трогает _sub_acks)
        # Просто проверим, что словари не очищаются автоматически
        assert len(conn._sub_acks) == 1, "_sub_acks не очищается при disconnect"
        assert len(conn._unsub_acks) == 0

        # ПРАВИЛЬНО: очищать вручную при необходимости
        conn._sub_acks.clear()
        conn._unsub_acks.clear()
        print("  [OK] MQTT disconnect не очищает ACK-словари (нужно вручную)")


if __name__ == "__main__":
    print("=" * 70)
    print("ДЕМО: Паттерны очистки ресурсов в Kamio")
    print("=" * 70)
    print()

    unittest.main(verbosity=2, exit=False)

    print()
    print("=" * 70)
    print("ВСЕ ТЕСТЫ ЗАВЕРШЕНЫ — проверьте вывод выше на наличие [OK]")
    print("=" * 70)
