import asyncio
import logging
import json
from typing import Any, Dict, Optional
from synapse import SynapseApp, Device, command, state, config, telemetry, event
from synapse.drivers.aten_vp2420 import ATEN_VP2420_Driver, VP2420Commands

# 1. Класс устройства ATEN VP2420
@SynapseApp.device
class ATEN_VP2420(Device):
    """
    Устройство ATEN VP2420.
    Демонстрирует:
    - Полный набор команд на основе API.
    - Кастомный Keep-Alive с опросом железа.
    - Инициализацию драйвера через конфиг.
    - Различные типы событий.
    - Авторизацию (через драйвер).
    - Валидацию ответов и подтверждения (ACK).
    """
    
    # Конфигурация (из config.json или defaults)
    host: str = config(default="192.168.1.100")
    port: int = config(default=23)
    keepalive_interval: float = config(default=10.0)
    auth_token: Optional[str] = config(default=None) # Пример для авторизации

    # Состояние
    active_input: str = state(default="i01", writable=True)
    mute_state: str = state(default="off", writable=True)
    display_mode: str = state(default="matrix", writable=True)
    current_scaling: Dict[str, Any] = state(default={}, writable=True)
    
    # События
    device_error = event(python_type=str) # Событие ошибки
    input_switched = event(python_type=dict) # Событие переключения входа

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.driver: Optional[ATEN_VP2420_Driver] = None

    async def on_init(self, **kwargs):
        """
        Инициализация драйвера с использованием параметров из конфигурации устройства.
        Драйвер объявляется здесь, используя `self.host`, `self.port` и `self.auth_token`.
        """
        self.logger.info(f"Инициализация драйвера для {self.host}:{self.port}")
        self.driver = ATEN_VP2420_Driver(host=self.host, port=self.port, auth_token=self.auth_token)
        try:
            await self.driver.connect()
            self.logger.info(f"Драйвер {self.device_id} успешно подключен.")
        except Exception as e:
            self.logger.error(f"Ошибка подключения драйвера {self.device_id}: {e}")
            await self.emit("device_error", f"Connection failed: {e}")

    async def on_start(self, node):
        await super().on_start(node)
        # Запуск кастомного цикла Keep-Alive
        self.create_task(self._vp2420_keepalive_loop())

    # --- Продвинутый Keep-Alive (вызывает функцию устройства) ---
    async def _vp2420_keepalive_loop(self):
        """
        Цикл Keep-Alive, который вызывает метод `perform_keepalive_check` устройства.
        """
        while self.node and self.node.is_running:
            try:
                await self.perform_keepalive_check() # Вызов метода устройства
                await asyncio.sleep(self.keepalive_interval)
            except Exception as e:
                self.logger.error(f"Keep-Alive Loop Error: {e}")
                await asyncio.sleep(5) # Быстрая попытка при ошибке

    async def perform_keepalive_check(self):
        """
        Кастомная логика Keep-Alive: опрос реального состояния через драйвер.
        """
        from synapse.core.envelope import Envelope
        if not self.driver or not self.driver.is_connected:
            self.logger.warning(f"Драйвер {self.device_id} не подключен. Попытка переподключения...")
            try:
                await self.driver.connect()
            except Exception as e:
                self.logger.error(f"Не удалось переподключиться к драйверу {self.device_id}: {e}")
                await self.emit("device_error", f"Reconnect failed: {e}")
                return

        try:
            status = await self.driver.read_status()
            if status:
                # Обновляем локальное состояние на основе ответа от железа
                self.active_input = status.get("video_input", self.active_input)
                self.mute_state = status.get("audio_mute", self.mute_state)
                self.display_mode = status.get("display_mode", self.display_mode)
                
                # Отправляем системный Keep-Alive (тип 'k')
                env = Envelope.keepalive(source=self.node.device_id)
                await self.node.publish(env)
                
                # Синхронизируем состояние с MQTT (чтобы сервер видел актуальные данные)
                await self.request_state_sync()
                self.logger.debug(f"Keep-Alive для {self.device_id} успешно выполнен. Статус: {status}")
            else:
                self.logger.warning(f"Keep-Alive для {self.device_id}: Драйвер не вернул статус.")
                await self.emit("device_error", "Keep-Alive: No status from driver")
        except Exception as e:
            self.logger.error(f"Ошибка при выполнении Keep-Alive для {self.device_id}: {e}")
            await self.emit("device_error", f"Keep-Alive check failed: {e}")

    # --- Команды управления (на основе API) ---
    @command
    async def switch_input(self, input_source: str, output_port: str = VP2420Commands.Ports.VIDEO_OUTPUT_1):
        """Переключение входа на выход с валидацией и ACK."""
        if input_source not in VP2420Commands.Ports.VIDEO_INPUTS:
            await self.emit("device_error", f"Invalid input source: {input_source}")
            return {"status": "error", "message": f"Неверный вход: {input_source}"}

        res = await self.driver.execute("switch", {"input_source": input_source, "output_port": output_port})
        if res.get("status") == "success" and res.get("response") == "OK":
            self.active_input = input_source
            await self.request_state_sync() # Отправляем State Ack
            await self.emit("input_switched", {"input": input_source, "output": output_port})
            return {"status": "success", "input": input_source, "ack": True}
        
        await self.emit("device_error", f"Switch command failed: {res.get("response")}")
        return {"status": "error", "message": res.get("response"), "ack": False}

    @command
    async def set_mute(self, state: str, target_output: str = VP2420Commands.Ports.SYSTEM_AUDIO_OUTPUT):
        """Управление звуком (on/off) с валидацией."""
        if state not in ["on", "off"]:
            await self.emit("device_error", f"Invalid mute state: {state}")
            return {"status": "error", "message": "Состояние должно быть 'on' или 'off'"}

        res = await self.driver.execute("mute", {"state": state, "target_output": target_output})
        if res.get("status") == "success" and res.get("response") == "OK":
            self.mute_state = state
            await self.request_state_sync()
            return {"status": "success", "mute": state, "ack": True}
        
        await self.emit("device_error", f"Mute command failed: {res.get("response")}")
        return {"status": "error", "message": res.get("response"), "ack": False}

    @command
    async def set_scaling(self, output_port: str = VP2420Commands.Ports.VIDEO_OUTPUT_1, 
                          hor: Optional[int] = None, ver: Optional[int] = None, 
                          freq: Optional[int] = None, cs: Optional[str] = None, native: bool = False):
        """Настройка масштабирования для выходного порта."""
        params = {"output_port": output_port}
        if hor is not None: params["hor"] = hor
        if ver is not None: params["ver"] = ver
        if freq is not None: params["freq"] = freq
        if cs is not None: params["cs"] = cs
        params["native"] = native

        res = await self.driver.execute("set_scaling", params)
        if res.get("status") == "success" and res.get("response") == "OK":
            # В реальном устройстве нужно было бы прочитать текущие настройки масштабирования
            # Для примера просто сохраняем отправленные параметры
            self.current_scaling = {k:v for k,v in params.items() if k != "output_port"}
            await self.request_state_sync()
            return {"status": "success", "scaling": self.current_scaling, "ack": True}
        
        await self.emit("device_error", f"Scaling command failed: {res.get("response")}")
        return {"status": "error", "message": res.get("response"), "ack": False}

    @command
    async def reboot_device(self):
        """Перезагрузка устройства."""
        res = await self.driver.execute("reboot", {})
        if res.get("status") == "success" and res.get("response") == "OK":
            self.logger.info(f"Устройство {self.device_id} перезагружается...")
            return {"status": "rebooting", "ack": True}
        
        await self.emit("device_error", f"Reboot command failed: {res.get("response")}")
        return {"status": "error", "message": res.get("response"), "ack": False}

# 2. Логическое устройство: Менеджер Презентаций (D2D связь)
@SynapseApp.device
class PresentationManager(Device):
    active_presentation: str = state(default="none", writable=True)

    @command
    async def start_presentation(self, presentation_name: str, main_switcher_id: str):
        self.active_presentation = presentation_name
        await self.request_state_sync()

        self.logger.info(f"Запуск презентации: {presentation_name}")
        
        # Прямая передача задачи другому устройству (D2D)
        switcher: ATEN_VP2420 = self.node.app.get_device(main_switcher_id) # Получаем объект устройства
        if switcher:
            if presentation_name == "meeting_room_A":
                self.logger.info(f"[Manager] Команда коммутатору {main_switcher_id}: переключить на вход i01")
                # Вызов команды другого устройства
                await switcher.switch_input(input_source=VP2420Commands.Ports.VIDEO_INPUT_1)
                await switcher.set_mute(state="off")
            elif presentation_name == "training_session":
                self.logger.info(f"[Manager] Команда коммутатору {main_switcher_id}: переключить на вход i02")
                await switcher.switch_input(input_source=VP2420Commands.Ports.VIDEO_INPUT_2)
                await switcher.set_mute(state="on")
            else:
                self.logger.warning(f"Неизвестная презентация: {presentation_name}")
                return {"status": "error", "message": "Неизвестная презентация"}
        else:
            self.logger.error(f"Коммутатор {main_switcher_id} не найден.")
            return {"status": "error", "message": f"Коммутатор {main_switcher_id} не найден"}

        return {"status": "success", "presentation": presentation_name}

# 3. Основное приложение
async def main():
    # Имитация загрузки конфига из файла (vp2420_config.json)
    with open("/home/ubuntu/synapse_project/examples/vp2420_config.json", "r", encoding="utf-8") as f:
        app_config = json.load(f)

    app = SynapseApp(mqtt_broker=app_config["mqtt_broker"], app_name=app_config["app_name"])
    
    # Создание устройств на основе конфига
    for device_id, dev_cfg in app_config["devices"].items():
        if dev_cfg["type"] == "aten_vp2420":
            switcher = await app.create_device(device_id, dev_cfg["type"])
            await switcher.handle_config(dev_cfg["config"])
        elif dev_cfg["type"] == "presentationmanager":
            manager = await app.create_device(device_id, dev_cfg["type"])
            await manager.handle_config(dev_cfg["config"] if "config" in dev_cfg else {})

    await app.start()
    print("--- Система управления ATEN VP2420 запущена ---")

    # Демонстрация работы
    await asyncio.sleep(2)
    print("\n[User] Менеджер презентаций запускает 'meeting_room_A' на 'switcher_hall_1'...")
    # Получаем менеджер и вызываем его команду
    manager_device: PresentationManager = app.get_device("presentation_manager_main")
    if manager_device:
        await manager_device.start_presentation(presentation_name="meeting_room_A", main_switcher_id="switcher_hall_1")

    await asyncio.sleep(5)
    print("\n[User] Менеджер презентаций запускает 'training_session' на 'switcher_hall_2'...")
    if manager_device:
        await manager_device.start_presentation(presentation_name="training_session", main_switcher_id="switcher_hall_2")

    await asyncio.sleep(15) # Даем время для Keep-Alive и команд
    await app.stop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO) # Уровень логирования
    asyncio.run(main())
