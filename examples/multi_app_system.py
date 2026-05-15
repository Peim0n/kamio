import asyncio
import json
import logging
from synapse import SynapseApp, Device, command, state, event

# --- Устройства (используем упрощенные версии для демо) ---

@SynapseApp.device
class RemoteControlledLight(Device):
    power: bool = state(default=False, writable=True)
    
    @command
    async def set_power(self, value: bool):
        self.power = value
        await self.request_state_sync()
        print(f"[{self.device_id}] Питание: {value}")
        return {"status": "ok"}

@SynapseApp.device
class MasterController(Device):
    """
    Устройство в одном приложении, которое управляет устройством в ДРУГОМ приложении
    через общую шину MQTT.
    """
    @command
    async def trigger_remote_action(self, target_device_id: str, action: bool):
        print(f"[{self.device_id}] Отправка команды на удаленное устройство {target_device_id}...")
        
        # В Synapse Core взаимодействие между приложениями идет через MQTT.
        # Мы можем отправить команду в топик другого устройства.
        from synapse.core.envelope import Envelope
        
        # Формируем команду для удаленного устройства
        cmd_payload = {"method": "set_power", "params": {"value": action}}
        env = Envelope(
            source=self.node.device_id,
            type="sc", # Server Command
            payload=cmd_payload
        )
        
        # Публикуем в топик целевого устройства
        target_topic = f"synapse/v1/{target_device_id}/sc"
        await self.node.publish_raw(target_topic, env.to_json())
        
        return {"status": "command_sent_to_mqtt"}

# --- Запуск системы ---

async def run_app(app_id: str, app_config: dict, broker: str):
    """Функция для запуска отдельного экземпляра SynapseApp."""
    app = SynapseApp(mqtt_broker=broker, app_name=app_config["app_name"])
    
    # Создаем устройства для этого приложения
    for dev_id, dev_info in app_config["devices"].items():
        dev = await app.create_device(dev_id, dev_info["type"])
        if "config" in dev_info:
            await dev.handle_config(dev_info["config"])
    
    print(f"--- Запуск приложения: {app_config['app_name']} ---")
    await app.start()
    return app

async def main():
    # Загружаем единый конфиг
    with open("/home/ubuntu/synapse_project/examples/unified_system_config.json", "r") as f:
        config = json.load(f)
    
    broker = config["mqtt_broker"]
    
    # 1. Запускаем Приложение Видео (с лампой/устройством)
    video_app_cfg = {
        "app_name": "VideoApp",
        "devices": {"light_01": {"type": "remotecontrolledlight"}}
    }
    app_video = await run_app("video", video_app_cfg, broker)
    
    # 2. Запускаем Приложение Управления
    control_app_cfg = {
        "app_name": "ControlApp",
        "devices": {"master_ctrl": {"type": "mastercontroller"}}
    }
    app_control = await run_app("control", control_app_cfg, broker)

    await asyncio.sleep(2)
    
    # 3. Демонстрация взаимодействия
    print("\n[User] MasterController (из ControlApp) включает свет light_01 (в VideoApp)...")
    master = app_control.get_device("master_ctrl")
    await master.trigger_remote_action(target_device_id="light_01", action=True)

    await asyncio.sleep(5)
    
    # Остановка всех приложений
    await app_video.stop()
    await app_control.stop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())
