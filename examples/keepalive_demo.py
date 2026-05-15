import asyncio
import time
import logging
from synapse import SynapseApp, Device, telemetry, state, TaskManagerMixin

# Настройка логирования для наглядности
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("keepalive_demo")

@SynapseApp.device
class MonitoredDevice(Device):
    """Устройство с поддержкой Keep-Alive."""
    
    uptime: int = telemetry(freq="5s")
    status: str = state(default="online")

    async def on_start(self, node):
        await super().on_start(node)
        # Запускаем фоновую задачу отправки Keep-Alive
        self.create_task(self._keepalive_loop())
        self.start_time = time.time()

    async def _keepalive_loop(self):
        """Цикл отправки Keep-Alive сообщений."""
        while self.node and self.node.is_running:
            try:
                # Обновляем аптайм для телеметрии
                self.uptime = int(time.time() - self.start_time)
                
                # Отправка Keep-Alive сообщения (тип 'k')
                from synapse.core.envelope import Envelope
                env = Envelope.keepalive(source=self.node.device_id)
                await self.node.publish(env)
                
                logger.info(f"[Device] Отправлен Keep-Alive для {self.node.device_id}")
                await asyncio.sleep(10) # Интервал Keep-Alive
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Keep-Alive error: {e}")

async def main():
    app = SynapseApp(mqtt_broker="mqtt://localhost:1883")
    
    # Создаем устройство
    dev = await app.create_device("sensor_01", "monitoreddevice")
    
    # Логика мониторинга на стороне сервера (имитация)
    last_seen = {}

    @app.server_node.on("k") # Подписываемся на системный тип 'k'
    async def handle_keepalive(env):
        last_seen[env.source] = time.time()
        logger.info(f"[Server] Получен Keep-Alive от {env.source}")

    async def monitor_task():
        """Проверка доступности устройств по таймауту."""
        while app.is_running:
            now = time.time()
            for dev_id, last_ts in list(last_seen.items()):
                if now - last_ts > 25: # Порог отсутствия сообщений
                    logger.warning(f"[Server] Устройство {dev_id} OFFLINE (нет Keep-Alive > 25с)")
                    last_seen.pop(dev_id)
            await asyncio.sleep(5)

    # Запускаем приложение и мониторинг
    asyncio.create_task(monitor_task())
    await app.start()
    
    logger.info("--- Демонстрация Keep-Alive запущена (на 60 секунд) ---")
    await asyncio.sleep(60)
    
    await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
