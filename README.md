# Synapse Core v43: Фреймворк для IoT-приложений на Python

![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)

## Описание

**Synapse Core v43** — это мощный, декларативный и асинхронный фреймворк для разработки IoT-приложений на Python, использующий протокол MQTT для обмена сообщениями. Он предоставляет интуитивно понятный API для определения устройств, их состояний, телеметрии и команд, а также мощный механизм правил для автоматизации взаимодействий. Фреймворк разработан с учетом асинхронности (`asyncio`), что обеспечивает высокую производительность, отзывчивость и готовность к работе с реальным оборудованием.

## Основные возможности

*   **Декларативное определение устройств**: Легкое описание IoT-устройств с помощью классов Python, аннотаций и специальных функций (`telemetry`, `state`, `command`, `event`, `config`).
*   **Поддержка аппаратных драйверов**: Встроенная поддержка различных низкоуровневых драйверов для взаимодействия с реальным оборудованием (GPIO, Telnet, Serial, HTTP, Mock).
*   **MQTT-интеграция**: Встроенная поддержка MQTT v5 для надежной и эффективной коммуникации с брокерами, включая обратную совместимость с legacy-топиками.
*   **Асинхронность**: Полное использование `asyncio` для неблокирующей обработки событий и параллельных операций.
*   **Система правил**: Гибкий движок правил для создания логики автоматизации на основе изменений состояния или телеметрии устройств, а также по интервалу.
*   **Управление состоянием**: Централизованное управление состоянием устройств и корреляция команд/ответов.
*   **Расширяемость**: Модульная архитектура, позволяющая легко добавлять новые драйверы и функциональность.
*   **Home Assistant Discovery**: Поддержка автоматического обнаружения устройств в Home Assistant через MQTT.
*   **Конфигурация**: Гибкое управление конфигурацией через файлы JSON и переменные окружения.

## Установка

Synapse Core v43 доступен для установки через `pip`.

```bash
pip install synapse-core
```

### Зависимости

Для работы фреймворка требуется установленный MQTT-брокер (например, Mosquitto).

```bash
sudo apt-get update
sudo apt-get install -y mosquitto mosquitto-clients
sudo service mosquitto start
```

Для использования некоторых драйверов могут потребоваться дополнительные библиотеки:

*   **GPIOChipDriver**: `pip install gpiod`
*   **SerialDriver**: `pip install pyserial`
*   **HTTPDeviceDriver**: `pip install aiohttp`

## Быстрый старт

Создайте файл `my_app.py`:

```import asyncio
import logging
from synapse import SynapseApp, Device, command, telemetry, state
from synapse.drivers import MockHardwareDriver

# 1. Инициализация приложения
app = SynapseApp(
    mqtt_broker="mqtt://localhost:1883",
    client_id="my_iot_app",
    log_level=logging.INFO
)

# 2. Определение модели устройства с драйвером
@app.device
class SmartThermostat(Device):
    """Умный термостат с телеметрией температуры и целевым состоянием, использующий MockHardwareDriver."""
    temp: float = telemetry(unit="°C", freq="5s")
    target: float = state(default=22.0, writable=True)

    def __init__(self, **kwargs):
        super().__init__(driver=MockHardwareDriver(initial_state={"temp": 20.0}), **kwargs)

    @command
    async def set_target(self, value: float):
        self.logger.info(f"Обновление целевой температуры до {value}°C")
        self.target = value
        await self.request_state_sync()
        return {"status": "ok", "target": self.target}

    async def on_start(self, node):
        await super().on_start(node)

        self.create_task(
            self._read_temperature_periodically(),
            name="read_temp"
        )

    async def _read_temperature_periodically(self):
        while True:
            if self.driver:
                read_temp = await self.driver.read("temp")
                if read_temp is not None:
                    self.temp = float(read_temp)
            await asyncio.sleep(5) # Читаем каждые 5 секунд

# 3. Правила автоматизации
@app.rule(device=SmartThermostat, fields=["temp"], description="Контроль климата")
async def on_temp_change(snapshot: dict, app_instance: SynapseApp):
    """Реагирование на изменения температуры."""
    device_id = snapshot["device_id"]
    current_temp = snapshot.get("update", {}).get("temp")
    if current_temp is not None:
        thermostat = app_instance.devices.get(device_id)
        if thermostat and isinstance(thermostat, SmartThermostat):
            if current_temp > thermostat.target + 2.0:
                app_instance.logger.warning(f"[{device_id}] Высокая температура: {current_temp}°C, целевая: {thermostat.target}°C. Включаем охлаждение.")
                # Здесь можно отправить команду на устройство, например, включить кондиционер
            elif current_temp < thermostat.target - 2.0:
                app_instance.logger.info(f"[{device_id}] Низкая температура: {current_temp}°C, целевая: {thermostat.target}°C. Включаем обогрев.")
                # Здесь можно отправить команду на устройство, например, включить обогрев

# 4. Запуск приложения
if __name__ == "__main__":
    async def main():
        # Создание экземпляра устройства
        await app.create_device("room_thermostat", "smartthermostat")
        await app.start()
        app.logger.info("Приложение Smart Thermostat запущено. Press Ctrl+C to stop.")

        # Пример вызова команды через приложение (обычно это делается извне)
        await asyncio.sleep(10)
        app.logger.info("Отправка команды: set_target(24.0)")
        thermostat_instance = app.devices["room_thermostat"]
        await thermostat_instance.set_target(24.0)

        while True:
            await asyncio.sleep(3600) # Работаем час

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        app.logger.info("Приложение остановлено пользователем.")
    finally:
        asyncio.run(app.stop())
```

Запустите приложение:

```bash
python my_app.py
```

## Документация

*   [API Документация](docs/api.md)
*   [Руководство пользователя](docs/user_guide.md)
*   [Обзор архитектуры](docs/architecture.md)
*   [Драйверы](docs/drivers.md)
*   [Примеры](docs/examples.md)
*   [Рекомендации по развертыванию](docs/deployment.md)
*   [Руководство для контрибьюторов](docs/contributing.md)
*   [Полное руководство по возможностям Synapse Core](docs/Полное руководство по возможностям Synapse Core v43.md)

## Лицензия

Synapse Core v43 распространяется под лицензией MIT. См. файл `LICENSE` для получения дополнительной информации.

## Вклад

Мы приветствуем вклад в развитие Synapse Core. Пожалуйста, ознакомьтесь с [Руководством для контрибьюторов](docs/contributing.md) перед началом работы.
