# Рекомендации по развертыванию Synapse Core v43

Это руководство содержит рекомендации по развертыванию приложений на базе Synapse Core в производственной среде.

## 1. Выбор MQTT-брокера

Synapse Core требует надежного MQTT-брокера.

*   **Mosquitto**: Отличный выбор для большинства сценариев. Легковесный, быстрый и поддерживает все необходимые функции MQTT v5.
*   **EMQX**: Подходит для высоконагруженных систем и кластеризации.
*   **HiveMQ**: Мощное решение корпоративного уровня с расширенными возможностями управления и интеграции.

**Рекомендация**: Для начала используйте Eclipse Mosquitto.

## 2. Управление процессом

Приложение Synapse Core должно работать как фоновый процесс (демон). Не запускайте его напрямую в терминале для production.

### Systemd (Linux)

Рекомендуемый способ управления процессом в Linux — использование `systemd`.

1.  Создайте файл службы (например, `/etc/systemd/system/synapse-app.service`):

```ini
[Unit]
Description=Synapse Core IoT Application
After=network.target mosquitto.service

[Service]
Type=simple
User=synapse_user
WorkingDirectory=/opt/synapse_app
ExecStart=/opt/synapse_app/venv/bin/python /opt/synapse_app/main.py
Restart=on-failure
RestartSec=5
Environment="PYTHONUNBUFFERED=1"

[Install]
WantedBy=multi-user.target
```

2.  Включите и запустите службу:

```bash
sudo systemctl daemon-reload
sudo systemctl enable synapse-app
sudo systemctl start synapse-app
```

### Docker

Контейнеризация с помощью Docker упрощает развертывание и управление зависимостями.

Пример `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
```

Пример `docker-compose.yml` (включая Mosquitto):

```yaml
version: '3.8'

services:
  mqtt:
    image: eclipse-mosquitto:2
    ports:
      - "1883:1883"
    volumes:
      - ./mosquitto/config:/mosquitto/config
      - ./mosquitto/data:/mosquitto/data
      - ./mosquitto/log:/mosquitto/log

  synapse-app:
    build: .
    depends_on:
      - mqtt
    environment:
      - MQTT_BROKER=mqtt://mqtt:1883
    restart: always
```

## 3. Безопасность

*   **Аутентификация MQTT**: Всегда настраивайте имя пользователя и пароль для вашего MQTT-брокера. Передавайте их в `SynapseApp` через URL: `mqtt://user:password@broker_host:1883`.
*   **TLS/SSL**: Для защиты данных, передаваемых по сети, используйте TLS/SSL шифрование для MQTT-соединений (порт 8883).
*   **Изоляция сети**: Размещайте MQTT-брокер и приложение Synapse Core в защищенной внутренней сети, ограничивая доступ извне.

## 4. Логирование и мониторинг

*   **Уровни логирования**: В production используйте уровень `logging.INFO` или `logging.WARNING`. `logging.DEBUG` может генерировать слишком много данных.
*   **Сбор логов**: Настройте сбор логов (например, с помощью ELK stack, Graylog или сервисов вроде Datadog) для централизованного анализа и мониторинга.
*   **Мониторинг брокера**: Отслеживайте метрики вашего MQTT-брокера (количество подключений, пропускная способность, использование памяти), чтобы вовремя выявлять проблемы с производительностью.

## 6. Масштабирование

## 5. Использование Config и HADiscovery в развертывании

### 5.1. Управление конфигурацией

При развертывании приложений Synapse Core рекомендуется использовать класс `Config` для управления настройками. Это позволяет легко адаптировать приложение к различным средам без изменения кода.

*   **Файлы конфигурации**: Храните специфичные для среды настройки (например, адрес MQTT-брокера, учетные данные) в JSON-файлах (`config.json`).
*   **Переменные окружения**: Используйте переменные окружения (с префиксом `SYNAPSE_`) для переопределения настроек из файлов конфигурации. Это особенно полезно в контейнеризированных средах (Docker, Kubernetes) или при использовании CI/CD пайплайнов.

Пример использования в `systemd` service file:

```ini
[Service]
# ...
Environment="SYNAPSE_MQTT_BROKER=mqtt://production-broker:1883"
Environment="SYNAPSE_LOG_LEVEL=WARNING"
# ...
```

Пример использования в `docker-compose.yml`:

```yaml
services:
  synapse-app:
    # ...
    environment:
      - SYNAPSE_MQTT_BROKER=mqtt://mqtt:1883
      - SYNAPSE_LOG_LEVEL=INFO
    # ...
```

### 5.2. Home Assistant Discovery

Если ваше приложение Synapse Core предназначено для интеграции с Home Assistant, используйте `HADiscovery` для автоматического объявления устройств. Это значительно упрощает настройку и управление устройствами в HA.

*   Убедитесь, что MQTT-брокер, используемый Synapse Core, также настроен в Home Assistant для интеграции MQTT Discovery.
*   Вызывайте `ha_discovery.announce(device)` для каждого устройства после его создания и запуска.



Synapse Core может масштабироваться путем запуска нескольких экземпляров приложения. Однако, необходимо учитывать:

*   **Разделение ответственности**: Если у вас много устройств, вы можете разделить их между несколькими экземплярами `SynapseApp`, каждый из которых управляет своей группой устройств.
*   **Правила**: Если правила зависят от состояния устройств, управляемых разными экземплярами, вам потребуется механизм синхронизации состояния между этими экземплярами (например, через общую базу данных или Redis), так как `StateManager` работает только в рамках одного процесса.

---

*Автоматически сгенерировано Manus AI.*
