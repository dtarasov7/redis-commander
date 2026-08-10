# Тестовый стенд Redis Sentinel

Стенд использует host network и предназначен для Linux. Он запускает Redis на
портах `16381`–`16383` и Sentinel на портах `26381`–`26383`. Имя отслеживаемого
сервиса — `redis-production`, quorum — 2.

```bash
docker compose -f test-sentinel-stand/compose.yaml up -d --wait
python3 redis-commander.py -c test-sentinel-stand/redis_profiles.json
```

Для проверки failover остановите текущий master, например:

```bash
docker compose -f test-sentinel-stand/compose.yaml stop redis-1
```

Sentinel выберет новый master. Следующая однозначно отклоненная запись или
ошибка чтения заставит клиент обновить топологию. Запись с неоднозначным
результатом после сетевого обрыва автоматически не повторяется.

Удаление стенда:

```bash
docker compose -f test-sentinel-stand/compose.yaml down
```

