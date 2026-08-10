# Тестовый стенд Redis Commander

Стенд запускает standalone Redis 7.4 с 32 логическими DB. Redis доступен
только локально по адресу `127.0.0.1:16379`.

Запустить Redis и создать тестовые данные:

```bash
./test-stand/seed-data.sh
```

Скрипт очищает DB0 и DB1, а затем создает:

- в DB0 — 5 000 ключей
- в DB1 — 10 000 ключей
- равномерно распределенные типы: `string`, `hash`, `list`, `set`, `zset`, `stream`

Подключить Redis Commander:

```bash
python3 redis-commander.py -c test-stand/redis_profiles.json
```

Остановить и удалить контейнер:

```bash
docker compose -f test-stand/compose.yaml down
```

