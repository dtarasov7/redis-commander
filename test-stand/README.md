# Redis Commander test stand

The stand runs standalone Redis 7.4 with 32 logical databases and exposes it
only on `127.0.0.1:16379`.

Start Redis and create the test data:

```bash
./test-stand/seed-data.sh
```

The script clears DB0 and DB1, then creates:

- DB0: 5,000 keys
- DB1: 10,000 keys
- six key types distributed evenly: `string`, `hash`, `list`, `set`, `zset`, `stream`

Connect Redis Commander:

```bash
python3 redis-commander.py -c test-stand/redis_profiles.json
```

Stop and remove the container:

```bash
docker compose -f test-stand/compose.yaml down
```

