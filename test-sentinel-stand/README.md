# Redis Sentinel test stand

This Linux-oriented stand uses host networking. Redis listens on ports
`16381`–`16383`; Sentinel listens on `26381`–`26383`. The monitored service is
`redis-production` with quorum 2.

```bash
docker compose -f test-sentinel-stand/compose.yaml up -d --wait
python3 redis-commander.py -c test-sentinel-stand/redis_profiles.json
```

To exercise failover, stop the current master, for example:

```bash
docker compose -f test-sentinel-stand/compose.yaml stop redis-1
```

Sentinel promotes a new master. The next rejected write or failed read refreshes
the client topology. A network-interrupted write with an ambiguous result is not
retried automatically.

Remove the stand:

```bash
docker compose -f test-sentinel-stand/compose.yaml down
```

