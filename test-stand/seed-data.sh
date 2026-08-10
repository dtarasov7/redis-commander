#!/usr/bin/env bash
set -euo pipefail

stand_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
compose_file="${stand_dir}/compose.yaml"

docker compose -f "${compose_file}" up -d --wait redis
docker compose -f "${compose_file}" exec -T redis \
    sh /opt/redis-commander/seed-container.sh

echo "Redis test stand is ready at 127.0.0.1:16379"
echo "Run: python3 redis-commander.py -c test-stand/redis_profiles.json"
