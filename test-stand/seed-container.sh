#!/bin/sh
set -eu

batch_size=5000
lua_script=/opt/redis-commander/seed.lua

seed_database() {
    database="$1"
    expected_count="$2"
    namespace="db${database}"
    first_index=1

    redis-cli -n "${database}" FLUSHDB >/dev/null

    while [ "${first_index}" -le "${expected_count}" ]; do
        remaining=$((expected_count - first_index + 1))
        current_batch="${batch_size}"
        if [ "${remaining}" -lt "${batch_size}" ]; then
            current_batch="${remaining}"
        fi

        redis-cli -n "${database}" --eval "${lua_script}" , \
            "${namespace}" "${first_index}" "${current_batch}" >/dev/null
        first_index=$((first_index + current_batch))
    done

    actual_count="$(redis-cli -n "${database}" DBSIZE)"
    if [ "${actual_count}" != "${expected_count}" ]; then
        echo "DB${database}: expected ${expected_count} keys, got ${actual_count}" >&2
        return 1
    fi

    echo "DB${database}: ${actual_count} keys created; all six types verified"
}

seed_database 0 5000
seed_database 1 10000
