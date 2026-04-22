#!/bin/sh
set -eu

mkdir -p "${CACHE_DIR:-/data/cache}"

if [ "$(id -u)" = "0" ]; then
    chown -R app:app "${CACHE_DIR:-/data/cache}" || true
    exec su-exec app /app/freshrss-image-cache-service
fi

exec /app/freshrss-image-cache-service
