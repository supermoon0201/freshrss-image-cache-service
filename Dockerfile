# syntax=docker/dockerfile:1.7

FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/freshrss-image-cache-service .

FROM alpine:3.21

RUN apk add --no-cache ca-certificates su-exec tzdata wget

WORKDIR /app
COPY --from=builder /out/freshrss-image-cache-service /app/freshrss-image-cache-service
COPY entrypoint.sh /app/entrypoint.sh

RUN addgroup -S app && adduser -S -G app app \
    && mkdir -p /data/cache \
    && chown -R app:app /app /data \
    && chmod +x /app/entrypoint.sh

ENV LISTEN_ADDR=0.0.0.0:9090 \
    CACHE_DIR=/data/cache \
    ACCESS_TOKEN=change-me \
    FETCH_TIMEOUT=15s \
    MAX_BODY_BYTES=20971520 \
    CACHE_TTL=720h \
    JANITOR_INTERVAL=1h \
    MAX_CACHE_BYTES=10737418240 \
    UPSTREAM_CONCURRENCY=64 \
    UPSTREAM_CONCURRENCY_PER_HOST=8 \
    META_CACHE_ENTRIES=4096 \
    BLOB_CACHE_ENTRIES=256 \
    BLOB_CACHE_MAX_BYTES=67108864 \
    BLOB_FILE_MAX_BYTES=524288 \
    STALE_GRACE_PERIOD=10m \
    WARM_META_ON_START=true \
    WARM_META_ENTRIES=2048 \
    JANITOR_SHARD_BATCH=32

EXPOSE 9090
ENTRYPOINT ["/app/entrypoint.sh"]
