# Dockerfile
FROM golang:1.24-alpine AS builder

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/freshrss-image-cache-service .

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata wget

WORKDIR /app
COPY --from=builder /out/freshrss-image-cache-service /app/freshrss-image-cache-service

RUN addgroup -S app && adduser -S -G app app \
    && mkdir -p /data/cache \
    && chown -R app:app /app /data

USER app

ENV LISTEN_ADDR=0.0.0.0:9090 \
    CACHE_DIR=/data/cache \
    ACCESS_TOKEN=change-me \
    FETCH_TIMEOUT=15s \
    MAX_BODY_BYTES=20971520 \
    CACHE_TTL=720h \
    JANITOR_INTERVAL=1h \
    MAX_CACHE_BYTES=10737418240 \
    UPSTREAM_CONCURRENCY=16

EXPOSE 9090
CMD ["/app/freshrss-image-cache-service"]
