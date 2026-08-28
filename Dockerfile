# KDB product image: the Go-native kdb-service plus the kdb CLI and kdb-inspect tooling.
# Promoted from docs/benchmarks/lightsail-sim/Dockerfile (kdb-finish-up-plan Phase 2.9).
#
# Build:  docker build -t kdb-service --build-arg VERSION=$(cat VERSION) .
# Run:    docker run -v kdb-data:/var/lib/kdb -p 9090-9093:9090-9093 kdb-service \
#           --data-dir /var/lib/kdb --admin-addr 0.0.0.0:9093
#
# The image runs as a non-root user and honors the exit-75 orderly-abort contract
# (kdb-spec-layer13 Component 50) - run with --restart=on-failure so a pressure abort restarts.

FROM golang:1.26-alpine AS build
ARG VERSION=0.0.0-docker
WORKDIR /src
COPY go/go.mod go/go.sum ./
RUN go mod download
COPY go/ ./
RUN CGO_ENABLED=0 go build -ldflags "-X github.com/limidus/kdb/go/kdb/version.Version=${VERSION}" -o /out/kdb-service ./cmd/kdb-service \
    && CGO_ENABLED=0 go build -ldflags "-X github.com/limidus/kdb/go/kdb/version.Version=${VERSION}" -o /out/kdb ./cmd/kdb \
    && CGO_ENABLED=0 go build -ldflags "-X github.com/limidus/kdb/go/kdb/version.Version=${VERSION}" -o /out/kdb-inspect ./cmd/kdb-inspect

FROM alpine:3.20
RUN adduser -D -u 10001 kdb && mkdir -p /var/lib/kdb && chown kdb:kdb /var/lib/kdb
COPY --from=build /out/kdb-service /out/kdb /out/kdb-inspect /usr/local/bin/
USER kdb
VOLUME /var/lib/kdb
# 9090 SQL wire, 9091 peer sync, 9092 stream, 9093 admin (healthz/readyz/metrics/pprof)
EXPOSE 9090 9091 9092 9093
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s \
    CMD wget -q -O /dev/null http://127.0.0.1:9093/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/kdb-service"]
CMD ["--data-dir", "/var/lib/kdb", "--admin-addr", "0.0.0.0:9093", "--sql-addr", "tcp://0.0.0.0:9090?bind=true", "--peer-addr", "tcp://0.0.0.0:9091?bind=true", "--stream-addr", "tcp://0.0.0.0:9092?bind=true"]
