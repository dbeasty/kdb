# KDB product image: the Go-native kdb-service plus the kdb CLI and kdb-inspect tooling.
# Promoted from docs/benchmarks/lightsail-sim/Dockerfile (kdb-finish-up-plan Phase 2.9).
#
# Build:  docker build -t kdb-service \
#           --build-arg VERSION=$(cat VERSION) \
#           --build-arg GIT_COMMIT=$(git rev-parse HEAD) .
# Run:    docker run -v kdb-data:/var/lib/kdb -p 9090-9093:9090-9093 kdb-service \
#           --data-dir /var/lib/kdb --admin-addr 0.0.0.0:9093
#
# The image runs as a non-root user and honors the exit-75 orderly-abort contract
# (kdb-spec-layer13 Component 50) - run with --restart=on-failure so a pressure abort restarts.

FROM golang:1.26-alpine AS build
ARG VERSION=0.0.0-docker
# Only go/ is copied into the build stage, so there is no .git here for the Go toolchain to
# stamp the commit from automatically - it has to be passed in, or the image ships binaries that
# can't be traced back to their source. GIT_DIRTY guards against releasing an image built from a
# tree with uncommitted changes without that being visible in --version.
ARG GIT_COMMIT=unknown
ARG GIT_DIRTY=false
ARG BUILD_DATE
WORKDIR /src
COPY go/go.mod go/go.sum ./
RUN go mod download
COPY go/ ./
RUN VP=github.com/limidus/kdb/go/kdb/version; \
    LDFLAGS="-X $VP.Version=${VERSION} -X $VP.Commit=${GIT_COMMIT} -X $VP.Dirty=${GIT_DIRTY} -X $VP.BuildDate=${BUILD_DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"; \
    for bin in kdb-service kdb kdb-inspect; do \
      CGO_ENABLED=0 go build -ldflags "$LDFLAGS" -o "/out/$bin" "./cmd/$bin" || exit 1; \
    done

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
