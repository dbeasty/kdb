## Builds the Go-native kdb-service binary (Component 38, Layer 12) for the Lightsail-tier
## simulation in this directory. See README.md for how this approximates (and does not replace)
## a real $7/mo Lightsail load test.

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go/go.mod go/go.sum ./
RUN go mod download
COPY go/ ./
RUN CGO_ENABLED=0 go build -o /out/kdb-service ./cmd/kdb-service

FROM alpine:3.20
RUN adduser -D -u 10001 kdb
COPY --from=build /out/kdb-service /usr/local/bin/kdb-service
USER kdb
EXPOSE 9090
ENTRYPOINT ["/usr/local/bin/kdb-service"]
