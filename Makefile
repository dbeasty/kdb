.PHONY: test-go test-kotlin test-cross build-go build-kotlin

test-go:
	cd go && go test -race ./...

test-kotlin:
	./gradlew test --no-daemon

test-cross: test-kotlin test-go
	cd go && go test ./kdb/interop/... -v

build-go:
	cd go && go build -o bin/kdb ./cmd/kdb
	cd go && go build -o bin/kdb-service ./cmd/kdb-service
	cd go && go build -o bin/kdb-inspect ./cmd/kdb-inspect

build-kotlin:
	./gradlew build --no-daemon
