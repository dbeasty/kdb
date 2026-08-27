.PHONY: test-go test-kotlin test-cross build-go build-kotlin

# Single version source (see go/kdb/version). Release tags override: make build-go VERSION=v1.2.3
VERSION ?= $(shell cat VERSION)
GO_LDFLAGS := -X github.com/limidus/kdb/go/kdb/version.Version=$(VERSION)

test-go:
	cd go && go test -race ./...

test-kotlin:
	# `test` only runs plain-kotlin.jvm-plugin modules' test tasks - Kotlin Multiplatform
	# modules have jvmTest/jsTest/*Test instead, aggregated as allTests, and Gradle's `test`
	# invocation doesn't reach those at all. Both are required for real full coverage - this
	# silently ran only the former for a long time (see docs/kdb-finish-up-plan.md's "MPP
	# test-CI gap" finding) before allTests was repaired to actually pass.
	./gradlew test allTests --no-daemon

test-cross: test-kotlin test-go
	cd go && go test ./kdb/interop/... -v

build-go:
	cd go && go build -ldflags "$(GO_LDFLAGS)" -o bin/kdb ./cmd/kdb
	cd go && go build -ldflags "$(GO_LDFLAGS)" -o bin/kdb-service ./cmd/kdb-service
	cd go && go build -ldflags "$(GO_LDFLAGS)" -o bin/kdb-inspect ./cmd/kdb-inspect

build-kotlin:
	./gradlew build --no-daemon
