# Go port guide

KDB is implemented in parallel: **Kotlin** (Gradle, `kdb-*` modules) and **Go** (`go/` module).

## Module path

```
github.com/limidus/kdb/go
```

Packages mirror Kotlin `dev.kdb.*` under `go/kdb/`.

## Build and test

```bash
cd go
go test ./...
go test -race ./...
GOOS=js GOARCH=wasm go build -o /dev/null ./wasm/demo/
```

From repo root:

```bash
make test-go
make test-kotlin
```

## Build tags

| Tag | Use |
|-----|-----|
| `js && wasm` | Browser WASM (`storage/io`, `compute/webgpu`) |
| `!js` | Native server / CLI |

## Cross-language interop

Golden wire fixtures live in `go/testdata/golden/`. Regenerate from Kotlin:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
./gradlew :kdb-integration:test --tests "dev.kdb.integration.ExportGoldenTest"
```

Verify Go matches Kotlin bytes:

```bash
cd go && go test ./kdb/interop/... -v
```

Kotlin remains the reference for wire bytes until both sides pass shared golden tests.

## JDBC vs database/sql

- Java: `dev.kdb.jdbc.KdbDriver` (`jdbc:kdb:…`)
- Go: `github.com/limidus/kdb/go/kdb/driver` (`kdb://memory/…`, `kdb://file/…`)

Hibernate integration stays Kotlin-only.
