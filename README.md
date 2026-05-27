# KDB — Portable Embedded Database Engine

> **Status: early implementation.** This repository contains the architecture specification, a **Kotlin Multiplatform** implementation, a parallel **Go** implementation under [`go/`](go/), and design artefacts for KDB. APIs and behaviour are subject to change. See the [user guide](docs/kdb-user-guide.md) for how to run and embed what exists today. JVM **file persistence** (`jdbc:kdb:file://…`, CLI `--data-dir`) is implemented via the SERVER storage engine and delta log replay.

---

## Overview

KDB is a portable, multi-runtime embedded database engine. The **Kotlin Multiplatform** tree is the original implementation: the entire engine compiles to browser (Kotlin/JS), JVM, and native (Kotlin/Native). A **Go** port in [`go/`](go/) mirrors the same layered architecture for native servers, CLI, `database/sql`, and WASM (browser). Both implementations follow [`docs/kdb-spec.md`](docs/kdb-spec.md); Kotlin and Go are maintained side by side with cross-language wire golden tests.

KDB is best understood as **source control for structured documents**. You store whole JSON documents. You retrieve whole JSON documents. Optionally you declare a schema — a typed, indexed lens over part of each document — which unlocks SQL querying, JDBC connectivity, and ORM integration. The document is always the truth. The schema is always a lens. Both coexist without friction.

Primary storage is JSON. Binary storage uses the KDB binary codec — a schema-driven typed encoding supporting dates, timestamps, decimals, UUIDs, enums, named record types, maps with non-string keys, BigInteger, BigDecimal, and extensible custom types — compressed with zstd. SQL operates as an index and query layer over schema-declared fields, but raw JSON access is always available alongside SQL in the same query. All data lives in versioned, content-addressed namespaces with git-like history. Peer synchronisation follows a source-control model: peers are fully independent, can diverge arbitrarily, and merge when they choose to.

---

## Goals

- Full engine runs on every target: browser (JS), JVM backend, native binary
- Single Kotlin codebase compiled to all targets via Kotlin Multiplatform
- Documents are stored and retrieved as whole JSON — always, exactly as provided
- Schema is an optional typed lens over document fields, not a constraint on document shape
- SQL and raw JSON access work together in the same query via the `_doc` column
- JDBC driver for full compatibility with Java ORMs, SQL IDEs, and BI tools — highest priority
- Git-like versioning per namespace: full commit DAG, branches, tags, checkout, diff
- Source-control peer protocol: peers diverge independently and merge on reconnect
- Three client modes: pure stream, write-back stream, full peer
- Transaction-based conflict resolution with application-controlled replay
- Tiered storage: hot → warm → cold → ice archive
- CLI interface modelled on git for developer tooling
- GPU-accelerated bulk read path and vector similarity as an optional index type

## Non-Goals

- KDB is not a general-purpose SQL database; SQL is an index and query interface, not the storage model
- KDB does not enforce a central authoritative node; all peers are equal
- KDB does not replace Kafka as a high-throughput event bus
- KDB does not manage network topology; peer discovery is the application's responsibility

## Design Principles

- The document is always the truth; schema is always a lens
- The engine is the same on every platform; only adapters differ
- JSON is always the canonical representation of meaning
- KDB binary codec + zstd is always a storage and transport optimisation, never a requirement
- SQL addresses data via schema; `_doc` always gives access to the whole document
- Peers are equal; any peer can sync with any other peer directly
- Divergence is normal; merging is explicit and application-controlled
- Conflicts surface to the application; KDB never silently resolves them
- History is cheap because unchanged content is shared by hash

---

## Documentation

| Document | Audience |
|----------|----------|
| [**User guide**](docs/kdb-user-guide.md) | Run the inspect CLI, embed via JDBC (Java), or Kotlin/JS (browser) |
| [**Go porting guide**](docs/go-porting.md) | Go module layout, build tags, interop tests |
| [**Architecture specification**](docs/kdb-spec.md) | Full system design, protocols, and layer specs |

### Quick start (Kotlin)

```bash
./gradlew build
./gradlew :kdb-cli:runCli --args="init myapp/users"
./gradlew :kdb-jdbc:test
./gradlew :kdb-inspect:inspectCli --args="dump-wire --file /path/to/frame.bin"
```

```java
// JDBC (in-memory) — seed data via embedded runtime; see user guide
Class.forName("dev.kdb.jdbc.KdbDriver");
Connection c = DriverManager.getConnection("jdbc:kdb:memory:///demo/users");
```

### Quick start (Go)

```bash
cd go && go test ./...
make build-go
./go/bin/kdb --data-dir /tmp/kdb-data init myapp/users
./go/bin/kdb --data-dir /tmp/kdb-data put myapp/users doc1 '{"name":"Ada"}'
```

```go
import (
    "database/sql"
    _ "github.com/limidus/kdb/go/kdb/driver"
)
db, _ := sql.Open("kdb", "kdb://memory:///demo/users?unique=true")
```

---

## Specification

The current architecture specification is [`docs/kdb-spec.md`](docs/kdb-spec.md).

---

## License

Copyright © 2026 Limidus Corp. All rights reserved.

Licensed under the [GNU Affero General Public License v3.0](LICENSE).

---

## Disclaimer

This software and its associated specifications are provided "as is", without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose, and non-infringement. In no event shall the authors or copyright holders be liable for any claim, damages, or other liability, whether in an action of contract, tort, or otherwise, arising from, out of, or in connection with the software or the use or other dealings in the software.
