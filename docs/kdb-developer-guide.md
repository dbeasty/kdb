# KDB Developer Guide

For developers who will **use**, **embed**, **extend** or **contribute to** KDB. It covers
toolchains, the repository layout, building and testing, the data model, the Go APIs, the wire
protocol, configuration, extension points, and how changes are made and checked.

It does not repeat the operator material in the [User guide](kdb-user-guide.md) (running a fleet,
backup and recovery runbooks, resource governance, replication topology). It links there instead.
Where the two overlap, this guide was written against the code at **v0.6.2** and says so when the
code and an older document disagree (see [Known documentation drift](#known-documentation-drift)).

| If you want... | Read |
|---|---|
| to run the server and click around | [Quick start](#5-quick-start) |
| to put KDB inside a Go program | [Embedding in Go](#7-embedding-in-go), [database/sql](#9-the-databasesql-driver) |
| to talk to a server from Go | [The Go client SDK](#8-the-go-client-sdk) |
| to write a client in another language | [Wire protocol](#12-wire-protocol-for-client-authors) |
| to add an index, resolver, transport or auth provider | [Extension points](#15-extension-points) |
| to send a pull request | [Conventions](#18-conventions) and [CI](#19-pull-requests-and-ci) |

**Verification.** Every Go program in this guide is a file that was compiled, vetted and run
against the repository at v0.6.2 (module path `github.com/limidus/kdb/go`, Go 1.26.3, macOS
arm64). Shell commands were run unless marked **[not run]**. The last section,
[What was and was not verified](#21-what-was-and-was-not-verified), lists the exceptions.

**Licensing.** KDB is dual-licensed: AGPL-3.0 or a commercial licence from Limidus Corp
([LICENSING.md](../LICENSING.md)). Embedding it in a product you distribute, or running a modified
copy as a network service, has consequences under the AGPL; read that file before you depend on it.

---

## 1. Prerequisites and toolchains

| Tool | Version | Needed for | Source of the requirement |
|---|---|---|---|
| Go | **1.26.3** (`go 1.26.0`, `toolchain go1.26.3`) | everything under `go/` | `go/go.mod`. With `GOTOOLCHAIN=auto` (the default) an older `go` downloads the pinned patch release by itself |
| JDK | 21 in CI (Temurin); 17 for the Android job; the user guide says 17+ | the Kotlin tree | `.github/workflows/ci.yml` |
| Gradle | 8.11.1, via the committed `./gradlew` wrapper | the Kotlin tree | `gradle/wrapper/gradle-wrapper.properties` |
| Kotlin | 2.0.21 (plugins resolved by Gradle) | the Kotlin tree | `gradle/libs.versions.toml` |
| Python | 3.12 in CI, `pytest>=8` | the end-to-end suite | `kdb-integration/e2e/requirements.txt` |
| Node | 22 in CI, `>=20` declared | the TypeScript client | `packages/kdb-client/package.json` |
| `gomobile` / `gobind` | the module's pinned `golang.org/x/mobile` | `go/mobile/kdbsync` | `ci.yml` (`mobile-android`, `mobile-ios`) |
| Docker | any recent | the container image | `Dockerfile` |

You only need the toolchain for the part of the tree you touch. The Go tree builds and tests with
Go alone. The Kotlin tree needs only a JDK. The cross-language suites need both.

Runtime dependencies of the Go module are small: `klauspost/compress` (zstd), `golang.org/x/crypto`
(PBKDF2), `golang.org/x/sync`, the AWS SDK v2 (the optional S3 replica/archive tier),
and `dop251/goja` (the JavaScript engine behind stored procedures). The gRPC transport lives in a
**separate module** (`go/grpc`) precisely so none of its dependencies reach the embedded and
mobile builds.

Platform notes: the data-directory lock and read-only replicas need `flock(2)` (unix only); the
container image and the production target are Linux; macOS is the primary development platform. On
macOS `--sync-mode=full` uses `F_FULLFSYNC`, which is slow and makes benchmarks sensitive to other
I/O on the machine ([Benchmarks](#46-benchmarks)).

---

## 2. Getting the source and the repository map

```bash
git clone git@github.com:dbeasty/kdb.git
cd kdb
cat VERSION            # 0.6.2 - the single version source for Go and Kotlin
```

KDB is **two implementations of one specification** that share an on-disk format and a wire
protocol: a Kotlin Multiplatform tree (JVM, browser JS, native) and a Go tree. Either can open a
data directory written by the other.

```
kdb/
  VERSION  Makefile  Dockerfile  LICENSE  LICENSING.md
  go/                       Go module github.com/limidus/kdb/go
    cmd/
      kdb/                    the CLI (init/put/get/query/log/branch/sync/...)
      kdb-service/            the server (a one-line main; the service lives in kdb/service)
      kdb-inspect/            verify / repair / backup / restore / dump-wire
      kdb-loadtest/  kdb-pressure-test/        load generators for a running server
      kdb-e2e-helper/  kdb-tsfixtures/  kdb-tsinterop/   test tooling, no stability promise
    kdb/                    the library; one package per concern (below)
    grpc/                   SEPARATE module: gRPC transport + kdb-service-grpc binary
    mobile/kdbsync/         gomobile-shaped facade for phones
    wasm/demo/              browser WASM build check
    embedbundle/            generator for the embeddable source zip
    testdata/golden/        cross-language fixtures (codec, physical, conflict_corpus, search)
  kdb-*/                    ~60 Kotlin modules, one Gradle project each (below)
  kdb-integration/          Kotlin integration tests + the Python end-to-end suite (e2e/)
  packages/kdb-client/      @kdb/client - a third, TypeScript wire implementation
  docs/                     architecture, LLD, per-layer specs, plans, benchmarks, releases/
  deploy/                   compose.yaml and a systemd unit
  .github/workflows/        ci.yml, e2e-nightly.yml, benchmark.yml, release.yml
```

The Go library, grouped by role (`go/kdb/<pkg>`):

| Role | Packages |
|---|---|
| **Model and codec** | `codec` (KDB binary codec, hashes, UUIDs), `document` (documents, commits, ops, trees), `json`, `schema`, `error` (stable numeric error codes) |
| **History** | `dag` (commit DAG, branches, tags, walk), `compaction`, `tier` |
| **Storage** | `storage` (adapter interface), `storage/{wal,memtable,sstable,delta,engine,io,io/s3,mem}`, `compression`, `file` (attachments), `backup`, `integrity`, `recovery` |
| **Runtime** | `embed` (open a namespace on disk or in memory, the `Host`), `transaction`, `index` (+ `index/stores`, `fulltext`, `vector`), `sql`, `query`, `policy`, `script` (stored procedures), `compute` |
| **Server** | `server` (the engine behind every listener), `service` (the `kdb-service` main), `config`, `control` (HTTP control plane + UI), `metrics`, `inspect`, `version`, `auth` |
| **Network** | `wire` (frames and messages), `transport/{core,tcp,ws}`, `stream`, `client`, `driver` (`database/sql`), `peersync`, `replication`, `syncnode` |
| **Cross-language** | `interop` (golden and live-JVM tests), `integration` |

The Kotlin modules mirror the same layers (`kdb-codec`, `kdb-document`, `kdb-dag`, `kdb-storage-*`,
`kdb-sql`, `kdb-index-*`, `kdb-wire`, `kdb-transport-{core,tcp,ws}`, `kdb-peer-sync`, `kdb-server`,
`kdb-service`, `kdb-jdbc`, `kdb-hibernate`, `kdb-cli`, `kdb-inspect`, `kdb-benchmark`, ...). Package
names are `dev.kdb.*`. Go packages deliberately mirror them (`docs/go-porting.md`).

### 2.1 Using KDB from your own Go project

There is no published module proxy path yet (the user guide: "Published Maven / npm artifacts: not
yet"). You consume the Go tree one of three ways:

1. **A `replace` to a checkout** (what this guide's examples use).
2. **The embeddable source bundle**, `kdb-go-embed-<version>.zip`, built by `make release-bundle`
   and attached to each release. It contains the transitive closure of `kdb/embed`, `kdb/driver`,
   `kdb/sql`, `kdb/query/hybrid`, `kdb/index` and `kdb/server`
   (`go/embedbundle/entrypoints.txt`), and is unpacked and referenced with
   `go mod edit -replace` ([release plan §2.4](kdb-release-plan.md)).
3. **A vendored, re-homed copy** via the bundle's `rewrite-module.sh` (a convenience, not the
   supported path).

The verified recipe for (1):

```bash
mkdir myapp && cd myapp
go mod init example.com/myapp
go mod edit -require github.com/limidus/kdb/go@v0.0.0 \
            -replace github.com/limidus/kdb/go=/path/to/kdb/go
# ... write main.go that imports github.com/limidus/kdb/go/kdb/... ...
go mod tidy        # REQUIRED: without it the build fails with "missing go.sum entry"
go run .
```

`go mod tidy` is not optional: the module's dependency graph (zstd, x/crypto, the AWS SDK) is not
in your `go.sum` until it runs. The examples in this guide were built exactly this way.

---

## 3. Building

### 3.1 Go

```bash
make build-go          # -> go/bin/{kdb, kdb-service, kdb-inspect, kdb-service-grpc}
```

`make build-go` runs four `go build` lines with version stamping (`-ldflags -X
github.com/limidus/kdb/go/kdb/version.{Version,Commit,Dirty,BuildDate}=...`). The first three are
in the core module; `kdb-service-grpc` is built from `go/grpc` and is `kdb-service` with the gRPC
listener linked in (`--grpc-addr`). The plain `kdb-service` *refuses* a non-empty `--grpc-addr`
rather than ignoring it.

```bash
./go/bin/kdb --version
# 0.6.2 (commit b039218-dirty, built 2026-10-11T22:37:28Z, go1.26.3 darwin/arm64)
make print-version                       # what the next build will claim to be
```

Anything not stamped by `make` (a bare `go build ./cmd/kdb`) reports a placeholder build identity.
Release builds are reproducible: `make release` produces binaries for `linux/amd64`,
`linux/arm64`, `darwin/arm64` plus the source bundle in `dist/`, and `make release-verify`
rebuilds and compares ([release plan](kdb-release-plan.md)).

Other Go builds that CI checks:

```bash
cd go && GOOS=js GOARCH=wasm go build -o /dev/null ./wasm/demo/     # browser WASM
cd go && go build -o bin/kdb-e2e-helper ./cmd/kdb-e2e-helper         # needed by the e2e suite
cd go && go run ./cmd/kdb-tsfixtures                                 # regenerate TS-client fixtures
# phones (CI: mobile-android / mobile-ios), run from go/        [not run locally]
go install golang.org/x/mobile/cmd/gomobile golang.org/x/mobile/cmd/gobind
gomobile bind -target=android/arm64,android/amd64 -androidapi 21 -o kdbsync.aar ./mobile/kdbsync
gomobile bind -target=ios -o Kdbsync.xcframework ./mobile/kdbsync
```

### 3.2 Container image

```bash
docker build -t kdb-service \
  --build-arg VERSION=$(cat VERSION) --build-arg GIT_COMMIT=$(git rev-parse HEAD) .   # [not run]
docker run -v kdb-data:/var/lib/kdb -p 9090-9093:9090-9093 --restart=on-failure kdb-service
```

From the `Dockerfile`: a two-stage build (`golang:1.26-alpine` -> `alpine:3.20`), non-root user
10001, ports 9090 (SQL), 9091 (peer), 9092 (stream), 9093 (admin) and a `/healthz` healthcheck.
`--restart=on-failure` matters: a pressure abort exits **75** and relies on the supervisor to
restart. Compose and systemd examples are in `deploy/`.

### 3.3 Kotlin

```bash
./gradlew build --no-daemon           # what CI's kotlin-test job runs; also `make build-kotlin`
./gradlew :kdb-cli:runCli --args="--data-dir ~/.kdb init myapp/users"
./gradlew :kdb-inspect:inspectCli --args="dump-wire --file /path/to/frame.bin"
./gradlew :kdb-service:runService --args="--listen-sql-ws kdb-ws://127.0.0.1:17444/kdb?bind=true --namespace demo/interop"
```

All Kotlin commands in this guide are taken from the `Makefile`, the README and CI and were **not
re-run** for this guide (a full Gradle configuration of 60 modules is long, and the checkout is
shared). `./gradlew build` is the Kotlin gate: it covers `check` for the Multiplatform
`jvmTest`/`jsTest`/`linuxX64Test` source sets *and* compiles each source set's common metadata. The
older `test allTests` pair misses the metadata compile, which is how an `expect` declaration that
only the metadata compiler rejects once kept `build` red unnoticed. `macosArm64` tasks only run on
a macOS host.

---

## 4. Running tests

### 4.1 Go unit and integration tests

```bash
make test-go                 # cd go && go test -race ./...   then   cd go/grpc && go test -race ./...
cd go && go test ./kdb/wire/ ./kdb/driver/ ./kdb/client/ ./kdb/script/    # fast subset (ran in ~35 s)
cd go && go vet ./... && test -z "$(gofmt -l .)"                           # CI's lint step
```

The core module's `./...` does **not** reach `go/grpc`; it is a separate module and the Makefile
sweeps it explicitly. Most "integration" tests in Go are ordinary `_test.go` files that start real
listeners on port 0 in-process (see `go/kdb/client/*_test.go`, `go/kdb/server/*_test.go`).

Build-tagged tests, off by default:

| Tag | What it enables | How CI runs it |
|---|---|---|
| `interop` | Go client/tests against a **live JVM** `kdb-service` | `KDB_JVM_SQL_WS_URI=ws://127.0.0.1:17444/kdb go test -tags interop ./kdb/interop/... ./kdb/client/... -v` (job `go-interop`; fails if anything reports `SKIP`) |
| `s3integration` | the S3 replica/archive tier against LocalStack | `KDB_S3_BUCKET=kdb-ci KDB_S3_ENDPOINT=http://127.0.0.1:4566 KDB_S3_REGION=us-east-1 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test go test -tags s3integration ./kdb/storage/io/s3/... -v` (nightly) |

### 4.2 Kotlin tests

```bash
make test-kotlin             # ./gradlew test allTests --no-daemon
./gradlew :kdb-jdbc:test     # one module
./gradlew :kdb-integration:test :kdb-integration:e2ePython --no-daemon   # job `integration`
```

`e2ePython` drives the Python suite below and therefore needs the Go binaries built first.

### 4.3 End-to-end suite (Python, real processes)

`kdb-integration/e2e` launches real `kdb-service` processes (single servers, peer-sync clusters,
`kill -9` and corruption scenarios) and drives them through `go/bin/kdb-e2e-helper` and the CLI.

```bash
make build-go && (cd go && go build -o bin/kdb-e2e-helper ./cmd/kdb-e2e-helper)
pip install -r kdb-integration/e2e/requirements.txt
cd kdb-integration/e2e
python -m pytest -v                          # default: server/cluster/destructive
python -m pytest -q test_server_lifecycle.py # one file (4 passed in 2.4 s here)
python -m pytest -v -m "slow or destructive" # the nightly selection
python -m pytest -v -m interop               # needs the JVM CLI classpath, below
```

Markers (`pytest.ini`): `server`, `cluster`, `interop`, `slow`, `destructive`, `s3`. The default
`addopts` is `-m "not interop and not slow and not s3"`.

Environment: `KDB_SERVICE_BIN` and `KDB_E2E_HELPER_BIN` override the binaries (default
`go/bin/...`); `KDB_CLI_BIN` selects the Go CLI; `KDB_CLI_CLASSPATH_FILE` selects the JVM CLI. For
the differential Go-vs-JVM run (CI job `e2e-interop`):

```bash
./gradlew :kdb-cli:writeCliClasspath :kdb-cli:jar --no-daemon
KDB_CLI_CLASSPATH_FILE=$PWD/kdb-cli/build/cli-classpath.txt python -m pytest -v -m interop
```

### 4.4 Golden files and cross-language compatibility

Fixtures under `go/testdata/golden/` are committed so ordinary test runs need neither language's
toolchain to exercise the other's bytes. Two generators exist:

```bash
make test-physical-golden    # Kotlin exports -> Go checks, Go exports -> Kotlin checks, then
                             # `git diff --exit-code -- go/testdata/golden`
cd go && go test ./kdb/interop/... -count=1 -v          # Go side regenerates + verifies
cd go && go run ./cmd/kdb-tsfixtures                     # the TypeScript client's fixtures
```

Both exporters are idempotent, so a dirty tree afterwards means an encoder now produces different
bytes than the committed fixture, i.e. a **physical or wire format change**, which silently breaks a
mixed Go/JVM deployment. CI's `golden-freshness` job fails on drift *and* on exporter output nobody
committed. If the change is deliberate, review the diff and commit the fixtures in the same PR.

### 4.5 TypeScript client

```bash
cd packages/kdb-client && npm ci && npm run typecheck && npm test && npm run build
```

`npm test` includes a live interop suite that starts a real Go server (`cmd/kdb-tsinterop`) and
skips cleanly without the Go toolchain.

### 4.6 Benchmarks

```bash
make bench           # cd go && go test ./... -run '^$' -bench . -benchmem
make bench-write     # the write path: server, transaction, storage/engine, compression (-benchtime 1000x)
cd go && go test ./kdb/server/ -run '^$' -bench BenchmarkFileBackedUpsert -benchmem -benchtime 50x
./gradlew :kdb-benchmark:jmh --no-daemon          # JMH, weekly in CI
(cd go && go run ./cmd/kdb-loadtest -addr 127.0.0.1:9090 -duration 30s -concurrency 16)   # against a running server
```

`-benchmem` is always on: the engine's single largest allocation (a throwaway zstd encoder per
commit, ~21 MB) survived for as long as it did because no benchmark reported B/op. Rules that keep
numbers meaningful:

* **Check machine load first** (`uptime`). A benchmark on a box that is also running another suite,
  a Gradle build or an e2e run reports contention as a "regression", sometimes a very large one.
* **Never run two heavy jobs at once** (two benchmark suites, JMH plus Go, a build during a bench).
* On macOS, any process that adds a flushing log to the same device (it triggers `F_FULLFSYNC`) can
  halve a neighbour's throughput; run a control with no protocol attached before blaming code.
* Compare like with like: same `--durability`, same `--sync-mode`, same history mode. The
  comparison tables live in `docs/benchmarks/` (for example `workload-matrix.md` and
  `write-durability-modes.md`).

---

## 5. Quick start

### 5.1 The CLI (embedded, no server)

All of this ran against a scratch directory:

```bash
./go/bin/kdb --data-dir /tmp/kdb-data init myapp/users
./go/bin/kdb --data-dir /tmp/kdb-data put myapp/users '{"name":"Ada","age":36}'
# {"docId":"2953701b-6b53-4006-8c31-12607635ecb6","docIdShort":"2953701b","commit":"4d8f092b..."}
./go/bin/kdb --data-dir /tmp/kdb-data get myapp/users 2953701b-6b53-4006-8c31-12607635ecb6
# {"name":"Ada","age":36}
./go/bin/kdb --data-dir /tmp/kdb-data query myapp/users "SELECT kdb_id, name FROM users"
# kdb_id<TAB>name
./go/bin/kdb --data-dir /tmp/kdb-data log myapp/users --oneline
./go/bin/kdb --data-dir /tmp/kdb-data status myapp/users        # HEAD <hash> / namespace <ns>
```

`put` with a top-level `"id"` writes *that* document ([Data model](#6-the-data-model-in-practical-terms)),
so a second `put` of `{"id":"2953701b-...","name":"Ada L","age":37}` makes a second version of the
same document. Then `get <ns> <id> --at head~1` reads the earlier version, `diff <ns> head~1 head`
shows what changed, and `revert <ns> head~1` writes a *new* commit restoring that state.

The CLI takes the data directory's exclusive lock for the duration of each invocation, so it
cannot run while a `kdb-service` has the same directory open.

### 5.2 The server

```bash
./go/bin/kdb-service --data-dir /tmp/kdb-srv --namespace myapp/users \
    --sql-addr "tcp://127.0.0.1:9090?bind=true" --admin-addr 127.0.0.1:9099
```

The service logs one line summarising every subsystem and the exact build:

```
level=INFO msg="KDB service started" version=0.6.2 commit=b039218... sql="enabled (127.0.0.1:9090)" ...
rbac=disabled memory_limit="36864MB auto-detected (zones at 70/85/93%, reserve 48MB, ...)" namespace=myapp/users
```

```bash
curl -s localhost:9099/healthz     # ok + version/commit/build_date/node_id
curl -s localhost:9099/readyz      # ready   (503 "not ready: starting|draining" otherwise)
curl -s localhost:9099/metrics     # Prometheus text
```

Defaults worth knowing: Only the SQL listener is **on by default, on loopback**
(`127.0.0.1:9090`); the peer-sync, stream, admin, WebSocket, gRPC and control listeners are **off**,
so two default instances can run side by side. Pass an address (`--peer-addr tcp://127.0.0.1:9091?bind=true`)
to turn one on, or an empty value to turn it off. The `?bind=true` suffix on a listen address
is what marks a URI as a listen address (a client uses the same scheme without it). One server
serves **every namespace under its data root**: `--namespace` only names the default one, and a
write to a namespace that does not exist creates it (a read does not).

### 5.3 A client against it

````go
// vec/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := client.Connect(ctx, addr(), "")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	ns := "vec/items"
	for _, q := range []string{
		"CREATE TABLE items (name VARCHAR, price INT)",
		"CREATE INDEX idx_name ON items (name) USING FULLTEXT",
	} {
		fmt.Println(q, "->", c.Exec(ctx, ns, q, nil))
	}
	for i, n := range []string{"red apple", "green apple", "blue car"} {
		_, err := c.PutJSON(ctx, ns, fmt.Sprintf("00000000-0000-0000-0000-00000000000%d", i+1),
			[]byte(fmt.Sprintf(`{"name":%q,"price":%d,"emb":[%d,0,1]}`, n, 10*(i+1), i)))
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("vec index ->", c.Exec(ctx, ns, "CREATE INDEX idx_emb ON items (emb) USING VECTOR WITH (dimensions = 3, metric = 'cosine')", nil))
	for _, q := range []string{
		"SELECT name FROM items WHERE price BETWEEN 10 AND 20 ORDER BY price",
		"SELECT name FROM items WHERE MATCH(idx_name, 'apple') ORDER BY name",
		"SELECT name FROM items ORDER BY SIMILARITY(emb, [0,0,1]) LIMIT 2",
		"SELECT COUNT(*) AS n, MAX(price) AS hi FROM items",
	} {
		cols, rows, err := c.QueryRaw(ctx, ns, q, nil)
		fmt.Println(q, "->", cols, rows, err)
	}
}

// addr is the kdb-service SQL wire address: $KDB_ADDR, or the default listener.
func addr() string {
	if a := os.Getenv("KDB_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:9090"
}
````

Run it with `KDB_ADDR=127.0.0.1:9090 go run .` (excerpt of its output):

```
SELECT name FROM items WHERE price BETWEEN 10 AND 20 ORDER BY price -> [name] [[red apple] [green apple]]
SELECT name FROM items WHERE MATCH(idx_name, 'apple') ORDER BY name -> [name] [[green apple] [red apple]]
SELECT name FROM items ORDER BY SIMILARITY(emb, [0,0,1]) LIMIT 2 -> [name] [[blue car] [green apple]]
SELECT COUNT(*) AS n, MAX(price) AS hi FROM items -> [n hi] [[3 30]]
```

(Section 10 explains what each statement exercises.)

---

## 6. The data model in practical terms

KDB is **source control for JSON documents**. Everything below is visible in the examples above.

**Namespace.** The unit of storage, history, replication and (by default) transactions. Its id is a
path, `catalog/name` (`myapp/users`, or deeper: `site/berlin/orders`); the catalog is the first
segment. On disk it is `<dataRoot>/ns/<namespace>/`. A namespace is also, to SQL, **one table**
(the name after `FROM` is validated but the rows always come from the connection's namespace; use
`<catalog>.<name>` to reach another namespace inside a transaction). Reserved namespaces:
`_kdb/meta` (replicated definitions such as resolution chains and procedures) and
`_system/users`, `_system/roles` (the RBAC registry).

**Document.** A UUID plus a JSON object stored **byte-exact** (nothing injected, keys never
reordered). Identity comes from an optional top-level `id`:

| `id` in the body | Document id |
|---|---|
| absent | a random UUID, reported back (`PutResult.DocID`, `docId`, the `kdb_id` column); the body is stored untouched |
| a UUID string | that UUID |
| any other non-empty string `s` | `uuid8(sha256(KDB_DOC_ID_NAMESPACE || utf8(s)))`, deterministic and identical in Go and Kotlin, so writing `{"id":"order-1",...}` twice updates one document |
| non-string or `""` | rejected |

Writing over an existing document **merges** (a shallow, root-level merge) on the wire `UPSERT` and
SQL's `SET _doc`; it does not replace. A key the new body omits keeps its stored value.

**Commit.** An immutable, content-addressed (SHA-256) record: parent hashes, the namespace, a
transaction id, a timestamp, the authoring node, the list of **operations** (write, delete,
file-write, schema migration), and the hash of the resulting **document tree**. The tree is a
Merkle trie of `docId -> content hash`, so two replicas can compare state in `O(difference)`.
Every namespace starts from the same empty `genesis` commit (`a4fbe365e8a3...`). Operations carry no
pre-image, so **history cannot be run backwards**: `revert` writes a new commit whose tree is an
older one's, it never rewinds.

**Branch, tag, head.** A branch is a named pointer to a commit (`main` by default); `head` is the
current branch's tip. Revision strings accepted by the CLI, `client.History` and `AT COMMIT`:
`head`, `head~N`, `head^`, a full or abbreviated commit hash, `<hash>~N`, `tag:NAME`,
`branch:NAME`.

**Schema (a lens, not a constraint).** A namespace works with no schema at all. `CREATE TABLE`
declares typed columns over top-level fields, which makes them selectable as columns and lets `NOT
NULL` and `UNIQUE` be enforced at commit. The document is always the truth and `_doc` always
returns all of it.

**Transaction.** A set of document operations committed together, anchored on a `BaseVersion`
(the commit you read at). Conflict detection is *content based*: it compares the documents the
transaction touches between the base and the current head, not the head hashes, so unrelated
writers do not conflict. Cross-namespace transactions are atomic across namespaces of one data root
([§11.1](#111-transactions)).

**History retention.** `history=full` (default) keeps every commit. `history=none` keeps the live
dataset plus a window (`retain.duration` / `retain.commits`) and reclaims the rest. These are
engine settings, set by environment variable ([§14.4](#144-engine-settings-environment-only)).

**Durability.** A commit is acknowledged after the delta-log append and fsync (`--durability=sync`,
the default; concurrent commits share one fsync via group commit). `kill -9` is safe: the same
replay path runs on the next start.

**On disk.**

```
<dataRoot>/
  .kdb.lock  .kdb.write.lock     read/attach lock (shared) and writer lock (exclusive)
  NODE                           this node's identity (UUID); delete it when cloning a directory
  costmodel.json                 learned scan-cost priors (a cache, safe to delete)
  txn/  replication/  snap/      cross-namespace decisions, peer progress, checkpoints
  ns/<catalog>/<name>/
     meta.json  meta/
     delta/00000000000000000000.seg ...   the commit log: sequence order is commit order
     wal/  sstable/L0/                    blob write-ahead log and flushed blob generations
```

The **delta log alone** rebuilds a namespace, which is why verify, backup and restore all operate
on it.

---

## 7. Embedding in Go

Three levels, from highest to lowest. Pick the highest that does what you need.

| Level | Package | Use it when |
|---|---|---|
| SQL | `kdb/driver` (`database/sql`) | you want tables, rows and transactions; no server |
| Runtime | `kdb/embed` | you want documents, commits, branches and history directly |
| Server in your process | `kdb/server` + `kdb/client` | you want the wire protocol, auth and sessions inside your process, or to plug in a custom transport |

One writer per data directory (the writer lock is exclusive). Several read-only runtimes can attach
alongside a live writer on unix (`embed.OpenReadOnlyFileRuntime`, then `rt.Refresh()` to advance the
view). To serve several namespaces from one process, open an `embed.Host` once
(`embed.OpenFileHost(dataRoot, opts)`) and call `host.Namespace(catalog, id, schema)`; it shares the
directory lock, the I/O shim and the memory budget. `OpenFileRuntime` is that, with one namespace.

### 7.1 The runtime: documents, history, branches

````go
// embed1/main.go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

func main() {
	dir, err := os.MkdirTemp("", "kdb-embed-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	const ns = "myapp/users"
	catalog := embed.CatalogFromNamespace(ns) // "myapp"

	rt, err := embed.OpenFileRuntime(dir, catalog, ns, schema.None())
	if err != nil {
		log.Fatal(err)
	}

	// Each PutJSONDocument stores one document and appends one commit on the current branch.
	// A top-level "id" in the body becomes the document id; otherwise a random UUID is minted.
	ada, err := embed.PutJSONDocument(rt, ns, `{"name":"Ada","age":36}`)
	if err != nil {
		log.Fatal(err)
	}
	grace, err := embed.PutJSONDocument(rt, ns, `{"name":"Grace","age":45}`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("ada   :", ada.DocID, "commit", ada.Commit.Hex()[:12])
	fmt.Println("grace :", grace.DocID, "commit", grace.Commit.Hex()[:12])

	readAt := func(rt *embed.EmbeddedKdbRuntime, id codec.UUID, commit codec.Hash) string {
		c, err := rt.DAG.GetCommitOrThrow(commit)
		if err != nil {
			log.Fatal(err)
		}
		d, err := rt.Storage.GetDocument(ns, id, c.DocumentTreeHash)
		if err != nil {
			log.Fatal(err)
		}
		if d == nil {
			return "<absent>"
		}
		return d.JSON
	}
	head, _ := rt.DAG.Head()
	fmt.Println("at head       :", readAt(rt, ada.DocID, head))
	fmt.Println("at ada commit :", readAt(rt, grace.DocID, ada.Commit)) // grace did not exist yet

	for _, e := range rt.DAG.Walk(head, nil, 10) {
		if full, ok := e.(dag.FullEntry); ok {
			fmt.Printf("  commit %s ops=%d parents=%d\n", full.Commit.Hash.Hex()[:12], len(full.Commit.Operations), len(full.Commit.ParentHashes))
		}
	}

	if _, err := rt.DAG.CreateBranch("experiment", ada.Commit); err != nil {
		log.Fatal(err)
	}
	for _, b := range rt.DAG.ListBranches() {
		fmt.Println("branch", b.Name, b.HeadHash.Hex()[:12])
	}

	rt.Close() // flushes and seals; also what releases the directory lock

	rt2, err := embed.OpenFileRuntime(dir, catalog, ns, schema.None())
	if err != nil {
		log.Fatal(err)
	}
	defer rt2.Close()
	head2, _ := rt2.DAG.Head()
	fmt.Println("after reopen, head matches:", head2 == head, "doc:", readAt(rt2, grace.DocID, head2))

}
````

Output (excerpt): the history walk lists three commits (two writes and `genesis`, `ops=0`), the
read "at ada commit" of Grace's id prints `<absent>`, and after reopen `head matches: true`.

Things to notice: `EmbeddedKdbRuntime` exposes `DAG` (a `dag.CommitDAG`) and `Storage` (a
`storage.Adapter`) directly; `embed.PutJSONDocument` is the convenience that stores a document and
appends the commit; `DAG.Walk` yields `dag.FullEntry` values (or stubs for archived commits); and
`Close()` seals the active segment and releases the lock. Use `embed.RevertTo`,
`embed.DiffCommits`, `embed.DocumentAt` and `embed.DocumentVersions` for the higher-level history
operations the CLI is built on. For multi-operation, conflict-checked writes embed a
`transaction.Engine` (see `server.NewKdbServerRuntime`, which wires one with unique-key and
precondition support), or simply run a [server in your process](#73-a-server-in-your-process).

### 7.2 SQL and `database/sql`

See [§9](#9-the-databasesql-driver); this is the usual choice for application data.

### 7.3 A server in your process

This program is complete: it opens a runtime, wraps it in a server, installs an RBAC registry,
listens on a free port, and exercises the client SDK against it (documents, compare-and-set,
conflicts, leases, SQL, a transaction, history and revert, and RBAC denial).

````go
// inproc/main.go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/storage/mem"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	const ns = "demo/users"

	// --- server side: a runtime, a server around it, RBAC, one TCP listener -------------------
	rt, err := embed.OpenMemoryRuntime("demo", ns, schema.None())
	if err != nil {
		log.Fatal(err)
	}
	srv := server.NewKdbServerRuntime(rt)

	usersDag, _ := dag.NewInMemoryCommitDag(auth.UsersNamespace)
	rolesDag, _ := dag.NewInMemoryCommitDag(auth.RolesNamespace)
	store, err := auth.NewRegistryAuthStore(usersDag, rolesDag, mem.NewInMemoryStorageAdapter())
	if err != nil {
		log.Fatal(err)
	}
	must(store.CreateRole("app-writer", []string{"read:demo/*", "write:demo/*"}))
	must(store.CreateRole("app-reader", []string{"read:demo/*"}))
	must(store.CreateUser("alice", "s3cret", []string{"app-writer"}))
	must(store.CreateUser("bob", "hunter2", []string{"app-reader"}))
	srv.AuthEngine = auth.NewRegistryAuthEngine(store)

	// A commit hook: called synchronously after every durable commit (keep it fast). This is the
	// same seam the stream hub uses to fan commits out to subscribers.
	var commits atomic.Int32
	srv.CommitListener = func(namespaceID string, c document.Commit) { commits.Add(1) }

	// Port 0 picks a free port; "?bind=true" is what makes the URI a listen address.
	ln, err := server.ListenSqlWire("tcp://127.0.0.1:0?bind=true", srv)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
	addr := ln.Addr().String()
	fmt.Println("listening on", addr)

	// --- client side --------------------------------------------------------------------------
	_, err = client.Connect(ctx, addr, "alice:wrong")
	fmt.Println("bad password        ->", errors.Is(err, client.ErrUnauthenticated))
	c, err := client.Connect(ctx, addr, "alice:s3cret")
	must(err)
	defer c.Close()

	id, _ := codec.RandomUUID()
	docID := id.String()

	c1, err := c.PutJSON(ctx, ns, docID, []byte(`{"name":"Ada","visits":1}`))
	must(err)
	body, at, err := c.GetJSON(ctx, ns, docID)
	must(err)
	fmt.Println("read                ->", string(body), "at", at[:12], "(put returned", c1[:12]+")")

	// Compare-and-set: the SDK re-reads on every attempt and retries only lost races.
	_, err = c.CompareAndSwap(ctx, ns, docID, 5, func(cur []byte) ([]byte, error) {
		return []byte(`{"name":"Ada","visits":2}`), nil
	})
	must(err)

	// Optimistic concurrency with an explicit, now-stale base version -> ErrConflict.
	_, err = c.Commit(ctx, client.Transaction{
		Namespace:   ns,
		BaseVersion: c1,
		Writes:      []client.DocWrite{{DocID: docID, JSON: []byte(`{"name":"Ada","visits":99}`)}},
	})
	var ce *client.ConflictError
	if errors.Is(err, client.ErrConflict) && errors.As(err, &ce) {
		fmt.Println("stale commit        -> conflict:", errors.Is(err, client.ErrConflict))
	}

	// Document lease: a second session cannot write while it is held.
	lease, err := c.AcquireLock(ctx, ns, docID, 30*time.Second)
	must(err)
	fmt.Println("lease fence         ->", lease.Fence)
	must(c.ReleaseLock(ctx, ns, docID))

	// SQL over the same namespace.
	must(c.Exec(ctx, ns, "CREATE TABLE users (name VARCHAR NOT NULL, visits INT)", nil))
	must(c.Exec(ctx, ns, "INSERT INTO users (name, visits) VALUES (?, ?)", []any{"Grace", 5}))
	var rows []struct {
		KdbID  string `json:"kdb_id"`
		Name   string `json:"name"`
		Visits int    `json:"visits"`
	}
	must(c.Query(ctx, ns, "SELECT kdb_id, name, visits FROM users ORDER BY visits DESC", nil, &rows))
	for _, r := range rows {
		fmt.Println("row                 ->", r.Name, r.Visits)
	}

	// History, newest first, then an undo that itself is a new commit.
	h, err := c.History(ctx, client.HistoryRequest{Namespace: ns, Limit: 3})
	must(err)
	for _, cm := range h.Commits {
		fmt.Printf("commit %s ops=%d\n", cm.Hex[:12], cm.OperationCount)
	}
	rev, err := c.Revert(ctx, ns, "head~1")
	must(err)
	fmt.Println("revert              -> restored", rev.Restored, "removed", rev.Removed)

	// RBAC: bob may read but not write.
	bob, err := client.Connect(ctx, addr, "bob:hunter2")
	must(err)
	defer bob.Close()
	if _, _, err := bob.GetJSON(ctx, ns, docID); err == nil {
		fmt.Println("bob read            -> ok")
	}
	_, err = bob.Upsert(ctx, ns, docID, []byte(`{"name":"bob was here"}`))
	fmt.Println("bob write           ->", err)
	fmt.Println("commit hook calls   ->", commits.Load())
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
````

Output (ids and hashes vary): `bad password -> true`, the read-back, `stale commit -> conflict: true`,
`lease fence -> 1`, both SQL rows, three commits, `revert -> restored 0 removed 1`, `bob read -> ok`,
`bob write -> kdb: authorization denied: principal bob lacks write on demo/users/<id>`, and
`commit hook calls -> 3`.

This is a *single-namespace* server. `kdb-service` additionally wires a `server.NamespaceSet`
(multi-namespace, cross-namespace transactions, idle close), an index registry via
`srv.OpenIndexes(...)`, the maintenance loop, resource governance
(`srv.SetMemoryBudget`), peer replication and the listeners. Read `go/kdb/service/service.go`
(`Main`) as the reference for a production wiring; it is the same code the binary runs.

---

## 8. The Go client SDK

`go/kdb/client`. One `*client.Client` is one connection and is safe for concurrent use (every
request has its own correlation id). Per-namespace sessions are opened lazily.

```go
c, err := client.Connect(ctx, "tcp://127.0.0.1:9090", "alice:s3cret") // token "" when RBAC is off
// schemes: tcp:// tcps:// ws:// wss://, or a bare host:port (= tcp://)
c, err = client.ConnectWithOptions(ctx, "tcps://db:9090", token, client.ConnectOptions{
    TLS: &core.TransportTlsSettings{Enabled: true, CAFile: "/etc/kdb/ca.pem"}, // transport/core
})
```

WebSocket was verified: `client.Connect(ctx, "ws://127.0.0.1:19094/kdb", "")` against
`--ws-addr "ws://127.0.0.1:19094/kdb?bind=true"`.

| Call | Purpose |
|---|---|
| `PutJSON(ctx, ns, id, body)` | create/write one document; returns the commit hash |
| `GetJSON(ctx, ns, id)` | `(body, commit, err)`; `ErrNotFound` if absent |
| `GetJSONWithHash` | also returns the **content hash** to use as a CAS precondition |
| `Upsert` | unconditional write, never conflicts, shallow-merges into an existing document |
| `Commit(ctx, client.Transaction{Namespace, BaseVersion, Writes, Deletes})` | optimistic multi-document commit |
| `PutIfAbsent` / `ReplaceIf(hash)` / `ReplaceIfPresent` | conditional writes (`ErrPreconditionFailed`) |
| `CompareAndSwap(ctx, ns, id, maxAttempts, func(cur []byte) ([]byte, error))` | read-modify-write loop; re-reads each attempt; `nil` return aborts with `ErrAborted` |
| `AcquireLock` / `RenewLock` / `ReleaseLock` | document leases with a fencing token (default TTL 30 s, max 5 min) |
| `Query(ctx, ns, sql, args, &dest)` / `QueryRaw` / `Exec` | SQL; `Query` decodes rows into a slice of structs by `json` tag |
| `Begin` / `BeginTx(.., TxOptions{Snapshot: true})` | SQL transaction (`Exec`, `QueryRaw`, `Commit`, `Rollback`) |
| `CommitAcross(ctx, []Transaction)` | atomic commit across namespaces |
| `History(ctx, HistoryRequest)` / `Revert(ctx, ns, rev)` | list commits; undo by writing a new commit |
| `Search(ctx, SearchRequest)` | full-text / vector / fused search through the SEARCH frame |
| `NewSession()` | read-your-writes across replicas (`Session.GetJSON/PutJSON/...`) |
| `NewRouter(...)` | follow `NOT_HOME` redirects for single-home namespaces |

A cross-namespace example against a real `kdb-service` (address from `$KDB_ADDR`, default
`127.0.0.1:9090`; token from `$KDB_TOKEN`):

````go
// multi/main.go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	addr := os.Getenv("KDB_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9090"
	}
	c, err := client.Connect(ctx, addr, os.Getenv("KDB_TOKEN")) // "" when RBAC is off
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// One atomic commit across two namespaces: both get a commit carrying the same group id, or
	// neither does. A namespace that does not exist yet is created by a write to it.
	res, err := c.CommitAcross(ctx, []client.Transaction{
		{Namespace: "bank/accounts", Writes: []client.DocWrite{
			{DocID: "aaaaaaaa-0000-0000-0000-000000000001", JSON: []byte(`{"owner":"ada","balance":70}`)}}},
		{Namespace: "bank/ledger", Writes: []client.DocWrite{
			{DocID: "bbbbbbbb-0000-0000-0000-000000000001", JSON: []byte(`{"owner":"ada","debit":30}`)}}},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("group", res.GroupID[:8], "commits:", len(res.Commits))

	// The same atomicity over SQL: the tx is opened on one namespace; a table qualified as
	// <catalog>.<name> (bank.ledger == namespace bank/ledger) routes to the other.
	tx, err := c.Begin(ctx, "bank/accounts")
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback(ctx) // harmless after Commit
	if _, err := tx.Exec(ctx, "INSERT INTO bank.ledger (owner, debit) VALUES (?, ?)", "ada", 5); err != nil {
		log.Fatal(err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		log.Fatal(err)
	}

	// A refused participant refuses everyone, and names itself. Here "bank/accounts" is written
	// against a base version that another writer has since moved past.
	base := res.Commits["bank/accounts"]
	if _, err := c.Upsert(ctx, "bank/accounts", "aaaaaaaa-0000-0000-0000-000000000001", []byte(`{"balance":65}`)); err != nil {
		log.Fatal(err)
	}
	_, err = c.CommitAcross(ctx, []client.Transaction{
		{Namespace: "bank/accounts", BaseVersion: base,
			Writes: []client.DocWrite{{DocID: "aaaaaaaa-0000-0000-0000-000000000001", JSON: []byte(`{"balance":0}`)}}},
		{Namespace: "bank/ledger", Writes: []client.DocWrite{{DocID: "bbbbbbbb-0000-0000-0000-000000000002", JSON: []byte(`{"debit":1}`)}}},
	})
	var cne *client.CrossNamespaceError
	if errors.As(err, &cne) {
		fmt.Println("refused by", cne.Namespace, "| conflict =", errors.Is(err, client.ErrConflict))
	} else {
		fmt.Println("unexpected:", err)
	}
}
````

Output:

```
group 3fc60eb4 commits: 2
refused by bank/accounts | conflict = true
```

Each participant is checked (schema, unique keys, preconditions, conflicts) before any is written;
one refusal refuses the whole transaction. All participants must share one data root, where the
transaction's decision is recorded.

Retries: use `errors.Is` against the sentinels, never message text ([§16](#16-errors)).
`BusyError.RetryAfter()` is the server's suggested delay; `CompareAndSwap` already backs off with
jitter and prefers the server's hint.

---

## 9. The `database/sql` driver

```go
import _ "github.com/limidus/kdb/go/kdb/driver"   // registers "kdb"
db, err := sql.Open("kdb", "kdb://file:///var/lib/myapp/shop/orders")
```

**DSN forms** (all of these were opened and used in a test program; the `kdb://` prefix is
optional for `sql.Open`):

| DSN | Meaning |
|---|---|
| `kdb://memory:///catalog/namespace` | shared in-process database; every connection to the same group shares it |
| `kdb://memory:///c/ns?unique=true` | a fresh isolated database per `Open` (tests) |
| `kdb://memory:///c/ns?isolate=NAME` | a named shared instance |
| `kdb://memory:///c/ns?dropOnClose=true` | dropped when the last connection closes |
| `kdb://file:///abs/dataRoot/catalog/ns` | durable; the **last two path segments** are `catalog/namespace`, everything before is the data root (so `.../shop/orders` stores under `<dataRoot>/ns/shop/orders`) |
| `...?readOnly=true` (or `read_only=true`) | refuses writes |
| `memory:///c/ns;unique=true` | semicolon-separated parameters are also accepted for memory URLs |

A memory DSN with fewer segments defaults the catalog to `default` and the namespace to
`default/main`. The driver is **embedded only**: a network DSN is not supported (use the client SDK).

````go
// sqldrv/main.go
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/limidus/kdb/go/kdb/driver" // registers the "kdb" driver
)

func main() {
	dir, err := os.MkdirTemp("", "kdb-sql-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	ctx := context.Background()

	// DSN forms: see section 9. The data root is everything before the last two path segments.
	db, err := sql.Open("kdb", "kdb://file://"+dir+"/shop/orders")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, `CREATE TABLE orders (customer VARCHAR NOT NULL, total INT, status VARCHAR)`); err != nil {
		log.Fatal(err)
	}
	for _, o := range []struct {
		c      string
		total  int
		status string
	}{{"ada", 30, "open"}, {"grace", 70, "open"}, {"ada", 15, "paid"}} {
		if _, err := db.ExecContext(ctx, `INSERT INTO orders (customer, total, status) VALUES (?, ?, ?)`, o.c, o.total, o.status); err != nil {
			log.Fatal(err)
		}
	}

	rows, err := db.QueryContext(ctx, `SELECT customer, total FROM orders WHERE total >= ? ORDER BY total DESC`, 20)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var customer string
		var total int64
		if err := rows.Scan(&customer, &total); err != nil {
			log.Fatal(err)
		}
		fmt.Println("order:", customer, total)
	}
	rows.Close()

	// A snapshot transaction: a concurrent change is a conflict, never a lost update.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		log.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE orders SET status = 'shipped' WHERE customer = ? AND status = 'paid'`, "ada"); err != nil {
		log.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		if errors.Is(err, driver.ErrConflict) {
			log.Fatal("conflict: re-run the transaction")
		}
		log.Fatal(err)
	}

	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM orders WHERE status = 'shipped'`).Scan(&n); err != nil {
		log.Fatal(err)
	}
	fmt.Println("shipped:", n)

	var id, doc string
	if err := db.QueryRowContext(ctx, `SELECT kdb_id, _doc FROM orders WHERE customer = 'grace'`).Scan(&id, &doc); err != nil {
		log.Fatal(err)
	}
	fmt.Println("grace:", len(id), doc)
}
````

Output:

```
order: grace 70
order: ada 30
shipped: 1
grace: 36 {"customer":"grace","total":70,"status":"open"}
```

Semantics worth knowing (from `go/kdb/driver` and its tests): outside a transaction every write
commits immediately; `db.BeginTx` or `BEGIN`/`COMMIT`/`ROLLBACK` on one `sql.Conn` buffer writes;
`LevelSnapshot` / `LevelRepeatableRead` read every namespace as of `Begin` and turn any concurrent
change into `driver.ErrConflict` (match with `errors.Is`); `LevelSerializable` is refused; a table
qualified as `catalog.name` writes that namespace, and a transaction writing several namespaces
commits them atomically; values scan as `int64`, `float64`, `bool`, `string` and `NULL`.

---

## 10. Schema, indexes and SQL

### 10.1 The SQL surface (what the Go parser accepts)

`go/kdb/sql/parser.go` is the source of truth. These statements were run against a v0.6.2 server:

```sql
CREATE TABLE people (name VARCHAR NOT NULL, age INT, city VARCHAR, email VARCHAR UNIQUE)
CREATE TABLE t (a VARCHAR, b VARCHAR, UNIQUE (a, b))              -- composite uniqueness is parsed [not run]
INSERT INTO people (name, age, city, email) VALUES ('Ada', 36, 'London', 'ada@x.io'), ('Grace', 45, 'NYC', 'g@x.io')
UPDATE people SET age = 37 WHERE name = 'Ada'
DELETE FROM people WHERE name = 'Grace'
SELECT kdb_id, name, age FROM people WHERE age > ? ORDER BY age DESC LIMIT 5
SELECT city, COUNT(*) AS n, SUM(age) AS total FROM people GROUP BY city
SELECT name FROM people WHERE name LIKE 'A%'
SELECT name FROM people WHERE age IN (36, 45)
SELECT name FROM items WHERE price BETWEEN 10 AND 20
SELECT DISTINCT name FROM items
SELECT COUNT(*) AS n, MAX(price) AS hi FROM items
CREATE INDEX idx_age ON people (age) USING BTREE
CREATE INDEX idx_body ON docs (body) USING FULLTEXT
CREATE INDEX idx_emb ON items (emb) USING VECTOR WITH (dimensions = 3, metric = 'cosine')
DROP INDEX idx_age ON people                                      -- [not run]
SELECT name FROM docs WHERE MATCH(idx_body, 'source control')
SELECT name FROM items ORDER BY SIMILARITY(emb, [0,0,1]) LIMIT 2
-- FUSE(MATCH(..), SIMILARITY(..), 'rrf'|'weighted') combines both arms [parsed; not run]
BEGIN / COMMIT / ROLLBACK                                         -- also START TRANSACTION, END, ABORT
```

* Pseudo-columns: `kdb_id` (the UUID) and `_doc` (the whole JSON). `SELECT *` expands to `kdb_id`,
  the schema fields, then `_doc`.
* Parameters are positional `?`.
* Column types: `VARCHAR/TEXT/STRING/CHAR`, `INT/INTEGER`, `BIGINT/LONG`, `DOUBLE/FLOAT/REAL`,
  `BOOLEAN/BOOL`, `TIMESTAMP/DATETIME`, `UUID`. `NOT NULL` -> required, `UNIQUE` -> unique.
* Index kinds: `HASH`, `BTREE`, `FULLTEXT`, `VECTOR`. A server must have an index provider
  installed for `CREATE INDEX` and `MATCH`/`SIMILARITY` to work (`kdb-service` does; the minimal
  server in §7.3 does not, see [§15.1](#151-index-stores)).
* A trailing `;` is consumed by the parser. `INSERT` always mints a new id: write at a chosen id
  with `PutJSON`/`Upsert` (or put `"id"` in an inserted `_doc`).
* A violated `UNIQUE` fails with `kdb: schema violation: op 0: email: UNIQUE_CONSTRAINT (value
  already held by document <id>)`.
* `CREATE TABLE` is once per namespace; evolve with `schema.MigrationBuilder` and a schema
  migration op.
* Not accepted by the wire SQL path in the version tested: `EXPLAIN`, and `AT VERSION/COMMIT/TIME`
  (both failed with a parse error, `unexpected trailing input`). Historical reads go through
  `client.History`, `embed.DocumentAt`, `kdb get --at REV`, or the hybrid engine in `go/kdb/query`.

Other points that bite: `NULL = NULL` is true in the Go comparator (use `IS NULL`); comparing a
string column with a number compares as equal rather than coercing; only top-level schema fields
are columns (reach nested data through `_doc`); and a full scan is bounded by `--scan-row-budget`,
so always add `LIMIT`.

### 10.2 Indexes: the extension point, in action

[§15.1](#151-index-stores) is a complete program that installs an index registry on a server, shows
which stores the engine asks for, and queries by `MATCH` and through the `SEARCH` frame.

---

## 11. Transactions, branches, history and peer sync: how-tos

### 11.1 Transactions

| You need | Use |
|---|---|
| write one document, last writer wins | `Upsert` |
| create exactly once | `PutIfAbsent` |
| read-modify-write one document | `CompareAndSwap` (or `GetJSONWithHash` + `ReplaceIf`) |
| several documents, one namespace, detect concurrent change | `Commit` with `BaseVersion` |
| several statements in SQL | `client.Begin` / `driver` transaction |
| several namespaces, atomically | `CommitAcross`, or a SQL tx naming `catalog.name` tables |
| hold a record across human think-time | `AcquireLock` (lease with fence) |
| "this key belongs to one document" | `UNIQUE` column, enforced on every write path |

Cross-namespace transactions require the participants to share a data root (`kdb-service
--data-dir`). A group commit that replicates to a node lacking one of its namespaces is reported as
incomplete there. Design: [cross-namespace transactions](kdb-cross-namespace-transactions-plan.md).

### 11.2 Branches, tags, history

```bash
kdb --data-dir D branch create myapp/users feature          # branch at head   (ran)
kdb --data-dir D branch checkout myapp/users feature
kdb --data-dir D tag create myapp/users v1 [rev] [message]  # (ran: "tagged <hash> as v1")
kdb --data-dir D log myapp/users --limit 20 --skip 0 --oneline
kdb --data-dir D show myapp/users head~1
kdb --data-dir D diff myapp/users head~1 head                # "~ <docId>" for a changed document
kdb --data-dir D revert myapp/users head~1                   # new commit restoring that state
```

Over the wire: `client.History(ctx, HistoryRequest{Namespace, From, Skip, Limit})` and
`client.Revert`. In Go: `rt.DAG.CreateBranch`, `SetHead`, `ListBranches`, `Walk`, and the
`dag.HistoryNavigator` interface (tags and revision resolution).

### 11.3 Peer sync

Every node is a full replica of the namespaces it holds. Two nodes reconcile by fast-forward,
auto-merge (different documents changed) or a structured conflict (same document changed and no
rule decides). Verified with two local servers; node A is the hub, node B pulls and pushes:

```bash
./go/bin/kdb-service --data-dir /tmp/a --namespace myapp/users \
    --sql-addr "tcp://127.0.0.1:19090?bind=true" --peer-addr "tcp://127.0.0.1:19091?bind=true" --stream-addr ""
./go/bin/kdb-service --data-dir /tmp/b --namespace myapp/users \
    --sql-addr "tcp://127.0.0.1:29090?bind=true" --peer-addr "tcp://127.0.0.1:29091?bind=true" --stream-addr "" \
    --peer "name=a,addr=tcp://127.0.0.1:19091,namespaces=myapp/*,interval=2s,create=true"
# PutJSON on A; a GetJSON on B succeeds about 2 polls (~2-4 s) later.
```

`--peer` (repeatable, or `KDB_PEERS` `;`-separated) has the fields `name`, `addr`, `namespaces`
(patterns joined by `|`; `*` one segment, `**` any depth, `!` excludes), `mode`
(`both|pull|push`), `interval`, `user`, `password-env`, `create`, `bootstrap=snapshot`,
`writeback`, `filter=` (last, may contain commas) and `readthrough`. Other relevant flags:
`--peer-conflict-policy strict|last-write`, `--peer-create-namespaces`, `--peer-retention-grace`,
`--peer-http host:port` (peer sync over WebSocket at `/kdb/sync`, plain HTTP, terminate TLS in
front). One-shot CLI equivalents exist: `kdb sync <ns> <peer-addr> [--pull|--push]`,
`kdb conflicts <ns>`, `kdb resolve <ns> <id> --take local|remote`, `kdb scrub`, `kdb peer-diff`,
`kdb deepen` **[not run]**.

An application can become a sync node itself with `go/kdb/syncnode` (`syncnode.Open(host, set,
primary, syncnode.Config{Peers: ...})`; `node.Handler()` mounts peer sync on your HTTP router) and
phones use `go/mobile/kdbsync`. Everything else about topology, filtered peers, resolution chains,
the control plane's conflict endpoints and recovery is in the
[user guide](kdb-user-guide.md#peer-sync-and-replication). Go and Kotlin interoperate over sync
protocol v1 only; v2 (messages `0x26`-`0x3F`) is Go-only.

---

## 12. Wire protocol for client authors

Sources: `go/kdb/wire` (authoritative), `docs/kdb-lld-protocol.md`, and
`packages/kdb-client/src/wire` (an independent TypeScript implementation proven against fixtures
generated by the Go encoder). Everything below was checked by the program in §12.7, which speaks
the protocol with only `net` and `encoding/json`.

### 12.1 Framing

```
 0       4      6       8          12
 +-------+------+-------+----------+-----------------------------+
 | len   | type | proto | corr id  | payload (len - 12 bytes)    |
 +-------+------+-------+----------+-----------------------------+
  int32LE uint16LE uint16LE int32LE
```

* `len` is the **total** frame length **including** the 12 header bytes. Valid range
  `12 .. 16 MiB` (`wire.DefaultMaxFrameBytes`); anything else is a decode error and the connection
  cannot be resynchronised.
* `type` is the message type ([§12.4](#124-message-catalogue)); an unknown type is a decode error.
* `proto` is the protocol version, currently `1`. A peer outside `[1, 1]` is rejected at handshake.
* `corr id` is chosen by the client and **echoed in the reply**. Many requests may be in flight on
  one connection; the server serves frames concurrently (bounded to 64 in flight per connection;
  past that it stops reading and TCP backpressure paces the client). Frames naming the *same
  session* are processed in order.
* **Payload:** byte 0 is the encoding tag (`1` = JSON, `0` = `KDB_BINARY`), followed by the
  envelope. In v0.6.2 the Go codec always emits and accepts a **JSON envelope** (tags `0` and `1`
  both carry JSON). Send tag `1`.
* All integers are little-endian.

### 12.2 Transports

| Scheme | Carrying the frames |
|---|---|
| `tcp://host:port` | the raw byte stream: frames back to back, no extra framing |
| `tcps://` | the same inside TLS (mTLS with `--tls-client-auth`) |
| `ws://host:port/kdb`, `wss://` | RFC 6455; **one binary message (opcode 0x2) per frame**, unfragmented; pings are answered |
| `grpc://`, `grpcs://` | `go/grpc`: service `kdb.wire.v1.KdbWire`, `rpc Connect(stream Frame) returns (stream Frame)` where `Frame.payload` is one complete wire frame. One stream = one connection; sessions and locks die with the stream |

### 12.3 Handshake, auth, sessions

1. **`HANDSHAKE` (0x01).** Envelope `{"kind":"handshake","handshake":{...}}`:

   ```json
   {"nodeId":"my-client","namespaces":[],"localHeads":{},"supportsZstd":false,
    "supportsIndexHints":false,"supportsDirectDeltaIngest":false,"maxFrameBytes":16777216,
    "preferredEncodings":["JSON"],"clientMode":"SQL_CLIENT","protocolVersion":1,
    "token":"alice:s3cret"}
   ```

   Credentials are optional fields `user` + `password`, or `token` (`"user:secret"`). A WebSocket
   server can also read HTTP headers. `clientMode` is `SQL_CLIENT`, `STREAM_READ_ONLY`,
   `STREAM_WRITE_BACK` or `FULL_PEER` (the last three speak to the stream and peer listeners).
   `namespaces` may name the namespace to authorize against. The reply `handshakeAck` is
   `{"accepted":true,"negotiatedEncoding":"JSON","protocolVersion":1,"remoteHeads":{...}}`; on
   rejection `accepted` is false and `rejectionReason` says why.
2. **`SESSION_BEGIN` (0x0E)** per namespace: `{"namespace":"ns","sessionId":null,
   "readConsistency":"READ_COMMITTED"|"SNAPSHOT","baseVersionHex":null}`. The ack (0x13) returns
   `sessionId`, `headHex` and, if refused, an `error` and an empty `sessionId`. `SNAPSHOT` pins
   reads to the head at session start; `baseVersionHex` pins them to a commit. Authorization is
   re-checked here with the `read` grant.
3. Then any of the operations below. Dropping the connection ends every session and releases every
   lease the connection held.

### 12.4 Message catalogue

All names and codes are `go/kdb/wire/types.go`. Envelope `kind` values are the camel-case message
name (`sqlExec`, `sqlResult`, `documentGet`, `upsert`, ...).

| Code | Name | Direction | Purpose |
|---|---|---|---|
| 0x01 | `HANDSHAKE` | both | negotiate version/encoding, authenticate (reply kind `handshakeAck`) |
| 0x0E / 0x13 | `SESSION_BEGIN` / `_ACK` | c->s / s->c | open a session on a namespace |
| 0x0F / 0x10 | `SQL_EXEC` / `SQL_RESULT` | c->s / s->c | run SQL; rows or a typed error |
| 0x11 / 0x12 | `TX_COMMIT` / `TX_ROLLBACK` | c->s | flush or discard writes buffered on the session |
| 0x14 / 0x15 | `DOCUMENT_GET` / `_RESULT` | c->s / s->c | point read by id (sessionless) |
| 0x16 / 0x17 | `UPSERT` / `_RESULT` | c->s / s->c | write at a chosen id |
| 0x19-0x1C | `LOCK_ACQUIRE/RENEW/RELEASE`, `LOCK_RESULT` | | leases with a fence |
| 0x1D / 0x1E | `SEARCH` / `_RESULT` | | full-text / vector / fused search |
| 0x1F-0x22 | `HISTORY_LIST/_RESULT`, `REVERT/_RESULT` | | list commits; undo |
| 0x23 / 0x24 | `TX_COMMIT_MULTI` / `_RESULT` | | atomic multi-namespace commit |
| 0x02 | `DELTA_COMMIT` | s->c | a commit pushed to a stream subscriber |
| 0x0C | `POSITION_ACK` | c->s | stream subscriber's last applied commit |
| 0x06 / 0x07 | `TRANSACTION_REPLAY`, `CONFLICT_REPORT` | | stream mode 2 write-back; structured conflict |
| 0x03-0x05, 0x08-0x0B, 0x0D, 0x18 | `COMMIT_FETCH/PUSH/_ACK`, `DAG_DIFF`, `SNAPSHOT_*`, `SCHEMA_PUSH`, ... | peer <-> peer | peer sync v1 |
| 0x25-0x3F | `PEER_ERROR`, `SYNC_HELLO`, `REFS_*`, `FETCH_REQUEST`, `PACK_PAGE`, `REF_UPDATE*`, `SNAPSHOT_FETCH/PAGE`, `PROJECT_*`, `TREE_NODES*`, `OBJECT_FETCH*`, `DOC_FETCH*`, `GRAFT_PUSH*`, `META_VIEW*`, `HOME_REQUEST*` | peer <-> peer | peer sync v2 (Go-only) |

A **minimal client** needs only: handshake, session begin, `SQL_EXEC`/`SQL_RESULT`,
`DOCUMENT_GET`, `UPSERT`. Everything else is additive. Codes `0x14-0x3F` exist only in the Go
server; the JVM server implements the older subset, and a new client must not send a type an old
server does not know (an unknown type is a decode error). The TypeScript client handles that split
with `KdbUnsupportedError`.

Payload shapes (the DTOs in `go/kdb/wire/payload_dto.go`; all fields below were exercised):

```
sqlExec      {"namespace","sessionId","sql","parametersJson":null | "[30,\"x\"]"}
sqlResult    {"namespace","sessionId","columns":[..],"rows":[[strings..]],"rowsAffected","resolvedCommitHex",
              "readOnly","error":null|string,"generatedIds":[..],"errorCode"?,"retryAfterMs"?}
documentGet  {"namespace","docId","minCommit"?}       -> {"namespace","docId","json","commitHex","error"?,"errorCode"?}
upsert       {"namespace","docId","json":"<JSON text>","sessionId"?}  -> {"namespace","commitHex","error"?,"errorCode"?}
txCommit     {"namespace","sessionId","transactionBytes":[ints]}      -> conflictReport on conflict, else a result
```

Rows are arrays of **strings**. Byte arrays in JSON travel as **arrays of integers** (not base64;
this matches Kotlin's `ByteArray` serialisation and is a classic interop trap). Hashes are 64-char
hex; document ids are canonical UUID strings. `TX_COMMIT` carries a transaction in the KDB binary
codec (`go/kdb/wire/transaction_codec.go`; the TS client has an independent implementation in
`src/wire`); a client that only needs writes can use `UPSERT` and SQL instead.

### 12.5 Errors on the wire

Results carry an `error` string and, additively, an `errorCode` and `retryAfterMs`. Codes
(`wire.ErrorCode`): `BUSY`, `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `CONFLICT`,
`SCHEMA_VIOLATION`, `UNIQUE_VIOLATION`, `UNAUTHORIZED`, `UNSUPPORTED`, `NAMESPACE_MISMATCH`,
`INTEGRITY`, `NOT_HOME`, `INTERNAL`. An old client that reads only `error` keeps working; a new one
should switch on the code ([§16](#16-errors)). A failed precondition is a `CONFLICT_REPORT` whose
operation type is `PRECONDITION_FAILED` and which carries the `actualContentHash` that beat you.

### 12.6 Compatibility rules

New message types are additive (and unknown ones are a decode error); new fields are optional;
`ViolationType`/`ConflictOperationType` ordinals are wire-visible so members are **appended, never
inserted**; numeric error codes never change; Go and Kotlin must emit identical bytes for frames,
the codec, delta and SSTable formats and password hashes. A wire change therefore needs: the Go
change, the Kotlin change (or an explicit "Go-only" note as in `types.go`), regenerated golden
fixtures, and regenerated TypeScript fixtures (`go run ./cmd/kdb-tsfixtures`).

### 12.7 A client with no KDB dependency

Complete program (handshake, session, upsert, document get, SQL), speaking the protocol over a raw
TCP socket with `net` and `encoding/json` only. Run it against a server with
`go run . 127.0.0.1:9090`:

````go
// rawwire/main.go
// A wire client with no dependency on the KDB module: net + encoding/json only.
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
)

const (
	msgHandshake    = 0x01
	msgSessionBegin = 0x0E
	msgSQLExec      = 0x0F
	msgSQLResult    = 0x10
	msgSessionAck   = 0x13
	msgDocGet       = 0x14
	msgDocGetResult = 0x15
	msgUpsert       = 0x16
	msgUpsertResult = 0x17
	headerSize      = 12
	jsonEncodingTag = 1 // payload byte 0: 1 = JSON, 0 = KDB_BINARY envelope (still JSON today)
)

type conn struct {
	c    net.Conn
	corr int32
}

// send writes one frame: int32 LE total length (header included), uint16 LE type,
// uint16 LE protocol version, int32 LE correlation id, then the payload
// (1 encoding-tag byte + JSON envelope).
func (k *conn) send(msgType uint16, env map[string]any) {
	body, err := json.Marshal(env)
	if err != nil {
		log.Fatal(err)
	}
	payload := append([]byte{jsonEncodingTag}, body...)
	k.corr++
	frame := make([]byte, headerSize+len(payload))
	binary.LittleEndian.PutUint32(frame[0:], uint32(len(frame)))
	binary.LittleEndian.PutUint16(frame[4:], msgType)
	binary.LittleEndian.PutUint16(frame[6:], 1) // protocol version
	binary.LittleEndian.PutUint32(frame[8:], uint32(k.corr))
	copy(frame[headerSize:], payload)
	if _, err := k.c.Write(frame); err != nil {
		log.Fatal(err)
	}
}

// recv reads one frame and returns its type and decoded envelope.
func (k *conn) recv() (uint16, map[string]json.RawMessage) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(k.c, lenBuf[:]); err != nil {
		log.Fatal(err)
	}
	total := binary.LittleEndian.Uint32(lenBuf[:])
	rest := make([]byte, total-4)
	if _, err := io.ReadFull(k.c, rest); err != nil {
		log.Fatal(err)
	}
	msgType := binary.LittleEndian.Uint16(rest[0:])
	payload := rest[8:] // skip type(2) + version(2) + correlation(4)
	var env map[string]json.RawMessage
	if err := json.Unmarshal(payload[1:], &env); err != nil { // payload[0] is the tag
		log.Fatal(err)
	}
	return msgType, env
}

func main() {
	addr := "127.0.0.1:9090"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	k := &conn{c: nc}

	// 1. Handshake. Credentials (user/password or token "user:secret") are optional fields.
	k.send(msgHandshake, map[string]any{"kind": "handshake", "handshake": map[string]any{
		"nodeId": "raw-wire-example", "namespaces": []string{}, "localHeads": map[string]string{},
		"supportsZstd": false, "supportsIndexHints": false, "supportsDirectDeltaIngest": false,
		"maxFrameBytes": 16 << 20, "preferredEncodings": []string{"JSON"},
		"clientMode": "SQL_CLIENT", "protocolVersion": 1,
	}})
	_, env := k.recv()
	fmt.Println("handshakeAck:", string(env["handshakeAck"]))

	// 2. Session on a namespace.
	k.send(msgSessionBegin, map[string]any{"kind": "sessionBegin", "sessionBegin": map[string]any{
		"namespace": "raw/demo", "sessionId": nil, "readConsistency": "READ_COMMITTED", "baseVersionHex": nil,
	}})
	_, env = k.recv()
	var ack struct{ SessionID, HeadHex string }
	json.Unmarshal(env["sessionBeginAck"], &ack)
	fmt.Println("session:", ack.SessionID, "head:", ack.HeadHex)

	// 3. Write a document at a chosen id (UPSERT), then read it back (DOCUMENT_GET).
	docID := "11111111-2222-3333-4444-555555555555"
	k.send(msgUpsert, map[string]any{"kind": "upsert", "upsert": map[string]any{
		"namespace": "raw/demo", "docId": docID, "json": `{"hello":"wire"}`, "sessionId": ack.SessionID,
	}})
	_, env = k.recv()
	fmt.Println("upsertResult:", string(env["upsertResult"]))
	k.send(msgDocGet, map[string]any{"kind": "documentGet", "documentGet": map[string]any{
		"namespace": "raw/demo", "docId": docID,
	}})
	_, env = k.recv()
	fmt.Println("documentGetResult:", string(env["documentGetResult"]))

	// 4. SQL.
	k.send(msgSQLExec, map[string]any{"kind": "sqlExec", "sqlExec": map[string]any{
		"namespace": "raw/demo", "sessionId": ack.SessionID,
		"sql": "SELECT kdb_id, hello FROM demo", "parametersJson": nil,
	}})
	_, env = k.recv()
	fmt.Println("sqlResult:", string(env["sqlResult"]))
}
````

Output (hashes shortened): `handshakeAck: {"accepted":true,"negotiatedEncoding":"JSON","protocolVersion":1,...}`,
`session: sess-1 head: a82cbbeb...`, an `upsertResult` with a `commitHex`, a `documentGetResult` whose
`json` is `{"hello":"wire"}`, and an `sqlResult` with `columns:["kdb_id","hello"]` and the one row.

To debug a frame you have captured (or built), decode it with the inspector:

````go
// mkframe/main.go
// Writes one encoded wire frame to a file, for `kdb-inspect dump-wire --file`.
package main

import (
	"log"
	"os"

	"github.com/limidus/kdb/go/kdb/wire"
)

func main() {
	msg := wire.SqlExecMessage{
		H:         wire.Header{MessageType: wire.MsgSqlExec, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 7},
		Namespace: "demo/users",
		SessionID: "sess-1",
		SQL:       "SELECT COUNT(*) AS n FROM users",
	}
	frame, err := wire.NewCodec(wire.EncodingJSON).Encode(msg)
	if err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(os.Args[1], frame, 0o644); err != nil {
		log.Fatal(err)
	}
}
````

```bash
go run . /tmp/frame.bin && ./go/bin/kdb-inspect dump-wire --file /tmp/frame.bin
# {"body":{"correlationId":7,"messageType":"SQL_EXEC","message":{... "SQL":"SELECT COUNT(*) AS n FROM users"}, ...},
#  "header":{"correlationId":7,"messageType":"SQL_EXEC","payloadLength":139,"protocolVersion":1}}
```

---

## 13. Auth tokens and RBAC

**What "token" means today.** The credential on the wire is the pair `user`/`password`, or the
single string `"user:secret"` in the `token` field (what `client.Connect(ctx, addr, token)` sends).
There is no signed or expiring token format yet; the control plane's own note calls it "the static
`user:pass` bearer until real tokens land". Passwords are stored as PBKDF2-HMAC-SHA256 with a
per-user salt, cross-verified between Go and Kotlin.

**Enabling.** `--rbac` turns on the registry. With `--data-dir` it is durable (versioned documents in
the reserved `_system/users` and `_system/roles` namespaces); without it, it is in memory. Bootstrap
while the service is **stopped** (the directory lock enforces it):

```bash
./go/bin/kdb-service user role   --data-dir D --role app-writer --grants 'read:myapp/*,write:myapp/users'
./go/bin/kdb-service user create --data-dir D --user alice --password 's3cret' --roles app-writer
./go/bin/kdb-service user assign --data-dir D --user alice --role app-reader
./go/bin/kdb-service user list   --data-dir D
./go/bin/kdb-service --data-dir D --rbac --namespace myapp/users
```

**Grants** are `<kind>:<pattern>` over `namespace/collection/document`. A trailing `/*` is a prefix
wildcard that also matches the prefix itself; matching walks document -> collection -> database.

| Kind | Gates |
|---|---|
| `read` | session begin, read-only SQL, document read, stream subscribe |
| `write` | non-`SELECT` SQL (including `CREATE TABLE`), `TX_COMMIT`, per-document write/delete |
| `sync` (and `sync_pull`, `sync_push`) | peer sync; `sync` grants both directions |
| `resolve` | settling/dismissing replication conflicts where the chain has an `authority` rule |
| `admin` | the RBAC admin surface; the control plane's process-scoped endpoints need `admin:control` |

Observed in testing: a role with `read:*,write:*,admin:*` did **not** satisfy a read on `app/x`
(the control plane answered `principal root lacks read on app/x`); `read:app/*` did. Grant with an
explicit namespace prefix.

**Enforcement points** overlap deliberately: handshake, session begin, every `SQL_EXEC`, and per
operation at commit (so a revoked grant takes effect on the very next commit, not the next
connection). The auth engine is one object (`server.KdbServerRuntime.AuthEngine`), shared by the wire
listeners, the stream hub, the peer listener and the control plane. To replace it see
[§15.4](#154-auth-providers).

**Stream and anonymous access.** Under `--rbac` stream subscribers must authenticate;
`--stream-allow-anonymous` restores the old behaviour and exposes every commit.

---

## 14. Configuration reference

Precedence: **defaults < `--config` JSON file < `KDB_*` environment < explicitly set flags.** The
config file rejects unknown keys (verified: `json: unknown field "sqlAdr"` stops startup). I checked
that an environment variable overrides the file (`KDB_SQL_ADDR` moved a listener the file set).
Settings carry a mutability: *restart*, *live* (changeable through the control plane's `PATCH
/v1/settings`), or *new connections*.

### 14.1 `kdb-service` flags

`kdb-service --help` is the complete list; this is derived from it and from
`go/kdb/config/descriptors.go`. "Key" is the `--config` JSON key.

| Flag | Env | Key | Default | Mut. | Meaning |
|---|---|---|---|---|---|
| `--data-dir` | `KDB_DATA_DIR` | `dataDir` | - | restart | filesystem data root (exclusive lock) |
| `--memory` | `KDB_MEMORY` | `memory` | on if no data dir | restart | in-memory runtime |
| `--namespace` | `KDB_NAMESPACE` | `namespace` | `demo/users` | restart | default namespace |
| `--sql-addr` | `KDB_SQL_ADDR` | `sqlAddr` | `tcp://127.0.0.1:9090?bind=true` | restart | SQL wire listener |
| `--peer-addr` | `KDB_PEER_ADDR` | `peerAddr` | _(off)_ | restart | peer-sync listener, e.g. `tcp://127.0.0.1:9091?bind=true` |
| `--stream-addr` | `KDB_STREAM_ADDR` | `streamAddr` | _(off)_ | restart | stream listener, e.g. `tcp://127.0.0.1:9092?bind=true` |
| `--ws-addr` | `KDB_WS_ADDR` | `wsAddr` | off | restart | WebSocket SQL wire (`ws://host:port/kdb?bind=true`) |
| `--grpc-addr` | `KDB_GRPC_ADDR` | `grpcAddr` | off | restart | gRPC SQL wire; **kdb-service-grpc build only** |
| `--admin-addr` | `KDB_ADMIN_ADDR` | `adminAddr` | off | restart | `/healthz /readyz /metrics /debug/vars /debug/pprof`, no auth: bind privately |
| `--rbac` | `KDB_RBAC` | `rbac` | off | restart | user/role registry |
| `--tls-cert` `--tls-key` | `KDB_TLS_CERT` `KDB_TLS_KEY` | `tls.certFile` `tls.keyFile` | - | new conns | TLS on SQL/peer/stream (schemes upgrade to `tcps://`) |
| `--tls-ca` | `KDB_TLS_CA` | `tls.caFile` | - | new conns | CA for client certs |
| `--tls-client-auth` | `KDB_TLS_CLIENT_AUTH` | `tls.clientAuth` | off | new conns | require client certs (needs `--tls-ca`) |
| `--durability` | `KDB_DURABILITY` | `durability` | `sync` | restart | `sync`, `async`, `memory` |
| `--async-sync-interval-ms` | `KDB_ASYNC_SYNC_INTERVAL_MS` | `asyncSyncIntervalMs` | 5 | restart | background flush under `async` |
| `--compression` | `KDB_COMPRESSION` | `compression` | `zstd` | restart | `zstd` or `none` for new frames (recorded per frame) |
| `--sync-mode` | `KDB_SYNC_MODE` | `syncMode` | `full` | restart | `full` (macOS `F_FULLFSYNC`; Linux `fsync`) or `fast` (macOS `F_BARRIERFSYNC`; Linux `fdatasync`) |
| `--memory-budget-mb` | `KDB_MEMORY_BUDGET_MB` | `memoryBudgetMb` | 0 = auto (cgroup limit, else 75 % of RAM); -1 off | live | admission-control budget |
| `--memory-limit-mb` | `KDB_MEMORY_LIMIT_MB` | `memoryLimitMb` | - | live | deprecated alias (an explicit 0 disables) |
| `--memory-reserve-mb` | `KDB_MEMORY_RESERVE_MB` | `memoryReserveMb` | 48 | live | rescue reserve (clamped to a quarter of the budget) |
| `--max-connections` | `KDB_MAX_CONNECTIONS` | `maxConnections` | 256 | new conns | per-listener cap (0 unlimited) |
| `--scan-row-budget` | `KDB_SCAN_ROW_BUDGET` | `scanRowBudget` | 1 000 000 | live | rows one scan may *examine* |
| `--abort-after` | `KDB_ABORT_AFTER` | `abortAfter` | off | restart | sustained pressure -> drain and exit **75** |
| `--drain-timeout` | `KDB_DRAIN_TIMEOUT` | `drainTimeout` | 30s | restart | wait for admitted writes on SIGTERM |
| `--maintenance-interval` | `KDB_MAINTENANCE_INTERVAL` | `maintenanceInterval` | 5m | live | checkpoint/reclaim/compact tick (0 = at close only) |
| `--maintenance-sweep` | `KDB_MAINTENANCE_SWEEP` | `maintenanceSweep` | 30m | live | max time an idle namespace goes without a pass |
| `--maintenance-max-defer` | `KDB_MAINTENANCE_MAX_DEFER` | `maintenanceMaxDefer` | 6 | live | ticks write load may postpone a pass |
| `--control-addr` | `KDB_CONTROL_ADDR` | `controlAddr` | off | restart | control-plane HTTP API + UI (authenticated) |
| `--control-write` | `KDB_CONTROL_WRITE` | `controlWrite` | off | restart | allow mutating control endpoints |
| `--control-ui` | `KDB_CONTROL_UI` | `controlUi` | on | restart | serve the embedded UI |
| `--control-settings-persist` | `KDB_CONTROL_SETTINGS_PERSIST` | `controlSettingsPersist` | off | restart | write live changes back to the config file |
| `--control-promote` | `KDB_CONTROL_PROMOTE` | `controlPromote` | off | restart | allow promoting a staged restore (ends in exit 75) |
| `--control-backup-dir` `--control-staging-dir` | `KDB_CONTROL_BACKUP_DIR` `KDB_CONTROL_STAGING_DIR` | `controlBackupDir` `controlStagingDir` | - | restart | backup/restore locations; keep off the data volume |
| `--log-level` | `KDB_LOG_LEVEL` | `logLevel` | `info` | live | `debug|info|warn|error` |
| `--log-format` | `KDB_LOG_FORMAT` | `logFormat` | `text` | restart | `text|json` |
| `--config` | - | - | - | - | the JSON file |
| `--version` | - | - | - | - | print and exit |

**Flag-only** (no config-file key or descriptor in v0.6.2): `--peer` (repeatable; env `KDB_PEERS`,
`;`-separated), `--peer-conflict-policy`, `--peer-create-namespaces`, `--peer-retention-grace`
(168h), `--peer-http`, `--stream-allow-anonymous`, `--conflict-webhook`,
`--conflict-webhook-interval` (10s), `--conflict-webhook-secret` (env
`KDB_CONFLICT_WEBHOOK_SECRET`; HMAC-SHA256 signature header `X-KDB-Signature: sha256=<hex>`),
`--expire-field`, `--expire-grace`, `--expire-interval` (1m; documents past an RFC 3339 or epoch-ms
timestamp field are hidden, then deleted), `--scrub-interval` (0 = off).

Subcommand: `kdb-service user create|role|assign|list` ([§13](#13-auth-tokens-and-rbac)).

Signals: `SIGTERM`/`SIGINT` flip `/readyz` to `draining`, refuse new writes (`UNAVAILABLE`), wait up
to `--drain-timeout`, close listeners, seal storage, exit 0. Exit code **75** is the "restart me"
contract for the pressure watchdog and for control-plane promotion.

### 14.2 Config file

```json
{
  "dataDir": "/var/lib/kdb",
  "namespace": "myapp/users",
  "sqlAddr": "tcp://0.0.0.0:9090?bind=true",
  "peerAddr": "",
  "streamAddr": "",
  "adminAddr": "127.0.0.1:9099",
  "rbac": true,
  "tls": {"certFile": "cert.pem", "keyFile": "key.pem", "caFile": "ca.pem", "clientAuth": false},
  "memoryBudgetMb": 0,
  "drainTimeout": "30s",
  "logFormat": "json"
}
```

Durations are Go duration strings. Pointer semantics mean an absent key falls through to the
environment, flags and defaults.

### 14.3 The CLI and tools

`kdb` takes global `--data-dir DIR` (default `~/.kdb`), `--quiet`, `--version`; run it with no
arguments for the subcommand list. `kdb-inspect` takes per-subcommand `--data-dir`, `--namespace`
and `--to DIR|s3` (S3 from `KDB_S3_*`). `kdb-loadtest` takes `-addr -concurrency -doc-bytes
-doc-pool -duration -namespace -read-ratio -warmup`. The e2e harness reads `KDB_SERVICE_BIN
KDB_E2E_HELPER_BIN KDB_CLI_BIN KDB_CLI_CLASSPATH_FILE`. `KDB_NODE_ID` overrides the persisted `NODE`
identity for a process (not written back).

### 14.4 Engine settings (environment only)

Read by `embed.FileRuntimeOptionsFromEnv`; there is no flag or config key. The engine reads them
**leniently** (an unparseable value silently becomes the default); the control plane's settings
view reports a discarded value with a warning.

| Variable | Meaning |
|---|---|
| `KDB_HISTORY_MODE` | `full` or `none`. Refused at open if it disagrees with how the namespace was built; changing it is a migration (`kdb-inspect migrate-history --history-mode`) |
| `KDB_HISTORY_STRATEGY` | `replay` or `objects`; changing it is `kdb-inspect migrate-history --to ...` |
| `KDB_RECLAIM_MODE` | `manual`, `immediate`, `balanced` (default), `lazy` |
| `KDB_RETAIN_DURATION`, `KDB_RETAIN_COMMITS` | the `history=none` window (a floor at segment granularity) |
| `KDB_DOCUMENT_CACHE_BYTES`, `KDB_COMMIT_OPS_BYTES`, `KDB_HISTORY_TREE_CACHE_BYTES` | resident-memory budgets (0 = a share of the hot-tier budget) |
| `KDB_CHECKPOINTS` | `off` disables the checkpoint that lets an open skip the delta log |
| `KDB_ANCESTRY_PRUNING`, `KDB_GRAPH_FILE`, `KDB_GRAPH_REBUILD_COMMITS`, `KDB_GRAPH_MAX_SEGMENTS`, `KDB_HISTORY_ANCHOR_INTERVAL` | commit-graph and tree-anchor tuning. `KDB_GRAPH_FILE` maps the commit graph from segment files instead of the heap and is **on by default**; `KDB_GRAPH_FILE=off` holds it in the heap (see `docs/kdb-mmap-plan.md`) |
| `KDB_PREALLOCATE_SEGMENTS` | preallocate delta segments (off by default) |
| `KDB_S3_BUCKET`, `_ENDPOINT`, `_REGION`, `_ACCESS_KEY_ID`, `_SECRET_ACCESS_KEY`, `_PREFIX`, `_PATH_STYLE`, `_ENSURE_BUCKET` | S3 replica tier |
| `KDB_S3_ARCHIVE_BUCKET`, `_ENDPOINT`, `_REGION`, `_PREFIX` | S3 archive tier (keeps what `history=none` reclaims; use a different bucket/prefix from the replica) |

---

## 15. Extension points

Every example here is a compiled program. The seams are real interfaces in the public packages.

| You want to add | Implement | Install with |
|---|---|---|
| an index implementation | `index.StoreFactory` (+ `index.Store` / `index.DocumentStore`) | `index.NewRegistry` + `server.NewRegistryIndexProvider` + `srv.SetSQLIndexProvider` / `SetSearchProvider` |
| a whole query access path | `sql.IndexProvider` (+ `sql.IndexDDLExecutor`) | `srv.SetSQLIndexProvider` |
| conflict logic | JavaScript `main(conflict)`, or `transaction.ConflictResolver` | chain rule / `script.ConflictResolver` / `transaction.NewEngine(ConflictPolicyCustom, r)` |
| a transport | client `stream.Transport`; server `server.FrameConn` + `FrameHost` | `client.ConnectOptions.Transport`; `service.RegisterGRPCListener` |
| an auth provider | `auth.Engine` (`Authenticator` + `Authorizer`) | `srv.AuthEngine = ...` |
| a commit hook | `func(ns string, c document.Commit)` | `srv.CommitListener` |
| a storage backend | `storage.PlatformIOShim` / `storage.Adapter` | `embed` options (see `storage/io/s3`) **[not exercised]** |
| a compute backend | `compute.Adapter` | see below |

### 15.1 Index stores

`index.IndexType` is a closed enum (`HASH`, `BTREE`, `FULLTEXT`, `VECTOR`) that the SQL parser
recognises by name, so a *new kind* of index means editing `index/models.go`, the parser and
`index/stores/factory.go` (a contribution to the core). What you can add without touching the core
is a **different implementation** of an existing kind: a factory that returns your `index.Store`
(for the commit path to feed it, also implement `index.DocumentStore`: `PrepareDocument` validates
and extracts without mutating, and the returned `PreparedPut.Apply(commit)` records the update).

Contract details from `index/store.go`: ranked results sort by score descending then document id
ascending (canonical UUID string order) so two stores over one corpus give the same list;
`Lookup`/`Range` return ids only; `Snapshot`/`RestoreSnapshot` back persistence.

The program below installs a wrapping factory, creates a full-text and a hash index through SQL,
and queries through both `MATCH` and `client.Search`:

````go
// extindex/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// countingFactory is the smallest useful index.StoreFactory: it delegates to the stock factory
// and counts what the engine asks for. A real replacement would return its own index.Store (and,
// to be fed by the commit path, an index.DocumentStore) for the descriptor types it takes over.
type countingFactory struct {
	inner   index.StoreFactory
	created atomic.Int32
}

func (f *countingFactory) Create(d index.Descriptor) (index.Store, error) {
	f.created.Add(1)
	fmt.Printf("factory: creating %s index %q on %v\n", d.Type, d.IndexName(), d.Fields)
	return f.inner.Create(d)
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	const ns = "demo/docs"

	rt, err := embed.OpenMemoryRuntime("demo", ns, schema.None())
	if err != nil {
		log.Fatal(err)
	}
	srv := server.NewKdbServerRuntime(rt)

	// Build the registry around the factory, wrap it in the provider the SQL planner and the
	// SEARCH frame both talk to, and install it. Without this the server refuses CREATE INDEX.
	d := rt.DAG.(*dag.InMemoryCommitDag)
	factory := &countingFactory{inner: stores.NewFactory(d, stores.Options{})}
	reg := index.NewRegistry(ns, d, factory)
	provider := server.NewRegistryIndexProvider(reg, srv)
	srv.SetSQLIndexProvider(provider)
	srv.SetSearchProvider(provider)

	ln, err := server.ListenSqlWire("tcp://127.0.0.1:0?bind=true", srv)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()

	c, err := client.Connect(ctx, ln.Addr().String(), "")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	exec := func(sql string, args ...any) {
		if err := c.Exec(ctx, ns, sql, args); err != nil {
			log.Fatalf("%s: %v", sql, err)
		}
	}
	exec("CREATE TABLE docs (title VARCHAR, body VARCHAR)")
	exec("INSERT INTO docs (title, body) VALUES ('Intro', 'KDB is source control for documents')")
	exec("INSERT INTO docs (title, body) VALUES ('Other', 'nothing relevant here')")
	exec("CREATE INDEX idx_body ON docs (body) USING FULLTEXT")
	exec("CREATE INDEX idx_title ON docs (title) USING HASH")

	_, rows, err := c.QueryRaw(ctx, ns, "SELECT title FROM docs WHERE MATCH(idx_body, 'source control')", nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("MATCH rows:", rows, "factory calls:", factory.created.Load())

	// The same index through the SEARCH frame instead of SQL.
	res, err := c.Search(ctx, client.SearchRequest{
		Namespace:   ns,
		Text:        &client.SearchTextArm{Index: "idx_body", Query: "source control", Depth: 10},
		Limit:       5,
		IncludeJSON: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, h := range res.Hits {
		fmt.Printf("search hit %s score>0=%v %s\n", h.DocID[:8], h.Score > 0, h.JSON)
	}
}
````

Output:

```
factory: creating FULLTEXT index "idx_body" on [body]
factory: creating HASH index "idx_title" on [title]
MATCH rows: [[Intro]] factory calls: 2
search hit 93c91ad0 score>0=true {"title":"Intro","body":"KDB is source control for documents"}
```

`srv.OpenIndexes(stores.Options{})` does the standard wiring in one call and persists the registry
under `<dataRoot>/ns/<ns>/index/`.

### 15.2 Stored procedures and conflict resolvers

A procedure is a JavaScript program defining `main(arg)`, run by `go/kdb/script` (goja) with
limits (`script.Limits`: wall clock 5 s, output 1 MiB, log 64 KiB, 1000 host calls by default).
There are three places a procedure can decide a conflict:

1. **In a merge** (a resolution-chain `procedure` rule): runs on **every** node and must agree, so
   it runs in `ModePure` with no clock, randomness or database; the chain records the exact source
   revision and a node holding a different copy stops merging that namespace rather than merging
   under different rules. Define it through the control plane
   (`PUT /v1/ns/{ns}/procedures/{name}` with `{"source":"function main(c){...}"}`) and name it in
   the chain (`PUT .../resolution`).
2. **On the authority** (a chain `authority` rule with `procedure`): runs after the merge on one
   node in `ModeRead`, so it may call `kdb.get` and `kdb.query`; its answer is an ordinary commit.
3. **For a local transaction** (`script.ConflictResolver` implements
   `transaction.ConflictResolver`): for embedded callers with no server; non-determinism is fine
   because nothing is replicated as a merge.

What a procedure receives: `local`, `remote` (either may be `null` for a delete), `base`, `origins`
(`nodeId`, `commit`, `timestampMicros` per side), and `fields` (the differing paths, each marked
`local`, `remote`, `both` or `same`). `side0`/`side1` are aliases for callers that must not care
which node they run on. What it returns, exactly one of: `{take:"local"|"remote"}`, `{doc:{...}}`,
`{fork:{keep:"local"|"remote"}}`, `{delete:true}`, `{defer:true}`. A procedure that throws or times
out holds the merge; it never falls through to the next rule. `conflictOf` back-references a forked
document to its twin.

The Go runtime can be used directly:

````go
// extproc/main.go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/limidus/kdb/go/kdb/script"
)

const keepCancelled = `
function main(c) {
  if (c.local && c.local.status === "cancelled") { return { take: "local" }; }
  if (c.remote && c.remote.status === "cancelled") { return { take: "remote" }; }
  // otherwise add the two counters together relative to the common base
  if (c.local && c.remote && c.base) {
    return { doc: { status: c.local.status, n: c.local.n + c.remote.n - c.base.n } };
  }
  return { defer: true };
}`

func main() {
	rt := script.New(script.Limits{}) // zero values take script.DefaultLimits
	ctx := context.Background()
	s := func(v string) *string { return &v }

	for _, in := range []script.ConflictInput{
		{DocID: "d1", Base: s(`{"status":"open","n":1}`), Local: s(`{"status":"cancelled","n":1}`), Remote: s(`{"status":"open","n":4}`)},
		{DocID: "d2", Base: s(`{"status":"open","n":1}`), Local: s(`{"status":"open","n":3}`), Remote: s(`{"status":"open","n":4}`)},
		{DocID: "d3", Local: s(`{"status":"open","n":3}`), Remote: s(`{"status":"open","n":4}`)}, // no base -> defer
	} {
		// ModePure is what a merge-time procedure must use: no clock, no randomness, no database.
		d, err := rt.ResolveConflict(ctx, keepCancelled, script.ModePure, nil, in)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s -> kind=%d side=%d doc=%s\n", in.DocID, d.Kind, d.Side, d.Doc)
	}

	// The sandbox refuses non-determinism.
	_, err := rt.ResolveConflict(ctx, `function main(c){ return { doc: { t: Date.now() } }; }`,
		script.ModePure, nil, script.ConflictInput{DocID: "x", Local: s(`{}`), Remote: s(`{}`)})
	fmt.Println("Date.now in pure mode ->", err)

	// General-purpose calls: main(args) over JSON.
	res, err := rt.Call(ctx, `function main(a){ return a.xs.reduce(function(s,x){return s+x;},0); }`,
		script.ModePure, nil, `{"xs":[1,2,3]}`)
	fmt.Println("Call ->", res.Value, err)
}
````

Output:

```
d1 -> kind=1 side=0 doc=
d2 -> kind=2 side=0 doc={"status":"open","n":6}
d3 -> kind=0 side=0 doc=
Date.now in pure mode -> procedure threw: while calling main(args): Error: kdb: Date.now is not available in a deterministic procedure at kdb:pure-prelude:5:13(7)
Call -> 6 <nil>
```

(`DecisionKind`: 0 defer, 1 take, 2 doc, 3 fork, 4 delete.) The Kotlin tree has the same runtime on
GraalJS (`:kdb-script`). The Go server has no general-purpose "call a procedure" wire message yet:
procedures are reachable as conflict resolvers.

To hand conflicts to your own service instead, set `--conflict-webhook URL`
(`--conflict-webhook-secret`): each queued conflict is POSTed with an `X-KDB-Signature:
sha256=<hex>` HMAC and retried until answered with 2xx; settle with `POST
/v1/ns/{ns}/conflicts/{id}/resolve` on the control plane.

### 15.3 Transports

Two independent seams:

* **Client side** (`stream.Transport` + `stream.ConnectionHandle`): give `client.ConnectOptions` a
  `Transport`. Everything above the socket (handshake, correlation, sessions, retries) is
  unchanged. The `go/grpc` module exists because dialling gRPC could not live in the core module.
* **Server side** (`server.FrameHost`): supply an object with `Incoming() <-chan []byte` and
  `Send([]byte) error` (a `server.FrameConn`) and call `host.Serve(conn)`. You inherit the codec,
  handshake and authentication, RBAC, sessions and their frame ordering, memory-pressure shedding
  (use `host.Admit(frame)` if your transport holds whole messages), the in-flight bound, and the
  per-request panic backstop. To make `kdb-service` serve it, register a factory with
  `service.RegisterGRPCListener` from an `init` in a package your own `main` imports (see
  `go/grpc/register`), the pattern `kdb-service-grpc` uses.

The program in §15.4 uses both seams at once, with frames carried over Go channels instead of a
socket, together with a custom auth provider.

### 15.4 Auth providers

`auth.Engine` is two interfaces: `Authenticator.Authenticate(ctx, auth.Credentials) (auth.Principal,
error)` and `Authorizer.Authorize(ctx, principal, auth.Action) error`. `Credentials` carries
whatever the handshake carried (`User`/`Password`/`Token`). `Action` is a closed set of types
(`SessionBeginAction`, `SqlExecAction{ReadOnly}`, `TxCommitAction`, `DocumentReadAction`,
`DocumentWriteAction`, `DocumentDeleteAction`, `PeerSyncAction`/`PeerPullAction`/`PeerPushAction`,
`ConflictResolveAction`, `StreamSubscribeAction`, `AdminAction`); an engine written before an action
existed will deny it, so **default-deny unknown actions** and test the control plane and replication
with your engine (the control plane reuses `SqlExecAction` and `AdminAction{Scope:"control"}`).
`Principal` is `{ID, Roles, Claims}`.

````go
// extauthtransport/main.go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/stream"
)

// ---- A custom auth provider: bearer tokens looked up in a map ------------------------------

type tokenEngine struct{ tokens map[string]auth.Principal }

func (e tokenEngine) Authenticator() auth.Authenticator { return e }
func (e tokenEngine) Authorizer() auth.Authorizer       { return e }

// Authenticate receives whatever the client put in the handshake (User/Password or Token).
func (e tokenEngine) Authenticate(_ context.Context, c auth.Credentials) (auth.Principal, error) {
	if c.Token == nil {
		return auth.Principal{}, errors.New("token required")
	}
	p, ok := e.tokens[*c.Token]
	if !ok {
		return auth.Principal{}, errors.New("unknown token")
	}
	return p, nil
}

// Authorize is consulted for every Action: session begin, SQL, commit, per-document writes...
func (e tokenEngine) Authorize(_ context.Context, p auth.Principal, a auth.Action) error {
	if _, writer := p.Roles["writer"]; writer {
		return nil
	}
	switch act := a.(type) {
	case auth.SessionBeginAction, auth.DocumentReadAction:
		return nil
	case auth.SqlExecAction:
		if act.ReadOnly {
			return nil
		}
	}
	return fmt.Errorf("principal %s may only read", p.ID)
}

// ---- A custom transport: frames travel over Go channels instead of a socket ----------------

// pipeEnd is both stream.ConnectionHandle (client side) and server.FrameConn (server side).
type pipeEnd struct {
	in   chan []byte
	peer *pipeEnd
	once sync.Once
}

func (p *pipeEnd) Incoming() <-chan []byte { return p.in }
func (p *pipeEnd) Send(frame []byte) error {
	cp := append([]byte(nil), frame...) // transports own the bytes they carry
	select {
	case p.peer.in <- cp:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("pipe: peer not reading")
	}
}
func (p *pipeEnd) Close() error { p.once.Do(func() { close(p.peer.in) }); return nil }
func (p *pipeEnd) TryPoll() []byte {
	select {
	case f := <-p.in:
		return f
	default:
		return nil
	}
}

// pipeTransport implements stream.Transport (the client-side seam) and feeds the other end
// to a server.FrameHost (the server-side seam): supply frames, inherit everything above.
type pipeTransport struct{ host *server.FrameHost }

func (t pipeTransport) Connect(string) (stream.ConnectionHandle, error) {
	a := &pipeEnd{in: make(chan []byte, 64)}
	b := &pipeEnd{in: make(chan []byte, 64)}
	a.peer, b.peer = b, a
	go t.host.Serve(b) // returns when a.Close() closes b's inbound channel
	return a, nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	const ns = "demo/users"

	rt, err := embed.OpenMemoryRuntime("demo", ns, schema.None())
	if err != nil {
		log.Fatal(err)
	}
	srv := server.NewKdbServerRuntime(rt)
	srv.AuthEngine = tokenEngine{tokens: map[string]auth.Principal{
		"tok-writer": {ID: "svc-writer", Roles: map[string]struct{}{"writer": {}}},
		"tok-reader": {ID: "svc-reader", Roles: map[string]struct{}{}},
	}}

	opts := client.ConnectOptions{Transport: pipeTransport{host: server.NewFrameHost(srv)}}
	w, err := client.ConnectWithOptions(ctx, "pipe://in-process", "tok-writer", opts)
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()
	r, err := client.ConnectWithOptions(ctx, "pipe://in-process", "tok-reader", opts)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()
	if _, err := client.ConnectWithOptions(ctx, "pipe://in-process", "nope", opts); err != nil {
		fmt.Println("bad token  ->", err)
	}

	if _, err := w.PutJSON(ctx, ns, "00000000-0000-0000-0000-000000000001", []byte(`{"hello":"pipe"}`)); err != nil {
		log.Fatal(err)
	}
	body, _, err := r.GetJSON(ctx, ns, "00000000-0000-0000-0000-000000000001")
	fmt.Println("reader get ->", string(body), err)
	_, err = r.Upsert(ctx, ns, "00000000-0000-0000-0000-000000000001", []byte(`{"hello":"nope"}`))
	fmt.Println("reader put ->", err)
}
````

Output:

```
bad token  -> kdb: unauthenticated: unknown token
reader get -> {"hello":"pipe"} <nil>
reader put -> kdb: authorization denied: principal svc-reader may only read
```

To plug the same engine into `kdb-service` itself, build your own `main` around
`go/kdb/service` the way `go/grpc/cmd/kdb-service-grpc` does and assign `AuthEngine` where
`service.Main` does today (`service.go`, the `--rbac` block). There is no flag or plugin loader for
an external engine in v0.6.2.

### 15.5 Compute backends

`compute.Adapter` is `{Name() string; Dot(ctx, a, b []float32) (float32, error)}` with a portable
`CPUAdapter`; `compute/webgpu` has `js && wasm` and native implementations behind build tags. In
v0.6.2 nothing in the Go server or the vector index calls it (a search of the tree finds no import
of `kdb/compute` outside the webgpu package), so a Go compute backend is a seam awaiting a
consumer. The Kotlin tree has `kdb-compute`, `kdb-compute-jvm` and `kdb-compute-webgpu`.

---

## 16. Errors

### 16.1 What you handle in Go

| Test | Meaning | Do |
|---|---|---|
| `errors.Is(err, client.ErrConflict)` (+ `errors.As(&*ConflictError)`) | another writer changed a document you touched since `BaseVersion` | re-read, rebase, retry (the error carries a `RetryAfterMs` hint) |
| `errors.Is(err, client.ErrPreconditionFailed)` | `PutIfAbsent`/`ReplaceIf` assertion false | re-derive; **do not blind-retry**; `PreconditionError.ActualHash` is what beat you |
| `errors.Is(err, client.ErrLockUnavailable)` | another session holds the lease | wait, or read the holder from `*LockError` |
| `errors.Is(err, client.ErrBusy)` | queue full or memory pressure | wait `(*BusyError).RetryAfter()`, retry |
| `errors.Is(err, client.ErrUnavailable)` | server draining | reconnect, likely to a restarted instance |
| `errors.Is(err, client.ErrDeadlineExceeded)` | your deadline passed while queued | retry with a longer deadline |
| `errors.Is(err, client.ErrUnsupported)` | nothing configured to serve it (e.g. SEARCH with no index provider) | operator-side fix; never retry unmodified |
| `errors.Is(err, client.ErrNotFound)` | no such document | - |
| `errors.Is(err, client.ErrUnauthenticated)` | handshake or session auth failed | fix credentials |
| `errors.Is(err, client.ErrNotHome)` | namespace is single-home elsewhere | write to the node named in the error (`NotHomeError.Home`) |
| `errors.Is(err, client.ErrClosed)` / `*TransportError` | connection gone | reconnect |
| `client.ErrAborted`, `client.ErrTxDone` | your `CompareAndSwap` callback returned nil; a finished `Tx` was reused | programming error |
| message contains `UNIQUE_CONSTRAINT` (observed) / wire code `UNIQUE_VIOLATION` | a unique value is taken | change the value; retrying is pointless |
| `RESOURCE_EXHAUSTED` | request too large or scan budget exceeded | resubmit smaller / narrow the query |
| `errors.Is(err, driver.ErrConflict)` | `database/sql` write conflict | re-run the transaction |

`client` has no dedicated sentinel for `UNIQUE_VIOLATION` or `RESOURCE_EXHAUSTED` in v0.6.2; they
arrive as a plain `kdb: ...` error whose text names the cause. Their wire `errorCode` is on the
message if you speak the protocol yourself.

### 16.2 Engine error codes

Numeric and **stable** (`go/kdb/error/code.go`): `1001/1002` decode/encode, `1005` schema (codec),
`2001` JSONPath, `3001/3002` schema violation / migration failed, `3101` version not found, `3102`
archived commit, `3103` compaction boundary, `4001` conflict, `4002` document locked, `4101` storage
tier, `4102` data directory locked, `4201` namespace not found, `5001` index corruption, `6001/6002`
unsupported protocol / encoding negotiation, `6101` transport, `6201/6202` compute, `6301/6302`
authentication/authorization failed, `7001` archive restore.

In Go the pattern is a sentinel plus a typed error with `Unwrap`, so `errors.Is` and `errors.As`
both work (`BusyError`, `ConflictError`, `LockError`, `PreconditionError`, `UnsupportedError`,
`NotHomeError`, `CrossNamespaceError`). New errors should follow it.

---

## 17. Debugging and inspection

**Logs.** Structured `log/slog`; `--log-level debug` and `--log-format json`; `log.level` can be
changed live through the control plane. The startup line reports every subsystem's resolved status
and the exact build.

**Admin endpoint** (`--admin-addr`, unauthenticated, keep it private): `/healthz` (version, commit,
build date, `node_id`), `/readyz`, `/metrics`, `/debug/vars`, `/debug/pprof/`.

**Metrics** (Prometheus text): `kdb_build_info`; `kdb_stage_ops_total` and
`kdb_stage_latency_seconds{stage=lock_wait|fsync_wait|tree_rebuild, stat=mean|p50|p99|max}`;
`kdb_go_goroutines`, `kdb_go_heap_alloc_bytes`, `kdb_go_mem_total_bytes`, `kdb_go_gc_total`;
`kdb_draining`; `kdb_memory_zone` (0 normal ... 3 critical); `kdb_admission_granted_total{class}`,
`kdb_admission_denied_total{class,reason}`, `kdb_admission_outstanding_bytes`,
`kdb_admission_floor_bytes`, `kdb_admission_scan_row_budget`, `kdb_admission_zone_changes_total`,
`kdb_admission_critical_enters_total`; `kdb_cost_estimate_accuracy_p95`,
`kdb_cost_safety_multiplier`, `kdb_cost_learned_cells`; `kdb_conflicts_open`; and with peers
`kdb_replication_commits_total`, `kdb_replication_overshoot_commits_total`,
`kdb_replication_consecutive_failures`, `kdb_replication_last_success_seconds`. Alert on `kdb_memory_zone`, non-zero
`kdb_admission_denied_total` and rising `fsync_wait`.

**Control plane** (`--control-addr`, authenticated by the same engine; `--control-write` for
mutations): a JSON API plus embedded UI; routes are registered in `go/kdb/control/server.go`
(namespaces, status, log, commits and diffs, refs, docs and document history, schema, indexes,
conflicts, resolution, procedures, retention, integrity, checkpoints, backups, restore staging and
promotion, `PATCH /v1/settings`). Verified here: `GET /v1/namespaces`, `/v1/health` and
`/v1/settings` with HTTP basic auth (a user needs `admin:control` for the process-scoped ones). Other
request shapes **[not run]**; read the handlers.

**`kdb-inspect`** (works on a stopped data directory; mutating subcommands take the exclusive
lock):

```bash
kdb-inspect verify --data-dir D --namespace NS [--level L1|L2] [--json]
# namespace myapp/users: 1 segment(s) scanned / segment 0: 214 byte(s), 1 frame(s) / clean: no findings
kdb-inspect repair-segments --data-dir D --namespace NS [--dry-run]
kdb-inspect backup --data-dir D --namespace NS --to DIR|s3 [--base-backup-id ID]
# backup complete / backupId: <uuid> / commits: 1 / segments: 1 / head main: <hash>
kdb-inspect backup-list   --namespace NS --to DIR|s3
kdb-inspect backup-verify --namespace NS --to DIR|s3 --backup-id ID
kdb-inspect backup-fetch  --namespace NS --to DIR|s3 --backup-id ID --out DIR
kdb-inspect restore --namespace NS --out DIR [--source LABEL=PATH ...] [--from-backup DIR|s3 --backup-id ID]
kdb-inspect migrate-history --data-dir D --namespace NS --to replay|objects
kdb-inspect dump-wire --file FRAME.bin [--compact]
```

`verify`, `backup`, `list` ran as shown; with a service still holding the directory they fail
cleanly with `data directory is in use ... (stop kdb-service / the embedded runtime first)`.
Procedures and runbooks: [user guide, Operations](kdb-user-guide.md#operations--durability-backup-and-recovery).
The Kotlin inspector (`:kdb-inspect:inspectCli`) has `dump-delta`, `dump-wire`, `dump-commit` and
`dump-blob`.

**Profiling.** `go tool pprof http://127.0.0.1:9099/debug/pprof/profile` against `--admin-addr`.
`kdb-pressure-test` drives governance behaviour end to end and prints one machine-parseable `RESULT`
line.

---

## 18. Conventions

**Go.**
* `gofmt` clean and `go vet ./...` clean; CI fails otherwise. Run with `-race`.
* Packages mirror the Kotlin modules; a change that touches a wire or physical format changes both
  trees (or states "Go-only" in the type's comment), regenerates the golden fixtures and, for wire,
  the TypeScript ones.
* The existing comments explain *why*, including the failure that motivated the code, and name the
  spec section (`kdb-spec-layerNN`, "Component N §x"). Match that: a reviewer should be able to
  tell what breaks if the code is removed.
* Request-path code must not panic: parsers return `*ParseError`, the frame handler has a per-request
  panic backstop, decoders validate lengths before slicing.
* Wire/format evolution is **additive**: optional fields, appended enum members, numeric error codes
  never reused.
* Errors: sentinel + typed error + `Unwrap`; classify for the wire in one place
  (`go/kdb/server/error_classification.go`).
* The design is crash-only: every commit is durable at ack, a missing clean shutdown only costs an
  extra replay on the next start. Do not add a path that needs an orderly shutdown for correctness.
* Platform code is in `*_unix.go` / `*_other.go` / `*_linux.go` / `*_darwin.go` pairs; keep both
  halves compiling (`GOOS=js GOARCH=wasm` is in CI).
* Anything process-global that reads `KDB_*` goes through `go/kdb/config` descriptors so it shows up
  in the control plane's settings view.

**Kotlin.**
* `./gradlew build` must be green. Test `commonTest` code against JS too: Kotlin/JS mangles
  `internal` names per module, so a `commonTest` call to an `internal` member compiles and passes on
  the JVM but fails at runtime on JS with a `..._k$ is not a function` TypeError.
* Do not trust the line number of a failure inside `runBlocking { }`; read
  `build/test-results/**/*.xml` (CI uploads them on failure). CI runners are 2-core; reproduce
  concurrency flakes with `-XX:ActiveProcessorCount=2`.

**Tests.**
* Put tests beside the code (`foo_test.go`). Prefer real listeners on port 0 over mocks.
* Cross-language behaviour is proven by fixtures, not by reading both implementations. Use
  `go/testdata/golden/` and the existing exporters; add a fixture instead of a comment.
* A test that depends on another language or service sits behind a build tag (`interop`,
  `s3integration`) or a pytest marker and must not silently skip in the job that exists to run it.
* Known flaky areas: the `kill -9` durability e2e test (`test_crash_recovery.py`) has failed
  intermittently in CI on unmodified `main`, and two Kotlin timing tests
  (`TcpWireTransportTest.listenAccept_twoClients`, `IndexDurabilityIntegrationTest`) have flaked.
  Before blaming your branch, compare the push run and the pull_request run of the same SHA
  (CI builds every commit twice), but read **every** failing job: a recurring failure of the same
  jobs on every run is a broken build, not a flake.
* Benchmarks: see [§4.6](#46-benchmarks). Report B/op, check `uptime`, run alone.

**Docs and releases.**
* User-visible changes append to `docs/releases/unreleased.md`; the version bump renames it to
  `vX.Y.Z.md` (`docs/kdb-release-plan.md` §4). `VERSION` is the only version source and a release
  tag must equal `v$(cat VERSION)`.
* Design docs are numbered per layer (`kdb-spec-layerNN-componentNN-*.md`) and plans
  (`*-plan.md`) record the outcome of each unit; keep them current when you finish one.

---

## 19. Pull requests and CI

The repository history uses short-lived topic branches (`feat/...`, `fix/...`, `test/...`,
`docs/...`) merged by merge commit (`Merge pull request #114 from dbeasty/fix/mesh-merge-convergence`).
Commit subjects follow `type(scope): summary` (`fix(sql): a ranking is not cut at MaxRows before the
residual WHERE`, `feat(ts-client): delete by key`, `docs: ...`). Several people and agents work in
one checkout, so build into your own worktree or scratch directory.

Before pushing, run what CI runs for the tree you touched:

```bash
cd go && go vet ./... && test -z "$(gofmt -l .)" && go test -race ./...    # + (cd go/grpc && go test -race ./...)
make build-go && (cd go && go build -o bin/kdb-e2e-helper ./cmd/kdb-e2e-helper) \
  && (cd kdb-integration/e2e && python -m pytest -v)                        # server-visible changes
./gradlew build --no-daemon                                                 # any Kotlin change
make test-physical-golden                                                   # any format change
```

CI (`.github/workflows/ci.yml`, on every push and pull request):

| Job | Runs | Needs |
|---|---|---|
| `go-test` | `go vet`, `gofmt -l`, `go test -race ./...`, WASM build | - |
| `kotlin-test` | `./gradlew build --no-daemon`; uploads XML reports on failure | - |
| `integration` | `:kdb-integration:test :kdb-integration:e2ePython` | builds Go binaries first |
| `e2e` | `pytest -v` in `kdb-integration/e2e` | `go-test` |
| `e2e-interop` | `pytest -v -m interop` (Go vs JVM CLI, differential) | `e2e`, `kotlin-test` |
| `go-interop` | starts the JVM `kdb-service`, runs `-tags interop` tests; fails on any `SKIP` | `integration`, `go-test` |
| `golden-freshness` | regenerates both sides' fixtures, fails on drift or uncommitted fixtures | - |
| `ts-client` | typecheck, regenerate fixtures and diff, `npm test`, build | - |
| `mobile-android`, `mobile-ios` | `gomobile bind` the `kdbsync` facade; binary sizes to the job summary | `go-test` |

Also: `e2e-nightly.yml` (daily 08:17 UTC: slow and destructive scenarios, S3 against LocalStack),
`benchmark.yml` (weekly JMH), and `release.yml` (on a `v*` tag: guard that the tag equals `VERSION`,
race tests, cross-compiled binaries, the source bundle, jars, `SHA256SUMS` as a GitHub release, and
a GHCR image). A release is reproducible: `make release-verify TAG=vX.Y.Z`.

---

## 20. Troubleshooting and FAQ

**`missing go.sum entry for module providing package golang.org/x/crypto/pbkdf2` (or zstd, aws).**
Your module has not resolved KDB's dependencies. Run `go mod tidy` ([§2.1](#21-using-kdb-from-your-own-go-project)).

**`data directory is in use: <dir>/.kdb.lock` (`stop kdb-service / the embedded runtime first`).**
Another process holds the directory: a running `kdb-service`, another CLI, or an embedded runtime.
The CLI and `kdb-inspect` need exclusive access; a service that was just sent SIGTERM keeps the lock
until it finishes draining. After a crash the OS drops the `flock`; `kdb unlock` removes only a
leftover file whose recorded PID is dead.

**`config file ...: json: unknown field "x"`.** Config keys are strict; the names are the camelCase
keys in [§14.1](#141-kdb-service-flags). `--config` is overridden by `KDB_*` and by explicit flags.

**`grpc` flag refused.** `--grpc-addr` only works in the `kdb-service-grpc` build.

**My role has `read:*` but reads are denied.** Use a namespace prefix such as `read:myapp/*`
(observed, [§13](#13-auth-tokens-and-rbac)). The control plane's process endpoints also need
`admin:control`.

**`CREATE INDEX` or `MATCH` fails on my in-process server.** No index provider is installed; call
`srv.OpenIndexes(...)` or install one ([§15.1](#151-index-stores)). `kdb-service` installs one.

**`SELECT ... AT TIME '...'` fails with `unexpected trailing input`.** The wire SQL parser does not
take the version clause; use `client.History`, `embed.DocumentAt` or `kdb get --at`.

**The docs say `UPDATE`/`DELETE`/`GROUP BY` do not exist in Go.** They are stale; the parser accepts
them ([Known documentation drift](#known-documentation-drift)).

**A write succeeded on node A but is not on node B.** Replication is asynchronous: B pulls on its
`interval` (2-30 s). Use `client.Session` for read-your-writes across replicas, or single-home
namespaces if you need linearisable writes.

**The service exits with status 75.** The abort watchdog (`--abort-after`) or a control-plane
promotion asked a supervisor to restart it; this is intentional. Run with `--restart=on-failure`
or `Restart=on-failure`.

**Writes are slow on macOS.** The default `--sync-mode=full` uses `F_FULLFSYNC`. For development
`--sync-mode fast` or `--durability async` is much cheaper, but do not compare benchmarks across
modes, and remember that on Linux the two differ only modestly (`fdatasync` vs `fsync`).

**A copied data directory refuses to sync.** Two nodes with one `NODE` identity are refused at the
peer handshake; delete `NODE` in the clone first.

**Where is the authoritative description of X?** The code; then `docs/kdb-lld-*.md`
(implementation reference), the numbered layer specs (design intent), and the plans
(`docs/kdb-open-issues-plan.md`, `docs/kdb-finish-up-plan.md`).

### Known documentation drift

Found while writing this guide; the code is right and the text is stale. Worth a documentation PR:

* `docs/kdb-lld-query.md` (§2 grammar, §7 items 4-6 and 12, §8 table) and the SQL table in
  `docs/kdb-user-guide.md` say the Go parser has no `UPDATE`, `DELETE`, `GROUP BY`, aggregates other
  than `COUNT`, `LIKE`, `IN`, `BETWEEN`, `CREATE INDEX` or `BEGIN`. `go/kdb/sql/parser.go` implements
  all of them, and they run against a v0.6.2 server (§10.1). The same parser also takes `MATCH`,
  `SIMILARITY` and `FUSE`.
* `kdb-service --help` describes `--rbac` as in-memory with "no admin SQL surface yet"; with
  `--data-dir` the registry is durable and `kdb-service user ...` manages it.
* `docs/kdb-lld-protocol.md` §2 lists message types only to `0x1C` and §6.1 omits `UNSUPPORTED`,
  `NAMESPACE_MISMATCH`, `INTEGRITY` and `NOT_HOME`; `go/kdb/wire/types.go` and `error_code.go` go to
  `0x3F` and include them.
* The user guide's note that a unique violation shows `UNIQUE_VIOLATION` in the message: the message
  observed was `UNIQUE_CONSTRAINT (value already held by document ...)`; `UNIQUE_VIOLATION` is the
  wire `errorCode`.

---

## 21. What was and was not verified

Run on macOS arm64, Go 1.26.3, JDK 21, against the working tree at `b039218` (plus other sessions'
uncommitted changes), with binaries rebuilt by `make build-go`.

* **Compiled, vetted and executed:** every Go program in this guide (§5.3, §7.1, §7.3, §8, §9,
  §12.7, §15.1-15.4), plus a DSN matrix, a WebSocket client and a `gofmt`/`go vet` pass. §5.3, the §8
  program and §12.7 ran against a real `kdb-service` process; the rest are self-contained.
* **Executed:** the CLI sequences in §5.1 and §11.2; `kdb-service` with and without `--config`, RBAC
  bootstrap, `--ws-addr`, `--control-addr` (three endpoints), two-node `--peer` replication;
  `kdb-inspect verify|backup|backup-list|dump-wire`; `make build-go` and `kdb --version`;
  `go test ./kdb/wire ./kdb/driver ./kdb/client ./kdb/script ./kdb/interop`; `go vet`; one e2e file
  (`test_server_lifecycle.py`, 4 passed); `BenchmarkFileBackedUpsert` at `-benchtime 50x` to prove the
  command (numbers came from a loaded machine and are not reported).
* **Read from code or CI but not run:** every Gradle/Kotlin command (§3.3, §4.2), `make test-go` and
  `make test-physical-golden` in full, the `interop`, `s3integration` and mobile jobs, the Docker
  build, `make release*`, the TypeScript commands, stream-mode and peer-sync-v2 flows, `FUSE`,
  composite `UNIQUE (a, b)`, `DROP INDEX`, `kdb sync|conflicts|resolve|scrub|peer-diff|deepen`, and
  the control plane's mutating endpoints.
* **Side effect:** `make build-go` was run in the shared checkout and replaced `go/bin/*` with
  binaries built from the working tree at that moment (`kdb --version` reports `commit b039218-dirty`).
