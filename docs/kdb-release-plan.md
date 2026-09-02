# KDB Release Plan

How a KDB release is cut, what it contains, and how anyone can reproduce it byte-for-byte from
the tag alone.

Implemented and verified end to end (see §5 for what each item is and how it was checked):
the `go/embedbundle` generator, the `make release*` targets, the reproducibility fixes, and
`.github/workflows/release.yml` calling those same targets. No tag has ever been pushed
(`git tag` is empty) — `v0.1.0` will be the first.

---

## 0. Quick usage

```bash
make release            # Go only: binaries + embeddable source bundle + checksums (default)
make release-kotlin     # Kotlin jars only
make release-all        # everything, one SHA256SUMS covering all of it
make bundle-verify       # build the bundle and prove it compiles/vets/tests/cross-compiles alone
make release-verify      # prove the Go side rebuilds byte-for-byte (add TAG=vX.Y.Z for a pushed tag)
make release-clean       # rm -rf dist
```

Everything lands in `dist/`:

```
dist/
  bin/       kdb-linux-amd64, kdb-service-darwin-arm64, kdb-inspect-linux-arm64, ...  (9 files)
  bundle/    kdb-go-embed-<version>.zip
  jars/      kdb-embed-jvm-<version>.jar, kdb-cli-<version>.jar, ...                  (55 files)
  SHA256SUMS
```

`VERSION`, `GIT_COMMIT` and `RELEASE_DATE` all have sane defaults (current `VERSION` file,
`git rev-parse HEAD`, the commit's own timestamp) and can be overridden, which is how CI pins
them to the tag: `make release-all VERSION=0.1.0 GIT_COMMIT=$GITHUB_SHA GIT_DIRTY=false
RELEASE_DATE=$(git log -1 --format=%cI $GITHUB_SHA)`.

---

## 1. What a release contains

| Artifact | Name | Produced by |
|---|---|---|
| Go binaries | `kdb`, `kdb-service`, `kdb-inspect` × `linux/amd64`, `linux/arm64`, `darwin/arm64` | `make release-binaries` |
| **Embeddable Go source** | `kdb-go-embed-<version>.zip` | `make release-bundle` |
| Kotlin jars | one per Gradle module, `<module>-<version>.jar` | `make release-kotlin` |
| Container image | `ghcr.io/limidus/kdb/kdb-service:<tag>` | `docker/build-push-action` (unchanged) |
| Checksums | `dist/SHA256SUMS` over everything built so far | `make release-checksums` |

The governing rule for every one of these: **the workflow contains no build logic.** Each CI step
in `release.yml` is a single `make` target that a developer runs identically on a laptop — that's
what makes `make release-verify TAG=v0.1.0` a real reproduction of CI's output, not a separate,
possibly-diverging path.

Kotlin jars carry only the compiled classes, not `-sources.jar` — no plain-jvm module in this
tree has a `sourcesJar` task registered (only Kotlin Multiplatform modules auto-register
per-target ones like `jvmSourcesJar`), and wiring that up project-wide is a real Gradle change,
not a release-script concern. Tracked as **R5b** below.

---

## 2. The embeddable Go source bundle

### 2.1 What it is for

A downstream Go project vendors the KDB engine into its own tree and links it in-process — no
service, no wire protocol, no CLI. The bundle is therefore **only the packages an in-process
consumer can actually reach**, and it must be a self-contained, compiling, testable Go module on
its own.

### 2.2 Scope — derived, never hand-maintained

A hand-written file list rots on the first import added. The bundle's contents are computed at
release time by `go/embedbundle` (a small Go program, run as `go run ./embedbundle`) from a
declared set of **entry points**, in `go/embedbundle/entrypoints.txt`:

```
kdb/embed         # embedded runtime: open, put, commit, materialize
kdb/driver        # database/sql driver over that runtime
kdb/sql           # KDB-SQL parser, planner, executor
kdb/query/hybrid  # hybrid query engine
kdb/index         # index core
```

`go list -deps` over those entry points yields the transitive closure — **26 of the module's 35
top-level packages**:

```
kdb/auth          kdb/codec         kdb/codec/schema  kdb/compression
kdb/dag           kdb/document      kdb/driver        kdb/embed
kdb/error         kdb/index         kdb/json          kdb/metrics
kdb/policy        kdb/query/hybrid  kdb/schema        kdb/sql
kdb/storage       kdb/storage/delta kdb/storage/engine
kdb/storage/io    kdb/storage/io/s3 kdb/storage/mem   kdb/storage/memtable
kdb/storage/sstable kdb/storage/wal kdb/transaction
```

Excluded, because nothing an embedder imports reaches them: `kdb/server`, `kdb/client`,
`kdb/wire`, `kdb/transport`, `kdb/peersync`, `kdb/backup`, `kdb/recovery`, `kdb/integrity`,
`kdb/inspect`, `kdb/interop`, `kdb/compaction`, `kdb/file`, `kdb/config`, `kdb/tier`,
`kdb/version`, and all of `cmd/` and `wasm/`.

Two rules the generator follows, both of which a naive implementation gets wrong:

- **Copy whole package directories, not the file list `go list` returns.** `go list` on one
  GOOS/GOARCH silently drops build-tagged siblings — `kdb/embed/dir_lock_other.go`,
  `kdb/storage/system_memory_linux.go`, `kdb/storage/io/sync_other.go`. Copying the directory
  keeps every platform's files.
- **Compute the closure as a union across GOOS/GOARCH** (`linux/amd64`, `linux/arm64`,
  `darwin/arm64`, `windows/amd64`, `js/wasm` — `go/embedbundle/main.go`'s `platforms` list), so a
  package imported only on a platform the release host isn't cannot be missed.

`_test.go` files are included. They cost little (229 `.go` files, 1.5 MB unpacked) and they are
the bundle's own acceptance gate — `make bundle-verify` running `go test ./...` inside the
extracted bundle is what proves the extraction is complete. No package in the closure reads
`go/testdata/` (only `kdb/interop` does, and it's excluded), so no test fixtures travel with it.

### 2.3 Bundle contents

```
kdb-go-embed-<version>/
  go.mod                # module path unchanged; requires pruned by `go mod tidy`
  go.sum
  LICENSE
  EMBEDDING.md          # generated: how to consume, entry points, what's included/excluded
  bundle.json           # version, commit, build date, entry points, package list, per-file sha256
  bundleinfo.go         # package root: BundleVersion/BundleCommit/BundleBuildDate consts, so a
                         # consumer can report which drop it vendored without parsing bundle.json
  rewrite-module.sh     # optional: re-home the bundle under a different module path
  kdb/...               # the 26 packages
```

`go.mod` keeps the module path `github.com/limidus/kdb/go`, so **no import path in any copied
file is rewritten** — the bundle is the same code, not a transformed copy. Two edits are applied:

- Drop the `tool golang.org/x/mobile/cmd/gobind` directive, which drags `x/mobile`, `x/mod` and
  `x/tools` into the module graph for a tool the bundle has no use for.
- Run `go mod tidy` inside the staged bundle. This prunes the requires down to what the closure
  actually needs — `aws-sdk-go-v2` (+`config`, `credentials`, `s3`), `klauspost/compress`,
  `x/crypto`, and 14 indirects. `golang.org/x/sync`, `x/mobile`, `x/mod` and `x/tools` all fall
  away.

The staging directory is scratch: after the zip is written, `go/embedbundle` deletes it, so
`dist/bundle/` only ever holds the `.zip` — nothing loose duplicating the archive's contents in
the checksums or the release upload.

### 2.4 How a consumer embeds it

Documented in the generated `EMBEDDING.md`:

```bash
unzip kdb-go-embed-0.1.0.zip -d third_party/
go mod edit -replace github.com/limidus/kdb/go=./third_party/kdb-go-embed-0.1.0
go mod tidy
```

```go
import (
    "database/sql"
    _ "github.com/limidus/kdb/go/kdb/driver"
)
```

`rewrite-module.sh` ships alongside it for projects that would rather re-home the code under
their own module path (`go mod edit -module` plus a whole-prefix import rewrite). It's a
convenience, not the supported path — the `replace` directive is.

### 2.5 Validated, not assumed

`make bundle-verify` runs this for real every time; the checks below were also run manually
against the generator's first output, from two independent output directories to rule out any
built-in-path leakage:

| Check | Result |
|---|---|
| `go build ./...` in the extracted bundle | OK |
| `go vet ./...` | OK |
| `go test ./...` (26 packages) | all `ok`, no failures |
| Cross-compile `linux/amd64`, `linux/arm64`, `darwin/arm64` (release platforms) | OK |
| Cross-compile `windows/amd64`, `js/wasm` (closure-coverage check) | OK |
| Two independent runs, different output paths, one second apart | byte-identical zip (see §3) |
| Size | 229 files, 1.5 MB unpacked, ~390 KB zipped |

### 2.6 Known wart — the AWS SDK

`kdb/embed` imports `kdb/storage/io/s3` unconditionally (`file.go`, `segment_store.go`,
`storage_options.go`), so ~100 AWS SDK packages land in the dependency graph of every embedder,
including ones that will never touch S3. Fixing it means putting the S3 segment store behind a
build tag with a no-op fallback — a real code change to `kdb/embed`, out of scope here. Tracked as
work item **R8** below; the bundle ships correctly either way, just heavier than it needs to be.

---

## 3. The reproducibility contract

Given a commit, anyone can rebuild the Go side of a release and get **identical sha256 sums** —
verified end to end by `scripts/release-verify.sh`: it snapshots the source into two independent
temp copies (via `git archive` for a tag, or `rsync` of the working tree with no tag given), runs
`make release-go` in each with the same `GIT_COMMIT`/`RELEASE_DATE`/`VERSION`, and diffs
`dist/SHA256SUMS`. Confirmed reproducible against this tree.

Three things used to break that, all now fixed:

1. **`BUILD_DATE=$(date -u +…)` changed every run.** `release-binaries` and `release-bundle` now
   use `RELEASE_DATE`, which defaults to the target commit's own timestamp
   (`git log -1 --format=%cI $(GIT_COMMIT)`), not "now" — a release build's declared build date
   has to be a property of the commit, not of when someone happened to run `make`.
2. **No `-trimpath`, no `-buildid=`.** `release-binaries` now passes `-trimpath` and
   `-ldflags "... -buildid="`, stripping the builder's absolute source paths and the path-derived
   build ID Go otherwise embeds. Verified: two builds of the same commit from different absolute
   directories now produce identical binaries; without these flags they didn't.
3. **`go-version-file: go/go.mod` floats to the latest `1.26.x`.** `go/go.mod` now pins
   `toolchain go1.26.3`; `go build` auto-downloads that exact patch if the host has an older one
   (`GOTOOLCHAIN=auto`, the default), so the toolchain is part of the tagged source instead of
   whatever happened to be current on release day.

The zip archive itself is deterministic by construction: `go/embedbundle` writes entries with
`archive/zip` directly, in sorted path order, with a fixed `Modified` time from `RELEASE_DATE`
and fixed 0644/0755 modes — not `zip(1)`, which gives neither guarantee. Verified: two generator
runs one second apart, from different absolute output paths, produced a byte-identical archive.

Kotlin jars are **not yet** asserted reproducible — `build.gradle.kts` already keeps build
timestamps out of the manifest, but jar entry ordering and `isPreserveFileTimestamps` aren't
pinned, and `release-verify` only covers `release-go`. Tracked as **R5c**.

---

## 4. Tag and version flow

`VERSION` is the single source of truth; the Go ldflags and the jar manifests already read it.
The tag must agree with it — `.github/workflows/release.yml`'s `version-guard` job now fails the
whole run unless `v$(cat VERSION)` equals `$GITHUB_REF_NAME`, before anything else runs.

Cutting a release is still a manual `VERSION` bump + commit + `git tag -a vX.Y.Z && git push
origin vX.Y.Z` — no `cut-release.sh` helper exists yet (open item, not blocking; see §7).

---

## 5. Work items

Sized S/M/L on the finish-up plan's scale. **Done** items were implemented and verified as
described in this document, not just planned.

- **R1 — `go/embedbundle` generator. DONE.** `go/embedbundle/main.go` + `entrypoints.txt`: reads
  the entry points, computes the union closure across the five platforms in §2.2, copies whole
  package directories, writes `go.mod`/`go.sum`/`LICENSE`/`bundleinfo.go`/`bundle.json`/
  `EMBEDDING.md`/`rewrite-module.sh`, runs `go mod tidy` in the staged tree, zips deterministically,
  then deletes the staging directory. Lives in the main module but is excluded from the bundle's
  own closure (nothing in the closure imports it).
  *Verified:* the checks in §2.5.

- **R2 — Bundle acceptance gate. DONE.** `make bundle-verify` → `scripts/verify-bundle.sh`:
  unzips to a temp dir and runs `go build`/`go vet`/`go test`/cross-compile with nothing but
  what's inside the zip — no access to the rest of the repo.
  *Verified:* ran clean against the current tree (§2.5). Not yet exercised against a
  deliberately-broken closure (e.g. an import from `kdb/embed` reaching `kdb/server`) to confirm
  it fails loudly — worth a one-off manual check before the first real tag.

- **R3 — Reproducibility fixes. DONE.** `RELEASE_DATE` from the commit, `-trimpath` +
  `-ldflags -buildid=`, pinned `toolchain go1.26.3` in `go/go.mod`, deterministic zip.
  *Verified:* §3's byte-identical rebuild.

- **R4 — Makefile release targets. DONE.** `release`, `release-go`, `release-bundle`,
  `bundle-verify`, `release-binaries`, `release-kotlin`, `release-checksums`, `release-all`,
  `release-verify`, `release-clean` — root `Makefile`. One subtlety: `release-checksums` is
  invoked via a recursive `$(MAKE) release-checksums` from `release-go` and `release-all`'s
  recipes, not listed as an ordinary prerequisite — Make only remakes a phony target once per
  invocation, so a plain prerequisite reference from `release-all` (after `release-go` already
  triggered it) would silently no-op and miss whatever `release-kotlin` added. Caught by testing,
  not by inspection.

- **R5 — Kotlin jars in the release. DONE (compiled classes only).**
  `scripts/collect-kotlin-jars.sh`: reads the module list from `settings.gradle.kts` (not a glob
  over the repo — a glob would also sweep in stray build output from `.claude/worktrees/`
  checkouts) and copies each module's versioned `build/libs/*-<version>.jar`, skipping
  `*-metadata-<version>.jar` (Kotlin Multiplatform's non-consumable common-metadata jars).
  Depends on `./gradlew jar`, not `./gradlew build` — `build` is broken on a clean checkout
  (`docs/kdb-finish-up-plan.md`); `jar` alone is green.
  *Verified:* `make release-kotlin` from clean → 55 jars in `dist/jars/`.
  **Found and fixed in the process:** `:kdb-compute` (a Kotlin Multiplatform module) and
  `:kdb-compute-jvm` (a separate plain-JVM module) both resolved to the jar filename
  `kdb-compute-jvm-<version>.jar` — the KMP module via Kotlin's `<project>-<target>` naming
  convention, the plain module via its own project name. The collector now fails loudly on any
  such collision instead of silently overwriting one artifact with the other (which is what a
  naive `find ... | xargs cp` would have done); `kdb-compute-jvm/build.gradle.kts` now sets
  `archiveBaseName.set("kdb-compute-jvm-adapter")` to disambiguate. Nothing else in the Gradle
  graph depends on `:kdb-compute-jvm` by project reference, so this was safe to rename.
  - **R5b — sources jars.** Not done. No plain-`kotlin.jvm` module in this tree has a
    `sourcesJar` task; only Kotlin Multiplatform modules auto-register per-target ones
    (`jvmSourcesJar`, `jsSourcesJar`, ...). Wiring `java { withSourcesJar() }` (or equivalent)
    across every JVM-producing module is a real build change, not a release-script concern —
    deferred.
  - **R5c — jar reproducibility.** Not asserted. `release-verify` only covers `release-go`.

- **R6 — `release.yml` rewrite. DONE.** `version-guard` → `test-gate` (`go test -race`, `go vet`,
  `gofmt`) → `artifacts` (`make release-all` with Go+Java toolchains, `scripts/verify-bundle.sh`,
  upload `dist/bin/* dist/bundle/*.zip dist/jars/* dist/SHA256SUMS`) → `image` (unchanged, GHCR
  push). Every artifact-producing step is one `make` call or one script call.
  *Not yet verified against real CI* — no tag has been pushed. The commands are identical to
  what was run and verified locally in §2.5/§3, but GitHub Actions' Ubuntu runner, network
  conditions and Go/Gradle cache state are still an untested variable.

- **R7 — `scripts/release-verify.sh` / `make release-verify`. DONE.** Two independent clean
  snapshots (via `git archive $TAG`, or `rsync` of the working tree when no `TAG` is given),
  `make release-go` in each, diff `dist/SHA256SUMS`.
  *Verified:* `./scripts/release-verify.sh` (no-tag mode) against this tree → reproducible.
  *Not yet verified:* tag mode (`TAG=vX.Y.Z`) — untestable without a committed, tagged ref to
  point `git archive` at; exercise it against the first real tag.
  Not wired into a scheduled workflow yet (the plan's original suggestion) — open item, not
  blocking.

- **R8 — Optional: build-tag the S3 backend.** M, not started. Put `kdb/storage/io/s3` behind
  `//go:build kdb_s3` with a no-op fallback, dropping ~100 AWS packages from an embedder's graph.
  Independent of the release pipeline; do after `v0.1.0` ships.

- **R9 — Docs.** Partial. This document exists and is current. Still open: a "Releases" pointer
  in `README.md`'s documentation table, and an embedding section in `docs/kdb-user-guide.md`.

- **R10 — `scripts/cut-release.sh`.** Not started. A `VERSION` bump + `CHANGELOG.md` update +
  commit + annotated tag + push helper, so cutting a release isn't five manual git commands.
  Open item from §4, not blocking — the workflow's `version-guard` catches a mismatch either way.

---

## 6. Exit criteria

- `git tag -a v0.1.0 && git push origin v0.1.0` produces a GitHub release carrying binaries for
  three platforms, `kdb-go-embed-0.1.0.zip`, the jars, and `SHA256SUMS`. **Not yet exercised** —
  no tag pushed.
- `make release-verify TAG=v0.1.0` on a different machine reports the Go side matching. **Not yet
  exercised** for the same reason; the no-tag mode covering the same code path has been.
- A scratch Go project consuming only the zip compiles and runs a `database/sql` round-trip
  against a file-backed runtime, with no network access to this repository. **Not yet done** —
  `make bundle-verify` proves the bundle builds/tests/cross-compiles standalone, which is close
  but doesn't drive it through an actual external consumer module.
- The version guard rejects a tag that disagrees with `VERSION`. **Implemented, not yet exercised
  against a real mismatched tag push.**

---

## 7. Open decisions

1. **Bundle scope.** This plan takes the full in-process surface — engine + `database/sql` driver
   + KDB-SQL + hybrid query + index (26 packages). Dropping `kdb/sql`, `kdb/query/hybrid` and
   `kdb/index` gives a 21-package KV/document-only bundle. Note that `kdb/driver` does **not**
   import `kdb/sql`, so the smaller bundle is coherent — but it is a database whose embedders
   cannot write SQL. Full is the default; editing `go/embedbundle/entrypoints.txt` is the whole
   change needed to switch.
2. **Jar distribution.** Attached to the GitHub release (what's implemented) versus published to
   GitHub Packages or Maven Central. Publishing needs `maven-publish` wiring and, for Central,
   signing keys and a namespace — deferred to the parity track unless there is a consumer waiting.
3. **Pre-1.0 stability.** `0.x` says the embedding API can break between releases. Stated
   explicitly in the generated `EMBEDDING.md` so a downstream project pins a version rather than
   tracking `main`.
