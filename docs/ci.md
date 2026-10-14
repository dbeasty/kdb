# CI: what runs where, and how to run it locally

PR checks (`.github/workflows/ci.yml`) are sized to finish in a few minutes. The full suites run
every night (`.github/workflows/e2e-nightly.yml`, also startable by hand: Actions → E2E Nightly →
Run workflow). Nothing was dropped: what a PR skips, the nightly runs.

## The split

| | PR (`ci.yml`) | Nightly (`e2e-nightly.yml`) |
|---|---|---|
| Go, Linux, `-race` | `go-test (<shard>)`: six shards, `-short`, test cache from main | `go-race-full`: same shards, no `-short`, `-count=1` |
| Go, Windows | `go-windows (<shard>)`: three shards, `-short` | `go-windows-full`: no `-short`, `-count=1`, Kotlin interop included |
| Kotlin | `kotlin-test (jvm / js / native)`: `build`'s tasks per target, Gradle build cache | `kotlin-build`: plain `./gradlew build` |
| kill -9 | 1 iteration (`TestServerKill9…`), `TestIndexSnapshotsSurviveKill9` skipped | 3 iterations in `go-race-full`, plus the 150/60-iteration `kill9-soak` |
| Go↔Kotlin interop | one case (`TestKotlinPutThenGoGet_InteropDelta`), plus `go-interop` and `e2e-interop` | every `TestKotlin*` in `kdb/integration`, Linux and Windows |
| pytest e2e | `e2e (1..3)`: the default selection dealt over three jobs | `e2e-slow`, `e2e-upgrade` |

## The `-short` convention

`testing.Short()` marks what a PR leaves to the nightly. A test that is slow because it measures
something at scale (heap per document, open cost per commit, allocation bounds), soaks, or drives the
Kotlin CLI through gradle skips under `-short`. A randomized reference test runs fewer seeds or sizes.
Use the same pattern for a new test that takes more than a few seconds under `-race` on a CI runner
(about twice an M-series laptop):

```go
if testing.Short() {
	t.Skip("heap measurement; runs nightly, or without -short")
}
```

## Running it locally

From `go/`:

```bash
go test ./...                 # everything (what the nightly runs, minus -race)
go test -race ./...           # everything under the race detector (make test-go)
go test -short -race ./...    # what a PR runs
```

The Go shards, exactly as CI runs them:

```bash
scripts/ci/go-test-shard.sh list linux-race        # what each shard runs
scripts/ci/go-test-shard.sh linux-race embed -short
scripts/ci/go-test-shard.sh windows rest -short
```

Kotlin, per target as the PR jobs run it, or whole as the nightly does:

```bash
./gradlew jvmTest test allMetadataJar --build-cache --parallel
./gradlew jsTest jsJar jsBrowserDistribution --build-cache --parallel
./gradlew linuxX64Test --build-cache --parallel
./gradlew build
```

One e2e shard (from `kdb-integration/e2e`, after `make build-go`):

```bash
python -m pytest --shard 2/3
```

## Rebalancing the shards

Each Go shard uploads its `go test -json` as an artifact (`go-test-json-*`). When a shard grows
well past its siblings, move packages between shards, or split a package by test name, in
`scripts/ci/go-test-shard.sh`. A package no shard names lands in `rest` automatically.

## Caches

- Go: `.github/actions/setup-go-cached` restores `GOCACHE` and `GOMODCACHE` per job. Only pushes to
  main save them. PR runs read main's newest entry, so a PR re-tests only the packages its change
  reaches. `kdb/integration` and `cmd/kdb-inspect` always run with `-count=1`, because their tests
  build or run code Go's test cache cannot see.
- Gradle: `gradle/actions/setup-gradle` keeps the Gradle user home, build cache included. Main
  writes it and PRs read it. The jar manifests' commit attributes are excluded from runtime
  classpath normalization (`build.gradle.kts`), so an untouched module's tests stay cached across
  commits.
