.PHONY: test-go test-kotlin test-cross build-go build-kotlin bench bench-write print-version \
        release release-go release-bundle bundle-verify release-binaries release-kotlin \
        release-checksums release-all release-verify release-clean

# Single version source (see go/kdb/version). Release tags override: make build-go VERSION=v1.2.3
VERSION ?= $(shell cat VERSION)
# The commit the binary is built from, so any shipped binary can be traced back to its source.
# Full SHA, not the short form - short SHAs stop being unique as a repo grows. GIT_DIRTY records
# whether the tree had uncommitted changes, because then the commit alone doesn't identify it.
VERSION_PKG := github.com/limidus/kdb/go/kdb/version
GIT_COMMIT ?= $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
GIT_DIRTY ?= $(shell test -n "$$(git status --porcelain 2>/dev/null)" && echo true || echo false)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
GO_LDFLAGS := -X $(VERSION_PKG).Version=$(VERSION) \
              -X $(VERSION_PKG).Commit=$(GIT_COMMIT) \
              -X $(VERSION_PKG).Dirty=$(GIT_DIRTY) \
              -X $(VERSION_PKG).BuildDate=$(BUILD_DATE)

# --- Release (docs/kdb-release-plan.md) -------------------------------------------------------
#
# `make release`         Go artifacts only: binaries + the embeddable source bundle (default).
# `make release-kotlin`  Kotlin jars only.
# `make release-all`     everything, plus one SHA256SUMS covering all of it.
# `make release-verify`  proves the Go side rebuilds byte-for-byte (optionally: TAG=vX.Y.Z).
#
# Every artifact lands in dist/. RELEASE_DATE defaults to the target commit's own timestamp
# (not "now", unlike BUILD_DATE above) - a release build must be reproducible, so its declared
# build date has to be a property of the commit, not of when someone happened to run `make`.
DIST := dist
RELEASE_DATE ?= $(shell git log -1 --format=%cI $(GIT_COMMIT) 2>/dev/null || date -u +%Y-%m-%dT%H:%M:%SZ)
RELEASE_LDFLAGS := -X $(VERSION_PKG).Version=$(VERSION) \
                    -X $(VERSION_PKG).Commit=$(GIT_COMMIT) \
                    -X $(VERSION_PKG).Dirty=$(GIT_DIRTY) \
                    -X $(VERSION_PKG).BuildDate=$(RELEASE_DATE) \
                    -buildid=
RELEASE_PLATFORMS := linux/amd64 linux/arm64 darwin/arm64
SHA256 := $(shell command -v sha256sum >/dev/null 2>&1 && echo sha256sum || echo "shasum -a 256")

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

# Always -benchmem: the engine's single largest allocation (a throwaway zstd
# encoder per commit, ~21MB) survived for as long as it did because no benchmark
# reported B/op and none covered the real file-backed write path. ns/op alone
# would not have caught it - the allocation barely moved the latency.
bench:
	cd go && go test ./... -run '^$$' -bench . -benchmem

# The write path specifically, which is what regressions here tend to hit.
# BenchmarkFileBackedUpsert goes through the real disk-backed runtime end to end
# (schema, staging, commit trie, DAG, delta log, fsync).
bench-write:
	cd go && go test ./kdb/server/ ./kdb/transaction/ ./kdb/storage/engine/ ./kdb/compression/ \
		-run '^$$' -bench . -benchmem -benchtime 1000x

build-go:
	cd go && go build -ldflags "$(GO_LDFLAGS)" -o bin/kdb ./cmd/kdb
	cd go && go build -ldflags "$(GO_LDFLAGS)" -o bin/kdb-service ./cmd/kdb-service
	cd go && go build -ldflags "$(GO_LDFLAGS)" -o bin/kdb-inspect ./cmd/kdb-inspect

build-kotlin:
	./gradlew build --no-daemon

# What the binaries in bin/ will claim to be. Handy for checking the injection before a release.
print-version:
	@echo "version:    $(VERSION)"
	@echo "commit:     $(GIT_COMMIT)"
	@echo "dirty:      $(GIT_DIRTY)"
	@echo "build date: $(BUILD_DATE)"

release-clean:
	rm -rf $(DIST)

# Default release target: Go only. Most consumers of a release want the binaries and/or the
# embeddable source, not the Kotlin jars - `make release-kotlin` is separate and additive.
release: release-go

# release-checksums runs via a recursive $(MAKE) call, not as a plain prerequisite: Make only
# remakes each phony target once per invocation, so if it were a prerequisite here *and* listed
# again under release-all below, the second reference would silently no-op and dist/SHA256SUMS
# would miss whatever release-kotlin added. A sub-make always actually runs.
release-go: release-bundle release-binaries
	$(MAKE) release-checksums

# The embeddable Go source bundle (docs/kdb-release-plan.md §2): the transitive dependency
# closure of kdb/embed, kdb/driver, kdb/sql, kdb/query/hybrid and kdb/index
# (go/embedbundle/entrypoints.txt), zipped deterministically so the archive itself reproduces
# byte-for-byte (see release-verify below).
release-bundle:
	cd go && go run ./embedbundle \
		-version "$(VERSION)" -commit "$(GIT_COMMIT)" -date "$(RELEASE_DATE)" \
		-out "../$(DIST)/bundle"

# The bundle's own acceptance gate: unzips it and builds/vets/tests/cross-compiles it standalone,
# with nothing but what's inside the zip (docs/kdb-release-plan.md §2.5/R2).
bundle-verify: release-bundle
	./scripts/verify-bundle.sh $(DIST)/bundle/kdb-go-embed-$(VERSION).zip

# Cross-compiled binaries for every RELEASE_PLATFORMS entry. -trimpath strips the builder's
# absolute source paths out of the binary; -buildid= (in RELEASE_LDFLAGS) strips the path-derived
# build ID Go embeds by default. Without both, two builds of the identical commit differ by
# whatever directory each one happened to run in, even with everything else pinned.
release-binaries:
	mkdir -p $(DIST)/bin
	cd go && for target in $(RELEASE_PLATFORMS); do \
		GOOS=$${target%/*}; GOARCH=$${target#*/}; \
		for bin in kdb kdb-service kdb-inspect; do \
			CGO_ENABLED=0 GOOS=$$GOOS GOARCH=$$GOARCH \
				go build -trimpath -ldflags "$(RELEASE_LDFLAGS)" \
				-o "../$(DIST)/bin/$${bin}-$${GOOS}-$${GOARCH}" "./cmd/$${bin}" || exit 1; \
		done; \
	done

# Kotlin jars, one per Gradle module, collected by scripts/collect-kotlin-jars.sh. Deliberately
# depends on the `jar` task directly, not `build` - `./gradlew build` is broken on a clean
# checkout (docs/kdb-finish-up-plan.md); `jar` alone is green and is all a release needs.
release-kotlin:
	./gradlew jar --no-daemon
	./scripts/collect-kotlin-jars.sh "$(VERSION)" "$(DIST)/jars"

# Checksums over whatever has been built into dist/ so far - release-go alone, or release-go +
# release-kotlin under release-all. Re-run any time after adding an artifact; it always reflects
# the full current contents of dist/.
release-checksums:
	cd $(DIST) && find . -type f ! -name SHA256SUMS | sort | sed 's|^\./||' | xargs $(SHA256) > SHA256SUMS
	@echo "wrote $(DIST)/SHA256SUMS ($$(wc -l < $(DIST)/SHA256SUMS | tr -d ' ') files)"

release-all: release-go release-kotlin
	$(MAKE) release-checksums

# Proves the Go side of dist/ is reproducible: rebuilds it from two independent clean copies of
# the source and diffs the checksums. Pass TAG=vX.Y.Z to check a pushed tag via `git archive`;
# without one, checks the current working tree instead (see scripts/release-verify.sh).
release-verify:
	./scripts/release-verify.sh $(TAG)
