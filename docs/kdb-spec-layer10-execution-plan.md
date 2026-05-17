# KDB Layer 10 — Implementation Execution Plan

**Status:** Implemented (first Kotlin cut — May 2026)  
**Master spec:** `docs/kdb-spec-v0_9.md` §16.1 (Layer 10), §0 (session state)  
**Depends on:** Layer 9 complete (Components 25–28); Component 31 inspect already landed

-----

## Scope

| Component | Module | Spec file |
|---|---|---|
| 31 Inspect (done) | `:kdb-inspect` | `kdb-spec-layer10-component31-inspect-tooling.md` |
| 29 CLI | `:kdb-cli` | `kdb-spec-layer10-component29-cli.md` |
| 30 Integration Test Suite | `:kdb-integration` | `kdb-spec-layer10-component30-integration-test-suite.md` |

-----

## Normative implementation order

### Phase 1 — CLI (29)

1. Create `:kdb-cli` (JVM).
2. Implement `KdbCli.run`, `openCliRuntime`, commands `init`, `put`, `get`, `query`, `log`, `status`.
3. Optional: `sync` over `memory://` and `kdb-tcp://`.
4. **Tests:** cases 1–8, 10 from spec §7.
5. Paste public interface into master §17 → Layer 10 (Component 29).

**Exit criteria:** `./gradlew :kdb-cli:test` green; `java -jar` or Gradle `runCli` prints help.

### Phase 2 — Integration suite (30)

1. Create `:kdb-integration` (test-only JVM module).
2. Implement `IntegrationFixture` + scenarios 1–10.
3. **Run:** `./gradlew :kdb-integration:test`.

**Exit criteria:** All scenarios pass; Layer 7–9 tests still pass.

### Phase 3 — Master spec

1. Update §0: Layer 9 `[x]`, Layer 10 `[x]` for 29–30.
2. Update §16.1 Layer 10 block.
3. Update §14 cumulative (~131,850 + Layer 10 ~3,200).
4. Final §17 Layer 10 interfaces.

-----

## Verification checklist

- [x] `./gradlew :kdb-cli:test`
- [x] `./gradlew :kdb-integration:test`
- [x] Layer 9 transport/compute tests still pass
- [x] `./gradlew :kdb-peer-sync:jvmTest :kdb-jdbc:test`
- [x] Master spec §0 Layer 10 checklist updated

-----

## Estimated NBNC (Layer 10)

| Component | Lines |
|---|---|
| 29 CLI | ~1,800 |
| 30 Integration | ~1,400 |
| **Layer 10 subtotal (29+30)** | **~3,200** |

Component 31 (~1,000) already counted in prior cumulative.
