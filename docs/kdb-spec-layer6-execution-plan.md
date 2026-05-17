# KDB Layer 6 — Implementation Execution Plan

**Status:** Implemented (first Kotlin cut — May 2026)  
**Master spec:** `docs/kdb-spec.md` §16.1 (Layer 6), §0 (session state)  
**Depends on:** Layer 5 complete (Components 12–16 implemented; interfaces in §17)

-----

## Scope

Layer 6 delivers **hybrid query + policy**:

| Component | Module | Spec file |
|---|---|---|
| 17 Hybrid Query Engine | `:kdb-hybrid-query` | `kdb-spec-layer6-component17-hybrid-query-engine.md` |
| 18 Namespace Policy Engine | `:kdb-namespace-policy` | `kdb-spec-layer6-component18-namespace-policy-engine.md` |
| 19 Compaction Engine (DAG) | `:kdb-compaction` | `kdb-spec-layer6-component19-compaction-engine.md` |

**Not in Layer 6:** Storage Tier Manager (Component 20, Layer 7), wire protocol, JDBC, physical COLD/ICE moves.

-----

## Normative implementation order

### Phase 1 — Namespace Policy (18) first

Policy is a hard dependency for version rules and compaction scheduling.

1. Create `:kdb-namespace-policy` with `NamespacePolicy`, wire registry, JSON parser.
2. Implement `NamespacePolicyRegistry` on `StorageAdapter` metadata path.
3. Implement `PolicyValidator` + `CompactionPolicyEvaluator`.
4. Ship preset factories (`defaultMutable`, `appendOnlyEvents`, `cacheNoHistory`).
5. **Tests:** 12 cases from component spec §7.
6. Paste public interface into master spec §17 → Layer 6 (Component 18).

**Exit criteria:** Any namespace can `put`/`get` policy; evaluator returns boundary plans for synthetic DAG fixtures.

### Phase 2 — Hybrid Query (17)

Requires policy for `HistoryMode` and schema binding.

1. Create `:kdb-hybrid-query`; depend on `:kdb-sql`, `:kdb-namespace-policy`, `:kdb-transaction`.
2. Implement `VersionResolver` + `CheckoutStore`.
3. Implement `HybridSqlParser` (`AT VERSION` / `AT COMMIT` / `AT TIME` suffix).
4. Implement `DefaultHybridQueryEngine` facade over existing `SqlEngine`.
5. Wire `QueryContext.atCommit` from resolved version.
6. Implement `DmlHybridRouter` for `_doc` vs schema column updates (delegate writes to `TransactionEngine`).
7. Historical index pin: pass `atCommit` through to `IndexReader` (extend `:kdb-index` if needed).
8. **Tests:** 12 cases from spec §7 + integration with Layer 5 SQL tests.
9. Paste interface into §17 (Component 17).

**Exit criteria:** `SELECT … AT VERSION 'tag'` returns correct rows; DML blocked under checkout; `kdb_json_get` filters work.

### Phase 3 — Compaction Engine (19)

Requires policy evaluator and stable DAG APIs.

1. Create `:kdb-compaction`; depend on `:kdb-dag`, `:kdb-namespace-policy`, `:kdb-storage`.
2. Implement `CompactionPlanner` using `CompactionPolicyEvaluator` + `dag.compactableBefore`.
3. Implement `DefaultSnapshotMaterializer`.
4. Implement `CompactionEngine.runCycle` / `plan` calling `dag.squash`.
5. Implement `InProcessCompactionCoordinator` (peer registry + ack simulation).
6. Implement `DefaultOrphanBlobGc`.
7. Optional hook: enqueue one 10f job via `dev.kdb.storage.compaction` after squash.
8. **Tests:** 12 cases from spec §7 on `inMemoryCommitDag`.
9. Paste interface into §17 (Component 19).

**Exit criteria:** Linear commit chains squash without losing tags; `SquashMode.NEVER` yields empty plan; GC reclaims orphan blobs in test harness.

### Phase 4 — Integration + master spec

1. Node bootstrap: construct `namespacePolicyRegistry` → `hybridQueryEngine` → register compaction scheduler on JVM.
2. Update master §0 checklist: Layer 6 specs `[x]`, implementation `[~]` or `[x]` per component.
3. Update §14 stats table with Layer 6 subtotal.
4. Wire CLI stubs: `kdb compact` → `CompactionEngine.runCycle` (optional in same session).

-----

## Parallelism

| Can parallelize | Cannot start until |
|---|---|
| Policy wire types + registry (18a) | — |
| Hybrid parser (17a) | 18 `HistoryMode` enum stable |
| Compaction planner dry-run (19a) | 18 evaluator |
| Hybrid facade (17b) | 17a + Layer 5 `SqlEngine` |
| Compaction squash (19b) | 18 + 19a |

**Rule:** Do not implement 17 or 19 before 18’s `CompactionPolicy` and `HistoryMode` are frozen.

-----

## Gradle modules to add

```kotlin
// settings.gradle.kts
include(":kdb-namespace-policy")
include(":kdb-hybrid-query")
include(":kdb-compaction")
```

Dependency edges:

```
:kdb-namespace-policy  → kdb-codec, kdb-error, kdb-schema, kdb-storage, kdb-transaction
:kdb-hybrid-query      → kdb-sql, kdb-namespace-policy, kdb-transaction, kdb-dag, …
:kdb-compaction        → kdb-dag, kdb-namespace-policy, kdb-storage, kdb-storage-compaction (optional)
```

-----

## Estimated NBNC (Layer 6)

| Component | Lines |
|---|---|
| 17 Hybrid Query | ~2,000 |
| 18 Namespace Policy | ~1,500 |
| 19 Compaction Engine | ~3,000 |
| **Layer 6 subtotal** | **~6,500** |

Cumulative engine estimate after Layer 6: **~103,850** (see master §14).

-----

## Session prompts (copy-paste)

**Spec session (done):**
```
Generate implementation-ready component specs for Layer 6: Hybrid Query Engine,
Namespace Policy Engine, Compaction Engine. Follow Section 16.2 structure.
```

**Implement Component 18:**
```
Implement Component 18 per kdb-spec-layer6-component18-namespace-policy-engine.md.
Section 17 Layers 0–5 are fixed contracts.
```

**Implement Component 17:**
```
Implement Component 17 per kdb-spec-layer6-component17-hybrid-query-engine.md.
Depends on :kdb-namespace-policy and :kdb-sql.
```

**Implement Component 19:**
```
Implement Component 19 per kdb-spec-layer6-component19-compaction-engine.md.
Do not implement SSTable merge (that is 10f).
```

-----

## Verification checklist

- [x] `./gradlew :kdb-namespace-policy:jvmTest`
- [x] `./gradlew :kdb-hybrid-query:jvmTest`
- [x] `./gradlew :kdb-compaction:jvmTest`
- [x] Layer 5 tests still pass (`:kdb-sql:jvmTest`)
- [x] Master spec §0 Layer 6 checklist updated
- [x] Master spec §17 Layer 6 interfaces populated
- [x] Master spec §14 includes Layer 6 subtotal row
