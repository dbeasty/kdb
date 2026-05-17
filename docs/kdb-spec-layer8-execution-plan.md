# KDB Layer 8 — Implementation Execution Plan

**Status:** Implemented (first Kotlin cut — May 2026)  
**Master spec:** `docs/kdb-spec.md` §16.1 (Layer 8), §0 (session state)  
**Depends on:** Layer 7 complete (Components 20–22 implemented; interfaces in §17)

-----

## Scope

Layer 8 delivers **Mode 3 peer sync + JDBC**:

| Component | Module | Spec file |
|---|---|---|
| 23 Peer Sync Mode (Mode 3) | `:kdb-peer-sync` | `kdb-spec-layer8-component23-peer-sync-mode.md` |
| 24 JDBC Driver | `:kdb-jdbc` | `kdb-spec-layer8-component24-jdbc-driver.md` |

**Not in Layer 8:** WebSocket/TCP transport (25–26, Layer 9), CLI (29), integration test suite (30).

-----

## Normative implementation order

Product priority: JDBC (Phase 1 deliverable) after peer wire paths exist. Implement **23 → 24** so `CommitPush` round-trip is verified before JDBC ships.

### Phase 1 — Peer Sync (23)

1. Complete `CommitPush` encode/decode in `:kdb-wire` (length-prefixed commit payloads).
2. Create `:kdb-peer-sync`; depend on `:kdb-wire`, `:kdb-stream`, `:kdb-dag`, `:kdb-transaction`.
3. Implement `computeSyncPlan` + `CommitPushCodec` usage.
4. Implement `PeerSyncHost` (`CommitFetch`, `CommitPush`, `FULL_PEER` handshake).
5. Implement `PeerSyncClient` + `PeerSession.syncBidirectional`.
6. **Tests:** 12 cases from component spec §7 (in-memory wire hubs).
7. Paste public interface into master spec §17 → Layer 8 (Component 23).

**Exit criteria:** Two in-memory peers exchange commits after fork; heads match on linear history.

### Phase 2 — JDBC Driver (24)

1. Create `:kdb-jdbc` (JVM target only).
2. Implement `KdbJdbcUrl` parser (`memory`, `readOnly`).
3. Implement `EmbeddedKdbRuntime` + `openMemoryRuntime`.
4. Implement `KdbDriver`, `KdbConnection`, `KdbStatement`, `KdbPreparedStatement`, `KdbResultSet`, `KdbDatabaseMetaData`.
5. **Tests:** 12 cases from spec §7 via `DriverManager`.
6. Paste interface into §17 (Component 24).

**Exit criteria:** `DriverManager.getConnection("jdbc:kdb:memory:///demo")` runs `SELECT` and returns rows.

### Phase 3 — Master spec

1. Update §0 checklist: Layer 8 specs `[x]`, implementation `[x]`.
2. Update §16.1 Layer 8 block + implementation order.
3. Update §14 Layer 8 subtotal (~9,000 NBNC).
4. Update §17 Layer 8 interfaces.

-----

## Gradle modules

```kotlin
include(":kdb-peer-sync")
include(":kdb-jdbc")
```

```
:kdb-peer-sync → kdb-wire, kdb-stream, kdb-dag, kdb-document, kdb-transaction, kdb-storage, kdb-error
:kdb-jdbc      → kdb-hybrid-query, kdb-sql, kdb-dag, kdb-storage, kdb-schema, kdb-policy (jvm)
```

-----

## Estimated NBNC (Layer 8)

| Component | Lines |
|---|---|
| 23 Peer Sync | ~4,500 |
| 24 JDBC Driver | ~5,000 |
| **Layer 8 subtotal** | **~9,500** |

Cumulative after Layer 8: **~122,350** (see master §14).

-----

## Verification checklist

- [x] `./gradlew :kdb-wire:jvmTest`
- [x] `./gradlew :kdb-peer-sync:jvmTest`
- [x] `./gradlew :kdb-jdbc:test`
- [x] Layer 7 tests still pass (`:kdb-stream:jvmTest`, `:kdb-storage-tier:jvmTest`)
- [x] Master spec §0 Layer 8 checklist updated
- [x] Master spec §17 Layer 8 interfaces populated
- [x] Master spec §14 includes Layer 8 subtotal row
