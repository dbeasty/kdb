# KDB Storage Engine — Design Decisions v3

**Purpose:** Extended and updated storage engine design decisions. Covers multi-implementation model, mixed enlistment model, browser snapshot + repair, sub-enlistment eviction tiers, policy-driven index pinning, and GPU direct-from-delta-log materialisation. Feed this into a Layer 3 spec session alongside kdb-spec-v0.8.md.

**Supersedes:** kdb-storage-engine-design-decisions-v2.md. All decisions from v1 and v2 remain in force unless explicitly updated here.

---

## What Has Not Changed From v1/v2

- No external storage dependencies — pure Kotlin commonMain storage engine
- Two-store architecture: Delta Store (canonical truth) + Realized Store (materialisable working copy)
- BSON-native, append-only, large pages (8MB–16MB)
- Delta authorship envelope (principal, timestamp, rights_token, client_context) in every delta
- Storage Manager is the single global orchestrator per node
- Browser is a first-class multi-enlistment participant
- Three shipping engine implementations: Server, Browser, InMemory
- GPU engine is provisioned in the interface as a stub for later

---

## Decision: Browser Snapshot Scope (update to v2)

### What is snapshotted

localStorage/sessionStorage is written for the **realized store only** — the materialised document and index state used to serve SQL queries. The delta log is **not** snapshotted to localStorage.

Rationale: the delta log is the canonical truth, but re-fetching it from a peer on reload is cheaper and simpler than maintaining a durable local delta log across browser restarts. The realized store snapshot lets the browser serve queries immediately on reload without waiting for a full peer sync.

### Repair model

If the realized store snapshot is missing, partial, or fails integrity check on load, the repair strategy is:

**Re-request state from another peer.** No local recovery logic. The browser enlistment fetches the current realized state from its upstream peer (server or another browser peer) and rebuilds from that.

This means:
- No complex snapshot versioning or partial-write recovery is needed
- The snapshot is a performance optimisation (fast reload), not a durability guarantee
- The delta log is never written to localStorage — the browser is not a durability boundary for deltas

### Implications for BrowserStorageEngine

- Snapshot write is a best-effort serialisation of the current realized store to localStorage/sessionStorage
- On load: attempt to read snapshot → if valid, serve queries immediately and sync deltas in background → if invalid/missing, fetch state from peer before serving queries
- Snapshot format: BSON+zstd of the realized document map + index state at a named commit hash
- The commit hash in the snapshot is the reconciliation point: on reconnect, the browser tells the peer "I have state at hash X, give me deltas from X to HEAD"

---

## Decision: Sub-Enlistment Eviction Tiers (replaces v2 flat LRU)

The Storage Manager evicts at **sub-enlistment granularity**. A realized store is not a single atomic unit for eviction purposes — it has two independently evictable components:

### Eviction components

**Document store** — the full materialised documents (the `_doc` content for every document at this commit). Large. Required for document fetch and `_doc` queries. Can be evicted and rebuilt from the delta log.

**Index store** — the SQL query indexes (B-tree, hash, full-text, vector) for this enlistment. Smaller than the document store for typical namespaces. Required for indexed SQL queries. Can be evicted and rebuilt by replaying the document store through the index layer.

### Eviction priority (default)

```
Evict document store first  →  index store second  →  enlistment entry last
```

This means a query-heavy enlistment under memory pressure will lose its document store (raw `_doc` fetches become slower — require delta replay) before it loses its index (SQL queries continue to use indexes at full speed).

### Policy-driven index pinning

Namespace policy declares `indexRetention`, which controls whether the index store participates in normal eviction:

```kotlin
namespace("myapp/users") {
    indexRetention = PINNED      // index stays in memory as long as enlistment is open
                                 // only evicted when the enlistment itself is released
}

namespace("myapp/scratch") {
    indexRetention = EVICTABLE   // index participates in normal LRU eviction
                                 // evicted after document store, but still evictable
}

namespace("myapp/analytics") {
    indexRetention = EVICTABLE   // large analytical namespace; everything evictable
}
```

**PINNED** — the index store is excluded from LRU eviction. It is only released when the `RealizedStoreHandle` for that enlistment is explicitly released by the caller. Under extreme memory pressure the Storage Manager may log a warning but will not forcibly evict a pinned index.

**EVICTABLE** — the index store participates in LRU eviction, but with lower priority than the document store. The eviction manager tracks document store and index store as separate LRU entries with different weights.

### Storage Manager eviction state machine per enlistment

```
FULL          document store + index store both in memory
DOC_EVICTED   index store only; document store evicted; SQL queries fast, _doc slow
EVICTED       neither in memory; enlistment entry retained; full rebuild on next access
RELEASED      handle released; enlistment entry removed
```

Transitions:

```
FULL → DOC_EVICTED    eviction pressure; index is EVICTABLE or PINNED (either way, doc evicts first)
FULL → EVICTED        extreme pressure + index is EVICTABLE
DOC_EVICTED → EVICTED extreme pressure + index is EVICTABLE
DOC_EVICTED → FULL    demand rebuild of document store (async, from delta log)
EVICTED → FULL        demand rebuild of both (async, from delta log)
any → RELEASED        handle.release() called
```

### Rebuild scheduling

When a caller requests a `RealizedStoreHandle` for an enlistment in `DOC_EVICTED` or `EVICTED` state, the Storage Manager:
1. Returns the handle immediately (non-blocking)
2. Sets a `rebuildPending` flag on the handle
3. Schedules an async rebuild of the missing components
4. Caller can await `handle.awaitReady()` or poll `handle.isReady`
5. Queries against an incompletely rebuilt realized store return partial results or block per caller preference (declared at `requestRealized()` time)

---

## Decision: GPU Engine — Direct Delta Log Materialisation

### Architecture

The `GpuStorageEngine` is a **parallel realized store**, not a downstream consumer of the CPU realized store. It materialises directly from the compressed delta log, bypassing the CPU realized store entirely.

```
Delta Store (CPU, compressed BSON+zstd)
    │
    ├──► CPU Realized Store   (ServerStorageEngine / BrowserStorageEngine / InMemoryStorageEngine)
    │         serves: document fetch, SQL queries, peer sync
    │
    └──► GPU Realized Store   (GpuStorageEngine)  ← materialises independently from delta log
              serves: vector ANN search, bulk scan, GPU-accelerated analytical queries
```

Rationale: this keeps the CPU realized store lean and avoids the cost of serialising CPU-resident document data into GPU memory format. The GPU engine reads the same delta segments the CPU engine reads, applies its own decompaction and layout strategy, and produces a GPU-resident working copy optimised for GPU access patterns.

### GPU decompaction as a feature

Because GPU is extremely fast at decompression and memory operations, the GPU engine can:
- Store its working copy in a **more densely packed / columnar layout** than the CPU realized store
- Decompress and restructure BSON+zstd delta segments into GPU-friendly formats at materialisation time
- Re-compact (defragment) its GPU-resident working copy during idle cycles without CPU involvement
- The CPU delta log remains in compressed BSON+zstd form; the GPU engine does not write back to it

This means the GPU engine can maintain a working copy that is structurally different from (and potentially denser than) the CPU realized store, tuned for GPU vectorised access rather than random document lookup.

### Storage Manager promotion policy for GPU

The GPU engine has its own promotion policy, separate from the CPU eviction policy. The Storage Manager applies GPU-specific rules to decide when to promote delta segments to GPU memory:

**Promotion triggers (default policy, configurable per namespace):**

```kotlin
namespace("myapp/embeddings") {
    gpuPromotion {
        minSegmentAge    = 5.minutes      // don't promote segments still being actively written
        minSegmentSize   = 64.MB          // only promote large segments (GPU setup cost amortised)
        maxChangeRate    = 100.writes/min // don't promote high-churn data — GPU recompaction overhead
        strategy         = PROMOTE_ON_QUERY  // promote when a GPU-accelerated query first hits this segment
    }
}

namespace("myapp/vectors") {
    gpuPromotion {
        strategy = PROMOTE_EAGERLY        // promote as soon as segment is sealed, regardless of query demand
    }
}
```

**Key policy principle:** GPU promotion favours **large, low-churn segments**. Frequently updated data stays CPU-resident; the GPU engine holds the stable bulk of the dataset and re-materialises when a new large sealed segment arrives.

### GPU and the StorageCapabilitySet

The `StorageCapabilitySet` capability query (introduced in v2) is extended:

```
StorageCapabilitySet {
    persistsDeltaLog:          Boolean    // Server: true | Browser: false | InMemory: false | GPU: false
    persistsAcrossReload:      Boolean    // Server: true | Browser: partial | InMemory: false | GPU: false
    supportsGpuBulkRead:       Boolean    // GPU: true | others: false
    supportsDirectDeltaIngest: Boolean    // GPU: true (materialises from delta log directly) | others: false
    maxEnlistments:            Int?       // null = unlimited
    indexRetentionDefault:     IndexRetention  // namespace policy default if not declared
}
```

`supportsDirectDeltaIngest: true` tells the Storage Manager that this engine can be handed a delta segment reference directly rather than a pre-materialised document set. The Storage Manager uses this to skip the CPU realized store step when promoting to GPU.

---

## Updated Namespace Policy (additions)

Two new fields added to namespace policy, applicable from Layer 3 onwards:

```kotlin
namespace("myapp/users") {
    schema { ... }
    mode            = MUTABLE
    history         = FULL
    conflict        = STRICT
    indexRetention  = PINNED        // NEW: index stays in memory as long as enlistment is open
    gpuPromotion    = PROMOTE_ON_QUERY  // NEW: GPU promotion strategy (ignored if no GPU engine)
    compaction { ... }
    tiers { ... }
}
```

**`indexRetention`** — `PINNED` or `EVICTABLE`. Default: `EVICTABLE`. Hot query namespaces should declare `PINNED`.

**`gpuPromotion`** — `PROMOTE_ON_QUERY`, `PROMOTE_EAGERLY`, `NEVER`. Default: `NEVER`. Only meaningful when a `GpuStorageEngine` enlistment exists for this namespace.

---

## Updated Storage Manager Responsibilities

Additions to the Storage Manager responsibilities defined in v1:

- **Sub-enlistment eviction** — tracks document store and index store as separate LRU entries; evicts per the priority order and `indexRetention` policy
- **GPU promotion scheduling** — maintains a promotion queue; applies per-namespace GPU promotion policy; hands delta segment references directly to `GpuStorageEngine` when `supportsDirectDeltaIngest` is true
- **Browser repair coordination** — when a browser enlistment fails snapshot load, coordinates state fetch from peer; provides the commit hash anchor for delta sync
- **Rebuild state tracking** — tracks `FULL / DOC_EVICTED / EVICTED / RELEASED` state per enlistment; drives async rebuild scheduler
- **Enlistment engine selection** — applies hint → namespace policy → platform default → fallback chain at enlistment creation time

---

## Updated StorageAdapter Interface Requirements (for Layer 3)

The `StorageAdapter` interface defined in Layer 3 must accommodate all of the above. Key additions beyond v2:

1. **`capabilities(): StorageCapabilitySet`** — including `supportsDirectDeltaIngest`
2. **`ingestDeltaSegment(segment: DeltaSegmentRef)`** — called by Storage Manager on engines where `supportsDirectDeltaIngest = true`; engine materialises its own realized store from the segment directly
3. **`evictDocuments(enlistmentId)`** — Storage Manager calls this to transition an enlistment to `DOC_EVICTED`; engine discards document store, retains index store
4. **`evictIndex(enlistmentId)`** — Storage Manager calls this under extreme pressure on `EVICTABLE` indexes
5. **`rebuildDocuments(enlistmentId, fromDeltaLog)`** — async; engine rebuilds document store from provided delta log reference
6. **`rebuildIndex(enlistmentId, fromDocuments)`** — async; engine rebuilds index from current document store
7. **Bulk read API** — read operations must be expressible as typed batch requests (not per-document) to allow GPU implementations to serve them from GPU buffers without per-document CPU round-trips

---

## Open Questions (additions to Section 15 of master spec)

In addition to questions added in v0.8:

- **Mixed enlistment eviction fairness** — when evicting under memory pressure across mixed engine types, should `InMemoryStorageEngine` enlistments be considered separately from `ServerStorageEngine` ones, given InMemory has no rebuild path (data is simply gone)?
- **GPU promotion strategy feedback loop** — should the Storage Manager track GPU query hit rates per segment and use that to dynamically adjust the promotion policy thresholds, or is static per-namespace configuration sufficient?
- **Browser snapshot commit hash staleness** — if the browser is offline for a long time and the server has compacted past the commit hash recorded in the snapshot, the snapshot's anchor hash is gone. The repair path (re-fetch from peer) handles this, but the enlistment needs to signal "snapshot anchor compacted away" cleanly rather than treating it as a generic snapshot failure.
- **PINNED index under OOM** — if the process approaches OOM and all remaining memory is in PINNED indexes, the Storage Manager currently logs a warning but cannot forcibly evict. A hard OOM kill is worse than a policy violation. Define the escalation path: warn → degrade to EVICTABLE for one enlistment → emit `IndexPinViolationEvent` to caller → caller decides.
