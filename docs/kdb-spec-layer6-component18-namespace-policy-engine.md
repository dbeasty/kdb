# KDB Component Spec — Layer 6
## Component 18: Namespace Policy Engine
### `dev.kdb.policy`

**File:** `kdb-spec-layer6-component18-namespace-policy-engine.md`  
**Layer:** 6 — Hybrid Query + Policy  
**Status:** Implementation-ready  
**Gradle module:** `:kdb-namespace-policy`  
**Depends on:** Layers 0–2, Layer 3 (`StorageAdapter`, `ConflictPolicy`, `IndexRetention`), Layer 4b (`GpuPromotionPolicy`, tier hints — read-only), Layer 2 (`SchemaEngine`)

-----

## 1. Purpose

Loads, validates, persists, and serves **per-namespace policy** as defined in master spec §10: schema binding, write mode, history retention, conflict policy, compaction rules, tier declarations, GPU/index retention defaults, and vector-index tuning hooks.

This module is the single source of truth for policy configuration consumed by the Hybrid Query Engine (17), Transaction Engine (7), Compaction Engine (19), Storage Manager enlistment (11d), and (later) Storage Tier Manager (Layer 7). It does **not** move bytes between tiers or execute DAG squash — those are Components 19 and 20.

-----

## 2. Dependencies

| Module | Interfaces used |
|---|---|
| `dev.kdb.codec` | `KdbHash`, `encodeToBytes`, `decodeFromBytes`, `KdbTimestamp` |
| `dev.kdb.error` | `KdbException`, `NamespaceNotFoundException` |
| `dev.kdb.schema` | `KdbSchema`, `SchemaEngine` |
| `dev.kdb.storage` | `StorageAdapter` — metadata key `namespace/{id}/policy` |
| `dev.kdb.transaction` | `ConflictPolicy` (align enum values) |
| `dev.kdb.storage` (9) | `IndexRetention`, `GpuPromotionPolicy` shapes |

-----

## 3. Public Interface

```kotlin
package dev.kdb.policy

import dev.kdb.codec.KdbHash
import dev.kdb.schema.KdbSchema
import dev.kdb.storage.IndexRetention
import dev.kdb.storage.StorageAdapter
import dev.kdb.transaction.ConflictPolicy

interface NamespacePolicyRegistry {
    suspend fun get(namespaceId: String): NamespacePolicy
    suspend fun getOrNull(namespaceId: String): NamespacePolicy?
    suspend fun put(policy: NamespacePolicy)
    suspend fun delete(namespaceId: String): Boolean
    suspend fun list(): List<String>
}

fun namespacePolicyRegistry(storage: StorageAdapter): NamespacePolicyRegistry

/** Parse master-spec §10 DSL from string or JSON file body. */
interface NamespacePolicyParser {
    fun parse(source: String): NamespacePolicy
    fun parseJson(json: String): NamespacePolicy
}

fun defaultNamespacePolicyParser(): NamespacePolicyParser

data class NamespacePolicy(
    val namespaceId: String,
    val schema: KdbSchema?,                    // null = schema NONE (pure document)
    val mode: NamespaceMode,
    val history: HistoryMode,
    val conflict: ConflictPolicy,
    val compaction: CompactionPolicy,
    val tiers: TierPolicy,                     // declarative; physical moves = Layer 7
    val indexRetentionDefault: IndexRetention,
    val gpuPromotion: GpuPromotionPolicyRef?,  // mirrors storage adapter policy record
    val vectorIndex: VectorIndexPolicy = VectorIndexPolicy(),
    val revision: Long = 1L,                   // bumped on each put
)

enum class NamespaceMode { MUTABLE, APPEND_ONLY }

enum class HistoryMode {
    FULL,       // versioning + AT VERSION
    NONE,       // no DAG overhead (cache namespaces)
}

data class CompactionPolicy(
    val keepTagged: Boolean = true,
    val keepBranchPoints: Boolean = true,
    val squashAfter: SquashMode = SquashMode.AUTO,
    val retainGranularity: List<RetainRule> = defaultRetainGranularity(),
)

enum class SquashMode { AUTO, NEVER }

data class RetainRule(
    val olderThanMillis: Long,
    val strategy: RetainStrategy,
)

enum class RetainStrategy {
    FULL_HISTORY,       // keep all commits in window
    DAILY_SNAPSHOTS,    // at most one commit per UTC day
    TAGGED_ONLY,        // only tagged + branch points in window
}

data class TierPolicy(
    val hot: TierBand = TierBand(maxAgeMillis = 7L * 24 * 3600 * 1000),
    val warm: TierBand = TierBand(maxAgeMillis = 90L * 24 * 3600 * 1000),
    val cold: TierBand = TierBand(maxAgeMillis = 365L * 24 * 3600 * 1000),
    val ice: IceTierBand = IceTierBand(),
)

data class TierBand(val maxAgeMillis: Long, val storageKind: StorageKind = StorageKind.LOCAL)
data class IceTierBand(val storageKind: StorageKind = StorageKind.ARCHIVE)

enum class StorageKind { LOCAL, LOCAL_FS, OBJECT_STORE, ARCHIVE }

/** Subset of adapter GPU policy — stored for enlistment resolution. */
data class GpuPromotionPolicyRef(
    val minSegmentAgeMillis: Long,
    val minSegmentSizeBytes: Long,
    val maxChangeRatePerMinute: Double,
)

data class VectorIndexPolicy(
    val hnswM: Int = 16,
    val hnswEfConstruction: Int = 200,
    val defaultDimensions: Int = 128,
)

interface PolicyValidator {
    fun validate(policy: NamespacePolicy): PolicyValidationResult
}

data class PolicyValidationResult(
    val ok: Boolean,
    val errors: List<PolicyValidationError>,
)

sealed class PolicyValidationError {
    data class InvalidRetainOrdering(val message: String) : PolicyValidationError()
    data class SchemaRequired(val field: String) : PolicyValidationError()
    data class UnsupportedMode(val detail: String) : PolicyValidationError()
}

/** Effective compaction schedule for Component 19. */
interface CompactionPolicyEvaluator {
    fun boundaryCandidates(
        policy: CompactionPolicy,
        commitTimestamps: Map<KdbHash, KdbTimestamp>,
        tagged: Set<KdbHash>,
        branchHeads: Set<KdbHash>,
    ): List<CompactionBoundaryPlan>
}

data class CompactionBoundaryPlan(
    val boundary: KdbHash,
    val squashThrough: KdbHash,
    val strategy: RetainStrategy,
)
```

-----

## 4. Data Structures

### Persistence wire record
`NamespacePolicyWire` — Layer 0 record in `dev.kdb.policy.*` registry; `encodeToBytes` / `decodeFromBytes` for `StorageAdapter.putBlob` at metadata path `namespace/{namespaceId}/policy/v{revision}`.

### Default policies (normative presets)
| Preset | `mode` | `history` | `compaction` |
|---|---|---|---|
| `defaultMutable()` | MUTABLE | FULL | `keepTagged=true`, default retain rules |
| `appendOnlyEvents()` | APPEND_ONLY | FULL | `squashAfter=NEVER` |
| `scratchDocument()` | MUTABLE | FULL | default |
| `cacheNoHistory()` | MUTABLE | NONE | `squashAfter=NEVER` |

### DSL mapping (§10)
Kotlin DSL in master spec maps 1:1 to `NamespacePolicy` fields. JSON equivalent accepted for `kdb schema set` / CLI.

### `RetainRule` evaluation order
Rules sorted by `olderThanMillis` ascending. For a commit age `A`, apply the **last** rule where `A >= olderThanMillis`.

-----

## 5. Contracts

### `NamespacePolicyRegistry.get`
- **Postconditions:** Returns stored policy or `defaultMutable(namespaceId)` if none stored (namespace bootstrap).

### `put`
- **Preconditions:** `validate(policy).ok == true`.
- **Postconditions:** Policy persisted; `revision` incremented. In-memory caches invalidated.

### `NamespacePolicyParser.parse`
- **Postconditions:** Parsed policy passes `PolicyValidator` or throws `PolicyParseException` with line/column when DSL.

### `CompactionPolicyEvaluator.boundaryCandidates`
- **Input:** Commit timestamps from DAG walk, tag set, branch heads.
- **Output:** Ordered plans: each plan identifies a `boundary` commit (new synthetic root parent) and `squashThrough` (oldest hash in squash run). Respects `keepTagged`, `keepBranchPoints`, `SquashMode.NEVER`.

### Conflict / history coupling
- `HistoryMode.NONE` → `compaction.squashAfter` should be `NEVER` (validator warning if not).
- `NamespaceMode.APPEND_ONLY` → transaction engine rejects `Delete` ops (enforced at engine construction via policy snapshot).

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `NamespaceNotFoundException` | Explicit delete/get on unknown id (optional strict mode) |
| `PolicyParseException` | DSL/JSON syntax or unknown field |
| `PolicyValidationException` | `put` with failed validation |
| `PolicyConflictException` | Concurrent put with stale `revision` |

```kotlin
class PolicyParseException(message: String, val offset: Int = -1) : KdbException(message)
class PolicyValidationException(val errors: List<PolicyValidationError>) : KdbException(
    errors.joinToString { it.toString() }
)
```

-----

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `parse_usersPolicy` | Master §10 `users` DSL fragment | `keepTagged=true`, 3 retain rules |
| 2 | `parse_eventsNeverSquash` | `squashAfter = NEVER` | `SquashMode.NEVER` |
| 3 | `parse_scratchNoSchema` | `schema = NONE` | `schema == null` |
| 4 | `validate_retainOrdering` | Rules out of order | Validation error |
| 5 | `put_roundTrip` | put + get | Equal policy, `revision+1` |
| 6 | `default_whenMissing` | get unknown namespace | `defaultMutable` |
| 7 | `evaluator_dailySnapshots` | 48 commits in 2 days | ≤2 boundaries in window |
| 8 | `evaluator_taggedOnly` | Tagged mid-chain | Tagged hash never in squash list |
| 9 | `cachePolicy_historyNone` | `history=NONE` | Hybrid engine rejects version (integration) |
| 10 | `jsonRoundTrip` | parseJson ↔ encode | Byte-identical wire |
| 11 | `gpuPolicy_ref` | GPU block in DSL | `GpuPromotionPolicyRef` populated |
| 12 | `concurrentPut_revision` | Two puts same revision | Second fails `PolicyConflictException` |

-----

## 8. Non-Goals

- **Physical tier movement (COLD/ICE)** — Layer 7 Component 20.
- **Schema migration execution** — Layer 2 / transaction; policy only holds `KdbSchema` reference.
- **Distributed policy sync** — peers replicate policy via commit metadata / admin push (Layer 8).
- **AuthZ / RBAC** — future security layer.
- **Runtime policy hot-reload notifications** — v1: poll `revision` on access.

-----

## 9. Implementation Notes

### Parser v1
Ship **JSON policy documents** as canonical on-disk form; optional lightweight Kotlin DSL parser via simple tokenizer (~400 lines) or embed policy JSON in namespace init CLI.

### Storage key
`namespace/{namespaceId}/policy` — single latest blob; history of policy revisions optional audit trail (non-goal v1).

### Integration points
| Consumer | Uses |
|---|---|
| `hybridQueryEngine` | `history`, `schema` |
| `transactionEngine(...)` | `conflict`, `mode` |
| `CompactionEngine` (19) | `compaction`, `CompactionPolicyEvaluator` |
| `EnlistmentManager` (11d) | `indexRetentionDefault`, `gpuPromotion` |
| `DeltaLogTierRegistry` (11e) | `tiers` age bands (signal thresholds) |

### KMP
`commonMain` only.

### Wire registry
Add `KdbPolicyWireRegistry` in same module for deterministic policy bytes.

-----

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| `NamespacePolicy` + enums | 200 |
| Wire codec + registry | 250 |
| JSON parser + DSL parser | 400 |
| `NamespacePolicyRegistry` + storage | 200 |
| `PolicyValidator` + evaluator | 250 |
| Presets + tests | 200 |
| **Total** | **~1,500** |
