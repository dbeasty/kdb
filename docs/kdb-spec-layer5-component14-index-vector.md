# KDB Component Spec — Layer 5
## Component 14: Index — Vector (HNSW)
### `dev.kdb.index.vector`

**File:** `kdb-spec-layer5-component14-index-vector.md`  
**Layer:** 5 — Index Implementations  
**Status:** Implementation-ready  
**Gradle module:** `:kdb-index-vector`  
**Depends on:** Layer 0–3, Layer 4a (optional GPU ingest hooks via `StorageCapabilitySet`), Component 12 (`IndexKey.VectorKey`)

-----

## 1. Purpose

Implements `IndexType.VECTOR` — approximate nearest neighbour (ANN) search over fixed-dimension float embeddings using an HNSW (Hierarchical Navigable Small World) graph. Powers SQL `ORDER BY similarity(embedding, '…')` and `nearestNeighbours` on `IndexStore`.

Embeddings are **application-supplied at write time** (stored as `IndexKey.VectorKey`); text-to-vector conversion is an optional pluggable `EmbeddingProvider` but not required for core index operations.

-----

## 2. Dependencies

| Module | Interfaces used |
|---|---|
| `dev.kdb.codec` | `KdbHash`, `KdbUuid` |
| `dev.kdb.error` | `KdbException`, `IndexCorruptionException` |
| `dev.kdb.dag` | `CommitDag` |
| `dev.kdb.index` | `IndexStore`, `IndexDescriptor`, `IndexEntry`, `IndexKey`, `RankedResult`, `IndexType` |
| `dev.kdb.storage` | `StorageAdapter`, `StorageCapabilitySet` (GPU flags — optional fast path) |

-----

## 3. Public Interface

```kotlin
package dev.kdb.index.vector

import dev.kdb.dag.CommitDag
import dev.kdb.index.*
import dev.kdb.storage.StorageAdapter

fun interface VectorIndexStoreFactory {
    fun create(
        descriptor: IndexDescriptor,
        dag: CommitDag,
        storage: StorageAdapter,
        dimensions: Int,
    ): IndexStore
}

fun vectorIndexStoreFactory(
    dag: CommitDag,
    storage: StorageAdapter,
    dimensions: Int,
): IndexStoreFactory =
    IndexStoreFactory { descriptor ->
        require(descriptor.type == IndexType.VECTOR) {
            "VectorIndexStoreFactory expected VECTOR, got ${descriptor.type}"
        }
        DefaultVectorIndexStore(descriptor, dag, storage, dimensions)
    }

class DefaultVectorIndexStore(
    override val descriptor: IndexDescriptor,
    private val dag: CommitDag,
    private val storage: StorageAdapter,
    val dimensions: Int,
    private val hnswConfig: HnswConfig = HnswConfig.DEFAULT,
) : IndexStore

data class HnswConfig(
    val m: Int = 16,                 // max edges per node
    val efConstruction: Int = 200,
    val efSearch: Int = 64,
    val metric: VectorMetric = VectorMetric.COSINE,
) {
    companion object { val DEFAULT = HnswConfig() }
}

enum class VectorMetric { COSINE, L2, INNER_PRODUCT }

/** Optional text → vector for SQL convenience; may be null in tests. */
fun interface EmbeddingProvider {
    suspend fun embed(text: String): FloatArray
}

fun interface VectorIndexStoreFactoryWithEmbedding :
    (IndexDescriptor, CommitDag, StorageAdapter, Int, EmbeddingProvider?) -> IndexStore
```

`nearestNeighbours(queryVector, k, atCommit)` returns top-k by `VectorMetric`. `lookup`/`range`/`search` throw `IndexTypeMismatchException`.

-----

## 4. Data Structures

### `HnswGraph`
In-memory navigable small world graph; nodes are `docId` + `FloatArray` embedding + `commitHash`. Layers 0..L with exponentially decreasing density.

### `VectorIndexManifest`
```kotlin
data class VectorIndexManifest(
    val indexId: KdbUuid,
    val dimensions: Int,
    val metric: VectorMetric,
    val generation: Long,
    val graphSegmentHash: KdbHash,
    val nodeCount: Long,
)
```

### `IndexEntry` for VECTOR
`IndexKey.VectorKey(embedding)` must have `embedding.size == dimensions` or `put` throws `VectorDimensionMismatchException`.

-----

## 5. Contracts

### `put`
Insert or update node for `docId` at `commitHash`. HNSW insert with `efConstruction`. If `docId` already exists at HEAD, replace vector and repair graph.

### `delete`
Remove node from all layers; repair links (lazy deletion acceptable at HEAD if tombstone filtered on search).

### `nearestNeighbours(queryVector, k, atCommit)`
- `queryVector.size == dimensions`.
- Search with `efSearch`; filter candidates by DAG ancestry at `atCommit`.
- Return ≤ k `RankedResult` sorted by descending score (cosine similarity in [0,1] or negated distance per metric).

### `snapshot` / `restoreSnapshot`
Serialise graph topology + vectors (Layer 0 binary record format).

### Versioning
Same ancestry filter as Component 12: only nodes whose `commitHash` is ancestor of query commit and not superseded by delete.

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `VectorDimensionMismatchException` | Embedding length ≠ `dimensions`. |
| `IndexTypeMismatchException` | Wrong `IndexStore` method. |
| `IndexCorruptionException` | Graph segment decode failure. |

```kotlin
class VectorDimensionMismatchException(
    message: String,
    val expected: Int,
    val actual: Int,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.SCHEMA_VIOLATION
}
```

-----

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `put_nearestSelf` | Single vector v. Query v, k=1. | Returns that docId, score ≈ 1.0 (cosine). |
| 2 | `nearestK_ordering` | Three colinear vectors; query nearest to middle. | Middle ranked first. |
| 3 | `dimension_mismatch` | `dimensions=128`, put len-64 vector. | `VectorDimensionMismatchException`. |
| 4 | `delete_excludesFromAnn` | Put two docs; delete nearer; query. | Farther doc returned. |
| 5 | `historical_atCommit` | Put v1 at H1, replace v2 at H2. Query at H1. | ANN uses v1 position. |
| 6 | `bulkLoad_rebuild` | 500 random vectors; `bulkLoad`. | ANN recall@10 ≥ 0.9 vs brute force on sample. |
| 7 | `metric_l2` | `HnswConfig(metric=L2)`. | Ordering matches L2 brute force for k=5. |
| 8 | `k_largerThanN` | 3 vectors, k=10. | 3 results. |
| 9 | `snapshot_roundTrip` | Graph 100 nodes; snapshot/restore. | Same top-1 as before. |
| 10 | `lookup_throws` | `lookup` on VECTOR store. | `IndexTypeMismatchException`. |
| 11 | `emptyIndex_returnsEmpty` | No puts; query. | `[]`. |
| 12 | `concurrentRead_search` | Parallel `nearestNeighbours` during puts. | No crash; deterministic size ≤ k. |

-----

## 8. Non-Goals

- **Training embedding models** — application supplies vectors or optional `EmbeddingProvider`.
- **GPU HNSW build/search in v1** — CPU graph only; GPU adapters (Layer 9) may accelerate later.
- **Filtered ANN** (metadata pre-filter) — future.
- **Quantisation (PQ/SQ)** — v1 stores full float32 vectors.
- **Multi-vector per document** — one vector per `docId` at HEAD.

-----

## 9. Implementation Notes

### HNSW parameters
Defaults align with literature (M=16, efConstruction=200). Tune via `HnswConfig` on namespace policy (Layer 6) later.

### Persistence
Graph serialised as Layer 0 record list: nodes, level assignments, neighbour id lists. Large graphs flush to `StorageAdapter` segments.

### `EmbeddingProvider`
SQL layer (Component 15) calls `embed('user text')` when query uses string literal instead of vector literal; provider injected at engine bootstrap (hosted API / bundled model — open question in master spec §15).

### Schema
Vector fields use future `KdbFieldType.VectorType(dimensions)` or index-only DDL; until then `CREATE VECTOR INDEX` sets `dimensions` on factory.

### KMP
Pure `commonMain` — HNSW in Kotlin for portability.

### Recall testing
Test #6 uses seeded RNG; compare ANN top-10 to brute-force top-10 for 50 queries; require ≥90% overlap.

-----

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| HNSW graph + insert/search | 1,600 |
| `DefaultVectorIndexStore` | 600 |
| Metrics (cosine/L2/IP) | 150 |
| Persistence + manifest | 450 |
| `EmbeddingProvider` stub + SQL hook surface | 100 |
| Tests | 1,100 |
| **Total** | **~4,000** |
