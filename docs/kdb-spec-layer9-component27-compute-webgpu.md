# KDB Component Spec — Layer 9
## Component 27: Compute Adapter — WebGPU
### `dev.kdb.compute.webgpu`

**File:** `kdb-spec-layer9-component27-compute-webgpu.md`  
**Layer:** 9 — Platform Adapters  
**Status:** Implementation-ready  
**Gradle module:** `:kdb-compute-webgpu`  
**Source sets:** `commonMain` (types + factory expect), `jsMain` only (actual)  
**Depends on:** Layer 3 (`DeltaSegmentRef`, `StorageCapabilitySet`), Layer 4a (`GpuStorageEngine`), Layer 5 Component 14 (`VectorMetric`, `RankedResult`), Layer 9 Component 28 shared API (`:kdb-compute`)

-----

## 1. Purpose

Provides the **browser compute adapter** using WebGPU: optional acceleration for **vector ANN search** and **delta-segment materialisation** into GPU-resident buffers for bulk read paths (master §2, §9.2). Implements the shared `ComputeAdapter` interface consumed by `GpuStorageEngine.ingestDeltaSegment` and `:kdb-index-vector` when `StorageCapabilitySet.supportsGpuBulkRead` is true.

v1 ships **working WebGPU dispatch** for brute-force cosine top-k (parity with CPU vector index v1) and a **segment ingest queue** that records promoted segments; full HNSW-on-GPU is deferred.

-----

## 2. Dependencies

| Module | Interfaces used |
|---|---|
| `dev.kdb.compute` | `ComputeAdapter`, `GpuVectorSearchRequest`, `GpuSegmentIngestRequest` |
| `dev.kdb.storage` | `DeltaSegmentRef`, `StorageCapabilitySet` |
| `dev.kdb.index` | `RankedResult`, `IndexKey` |
| `dev.kdb.codec` | `KdbHash`, `KdbUuid` |
| `dev.kdb.error` | `ComputeUnavailableException`, `ComputeDispatchException` |

-----

## 3. Public Interface

```kotlin
package dev.kdb.compute.webgpu

import dev.kdb.compute.ComputeAdapter
import dev.kdb.compute.ComputeAdapterCapabilities

/** jsMain actual — returns null when WebGPU unavailable. */
expect fun createWebGpuComputeAdapter(): ComputeAdapter?

/** Test hook: force CPU fallback path in jsTest. */
expect fun createWebGpuComputeAdapterOrCpuFallback(): ComputeAdapter

data class WebGpuAdapterConfig(
    val maxVectorsPerBatch: Int = 65_536,
    val maxDimensions: Int = 2048,
    val preferredDevice: GpuDevicePreference = GpuDevicePreference.HIGH_PERFORMANCE,
)

enum class GpuDevicePreference { LOW_POWER, HIGH_PERFORMANCE }

/** jsMain — exposes device lost / recovery for enlistment manager. */
interface WebGpuLifecycle {
    fun onDeviceLost(handler: () -> Unit)
    suspend fun tryRecover(): Boolean
}
```

All normative query/ingest signatures live on `ComputeAdapter` in `:kdb-compute` (Component 28 shared module).

-----

## 4. Data Structures

### WGSL kernel (v1) — `cosineTopK.wgsl`
```
// workgroup size 256
// inputs: query[D], matrix[N*D], ids[N]
// output: top-k heap in storage buffer (atomic min-heap simplified: sort on CPU for k<=64 v1)
```

v1 pipeline:
1. Upload query + candidate matrix slice to `GPUBuffer`.
2. Dispatch compute shader writing per-vector cosine scores.
3. Read back scores; **top-k selection on CPU** for k ≤ 64 (acceptable for browser v1).

### Segment ingest
```kotlin
data class GpuSegmentHandle(
    val segmentId: KdbUuid,
    val byteSize: Long,
    val deviceBufferId: String,  // opaque
)
```

`ingestDeltaSegment`: decompress zstd segment bytes (CPU, `:kdb-compression`) → upload columnar doc-id + embedding columns to GPU buffers → register handle in adapter map keyed by `DeltaSegmentRef`.

### Availability probe
```kotlin
// jsMain
suspend fun isWebGpuAvailable(): Boolean
```

Returns false on Safari without WebGPU, headless CI, or missing `navigator.gpu`.

-----

## 5. Contracts

### `ComputeAdapter.isAvailable`
- **Postconditions:** `true` only when device acquired and shaders compiled.

### `vectorNearestNeighbours`
- **Preconditions:** All vectors same `dimensions`; metric COSINE only in v1 WebGPU path.
- **Postconditions:** Returns ≤ k results sorted by descending similarity; empty if no candidates.
- **Fallback:** When GPU fails mid-flight, adapter may complete on CPU and emit metric event (no throw if `allowCpuFallback=true` in config).

### `ingestDeltaSegment`
- **Preconditions:** `segment.compressedBytes` readable; namespace matches enlistment.
- **Postconditions:** Handle registered; `GpuStorageEngine.pendingSegments()` decreases after successful ingest.
- **Idempotent:** Re-ingest same `segmentId` replaces buffer.

### Device loss
On device lost: mark adapter unavailable; in-flight searches throw `ComputeUnavailableException`; enlistment manager may rebuild from CPU snapshot (11d).

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `ComputeUnavailableException` | No WebGPU, device lost, shader compile failed |
| `ComputeDispatchException` | GPU OOM, buffer mapping failed |
| `IllegalArgumentException` | dimension mismatch, k > maxVectorsPerBatch |

```kotlin
class ComputeUnavailableException(message: String) : KdbException(message)
class ComputeDispatchException(message: String, cause: Throwable? = null) : KdbException(message, cause)
```

-----

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `probe_unavailable_headless` | no GPU | `createWebGpuComputeAdapter()` null |
| 2 | `cpuFallback_top3` | 100 random vectors, k=3 | 3 results, cosine order |
| 3 | `ingest_emptySegment` | zero docs | handle with size 0 |
| 4 | `ingest_idempotent` | same segment twice | one handle |
| 5 | `search_afterIngest` | ingest + query | non-empty top-k |
| 6 | `rejectDimensionMismatch` | D=128 query, D=64 store | `IllegalArgumentException` |
| 7 | `maxK_cap` | k=1000, cap 64 | at most 64 results |
| 8 | `deviceLost_marksUnavailable` | simulate lost | `isAvailable=false` |
| 9 | `recover_afterLost` | tryRecover | true → searches work |
| 10 | `cosine_knownAnswer` | orthogonal unit vectors | expected doc id rank 1 |
| 11 | `parallelBatches` | 2 namespaces | isolated handles |
| 12 | `integrate_vectorIndexStore` | DefaultVectorIndexStore + adapter | same results as CPU brute force |

-----

## 8. Non-Goals

- **CUDA/Vulkan** — Component 28 (jvmMain).
- **HNSW graph build on GPU** — CPU graph remains authoritative v1.
- **Full columnar SQL scan GPU engine** — stub ingest queue only; bulk scan kernel Phase 3+.
- **Training / embedding models** — `EmbeddingProvider` stays application-side.
- **WebGL fallback** — WebGPU only.

-----

## 9. Implementation Notes

### Module layout
```
kdb-compute/           commonMain — ComputeAdapter interface
kdb-compute-webgpu/
  commonMain/  expect factories, config
  jsMain/      WebGpuComputeAdapter.kt, shaders/, buffer pool
  jsTest/      CpuFallbackComputeAdapter for CI
```

### Browser constraints
- Request `limits.maxStorageBufferBindingSize` before large uploads.
- Avoid blocking main thread: use `GPUQueue.onSubmittedWorkDone` suspending wrapper.

### Integration
`GpuStorageEngine` constructor accepts optional `ComputeAdapter`; browser enlistment factory passes `createWebGpuComputeAdapterOrCpuFallback()`.

### KMP
No jvmMain in this module. JVM uses Component 28.

-----

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| WGSL + pipeline | 600 |
| Buffer pool + ingest | 500 |
| Vector search dispatch | 700 |
| Lifecycle + fallback | 400 |
| Tests (js + fallback) | 800 |
| **Total** | **~3,000** |
