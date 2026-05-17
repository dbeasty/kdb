# KDB Component Spec — Layer 9
## Component 28: Compute Adapter — CUDA / Vulkan
### `dev.kdb.compute.jvm`

**File:** `kdb-spec-layer9-component28-compute-cuda-vulkan.md`  
**Layer:** 9 — Platform Adapters  
**Status:** Implementation-ready  
**Gradle module:** `:kdb-compute-jvm` (JVM); optional `nativeMain` Vulkan in same module or `:kdb-compute-native` follow-on  
**Depends on:** `:kdb-compute` (shared API), Layer 4a `GpuStorageEngine`, Layer 5 vector index

-----

## 1. Purpose

Provides the **server-side compute adapter** for JVM backends: CUDA when an NVIDIA driver and JNA/JNI bindings are present; **Vulkan compute fallback** when CUDA is absent but Vulkan is available; **CPU SIMD fallback** when neither is available (must still implement `ComputeAdapter` so engine code paths stay uniform).

Accelerates the same operations as WebGPU (Component 27): delta segment ingest to GPU memory and brute-force cosine top-k for vector search v1.

-----

## 2. Dependencies

| Module | Interfaces used |
|---|---|
| `dev.kdb.compute` | `ComputeAdapter`, `GpuVectorSearchRequest`, `GpuSegmentIngestRequest`, `ComputeBackend` |
| `dev.kdb.storage` | `DeltaSegmentRef` |
| `dev.kdb.index` | `RankedResult` |
| `dev.kdb.compression` | zstd decompress before upload |
| `dev.kdb.error` | `ComputeUnavailableException` |

-----

## 3. Public Interface

```kotlin
package dev.kdb.compute

/** Shared commonMain — implemented by WebGPU (27) and JVM (28). */
interface ComputeAdapter {
    val capabilities: ComputeAdapterCapabilities
  val isAvailable: Boolean
    val backend: ComputeBackend

    suspend fun ingestDeltaSegment(request: GpuSegmentIngestRequest): GpuSegmentHandle
    suspend fun releaseSegment(handle: GpuSegmentHandle)

    suspend fun vectorNearestNeighbours(request: GpuVectorSearchRequest): List<RankedResult>

    suspend fun shutdown()
}

data class ComputeAdapterCapabilities(
    val supportsVectorSearch: Boolean,
    val supportsDirectDeltaIngest: Boolean,
    val maxDimensions: Int,
    val maxBatchVectors: Int,
)

enum class ComputeBackend { CPU, CUDA, VULKAN, WEBGPU }

data class GpuVectorSearchRequest(
    val namespaceId: String,
    val queryVector: FloatArray,
    val dimensions: Int,
    val metric: dev.kdb.index.vector.VectorMetric,
    val k: Int,
    val candidateDocIds: List<dev.kdb.codec.KdbUuid>? = null,
    val atCommit: dev.kdb.codec.KdbHash,
)

data class GpuSegmentIngestRequest(
    val segment: DeltaSegmentRef,
    val compressedBytes: ByteArray,
)

data class GpuSegmentHandle(
    val segmentId: dev.kdb.codec.KdbUuid,
    val backend: ComputeBackend,
    val nativeHandle: Long,  // opaque pointer / VkBuffer handle
)

package dev.kdb.compute.jvm

/** Selects best backend: CUDA > Vulkan > CPU. */
fun createJvmComputeAdapter(config: JvmComputeConfig = JvmComputeConfig()): ComputeAdapter

data class JvmComputeConfig(
    val preferBackend: ComputeBackend? = null,
    val cudaDeviceIndex: Int = 0,
    val enableVulkan: Boolean = true,
    val cpuThreads: Int = Runtime.getRuntime().availableProcessors(),
)

/** Diagnostics for ops / JDBC meta. */
class ComputeAdapterInfo(
    val backend: ComputeBackend,
    val deviceName: String?,
    val totalVramBytes: Long?,
)
fun probeComputeAdapter(): ComputeAdapterInfo
```

Native targets reuse Vulkan path via `nativeMain` actual of `createJvmComputeAdapter` renamed `createNativeComputeAdapter` in follow-on — v1 JVM-only module acceptable if native ships CPU-only until Vulkan port lands.

-----

## 4. Data Structures

### Backend selection (startup)
```
if (config.preferBackend != null) use it
else if (CudaRuntime.load()) backend = CUDA
else if (VulkanRuntime.load()) backend = VULKAN
else backend = CPU
```

### CUDA path (v1)
- JCuda or custom JNI minimal: allocate device buffers, `cuMemcpyHtoD`, launch `cosineTopK` kernel.
- Pinned host memory for readback when k ≤ 64.

### Vulkan path (v1)
- LWJGL Vulkan or kotlinx-gpu experimental — **spec allows stub** returning CPU results while reporting `ComputeBackend.VULKAN` only after kernel executes; tests use CPU fallback.

### CPU path
- Kotlin multi-threaded brute force (same algorithm as `:kdb-index-vector` v1) — **reference implementation** for correctness tests.

### JNI boundary
All native pointers wrapped; `shutdown()` frees buffers and contexts.

-----

## 5. Contracts

### `createJvmComputeAdapter`
- **Postconditions:** Never returns null; `isAvailable` true for CPU backend always; GPU backends false when drivers missing.
- **Guarantee:** `vectorNearestNeighbours` on CPU path matches `:kdb-index-vector` flat search within 1e-5 float tolerance.

### Resource lifecycle
- **Preconditions:** `releaseSegment` called when segment evicted (Storage Manager 11b).
- **Postconditions:** `shutdown` idempotent; safe after all segments released.

### Thread safety
Adapter methods are thread-safe via internal mutex or per-stream queues; concurrent searches on same segment allowed.

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `ComputeUnavailableException` | Explicit shutdown; GPU required but disabled |
| `ComputeDispatchException` | CUDA error code, Vulkan validation error |
| `OutOfMemoryError` | Caught and wrapped as `ComputeDispatchException` |

-----

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `probe_cpuAlwaysAvailable` | no GPU drivers | `ComputeBackend.CPU`, isAvailable=true |
| 2 | `vectorSearch_matchesIndex` | same data as DefaultVectorIndexStore | equal top-k ids |
| 3 | `ingestAndSearch` | segment with 10 vectors | correct nearest |
| 4 | `releaseSegment_freesMemory` | ingest + release + OOM guard | no leak (heap stable) |
| 5 | `preferBackend_cuda` | CUDA present | backend CUDA |
| 6 | `preferBackend_vulkan` | CUDA absent, Vulkan ok | VULKAN |
| 7 | `shutdown_idempotent` | shutdown twice | no throw |
| 8 | `concurrentSearch` | 10 parallel | all complete |
| 9 | `l2Metric_cpuPath` | metric L2 | ordered by distance |
| 10 | `rejectWrongDimensions` | bad query | IAE |
| 11 | `gpuStorageEngine_integration` | GpuStorageEngine + adapter | ingest clears pending |
| 12 | `probeComputeAdapter_info` | any | non-null deviceName or "CPU" |

-----

## 8. Non-Goals

- **WebGPU** — Component 27.
- **Distributed multi-GPU** — single device v1.
- **FP16 Tensor cores / batched GEMM** — future optimisation.
- **Native CUDA on Kotlin/Native** — JVM module only v1; native uses Vulkan or CPU.
- **Packaging CUDA redistributable** — operator installs driver; optional `kdb-compute-jvm-cuda` artifact.

-----

## 9. Implementation Notes

### Gradle artifacts
```
:kdb-compute          commonMain API only
:kdb-compute-jvm      jvmMain CUDA/Vulkan/CPU
:kdb-compute-webgpu   jsMain (27)
```

Optional profile:
```kotlin
// gradle property kdb.cuda=true enables JCuda dependency
```

### Vulkan vs CUDA
Implement **CPU first**, then CUDA, then Vulkan — execution plan Phase 3 ordering.

### Security
Never execute untrusted WGSL/SPIR-V from wire; shaders ship with JAR.

### Native follow-on
`nativeMain` reuses Vulkan C interop from 10g POSIX patterns; share `cosineTopK` SPIR-V module.

-----

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| `:kdb-compute` API | 200 |
| CPU reference backend | 500 |
| CUDA backend | 1,200 |
| Vulkan backend | 800 |
| JNI + probes | 300 |
| Tests | 1,000 |
| **Total** | **~3,000** |
