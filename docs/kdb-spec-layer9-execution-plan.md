# KDB Layer 9 — Implementation Execution Plan

**Status:** Implemented (first Kotlin cut — May 2026)  
**Master spec:** `docs/kdb-spec.md` §16.1 (Layer 9), §0 (session state)  
**Depends on:** Layer 7–8 complete (Components 20–24; `WireTransport` in §17)

-----

## Scope

Layer 9 delivers **platform adapters** — real network transport and optional GPU compute:

| Component | Module | Spec file |
|---|---|---|
| (shared) Frame streaming | `:kdb-transport-core` | §4 in component 26 spec |
| 26 Transport — TCP | `:kdb-transport-tcp` | `kdb-spec-layer9-component26-transport-tcp.md` |
| 25 Transport — WebSocket | `:kdb-transport-ws` | `kdb-spec-layer9-component25-transport-websocket.md` |
| (shared) Compute API | `:kdb-compute` | §3 in component 28 spec |
| 27 Compute — WebGPU | `:kdb-compute-webgpu` | `kdb-spec-layer9-component27-compute-webgpu.md` |
| 28 Compute — CUDA/Vulkan/CPU | `:kdb-compute-jvm` | `kdb-spec-layer9-component28-compute-cuda-vulkan.md` |

**Not in Layer 9:** CLI (29), integration test suite (30), TLS/auth frameworks, WebRTC.

-----

## Normative implementation order

Product priority (master §14 Phase 1): **TCP before WebSocket** for backend JDBC/network. Compute can proceed in parallel once `:kdb-compute` API is frozen.

### Phase 0 — Shared transport framing

1. Create `:kdb-transport-core` with `FrameStreamReader`, `FrameStreamWriter`, `TransportConnectOptions`.
2. Unit tests: framing cases 1–6 from component 26 §7 (no sockets).
3. No §17 paste yet (internal module).

**Exit criteria:** Split/coalesced TCP byte streams reassemble valid frames.

### Phase 1 — TCP transport (26)

1. Create `:kdb-transport-tcp` (`jvmMain` first).
2. Implement `TcpWireTransport.connect` + `listen` on loopback.
3. Wire `WireConnection.send` / `incoming()` through framer.
4. **Tests:** cases 7–11 from spec §7.
5. Integration: `PeerSyncClient` over `kdb-tcp://127.0.0.1:port`.
6. Add `nativeMain` socket backend (case 12).
7. Paste `TcpWireTransport` + URI types into master §17 → Layer 9.

**Exit criteria:** Two JVM peers sync commits over TCP; stream coordinator fan-out works.

### Phase 2 — WebSocket transport (25)

1. Create `:kdb-transport-ws` depending on `:kdb-transport-core`.
2. Implement `jsMain` client + `jvmMain` in-process test server.
3. **Tests:** 12 cases from spec §7.
4. Integration: browser `StreamSubscriber` against JVM coordinator (manual or Playwright later).
5. Paste WebSocket types into §17.

**Exit criteria:** Binary WS round-trip equals TCP framing semantics.

### Phase 3 — Compute API + CPU backend (28a)

1. Create `:kdb-compute` (commonMain interface only).
2. Create `:kdb-compute-jvm` with **CPU reference** `ComputeAdapter` first.
3. Hook `GpuStorageEngine` to optional adapter (ingest queue drains).
4. Hook `:kdb-index-vector` to call adapter when `supportsGpuBulkRead`.
5. **Tests:** cases 1–4, 7, 9–12 from component 28 §7.

**Exit criteria:** Vector search results match CPU index without GPU drivers.

### Phase 4 — WebGPU (27)

1. Create `:kdb-compute-webgpu` (`jsMain`).
2. Implement availability probe + CPU fallback factory.
3. WGSL cosine kernel + readback top-k.
4. **Tests:** jsTest with fallback; case 12 integration.
5. Paste WebGPU factory into §17.

**Exit criteria:** jsTest passes in CI via CPU fallback; manual browser test shows GPU path when available.

### Phase 5 — CUDA / Vulkan (28b)

1. Add CUDA backend behind Gradle property `kdb.cuda=true`.
2. Add Vulkan backend or stub that delegates to CPU until SPIR-V ready.
3. `probeComputeAdapter()` for ops logging.
4. **Tests:** case 5–6 when hardware present; skip otherwise.

**Exit criteria:** JVM with CUDA sees `ComputeBackend.CUDA` and passes case 2.

### Phase 6 — Master spec + network JDBC stub

1. Update §0 checklist: Layer 9 specs `[x]`, implementation `[x]` per component.
2. Update §16.1 Layer 9 block + implementation order.
3. Update §14 Layer 9 subtotal (~9,500 NBNC).
4. Update §17 Layer 9 interfaces (final).
5. Optional: extend `:kdb-jdbc` `JdbcMode.NETWORK` to use `TcpWireTransport` (thin — may be Layer 8 follow-on).

-----

## Parallelism

| Can parallelize | Cannot start until |
|---|---|
| transport-core (0) | — |
| TCP jvmMain (1) | transport-core |
| WebSocket jsMain (2) | transport-core |
| `:kdb-compute` API (3a) | — |
| CPU compute backend (3) | `:kdb-compute` API |
| WebGPU (4) | compute API + vector index hooks |
| CUDA/Vulkan (5) | CPU backend tests green |

**Rule:** Do not mark Layer 9 complete until TCP + at least one compute backend (CPU) integrate with `PeerSyncClient` and `GpuStorageEngine`.

-----

## Gradle modules

```kotlin
include(":kdb-transport-core")
include(":kdb-transport-tcp")
include(":kdb-transport-ws")
include(":kdb-compute")
include(":kdb-compute-jvm")
include(":kdb-compute-webgpu")
```

```
:kdb-transport-core   → kdb-wire, kdb-error
:kdb-transport-tcp    → kdb-transport-core, kdb-stream
:kdb-transport-ws     → kdb-transport-core, kdb-stream
:kdb-compute          → kdb-storage, kdb-index, kdb-codec, kdb-error
:kdb-compute-jvm      → kdb-compute, kdb-compression, kdb-index-vector
:kdb-compute-webgpu   → kdb-compute, kdb-compression
```

-----

## Estimated NBNC (Layer 9)

| Component | Lines |
|---|---|
| transport-core (shared) | ~350 |
| 26 TCP | ~1,650 |
| 25 WebSocket | ~1,500 |
| `:kdb-compute` API | ~200 |
| 27 WebGPU | ~3,000 |
| 28 JVM compute | ~2,800 |
| **Layer 9 subtotal** | **~9,500** |

Cumulative after Layer 9 specs: **~131,850** (see master §14).

-----

## Session prompts (copy-paste)

**Spec session (done):**
```
Generate implementation-ready component specs for Layer 9: WebSocket transport,
TCP transport, WebGPU compute, CUDA/Vulkan compute. Follow Section 16.2 structure.
Save execution plan kdb-spec-layer9-execution-plan.md.
```

**Implement Phase 0–1:**
```
Implement :kdb-transport-core and :kdb-transport-tcp per layer 9 specs.
Use WireTransport from :kdb-stream. Loopback integration test with PeerSyncClient.
```

**Implement Phase 2:**
```
Implement :kdb-transport-ws per kdb-spec-layer9-component25-transport-websocket.md.
jsMain client + jvmTest in-process server.
```

**Implement Phase 3–5:**
```
Implement :kdb-compute API, CPU JVM backend, WebGPU jsMain, optional CUDA per
kdb-spec-layer9-component27/28. Hook GpuStorageEngine and vector index.
```

-----

## Verification checklist

- [x] `./gradlew :kdb-transport-core:jvmTest`
- [x] `./gradlew :kdb-transport-tcp:jvmTest`
- [x] `./gradlew :kdb-transport-ws:jvmTest`
- [x] `./gradlew :kdb-compute-jvm:test`
- [x] `./gradlew :kdb-compute-webgpu:jsTest` (CPU fallback)
- [x] `./gradlew :kdb-peer-sync:jvmTest` + TCP integration test
- [x] Layer 7–8 tests still pass
- [x] Master spec §0 Layer 9 checklist updated
- [x] Master spec §17 Layer 9 interfaces populated
- [x] Master spec §14 includes Layer 9 subtotal row
