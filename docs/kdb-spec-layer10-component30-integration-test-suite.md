# KDB Component Spec — Layer 10
## Component 30: Integration Test Suite
### `dev.kdb.integration`

**File:** `kdb-spec-layer10-component30-integration-test-suite.md`  
**Layer:** 10 — Tooling  
**Status:** Implementation-ready  
**Gradle module:** `:kdb-integration` (JVM `jvmTest` only)  
**Depends on:** All production layers 0–9 (smoke coverage), `:kdb-jdbc`, `:kdb-peer-sync`, `:kdb-stream`, `:kdb-transport-tcp`, `:kdb-hybrid-query`

-----

## 1. Purpose

Provides **cross-layer integration tests** that prove the engine works end-to-end: document write → commit → index → hybrid SQL → JDBC → wire stream → peer sync → TCP transport. Catches regressions that unit tests in individual modules miss.

-----

## 2. Dependencies

| Module | Role in tests |
|---|---|
| `:kdb-jdbc` | DriverManager memory URL, SELECT |
| `:kdb-peer-sync` | FULL_PEER pull/push |
| `:kdb-stream` | coordinator + subscriber |
| `:kdb-transport-tcp` | loopback peer sync |
| `:kdb-hybrid-query` | AT COMMIT queries |
| `:kdb-transaction` | transactional writes |

-----

## 3. Public Interface

This module exposes **no production API**. Test fixtures only:

```kotlin
package dev.kdb.integration.fixtures

/** Shared namespace + in-memory stack for integration scenarios. */
class IntegrationFixture(
    val namespaceId: String = "integration/test",
) {
    val runtime: dev.kdb.jdbc.EmbeddedKdbRuntime
    suspend fun writeJson(json: String): dev.kdb.codec.KdbUuid
    suspend fun head(): dev.kdb.codec.KdbHash
}

fun integrationFixture(namespaceId: String = "integration/test"): IntegrationFixture
```

-----

## 4. Data Structures

```kotlin
data class IntegrationTestReport(
    val scenario: String,
    val passed: Boolean,
    val detail: String? = null,
)
```

Scenarios are plain JUnit/`kotlin.test` classes — no custom runner v1.

-----

## 5. Contracts

- Each scenario is **hermetic**: own namespace id; no shared mutable state between tests.
- Tests use `runTest` or `runBlocking` with ≤5s implicit timeout per scenario.
- Failures print layer hint in assertion message (e.g. `"Layer 8 JDBC: ..."`).

-----

## 6. Error Cases

Tests **expect** failures only in negative cases (`@Test expected = ...`). Production exceptions propagate as test failures.

-----

## 7. Test Cases

| # | Scenario class | Validates |
|---|---|---|
| 1 | `Layer3WritePathTest` | Transaction → commit → DAG head |
| 2 | `Layer5SqlTest` | Index + SQL SELECT |
| 3 | `Layer6HybridQueryTest` | AT COMMIT suffix |
| 4 | `Layer7StreamTest` | Coordinator publish + subscriber |
| 5 | `Layer8JdbcTest` | DriverManager memory SELECT |
| 6 | `Layer8PeerSyncTest` | In-memory pull missing |
| 7 | `Layer9TcpPeerSyncTest` | Peer sync over TCP loopback |
| 8 | `Layer9TransportFramingTest` | transport-core framing (delegates) |
| 9 | `FullStackDocumentTest` | put → query → get round-trip |
| 10 | `NegativeNamespaceTest` | bad namespace rejected |

-----

## 8. Non-Goals

- Hibernate/jOOQ/Spring Data (listed in master §14 as follow-on).
- Browser Playwright tests.
- Performance benchmarks / load tests (see `:kdb-benchmark` JMH module).
- CI matrix for CUDA/WebGPU hardware.

-----

## 9. Implementation Notes

- Gradle: `plugins { kotlin-jvm }`; `dependencies { testImplementation(project(...)) }`.
- Register in root `settings.gradle.kts` as `:kdb-integration`.
- Run via `./gradlew :kdb-integration:test` in CI verification checklist.

-----

## 10. Estimated Lines

| Sub-component | Est. NBNC |
|---|---|
| Fixtures | 200 |
| Scenario tests | 1,200 |
| **Total** | **~1,400** |
