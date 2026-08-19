# KDB Component Spec — Layer 11 (proposed)
## Component 32: Stored Procedure Engine
### `dev.kdb.script`

**File:** `kdb-spec-layer11-component32-stored-procedures.md`
**Layer:** 11 — Server-Side Scripting (new; follows Layer 10 Tooling)
**Status:** Phases 1–4 implemented and tested (registry, sandboxed GraalVM runtime, host API with per-call authorization, implicit-transaction commit); Phase 5 (wire protocol / `SqlWireHost` integration), Phase 6 (audit logging, CLI) not yet started. See §11.
**Gradle module:** `:kdb-script` — interfaces, registry, host API, and the GraalVM runtime all live in one JVM-only module (a pragmatic simplification of the original two-module `:kdb-script` + `:kdb-script-graal` split; nothing here depends on `kdb-server`, so the split can still happen later at no cost).
**Depends on:** Layer 3 (`TransactionEngine`, `AuthEngine`/`AuthAction`), Layer 5 (`kdb-sql`, `kdb-index`), Layer 6 (`HybridQueryEngine`), `kdb-auth`
**Runs on:** JVM backend only, by design — see §1.2

-----

## 1. Purpose and scope

### 1.1 What this is

Server-side stored procedures: named, versioned scripts that run **inside the backend process**, close to storage, callable over the wire like a SQL statement. Scripts are written in a **restricted subset of JavaScript** (not full Node.js — no filesystem, no network, no process, no native modules) and interact with data exclusively through a host-provided `kdb` API object that proxies into the existing `TransactionEngine` / `HybridQueryEngine`.

### 1.2 Why JavaScript, and why backend-only

- KDB is Kotlin Multiplatform, but stored procedures are explicitly a **server-side** feature: they exist to push logic to the data instead of round-tripping documents to a client. Browser/embedded targets do not need to *host* procedures — they only need to *call* them, which is just another wire request.
- This means Component 32 does **not** need an `expect/actual` scripting engine across JVM/JS/Native. It needs exactly one embedding: **GraalVM Polyglot (`org.graalvm.js:js`) on the JVM**, running inside `kdb-server`. If a native-target backend is ever required, a second actual (QuickJS via cinterop) can be added later behind the same `dev.kdb.script` interface — but that is out of scope for v1.
- JavaScript (a restricted subset) is chosen over a bespoke DSL because: it's familiar to application developers, GraalVM's Polyglot API gives strong, mature sandboxing primitives (`HostAccess`, resource limits) for free, and it avoids inventing/maintaining a new language and parser.

### 1.3 Non-goals (v1)

- No arbitrary npm/Node module resolution, no `require`/`import` of anything but the injected `kdb` binding.
- No cross-procedure calling by name resolution at parse time (procedures may call other procedures only through an explicit, authorized `kdb.callProc(...)`, itself subject to the same authorization path — no macro-style inlining).
- No client-side (browser/embedded) procedure execution in v1.
- No long-running/background procedures (cron-like jobs) in v1 — every invocation is a bounded, synchronous-from-the-caller's-perspective request/response over the wire, like `SqlExec`.
- Procedures do not get elevated ("owner") privileges — see §5.2. This is a deliberate scope cut to avoid confused-deputy problems; it can be revisited later as an explicit, audited opt-in.

-----

## 2. Where this sits in the existing architecture

```
 Wire client
     │  ProcExec { namespace, procName, args }
     ▼
 SqlWireHost / new ProcWireHost   (kdb-server)
     │  1. authenticate connection (existing AuthEngine)
     │  2. authorize AuthAction.ProcExec(namespace, procName)   ← NEW action
     ▼
 ProcedureRegistry.get(namespace, procName)      (kdb-script)
     │  loads compiled/cached script body + declared permissions
     ▼
 ProcedureRuntime.invoke(principal, args, ...)   (kdb-script-graal)
     │  builds a fresh sandboxed Polyglot Context per call
     │  binds a per-call `kdb` host object closed over (principal, namespace, txn)
     ▼
 kdb host API  →  HybridQueryEngine.execute() / TransactionEngine  (existing engines)
     │  every call re-enters dev.kdb.auth.Authorizer.authorize(principal, action)
     ▼
 StorageAdapter / commit path (unchanged)
```

Key point: **the script never talks to storage directly**. It only ever calls Kotlin-side host functions, and those host functions are the *same* authorized entry points (`HybridQueryEngine`, `TransactionEngine`) already used by ordinary SQL/document requests. Component 32 adds no new privileged path into storage.

-----

## 3. Public interface (draft)

```kotlin
package dev.kdb.script

import dev.kdb.auth.Principal

/** A stored, versioned procedure definition. */
public data class ProcedureDefinition(
    val namespaceId: String,
    val name: String,
    val source: String,               // restricted-JS source
    val paramSchema: ProcParamSchema?, // optional declared arg shape, validated before invoke
    val requiredPermission: String?,  // e.g. "proc:orders/*" — checked in addition to data-level checks
    val revision: Long = 1L,
    val createdBy: String,
    val createdAt: Long,
)

public interface ProcedureRegistry {
    suspend fun put(def: ProcedureDefinition): ProcedureDefinition   // bumps revision, versioned like docs
    suspend fun get(namespaceId: String, name: String): ProcedureDefinition?
    suspend fun list(namespaceId: String): List<String>
    suspend fun delete(namespaceId: String, name: String): Boolean
}

public data class ProcResult(
    val value: kotlinx.serialization.json.JsonElement,
    val logs: List<String>,          // captured console.log output, size-capped
)

public sealed class ProcException(message: String) : RuntimeException(message) {
    public class NotFound(namespace: String, name: String) : ProcException("no such procedure: $namespace/$name")
    public class CompileError(detail: String) : ProcException(detail)
    public class Timeout(millis: Long) : ProcException("procedure exceeded ${millis}ms")
    public class ResourceLimitExceeded(detail: String) : ProcException(detail)
    public class ScriptRuntimeError(detail: String) : ProcException(detail)
    public class Denied(detail: String) : ProcException(detail)   // authorization failure inside the script
}

public interface ProcedureRuntime {
    suspend fun invoke(
        principal: Principal,
        namespaceId: String,
        name: String,
        args: kotlinx.serialization.json.JsonElement,
        limits: ProcLimits = ProcLimits.DEFAULT,
    ): ProcResult
}

public data class ProcLimits(
    val wallClockMillis: Long = 5_000,
    val maxHostCalls: Int = 1_000,      // bounds fan-out of kdb.get/put/query calls
    val maxLogBytes: Int = 64 * 1024,
    val maxHeapMb: Int = 64,
) {
    public companion object { public val DEFAULT: ProcLimits = ProcLimits() }
}
```

New `AuthAction` variant (added to `dev.kdb.auth.AuthAction`, mirroring `SqlExec`):

```kotlin
public data class ProcExec(
    val namespace: String,
    val procName: String,
    val readOnly: Boolean,
) : AuthAction()

public data class ProcManage(val namespace: String) : AuthAction()   // define/update/delete a procedure
```

-----

## 4. The `kdb` host API exposed inside scripts

A minimal, explicit surface — not a general JS binding to Kotlin objects (GraalVM `HostAccess` is configured to expose *only* this object, nothing else of the JVM):

```js
// Inside a stored procedure, `kdb` and `args` are the only injected globals.
// No `require`, no `Java.type`, no filesystem/network globals — GraalVM
// HostAccess.EXPLICIT + a stripped-down Context builder remove all of that.

function main(args) {
  const doc = kdb.get(args.id);                 // -> parsed JS object, or null
  if (!doc) throw new Error("not found");

  kdb.put({ ...doc, status: "shipped" });        // versioned write, goes through TransactionEngine

  const rows = kdb.query(
    "SELECT id, total FROM orders WHERE status = ?", ["pending"]
  );                                             // -> array of row objects, via HybridQueryEngine

  kdb.log(`processed ${rows.length} pending orders`);
  return { ok: true, count: rows.length };
}
```

Host API surface (v1):

| Function | Backing call | Notes |
|---|---|---|
| `kdb.get(id)` | `HybridQueryEngine` doc read | namespace is fixed to the procedure's namespace at invocation |
| `kdb.put(doc)` | `TransactionEngine` write | participates in the procedure's implicit transaction (§6) |
| `kdb.delete(id)` | `TransactionEngine` write | ditto |
| `kdb.query(sql, params)` | `HybridQueryEngine.execute` | parameterized only — **no string-built SQL**, see §5.3 |
| `kdb.callProc(name, args)` | recurse into `ProcedureRuntime.invoke` | re-authorized independently; depth-limited (default 3) to prevent runaway recursion |
| `kdb.log(msg)` | appends to `ProcResult.logs`, capped by `ProcLimits.maxLogBytes` | no external sink; not `console.log` writing to server stdout |

Every one of these is a thin wrapper that (a) increments the call's `maxHostCalls` budget, (b) calls `Authorizer.authorize(principal, action)` before doing anything, (c) delegates to the existing engine. No host function bypasses the authorizer, including the one calling itself via `callProc`.

**`kdb_id` round-tripping (implementation note).** `kdb_id` is a SQL pseudo-column (Component 15) — it is not part of a document's own JSON body, so `SELECT _doc FROM ns WHERE kdb_id = ?` never returns it embedded in the result. `kdb.get(id)` merges it in (`{...doc, kdb_id: id}`) purely so that `kdb.put(doc)` can tell, by the presence of that field, whether to `UPDATE ... WHERE kdb_id = ?` or `INSERT`; `kdb.put` strips it back out of the body before writing `_doc` so stored content isn't polluted by the injected field. A script that constructs a brand-new object (rather than spreading a `kdb.get` result) simply gets an `INSERT`, exactly as documented in §7.1.

-----

## 5. Access control — the part that must not be fudged

### 5.1 Two layers of authorization, both mandatory

1. **Invocation gate** — before the script is even loaded/compiled: `Authorizer.authorize(principal, AuthAction.ProcExec(namespace, name, readOnly))`. This answers "is this principal allowed to run this named procedure at all." Mirrors the existing `SqlExec` check in `SqlWireHost`.
2. **Per-operation gate** — every single `kdb.*` call the script makes during execution re-invokes the authorizer for the *specific* action being attempted (`SqlExec` for `kdb.query`, an equivalent doc-level action for `kdb.get`/`kdb.put`/`kdb.delete`). This is not optional and is not cached across calls within one invocation — a procedure that writes to five different namespaces in a loop gets checked five times, not once.

This two-layer design is deliberate: (1) alone would let an authorized-to-invoke script silently do anything server-side; (2) alone would mean an unauthorized principal could still trigger compilation/execution overhead (a minor DoS surface) before being denied. Both checks use the **caller's own `Principal`**, obtained from the connection/session exactly as it is for ordinary SQL requests — never a principal embedded in or elevated by the procedure definition.

### 5.2 No privilege escalation via procedure ownership

A common stored-procedure pitfall (see Postgres `SECURITY DEFINER`) is letting a procedure run with its *creator's* rights so lower-privileged callers can do more than they could directly. **v1 deliberately does not offer this.** Every procedure runs with the *invoking* principal's rights, full stop. If a future version wants a `SECURITY DEFINER`-style escalation, it must be:
- an explicit, separately-permissioned opt-in (`AuthAction.ProcManage` alone should not be sufficient to grant it),
- audited distinctly (every call logged with both the invoker and the "runs as" principal),
- and is out of scope here.

### 5.3 No dynamic SQL / injection surface

`kdb.query` takes `(sql, params)` with positional/named parameter binding through the existing `HybridQueryEngine` parser — never raw string concatenation of `args` into SQL text. The script has no way to obtain a "raw" query execution path; the binding layer is the only door.

### 5.4 Sandbox boundaries (script can't get around the API)

Configured via GraalVM `Context.newBuilder("js")`:
- `HostAccess.EXPLICIT` (or a custom `HostAccess` policy) exposing *only* the `kdb` and `args` bindings — no reflective access to any other JVM class.
- `allowIO(false)`, `allowCreateProcess(false)`, `allowNativeAccess(false)`, `allowHostClassLookup { false }`.
- No `Java.type(...)` / Nashorn-style interop enabled.
- Resource limits via GraalVM's `ResourceLimits` (CPU time / statement count) plus a wall-clock watchdog that force-closes the `Context` from a coroutine `withTimeout`.
- A fresh `Context` per invocation (no state leaks between calls; no shared globals across principals/tenants).
- `kdb.callProc` depth-limited and its own `maxHostCalls` budget shared with the parent invocation, so nested calls can't multiply the total amount of work unboundedly.

### 5.5 Definition-time controls

- Writing/updating a procedure (`ProcManage`) is itself authorized and separate from the ability to *run* it — an operator can grant "run orders/shipOrder" without granting "edit any procedure in orders/*".
- Procedure source is stored as a versioned document (reuses the existing document/commit model — Component 3), so every edit is in the commit history with author/timestamp, same as data. No separate ad hoc file-based deploy mechanism.
- (Recommended, not required for v1): static lint pass at `put()` time rejecting obviously-disallowed syntax (e.g. `eval`, `Function(...)`, `import`) as defense in depth — belt-and-suspenders on top of the runtime sandbox, not a substitute for it.

### 5.6 Audit logging

Every `kdb.*` call made during a procedure invocation is recorded (principal id, namespace, procedure name+revision, action kind, target id/namespace, allow/deny, timestamp) through the same audit hook the rest of the system uses (if one doesn't exist yet centrally, this is the forcing function to add it — see `kdb-spec-layer0-error-model.md` / existing `KdbException` conventions for the error shape to reuse).

-----

## 6. Transaction semantics

- By default, a procedure invocation runs inside **one implicit transaction**: all `kdb.put`/`kdb.delete` calls made during the script are staged and committed atomically when `main()` returns successfully, or rolled back if the script throws, times out, or is denied mid-execution. This matches the mental model of `SqlExec` inside an active session in `SqlWireHost`.
- `kdb.query` reads see a consistent snapshot for the duration of the call (reuses `HybridQueryEngine`'s existing checkout/read-commit resolution — Component 17).
- Conflict handling on commit reuses the existing `ConflictPolicy` machinery (Component 7) — a procedure's implicit transaction is not a new commit mechanism, just a caller of the existing one.

-----

## 7. Wire protocol addition

Extend the existing frame set (Component 21, `:kdb-wire`) with two message pairs, following the shape of the existing `SqlExec`/`SqlResult`:

```
ProcExec    { namespace, procName, argsJson }        → ProcResult { valueJson, logs[] } | ProcError { code, message }
ProcDefine  { namespace, procName, source, params? } → ProcDefineAck { revision } | ProcError
```

`SqlWireHost` (or a sibling `ProcWireHost`) handles these the same way it already handles `handleSqlExec`: authenticate → authorize → delegate → map exceptions to `ProcError` using existing `KdbErrorCode` conventions.

### 7.1 Worked example — define then execute

**Step 1 — write the procedure.** A restricted-JS source file, `shipOrder.js`. `main` is the only required export; `args` and `kdb` are the only globals available:

```js
// shipOrder.js — namespace: orders
function main(args) {
  const doc = kdb.get(args.id);
  if (!doc) throw new Error(`order ${args.id} not found`);
  if (doc.status !== "pending") throw new Error(`order ${args.id} is not pending`);

  kdb.put({ ...doc, status: "shipped", shippedAt: args.now });

  const stillPending = kdb.query(
    "SELECT count(*) AS n FROM orders WHERE status = ?", ["pending"]
  );

  kdb.log(`shipped ${args.id}; ${stillPending[0].n} orders still pending`);
  return { ok: true, id: args.id };
}
```

**Step 2 — define it (`ProcDefine`).** Over the wire (CLI shown as the human-facing equivalent — `kdb-cli`, Component 29, would add a `proc` subcommand that just encodes this frame):

```
$ kdb proc put orders shipOrder ./shipOrder.js
```

which the client encodes as, and the server handles as:

```kotlin
// client → server
WireMessage.ProcDefine(namespace = "orders", procName = "shipOrder", source = fileText, params = null)

// server (ProcWireHost / SqlWireHost)
sqlAuth.authorize(principal, AuthAction.ProcManage("orders"))   // can this principal edit procs in `orders`?
val def = ProcedureDefinition(
    namespaceId = "orders", name = "shipOrder", source = fileText,
    paramSchema = null, requiredPermission = "proc:orders/*",
    createdBy = principal.id, createdAt = now(),
)
registry.put(def)   // versioned document write — revision 1, then 2, 3... on each redefine
→ WireMessage.ProcDefineAck(revision = 1)
```

`registry.put` goes through the ordinary document/commit path (Component 3), so `kdb log --namespace orders` shows the procedure's own edit history alongside data commits.

**Step 3 — execute it (`ProcExec`).**

```
$ kdb proc exec orders shipOrder '{"id": "ord-42", "now": 1755600000000}'
```

```kotlin
// client → server
WireMessage.ProcExec(namespace = "orders", procName = "shipOrder", argsJson = """{"id":"ord-42","now":1755600000000}""")

// server
sqlAuth.authorize(principal, AuthAction.ProcExec("orders", "shipOrder", readOnly = false))   // gate 1: may this principal run this proc?
val def = registry.get("orders", "shipOrder") ?: throw ProcException.NotFound(...)
val result = procedureRuntime.invoke(principal, "orders", "shipOrder", parsedArgs, ProcLimits.DEFAULT)
→ WireMessage.ProcResult(valueJson = """{"ok":true,"id":"ord-42"}""", logs = ["shipped ord-42; 3 orders still pending"])
```

**Step 4 — what happens inside `invoke` (gate 2, per operation):**

```
GraalProcedureRuntime.invoke(principal, "orders", "shipOrder", args, limits)
  1. open a fresh sandboxed Context (HostAccess.EXPLICIT, no IO/net/process, ResourceLimits from `limits`)
  2. bind `kdb` = HostBindings(principal, namespace="orders", txn=<new implicit tx>, budget=limits.maxHostCalls)
  3. bind `args` = parsed JSON args
  4. compile + call main(args), under a `withTimeout(limits.wallClockMillis)`

  inside the script:
    kdb.get("ord-42")
      → HostBindings.get: authorize(principal, AuthAction.SqlExec("orders", readOnly=true)) [or a doc-level read action]
      → HybridQueryEngine read, snapshot-consistent for the invocation
    kdb.put({...})
      → HostBindings.put: authorize(principal, AuthAction.SqlExec("orders", readOnly=false))
      → staged into the implicit transaction, not yet committed
    kdb.query("SELECT count(*)...", [...])
      → HostBindings.query: authorize(principal, AuthAction.SqlExec("orders", readOnly=true))
      → HybridQueryEngine.execute (parameterized — args never concatenated into SQL text)
    kdb.log(...)
      → appended to ProcResult.logs, capped at limits.maxLogBytes

  5. main() returns normally → commit the implicit transaction (§6)
  6. return ProcResult(value, logs) to the wire host
```

If the same principal were authorized for `ProcExec("orders", "shipOrder")` but *not* for writes to `orders` (e.g. a read-only role), step 4's `kdb.put` call would fail its own `authorize(...)` and raise `ProcException.Denied` — the procedure aborts, the implicit transaction rolls back, and the client gets a `ProcError`. Being allowed to *invoke* the procedure never implies being allowed to *do* what it tries to do.

-----

## 8. Module layout (proposed)

```
kdb-script/                      (commonMain-ish, but effectively JVM-targeted for v1)
  ProcedureDefinition, ProcParamSchema, ProcResult, ProcException, ProcLimits
  ProcedureRegistry (interface) + in-memory/document-backed impl (reuses StorageAdapter, like NamespacePolicyRegistry)
  ProcedureRuntime (interface)

kdb-script-graal/                (JVM only, depended on by kdb-server)
  GraalProcedureRuntime : ProcedureRuntime
  HostBindings — builds the `kdb` object per invocation, wraps HybridQueryEngine + TransactionEngine + Authorizer
  SandboxConfig — Context.Builder setup, ResourceLimits, HostAccess policy

kdb-server/
  ProcWireHost (or extend SqlWireHost) — wire message handling, reuses SqlAuthSupport pattern
```

Dependency direction mirrors Component 18 (Namespace Policy Engine): `kdb-script` depends on `kdb-auth`, `kdb-storage`, `kdb-transaction`, `kdb-hybrid-query`; `kdb-script-graal` depends on `kdb-script` + GraalVM `org.graalvm.js:js`; only `kdb-server` depends on `kdb-script-graal`.

-----

## 9. Implementation phases

1. **Registry + storage** — `ProcedureDefinition` as a versioned document (reuse Component 3 document/commit model), `ProcedureRegistry` backed by `StorageAdapter` (pattern-match `NamespacePolicyRegistry`). No execution yet.
2. **Sandboxed runtime, no host API** — stand up `GraalProcedureRuntime` with a locked-down `Context`, prove scripts can run, return a value, and are killed on timeout/resource limits, with zero data access. This validates the sandbox boundary in isolation before any data path is wired in.
3. **Host API + per-call authorization** — add `kdb.get/put/delete/query/log`, each wrapping the real engines and each independently calling `Authorizer.authorize`. Unit tests specifically assert that a principal authorized for `ProcExec` but *not* for the underlying `SqlExec`/doc action is denied mid-script, not just at invocation.
4. **Transaction integration** — implicit transaction wrapping per §6, commit/rollback on script success/failure.
5. **Wire protocol + `SqlWireHost`/`ProcWireHost` integration** — `ProcExec`/`ProcDefine` frames, error mapping.
6. **`kdb.callProc` + depth/budget limits**, audit logging, CLI (`kdb proc put/list/exec`) if desired.

Each phase should ship with tests before the next starts, per the project's existing "spec vs. implementation" discipline — this document is the spec; implementation follows in a separate session per house convention (see master spec §0: "Never mix spec generation and implementation in the same session").

-----

## 10. Open questions for follow-up

- Should `kdb.query` results be capped in row count by default to bound memory, independent of `maxHostCalls`?
- Do we want a `dry-run`/`explain` mode for `ProcExec` (mirrors `HybridQueryEngine.explain`) so operators can see what a procedure *would* touch before granting broader `ProcExec` grants?
- Is per-procedure rate limiting (calls/sec per principal) needed at the wire layer, or is `ProcLimits` per-invocation enough?
- Native/embedded execution of procedures (QuickJS actual) — worth doing, or keep this permanently server-only?

-----

## 11. Implementation status

Phases 1–4 are implemented in `:kdb-script`, with tests (`ProcedureRegistryTest`, `GraalProcedureRuntimeTest`) exercising registry versioning, the full define→execute flow against real (in-memory) engines, sandbox escape resistance (`Java.type(...)` fails), wall-clock timeout interruption of a busy loop, and — the load-bearing case — a principal authorized to *invoke* a procedure but not to *write* getting denied mid-script with the transaction left uncommitted.

Two real findings surfaced while wiring this to the actual engines, both fixed:

1. **`kdb-sql`'s `DmlExecutor.evalDocAssignment` never handled a bound parameter** — `UPDATE ns SET _doc = ? WHERE kdb_id = ?` silently left the document unchanged (it only handled `SqlExpr.Literal`/`SqlExpr.FunctionCall`, falling through to `currentJson` for `SqlExpr.Parameter`). This blocked exactly the parameterized-write pattern §5.3 requires (`kdb.put` must never string-build SQL), so it's fixed at the source in `kdb-sql`, not worked around in `kdb-script`. Any other caller doing a parameterized `_doc` update gets the same fix.
2. **`kdb_id` isn't part of a document's JSON body** — see the implementation note in §4. `kdb.get` embeds it, `kdb.put` strips it back out.

Remaining before this is callable over the wire: Phase 5 (`WireMessage.ProcExec`/`ProcDefine`, `SqlWireHost`/`ProcWireHost` integration) and Phase 6 (audit logging, `kdb-cli proc` subcommand). The `HybridScriptDataAccess`/`GraalProcedureRuntime` pair is wire-protocol-agnostic, so Phase 5 is additive — no changes anticipated to the code landed so far.
