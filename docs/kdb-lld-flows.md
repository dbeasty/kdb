# KDB — Low-Level Design

## Part 2 · Flows

**Parent:** [Part 0 — Index & architecture](kdb-lld.md) · **See also:**
[High-level architecture](kdb-architecture.md) · [Components](kdb-lld-components.md) ·
[Concurrency](kdb-lld-concurrency.md) · [Storage](kdb-lld-storage.md) ·
[Query](kdb-lld-query.md) · [Protocol](kdb-lld-protocol.md) ·
[User guide](kdb-user-guide.md)

Every significant path through the system, as a sequence. Each flow names the exact functions
involved so the diagram can be walked in the source.

**Flow index**

| # | Flow | Entry point |
|---|------|-------------|
| 1 | Opening an embedded file runtime | `embed.OpenFileRuntimeWithOptions` |
| 2 | Embedded write (`kdb put`) | `embed.PutJSONDocument` |
| 3 | Transaction commit — the full engine sequence | `transaction.defaultEngine.Commit` |
| 4 | Blob write and durability | `ServerEngine.WriteBlob` |
| 5 | Commit durability (delta log) | `PersistingCommitDAG.PersistAsync` |
| 6 | Connect, handshake, session begin | `sqlWireConnHandler.handleHandshake` |
| 7 | SQL `SELECT` over the wire | `sqlWireConnHandler.execRead` |
| 8 | `INSERT` + `TX_COMMIT` | `execInsert` → `handleTxCommit` |
| 9 | Point read (`DOCUMENT_GET`) and `UPSERT` | `handleDocumentGet` / `handleUpsert` |
| 10 | Conflict and client retry | `detectConflicts` → `ConflictReport` |
| 11 | Crash recovery / delta replay | `embed.replayDeltaNamespace` |
| 12 | Peer sync (Mode 3) | `peersync.Client.PullMissing` / host `CommitPush` |
| 13 | Stream Mode 1 fan-out and Mode 2 write-back | `StreamHub.Publish` / `TransactionReplayer` |
| 14 | Memory pressure and shedding | `MemoryGuard.observe` → `Admission` |
| 15 | Orderly shutdown and abort | SIGTERM path / `AbortWatchdog` |
| 16 | Verify and repair | `integrity.Verify` → `integrity.Repair` |
| 17 | Backup and restore | `backup.Create` → `recovery.HybridRestore` |
| 18 | Schema DDL and migration | `DDLExecutor` / `schema.ApplyMigration` |
| 19 | RBAC enforcement points | `auth.Engine` at four layers |

-----

## 1. Opening an embedded file runtime

The single most consequential startup path: it takes the directory lock, rebuilds all in-memory
state from the durable log, and decides whether the namespace is openable at all.

```mermaid
sequenceDiagram
    autonumber
    participant C as Caller (CLI / driver / service)
    participant E as embed.OpenFileRuntimeWithOptions
    participant L as dirLock (.kdb.lock)
    participant IO as FileBackedPlatformIO / OSByteStore
    participant F as engine.DefaultFactory
    participant R as replayDeltaNamespace
    participant D as InMemoryCommitDag
    participant S as ServerEngine

    C->>E: dataRoot, catalog, namespace, schema, opts
    E->>L: acquireDirLock(dataRoot)  (flock, exclusive)
    alt lock held by another process
        L-->>C: DataDirectoryLocked (4102)
    end
    E->>E: ensureNamespaceDirs — ns/[namespace]/delta, /meta, meta.json
    E->>IO: open store (OS, + S3 replicas if KDB_S3_* set)
    E->>F: Open(namespace, StorageEngineConfig)
    F->>S: NewServerEngine(+WAL)
    F->>F: delta.Factory.OpenWriter → next sequence
    Note over F: refuses with LegacySegmentFormatError<br/>if any pre-Layer-13 segment name exists
    E->>D: NewInMemoryCommitDag(namespace)  → genesis + main
    E->>R: replayDeltaNamespace(dag, storage, deltaReader)
    R->>IO: ListSegments (sequence order)
    loop each segment, oldest first
        R->>R: ReadAll → ScanSegmentBytes
        alt torn tail on the newest segment
            R->>R: log and keep commits read before it
        else corruption elsewhere
            R-->>C: error naming kdb-inspect repair-segments
        end
    end
    R->>R: applyCommitsTopologically
    loop rounds until no pending commits
        R->>S: PutDocument / DeleteDocument per op
        R->>S: CommitTree(parentTreeHash)
        R->>D: PutDocumentTree, PutCommit(requireParents=true), SetHead("main")
    end
    E->>E: wrap DAG in PersistingCommitDAG (+ commitLogWriter)
    E-->>C: EmbeddedKdbRuntime{DAG, Storage, Schema, DataRoot}
```

**Why replay is round-based rather than file-ordered.** Segment file order *is* commit order by
construction (zero-padded sequence names), but replay does not depend on it: a commit is applied
only once every parent is present. A bug in ordering therefore degrades to a slower open, not a
permanently unopenable namespace — which is exactly the failure mode Layer 13 Component 47 was
written to remove.

**Close is the mirror image** and is deliberately *not* load-bearing: `Close()` drains the commit
log, flushes and seals the active delta segment, closes the engine handle (final WAL sync), then
releases the directory lock. Skipping all of it (`kill -9`) is safe — it only means the next open
pays for a torn-tail scan and leaves one extra small segment behind.

-----

## 2. Embedded write — `kdb put`

```mermaid
sequenceDiagram
    autonumber
    participant U as kdb CLI
    participant P as embed.PutJSONDocument
    participant DOC as document
    participant S as storage.Adapter
    participant D as PersistingCommitDAG
    participant W as commitLogWriter
    participant IO as delta segment

    U->>P: namespace, json text
    P->>P: resolveDocID (json "id" or RandomUUID)
    P->>DOC: EnsureIDInJSON, FromJSONWithID
    P->>S: PutDocument (staged only)
    P->>D: Head() → parent commit
    P->>S: CommitTree(parent.DocumentTreeHash)
    S-->>P: new DocumentTree (O(depth) trie update)
    P->>D: AppendCommit(tx, head, tree, nil, "")
    D->>D: delegate.AppendCommit → hash, index by txID, move main
    D->>W: EnqueueAsync(DeltaRecord{hash, payload})
    W->>IO: PageCodec.Frame(payload, zstd) → AppendToSegment
    W->>IO: FlushSegment (fsync, coalesced across the batch)
    W-->>D: ack
    D-->>P: Commit
    P-->>U: {docId, docIdShort, commit}
```

`kdb get` resolves its argument as either a full UUID or an **unambiguous 8+ hex prefix**
(`resolveDocSelector`), then reads through `storage.Adapter.GetDocument` at the head commit's
tree hash.

-----

## 3. Transaction commit — the full engine sequence

This is the heart of the write path. It runs identically for embedded and server callers; the
server merely wraps it in admission, authorization, and the write gate.

```mermaid
flowchart TD
    A[Commit tx, dag, store, schema, targetHead] --> B{tx.BaseVersion present in DAG?}
    B -- no --> B1[BaseNotFoundError]
    B -- yes --> C{txIndex hit with identical parents?}
    C -- yes --> C1[ResultSuccess with the original commit<br/>idempotent retry]
    C -- no --> D[preflightFileWrites<br/>every FileWriteOp blob must exist]
    D -- missing --> D1[ResultSchemaError]
    D -- ok --> E[runSchemaPhase]
    E --> E1[per WriteOp: base.Merge patch or FromJSONWithID]
    E1 --> E2[schema.Validate against the rolling schema]
    E2 --> E3[SchemaMigrationOp advances the rolling schema]
    E3 --> F{violations?}
    F -- yes --> F1[ResultSchemaError with FieldViolations]
    F -- no --> G{conflict policy}
    G -- AppendOnly / LastWrite --> J[write phase]
    G -- Strict / Custom --> H[detectConflicts:<br/>content hash at base tree vs target tree]
    H --> I{conflicts?}
    I -- no --> J
    I -- yes, Strict --> I1[ResultConflict + ConflictReport]
    I -- yes, Custom --> I2[resolver per WriteOp, re-validate]
    I2 -- unresolved --> I1
    I2 -- resolved --> J
    J[PutDocument / DeleteDocument per op] --> K{write error?}
    K -- yes --> K1[DiscardPending → ResultAborted]
    K -- no --> L[CommitTree anchor.DocumentTreeHash]
    L --> M[AppendCommit tx, anchor, tree, schemaHash]
    M --> N[ResultSuccess commit, newTreeHash]
```

**Conflict classification** (`detectConflicts`) compares, per touched document, the content hash
at the transaction's *base* tree against the hash at the *target* (current head) tree:

| base | target | classification |
|------|--------|----------------|
| equal hashes | — | not a conflict (someone wrote the same content) |
| both absent | — | not a conflict |
| present, changed | present | `ConcurrentWrite` |
| absent | present | `DeleteWrite` |
| present | absent | `WriteDelete` |

**Server wrapper** (`KdbServerRuntime.runTransaction`), in cheapest-first order:

```mermaid
sequenceDiagram
    autonumber
    participant H as wire handler
    participant RT as KdbServerRuntime
    participant AD as Admission
    participant WG as writeGate
    participant TE as transaction.Engine
    participant PD as PersistingCommitDAG
    participant CL as CommitListener (stream hub)

    H->>RT: Commit/Upsert/Replay
    RT->>RT: draining? → UnavailableError
    RT->>RT: authorizeOperations (per-op RBAC)
    RT->>AD: Acquire(ctx, ClassWrite, payloadBytes)
    AD-->>RT: Grant  (or MemoryPressureError / BusyError / ResourceExhaustedError)
    RT->>WG: acquire(ctx)  (queue full → BusyError; deadline → DeadlineExceededError)
    RT->>TE: Commit(...)   ← the flow above
    TE-->>RT: ResultSuccess
    RT->>PD: PersistAsync(commit)   [queued under the gate: queue order = commit order]
    RT->>WG: release  ← as soon as log position is fixed
    RT->>PD: wait()   [fsync happens off the gate; concurrent commits share it]
    RT->>CL: CommitListener(namespace, commit)
    RT->>AD: Grant.Release (deferred — held across the durability wait)
    RT-->>H: Commit
```

The two release points are the performance-critical detail: the **gate** is released once the
commit's position in the delta log is fixed, so the next writer's validate/apply overlaps this
one's disk write; the **grant** is held until the operation genuinely stops occupying memory.

-----

## 4. Blob write and durability

```mermaid
sequenceDiagram
    autonumber
    participant C as caller
    participant SE as ServerEngine.WriteBlob
    participant W as DefaultWriteAheadLog
    participant G as GroupCommitter
    participant IO as PlatformIOShim
    participant MT as memtable.Manager
    participant SST as LsmBlobStore

    C->>SE: bytes
    SE->>SE: SHA-256 → content hash
    SE->>W: Append(Record{PutBlob, hash‖bytes})
    W->>IO: AppendToSegment(active WAL segment)  [rotates at 64 MiB]
    W-->>SE: AppendResult{sequence}
    alt Durability = Sync
        SE->>G: SyncTo(sequence, wal.Sync)
        G->>IO: FlushSegment  (one fsync per round of waiters)
        G-->>SE: durable
    else Durability = Async
        Note over SE: acked now; a background ticker syncs every N ms
    else Durability = MemoryOnly
        Note over SE: never synced by the engine
    end
    SE->>MT: Put(hash, bytes)
    Note over MT,SST: Flush(level) later writes an SSTable and registers it with LsmBlobStore
    SE-->>C: hash
```

`WriteBlob` takes **no engine-wide lock**: `WAL.Append` and `memTable.Put` are each internally
thread-safe, and `FileBackedPlatformIO.FlushSegment` deliberately does not hold the per-segment
append mutex, so new appends can register with the `GroupCommitter` while an fsync is in flight.

-----

## 5. Commit durability — the delta commit log

```mermaid
sequenceDiagram
    autonumber
    participant A as caller (holding write gate)
    participant P as PersistingCommitDAG
    participant Q as commitLogWriter.reqs (buffered 256)
    participant DR as drain goroutine
    participant DW as delta.DefaultWriter
    participant IO as segment file

    A->>P: PersistAsync(commit)
    P->>P: commit.ToPayloadBytes()
    P->>Q: enqueue DeltaRecord (+ ack chan under Sync)
    P-->>A: wait func
    A->>A: release write gate
    DR->>Q: drain up to 256 records
    loop each record
        DR->>DW: Append → PageCodec.Frame(payload, codec)
        DW->>IO: AppendToSegment
    end
    DR->>DW: Flush → fsync (one per batch)
    DR-->>A: ack (wait returns)
    Note over DR: any append/flush error latches permanently —<br/>a log with a hole must not be appended to
```

Ordering guarantee: callers enqueue **while holding the write gate**, so enqueue order is commit
order; a single drain goroutine appends in that order; delta replay depends on it.

-----

## 6. Connect, handshake, session begin

```mermaid
sequenceDiagram
    autonumber
    participant CL as client
    participant T as tcp.Transport
    participant H as sqlWireConnHandler
    participant AU as auth.Engine
    participant SM as SessionManager
    participant D as DAG

    CL->>T: dial tcp:// | tcps:// | ws:// | wss://
    T->>T: MaxConnections check (close at accept if over cap)
    CL->>H: HANDSHAKE {clientMode=SQL_CLIENT, namespaces, user/password/token}
    H->>H: reject unless clientMode == SQL_CLIENT
    H->>AU: Authenticate(credentials)
    AU-->>H: Principal (or error → rejected ack with reason)
    H->>AU: Authorize(SessionBeginAction{namespace or server default})
    H->>D: Head()
    H-->>CL: HANDSHAKE ack {accepted, negotiatedEncoding, protocolVersion, remoteHeads}
    Note over H: principal is bound to the connection for its lifetime
    CL->>H: SESSION_BEGIN {namespace, readConsistency, baseVersionHex?, sessionId?}
    H->>AU: Authorize(SessionBeginAction{namespace})
    H->>SM: Begin(...)
    SM->>D: Head() (or the supplied base version, which must exist)
    SM->>SM: ReadPin = head when readConsistency = SNAPSHOT
    SM-->>H: KdbSession
    H-->>CL: SESSION_BEGIN_ACK {sessionId, headHex, readConsistency}
```

Read consistency: `SNAPSHOT` pins the session's reads to the head at session start;
`READ_COMMITTED` and `READ_YOUR_WRITES` read the live head on every statement.

-----

## 7. SQL `SELECT` over the wire

```mermaid
sequenceDiagram
    autonumber
    participant CL as client
    participant FA as FrameAdmitter
    participant H as execRead
    participant AD as Admission + CostModel
    participant SQ as sql.Engine
    participant ST as storage.Adapter
    participant D as DAG

    CL->>FA: SQL_EXEC frame (header only so far)
    FA->>FA: opClassForMessage → ClassScan; admitInZone?
    alt shed by zone
        FA-->>CL: SQL_RESULT {error, code=BUSY, retryAfterMs}  (body never read)
    end
    FA->>H: full frame
    H->>H: parse; authorize SqlExecAction{readOnly: isSelect}
    H->>H: head = session.ReadPin ?? DAG.Head()
    H->>AD: EstimateScan{shape, treeSize, maxRows, rowBudget}
    H->>AD: AcquireBytes(ClassScan, estimate)  [2s timeout]
    AD-->>H: Grant (or typed error → classified SQL_RESULT)
    H->>SQ: Execute(sql, QueryContext{ns, schema, atCommit, params, maxRows, rowBudget, stats})
    SQ->>SQ: parse → plan (full scan + limit) → execute
    SQ->>D: GetCommitOrThrow(atCommit) → DocumentTreeHash
    SQ->>ST: ScanDocuments(batch 256) / GetDocument per id
    SQ-->>H: QueryResult{columns, rows}
    H->>AD: ObserveScanActual(shape, estimate, stats.RetainedBytes + wire bytes)
    H->>AD: ObserveDocSize(namespace, mean doc bytes)
    H-->>CL: SQL_RESULT {columns, rows, resolvedCommitHex, readOnly=true}
    H->>AD: Grant.Release
```

Execution details (ordering vs. limit, aggregates, `_doc`, row budget) are in
[Part 5](kdb-lld-query.md).

-----

## 8. `INSERT` then `TX_COMMIT`

SQL writes are **buffered**, not auto-committed, on the wire path: `INSERT` accumulates document
operations on the session's pending `transaction.Builder`; `TX_COMMIT` flushes them.

```mermaid
sequenceDiagram
    autonumber
    participant CL as client
    participant H as handler
    participant SQ as sql.DMLExecutor
    participant B as session.Pending (transaction.Builder)
    participant LM as LockManager
    participant RT as KdbServerRuntime

    CL->>H: SQL_EXEC "INSERT INTO t (a,b) VALUES (?,?)"
    H->>SQ: ExecuteDML → []document.Op (fresh UUID per row, schema-validated)
    SQ-->>H: ops
    H->>B: Write(docID, patch) per op
    H-->>CL: SQL_RESULT {rowsAffected, generatedIDs, readOnly=false}

    CL->>H: TX_COMMIT {sessionId} (or transactionBytes from the SDK)
    alt transactionBytes present
        H->>H: wire.DecodeTransaction (client-built, caller-chosen doc ids)
    else
        H->>B: Build(timestamp) → document.Transaction
    end
    H->>LM: AcquireAllForTransaction(ns, sessionId, tx)   [sorted order; all-or-nothing]
    alt a document is locked by another session
        LM-->>CL: SQL_RESULT {error: document locked}
    end
    H->>RT: Commit(ns, tx, sessionId, principal)   ← flow 3
    H->>LM: ReleaseAll(sessionId)
    H->>H: ClearPending
    alt conflict
        H-->>CL: CONFLICT_REPORT {reportBytes}
    else success
        H-->>CL: SQL_RESULT {rowsAffected, resolvedCommitHex}
        Note over H: session.BaseVersion advances to the new commit
    end
```

`TX_ROLLBACK` releases the session's locks and clears the pending builder; it never touches the
DAG.

-----

## 9. Point read and upsert

```mermaid
sequenceDiagram
    autonumber
    participant CL as client
    participant H as handler
    participant AD as Admission
    participant RT as KdbServerRuntime
    participant ST as storage

    CL->>H: DOCUMENT_GET {namespace, docId}
    H->>AD: AcquireBytes(ClassPointRead, EstimatePointRead(namespace))
    Note over AD: point reads are never shed by zone policy;<br/>a cost above capacity is charged the whole capacity instead of refused
    H->>RT: GetDocument(ns, docId)
    RT->>ST: GetDocument at head tree
    H-->>CL: DOCUMENT_GET_RESULT {json, commitHex, found}

    CL->>H: UPSERT {namespace, docId, json}
    H->>RT: Upsert → UpsertEngine (ConflictPolicyLastWrite), anchored at current head
    RT-->>H: commit  (never conflicts)
    H-->>CL: UPSERT_RESULT {commitHex}
```

-----

## 10. Conflict and client retry

```mermaid
sequenceDiagram
    autonumber
    participant A as client A
    participant B as client B
    participant S as server
    participant D as DAG

    A->>S: read doc X at commit C0 (BaseVersion = C0)
    B->>S: read doc X at commit C0
    B->>S: Commit {base C0, write X=v2}
    S->>D: head C0 → C1
    S-->>B: C1
    A->>S: Commit {base C0, write X=v3}
    S->>S: detectConflicts: hash(X@C0) != hash(X@C1) → ConcurrentWrite
    S-->>A: CONFLICT_REPORT {txId, baseHash C0, targetHash C1, local v2, incoming v3}
    Note over A: client SDK surfaces *ConflictError satisfying errors.Is(err, ErrConflict)
    A->>S: re-read X at C1, rebase, Commit {base C1, write X=v4}
    S-->>A: C2
```

Three ways to avoid the conflict entirely, all supported: `Upsert` (last-write-wins, no base
version), `AppendEvent` (append-only namespaces, every write independent), or a `Custom`
conflict resolver configured on the engine.

-----

## 11. Crash recovery

```mermaid
flowchart TD
    A[process dies: kill -9, OOM kill, power loss] --> B[restart, OpenFileRuntime]
    B --> C[acquire .kdb.lock]
    C --> D[ListSegments in sequence order]
    D --> E{scan each segment}
    E -- clean --> F[collect commits]
    E -- short trailing frame on the NEWEST segment --> G[torn tail: log it, keep the prefix]
    E -- CRC mismatch / corruption elsewhere --> H["refuse to open — run kdb-inspect repair-segments"]
    F --> I[applyCommitsTopologically]
    G --> I
    I --> J{every parent resolvable?}
    J -- yes --> K[storage rebuilt, DAG rebuilt, main head set]
    J -- no --> L[error naming the first unresolved commit:<br/>the log is missing data → restore]
    K --> M[OpenWriter starts a NEW segment at maxSeq+1]
```

Two properties make this safe on every start: replay never trusts file order for correctness
(only for speed), and corruption is tolerated **only** where an unclean shutdown can legitimately
produce it — the tail of the most recently written segment. Anything else is reported rather than
silently truncated.

-----

## 12. Peer sync (Mode 3)

Peers are equal; either side may initiate. The decision function is shared by both directions so
the two can never disagree.

```mermaid
sequenceDiagram
    autonumber
    participant A as peer A (client)
    participant B as peer B (host)
    participant RD as ResolveDivergence
    participant D as DAG (A)

    A->>B: HANDSHAKE {clientMode=FULL_PEER, localHeads}
    B->>B: authorize PeerSyncAction
    B-->>A: ack {remoteHeads}
    A->>B: COMMIT_FETCH {namespace, sinceHash, maxCommits}
    B-->>A: COMMIT_PUSH {commits, parent-before-child}
    loop each commit
        A->>D: PutCommit(requireParents=true, hash verified)
        A->>A: MaterializeCommit → storage + tree-hash check
        A->>A: Persist → local delta log (file-backed runtimes)
    end
    A->>RD: ResolveHeadUpdate(local, incoming)
    alt fast-forward
        RD->>D: SetHead(main, incoming)
    else already ancestor
        RD-->>A: no-op
    else diverged
        RD->>RD: CommonAncestor, per-document change sets
        alt disjoint documents
            RD->>D: AppendMergeCommit(primary, merged) → OutcomeMerged
        else same document changed on both sides
            alt policy = LastWrite / Custom resolves
                RD->>D: apply the winning side
            else
                RD-->>A: OutcomeConflict + ConflictReport, head left untouched
            end
        end
    end
    A->>B: COMMIT_PUSH (CommitsToPush: the true set difference)
    B-->>A: COMMIT_PUSH_ACK {appliedCommits, headHex}
```

Two subtleties worth knowing:

- `CommitsToPush` computes a genuine set difference (everything reachable from the remote head is
  excluded), never a `Walk` pruned at the remote head — otherwise a merge commit's other parent
  is missed and the peer rejects the push with "missing parent".
- `ResolveDivergence` is serialized **per namespace** by a package-level lock map, because it
  reads the head, decides, and mutates across several non-atomic calls.

-----

## 13. Stream modes

```mermaid
sequenceDiagram
    autonumber
    participant W as any writer
    participant RT as KdbServerRuntime
    participant HUB as StreamHub
    participant S1 as Mode 1 subscriber (read-only)
    participant S2 as Mode 2 subscriber (write-back)

    S1->>HUB: HANDSHAKE {clientMode=STREAM_READ_ONLY, nodeId}
    HUB-->>S1: ack
    S2->>HUB: HANDSHAKE {clientMode=STREAM_WRITE_BACK, nodeId}
    HUB-->>S2: ack

    W->>RT: commit (any path: SQL, upsert, peer sync)
    RT->>HUB: CommitListener(namespace, commit)
    HUB->>S1: DELTA_COMMIT {commitHash, parentHash, operations, indexHints}
    HUB->>S2: DELTA_COMMIT
    S1->>HUB: POSITION_ACK {commitHash}

    S2->>HUB: TRANSACTION_REPLAY {namespace, baseVersion, transactionBytes}
    HUB->>RT: Replay(tx, replayTarget = current head)
    alt applied
        HUB-->>S2: SQL_RESULT {resolvedCommitHex}
    else conflict
        HUB-->>S2: CONFLICT_REPORT
    end
```

Mode 1 is a pure fan-out subscriber: it receives deltas and acknowledges positions but cannot
write. Mode 2 adds `TRANSACTION_REPLAY`, which the server applies **onto the current head** (not
onto a client-supplied target) and awaits, so the client learns the real outcome.

-----

## 14. Memory pressure and shedding

```mermaid
sequenceDiagram
    autonumber
    participant T as sampler (200 ms)
    participant G as MemoryGuard
    participant A as Admission
    participant FA as FrameAdmitter
    participant OP as an operation

    T->>G: currentMemoryUsageBytes (cgroup memory.current, else runtime/metrics)
    G->>G: push into 5-sample ring → smoothed average
    G->>G: zoneFor(smoothed, current)  [up immediately; down only after 600 ms dwell]
    G->>A: observer(smoothed, zone)
    A->>A: applyFloor: hold (smoothed − granted) bytes of the semaphore
    Note over A: this is how monotonic DAG growth becomes a throttle instead of an OOM
    alt entering Critical
        A->>A: release rescue reserve (48 MiB default, clamped to ¼ of budget)
        A->>A: debug.FreeOSMemory()
    else returning to Normal
        A->>A: re-allocate the reserve (failure is itself latched as a signal)
    end
    A->>A: scanRowBudget = base / {1,2,4,8} by zone

    OP->>FA: frame header arrives
    FA->>G: CurrentZone
    alt class not admitted in this zone
        FA-->>OP: typed rejection frame, body never read
    else
        FA->>A: Acquire(class, estimate)
        A-->>OP: Grant or BusyError / ResourceExhaustedError
    end
```

Per-zone class policy:

| Zone | entry (default) | point read | scan | write | replication |
|------|------------------|-----------|------|-------|-------------|
| Normal | < 70 % | admit | admit | admit | admit |
| Elevated | ≥ 70 % | admit | admit (½ row budget) | admit | admit |
| High | ≥ 85 % | admit | **shed** | **shed** | admit |
| Critical | ≥ 93 % | admit | shed | shed | **shed** |

-----

## 15. Orderly shutdown and abort

```mermaid
flowchart TD
    subgraph SIGTERM["SIGTERM / SIGINT"]
        A1[signal] --> A2[AdminServer.SetReady false, 'draining']
        A2 --> A3[runtime.BeginDraining → new writes get UnavailableError]
        A3 --> A4[WaitForWritesToDrain drain-timeout]
        A4 --> A5[close listeners]
        A5 --> A6[runtime.Release → EmbeddedKdbRuntime.Close]
        A6 --> A7[drain commit log → flush+seal delta segment → WAL final sync → release .kdb.lock]
        A7 --> A8[exit 0]
    end
    subgraph WATCHDOG["AbortWatchdog · sustained pressure"]
        B1[ShouldReject true for abort-after] --> B2[BeginDraining]
        B2 --> B3[close listeners]
        B3 --> B4[grace period for in-flight work]
        B4 --> B5[flush and seal storage]
        B5 --> B6[exit 75 EX_TEMPFAIL → supervisor restarts]
    end
```

Both paths converge on the same flush/seal sequence, and both are optional for correctness: the
replay path that handles `kill -9` also handles a clean exit.

-----

## 16. Verify and repair

```mermaid
sequenceDiagram
    autonumber
    participant OP as operator
    participant I as kdb-inspect
    participant L as .kdb.lock
    participant V as integrity.Verify
    participant R as integrity.Repair

    OP->>I: kdb-inspect verify --data-dir D --namespace NS --level L2
    I->>L: LockDataDir (refuses while a service holds it)
    I->>V: walk segments
    V->>V: L1 — per-frame magic, length, CRC, decode
    V->>V: L2 — parent closure across segments (genesis exempt)
    V-->>OP: Report{findings, segment summaries}  (exit non-zero if not clean)

    OP->>I: kdb-inspect repair-segments --data-dir D --namespace NS [--dry-run]
    I->>R: act only on findings Verify produced
    alt torn tail
        R->>R: truncate to the last clean frame
    else mid-log corruption
        R->>R: quarantine the segment into ns/[namespace]/quarantine/
        alt parent closure holds with the good prefix
            R->>R: rewrite the segment as that prefix
        else
            R-->>OP: REFUSED, naming the commits that would be lost → run restore
        end
    end
```

-----

## 17. Backup and restore

```mermaid
sequenceDiagram
    autonumber
    participant OP as operator
    participant B as backup.Create
    participant S as ObjectStore (DirStore | S3)
    participant RS as recovery.HybridRestore

    OP->>B: kdb-inspect backup --data-dir D --namespace NS --to DIR|s3 [--base-backup-id ID]
    B->>B: sealed segments in full (SHA-256 each)
    B->>B: active segment as its CRC-verified prefix
    B->>S: upload objects (skipping segments the base manifest already names)
    B->>S: upload manifest LAST — its presence defines the backup
    B-->>OP: Manifest{segments, tips}

    OP->>S: kdb-inspect backup-verify --backup-id ID
    S-->>OP: re-download, re-hash, report problems

    OP->>RS: kdb-inspect restore --namespace NS --out DIR --source live=/damaged --from-backup DIR --backup-id ID
    RS->>RS: union of CRC-verified commits from every source (hash-keyed)
    RS->>RS: topological order (parents first; genesis exempt)
    RS->>RS: write a fresh sequenced delta log into --out
    RS-->>OP: Result{applied, sourcesUsed, missingHashes}
```

An unverified byte is never trusted just because it is the only copy available — a commit whose
parent exists in no source is reported as missing rather than applied out of order.

-----

## 18. Schema DDL and migration

```mermaid
flowchart TD
    A["CREATE TABLE t (name VARCHAR NOT NULL, age INT)"] --> B[DefaultParser → StmtCreateTable]
    B --> C{namespace already has a schema?}
    C -- yes --> C1[PlanningError: schema already exists]
    C -- no --> D[DDLExecutor: columns → schema.Field, Indexed=true]
    D --> E[schema.Build → SchemaHash, Version 1]
    E --> F[QueryResult.AppliedSchema]
    F --> G[server: SetSchema under schemaMu]
    subgraph MIG["Later evolution"]
        H[MigrationBuilder: AddField / DropField / WidenEnum / …] --> I[SchemaMigration]
        I --> J[SchemaMigrationOp inside a transaction]
        J --> K[runSchemaPhase: ApplyMigration advances the rolling schema]
        K --> L[commit carries the new SchemaHash]
    end
```

`IsBreaking(step)` classifies each step, and `DiffSchemas` reports added/removed/modified fields
with typed changes, so a caller can refuse a breaking migration before submitting it.

-----

## 19. RBAC enforcement points

Authorization is checked at **four** independent places, deliberately overlapping so no caller
path can bypass it:

```mermaid
flowchart LR
    A[HANDSHAKE] -->|Authenticate + SessionBeginAction| B[SESSION_BEGIN]
    B -->|SessionBeginAction per namespace| C[SQL_EXEC]
    C -->|SqlExecAction ReadOnly=isSelect| D[Commit]
    D -->|DocumentWrite/DeleteAction per operation| E[(storage)]
    F[peer sync listener] -->|PeerSyncAction| E
```

- `SqlExecAction.ReadOnly` is true **only for `SELECT`** — `CREATE TABLE` counts as a write, so a
  read-only principal cannot rewrite the namespace schema.
- `KdbServerRuntime.Commit` re-checks per operation even though the wire layer already checked,
  so a Go caller reaching the runtime directly is still enforced.
- Grants are wildcard-aware over `namespace/collection/document`; a database-level grant covers
  every collection beneath it, and a collection grant never leaks to a sibling.

-----

## Cross-references

- Types named in these flows: [Part 1 — Components](kdb-lld-components.md)
- What is safe to run concurrently: [Part 3 — Concurrency](kdb-lld-concurrency.md)
- Byte formats the flows write: [Part 4 — Storage](kdb-lld-storage.md)
- Query semantics: [Part 5 — Query](kdb-lld-query.md)
- Frames and error codes: [Part 6 — Protocol](kdb-lld-protocol.md)
