# KDB — Low-Level Design

## Part 1 · Component and Class Reference

**Parent:** [Part 0 — Index & architecture](kdb-lld.md) · **See also:**
[High-level architecture](kdb-architecture.md) · [Flows](kdb-lld-flows.md) ·
[Concurrency](kdb-lld-concurrency.md) · [Storage](kdb-lld-storage.md) ·
[Query](kdb-lld-query.md) · [Protocol](kdb-lld-protocol.md) ·
[User guide](kdb-user-guide.md)

This part documents **every package and every significant type**: what it is, what state it owns,
what its key methods do, and what invariants it maintains. Types are grouped by layer, bottom-up.
Kotlin counterparts are named where they exist.

-----

## Layer 0 — Foundation

### 1.1 `codec` — the KDB binary codec

Schema-driven typed binary encoding. Used for every hashed payload (documents, commits, schemas)
and as an optional wire encoding.

| Type | What it is |
|------|-----------|
| `Value` (interface) | Sum type over all encodable values: `NullValue`, `BoolValue`, `Int32Value`, `Int64Value`, `Float64Value`, `StringValue`, `BytesValue`, `ArrayValue`, `MapValue`, `RecordValue`, `EnumValue`, `UnionValue`, `FixedValue`, `DateValue`, `TimestampValue`, `UUIDValue` |
| `RecordValue` | `Fields map[int]Value` — keyed by **field id**, not name, so field renames do not change bytes |
| `UUID` | `{MSB, LSB int64}`; `RandomUUID`, `UUIDFromBytes/String`, `String()`, `Bytes()` |
| `Hash` | `[32]byte` SHA-256; `HashFromBytes/Hex`, `Hex()` |
| `Timestamp` | `{EpochMillis, MicroRemainder}`; `TimestampNow`, `TimestampFromEpochMicros`, `TimestampFromISO8601`, `EpochMicros()` |
| `cursor` (internal) | Bounds-checked decode cursor: `u8`, `rawN`, `leb`, `checkElementCount` |

**Key functions.** `wireEncode(v, type, registry)` / `wireDecode(bytes, type, registry)` walk the
type tree and emit/consume LEB128-prefixed values. `wireDecodeFirst` decodes one value and
reports bytes consumed (used to read a value out of a larger buffer).

**Hardening invariants** (each has a regression test): array/map element counts are validated
against *remaining input* before allocation, LEB128 varints are rejected on truncation and on
overflow, empty input is rejected, and trailing bytes after a complete value are an error.

### 1.2 `codec/schema` — the codec type system

| Type | Role |
|------|------|
| `PhysicalKind` | wire-level kind byte (`Int32`, `Int64`, `Float64`, `String`, `Bytes`, `Fixed`, …); `PhysicalFromTag`/`Tag()` |
| `LogicalAnnotation` | semantic overlay on a physical kind: `LogicalDate`, `LogicalTimeMicros`, `LogicalTimestampMicros/Millis`, `LogicalUUID`, `LogicalDuration`, `LogicalCustom{ID}` |
| `Type` (interface) | `Primitive{Physical, Logical}`, `Ref{FullyQualifiedName}`, `Nullable{Inner}`, `Array{Element}`, `Map{Key,Value}`, `Union{Branches}` |
| `FieldSchema` | `{ID, Name, Type, Default…}` — the id is what the encoding uses |
| `RecordSchema` / `EnumSchema` / `FixedSchema` | named schemas, `FQName() = Namespace + "." + Name` |
| `Registry` | name → named schema; `RegisterRecord/Enum/Fixed`, `Resolve`, `Freeze()` (a frozen registry panics on further registration — the builtin registries are frozen at init) |

### 1.3 `error` — the error model

| Type | Role |
|------|------|
| `Code` | stable numeric codes; **values never change once published** (1001 decode, 3001 schema violation, 4001 conflict, 4002 document locked, 4102 data directory locked, 6001 unsupported protocol, 6301/6302 auth …) |
| `Result[T]` | success/failure carrier used by schema validation and migration |
| `FieldViolation` | `{FieldName, ViolationType, Detail}` |
| `ConflictReport` / `ConflictItem` | structured conflict payload sent over the wire: transaction id, base hash, target hash, per-document local/incoming JSON, `ConflictOperationType` (`ConcurrentWrite`, `DeleteWrite`, `WriteDelete`) |
| typed errors | `VersionNotFoundError`, `DocumentLockedError`, `IceStorageError`, `SchemaViolationError`, `UnsupportedProtocolVersionError`, `EncodingNegotiationFailureError`, … each exposing `Code()` |

The full code table and the wire-level `ErrorCode` mapping are in
[Part 6 §6](kdb-lld-protocol.md).

-----

## Layer 1 — Core types

### 1.4 `json` — JSON functions engine

A self-contained JSON implementation (no `encoding/json` for document bodies) so behaviour is
identical across Go, JVM, and JS.

| Type / function | Role |
|-----------------|------|
| `Value` | `StringValue`, `NumberValue`, `IntValue`, `BoolValue`, `NullValue`, `ObjectValue` (field map **plus key order**), `ArrayValue` |
| `ParseValue` / `ToJSONString` | parse and compact-serialise; the parser handles `\uXXXX` including surrogate pairs |
| `Path` / `CompilePath` | compiled JSONPath (`$.field`, `$.a[0]`, `$.a[*]`, `$.*`); compiled paths are cached |
| `Get`, `GetString`, `GetAll` | read one value / read via a string path / collect wildcard matches |
| `Set`, `Delete`, `Merge` | immutable updates returning new JSON text; `Merge` is a shallow root merge |
| `Contains`, `Keys`, `TypeName`, `ArrayLength` | the JSON function surface exposed to SQL |
| `FromKdbValue` / `ToKdbValue` | bridge to the binary codec |

Negative array indices normalise against length; out-of-range reads return nil rather than
erroring (get semantics) while writes error (set semantics).

### 1.5 `document` — documents, commits, trees

| Type | Role and key methods |
|------|----------------------|
| `Document{ID, JSON}` | `ContentHash()`, `Merge(patch)`, `WithJSON`, `FromJSON`, `FromJSONWithID`, `ToDocumentBodyValue`, `FromDocumentBodyValue`. Root must be a JSON object |
| `DocumentTree{TreeHash, …}` | persistent Merkle trie: `With`, `Without`, `HashFor`, `Contains`, `Size`, `Walk`, `MaterializedEntries`, `BuildDocumentTree`, `EmptyDocumentTree` |
| `trieNode` / `trieLeaf` (internal) | 16-way radix nodes over UUID nibbles; `leafHash`, `internalHash`, `trieInsert/Delete/Build/Get/Walk/TreeHash` |
| `Commit` | see [Part 0 §5.5]; `BuildCommit`, `ComputeCommitHash`, `ToPayloadBytes`, `FromPayloadBytes` |
| `Op` union | `WriteOp`, `DeleteOp`, `FileWriteOp`, `SchemaMigrationOp`, plus `OpFromValue` |
| `Transaction{ID, BaseVersion, Operations, Timestamp, AuthorNodeID}` | the unit submitted to the transaction engine |
| `Branch`, `Tag`, `CommitStub` | version pointers and archive stubs |
| `EnsureIDInJSON` | injects/validates the `id` field |
| `SHA256Digest`, `WireRegistry()` | hashing and the `dev.kdb.document` codec schemas |

### 1.6 `file` — attachments

Blob-backed file records: a `FileWriteOp` references a blob already written via
`Adapter.WriteBlob`, and the transaction engine preflights its existence
(`preflightFileWrites`) so a commit can never reference a missing blob.

-----

## Layer 2 — Schema and DAG

### 1.7 `schema`

| Type / function | Role |
|-----------------|------|
| `KdbSchema` | `{SchemaHash, Fields, Version, CreatedAt, Description}`; `None()`, `IsNone()`, `HasField`, `Field`, `Build`, `ToBytes`, `FromBytes` |
| `Field` | `{Name, Type, Required, Indexed, Unique}`; `NewField` validates the name against `^[a-zA-Z_][a-zA-Z0-9_]*$` |
| `FieldType` | `StringType`, `Int32Type`, `Int64Type`, `Float64Type`, `BoolType`, `TimestampType`, `UUIDType`, `ObjectType`, `ArrayType`, `EnumType{values}`; each exposes `SQLTypeName()` and `CodecTypeLabel()` |
| `Validate(doc, schema)` | per-field checking against the document JSON; returns `Result[Document]` with `FieldViolation`s. Integral float check guards the `MaxInt64` rounding trap (`twoToThe63`) |
| `SchemaMigration` + `MigrationStep` | ordered evolution steps; `IsBreaking(step)`, `ApplyMigration`, `IsBackwardCompatible` |
| `MigrationBuilder` | fluent DSL: `AddField`, `DropField`, `WidenEnum`, `NarrowEnum`, `Description`, `Build(migrationID)` |
| `Diff` / `FieldDiff` / `FieldChange` | `DiffSchemas(from,to)` → added/removed/modified with typed changes (`TypeChanged`, `RequiredChanged`, `IndexedChanged`, `UniqueChanged`, `EnumValuesChanged`); `IsBreaking()` |
| `ComputeSchemaHash` | canonical hash used on commits |

### 1.8 `dag` — the commit DAG

`InMemoryCommitDag` is the single most central mutable structure in the engine.

**State** (all guarded by one `sync.RWMutex`):

| Field | Contents |
|-------|----------|
| `commits` | `map[Hash]Commit` |
| `stubs` | `map[Hash]CommitStub` — archived commits |
| `trees` | `map[Hash]DocumentTree` |
| `branches` | `map[string]Branch` (always contains `main`) |
| `tags` | `map[string]Tag` |
| `hexSorted` | sorted hex strings, for prefix lookup (`kdb get <8-hex-prefix>`) |
| `txIndex` | `map[UUID]Hash` — transaction id → commit, for O(1) idempotent-retry detection |
| `ancestryVersion` | monotonic counter bumped whenever the graph *shape* changes (insert, squash, stub) |

**Method groups:**

| Group | Methods | Notes |
|-------|---------|-------|
| Read | `GetCommit`, `GetCommitOrThrow`, `HasCommit`, `GetStub`, `HasStub`, `GetDocumentTree(OrThrow)`, `LookupHashPrefix`, `GetCommitByTransactionID` | `GetCommitOrThrow` raises `IceStorageError` for a stubbed commit, `VersionNotFoundError` for an unknown one |
| Write | `PutCommit(commit, requireParents)`, `AppendCommit(tx, parent, tree, schemaHash, msg)`, `AppendMergeCommit(tx, primary, merged, tree, schemaHash, msg)`, `PutDocumentTree`, `StubCommit`, `Squash` | `PutCommit` verifies the hash (untrusted source); `AppendCommit` does not (it just built it) |
| Branch | `Head`, `SetHead`, `GetBranch(OrThrow)`, `ListBranches`, `CreateBranch`, `DeleteBranch` | `main` cannot be deleted; `SetHead` requires the target commit to be present |
| Ancestry | `CommitsSince(from, exclude)`, `CommonAncestor(a,b)`, `IsAncestor`, `AncestorSet`, `AncestryVersion` | `AncestorSet` is the batched form — use it instead of N× `IsAncestor` |
| Traversal | `Walk(from, until, limit)` → `[]TraversalEntry` (`FullEntry` \| `StubbedEntry`) | newest-first by timestamp; `until` **prunes a branch, never aborts the walk** (a merge commit's other parent must still be visited) |
| Diff | `Diff(from,to)` → `CommitDiff{Entries}` with `DiffAdded/Removed/Modified`, `IsEmpty()` | compares document trees |
| Refs | `CommitRef` union: `RefByHash`, `RefByBranch`, `RefByTag`, `RefByTime` | resolved by `query/hybrid` |

`CommitDAG` is the interface subset used by callers that do not need the concrete type;
`embed.PersistingCommitDAG` implements it (see §1.22).

-----

## Layer 3 — Write path

### 1.9 `transaction`

| Type | Role |
|------|------|
| `Engine` (interface) | `Commit`, `Replay`, `Merge`, `Validate`, `ConflictPolicy()`, `CustomResolver()` |
| `defaultEngine` | the only implementation; stateless apart from its policy and resolver |
| `ConflictPolicy` | `AppendOnly`, `LastWrite`, `Strict`, `Custom` |
| `ConflictResolver` | `Resolve(DocumentConflict) (*Document, error)` — consulted under `Custom` |
| `TransactionResult` union | `ResultSuccess{Commit, NewTreeHash}`, `ResultConflict{Report, ConflictingOps}`, `ResultSchemaError{Violations}`, `ResultAborted{Cause}` |
| `OperationConflict` / `OperationViolation` | per-op diagnostics carrying base/existing/incoming documents |
| `Builder` | accumulates `Write`/`Delete` ops against a base version, `Build(timestamp)` → `Transaction`; `NewBuilder` anchors at the DAG head |
| `LockManager` | per-`(namespace, docID)` exclusive locks owned by a session id |
| `DocumentIDsIn(ops)` | distinct document ids referenced by a transaction |
| `DecodeMigration` | decodes a `SchemaMigrationOp` payload |

**`defaultEngine.Commit` in phases** (full sequence in [Part 2 §3](kdb-lld-flows.md)):

1. **Idempotency** — `findExistingCommit` looks the transaction id up in `dag.txIndex`; a match
   with identical parents returns the original commit.
2. **Preflight** — `preflightFileWrites` verifies every `FileWriteOp` blob exists.
3. **Schema phase** — `runSchemaPhase` merges each `WriteOp` patch onto its base document,
   validates against the rolling schema, applies `SchemaMigrationOp`s, and collects
   `writesByOpIndex`.
4. **Conflict detection** — under `Strict`/`Custom`, `detectConflicts` compares each touched
   document's content hash at *base tree* vs *target tree*; equal hashes are not conflicts.
   `AppendOnly` and `LastWrite` skip detection entirely.
5. **Resolution** — `Custom` calls the resolver per conflicting `WriteOp` and re-validates the
   resolved document; anything else, or a nil result, degrades to reporting the conflict.
6. **Write phase** — staged `PutDocument`/`DeleteDocument` calls; **any error triggers
   `DiscardPending` and returns `ResultAborted`**.
7. **Publish** — `CommitTree(parentTreeHash)` then `AppendCommit`, returning `ResultSuccess`.

`Replay` is `Commit` with base = baseline = target = the replay target (no optimistic check):
the Mode 2 write-back and merge-step path. `Merge` finds the common ancestor, topologically
sorts the merged branch's commits (`topoSort`, Kahn's algorithm with deterministic hex-ordered
tie-breaks), replays each onto the primary head, then appends a real two-parent merge commit.

### 1.10 `index`

| Type | Role |
|------|------|
| `Store` (interface) | `Put`, `Delete`, `BulkLoad`, `Lookup`, `Range`, `Search`, `NearestNeighbours`, `Rebuild`, `Clear`, `IsValid`, `Snapshot`, `RestoreSnapshot` |
| `Key` union | `NullKey`, `BoolKey`, `Int32Key`, `Int64Key`, `Float64Key`, `TimestampKey`, `StringKey`, `UUIDKey`, `VectorKey`, `CompositeKey`; `CompareKeys` gives total order for range scans |
| `Descriptor` | `{NamespaceID, Field, IndexType, Unique, Retention}` |
| `IndexType` | hash, btree, fulltext, vector, composite |
| `Entry` | `{Key, DocID, AtCommit}` |
| `VersionedEngine` | the shared replay engine: `Put`/`Delete`/`Lookup`/`Range` **as of any commit**, `HeadBuckets`, snapshot round-trip |
| `eventLog` (internal) | chronological put/delete event slices plus a **bucket memo** keyed by `(cutoff commit, event counts, DAG ancestryVersion)` — an unchanged DAG serves repeat lookups without replaying |
| `MemoryStore` | `Store` over `VersionedEngine`, used by tests and the composite factory |
| `HintAction` / index hints | replicated index updates carried on `DeltaCommit` frames |

Versioned semantics: a lookup at commit *C* returns only entries whose `AtCommit` is an ancestor
of *C*, so an index answers historical queries without a rebuild.

### 1.11 `storage` — the adapter surface

```go
type Adapter interface {
    Capabilities() CapabilitySet
    GetDocument / GetDocumentOrThrow / GetDocuments / ScanDocuments
    PutDocument / DeleteDocument / DiscardPending
    CommitTree(namespaceID, parentTreeHash) (DocumentTree, error)
    Flush(namespaceID) error
    ReadBlob / WriteBlob
    IngestDeltaSegment(DeltaSegmentRef) error
}
```

| Type | Role |
|------|------|
| `EvictableAdapter` | `Adapter` + `EvictDocuments`, `EvictIndex`, `RebuildDocuments`, `RebuildIndex`, `EvictionState` |
| `CapabilitySet` | `PersistsDeltaLog`, `PersistsAcrossReload`, `SupportsGpuBulkRead`, `SupportsDirectDeltaIngest`, `IndexRetentionDefault` |
| `StorageEngineConfig` | `GlobalMemoryBudgetBytes`, `CompressionCodec`, `DefaultIndexRetention`, `IOShim`, `Durability`, `AsyncSyncIntervalMillis`, `WalMaxSegmentBytes`, `WalSkipCorruptRecords` |
| `Durability` | `Sync` (fsync before ack), `Async` (ack on queue, background flush), `MemoryOnly` |
| `CompressionCodec` | `None`, `ZSTD` |
| `DeltaSegmentWriter` / `DeltaSegmentReader` | append/seal and read/scan the delta log |
| `DeltaRecord` | `{CommitHash, NamespaceID, Authorship, CommitPayload}` |
| `DeltaAuthorshipEnvelope` | `{Principal, Timestamp, RightsToken, ClientContext}` |
| `DeltaSegmentRef` | `{SegmentID, NamespaceID, SequenceNumber, FirstCommitHash, LastCommitHash, SizeBytes, Compression}` |
| `EnlistmentEvictionState` | `Full`, `DocEvicted`, `Evicted`, `Released` |
| `PlatformIOShim` | the platform boundary — see §1.17 |
| `TotalSystemMemoryBytes()` | per-OS implementations (darwin/linux/other) feeding the memory budget default |

### 1.12 `storage/mem` — in-memory adapter

`InMemoryStorageAdapter` mirrors `ServerEngine`'s contract without any I/O: sharded document
map, per-namespace pending stage, blob shard, running document tree. Used by memory runtimes,
the RBAC in-memory registry, and most tests.

-----

## Layer 4a — Storage engine

### 1.13 `storage/engine.ServerEngine`

The production `Adapter`. One instance per namespace.

| Field | Role |
|-------|------|
| `docs *shardedDocStore` | **committed** documents, 64 independently-locked shards keyed by `UUID.LSB % 64` |
| `pending *shardedPendingStore` | **staged** puts/deletes, same sharding; a document is in at most one of puts/deletes |
| `tree` + `treeMu` | the running `DocumentTree`, updated incrementally on `CommitTree` |
| `memTable *memtable.Manager` | blob storage front end |
| `wal wal.WriteAheadLog` | blob WAL (nil for memory targets) |
| `groupCommit *wal.GroupCommitter` | coalesces concurrent fsyncs |
| `enlistmentStates` | eviction state per enlistment |
| `asyncStop/asyncDone` | background sync ticker under `DurabilityAsync` |

| Method | Behaviour |
|--------|-----------|
| `WriteBlob(bytes)` | hash → WAL append → durability step (sync via `GroupCommitter.SyncTo`, async = ack on append, memory = nothing) → `memTable.Put`. **Takes no engine-wide lock** — this is the 1M-writes/sec hot path |
| `ReadBlob(hash)` | memtable → pending-flush generation → SSTables |
| `PutDocument` / `DeleteDocument` | stage only; invisible to `GetDocument` until `CommitTree` |
| `DiscardPending` | drop the whole stage (transaction rollback) |
| `CommitTree(ns, parentTreeHash)` | take-and-clear the stage, apply deletes then puts to `docs`, fold each change into `tree` via the persistent trie, return the new tree. `parentTreeHash` is ignored — the engine tracks *current committed* state, not per-branch history |
| `ScanDocuments(ns, at, batchSize, onBatch)` | **streams** shard by shard; peak memory is one shard plus one batch, and a callback error stops the scan immediately |
| `RecoverBlobsFromWal` | replays `PutBlob` records into the memtable on open |
| `Flush` | memtable flush + WAL sync |
| `Close` | stops the async ticker, final WAL sync |

`DefaultFactory{EngineTarget}` opens engines per target (`Server` with WAL + delta writer,
`Browser`, `InMemory`, `GPU`) and returns a `Handle` bundling adapter + delta writer/reader.

### 1.14 `storage/wal`

| Type | Role |
|------|------|
| `WriteAheadLog` (interface) | `Append`, `AppendBatch`, `Sync`, `Recover`, `Truncate`, `Close`, `WalID`, `LastSequence`, `ActiveSegmentSizeBytes` |
| `DefaultWriteAheadLog` | mutex-guarded WAL over a **chain of segments**; rotates when an append would exceed `walMaxSegmentBytes` (default 64 MiB) |
| `Record` | `{Sequence, Timestamp, Kind, Payload}`; kinds: `PutBlob`, `DeleteBlob`, `FlushCheckpoint`, `Marker` |
| `DefaultFactory` | `OpenOrCreate` (reads existing chain, restores active segment size), `ActiveSegmentName` |
| `GroupCommitter` | coalesces concurrent `SyncTo(seq)` calls into as few physical `fsync`s as possible; waiters arriving during an in-flight sync are deferred to the next round |
| `SegmentInfo`, `RecoverySummary`, `CorruptionError`, `ClosedError` | metadata and typed failures |

Record framing and rotation naming are in [Part 4 §3](kdb-lld-storage.md).

### 1.15 `storage/memtable`

| Type | Role |
|------|------|
| `SortedTable` | insertion-ordered map with **tombstones**: `Put`, `Get`, `Lookup(value, deleted, found)`, `Delete`, `SizeBytes` |
| `Manager` | active + pending-flush generations over `LsmBlobStore`; `Put`, `Get` (active → pending → SSTables), `Delete`, `Flush(level)` |

`Lookup` (not `Get`) is what lets a delete shadow an older generation: `found=true, deleted=true`
means *stop searching*. Tombstones are dropped at flush because the SSTable format has no delete
marker — a documented limitation (a delete of an already-flushed blob holds only while the
tombstone lives in memory).

### 1.16 `storage/sstable`

| Type | Role |
|------|------|
| `DefaultWriter` | `Put(key,value)` buffers, `Finish()` writes one compressed block per entry, then the footer, then seals the segment |
| `DefaultReader` | `Get(key)`: read trailer → locate footer → parse index → read block → verify CRC → decompress |
| `Handle` | `{FileHash, Level, SegmentName}` |
| `BlockHandle` | `{Offset, CompressedSize, UncompressedSize}` |
| `BlockCache` | byte-capacity FIFO cache keyed by `(fileHash, offset)`; capacity is ¼ of the global memory budget |
| `LsmBlobStore` | ordered list of table handles, searched newest-first; `AddTable` copies the slice so concurrent readers keep a stable view |

### 1.17 `storage/io` — the platform I/O shim

| Type | Role |
|------|------|
| `PlatformIOShim` (in `storage`) | `AppendToSegment`, `ReadFromSegment`, `FlushSegment`, `SealSegment`, `ListSegments`, `DeleteSegment`, `AvailableBytes`, `ReadSnapshot/WriteSnapshot/DeleteSnapshot` |
| `SegmentByteStore` | the platform-specific half: `Append`, `Read`, `Flush(fsync)`, `MarkSealed`, `List`, `Delete`, snapshots |
| `FileBackedPlatformIO` | shared logic over any byte store: per-segment mutexes, sealed-segment tracking, name validation, append size cap. **`FlushSegment` deliberately does not take the segment mutex** so appends can proceed while an fsync is in flight (that is what makes group commit real) |
| `OSByteStore` | filesystem store with a **cached `*os.File` per segment** opened `O_APPEND`; reads clamp the requested length to the real file size before allocating |
| `InMemoryByteStore` | test/browser store |
| `PrimaryWithReplicas` | fans writes to replica sinks under a `ReplicationPolicy` (fail-open or fail-closed) |
| `SegmentNameBuilder` | canonical names: `ns/{ns}/delta/{seq}.seg`, `ns/{ns}/wal/{walId}[.{firstSeq}]`, `ns/{ns}/sstable/L{level}/{fileId}` |
| `ParseDeltaSequencedFileName` | parses `%020d.seg`; anything else is a *legacy* name and is refused rather than mis-ordered |
| `ValidateSegmentName` | must start with `ns/`, must not contain `..` |
| `SyncMode` | `Full` (F_FULLFSYNC on darwin) vs `Fast` (F_BARRIERFSYNC / fdatasync) |
| `s3.ReplicaSink`, `s3.BlobStore`, `s3.Config` | S3-compatible replication (LocalStack/MinIO/AWS), configured from `KDB_S3_*` |

### 1.18 `storage/delta` — the commit log

| Type | Role |
|------|------|
| `PageCodec` | v2 **KDBP** frame: `Frame(payload, codec)` / `Parse(frame)`; the codec id is recorded per frame so a segment may mix codecs and a config change never invalidates old data |
| `DefaultWriter` | `Append(DeltaRecord) (offset, error)`, `Flush`, `Seal() → DeltaSegmentRef`; tracks first/last commit hash and size |
| `DefaultReader` | `ReadAll`, `ReadRange`, `ListSegments` (in **sequence = commit order**) |
| `ScanSegmentBytes` | frame-by-frame scan; a *short* trailing frame stops cleanly (torn tail), a *CRC mismatch* returns `CorruptFrameError` **plus every commit scanned before it** |
| `Factory` | `OpenWriter` always starts a **new** segment at `maxExistingSeq+1`; `OpenReader` |
| `LegacySegmentFormatError` | refuses to open a namespace containing pre-Layer-13 random-UUID segment names, naming the repair command |

### 1.19 `compression`

`Compress(bytes, level)` / `Decompress(body, exactSize)` (zstd) and `CRC32`/`CRC32All`. Decompress
takes the *recorded* uncompressed size, so a wrong size is corruption, not a resize.

### 1.20 `storage/manager`, `tier`

`storage/manager` holds the Layer 4b skeleton (realized store pool, eviction manager, rebuild
scheduler, enlistment manager, tier signals) — the interfaces exist and are wired, the policies
are minimal in Go and fuller in Kotlin. `tier.Manager` emits `Signal`s on hot/warm/cold/ice
transitions and exposes `ArchiveCommit` (which is what turns a commit into a `CommitStub`).

-----

## Layer 5–6 — Query, policy, compaction

### 1.21 `sql`

Documented in depth in [Part 5](kdb-lld-query.md); the type inventory:

| Type | Role |
|------|------|
| `Parser` / `DefaultParser` / `rdParser` | recursive-descent parser for `SELECT`, `INSERT`, `CREATE TABLE` |
| `Statement` union | `StmtSelect`, `StmtInsert`, `StmtCreateTable` |
| `SelectQuery` | `{Distinct, Projections, From, Where, OrderBy, Limit, Offset}` |
| `Projection` union | `ProjStar`, `ProjColumn{Name,Alias}`, `ProjExpression{Expr,Alias}` |
| `Expr` union | `ExprLiteral`, `ExprColumnRef`, `ExprParameter`, `ExprBinary`, `ExprUnary`, `ExprFunctionCall` |
| `Cell` union | `CellNull`, `CellString`, `CellLong`, `CellDouble`, `CellJSON` |
| `Parameter` union | `ParamString`, `ParamInt`, `ParamDouble`, `ParamBool`, `ParamNull` |
| `QueryContext` | namespace, schema, `AtCommit`, parameters, `MaxRows`, **`RowBudget`** (rows *examined*), `Stats` |
| `ExecStats` | `RowsExamined`, `DocsRead`, `DocBytesRead`, `RetainedBytes` — the measured actual fed back to the cost model |
| `PhysicalPlan` union | `PlanFullScan`, `PlanFilter`, `PlanLimit` |
| `Planner` / `DefaultPlanner` | validates projected column names, emits full scan + limit, returns the residual predicate |
| `Executor` | `ExecuteSelect`, `ResolveDocIDsForWhere`, `fullScan`, `filterIDs`, aggregate path |
| `DMLExecutor` | `ExecuteInsert` → `[]document.Op` |
| `DDLExecutor` | `ExecuteCreateTable` → new `KdbSchema` |
| `Engine` / `defaultEngine` | `Execute` (SELECT/DDL) and `ExecuteDML` (INSERT) |
| `QueryShape` / `ShapeOfSelect` | literal-free structural fingerprint used by the cost model |
| `ScanRowBudgetExceededError` | typed abort surfaced as `RESOURCE_EXHAUSTED` |

### 1.22 `query/hybrid`

| Type | Role |
|------|------|
| `Engine` | `Execute`, `Prepare`, `Checkout(namespace, ref)`, `ResetCheckout` |
| `SQLParser` / `DefaultSQLParser` | `ParseWithVersion` strips a trailing `AT VERSION|COMMIT|TIME '<literal>'` and validates the remainder with the base SQL parser |
| `VersionClause` union | `AtTag{Tag}`, `AtCommit{Hex}`, `AtTime{ISO8601}` |
| `VersionResolver` | resolves a clause (or an active checkout) to a concrete commit hash |
| `CheckoutStore` | per-namespace pinned commit — the `kdb checkout` equivalent for queries |
| `Request` / `Result` | namespace, schema, parameters, max rows / rows + resolved commit |

### 1.23 `policy`

| Type | Role |
|------|------|
| `NamespacePolicy` | mode (mutable/append-only/immutable), history mode, compaction policy, tier policy, vector index policy, GPU promotion ref |
| `RetainRule` / `RetainStrategy` | retention granularity (all / daily / weekly / monthly …) |
| `CompactionPolicy` | `SquashAfter` (`Never`/duration), boundaries, peer safety |
| `TierPolicy` / `TierBand` / `IceTierBand` | hot/warm/cold/ice thresholds |
| `Parser` | DSL and JSON forms |
| `Validator` | `ValidationResult{Issues}`; ordering and consistency checks |
| `Registry` / `InMemoryRegistry` / `StorageBackedRegistry` | policy storage; the storage-backed form also writes a blob copy |
| `Evaluator` / `DefaultEvaluator` | `BoundaryCandidates(...)` → `[]BoundaryPlan`, the input to DAG compaction |
| presets | `DefaultMutable`, `AppendOnlyEvents`, `ScratchDocument`, `CacheNoHistory` |

### 1.24 `compaction`

`Engine{Plan, RunCycle, UpdatePeerHeads}` orchestrates **DAG squash** (distinct from
`storage-compaction`, which merges SSTables). `Plan` refuses when policy says
`squashAfter=NEVER` and reports `Blocker`s (`PolicyDisabled`, peer-safety); the Go
implementation is a skeleton with the policy gate and structure in place, the Kotlin
`:kdb-compaction` module carries the fuller boundary logic.

-----

## Layer 7–9 — Wire, stream, transports

### 1.25 `wire`

| Type | Role |
|------|------|
| `Header` | `{MessageType, ProtocolVersion, CorrelationID, PayloadLength}` |
| `Codec` / `DefaultCodec` | `Encode(Message)`, `Decode(frame)`, `EncodeFrameOnly`, `DecodeHeader` |
| `DecodeHeader` / `PeekHeader` | strict decode (whole frame present) vs. header-only peek used by early admission |
| `MessageType` | 0x01–0x18, see [Part 6 §2](kdb-lld-protocol.md) |
| message structs | `HandshakeMessage`, `HandshakeAckMessage`, `SessionBeginMessage/Ack`, `SqlExecMessage`, `SqlResultMessage`, `TxCommitMessage`, `TxRollbackMessage`, `DocumentGetMessage/Result`, `UpsertMessage/Result`, `TransactionReplayMessage`, `ConflictReportMessage`, `DeltaCommitMessage`, `CommitFetchMessage`, `CommitPushMessage`, `CommitPushAckMessage`, `DagDiffMessage`, `CompactionNoticeMessage`, `IceArchiveNoticeMessage`, `SnapshotRequest/Response`, `PositionAckMessage`, `SchemaPushMessage` |
| `HandshakeNegotiator` | version and encoding negotiation; rejects out-of-range protocol versions and empty encoding intersections |
| `PayloadEncoding` | `KdbBinary`, `JSON` (the JSON envelope is what ships today) |
| `EncodeTransaction` / `DecodeTransaction` | client-built transaction payloads for `TX_COMMIT` |
| `ErrorCode` | `BUSY`, `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `CONFLICT`, `SCHEMA_VIOLATION`, `UNAUTHORIZED`, `INTERNAL` |
| `IndexHint` | replicated index update carried on delta frames |

### 1.26 `stream`

| Type | Role |
|------|------|
| `Coordinator` | `Start(SessionConfig)`, `Publish(PublishedCommit)`, `Stop`, `Subscribers()`, `SetTransactionReplayer` |
| `Subscriber` | connects, handshakes, applies deltas, acks positions, and (Mode 2) submits transactions |
| `ClientMode` | `ClientReadOnly` (Mode 1) / `ClientWriteBack` (Mode 2) |
| `PublishedCommit` | `{CommitHash, ParentHash, Operations, IndexHints, TimestampMicros}` |
| `Connection` | live handle: `Position()`, `SubmitTransaction(tx) → ReplayResult` |
| `ReplayResult` | `{Applied *Hash, Conflict *ConflictReport, Rejected *string}` |
| `Event` / `EventKind` | `Connected`, `DeltaReceived`, `PositionUpdated`, `CompactionWarning`, `IceArchived`, `Disconnected`, `Error` |
| `TransactionReplayer` | server-supplied closure handling Mode 2 replay (keeps `stream` independent of `server`) |
| `Transport` / `InMemoryTransport` / `Hub` | in-process transport used by tests and the browser demo |
| `HintApplier` | applies index hints to a local index store |

### 1.27 `transport/core`, `transport/tcp`, `transport/ws`

| Type | Role |
|------|------|
| `TransportConnectOptions` | timeouts, `MaxFrameBytes` (16 MiB default), headers, TLS, `Admitter`, `MaxConnections`, `IncomingQueueFrames` (default **4**) |
| `TransportTlsSettings` | `Enabled`, `CAFile`, `CertFile`, `KeyFile`, `ServerName`, `InsecureSkipVerify`, `RequireClientAuth` (mTLS); `BuildTLSConfig(server bool)` |
| `FrameAdmitter` | `func(wire.Header) (rejection []byte, err error)` — consulted **before the body is buffered** |
| `tcp.Transport` | `Connect`, `Listen`, `ListenBound` (bind synchronously to learn the port), `Serve` |
| `socketConnection` | one goroutine per connection: `readLoop` is the *only* sender on and closer of `incoming`; blocking sends give real backpressure, `done` interrupts them on close |
| `ws.Transport` | the same contract over WebSocket, including `wss://` |

Schemes: `tcp://`, `tcps://`, `ws://`, `wss://`. A secure scheme with no usable TLS settings is a
hard error — never a silent downgrade.

-----

## Layer 8/12 — Peer sync, server, client, embed

### 1.28 `peersync`

| Type | Role |
|------|------|
| `HostConfig` | namespace, node id, hub, `MaterializeCommit`, `Persist`, conflict policy/resolver |
| `ClientConfig` | as above plus `PeerURI`, `ConnectionContext`, TLS |
| `HeadUpdate` | `HeadFastForward`, `HeadAlreadyAncestor`, `HeadDiverged` — the git classification |
| `ResolveHeadUpdate(dag, local, incoming)` | pure classification |
| `ResolveDivergence(...) → CommitPushOutcome` | performs the decision: `NoOp`, `FastForwarded`, `Merged` (real two-parent merge commit for disjoint document sets), `Conflict` (branch untouched, structured report returned). **Serialized per namespace** by a package-level lock map |
| `ResolutionOptions` | `Policy` (`Strict` = report, `LastWrite` = incoming wins, `Custom` = resolver) |
| `ComputeSyncPlan` | `{CommonAncestor, LocalOnly, RemoteOnly}` |
| `CommitsToPush(dag, localHead, remoteHead, limit)` | true set difference in parent-before-child order (never a plain `Walk` with an `until`, which would miss merge parents) |
| `Client` / `Host` | `PullMissing`, `PushLocal`, frame handlers, RBAC gate (`auth.PeerSyncAction`) |
| `Result` | `{AppliedCommits, PushedCommits, FinalHead, Plan, Conflict}` |

### 1.29 `server`

| Type | Role |
|------|------|
| `KdbServerRuntime` | the server-side façade over an embedded runtime: transaction engines (strict + last-write for upsert), SQL engine, `LockManager`, `AuthEngine`, `CommitListener`, write gate, memory guard/admission, draining state, schema RW lock, ref counting |
| `ServerRuntimeRegistry` | key → runtime with `GetOrOpen`/`Release` ref counting |
| `writeGate` | `queued` (bounded, default 64) + `running` (capacity 1) channels; `acquire(ctx)` returns `*BusyError` when the queue is full and `*DeadlineExceededError` when the caller's deadline passes; `quiesced()` for drain |
| `SessionManager` / `KdbSession` | per-connection sessions: id, namespace, `BaseVersion`, `ReadPin` (SNAPSHOT), read consistency, pending `transaction.Builder`, principal |
| `ReadConsistency` | `ReadCommitted`, `ReadYourWrites`, `Snapshot` |
| `MemoryGuard` | 200 ms sampler over cgroup `memory.current` (or `runtime/metrics` total−released), 5-sample moving average, four `Zone`s with hysteresis and dwell, observer callback |
| `Zone` | `Normal`, `Elevated`, `High`, `Critical` |
| `Admission` | the grant system: `semaphore.Weighted` over `limit − reserve`, a dynamic **non-granted floor** re-held every sample, per-zone class policy, per-zone scan row budgets, `rescueReserve`, `AdmissionStats` |
| `Grant` | one reservation; `Release()` idempotent |
| `CostModel` | per-class base+k calibration for writes, learned per-`QueryShape` p95 (with spread guard and minimum observations) for scans, per-namespace observed document size; `EstimateScan`, `EstimatePointRead`, `ObserveScanActual`, `ObserveDocSize`, JSON save/restore |
| `OpClass` | `PointRead`, `Scan`, `Write`, `Replication` |
| `AbortWatchdog` | orderly abort after sustained pressure: drain → close listeners → flush/seal → `os.Exit(75)` |
| `Listener` | listener handle with `Addr()` and `Close()` |
| `sqlWireConnHandler` | per-connection frame dispatch (handshake, session begin, SQL exec, tx commit/rollback, document get, upsert, transaction replay) |
| `StreamHub` | per-connection subscriber registry and `Publish` fan-out for Mode 1/2 |
| `AdminServer` | `/healthz`, `/readyz`, `/metrics`, `/debug/vars`, `/debug/pprof` |
| `frameAdmitter` | zone-policy check from the frame header alone, with a typed rejection frame |
| errors | `BusyError`, `DeadlineExceededError`, `UnavailableError`, `MemoryPressureError`, `ResourceExhaustedError`, `ConflictError`, `SchemaError`, `AuthorizationError` |

### 1.30 `client` — the Go SDK

| Method | Semantics |
|--------|-----------|
| `Connect(ctx, addr, token)` / `ConnectWithOptions(..., ConnectOptions{TLS})` | dial (`tcp`/`tcps`/`ws`/`wss`, or bare `host:port`), handshake, authenticate |
| `PutJSON(ctx, ns, docID, body) → commitHex` | create/replace at a caller-chosen id via an encoded transaction |
| `GetJSON(ctx, ns, docID) → (body, commitHex)` | point read; `ErrNotFound` when absent |
| `Upsert(ctx, ns, docID, body)` | unconditional write (server-side last-write policy); never conflicts |
| `Commit(ctx, Transaction{Namespace, BaseVersion, Writes})` | optimistic CAS; `*ConflictError` wrapping `ErrConflict` on conflict |
| `AppendEvent(ctx, ns, docID, body)` | append-only namespaces |
| `Query(ctx, ns, sql, args, &dest)` | decodes rows into a struct slice by case-insensitive column name |
| `QueryRaw(ctx, ns, sql, args)` | `(columns, rows)` for tooling and `_doc` |
| `Exec(ctx, ns, sql, args)` | DDL and INSERT (auto-committed as one unit of work) |
| `Close()` | idempotent |

Sentinel errors: `ErrConflict`, `ErrBusy`, `ErrUnavailable`, `ErrDeadlineExceeded`,
`ErrNotFound`, `ErrUnauthenticated`, `ErrClosed`, plus `TransportError`. One connection is safe
for concurrent use (every request carries a correlation id and the read loop demultiplexes).

### 1.31 `driver` — `database/sql`

| Type | Role |
|------|------|
| `Driver` | registered as `"kdb"`; `Open(name)` |
| `ParsedURL` / `ParseURL` / `ParseDSN` | `kdb://memory:///cat/ns?unique=true&dropOnClose=true`, `kdb://file:///path?...` |
| `memoryPool` | shared in-memory runtimes keyed by `(catalog, namespace, isolate)`; `unique=true` isolates, `dropOnClose=true` frees on close |
| `conn` / `stmt` / `rows` | `Prepare`, `Exec`, `Query`, `Ping`, `Catalog()`, `NamespaceID()`, `Runtime()` |

The Go driver is **embedded-only**; network SQL goes through `client` (or JDBC on the JVM).

### 1.32 `embed` — the embedded runtime

| Type / function | Role |
|-----------------|------|
| `EmbeddedKdbRuntime` | `{Catalog, DAG, Storage, Schema, DefaultNamespace, WriteBaseVersion, DataRoot}` plus `release` and `storageClose`; `Close()` flushes/seals then releases the directory lock |
| `OpenMemoryRuntime(catalog, ns, schema)` | in-memory DAG + `storage/mem` adapter |
| `OpenFileRuntime` / `OpenFileRuntimeWithOptions` | directory lock → namespace dirs → platform IO (with optional S3 replica) → `ServerEngine` handle → **delta replay** → `PersistingCommitDAG` |
| `FileRuntimeOptions` / `StorageOptions` | S3 config, replication policy, durability, compression, async interval, sync mode |
| `PersistingCommitDAG` | `dag.CommitDAG` wrapper that persists every appended commit; `Persist`, `PersistAsync` (queue under a lock, wait after releasing it), `Delegate()`, `Close()` |
| `commitLogWriter` | single drain goroutine, batch ≤ 256, coalesced flush, **fail-stop latch** on any append/flush error |
| `replayDeltaNamespace` | reads every segment in sequence order, tolerates a torn tail **only on the most recent segment**, then applies commits **topologically** |
| `applyCommitsTopologically` | round-based: a commit applies only once all parents are present; no progress in a round = the log is missing data (named error) |
| `MaterializeCommit` | replays one commit's ops into storage and verifies the rebuilt tree hash matches the commit's declared hash (`TreeHashMismatchError`) |
| `PutJSONDocument` | the simple embedded write path used by the CLI |
| `acquireDirLock` / `LockDataDir` | exclusive `flock` on `{dataRoot}/.kdb.lock` (unix) with a portable fallback |
| `OpenFileAuthRegistry` | durable RBAC registry under `_system/users`, `_system/roles` |
| `buildSegmentByteStore` | OS store, optionally wrapped with S3 replicas |

-----

## Layer 11 — Security

### 1.33 `auth`

| Type | Role |
|------|------|
| `Engine` | `Authenticator()` + `Authorizer()`; `AllowAll` is the default |
| `Credentials` | `{User, Password, Token}` |
| `Principal` | `{ID, Roles, Attributes}` |
| `Action` union | `SessionBeginAction`, `SqlExecAction{ReadOnly}`, `DocumentWriteAction`, `DocumentDeleteAction`, `DocumentReadAction`, `PeerSyncAction` |
| `ResourcePath` | `{Namespace, Collection, DocID}` |
| `PermissionKind` / `PermissionMatchesPath` / `PrincipalHasPermission` | wildcard-aware grant matching (`read:myapp/*`, `write:myapp/users/<id>`) |
| `HashPassword` / `VerifyPassword` | PBKDF2-HMAC-SHA256, cross-verified against the Kotlin implementation by a golden test |
| `RegistryAuthStore` | users and roles as documents in `_system/users` / `_system/roles`, each mutation a commit; `CreateUser`, `VerifyPassword`, `AssignRole`, `RevokeRole`, `CreateRole`, `UpdateGrants`, `GrantsByRole` |
| `RegistryAuthEngine` | `Engine` over the store |

-----

## Layer 15 — Integrity, backup, recovery

### 1.34 `integrity`

| Type / function | Role |
|-----------------|------|
| `Level` | `L1` (per-frame CRC/structure), `L2` (cross-segment parent closure); L3 (semantic) specified, not implemented |
| `Classification` | `torn_tail`, `mid_log_corruption`, `missing_parent`, `legacy_segment`, … |
| `Finding` | `{Classification, Segment, Offset, CommitHash, Detail}` |
| `Report` | `{Findings, Segments []SegmentSummary}`; `Clean()` |
| `Verify(shim, ns, Options)` | walks the log, changes nothing |
| `Repair(shim, report, Options)` | acts **only** on findings Verify produced: truncates a torn tail; quarantines a corrupt segment (into `ns/{ns}/quarantine/…`, deliberately outside `delta/`) and rewrites its good prefix **only if parent closure still holds**; otherwise refuses and names the commits that would be lost. Idempotent |
| `GenesisCommitHash(ns)` | reconstructs the never-logged genesis commit |
| `ScanVerifiedCommits`, `VerifiedSegmentPrefix`, `ListSequencedSegments` | the primitives backup and restore build on |

### 1.35 `backup`

| Type / function | Role |
|-----------------|------|
| `ObjectStore` | `Put`, `Get`, `Delete`, `List` — satisfied by `DirStore` and the S3 blob store |
| `Manifest` | the single JSON object whose presence defines a backup (written **last**): format version, namespace, backup id, created-at, segment entries (name, sequence, size, SHA-256, full-vs-prefix), tips |
| `Create(shim, ns, store, baseBackupID)` | sealed segments in full; the active segment as its CRC-verified prefix; with a base id, unchanged segments are referenced rather than re-uploaded (incremental) |
| `Verify(store, ns, backupID)` | re-downloads and re-hashes every named object |
| `FetchToDir` | materialises a backup as a data-directory-shaped tree |
| `ListBackups` | ids present for a namespace |

### 1.36 `recovery`

`HybridRestore(sources, ns, codec, out)` unions the **CRC-verified** commits from any number of
sources (a damaged directory, a fetched backup, or both), orders them topologically, and writes
a fresh sequenced delta log. A commit whose parent exists in no source is reported in
`Result.MissingHashes` rather than applied out of order.

-----

## Tooling and support packages

### 1.37 `inspect`

`WireFrameInspector.DumpFrame(frame, pretty)` renders a captured wire frame as JSON (header,
message type, decoded payload) — the `kdb-inspect dump-wire` backend.

### 1.38 `config`

| Type | Role |
|------|------|
| `ProductConfig` | `kdb-product.json` parity with Kotlin `kdb-config` |
| `ServiceSettings` | one field per `kdb-service` flag |
| `ServiceFile` | JSON config shape; **unknown fields are rejected** so a typo fails at startup |
| `ResolveService` | merges `defaults < config file < KDB_* env < explicitly-set flags` |
| `ParseDurability/Compression/SyncMode` | validated enum parsing |

### 1.39 `metrics`, `version`

`metrics.Recorder` accumulates per-stage latency samples (`lock_wait`, `fsync_wait`,
`tree_rebuild`) with a 4096-sample ring for percentiles; `metrics.Default` is the process-wide
instance the admin `/metrics` endpoint exposes. `version.Get()` returns
`{Version, Commit, Dirty, BuildDate}` stamped at build time from the repo `VERSION` file and git.

### 1.40 `compute`, `compute/webgpu`

The GPU adapter surface (`Adapter`, dispatch errors, capability probing) with a CPU fallback;
WebGPU has separate native and WASM builds. Bulk-read and vector-similarity acceleration are
specified in Layer 9 and remain a fallback-only implementation today.

-----

## Cross-references

- The **runtime relationships** between these types: [Part 0 §4](kdb-lld.md)
- How they **execute a request**: [Part 2 — Flows](kdb-lld-flows.md)
- Which of them are **touched concurrently**: [Part 3 — Concurrency](kdb-lld-concurrency.md)
- What they **write to disk**: [Part 4 — Storage](kdb-lld-storage.md)
