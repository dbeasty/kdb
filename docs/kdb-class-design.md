# KDB Class & Type Design

Status: reference document, verified against the Go tree under `go/kdb/*` (module
`github.com/limidus/kdb/go`, Go 1.26) and the Kotlin Multiplatform modules at the repository root
(`kdb-*`) on `main` of 2026-10-23 (commit `e79bb674`; first verified 2026-10-11 at `838a08e0`).

`docs/kdb-lld-components.md` is the older low-level design. It is a good orientation but predates
peer-sync v2, `replication`, `syncnode`, `control`, the cross-namespace transaction coordinator,
history modes, attachments, the mapped commit graph and tree files (it does cover `Admission` and
peer-sync v1). Where this document and the LLD disagree, this
document was checked against code. Section 9 lists the specific staleness and oddities found.

## 0. Reading guide and conventions

* **Signatures** are the real Go signatures, abbreviated only by dropping unexported parameters of
  no interest. Class diagrams (Mermaid `classDiagram`) use simplified signatures (no package
  qualifiers, no multi-value returns spelled out) because Mermaid does not parse Go syntax.
* **Arrow legend** in every class diagram:
  * `A <|.. B` : B implements interface A (Go structural typing; an explicit `var _ I = (*T)(nil)`
    assertion exists only for a handful of types, see section 3).
  * `A *-- B` : A owns B and controls its lifetime (composition).
  * `A o-- B` : A holds a reference to B but does not close it (aggregation).
  * `A ..> B` : A depends on / calls B.
* **Naming idioms.** `Foo` / `FooOrThrow` pairs: `GetCommit` returns `(Commit, bool)`,
  `GetCommitOrThrow` returns `(Commit, error)`. The "throw" is a leftover of the Kotlin API shape;
  Go never panics there. `New*` constructors return pointers; value types (`Hash`, `UUID`,
  `Timestamp`, `DocumentTree`, `Commit`, `Transaction`) are passed and compared by value.
* **Identity types.** `codec.Hash` (32 bytes, SHA-256), `codec.UUID` (`MSB`,`LSB` int64),
  `codec.Timestamp` (`EpochMillis` + `MicroRemainder`). All are comparable and used as map keys.
* **Namespace.** The unit of versioning. Each namespace has its own commit DAG, document trie,
  delta log, WAL, indexes, schema and policy. `catalog` is the prefix of a namespace id up to the
  first `/` (`embed.CatalogFromNamespace`).
* **Go is the reference implementation.** Kotlin mirrors the single-node core (sections 2.1-2.6,
  including attachment tables; `kdb-jdbc`'s `file` package reads and writes Go checkpoints), v1 peer sync and the peer-sync v2 core with blob transfer; `kdb-replica` is the Kotlin
  client replica. Everything else from section 2.7 onward is Go-only or mostly Go-only (section 7).

## 1. Architecture at a glance

### 1.1 Layering

![Diagram 1](diagrams/class-01.svg)

<details><summary>Mermaid source</summary>

```mermaid
flowchart TB
  subgraph L0["Foundation"]
    error["error"]; codec["codec + codec/schema"]; json["json"]; compression["compression"]
    metrics["metrics"]; version["version"]; file["file (unused)"]; compute["compute (+webgpu)"]
    fsx["fsx"]; memtune["memtune"]; mmapfile["storage/mmapfile"]; bufpool["storage/bufpool"]
  end
  subgraph L1["Data model"]
    document["document (+attachments, tree slab, tree files)"]; schema["schema"]; dag["dag (+graph segments)"]
  end
  subgraph L2["Write path / indexing / policy"]
    transaction["transaction"]; index["index (+fulltext, vector, fusion, analyzer, stores)"]
    policy["policy"]; script["script"]; compaction["compaction (unwired)"]
  end
  subgraph L3["Storage"]
    storage["storage (ports)"]; sengine["storage/engine"]; wal["storage/wal"]
    sdelta["storage/delta"]; mt["storage/memtable"]; sst["storage/sstable"]
    sio["storage/io (+s3)"]; smem["storage/mem"]; smgr["storage/manager"]
    attachstore["storage/attachstore"]; deltaindex["storage/deltaindex"]
  end
  subgraph L4["Query"]
    sql["sql"]; hybrid["query/hybrid (unwired by server)"]
  end
  subgraph L5["Wire & transport"]
    wire["wire"]; stream["stream"]; tcore["transport/core"]; ttcp["transport/tcp"]; tws["transport/ws"]
  end
  subgraph L6["Runtime"]
    embed["embed"]; server["server"]; auth["auth"]; config["config"]
  end
  subgraph L7["Distribution"]
    peersync["peersync"]; replication["replication"]; syncnode["syncnode"]
  end
  subgraph L8["Edges"]
    client["client"]; driver["driver"]; control["control"]; service["service (Main)"]
  end
  subgraph L9["Ops"]
    integrity["integrity"]; backup["backup"]; recovery["recovery"]
  end
  codec --> error
  document --> mmapfile
  dag --> document
  dag --> mmapfile
  transaction --> dag
  transaction --> storage
  index --> dag
  index --> fsx
  storage --> document
  sengine --> storage
  sengine --> wal
  sdelta --> sio
  sdelta --> bufpool
  attachstore --> sio
  attachstore --> document
  deltaindex --> sdelta
  deltaindex --> mmapfile
  sql --> storage
  stream --> wire
  embed --> sengine
  embed --> dag
  server --> embed
  server --> sql
  server --> peersync
  peersync --> wire
  replication --> peersync
  syncnode --> replication
  syncnode --> server
  service --> syncnode
  service --> memtune
  embed --> attachstore
  control --> server
  client --> wire
  driver --> embed
  integrity --> sio
```

</details>

Dependency direction rules actually enforced by the import graph:

* `driver` imports `embed`, `transaction`, `sql`, `dag`, `document`, `schema`, `codec` but **not**
  `server` (verified by import grep), so `database/sql` users do not link the network stack. That is why
  `embed.CommitGroup` takes the `GroupParticipant` interface and two independent participants exist:
  `server.serverParticipant` (a write gate) and `driver.driverParticipant` ("a namespace mutex where
  the server has a write gate", `driver/commit.go`).
* `peersync` never imports `server`. It reaches the local node through two small interfaces it
  defines itself (`peersync.LocalNode`, `peersync.NamespaceProvider`); `server` implements them
  (`serverLocalNode`, `peerNamespaceProvider`, both in `server/peersync_ingest.go`).
* `compaction` and `query/hybrid` have **no non-test importer** inside `go/` (verified by import
  grep); `file` has none either. They are complete, tested ports of the Kotlin modules that the Go
  server does not wire in: Go's compaction is `embed.Maintain`/`checkpointAndTruncate`, and Go's SQL
  path calls `sql.Engine` directly. There is no Go `tier` package at all (Kotlin `kdb-storage-tier`
  has no Go port); Go's history tiering is `storage.HistoryMode` + `ReclaimMode`.
* `document` and `dag` import `storage/mmapfile` (and nothing else under `storage`): tree segments
  and commit-graph segments are mapped files owned by the data model, not by the storage engine.
* `fsx` (Windows-safe `OpenFile`/`Rename`/`Remove`/`RemoveAll`/`SyncDir`, plain `os` elsewhere) is
  imported by every package that renames or deletes files under the data root (`embed`, `index`,
  `storage/io`, `config`, `backup`, `replication`, `recovery`, `peersync`, `server`, `control`).

### 1.2 Object graph of one running namespace (file-backed, server mode)

The following is what `embed.Host.openNamespace` (`embed/file.go`) actually builds, then
`server.NewKdbServerRuntime` wraps.

![Diagram 2](diagrams/class-02.svg)

<details><summary>Mermaid source</summary>

```mermaid
classDiagram
  class Host {
    -dataRoot string
    -opts FileRuntimeOptions
    -lock dirLock
    -io PlatformIOShim
    -arbiter BudgetArbiter
    -adapter MultiplexAdapter
    -txn TxnCoordinator
    -nss map~string,namespaceEntry~
    +Namespace(catalog, ns, schema) EmbeddedKdbRuntime
    +NamespaceWithOptions(catalog, ns, schema, sopts) EmbeddedKdbRuntime
    +ReopenNamespace(catalog, ns, schema, sopts) ReopenResult
    +CloseNamespace(ns) error
    +Close() error
  }
  class EmbeddedKdbRuntime {
    +Catalog string
    +DAG CommitDAG
    +Storage Adapter
    +Schema KdbSchema
    +DefaultNamespace string
    +WriteBaseVersion Hash
    +DataRoot string
    +ReadOnly bool
    -attach attachmentState
    +Maintain() TruncationResult
    +SetHistoryMode(to, reclaim) HistoryModeSwitch
    +AttachmentStore(ns) attachstore.Store
    +PutAttachment(ns, docID, r, opts) Hash,AttachmentInfo
    +CommitGraphStatus() GraphFileStatus
    +Refresh() error
    +Close()
  }
  class PersistingCommitDAG {
    -delegate InMemoryCommitDag
    -writer DeltaSegmentWriter
    -log commitLogWriter
    -barrier atomic.Pointer~TxnGroup~
    -attachmentsCommitted atomic.Pointer
    +Persist(commit) error
    +PersistAsync(commit) wait,error
  }
  class ServerEngine {
    -wal WriteAheadLog
    -memTable memtable.Manager
    -blobStore LsmBlobStore
    -docsByHash shardedDocByHashStore
    -deltaLoader deltaColdLoader
    -tree DocumentTree
    -treeFile TreeFile
    -treesByHash boundedTreeStore
  }
  class KdbServerRuntime {
    +Runtime EmbeddedKdbRuntime
    +TransactionEngine Engine
    +UpsertEngine Engine
    +DocumentLocks LockManager
    +UniqueKeys UniqueKeyRegistry
    +Conflicts ConflictQueue
    -writeGate writeGate
    -changes changeLog
    -admission Admission
    -memGuard MemoryGuard
    +Commit(ns, tx, session, principal) Commit
    +CommitImmediate(principal, session, build) Commit,bool
    +Upsert(ns, id, json, principal) Commit
    +GetDocument(ns, id) json,hex,found
  }
  class TreeFile {
    +NewTreeFile(TreeFileOptions) TreeFile
    +Close()
  }
  Host *-- EmbeddedKdbRuntime : one per namespace
  Host *-- TxnCoordinator
  EmbeddedKdbRuntime o-- PersistingCommitDAG : DAG field
  PersistingCommitDAG *-- InMemoryCommitDag : delegate
  EmbeddedKdbRuntime o-- ServerEngine : Storage field
  EmbeddedKdbRuntime *-- AttachStore : attachstore.Store per namespace
  InMemoryCommitDag o-- ServerEngine : SetTreeStore
  InMemoryCommitDag *-- AttachmentSetPool
  InMemoryCommitDag *-- nodeStore : resident records + graph segments
  ServerEngine o-- TreeFile : live tree paging
  KdbServerRuntime o-- EmbeddedKdbRuntime
  NamespaceSet o-- KdbServerRuntime : runtimes map
  NamespaceSet o-- TxnCoordinator
```

</details>

Three facts that are easy to miss:

1. `EmbeddedKdbRuntime.DAG` is typed `dag.CommitDAG` (the narrow interface) but nearly all of the
   server needs the concrete `*dag.InMemoryCommitDag`; `NewKdbServerRuntime` type-switches on
   `rt.DAG` (`*dag.InMemoryCommitDag` for memory mode, `*embed.PersistingCommitDAG` for file mode,
   unwrapping via `Delegate()`). Any new DAG wrapper must be added to that switch.
2. The `ServerEngine` doubles as the DAG's `DocumentTreeStore` (`d.SetTreeStore(eng)`), so trees
   exist in one place only; the DAG's own `trees` map is empty in an assembled engine. With
   `tree.file` on (the default where `mmapfile.Supported`), `openNamespace` creates a
   `document.TreeFile` *before* restore and hands it to the engine (`eng.SetTreeFile`), so the
   checkpoint's live tree is mapped from its segments rather than rebuilt in the heap; on a host
   without mapping (`ErrTreeFileUnsupported`, e.g. Windows) the live tree stays in the slab.
3. Commits are held in `InMemoryCommitDag.nodes` (a `nodeStore` of fixed-width records, not Go
   structs); with the graph file on (`ConfigureGraphFile` + `OpenGraphFile`) older records are
   mapped graph segments. Attachment sets live in the DAG's `AttachmentSetPool`, spilled to the
   engine's blob store when over budget (`SetAttachmentSetStore(engineSetStore{eng}, budget)`);
   attachment *bytes* live in a separate per-namespace `attachstore.Store`.

### 1.3 Write path (one client transaction)

![Diagram 3](diagrams/class-03.svg)

<details><summary>Mermaid source</summary>

```mermaid
sequenceDiagram
  participant C as client.Client
  participant H as sqlWireConnHandler (server/wire_listen.go)
  participant R as KdbServerRuntime
  participant A as Admission + writeGate
  participant T as transaction.Engine
  participant D as PersistingCommitDAG
  participant S as ServerEngine
  C->>H: TX_COMMIT (wire frame)
  H->>R: Commit(ns, tx, session, principal)
  R->>R: admitWrite (draining? read-only? fenced? projection? home? RBAC)
  R->>R: dag.Pin(base) + pinBaseTree(base)
  R->>A: admission.Acquire(ClassWrite, bytes) then writeGate.acquire(ctx)
  R->>R: commitAtGateFront: prepareIndexes(tx)
  R->>T: Commit(tx, dag, store, schema, head, msg)
  T->>T: plan: validate schema, attachment preflight, detect conflicts, unique keys, preconditions
  T->>S: PutDocument / DeleteDocument / CommitTree(anchorTree)
  T->>D: AppendCommit on Delegate() (the InMemoryCommitDag, not the persisting wrapper)
  T-->>R: ResultSuccess{Commit}
  R->>R: changes.record(commit) (changeLog for immediate UPDATEs)
  R->>R: preparedForStored + commitToIndexes(stored, commitHash)
  R->>D: PersistAsync(commit) returns wait()
  R->>A: releaseNow() (writeGate released before fsync wait)
  R->>R: indexProvider.maintain() (store bookkeeping, off the gate)
  R->>D: wait() (group-commit fsync)
  R->>R: CommitListener(ns, commit) (StreamHub.Publish, replicator.OnLocalCommit), notifyHeadWatchers
  R-->>H: Commit
  H-->>C: SQL_RESULT (commit hash) / error classified to wire.ErrorCode
```

</details>

Notes: the `writeGate` serialises *commit planning and DAG append* (capacity-1 channel, bounded
queue, EWMA of service time used for `RetryAfterMs`), but the durable-fsync wait happens **after**
the gate is released (`releaseNow()` before `waitDurable()`), so concurrent writers share one
group-commit flush (`commitLogWriter` batches `logRequest`s on a channel). On any non-success
`TransactionResult` the runtime converts it to a typed error (`ConflictError`, `SchemaError`, or the
`ResultAborted.Cause`). Everything from `prepareIndexes` to the release is `commitAtGateFront`,
shared with `CommitImmediate(principal, session, build)`, whose operations are built *at* the gate
front against the live head (immediate `UPDATE`s such as `UPDATE ... ORDER BY ... LIMIT ...
RETURNING` queue claims, `server/immediate_update.go`, and immediate `INSERT`s,
`immediate_insert.go`); `changeLog` lets those builds find what changed since a recorded commit without
diffing trees. Index-store bookkeeping (`RegistryIndexProvider.maintain`: `Registry.BoundHistoryIfDue`, then
rebuilding stores in `NeedingRebuild`) runs after the gate is released, overlapped with the fsync
wait; a pass already running makes it a no-op (`maintainMu.TryLock`).

### 1.4 Read path (point read)

`KdbServerRuntime.GetDocument(ns, id)` -> resolve head and commit with one atomic pointer load
(`DAG.HeadCommit()`, backed by `InMemoryCommitDag.head atomic.Pointer[headSnapshot]`, no lock) ->
`storage.Adapter.GetDocument(ns, id, commit.DocumentTreeHash)` -> `ServerEngine` looks up the tree
(`latestTree` atomic snapshot or `treesByHash` bounded LRU; a file-backed live tree reads cold trie
records from mapped `TreeFile` segments) for `docID -> contentHash`, then the document body in the
sharded by-hash cache (`docsByHash`; live bodies are evictable too under `BoundedLiveBodies`). A
miss is a cold load (`coldLoads` counter): the LSM blob store first, only under a history mode that
writes into it, otherwise `deltaColdLoader`, which finds the version's frame through the
`deltaindex` sidecars and reads that one frame from the delta log (via the per-namespace
`delta.FrameCache`). A document past its expiry is reported absent (`isExpiredAtHead`). SQL reads
go through `sql.Executor` with `QueryContext.AtCommit` set from the session's `ReadConsistency`.

---

## 2. Package reference

Each package entry has: **Purpose**, **Key types** (real names and fields/methods), **Lifecycle**
and, per group, a class diagram.

### 2.1 Foundation

#### 2.1.1 `error` (import alias `kdberr`)

**Purpose.** Stable numeric error codes and the typed error hierarchy shared by every layer; also a
small `Result[T]` type.

**Key types.**

```go
type Code int                       // stable numeric values, see section 4
func (Code) Numeric() int
type Exception interface { error; Code() Code }
type base struct { code Code; msg string; cause error }   // unexported, embedded by pointer
func (*base) Error() string; Unwrap() error; Code() Code
func IsException(err error) bool
func CodeOf(err error) Code
func AsException(err error) (Exception, bool)
```

Concrete exceptions (all embed `*base`; constructors `NewXxx`): `DecodeError{Offset}`,
`EncodeError`, `SchemaError`, `JsonPathError{Path}`, `VersionNotFoundError{NamespaceName,
Reference}`, `IceStorageError{NamespaceName, Hash, ArchiveLocation}`, `ConflictError{Report
ConflictReport}`, `DocumentLockedError{NamespaceName, DocID, Owner}`,
`NamespaceNotFoundError`, `TransportErr`, `UnsupportedProtocolVersionError{PeerVersion,
SupportedVersion}`, `EncodingNegotiationFailureError{LocalEncodings, RemoteEncodings}`,
`ConnectionClosedError{BufferedBytes}`, `SchemaViolationError{Violations []FieldViolation}`,
`SchemaMigrationError{FieldName, Step}`.

Payload structs: `FieldViolation{FieldName, ViolationType, Detail}` (`ViolationType`:
`RequiredFieldMissing`, `TypeMismatch`, `UniqueConstraint`, `EnumValueNotDeclared`,
`CustomConstraint`), `ConflictReport{TransactionID, BaseHash, TargetHash, Conflicts
[]ConflictItem}`, `ConflictItem{DocumentID, OperationType, LocalDoc, IncomingDoc,
ActualContentHash}`, `ConflictOperationType` (`ConcurrentWrite`, `WriteDelete`, `DeleteWrite`,
`SchemaIncompatible`, `PreconditionFailed`; has JSON marshalling).

`Result[T]`: `Ok[T](v)`, `Fail[T](e Exception)`, `Run[T](fn func() (T, error))`, methods
`IsSuccess/IsFailure/Value/Exception/MustValue`. Used by `schema.Validate` and
`schema.ApplyMigration`; almost nothing else.

**Lifecycle.** Immutable values; no state.

#### 2.1.2 `codec` and `codec/schema`

**Purpose.** The deterministic binary encoding used for commits, documents, trees and wire
payloads, plus the identity primitives. `codec/schema` is the codec's own type system (not the
user-facing `schema` package).

```go
// primitives
type UUID struct{ MSB, LSB int64 };  func RandomUUID() (UUID, error); func ParseUUID(string)
(UUID).AppendBytes(dst []byte) []byte                              // 16 bytes, no separate alloc
type Hash struct{ Bytes [32]byte };  func HashFromHex(string) (Hash, error); (Hash).Hex() string
type Timestamp struct{ EpochMillis int64; MicroRemainder int }; func TimestampNow() Timestamp
func DerivedUUID(s string) UUID                                  // name-based, deterministic

// dynamic value tree (sealed interface, isValue())
type Value interface{ isValue() }
// NullValue BoolValue Int32Value Int64Value Float64Value StringValue BytesValue ArrayValue
// MapValue{[]MapEntry} RecordValue{Fields map[int]Value} EnumValue UnionValue FixedValue
// DateValue TimestampValue UUIDValue
func EncodeBytes(v Value, typ schema.Type, reg *schema.Registry) ([]byte, error)
func DecodeBytes(b []byte, typ schema.Type, reg *schema.Registry) (Value, error)
func DecodeFrom(r io.Reader, typ schema.Type, reg *schema.Registry) (Value, error)
```

`codec/schema`: sealed `Type` (`Primitive{Physical PhysicalKind; Logical LogicalAnnotation}`,
`Ref{FullyQualifiedName}`, `Nullable`, `Array`, `Map`, `Union`), named schemas (`RecordSchema`
with numbered `FieldSchema{ID,Name,Type,Default}`, `EnumSchema`, `FixedSchema`) and
`Registry` (`RegisterRecord/Enum/Fixed`, `Freeze`, `Resolve`). Fields are encoded by numeric id,
which is what makes the format evolvable. `BuiltinRegistry()` is the base; `document.WireRegistry()`
and `wire`'s registry extend it.

**Lifecycle.** A `Registry` is built once at init, frozen, then read concurrently.

#### 2.1.3 `json`

**Purpose.** KDB's own JSON value tree, path language, merge-patch and three-way field diff
(used for document merge and for conflict resolution; it is *not* `encoding/json`).

```go
type Value interface{ isJSONValue() }   // StringValue NumberValue IntValue BoolValue NullValue
                                        // ObjectValue{Keys []string; Fields map[string]Value} ArrayValue
type Path struct{ Expression string }; func CompilePath(expr string) (*Path, error)
func Get/GetAll/Set/Delete/Merge/Contains/Keys/TypeName/ArrayLength(jsonText string, ...) ...
func DiffConflict(base, local, remote Value) []FieldChange   // Kind: ChangeLocal|Remote|Both|Same
func FromKdbValue(codec.Value) (Value, error); func ToKdbValue(Value) (codec.Value, error)
func CanonicalText(v Value) string                           // key-order-stable text used for hashing
```

`ObjectValue` keeps key order (`Keys`) so round-trips preserve author order while `CanonicalText`
sorts for hashing. `CompilePath` results are memoised in a process-wide map guarded by `pathCacheMu sync.Mutex`.

#### 2.1.4 Small foundation packages

| Package | Contents | Notes |
|---|---|---|
| `compression` | `Compress(input, level)`, `CompressAppend(dst, input, level)`, `Decompress(input, maxOut)`, `DecompressInto(dst, input, maxOut)` (zstd), `ErrOutputTooLarge`, `CRC32`, `CRC32All` | Pooled encoders, one shared decoder. `maxOut` (default 64 MiB) is checked against the frame header's declared size *before* allocating. `CRC32` is now `hash/crc32` (hardware CRC), byte-identical to the old table loop it replaced. |
| `metrics` | `Recorder` (`Record`, `Track`, `Snapshot() []StageSnapshot`), stage names `StageLockWait`, `StageFsyncWait`, `StageTreeRebuild` | One `sync.Mutex`; consumed by `embed/commit_log.go`, `storage/engine`, `server/admin.go`, `control/ops_dashboard.go`. |
| `version` | `Info{Version, Commit, BuildDate, Dirty, GoVersion, OS, Arch}`, `Get()` | Build-stamped via `-ldflags`. |
| `file` | `Metadata{Name, ContentType, Size, SHA256}`, `Ingest(name, ct, r) (Metadata, []byte, error)` | 30 lines, no non-test importer. Go attachments are `document.Attachment*` + `storage/attachstore` + `embed.PutAttachment` (sections 2.2.1, 2.2.3); Kotlin `kdb-file` keeps the zip/bundle ingest. |
| `compute` | `Adapter{Name(); Dot(ctx, a, b []float32) (float32, error)}`, `CPUAdapter`; `compute/webgpu.Adapter` (build-tagged native and wasm) | Only the dot-product seam exists; not referenced by the vector index. |
| `inspect` | `WireFrameInspector.DumpFrame(frame, pretty)` | Used by `cmd/kdb-inspect`. |
| `fsx` | `Open`, `OpenFile`, `Rename`, `Remove`, `RemoveAll`, `SyncDir` | Exactly `os` on unix; on Windows opens with `FILE_SHARE_DELETE`, retries sharing violations, renames with `MOVEFILE_WRITE_THROUGH`, and `SyncDir` is a no-op. |
| `memtune` | `Options{HeadroomPercent, MinHeadroomBytes, MaxHeadroomBytes, SmallHeapBytes, Interval}`, `(Options).GCPercent(live)`, `Start(o) *Tuner` (`Active`, `GCPercent`, `Stop`) | Samples the live heap (`runtime/metrics`) and sets GOGC so the goal is live + headroom (default 25%, 128 MiB..2 GiB) above 256 MiB live; never sets a memory limit, stands down if GOGC is set. Started by `service` and `cmd/kdb-diskbench`. |

### 2.2 Data model: `document`, `schema`, `dag`

#### 2.2.1 `document`

**Purpose.** The immutable value objects of the versioned store: documents, operations,
transactions, commits, the document trie (and its slab and tree files), attachment tables and refs.

```go
type Document struct{ ID codec.UUID; JSON string }
func FromJSON(jsonText string) (Document, error)                // top-level "id": UUID string = identity,
                                                                // other string = codec.DerivedUUID(s), absent = minted
func FromJSONWithID(id codec.UUID, jsonText string) (Document, error); func ResolveID(jsonText) (id, supplied, err)
func (Document) ContentHash() (codec.Hash, error); Merge(patchJSON string); WithJSON(string)
func (Document) RebasedOnto(before, onto *Document) (Document, error) // field-level rebase used by merge
type ContentHasher struct{ /* reusable buf */ }; (*ContentHasher).Hash(id, json) / HashBytes(id, []byte)

type Op interface{ isOp(); toValue() codec.Value }             // sealed; union branch in ()
type WriteOp  struct{ DocID codec.UUID; Patch string; Replace bool } // (0) Replace: request-only, never stored
type DeleteOp struct{ DocID codec.UUID }                         // (1)
type FileWriteOp struct{ Path string; BlobHash codec.Hash }      // (2)
type SchemaMigrationOp struct{ MigrationID codec.UUID; MigrationPayload string } // (3)
type AttachmentPutOp struct{ DocID, AttachmentID codec.UUID; Name string; Scheme byte
  ContentHash codec.Hash; Size int64; Mime string }              // (4)
type AttachmentDeleteOp struct{ DocID, AttachmentID codec.UUID } // (5)
func OpDocID(op Op) (codec.UUID, bool); func OpFromValue(codec.Value) (Op, error)

type Transaction struct {
  ID codec.UUID; BaseVersion codec.Hash; Operations []Op; Timestamp codec.Timestamp
  AuthorNodeID codec.UUID; ResultVersion *codec.Hash; Preconditions []Precondition
  Attachments *AttachmentTable }                                 // nil: DAG derives it from parent + ops
type Precondition struct{ OpIndex int; Kind PreconditionKind; ContentHash codec.Hash }
// PreconditionKind: ExpectAny | ExpectAbsent | ExpectPresent | ExpectContentHash   (compare-and-set)

type Commit struct {
  Hash codec.Hash; ParentHashes []codec.Hash; NamespaceID string; TransactionID codec.UUID
  Timestamp codec.Timestamp; AuthorNodeID codec.UUID; Operations []Op
  DocumentTreeHash codec.Hash; SchemaHash *codec.Hash; Message string
  AttachmentTreeHash *codec.Hash }                               // field 10, omitted when nil
func BuildCommit(parents, ns, txID, ts, author, ops, treeHash, schemaHash, msg) (Commit, error)
func BuildCommitWithAttachments(..., msg, attachmentTreeHash *codec.Hash) (Commit, error)
func ComputeCommitHash(c Commit) (codec.Hash, error)
func FromPayloadBytes([]byte) (Commit, error);  (*Commit).ToPayloadBytes() / AppendPayloadBytes(dst) / PayloadSize()
func ScanCommitWrites(payload, docID, fn func(patch []byte) bool) error   // walk encoded writes in place
func ScanAllCommitWrites(payload, fn func(docID, patch []byte) bool) error

type DocumentTree struct{ TreeHash codec.Hash; Entries map[codec.UUID]codec.Hash; size int; trie *trieTree }
func (DocumentTree) With(docID, contentHash) (DocumentTree, error); Without(docID); WithAll([]TreeEntry)
func (DocumentTree) Size/Contains/HashFor/Walk/WalkOrdered/WalkAfter(after, fn)/MaterializedEntries/SlabBytes
func (DocumentTree) AppendPacked(dst) []byte; WritePacked(w); func BuildTrieDocumentTreePacked(packed) // 48 B/entry
func (DocumentTree) Node(prefix string, entryLimit int) (TreeNode, error)   // 16-ary Merkle node
func (DocumentTree) Proof(id codec.UUID) TreeProof; func VerifyTreeProof(root, p) (Hash, bool, error)
func DiffTrees(local, remote NodeSource) ([]TreeDifference, error)           // Merkle diff, used by repair

type TreeFileOptions struct{ Dir string; ResidentBytes int64; MaxSegments int; ReadOnly bool }
type TreeFile struct{ /* segment seq, keep set, background rewrites */ }   // one per namespace
func NewTreeFile(TreeFileOptions) (*TreeFile, error)       // ErrTreeFileUnsupported without mmap/LE
func OpenTreeDigest(digest, entries, f) (DocumentTree, bool); OpenTreePacked(packed, f); OpenTreeEntries(m, f)
func BindTreeFile(t, f) DocumentTree; SaveTreeSegment(t, packed); SaveTreeSegmentDigest(t, digest)
func (DocumentTree) Paged() (bound, mapped bool); Rebased(); DropMappedPages(); func RebaseTrees([]DocumentTree)

type AttachmentRef struct{ Scheme byte; ContentHash codec.Hash; Size int64; Mime string } // Hash(): 0x41-domain SHA-256
type Attachment struct{ ID codec.UUID; Name string; AttachmentRef }
type AttachmentSet []Attachment                                // one document's, sorted by ID; Hash(), ByName
type AttachmentTable struct{ tree DocumentTree; sets *AttachmentSetPool; total int } // docID -> set hash
func (AttachmentTable) Root() *codec.Hash; List/Count/Get/GetByName(docID, ...); Put/Delete/DeleteAll -> AttachmentTable
func (AttachmentTable) Apply(ops []Op) (AttachmentTable, error); Walk; WalkDocuments; InPool(p)
func AppendPackedAttachmentTables(dst, tables) ([]byte, error); ParsePackedAttachmentTables[Into](pool, packed)
type AttachmentSetStore interface{ WriteAttachmentSet(h, encoded) error; ReadAttachmentSet(h) ([]byte, bool, error) }
type AttachmentSetPool struct{ /* LRU of sets by hash, budget, spill to store */ }
func NewAttachmentSetPool(store AttachmentSetStore, budgetBytes int64) *AttachmentSetPool // DefaultAttachmentSetBudget 8 MiB
// content identity (scheme 1, AttachmentHashSchemeChunked1): RFC 6962 Merkle over 1 MiB chunks
func ChunkLeafHash(chunk) Hash; MerkleTreeHash(leaves) Hash; AttachmentIdentity(tree, size) Hash
func AttachmentContentHash(data) Hash; MerkleInclusionProof(leaves, i); VerifyMerkleInclusion(...)
type MerkleBuilder struct{...}  // streaming Add(leaf)/Root(); type MerkleTree struct{...} // Proof(i)

type Branch struct{ Name, NamespaceID string; HeadHash codec.Hash; CreatedAt, UpdatedAt codec.Timestamp }
type Tag struct{ Name, NamespaceID string; CommitHash codec.Hash; CreatedAt codec.Timestamp; Message string }
type CommitStub struct{ OriginalHash codec.Hash; ArchiveLocation string; StubbedAt codec.Timestamp }
```

Key design points:

* `Commit.Hash` is the hash of the payload bytes, so a commit is self-verifying; parents form the
  Merkle DAG. A **merge commit has two parents**; `Operations` on a commit are *not* invertible
  (no pre-images), so history cannot be folded backward. Since #281 a logged `WriteOp` is the whole
  stored document (staging resolves merge patches and `Replace` alike), which is why merges go
  through `RebasedOnto`. `AttachmentTreeHash` and `SchemaHash` are omitted from the payload when
  nil, so a commit without attachments keeps the hash it always had (and matches Kotlin).
* `commit_direct.go` is a direct `CommitPayload` codec straight from/to `Commit` fields (one exact
  allocation per encode, none to hash); it accepts only the canonical layout and falls back to the
  generic `codec.Value` path for anything else. `ScanCommitWrites` reads patches in place from an
  encoded commit without decoding it.
* `DocumentTree` is value-typed and **persistent**: `With`/`Without` return a new tree sharing
  structure. The trie is 16-ary over the UUID's 32 nibbles with **leaf compression** (a leaf stands
  for any single-entry subtree; hashes are unchanged), and its nodes live in a **pointer-free slab**
  (`document_tree_slab.go`): fixed-size `nodeRec` (96 B) / `leafRec` (80 B) records addressed by
  `uint32` `tref`, reference-counted, freed by a runtime cleanup on the per-version `*trieTree`
  handle, so the GC never traces the tree (the trie costs ~120 B/doc per the `BuildTrieDocumentTree`
  comment, down from ~4.9 KB before leaf compression). `Entries` is nil for any trie-backed tree; it
  is populated only for trees built from a flat map by `BuildDocumentTree` (`BuildTrieDocumentTree`
  drops the map; `OpenTreeEntries` falls back to it when no tree file is bound).
  Always go through `Size/Contains/HashFor/Walk/MaterializedEntries`, never read `Entries` directly.
* A slab bound to a `TreeFile` keeps two tiers: an immutable mapped **tree segment**
  (`ns/<ns>/tree/tree-<seq>.kdbt`, `refFile` references) plus a resident overlay; past
  `ResidentBytes` the tree is rewritten whole in the background and later updates `Rebase` onto
  the new segment. Segments are a cache, never an authority: a checkpoint still carries the packed
  tree, and a segment tagged with the packed image's digest (`.src` sidecar) is mapped on open
  instead of rebuilt (`OpenTreeDigest`).
* `AttachmentTable` is itself a `DocumentTree` (docID -> `AttachmentSet.Hash`) over a shared
  `AttachmentSetPool`; its `Root()` is the commit's `AttachmentTreeHash`. Attachments are not part
  of the document's content hash. An attachment's `ContentHash` is the Merkle identity
  `SHA-256(0x02 || be64(size) || MTH(chunk leaves))`, so chunks are verifiable on their own
  (range reads, resumed transfers).
* `TreeNode`/`TreeProof` give a 16-ary Merkle view over the same trie, which is what peer-sync
  repair (`peersync.RepairSession`) uses to find divergent documents without shipping the tree.
* `document.WireRegistry()` registers the codec record schemas for all of the above.

#### 2.2.2 `schema`

**Purpose.** User-facing document schemas (field types, required/indexed/unique), versioned
migrations, validation.

```go
type KdbSchema struct {
  SchemaHash codec.Hash; Fields []Field; Version int; CreatedAt codec.Timestamp
  Description string; UniqueConstraints []UniqueConstraint }
type Field struct{ Name string; Type FieldType; Required, Indexed, Unique bool }
type FieldType interface{ isFieldType(); SQLTypeName() string; CodecTypeLabel() string }
// StringType Int32Type Int64Type Float64Type BoolType TimestampType UUIDType ObjectType ArrayType EnumType{Values}
type UniqueConstraint struct{ Fields []string }   // (KdbSchema).UniqueTuples() [][]string
func None() KdbSchema                      // "no schema"; (KdbSchema).IsNone()
func (KdbSchema) IsSchemaless() bool       // IsNone() or no declared fields (constraints-only schema)
func (KdbSchema) WithUniqueTuple(fields []string, createdAt) (KdbSchema, error)   // version+1, idempotent
func Build(fields, version, createdAt, desc) (KdbSchema, error)
func BuildWithConstraints(fields, constraints, version, createdAt, desc) (KdbSchema, error)
func Validate(doc document.Document, sch KdbSchema) kdberr.Result[document.Document]
func ApplyMigration(current KdbSchema, m SchemaMigration) kdberr.Result[KdbSchema]
func DiffSchemas(from, to KdbSchema) Diff;  func IsBackwardCompatible(current, m) bool

type SchemaMigration struct{ MigrationID codec.UUID; FromVersion, ToVersion int; Steps []MigrationStep; Description string }
type MigrationStep interface{ isMigrationStep() }   // sealed: AddFieldStep DropFieldStep RenameFieldStep
// ChangeTypeStep AddIndexStep DropIndexStep SetRequiredStep SetUniqueStep WidenEnumStep NarrowEnumStep
func Migrate(base KdbSchema, fn func(*MigrationBuilder)) (SchemaMigration, error)

func FieldValue(docJSON, fieldName string) (any, error)          // decodes the whole body
func FieldRaw(docJSON, fieldPath string) (raw string, found bool)  // byte scan, no decode
type TopLevelMember struct{ Key string; Start, End int }; func TopLevelMembers(docJSON) ([]TopLevelMember, bool)
```

A schema is itself content-addressed (`SchemaHash`); commits point at it via `Commit.SchemaHash`,
and a `SchemaMigrationOp` inside a transaction is how a schema change enters history. A
**constraints-only schema** (no fields, only `UniqueConstraints` over JSON paths, built by
`WithUniqueTuple` on a schemaless namespace) is a real schema for the commit path (`IsNone` is
false: it is recorded, replicated and enforced) but schemaless for query planning, DML and
validation (`IsSchemaless`). `FieldRaw`/`TopLevelMembers` (`json_lookup.go`) let unique-key
extraction scan a body once instead of decoding it per constraint.
`schema.FieldType.SQLTypeName()` feeds `sql.ColumnDefinition`; `index.InferIndexType(ft)` and
`index.DescriptorsFromSchema` derive index descriptors from `Indexed`/`Unique` fields.

#### 2.2.3 `dag`

**Purpose.** The per-namespace commit graph, branches/tags, traversal, diff, squash, retention
pins and history navigation. One concrete implementation.

```go
type CommitDAG interface {                // narrow port used by runtime wrappers
  GetCommit(hash) (document.Commit, bool);  GetCommitOrThrow(hash) (document.Commit, error)
  PutCommit(commit document.Commit, requireParents bool) error
  GetDocumentTree(h) (document.DocumentTree, bool); GetDocumentTreeOrThrow(h) (...)
  PutDocumentTree(tree document.DocumentTree)
  AppendCommit(tx document.Transaction, parent codec.Hash, newTree document.DocumentTree,
               schemaHash *codec.Hash, message string) (document.Commit, error)
  Head() (codec.Hash, error); HeadCommit() (codec.Hash, document.Commit, bool, error)
  SetHead(branch string, hash codec.Hash) error
  GetBranch(name) (document.Branch, bool); ListBranches() []document.Branch
  CreateBranch(name string, from codec.Hash) (document.Branch, error)
  Walk(from codec.Hash, until *codec.Hash, limit int) []TraversalEntry }

type HistoryNavigator interface {         // ref/revision resolution, tags, listing
  ResolveRef(CommitRef) (codec.Hash, error); ResolveRevision(spec string) (codec.Hash, error)
  NthAncestor(from, n) ; CommitAtOrBefore(from, ts) ; ListCommits(from, skip, limit) ([]CommitInfo, error)
  CreateTag/GetTag/ListTags/DeleteTag ... }

type DocumentTreeStore interface{ GetTree(hash) (document.DocumentTree, bool); PutTree(tree) }
type CommitOperationsLoader func(hash codec.Hash) ([]document.Op, error)
type CommitPayloadLoader func(hash codec.Hash) ([]byte, bool, error)   // SetPayloadLoader, LoggedPayload
type InMemoryCommitDag struct{ NamespaceID string; /* unexported: head atomic.Pointer[headSnapshot]
   (cache-line padded), mu sync.RWMutex, nodes nodeStore, treeIndex, stubs, shallow, trees, treeStore,
   branches, tags, txIndex, pins, attachment tables + pool, graph file ... */ }
type GraphSettings struct{ AncestryPruning, FileEnabled bool; RebuildCommits, MaxSegments, ChunkBytes int
   Memory GraphMemory /* "kdb" (default) | "go" */; AnchorInterval int }
type GraphSegmentRef struct{ Seq uint64; CRC uint32; Records int }
type GraphFileStatus struct{ Segments, FileCommits, HiddenRecords, ResidentCommits int; FileBytes int64 }
type CheckpointState struct{ Commits []CheckpointCommit; PackedCommits []byte; Branches []document.Branch; Tags []document.Tag }
type MergeBaseFinder struct{ /* reusable paint-down state */ }   // (d).NewMergeBaseFinder().MergeBases(as, bs)
```

`InMemoryCommitDag` is the only implementation of `CommitDAG` besides the decorator
`embed.PersistingCommitDAG`. Its extra (non-interface) surface is what the rest of the code
actually depends on, grouped by file:

| File | Capability | Representative methods |
|---|---|---|
| `in_memory_commit_dag.go` | storage of commits/stubs/shallow roots/trees/branches | `StubCommit`, `PutShallowCommit`, `DropShallowCommit`, `MarkShallow`, `Unshallow`, `Horizon`, `CommitForTree`, `Squash`, `AppendCommitDetached`, `LookupHashPrefix`, `SetTreeStore`, `DiffTrees` (func) |
| `node_store.go`, `graph_record.go` | GC-free commit storage | unexported `nodeStore`: fixed-width commit records in raw byte chunks indexed by a pointer-free `hash -> slot` map (resident tier) plus mapped graph segments (segment tier); a commit lives in exactly one tier; segment records can be *hidden*. `GraphMemoryGo` keeps the old `map` of `document.Commit` (`goNode`). |
| `graph_file.go`, `graph_attach.go`, `graph_checkpoint.go` | on-disk commit graph segments (mapped, `storage/mmapfile`) | `ConfigureGraphFile(dir, readOnly)`, `OpenGraphFile`, `MaybeRebuildGraphFile`, `CompactGraphFile`/`RebuildGraphFile`, `CloseGraphFile`, `GraphFileStats`; `CheckpointSnapshotWithGraph(keep)` returns `GraphSegmentRef`s instead of the segment-resident commits, `AttachCheckpointGraph(refs, hidden)`, `SetGraphCheckpointRefs`, `DetachGraph` |
| `ancestry.go`, `merge_bases.go`, `commits_above.go` | graph algorithms | `IsAncestor`, `AncestorTest(base)`, `CommonAncestor`, `CommitsSince`, `AncestorSet[Of]`, `AppendMergeCommit`, `AppendMergeCommitOnto(expectedHead, ...)`; `MergeBases(as, bs)` (all nearest common ancestors, git paint-down), `NewMergeBaseFinder`, `MergeBaseSteps`; `CommitsAbove(heads, stops)`, `HistoryFloor` |
| `join.go` | content-free peer-sync merges | `IsContentFreeMerge`, `Frontier`/`FrontierOf`, `MaximalOf`, `Incorporates`, `SupersedesHead`, `AppendCommitOnto`, `SortHashes`; `MergeAuthorNodeID`, `MergeMessage` |
| `generations.go`, `graph_settings.go`, `memory_accounting.go` | generation numbers for pruned ancestry, graph settings, resident size | `GraphSettings{...}`, `SetGraphSettings`, `GraphFileActive`, `ParseGraphMemory`, `GenerationDistance`, `InvalidateGenerations`, `GraphResidentBytes` |
| `ops_retention.go` | bounded resident operations | `SetOperationsLoader(loader, budgetBytes)`, `SetPayloadLoader`, `LoggedPayload`, `CommitOperations(hash)`, `WalkWithOperations`, `SetOperationsBudget`, `OperationsResidentBytes` |
| `unlogged.go` | peer commits stored but not yet in the delta log | `PutUnloggedCommit`, `PutUnloggedShallowCommit`, `MarkUnlogged`/`MarkLogged`, `IsUnlogged`, `OperationsEvicted`, `RefillUnloggedCommit`; `ErrOperationsNotLogged` |
| `attachments.go`, `attachment_merge.go` | per-commit attachment tables | `AttachmentTableAt(commit)`, `SetAttachmentSetStore(store, budget)`, `AttachmentSetPool`, `HasAttachments`, `SeedAttachmentTable`, `PackAttachmentTables`/`SeedPackedAttachmentTables` (checkpoint section), `ForgetAttachmentTablesExcept`, `PinnedAttachmentTables`; every append (incl. merges) derives the table from the first parent + ops, and a merge gets three-way attachment ops (`mergeAttachmentOpsLocked`) so the same parents give the same commit hash on any node |
| `retention.go` | reader pins | `Pin(hash) (release func())`, `IsPinned`, `PinnedCount` |
| `provisional.go` | provisional commits (peer-sync hold) | `AppendCommitWith(..., AppendOptions{Detached, Provisional})`, `SettleProvisional` |
| `checkpoint.go`, `checkpoint_packed.go` | snapshot/restore for fast open | `CheckpointSnapshot[Retaining\|Logged]` (`Logged` refuses with `ErrHeadNotLogged`), `RestoreCheckpoint` (takes `Commits` and/or `PackedCommits`), `RestoreCommitOperations`, `HydrateBranchHeads`; `PackCheckpointCommits`, `ForEachPackedCommit`, `PackedCommitsHaveAttachments` |
| `history_nav.go` | `HistoryNavigator` impl | `ResolveRevision`, `CreateTag`, ... ; `ParseRevision(spec)` |
| `types.go` | value types | `TraversalEntry` (sealed: `FullEntry`, `StubbedEntry`), `CommitDiff` + `DiffEntry` (sealed: `DiffAdded/Removed/Modified`), `CommitRef` (sealed: `RefByHash/Branch/Tag/Time`) |

**Ownership and lifecycle.** One `InMemoryCommitDag` per namespace, created in
`Host.openNamespace`, given its graph directory (`ConfigureGraphFile`) and graph settings,
populated by `restoreNamespace` (checkpoint load, attaching the checkpoint's graph segments when it
names them, then delta-log replay), then wrapped by `PersistingCommitDAG`. Its concurrency design is unusual and load-bearing: reads
resolve the head through `head atomic.Pointer[headSnapshot]` padded off the `mu sync.RWMutex` cache
line, and **every mutator must call `publishHeadLocked` before unlocking**.

Errors (`dag/errors.go`): `ConsistencyError`, `BranchNotFoundError`, `TagNotFoundError`,
`CompactionSafetyError`, `HeadConflictError{Namespace, Branch, Expected, Actual}` (the
compare-and-swap failure of `AppendMergeCommitOnto`), `RevisionNotFoundError`. These implement
`Code()` directly rather than embedding `kdberr.base`. Sentinels: `ErrHeadNotLogged`
(`checkpoint.go`), `ErrOperationsNotLogged` (`unlogged.go`).

![Diagram 4](diagrams/class-04.svg)

<details><summary>Mermaid source</summary>

```mermaid
classDiagram
  class CommitDAG {
    <<interface>>
    +GetCommit(hash) Commit,bool
    +PutCommit(commit, requireParents) error
    +AppendCommit(tx, parent, tree, schemaHash, msg) Commit
    +Head() Hash
    +SetHead(branch, hash) error
    +Walk(from, until, limit) TraversalEntry[]
  }
  class HistoryNavigator {
    <<interface>>
    +ResolveRevision(spec) Hash
    +ListCommits(from, skip, limit) CommitInfo[]
    +CreateTag(name, hash, msg) Tag
  }
  class DocumentTreeStore {
    <<interface>>
    +GetTree(hash) DocumentTree,bool
    +PutTree(tree)
  }
  class InMemoryCommitDag {
    +NamespaceID string
    -nodes nodeStore
    -head atomic.Pointer
    -mu RWMutex
    +IsAncestor(a, d) bool
    +CommonAncestor(a, b) Hash
    +MergeBases(as, bs) Hash[]
    +Squash(hashes, boundary, tree, schema, msg) Commit
    +Pin(hash) release
    +CheckpointSnapshotWithGraph(keep) CheckpointState,GraphSegmentRef[]
    +AttachmentTableAt(commit) AttachmentTable
    +OpenGraphFile() error
    +PutUnloggedCommit(commit) error
  }
  class nodeStore {
    -index map~Hash,uint32~
    -chunks resident records
    -segments mapped graph files
  }
  class GraphSettings {
    +AncestryPruning bool
    +FileEnabled bool
    +RebuildCommits int
    +MaxSegments int
    +ChunkBytes int
    +Memory GraphMemory
    +AnchorInterval int
  }
  class AttachmentSetPool {
    +Empty() AttachmentTable
    +Stats() AttachmentSetPoolStats
    +Contains(hash) bool
  }
  class AttachmentSetStore {
    <<interface>>
    +WriteAttachmentSet(hash, encoded) error
    +ReadAttachmentSet(hash) bytes,bool
  }
  class PersistingCommitDAG {
    -delegate InMemoryCommitDag
    +Persist(commit) error
    +PersistAsync(commit) wait
  }
  CommitDAG <|.. InMemoryCommitDag
  CommitDAG <|.. PersistingCommitDAG
  HistoryNavigator <|.. InMemoryCommitDag
  HistoryNavigator <|.. PersistingCommitDAG
  PersistingCommitDAG *-- InMemoryCommitDag
  InMemoryCommitDag o-- DocumentTreeStore : SetTreeStore
  InMemoryCommitDag *-- nodeStore : nodes
  InMemoryCommitDag ..> GraphSettings : SetGraphSettings
  InMemoryCommitDag *-- AttachmentSetPool
  AttachmentSetPool o-- AttachmentSetStore : spill
  InMemoryCommitDag ..> MergeBaseFinder : creates
```

</details>

### 2.3 Write path, indexing, policy: `transaction`, `index/*`, `policy`, `script`, `compaction`

#### 2.3.1 `transaction`

**Purpose.** Turns a `document.Transaction` into a commit: schema validation, optimistic conflict
detection against the target head, unique-key enforcement, compare-and-set preconditions, merge
commits, replay of remote transactions, attachment-op preflight and conflicts, and document-level
leases.

```go
type Engine interface {
  ConflictPolicy() ConflictPolicy
  CustomResolver() ConflictResolver
  Commit(tx document.Transaction, d *dag.InMemoryCommitDag, store storage.Adapter, sch schema.KdbSchema,
         targetHead *codec.Hash, message string) (TransactionResult, error)
  Replay(tx document.Transaction, d *dag.InMemoryCommitDag, store storage.Adapter, sch schema.KdbSchema,
         replayTarget codec.Hash, message string) (TransactionResult, error)
  Merge(primaryHead, mergedHead codec.Hash, d *dag.InMemoryCommitDag, store storage.Adapter,
        sch schema.KdbSchema, message string) (TransactionResult, error)
  Validate(tx document.Transaction, d *dag.InMemoryCommitDag, store storage.Adapter,
           sch schema.KdbSchema) ([]OperationViolation, error)
}
func NewEngine(policy ConflictPolicy, resolver ConflictResolver) Engine
func NewEngineWithOptions(policy ConflictPolicy, resolver ConflictResolver, opts EngineOptions) Engine
type EngineOptions struct{ UniqueKeys *UniqueKeyRegistry; Preconditions bool }

type Preparer interface {  // optional split of Commit into check phase and write phase
  PrepareCommit(tx, d, store, sch) (*PreparedCommit, TransactionResult, error) }
type PreparedCommit struct{ /* unexported */ }   // Namespace() Anchor() Apply(ApplyOptions{Message,Provisional}) Discard()

type ConflictPolicy int   // ConflictPolicyAppendOnly | LastWrite | Strict | Custom
type ConflictResolver interface{ Resolve(c DocumentConflict) (*document.Document, error) }
type TransactionResult interface{ isTransactionResult() }  // sealed sum type
// ResultSuccess{Commit, NewTreeHash} | ResultConflict{Report, ConflictingOps}
// | ResultSchemaError{Violations []OperationViolation} | ResultAborted{Cause error}

type BlobPresence interface{ HasBlob(contentHash codec.Hash) (bool, error) }            // optional on storage.Adapter
type AttachmentBlobSource interface{ HasAttachmentBlob(ns string, contentHash codec.Hash) (bool, error) }
func RegisterAttachmentBlobs[T any, P interface{ *T; AttachmentBlobSource }](store storage.Adapter, owner P)
```

The single implementation is the unexported `defaultEngine` (`default_engine.go`, `prepare.go`).
`Commit` is literally `PrepareCommit` followed by `Apply`. `Preparer` is deliberately an *optional*
interface (callers do `engine.(transaction.Preparer)`), so wrapping engines elsewhere keep compiling;
it exists so a cross-namespace transaction can check every participant before writing any.

How the engines are instantiated by `server.NewKdbServerRuntime`:

* `TransactionEngine = NewEngineWithOptions(ConflictPolicyStrict, nil, {UniqueKeys, Preconditions:true})`
  used for client transactions (stale base -> `ResultConflict`).
* `UpsertEngine = NewEngineWithOptions(ConflictPolicyLastWrite, ...)` used by `Upsert`/`PutJSON`.
* Peer-sync merges use `ResolutionOptions.Policy` (`KdbServerRuntime.PeerSyncConflictPolicy`) and
  `ResolutionChain`, not these engines.

Supporting types:

| Type | Role |
|---|---|
| `Builder{NamespaceID, BaseVersion, AuthorNodeID, Schema}` | Fluent transaction assembler (own `sync.Mutex`): `Write`, `Replace` (`WriteOp{Replace:true}`: whole-document write), `WriteDocument`, `Delete`, `FileWrite`, `Attach(docID, attachmentID, name, contentHash, size, mime)`, `Detach(docID, attachmentID)`, `Append(op)`, `SchemaMigration`, `Build()`. `NewBuilder(ns, dag, author, schema)` pins `BaseVersion` to the current head. A session holds one (`KdbSession.Pending`). |
| `UniqueKeyRegistry` | In-memory map `UniqueKey{NamespaceID, Fields, Value} -> owner DocID`; `Apply`, `Owner`, `Rebuild`, `RebuildLenient`, `Reset`. Rebuilt from head on open (`KdbServerRuntime.RebuildUniqueKeys`; failure is parked in `UniqueKeyRebuildError`). `UniqueKeysFor(ns, sch, doc)` computes the keys: arrays are **multikey** (each element is claimed; a compound tuple claims every combination), and fields are read by one top-level byte scan (`fieldReader` over `schema.TopLevelMembers`) instead of a decode per constraint; violation type `UniqueConstraintError`. |
| `LockManager` | Two lock families behind one mutex: transaction-scoped locks (`TryAcquire`, `AcquireAllForTransaction`, `ReleaseAll`, `AssertHeld`, `AssertUnheldByOthers`) and expiring **leases** with monotonically increasing fence tokens (`TryAcquireLease`, `Renew`, `ReleaseLeases`, `ValidateFences`, `Sweep`, `Held`). `Lease{NamespaceID, DocID, SessionID, Fence, ExpiresAt, GrantedNow}`. Clock is injectable (`NewLockManagerWithClock`). |
| `DocumentConflict`, `ConflictOrigin{NodeID, Commit, TimestampMicros}` | Input to a `ConflictResolver`; origin metadata is JSON-marshalled for conflict records. |
| `OperationConflict`, `OperationViolation` | Per-op detail inside `ResultConflict` / `ResultSchemaError`. |
| `EncodeMigration/DecodeMigration` | Schema migration <-> `SchemaMigrationOp.MigrationPayload`. |
| `BaseNotFoundError`, `MergeBaseNotFoundError` | Embed `*kdberr.VersionNotFoundError`. |
| `branchDocuments` (unexported, `default_engine.go`) | Merge step: rebases each of the merged branch's whole-document writes into a field-level change onto the primary's document (`Document.RebasedOnto`) and emits it as a replace write. Kotlin: `BranchDocuments`. |
| attachment rules (`attachments.go`) | `preflightAttachments` (ID, valid unique name, blob present via `AttachmentBlobSource` then `BlobPresence` then `ReadBlob`, document exists after the tx) returns `ResultSchemaError`; `detectAttachmentConflicts` conflicts a change to one attachment ID with a concurrent change to the same ID or a concurrent document delete; JSON-only writes never conflict with attachment changes. `RegisterAttachmentBlobs` holds the owner weakly in a `sync.Map` keyed by adapter address. |

**Concurrency.** `defaultEngine` is stateless apart from the injected `UniqueKeys` registry; callers
must serialise `Commit` per namespace (the server does with `writeGate`, the driver with
`namespace.write sync.Mutex`). `LockManager` and `UniqueKeyRegistry` have their own mutexes.
`transaction` starts no goroutines; its only primitives are the `sync.Mutex`es in `Builder`, `LockManager` and `UniqueKeyRegistry`, plus the `attachmentBlobs sync.Map`.

![Diagram 5](diagrams/class-05.svg)

<details><summary>Mermaid source</summary>

```mermaid
classDiagram
  class Engine {
    <<interface>>
    +Commit(tx, dag, store, schema, head, msg) TransactionResult
    +Replay(tx, dag, store, schema, target, msg) TransactionResult
    +Merge(primary, merged, dag, store, schema, msg) TransactionResult
    +Validate(tx, dag, store, schema) OperationViolation[]
  }
  class Preparer {
    <<interface>>
    +PrepareCommit(tx, dag, store, schema) PreparedCommit,TransactionResult
  }
  class PreparedCommit {
    +Namespace() string
    +Anchor() Hash
    +Apply(ApplyOptions) TransactionResult
    +Discard()
  }
  class ConflictResolver {
    <<interface>>
    +Resolve(DocumentConflict) Document
  }
  class Builder {
    +Write(docID, patch)
    +Replace(docID, body)
    +Delete(docID)
    +Attach(docID, attID, name, hash, size, mime)
    +Detach(docID, attID)
    +Build() Transaction
  }
  class AttachmentBlobSource {
    <<interface>>
    +HasAttachmentBlob(ns, hash) bool
  }
  class TransactionResult {
    <<sealed>>
  }
  Engine <|.. defaultEngine
  Preparer <|.. defaultEngine
  defaultEngine o-- ConflictResolver
  defaultEngine o-- UniqueKeyRegistry
  defaultEngine ..> PreparedCommit : creates
  defaultEngine ..> TransactionResult : returns
  ResultSuccess --|> TransactionResult
  ResultConflict --|> TransactionResult
  ResultSchemaError --|> TransactionResult
  ResultAborted --|> TransactionResult
  ConflictResolver <|.. ScriptConflictResolver : script.ConflictResolver
  defaultEngine ..> AttachmentBlobSource : preflight
  AttachmentBlobSource <|.. EmbeddedKdbRuntime : RegisterAttachmentBlobs
```

</details>

#### 2.3.2 `index` and sub-packages

**Purpose.** Secondary indexes that are *versioned*: every lookup can be answered "as of commit X".
Four index types, one store port, one per-namespace registry that fans a commit out to all stores.

```go
type IndexType int  // IndexTypeHash | BTree | FullText | Vector
type Descriptor struct { IndexID codec.UUID; NamespaceID, FieldName string; Fields []string; Type IndexType
  Unique bool; SchemaVersion int; CreatedAtHash codec.Hash; Options map[string]string }
type Key interface{ isKey() }   // NullKey BoolKey Int32Key Int64Key Float64Key TimestampKey StringKey UUIDKey VectorKey CompositeKey
type Store interface {
  Descriptor() Descriptor
  Put(entry Entry) error;  Delete(docID codec.UUID, atCommit codec.Hash) error;  BulkLoad([]Entry) error
  Lookup(key Key, atCommit *codec.Hash) ([]codec.UUID, error)
  Range(from, to Key, atCommit *codec.Hash, limit int, ascending bool) ([]codec.UUID, error)
  Search(query string, atCommit *codec.Hash, limit int) ([]RankedResult, error)          // full-text
  NearestNeighbours(q []float32, k int, atCommit *codec.Hash) ([]RankedResult, error)    // vector
  Rebuild([]Entry) error;  Clear() error;  IsValid(atCommit codec.Hash) (bool, error)
  Snapshot() ([]byte, error);  RestoreSnapshot([]byte) error }
type DocumentStore interface { Store
  PrepareDocument(docID codec.UUID, jsonText string) (PreparedPut, error)   // validate, no mutation
  PutDocument(docID codec.UUID, commitHash codec.Hash, jsonText string) error }
type PreparedPut interface{ Apply(commitHash codec.Hash) (Hint, error) }
type ParsedDocumentStore interface{ PrepareParsedDocument(docID, jsonText string, root json.Value) (PreparedPut, error) }
type StoreFactory interface{ Create(descriptor Descriptor) (Store, error) }   // StoreFactoryFunc adapter

// bounded as-of history, shared by the keyed, full-text and vector stores
type Lineage struct{ /* dag, cutoff, steps []Hash, stepBase, stepIdx, pending, fresh */ }
func NewLineage(d *dag.InMemoryCommitDag) *Lineage
func (*Lineage) Start(target); Advance(target) bool; Classify(commit) (in bool, cstep uint32, exact bool)
func (*Lineage) Locate(x codec.Hash, complete bool) (Placement, bool); TrimSteps(keepFrom, maxSteps)
func (Placement) Visible(commit, step, cstep, exact) int
var ErrHistoryUnavailable  // read behind the retained window: caller scans the documents instead
type MemoryOptions struct{ HistoryEvents int }   // 0 = DefaultHistoryEvents (1<<15); <0 = unbounded

// ordered reads for top-k ORDER BY ... LIMIT and COUNT (MemoryStore only)
type KeyBucket struct{ Key Key; DocIDs []codec.UUID }
func (*MemoryStore) KeyGroups(kind, after Key, atCommit *codec.Hash, maxIDs int) ([]KeyBucket, error)
func (*MemoryStore) CompositeGroups(prefix []Key, after Key, ascending bool, atCommit *codec.Hash, maxIDs int) ([]KeyBucket, error)
func (*MemoryStore) LookupCount(key, atCommit) (int, error); Unfiled(ids, atCommit); KeysOnlyOfTypes(kinds, atCommit)
var ErrKeyGroupsUnsupported   // key-group read that would need undo: caller reads documents
```

Implementations:

| Type | Package | Index types | Notes |
|---|---|---|---|
| `MemoryStore` | `index` | Hash, BTree (single-field and composite) | One implementation for both, over an unexported `eventLog` (`event_log.go`): the authoritative structure is the **head state** (interned keys in a pointer-free `keyTable`, order-preserving key encoding `keyenc.go`, postings per key), advanced in place; only the last `MemoryOptions.HistoryEvents` events are kept, each with its recorded effect, and an as-of read undoes the retained events the cutoff must not see (`Lineage`). Outside the window: `ErrHistoryUnavailable`; a head state that cannot follow the graph: `NeedsRebuild`. `HistoryBytes`, `TrimHistoryOutside`, `ForgetHistory`. Snapshots are binary format 2 (`snapshot_binary.go`; complete log or head state), text format 1 still read. `VersionedEngine` is now a thin wrapper over the same `eventLog`. |
| `fulltext.Store` | `index/fulltext` | FullText | BM25 (`K1`, `B`), `analyzer.Analyze` (tokeniser + Porter stemmer + stopwords), phrase queries via `ParseQuery`, `SaveToDir/LoadFromDir`, `Stats`. Head view + bounded version history via `index.Lineage` (`Options.HistoryEvents`). |
| `vector.Store` | `index/vector` | Vector | HNSW (`DefaultM`, `DefaultEfConstruction`, `DefaultEfSearch`) with exact fallback below `Options.ExactThreshold`; metrics `Cosine`, `L2`, `InnerProduct`. Bounded history via `index.Lineage` (`Options.HistoryEvents`); `LiveCount`, `ExactNearestNeighbours`. |
| `stores.NewFactory(d, Options{Keyed, FullText, Vector})` | `index/stores` | all | `StoreFactory` that switches on `Descriptor.Type`; `stores.Open(dir, ns, d, opts)` = `index.LoadRegistry` + factory; `OpenWithLoadOptions(..., LoadOptions{KeepBehind})`. |
| `fusion.Fuse(arms, mode, limit)` | `index/fusion` | n/a | Reciprocal-rank-fusion (`ModeRRF`, `DefaultRRFK`) or weighted sum over `Arm{Results, Weight, Depth, MinScore}`. Used by the hybrid search in `server/search.go`. |

`Registry` (`index/registry.go`) is the fan-out hub: `entries []*registryEntry` guarded by
`mu sync.RWMutex`; `Add/Remove/Descriptors/ByName/ByField/Resolve/ByID`, two-phase writes
`PrepareWrite(docID, json) (*PreparedWrite, error)` / `PrepareDelete` -> `ApplyWrite/ApplyDelete`
(`(*PreparedWrite).Commit(hash)` returns `[]Hint` for replication; the document is parsed once
for every store that is a `ParsedDocumentStore`), `ApplyHints` (replicated hints from peers,
unknown indexes ignored), `MarkCommit(hash)`, `RebuildFromScan(scan, head, only)`,
`CatchUpStores(ids, head, changes)` (brings `Behind` stores forward from `DocumentChange`s),
`NeedingRebuild`, `CompositeBTrees`, `SaveAll`. **Shared history budget**: `SetHistoryBudget(fn)`
makes every store keep one common commit window (`HistoryWindow`) instead of one window per
index (the server passes `engine.MemoryBudgetBytes() * storage.DefaultIndexHistoryFraction`); `DeferHistoryBounding` + `BoundHistoryIfDue` move that check off
the write gate. Persistence: `Catalog` (`index-catalog.json`), `Manifest` + snapshot per index
(`SaveStoreSnapshot`, `LoadStoreSnapshot`, `InvalidateStoreSnapshot`; snapshot then manifest, with
a durable directory sync between), and an off-gate pair `CaptureSnapshots(saved gens)` ->
`SnapshotCapture{Head, Stores []CapturedStore}` -> `WriteSnapshots(dir, c)` that skips stores
whose generation (`Generations`) has not changed. `LoadRegistry[WithOptions](dir, ns, dag,
factory[, LoadOptions])` returns `LoadReport{CatalogFound, Fresh, Stale, Behind}`.
`DescriptorsFromSchema(ns, sch)` + `DerivedIndexID(ns, field, type)` give deterministic ids for
schema-declared indexes. `Descriptor` helpers (`options.go`): `IndexName`, `FieldPaths` (a
**composite BTREE** is a BTREE with two or more), `FirstField`, `FieldWeights`, `IntOption`,
`StringOption`, `WithOption`, `IsSystem` (`OptionSystem`: indexes the server keeps for itself,
such as the expiry index in `server/expiry_index.go`; never returned by `ByField` or
`CompositeBTrees`). `paths.go`: `ParseDocument`, `SplitFieldPath`, `PathValues[Raw]`,
`FieldStrings`, `FieldVector`, `KeyFromJSONValue`.

Index errors embed kdberr bases: `NotFoundError` and `TypeMismatchError` embed `*kdberr.SchemaError`;
`UniqueViolationError` and `DimensionMismatchError` embed `*kdberr.SchemaViolationError`.

The SQL layer sees indexes only through `sql.IndexProvider`; the server's adapter is
`server.RegistryIndexProvider` (also implements `sql.IndexDDLExecutor` for `CREATE/DROP INDEX`).

![Diagram 6](diagrams/class-06.svg)

<details><summary>Mermaid source</summary>

```mermaid
classDiagram
  class Store {
    <<interface>>
    +Lookup(key, atCommit) UUID[]
    +Range(from, to, atCommit, limit, asc) UUID[]
    +Search(q, atCommit, limit) RankedResult[]
    +NearestNeighbours(v, k, atCommit) RankedResult[]
    +Snapshot() bytes
  }
  class DocumentStore {
    <<interface>>
    +PrepareDocument(id, json) PreparedPut
    +PutDocument(id, commit, json)
  }
  class StoreFactory {
    <<interface>>
    +Create(Descriptor) Store
  }
  class ParsedDocumentStore {
    <<interface>>
    +PrepareParsedDocument(id, json, root) PreparedPut
  }
  class MemoryStore {
    -log eventLog
    +KeyGroups(kind, after, atCommit, maxIDs) KeyBucket[]
    +CompositeGroups(prefix, after, asc, atCommit, maxIDs) KeyBucket[]
    +LookupCount(key, atCommit) int
    +NeedsRebuild() bool
    +TrimHistoryOutside(inWindow)
  }
  class Lineage {
    +Advance(target) bool
    +Classify(commit) in,cstep,exact
    +Locate(x, complete) Placement
    +TrimSteps(keepFrom, maxSteps)
  }
  class FullTextStore["fulltext.Store"]
  class VectorStore["vector.Store"]
  class Registry {
    +Add(desc)
    +Remove(id)
    +PrepareWrite(docID, json) PreparedWrite
    +ApplyHints(hints)
    +MarkCommit(hash)
    +SetHistoryBudget(fn)
    +BoundHistoryIfDue()
    +CatchUpStores(ids, head, changes) RebuildStats
    +CaptureSnapshots(saved) SnapshotCapture
    +SaveAll(dir)
  }
  class RegistryIndexProvider {
    +FullTextSearch(...)
    +VectorSearch(...)
    +ExactLookup(...)
    +CreateIndex(...)
  }
  class IndexProvider {
    <<interface>>
  }
  Store <|-- DocumentStore
  DocumentStore <|.. MemoryStore
  DocumentStore <|.. FullTextStore
  DocumentStore <|.. VectorStore
  ParsedDocumentStore <|.. MemoryStore
  ParsedDocumentStore <|.. FullTextStore
  ParsedDocumentStore <|.. VectorStore
  MemoryStore *-- Lineage : via eventLog
  FullTextStore *-- Lineage
  VectorStore *-- Lineage
  Lineage o-- InMemoryCommitDag
  Registry o-- StoreFactory
  Registry *-- DocumentStore : entries
  RegistryIndexProvider o-- Registry
  IndexProvider <|.. RegistryIndexProvider
  Registry ..> MemoryStore : shared history window
```

</details>

Note: `MemoryStore` and the two document stores assert `index.DocumentStore` only for fulltext and
vector (`var _ index.DocumentStore = (*Store)(nil)`); `MemoryStore` is accepted by the factory
through the same method set. Ordered key-group reads (`KeyGroups`, `CompositeGroups`) answer only
from a state with nothing to undo (head cutoff or a complete log) and otherwise return
`ErrKeyGroupsUnsupported`; they are what lets the SQL executor stop a BTREE `ORDER BY ... LIMIT k`
(including `WHERE prefix = ... ORDER BY last LIMIT k` on a composite) after about k documents.

#### 2.3.3 `policy`

**Purpose.** Declarative per-namespace policy (mode, history, conflict, compaction, tiers, vector
index parameters, document expiry) with a registry, parser, validator and presets.

```go
type NamespacePolicy struct { NamespaceID string; Schema *schema.KdbSchema; Mode NamespaceMode /*Mutable|AppendOnly*/
  History HistoryMode /*Full|None*/; Retain storage.RetentionWindow; Conflict transaction.ConflictPolicy
  Compaction CompactionPolicy; Tiers TierPolicy; IndexRetentionDefault storage.IndexRetention
  GpuPromotion *GpuPromotionPolicyRef; VectorIndex VectorIndexPolicy; DocumentExpiry *DocumentExpiryPolicy; Revision ... }
type Registry interface { Get(ns) (NamespacePolicy, error); GetOrNull(ns) (*NamespacePolicy, error)
  Put(NamespacePolicy) error; Delete(ns) (bool, error); List() ([]string, error) }
// InMemoryRegistry, StorageBackedRegistry(storage.Adapter, delegate Registry)
type Parser interface { Parse(source string) (NamespacePolicy, error); ParseJSON(json string, sch *schema.KdbSchema) (NamespacePolicy, error) }
type Validator interface{ Validate(NamespacePolicy) ValidationResult }
type Evaluator interface { BoundaryCandidates(policy CompactionPolicy, commitTimestamps map[Hash]Timestamp,
  tagged, branchHeads map[Hash]struct{}, head Hash, parentOf func(Hash) *Hash) []BoundaryPlan }   // impl: defaultEvaluator
func DefaultMutable(ns, sch) NamespacePolicy; AppendOnlyEvents(ns, sch); ScratchDocument(ns); CacheNoHistory(ns)
```

In the Go server the only live consumer is **document expiry** (`server/expiry.go` reads
`DocumentExpiryPolicy`; settable per runtime via `SetDocumentExpiry`; the sweep finds due documents
through a system BTREE expiry index, `server/expiry_index.go`) and the settings plumbing in
`service`. History retention in Go is driven by `storage.HistoryMode` / `storage.RetentionWindow`
(section 2.4), not by `policy.HistoryMode`. The two enums share names (`HistoryModeFull`,
`HistoryModeNone`) with different numeric meaning (`policy.HistoryMode.StorageMode()` converts);
do not confuse them.

#### 2.3.4 `script`

**Purpose.** Sandboxed JavaScript (goja) for **stored conflict-resolution procedures**.

```go
type Runtime struct{ /* goja, limits, compile cache */ }
func New(limits Limits) *Runtime
func (*Runtime) Compile(source string) error
func (*Runtime) Call(ctx context.Context, source string, mode Mode, host Host, argsJSON string) (Result, error)
func (*Runtime) ResolveConflict(...) (Decision, error)
type Mode int      // ModePure (no host calls) | ModeRead (Host.Get / Host.Query allowed)
type Host interface { Get(docID string) (string, error); Query(sql string, params []any) (string, error) }
type Limits struct{ WallClock time.Duration; MaxOutputBytes, MaxLogBytes, MaxHostCalls int }
type Decision struct{ Kind DecisionKind; Side int; Doc string } // DecideDefer|Take|Doc|Fork|Delete
type ConflictResolver struct{ Runtime *Runtime; Source string; Mode Mode; Host Host; Context context.Context }
func SourceHash(source string) string   // procedures are addressed by hash of their source
```

`script.ConflictResolver` implements `transaction.ConflictResolver` (the one explicit
`var _ transaction.ConflictResolver = ConflictResolver{}` assertion), so a stored procedure can be
plugged in as a `ConflictPolicyCustom` resolver. Errors are sentinels `ErrCompile`, `ErrTimeout`,
`ErrRuntime`, `ErrOutput`, `ErrDenied`, `ErrNotFound`. The server hosts one shared runtime
(`server.ProcedureRuntime()`), procedure definitions live in the meta namespace, and
`server.ConflictProcedure` is a background loop that applies authority procedures to the
conflict queue. Kotlin has the same idea (`kdb-script`: `ProcedureRuntime`, `GraalProcedureRuntime`)
but a different engine (Graal) and no in-merge parity (Kotlin merges are non-deterministic).

#### 2.3.5 `compaction` (unwired)

```go
type Engine interface { RunCycle(Request) (Result, error); Plan(Request) (Plan, error)
                        UpdatePeerHeads(ns string, heads map[string]codec.Hash) }
type Config struct { NamespaceID string; DAG dag.CommitDAG; Storage storage.Adapter
                     PolicyRegistry policy.Registry; Evaluator policy.Evaluator; Coordinator Coordinator }
type Coordinator interface{ RegisterPeer(namespaceID, peerID string) }   // InProcessCoordinator
type Blocker interface{ blocker() }   // ProtectedTag | ProtectedBranch | PeerBelowBoundary | PolicyDisabled
```

`defaultEngine.UpdatePeerHeads` is an empty method; the package has no non-test importer. The
operative Go compaction path is `EmbeddedKdbRuntime.Maintain()` / `CompactHistory()` ->
`checkpointAndTruncate` (checkpoint, then delete delta segments wholly below checkpoint and
retention window; section 2.4.8), with `dag.Squash` / `StubCommit` as DAG primitives.

### 2.4 Storage

Storage is a stack of small ports. From the bottom: `PlatformIOShim` (bytes in named segments) ->
WAL / delta log / SSTables (formats) -> `ServerEngine` (the composed per-namespace adapter) ->
`MultiplexAdapter` (routes by namespace id) -> `Adapter` as seen by transaction/sql. Beside it:
derived, rebuildable sidecars (`deltaindex`, SSTable index sidecars) memory-mapped through
`mmapfile`, and the attachment blob store `attachstore`.

#### 2.4.1 `storage` (ports and config)

```go
type Adapter interface {                                   // THE storage port (per namespace id)
  Capabilities() CapabilitySet
  GetDocument(ns string, docID codec.UUID, atCommit codec.Hash) (*document.Document, error)
  GetDocumentOrThrow(...) (document.Document, error);  GetDocuments(ns, ids, atCommit) ([]*document.Document, error)
  ScanDocuments(ns string, atCommit codec.Hash, batchSize int, onBatch func([]document.Document) error) error
  PutDocument(ns string, doc document.Document) error;  DeleteDocument(ns string, docID codec.UUID) error
  DiscardPending(ns string) error                          // abandon staged put/delete
  CommitTree(ns string, parentTreeHash codec.Hash) (document.DocumentTree, error)   // fold staged ops into a tree
  Flush(ns string) error;  ReadBlob(h codec.Hash) ([]byte, error);  WriteBlob(b []byte) (codec.Hash, error)
  IngestDeltaSegment(segment DeltaSegmentRef) error }
```

Write protocol: `PutDocument/DeleteDocument` *stage*; `CommitTree(parentTree)` produces the new
`DocumentTree` (the caller then appends the commit to the DAG); `DiscardPending` rolls back.

Optional capabilities probed by type assertion (the "optional interface" idiom, used heavily here):
`DamageTolerantReader` (`SkimSegment` -> `SegmentSkim`, `StreamCommitsPastDamage`, for integrity
repair), `TreeResolver` (`TreeAt(hash)`), `TreeWalker` (`WalkTree`), `TreePinner`
(`PinTree/UnpinTree`), `ForeignTreeStore` (`StoreForeignTree`, for snapshot-installed trees),
`DocumentCounter` (`CountDocuments(ns, tree)`: `COUNT(*)` without reading bodies),
`HiddenDocumentLister`, `OrderedScanner` (`ScanDocumentsAfter(ns, at, after *UUID, ...)`: id-ordered
keyset scans for paged SELECTs; `ErrOrderedScanUnsupported`), `ThroughReader`
(`ReadDocumentsThrough`, bulk reads for snapshot serving), `SnapshotApplier` (`ApplySnapshotBatch`,
snapshot bootstrap), `TreeBodyStreamer` (`StreamTreeBodies`; `ErrTreeBodiesUnsupported`),
`EvictableAdapter` (`EvictDocuments/EvictIndex/RebuildDocuments/RebuildIndex/EvictionState`, in
`enums.go`), `RealizedStoreHandle`.

Delta log ports (`delta_types.go`): `DeltaSegmentWriter{Append(DeltaRecord) (int64, error); Flush(); Seal()
(DeltaSegmentRef, error); IsSealed(); CurrentSizeBytes(); NamespaceID(); SegmentID()}`,
`DeltaSegmentReader{ReadAll; ReadRange; ListSegments}`, and optional reader capabilities:
`DeltaCommitStreamer{StreamCommits; ReadCommitAt}`, `DeltaPayloadReader{ReadPayloadAt}` (a commit's
canonical bytes, forwarded undecoded to peers), `DeltaPayloadStreamer{StreamFramePayloads}`,
`DeltaHashStreamer{StreamCommitHashesPastDamage}` (hashes only, no decode), `DeltaDocumentReader{ReadDocumentAt}`
(one document's patches out of a frame: the cold point read), `DeltaSegmentSequenceLister{SegmentSequences}`,
`DeltaSegmentRangeLister{ListSegmentsAfter}`, `DeltaSegmentStamper{StampSegment}`,
`DeltaFingerprintMemo{RememberFingerprint; RememberedFingerprint; SegmentRef}` (checkpoints fingerprint
unchanged segments without reading them). `DeltaSegmentSequencer{SequenceNumber()}`.
`DeltaRecord{CommitHash, NamespaceID, Authorship DeltaAuthorshipEnvelope, CommitPayload []byte,
DocumentPatches []DocumentPatch}`. `TailKind` (`TailClean | TailTorn | TailDamaged`) + `SegmentTail`
classify how a segment's intact frames end (`delta.go`).

I/O port: `PlatformIOShim` (`AppendToSegment`, `ReadFromSegment`, `FlushSegment`, `SealSegment`,
`ListSegments`, `DeleteSegment`, `AvailableBytes`, `Read/Write/DeleteSnapshot`), with optional
capabilities in `platform_io.go`: `SegmentSizer`, `SegmentStamper` (`SegmentStamp{Size, ModTimeNanos}`:
"unchanged since" without reading), `SegmentIntoReader` (`ReadFromSegmentInto(dst)`, for pooled
buffers), `SnapshotStreamer` (`WriteSnapshotStream`, `LocalSnapshotPath`; helper
`storage.WriteSnapshotStream(shim, key, write)` buffers for shims without it), `LocalPathResolver`
(`LocalSegmentPath`, where sidecars are kept beside a segment), `SegmentRenamer` (`RenameSegment`,
atomic publish of a merge output; `ErrRenameUnsupported`), `SegmentGenerationer`
(`SegmentGeneration(name) (gen, ok)`: a process-unique incarnation number, changed on delete, from
`NextSegmentGeneration()`; the key of `delta.FrameCache`).

Config/enums (all in `storage`):

| Type | Values / fields |
|---|---|
| `Durability` | `DurabilitySync` (default; ack after fsync), `DurabilityAsync` (ack after in-memory WAL append, background flush), `DurabilityMemoryOnly` |
| `HistoryMode` | `Unset`, `Full` (keep every commit), `None` (current data + `RetentionWindow`) |
| `HistoryStrategy` | `Unset`, `Replay` (rebuild historical trees by replaying the delta log, lazily), `Objects` (persist per-commit tree objects) |
| `ReclaimMode` | `Unset`, `Manual`, `Immediate`, `Balanced` (default), `Lazy`; `ReclaimCadence{Interval, Sweep, RespectLoad, MaxDefer, SealTriggered}` |
| `RetentionWindow` | `{Duration time.Duration; Commits int64}`; `RetainNothing`; `DefaultRetentionDuration` |
| `CompressionCodec` | `CompressionNone`, `CompressionZSTD` |
| `IndexRetention` | `Pinned`, `Evictable` |
| `StorageEngineConfig` | `PageTargetSizeBytes, PageMaxSizeBytes, GlobalMemoryBudgetBytes, HotTierMemory HotTierMemoryConfig{FixedBytes, PercentOfAvailable}, CompressionCodec, DefaultIndexRetention, IOShim, Durability, AsyncSyncIntervalMillis, WalMaxSegmentBytes, WalSkipCorruptRecords, Reclaim, DeltaMaxSegmentBytes, HistoryStrategy, HistoryMode, Retain, DocumentCacheBytes, BoundedLiveBodies, CommitOpsBytes, TreeChainLimit, MemtableFlushBytes, HistoryTreeCacheBytes, DeltaFrameCacheBytes, DisableCheckpoints, LargeCommitBodyBytes, DocumentCopies`; `ResolvedGlobalMemoryBudgetBytes()`, `ResolvedLargeCommitBodyBytes()` (`DefaultLargeCommitBodyBytes` 64 KiB, `DocumentCopiesBodyBytes` 16 KiB) |
| `BudgetArbiter` | Divides a host-wide memory pool among registered `BudgetParticipant{DemandBytes(); SetBudgetBytes(int64)}`s on a ticker (`NewBudgetArbiter(total, interval)`, `Register/Reserve/Unregister/Shares/Rebalance/SetFloorBytes/TotalBytes/Close`, `DefaultNamespaceFloorBytes`, `DefaultBudgetRebalanceInterval`). |

`BoundedLiveBodies` lets versions the live tree names be evicted once their commit is in the delta
log (read back through a location record or the cold loader). `DocumentCopies` lowers the
"file by value" threshold so a version in a large commit frame gets its own blob-store copy
instead of a 17-byte location into the frame; changing it between opens re-files existing
versions in the background (`engine.RefileVersions`).

Memory fractions (`memory_budget.go`) split one namespace's hot-tier budget and sum to 1.0:
`DefaultDocumentCacheFraction` (0.30), `DefaultCommitOpsFraction` (0.10), `DefaultMemtableFraction`
(0.20), `DefaultSSTableIndexFraction` (0.08), `DefaultBlockCacheFraction` (0.15),
`DefaultHistoryTreeFraction` (0.05), `DefaultIndexHistoryFraction` (0.05),
`DefaultDeltaFrameCacheFraction` (0.07), with `Resolved*Bytes(cfg)` helpers. `DefaultHotTierBytes`
(128 MiB) applies when nothing is configured; `TotalSystemMemoryBytes()` (`system_memory_*.go`)
backs `PercentOfAvailable` and the server's machine-sized default (`server.DetectMemoryBudgetBytes`).

#### 2.4.2 `storage/engine`

```go
type Engine interface{ storage.EvictableAdapter; NamespaceID() string }
type Handle interface { NamespaceID() string; Adapter() storage.EvictableAdapter
                        DeltaWriter() storage.DeltaSegmentWriter; DeltaReader() storage.DeltaSegmentReader; Close() error }
type Factory interface{ Target() Target; Open(namespaceID string, config storage.StorageEngineConfig) (Handle, error) }
type Target int   // TargetServer | TargetBrowser | TargetInMemory | TargetGPU | TargetReadOnly; IsReadOnly()
type DefaultFactory struct{ EngineTarget Target }
func NewServerEngine(ns string, config storage.StorageEngineConfig, w wal.WriteAheadLog) *ServerEngine
type BrowserEngine = ServerEngine;  type InMemoryEngine = ServerEngine   // aliases, not distinct classes
type MultiplexAdapter struct{ ... }   // NewMultiplexAdapter(); Register(ns, Adapter) / Unregister / Namespaces;
                                      // routes every call by ns (ErrUnknownNamespace)
type MemoryBreakdown struct{ ... };  type DocArenaStats struct{ ... };  type RefileStats struct{ ... }
type TreeChange struct{ ... }        // one put recorded for a tree object (RecordTreeObject)
```

`ServerEngine` (the real adapter) composes: a `wal.WriteAheadLog` + `wal.GroupCommitter` (blob puts
are logged), `memtable.Manager` -> `sstable.LsmBlobStore` (content-addressed blobs: documents keyed
by content hash, plus location records into delta frames), a sharded by-hash document version store
(`docsByHash *shardedDocByHashStore`: per shard an append-only byte arena + pointer-free `bodyRec`
slots threaded on an index-linked recency ring, looked up through an open-addressing `slotIndex`; live
versions pinned unless `BoundedLiveBodies`), a cold path for misses (`coldLoader`, normally
`deltaColdLoader` over a `deltaindex.Index` of versions -> frames, configured by `ConfigureColdIndex`,
plus `frameReads`: `delta.DeltaDocumentReader` behind a `delta.FrameCache`, `DeltaFrameCache()`),
a sharded pending store (staged ops), the live `tree` (+ `latestTree atomic.Pointer[treeSnapshot]`,
an optional `treeFile *document.TreeFile` from `SetTreeFile`, `treeChain` of incremental steps
bounded by `TreeChainLimit`) and a `boundedTreeStore` LRU of historical trees with a `treeRebuilder`
(strategy `Replay`) or recorded locations (`RecordCommitLocation`, `RecordFrameLocations`,
`RecordDocumentLocation`, `RecordTreeObject`; strategy `Objects`). Live knobs read at maintenance
time (`retainLive`, `reclaimLive`, `modeLive`, `strategyLive` under `retainMu`) let `SetHistoryMode`
switch without a reopen. It is also a `storage.BudgetParticipant` (`MemoryDemandBytes`,
`SetMemoryBudgetBytes`, `MemoryBreakdown`), a `DocumentTreeStore` (`GetTree/PutTree`),
`TreeResolver`, `TreePinner`, `ForeignTreeStore`, `DocumentCounter`, `OrderedScanner`,
`ThroughReader`, `SnapshotApplier`, `TreeBodyStreamer`.

Other entry points: `WriteKeyedBlob/WriteUnloggedBlob` (attachstore's small blobs),
`MaterializeLiveBodies/PrepareForTruncation/SetBodiesExternal` (make live bodies durable in the
blob store before log truncation or after a snapshot bootstrap), `CompactBlobStore`,
`CompactBlobStoreUpTo(maxBytes)` (bounded merge used by close), `CompactBlobStoreContext/AllContext`,
`BlobMergeDue/BlobRunCount/BlobTableCount`, `SetExtraReachable` (extra keep predicate for merges),
`RefileVersions(ctx)` / `VersionFiledByValue` (the `DocumentCopies` background re-file),
`EnableDurabilityTracking/NoteCommitDurable`, `RestoreLiveTree/RegisterHistoricalTree/SetTreeRebuilder`.
Scans resolve tree entries in windows (`scan_prefetch.go`) so a scan's bodies are bounded.

`DefaultFactory.Open` builds the WAL (`TargetServer` only), the `ServerEngine` and the delta
writer/reader (via `delta.Factory`) from the config's `IOShim`; for `TargetServer`/`TargetReadOnly`
it also installs the delta cold loader, the commit frame reader and the frame reads, and removes
orphan SSTable sidecars (read-only: sidecar builds off). It returns a `Handle`. In file mode the
`Host` calls it once per namespace with `EngineTarget` `TargetServer` or `TargetReadOnly`.

#### 2.4.3 `storage/wal`

```go
type WriteAheadLog interface { WalID() codec.UUID; PartitionKey() string; LastSequence() int64
  ActiveSegmentSizeBytes() int64; Append(Record) (AppendResult, error); AppendBatch([]Record) (AppendResult, error)
  Sync() error; Recover(handler func(Record) error) (RecoverySummary, error); Truncate(through int64) error; Close() error }
type Factory interface { OpenOrCreate(partitionKey string, config storage.StorageEngineConfig, io storage.PlatformIOShim)
  (WriteAheadLog, error); ActiveSegmentName(partitionKey string, walID codec.UUID) string }
type SegmentCatalog interface{ ListSegments(partitionKey string) ([]SegmentInfo, error); DeleteSegment(name string) error }
type Record struct{ Sequence int64; Timestamp codec.Timestamp; Kind RecordKind; Payload []byte }
// RecordKind: PutBlob | DeleteBlob | FlushCheckpoint | Marker
```

`DefaultWriteAheadLog` (one `sync.Mutex`, rotating segments named by
`RotatedSegmentName(partition, walID, firstSequence)`); `GroupCommitter{SyncTo(seq), SyncedSeq()}`
coalesces concurrent `Sync` requests into one fsync (goroutine + `WaitGroup`). `CorruptionError`
carries `Code() = StorageTierError`; `ClosedError`.

#### 2.4.4 `storage/delta` (+ `deltaindex`)

The **commit log**: the per-namespace append-only sequence of commit frames that is the source of
truth (everything else is rebuildable from it).

```go
type DefaultWriter struct{...}  // storage.DeltaSegmentWriter + DeltaSegmentSequencer; AppendBatch([]DeltaRecord) ([]int64, error);
                                // RotateIfNeeded(); Seal(); NewDefaultWriter(ns, segmentID, seq, shim, config)
type DefaultReader struct{...}  // storage.DeltaSegmentReader + every optional reader capability in 2.4.1
                                // + DamageTolerantReader; caches each sealed segment's scan under its stamp;
                                // SetFrameCache/FrameCache
type Factory struct{ Config storage.StorageEngineConfig }; (Factory).OpenWriter/OpenReader
type PageCodec struct{}         // Frame/Parse: PageFrameHeaderSize = 20, PageFormatVersion = 2 (codec id per frame); zstd
type FrameCache struct{...}     // NewFrameCache(budgetBytes); SetBudget/Budget/Bytes/Stats() FrameCacheStats
func ScanSegment(raw) ([]ScannedCommit, storage.SegmentTail);  func NextIntactFrame(raw, from) int
func ScanSegmentBytes/ScanSegmentFrames/ScanSegmentBounds/ScanSkippingCorrupt/ScanHashesSkippingCorrupt/WalkFrames/DecodeFrame(...)
type CorruptFrameError{Offset, Reason};  LegacySegmentFormatError{NamespaceID, Names};  UnreadableSegmentError
type DamagedSegmentError{...}   // intact frames follow damage: open refuses rather than drop history
const DefaultDeltaMaxSegmentBytes; func EffectiveMaxSegmentBytes(configured int64) int64
```

Segments are named `DeltaSequencedFileName(seq)` (`storage/io`). A torn tail (`TailTorn`) is the
expected shape of an unclean shutdown and is dropped; `TailDamaged` (intact frames after bad bytes,
or a CRC-valid frame that will not decode) is a hard error (`DamagedSegmentError`; see `integrity`).

`FrameCache` is the analogue of Cassandra's ChunkCache: a byte-budgeted, sharded LRU of
decompressed frames keyed by (segment generation, offset), pointer-free (256-byte pages in 1 MiB
arena chunks, `uint32` page/entry links). Only CRC-checked complete frames are inserted; a hit is
copied out under the shard lock. A nil `*FrameCache` caches nothing.

`deltaindex` maps a key (a version's content hash, or a commit hash) to the frame holding it
without a heap entry per key: per segment a sorted fixed-width (hash, offset) array, binary
searched newest segment first. Sealed segments' arrays are sidecar files (`SidecarSuffix = ".kdbx"`,
CRC'd, self-describing) memory-mapped via `mmapfile` (positioned reads where mmap is unsupported);
the newest segment's stays on the heap. A sidecar is a cache, never authoritative: any mismatch
rebuilds it.

```go
type Options struct{ Kind string; Keys KeyFunc; HashKeys bool }   // Kind is in the sidecar name ("versions", "commits")
type KeyFunc func(c document.Commit, emit func(codec.Hash))
func New(reader storage.DeltaSegmentReader, opts Options) *Index
// (*Index) SetDir(dir, readOnly); Lookup(key) (Location{Segment, FrameOffset}, bool, error); Refresh(); Prune(); Stats(); Close()
```

Users: the engine's `deltaColdLoader` (versions) and `embed`'s commit-operations loader (commits).

#### 2.4.5 `storage/memtable` and `storage/sstable`

`memtable.Table{Put, Get, Lookup, Delete, SizeBytes}` implemented by `SortedTable` (values in an
append-only `valueArena` of doubling chunks, records pointer-free; `FootprintBytes`, `Len`,
`RecordOverheadBytes`); `memtable.Manager` (`Put/Get/Contains/Delete/Flush(level)/SizeBytes/FootprintBytes`)
flushes to an `sstable.LsmBlobStore` when over `MemtableFlushBytes`. `sstable`: `Writer{Put, Delete,
Finish() (Handle, error)}`, `Reader{Get, Lookup}`, `DefaultWriter` (`NewDefaultWriter`,
`NewDefaultWriterUnique`, `PutShared`) / `DefaultReader` (`Index`, `ReadBlockAt`), `BlockCache`
(`NewBlockCache`, `Get/Put/RemoveSegment/SetCapacity/UsedBytes/Stats`), `Handle{FileHash, Level,
SegmentName}`, `Compact/CompactContext(ctx, io, ns, outLevel, inputs, dropTombstones, keep func(Hash) bool)
([]Handle, error)` with `ErrNothingToCompact`, `ErrCorruptTable`. `LsmBlobStore` (`Get/Lookup/Contains/
Tables/AddTable/DiscoverTables/RunCount/TableBytes/PendingMerge(trigger)`, `Compact(trigger, keep)`,
`CompactContext`, `CompactContextUpTo(..., maxBytes)`, `CompactAllContext` -> `CompactionResult{Merged,
Removed, Deferred, Dropped, Full, Outputs, Postponed}`; `SetBlockCacheBudget`, `SetIndexCacheBudget`,
`SetSidecarBuild`, `SidecarMappedBytes`, `RemoveOrphanSidecars`, `Close`) is the multi-run read path;
its `keep` predicate is how unreachable blobs are dropped during compaction.

* **Size-tiered merges** (`tiering.go`, after RocksDB universal compaction): a merge takes a window
  of consecutive runs, newest first. A full merge (the only one that drops tombstones) happens when
  everything newer adds up to the oldest run (`fullMergeRatio`); otherwise a window of at least
  `DefaultCompactionTrigger` similar-sized runs. Merges stream their output (`runWriter`) under a
  partial name and publish it with `SegmentRenamer`.
* **Sequence names** (`sequence.go`): `Sequence{Next(), Observe(name)}` hands out per-namespace table
  ages past the highest present, matching Kotlin's `SsTableSequence`.
* **Index sidecars** (`sidecar.go`, `sidecar_store.go`): a Go-only file per table under
  `ns/<ns>/sstidx/` holding a sorted fixed-width (key, block handle) array with a fanout table and a
  blocked Bloom filter (`bloom.go`), searched in place through `mmapfile`. Derived state: a missing or
  mismatched sidecar falls back to the table's footer index (`tableIndexCache`,
  `DefaultIndexCacheBytes`) and is rebuilt.

#### 2.4.6 `storage/io` (+ `s3`)

```go
type SegmentByteStore interface { Append(name string, b []byte) (int64, error); Read(name string, off int64, n int) ([]byte, error)
  Flush(name string, fsync bool) error; MarkSealed(name string) error; List(prefix string) ([]string, error)
  Delete(name string) error; AvailableBytes() (int64, error); Read/Write/DeleteSnapshot(...) }
// optional store capabilities (unexported): Stamp, ReadInto, Size, LocalSegmentPath, Rename
type ReplicaSink interface { PutSegment(name string, data []byte) error; DeleteSegment(name string) error
  WriteSnapshot(key string, data []byte) error; DeleteSnapshot(key string) error }
type SegmentFetcher interface{ GetSegment(name string) ([]byte, error) }
type HandleReleaser interface{ ReleaseHandles(prefix string) error }
type PlatformIOConfig struct{ RootDirectory *string; FsyncOnFlush bool; SyncMode SyncMode; MaxAppendBytes int; PreallocateBytes int64 }
```

Implementations: `OSByteStore` (files; `SyncMode` `SyncModeFull`/`SyncModeFast`, preallocation; a
write handle per segment until `MarkSealed` closes it, plus a bounded cache of read handles,
`maxReadHandles`; `Rename`, `WriteSnapshotStream`, `LocalSnapshotPath`, `SnapFileName`),
`PrimaryWithReplicas` (**decorator**: a primary `SegmentByteStore` plus `[]RoledSink{Sink, Role:
SinkReplica|SinkArchive}`; `ReplicationPolicy{FailOnReplicaError}`; adds `HasArchive`,
`RestoreSegment`), `FileBackedPlatformIO` (adapts a `SegmentByteStore` to `storage.PlatformIOShim`
and every optional shim capability in 2.4.1, built by `FileBackedPlatformIOFactory.Open`; per-segment
state in `segment_state.go` is bounded: striped `segmentLocks`, run-length `sealedSegments`,
`segmentGenerations`), `InMemoryPlatformIO`. Segment naming: `SegmentKind` (`Delta|WAL|SSTable`),
`DeltaSequencedFileName/ParseDeltaSequencedFileName`, `ValidateSegmentName`. `s3.ReplicaSink`
implements `ReplicaSink` + `SegmentFetcher` over `s3.BlobStore{Put,Get,Delete,List}` (`AWSBlobStore`,
`MemoryBlobStore`, `PrefixedStore`), configured by `s3.Config{Bucket, Region, Prefix, Endpoint,
PathStyle, EnsureBucket}` (`ConfigFromEnv`, `ArchiveConfigFromEnv`). `PlatformIOError` carries
`Code() = StorageTierError`.

#### 2.4.7 `storage/mem` and `storage/manager`

`mem.InMemoryStorageAdapter` implements `storage.Adapter` (+ `TreeResolver`, `WalkTree`,
`StoreForeignTree`, `OrderedScanner`, `DocumentCounter`, `WriteKeyedBlob`) with no persistence; it
is what `embed.OpenMemoryRuntime` and many tests use. `storage/manager` is a process-global
singleton (`Install(Manager)`/`Get()`; `DefaultManager`, `RealizedStorePool`, `PooledRealizedHandle`)
for the "realized store" enlistment model ported from Kotlin; the Go server does not use it
(`NotInstalledError` if you ask).

#### 2.4.8 Lifecycle of the persistence stack (in `embed`)

* **Open** (`restoreNamespace`): load newest valid *checkpoint* (a serialised `dag.CheckpointState`
  plus live tree, stored with `PlatformIOShim.WriteSnapshot("kdb:checkpoint:<ns>")`). Versions:
  `checkpointFormatVersion = 2` (codec records in schema namespace `dev.kdb.checkpoint`, still
  written for namespaces up to `legacyCheckpointMaxEntries` so v0.6.2 can roll back), 3 (references
  on-disk graph segments), 4 (packed live tree + commits), 5 (4 + attachment tables), 6 (tree and
  commits in a separately streamed, mapped section, `checkpoint_section.go`). A version or integrity
  mismatch is ignored and the log is replayed instead. Then replay delta segments after it
  (`replayDeltaNamespaceFrom`), then `PersistingCommitDAG` is layered on top. A full replay is
  followed by `checkpointAfterFullReplay`. A writable open that finds an SSTable backlog merges it
  in the background (`startBackgroundCompaction`), cancelled by close.
* **Write**: `PersistingCommitDAG.PersistAsync(commit)` enqueues a `logRequest` on
  `commitLogWriter.reqs`; one writer goroutine appends `DeltaRecord`s to the segment writer,
  flushes per `Durability`, and completes the returned `wait()` (group commit). `onPersisted`
  feeds `ServerEngine.RecordCommitLocation`; `onSealed` notifies the maintenance scheduler.
* **Maintain**: `EmbeddedKdbRuntime.Maintain()` -> `checkpointAndTruncate` (write checkpoint, then
  delete wholly-obsolete segments per `HistoryMode`/`RetentionWindow`/`ReclaimMode`, merge SSTables,
  respect `SetPeerRetentionFloor`). Driven by `MaintenanceScheduler` (section 2.8.1).
* **Close**: flush + seal the writer, `checkpointOnClose`, merge SSTables only up to
  `DefaultCloseCompactionBytes` (64 MiB, `StorageOptions.CloseCompactionBytes`;
  `engine.CompactBlobStoreUpTo`), close the handle.

#### 2.4.9 `storage/mmapfile` and `storage/bufpool`

`mmapfile` maps immutable files read-only so their bytes live outside the Go heap (commit graph
segments, `deltaindex` and SSTable sidecars, checkpoint sections). `Open(path) (*Mapping, error)`;
`Mapping{Size, Acquire() (*Region, error), Close}` is reference counted (unmapped only after the
owner's `Close` and every `Region.Release`); `Region{Bytes, CopyAt (turns SIGBUS into an error),
Guard, DropPages, Release}`. `MappedBytes()`, `ResidentBytes()` (mincore, cached) feed the memory
guard. `Supported`, `ErrUnsupported`, `ErrClosed`; callers fall back to the heap path on any error.

`bufpool` keeps size-classed `sync.Pool`s (4 KiB .. 4 MiB, `MaxPooled`) of read buffers:
`Get(n) *[]byte`, `Put(bp)`. Rule: `Put` only a buffer nothing can still reference (frame and block
reads that are decoded and dropped).

#### 2.4.10 `storage/attachstore`

Attachment bytes: one blob per scheme-1 content hash (`document.AttachmentContentHash`, a Merkle root
over 1 MiB chunks), streamed in and out. Small blobs (`Config.SmallMaxBytes`) go to the namespace
blob store through `SmallBlobs`; larger ones are standalone segments `<ns>/attachment/u-<id>` found
through pointer segments `h-<hash>.<generation>`, indexed by a bounded `pointerIndex` (sorted runs +
overlay + LRU). An older version can be re-stored as a binary delta against its successor; deltas
are read by random access, never rebuilt in memory. Readers pin segments (leases,
`Config.ReadLease`); finished uploads are held for `Config.UploadGrace` until a commit references
them. Created per namespace by `embed` (`attachments.go`).

```go
func New(shim storage.PlatformIOShim, namespaceID string, small SmallBlobs, cfg Config) *Store
type SmallBlobs interface{ ReadBlob(key codec.Hash) ([]byte, error); WriteKeyedBlob(key codec.Hash, b []byte) error }
type Config struct{ SmallMaxBytes, MaxBytes, DeltaMaxBytes int64; DeltaIndexEntries int; DeltaStepBytes int64
                    DisableDeltas bool; DeltaMaxDepth, HashWorkers int; ReadLease, UploadGrace time.Duration; PointerCacheBytes int64 }
// (*Store) Begin() *Upload{Write, Finish() (Info{Hash, Size}, error), Abort};  Put(io.Reader) (Info, error)
//          Open(h) (*Reader{Size, Hash, ChunkLeaf, Read, ReadAt, Close}, error);  ReadAll(h, limit);  Has; Hold; Committed
//          StoreAsDelta(old, base); DeltaStep(old, base, budget) (DeltaOutcome, int64, error); Deltas; InflateStep
//          Collect(keep) (int, error); Mark/SetMark; RefIndex/SetRefIndex (RefIndex{Has}); LargeBlobs; PinnedSegments
type DeltaOutcome int   // DeltaSkipped | DeltaStored | DeltaPending
var ErrNotFound, ErrTooLarge, ErrCorrupt, ErrReadLeaseExpired;  func ReportCorrupt(err, attrs...) bool; CorruptReads()
```

![Diagram 7](diagrams/class-07.svg)

<details><summary>Mermaid source</summary>

```mermaid
classDiagram
  class Adapter {
    <<interface>>
    +GetDocument(ns, id, atCommit) Document
    +PutDocument(ns, doc)
    +CommitTree(ns, parentTree) DocumentTree
    +ReadBlob(hash) bytes
    +IngestDeltaSegment(ref)
  }
  class EvictableAdapter {
    <<interface>>
    +EvictDocuments(id)
    +RebuildIndex(id, from)
  }
  class MultiplexAdapter {
    +Register(ns, adapter)
    +Unregister(ns)
  }
  class WriteAheadLog {
    <<interface>>
    +Append(record) AppendResult
    +Recover(handler) RecoverySummary
  }
  class DeltaSegmentWriter {
    <<interface>>
    +Append(record) int64
    +Seal() DeltaSegmentRef
  }
  class DeltaSegmentReader {
    <<interface>>
    +ReadAll(ref) DeltaRecords
    +ListSegments() DeltaSegmentRefs
  }
  class PlatformIOShim {
    <<interface>>
    +AppendToSegment(name, bytes) int64
    +ReadFromSegment(name, off, n) bytes
    +WriteSnapshot(key, data)
  }
  class SegmentByteStore {
    <<interface>>
  }
  class ReplicaSink {
    <<interface>>
  }
  class LsmBlobStore {
    +Lookup(key) bytes
    +CompactContextUpTo(ctx, trigger, keep, maxBytes) CompactionResult
  }
  class AttachStore["attachstore.Store"] {
    +Begin() Upload
    +Open(hash) Reader
    +Collect(keep) int
  }
  Adapter <|-- EvictableAdapter
  EvictableAdapter <|.. ServerEngine
  Adapter <|.. MultiplexAdapter
  Adapter <|.. InMemoryStorageAdapter
  MultiplexAdapter o-- ServerEngine : per namespace
  ServerEngine *-- WriteAheadLog
  ServerEngine *-- LsmBlobStore
  ServerEngine *-- MemtableManager
  MemtableManager ..> LsmBlobStore : flush
  LsmBlobStore ..> PlatformIOShim
  WriteAheadLog <|.. DefaultWriteAheadLog
  DefaultWriteAheadLog ..> PlatformIOShim
  DeltaSegmentWriter <|.. DefaultWriter
  DeltaSegmentReader <|.. DefaultReader
  DefaultWriter ..> PlatformIOShim
  DefaultReader ..> PlatformIOShim
  ServerEngine ..> DefaultReader : cold reads
  AttachStore ..> PlatformIOShim : large blobs
  AttachStore ..> ServerEngine : small blobs
  PlatformIOShim <|.. FileBackedPlatformIO
  PlatformIOShim <|.. InMemoryPlatformIO
  FileBackedPlatformIO o-- SegmentByteStore
  SegmentByteStore <|.. OSByteStore
  SegmentByteStore <|.. PrimaryWithReplicas
  PrimaryWithReplicas o-- SegmentByteStore : primary
  PrimaryWithReplicas o-- ReplicaSink : replicas and archive
  ReplicaSink <|.. S3ReplicaSink
```

</details>

![Diagram 8](diagrams/class-08.svg)

<details><summary>Mermaid source</summary>

```mermaid
classDiagram
  class ServerEngine {
    +GetDocument(ns, id, atCommit) Document
    +SetColdLoader(loader, budget)
    +RefileVersions(ctx) RefileStats
    +CompactBlobStoreUpTo(maxBytes) CompactionResult
  }
  class shardedDocByHashStore {
    arena bytes
    slots bodyRec
    slotIndex
  }
  class deltaColdLoader {
    +load(docID, contentHash) Document
  }
  class DeltaIndex["deltaindex.Index"] {
    +Lookup(key) Location
    +Refresh() bool
    +Prune()
  }
  class frameReads {
    reader DeltaDocumentReader
    cache FrameCache
  }
  class FrameCache["delta.FrameCache"] {
    +SetBudget(bytes)
    +Stats() FrameCacheStats
  }
  class DefaultReader["delta.DefaultReader"] {
    +ReadDocumentAt(ref, off, id, fn)
    +ReadCommitAt(ref, off) Commit
    +StampSegment(seq) SegmentStamp
  }
  class LsmBlobStore {
    +Lookup(key) bytes
    +PendingMerge(trigger) int64
  }
  class BlockCache
  class Sequence["sstable.Sequence"] {
    +Next() string
  }
  class sidecar["sstable sidecar"] {
    fanout
    bloom
  }
  class Mapping["mmapfile.Mapping"] {
    +Acquire() Region
    +Close()
  }
  class SegmentGenerationer {
    <<interface>>
    +SegmentGeneration(name) uint64
  }
  ServerEngine *-- shardedDocByHashStore : hot versions
  ServerEngine *-- deltaColdLoader : misses
  ServerEngine *-- frameReads
  ServerEngine *-- LsmBlobStore : by value and locations
  deltaColdLoader *-- DeltaIndex
  DeltaIndex ..> DefaultReader : scans segments
  DeltaIndex ..> Mapping : sealed-segment sidecars
  frameReads o-- FrameCache
  frameReads o-- DefaultReader
  FrameCache ..> SegmentGenerationer : key
  LsmBlobStore o-- BlockCache
  LsmBlobStore *-- Sequence : table names
  LsmBlobStore *-- sidecar : per table
  sidecar ..> Mapping
```

</details>

### 2.5 Query: `sql`, `query/hybrid`

#### 2.5.1 `sql`

**Purpose.** A hand-written SQL subset over JSON documents: parser, planner, executor, DML that
emits `document.Op`s (not storage writes), DDL for tables/indexes, aggregates, attachments, and the
hybrid operators `MATCH`, `SIMILARITY`, `FUSE`.

```go
type Engine interface { Execute(sql string, ctx QueryContext) (QueryResult, error)
                        ExecuteDML(sql string, ctx QueryContext) (DMLResult, error) }
func NewEngine(store storage.Adapter, d *dag.InMemoryCommitDag) Engine
func NewEngineWithIndexes(store storage.Adapter, d *dag.InMemoryCommitDag, provider IndexProvider) Engine
func SetAttachmentBlobs(e Engine, blobs AttachmentBlobs)
type Parser interface{ Parse(sql string) (Statement, error) }          // DefaultParser
type Planner interface{ PlanSelect(q SelectQuery, sch schema.KdbSchema) (PhysicalPlan, Expr, error) } // DefaultPlanner
type IndexProvider interface {
  FullTextSearch(ctx QueryContext, indexOrField, query string, depth int) (hits []index.RankedResult, ok bool, err error)
  VectorSearch(ctx QueryContext, field string, vector []float32, depth int) (...)
  ExactLookup(ctx QueryContext, field string, value Cell) (ids []codec.UUID, ok bool, err error)
  RangeLookup(ctx QueryContext, field string, low, high Cell, lowInclusive, highInclusive bool) (...) }
// optional IndexProvider capabilities:
type ExactCounter interface{ ExactCount(ctx, field string, value Cell) (n int, ok bool, err error) }       // COUNT from index counts
type OrderedKeyIndex interface{ IndexKeyGroups(ctx, field string, kind IndexKeyKind, after Cell, maxIDs int) ([]IndexKeyGroup, bool, error)
                                UnindexedDocs(ctx, field string, ids []codec.UUID) ([]codec.UUID, bool, error) }  // BTREE top-k
type CompositeOrderedIndex interface{ CompositeBTrees() []index.Store }                                  // composite top-k
type IndexDDLExecutor interface{ CreateIndex(StmtCreateIndex, QueryContext) error; DropIndex(StmtDropIndex, QueryContext) error }
type AttachmentBlobs interface{ PutAttachmentBlob(ns, r io.Reader) (codec.Hash, int64, error)
                                HasAttachmentBlob(ns, h) (bool, error); ReadAttachmentBlob(ns, h, limit int64) ([]byte, error) }
// optional Engine capabilities (all implemented by defaultEngine), used by the server's write gate:
type ImmediateUpdater interface{ UpdateCandidates(upd UpdateStatement, ctx QueryContext, k int) (ids []codec.UUID, complete bool, err error)
                                 ExecuteUpdateAmong(upd UpdateStatement, ctx QueryContext, among []codec.UUID) (DMLResult, error) }
type KeyedUpdater interface{ UpdateKeyedTargets(upd UpdateStatement, ctx QueryContext) (ids []codec.UUID, ok bool) }
type ImmediateInserter interface{ PrepareImmediateInsert(ins, ctx) (InsertRow, error); InsertReturning(ins, ctx, row InsertRow) DMLResult }
type CandidateEstimator interface{ EstimateCandidates(q SelectQuery, ctx QueryContext) int }           // admission sizing
type Executor struct{ Storage storage.Adapter; DAG *dag.InMemoryCommitDag; IndexProvider IndexProvider; Attachments AttachmentBlobs }
type DMLExecutor struct{ Executor *Executor }   // ExecuteInsert/Update/Delete -> DMLResult{Operations []document.Op, RowsAffected,
                                                //   GeneratedIDs, Columns, Rows (RETURNING)}
type DDLExecutor struct{}                        // ExecuteCreateTable -> new schema
type QueryContext struct { NamespaceID string; Schema schema.KdbSchema; AtCommit *codec.Hash; Parameters []Parameter
                           MaxRows, RowBudget int; Stats *ExecStats; Page *PageRequest; RetainedLimit int64 }
type PageRequest struct{ Offset, Size int; After *codec.UUID; OffsetOnly bool }   // paged SELECT (keyset after kdb_id)
type QueryResult struct{ Columns; Rows; RowsAffected; GeneratedIDs; AppliedSchema; Plan string; More bool; RowDocIDs []codec.UUID }
```

Sealed ASTs: `Statement` (`StmtSelect{Query, Explain}/Insert/CreateTable/Update/Delete/CreateIndex/DropIndex/Begin/Commit/Rollback`),
`Expr` (`ExprLiteral/ColumnRef/Parameter/Binary/Arith/Unary/FunctionCall/In/Between/Case/Match/Similarity/Fuse/Stream`),
`Projection` (`ProjStar/Column/Expression`), `Cell` (`CellNull/String/Long/Double/JSON/Bool/Bytes`, plus internal
`cellAttachmentRef`), `Parameter` (`ParamString/Int/Double/Bool/Null/Vector/Bytes/AttachmentRef`),
`PhysicalPlan` (`PlanFullScan/SingleRow/Filter/Limit/IndexLookup/Search`; `PlanIndexLookup.Kind` is
`eq | range | in`).

Statement surface beyond plain CRUD: `UpdateStatement{Table, Assignments, Where, Attachments
[]AttachmentChange, Removes []string (UPDATE ... REMOVE path), OrderBy, Limit, Returning}`
(`UPDATE ... ORDER BY ... LIMIT k RETURNING` is the queue claim); `InsertStatement{Table, Columns, Rows,
Attachments []AttachmentSpec, OnConflict *OnConflict{Columns, Update}, Returning}` (`INSERT ... ON
CONFLICT (cols) DO UPDATE | DO NOTHING`, decided by the server under its write gate; `upsert.go`);
`StmtCreateIndex{Name, Table, Fields []IndexField{Path, Weight}, Using, Unique, With}` (several fields:
composite BTREE). Attachments (`attachments.go`): `WITH ATTACHMENT`, `SET/DROP/RENAME ATTACHMENT`,
`STREAM ?` (an uploaded blob), virtual column `_attachments`, `ATTACHMENT()`, `ATTACHMENT_COUNT/SIZE/MIME()`,
`X'..'`/`FROM_BASE64` literals (`MaxSqlLiteralBytes`, `MaxSqlAttachmentReadBytes`). Scalar functions
include `COALESCE`, `CASE WHEN`, arithmetic, `ARRAY_CONTAINS[_ANY]`, `ARRAY_LENGTH`, `EXISTS`, `ALL_VALUES`.

Executor access paths: index lookup per conjunct; an `OR` served as the deduplicated union of its
disjuncts' paths (`orLookup`, label `index:or(...)`) when every disjunct has one; `COUNT(*)` from
`ExactCounter` or `storage.DocumentCounter` without reading bodies; bounded top-k for `ORDER BY ...
LIMIT` (`topk.go`, `maxTopK`), served from a BTREE walk in key order where it provably agrees with
SQL order (`ordered_topk.go`, ascending only) or from a composite BTREE under an equality prefix
(`composite_topk.go`, either direction); id-ordered keyset scans (`storage.OrderedScanner`) for
paging, with `QueryResult.More` driving the wire continuation token.

Cost control: `QueryShape` (`ShapeOfSelect`; `Fingerprint, HasOrderBy, HasAggregate, HasPredicate,
ProjStar, CountOnly, ...`) is the input of `server.CostModel.EstimateScan`; `QueryContext.RowBudget`
makes the executor fail with `ScanRowBudgetExceededError{Budget}` (classified to `RESOURCE_EXHAUSTED`
on the wire); `QueryContext.RetainedLimit` stops a query holding too much memory
(`RetainedLimitError{Limit, Held}`). `ExecStats{RowsExamined, DocsRead, DocBytesRead, RetainedBytes,
IndexBehind}` feeds `CostModel.ObserveScanActual`. Errors: `ParseError` (embeds
`*kdberr.DecodeError`), `PlanningError` (embeds `*kdberr.SchemaError`).

Stateless apart from the engine's references; safe for concurrent `Execute` as long as the
storage adapter is. `server.KdbServerRuntime` rebuilds its `sqlEngine` (under `sqlEngineMu`, a
`sync.RWMutex`) whenever the index provider changes; `server.RegistryIndexProvider` implements
`IndexProvider`, `ExactCounter`, `OrderedKeyIndex`, `CompositeOrderedIndex` and `IndexDDLExecutor`.

#### 2.5.2 `query/hybrid` (unwired)

A port of Kotlin `kdb-hybrid-query`: `Engine{Execute, Prepare, Checkout, ResetCheckout}`,
`Config{SQL, DAG, PolicyRegistry, Storage, Parser, VersionResolver, CheckoutStore}`,
`VersionClause` (sealed `AtTag/AtCommit/AtTime`, parsed by `StripVersionClause` from
`... AS OF ...`), `Request{..., ReadConsistency, ReadPin, SessionCheckout, DeferCommit,
TransactionBase}`, `CheckoutStore`. Errors `HistoryDisabledError`, `HistoryNotRetainedError`,
`ReadOnlyCheckoutError`. The Go server implements read-consistency and time travel directly in
`server/wire_listen.go` (`execRead`) and `server/history.go`, so this package is only exercised by
its tests.

![Diagram 9](diagrams/class-09.svg)

<details><summary>Mermaid source</summary>

```mermaid
classDiagram
  class SqlEngine["sql.Engine"] {
    <<interface>>
    +Execute(sql, ctx) QueryResult
    +ExecuteDML(sql, ctx) DMLResult
  }
  class ImmediateUpdater {
    <<interface>>
    +UpdateCandidates(upd, ctx, k) ids
    +ExecuteUpdateAmong(upd, ctx, among) DMLResult
  }
  class ImmediateInserter {
    <<interface>>
    +PrepareImmediateInsert(ins, ctx) InsertRow
  }
  class CandidateEstimator {
    <<interface>>
    +EstimateCandidates(q, ctx) int
  }
  class Parser {
    <<interface>>
  }
  class Planner {
    <<interface>>
  }
  class IndexProvider {
    <<interface>>
  }
  class ExactCounter {
    <<interface>>
  }
  class OrderedKeyIndex {
    <<interface>>
  }
  class CompositeOrderedIndex {
    <<interface>>
  }
  class IndexDDLExecutor {
    <<interface>>
  }
  class AttachmentBlobs {
    <<interface>>
  }
  SqlEngine <|.. defaultEngine
  ImmediateUpdater <|.. defaultEngine
  ImmediateInserter <|.. defaultEngine
  CandidateEstimator <|.. defaultEngine
  defaultEngine *-- Parser
  defaultEngine *-- Planner
  defaultEngine *-- Executor
  defaultEngine *-- DMLExecutor
  defaultEngine *-- DDLExecutor
  Parser <|.. DefaultParser
  Planner <|.. DefaultPlanner
  Executor o-- IndexProvider
  Executor o-- AttachmentBlobs
  Executor ..> StorageAdapter
  Executor ..> InMemoryCommitDag
  IndexProvider <|.. RegistryIndexProvider
  ExactCounter <|.. RegistryIndexProvider
  OrderedKeyIndex <|.. RegistryIndexProvider
  CompositeOrderedIndex <|.. RegistryIndexProvider
  IndexDDLExecutor <|.. RegistryIndexProvider
```

</details>

### 2.6 Wire, stream, transport

#### 2.6.1 `wire`

**Purpose.** The frame protocol shared by the SQL/session wire, the stream (subscriber) protocol,
both peer-sync protocol versions, attachments and the client replica's ref watch.

Frame layout (`wire/frame.go`, `FrameHeaderSize = 12`, `DefaultMaxFrameBytes = 16 MiB`, all
little-endian): `[0:4] int32 total frame length | [4:6] int16 MessageType | [6:8] int16
ProtocolVersion | [8:12] int32 CorrelationID`, then payload. `KdbWireProtocolVersion = 1`
(`MinSupportedWireProtocolVersion = 1`); `SyncProtocolVersion = 2` is the peer-sync-v2 generation
carried in `SYNC_HELLO`.

Payload encoding (`binary.go`): the payload starts with a one-byte tag. Tags 0/1 are the JSON
envelope; tag 2 is a binary body (the message's fields in protobuf wire format, strings and bytes
raw, unknown fields skipped); tag 3 is one zstd frame of a tag-2 body (bodies past
`zstdFrameThreshold`). A sender uses tags 2/3 only after the handshake ack lists the matching
feature; a decoder accepts every tag. Features (`SupportedWireFeatures`, offered in
`HandshakePayload.WireFeatures`, settled by `NegotiateWireFeatures`): `WireFeatureBinary`
(`"binary-frames"`, core load-path messages), `WireFeatureBinary2` (`"binary-frames-2"`, the rest of
the client set), `WireFeatureZstd` (`"zstd-frames"`), `WireFeatureAttachments` (`"attachments"`:
attachment messages, `CellKindBytes`, `$bytes`/`$attachment` parameters), `WireFeatureAttachmentProofs`.
The Kotlin server acks none, so it stays on JSON.

```go
type Header struct{ MessageType MessageType; ProtocolVersion, CorrelationID, PayloadLength int }
type Message interface{ Header() Header }          // every message struct embeds H Header
type Codec interface { Encoding() PayloadEncoding; Encode(Message) ([]byte, error); Decode(frame []byte) (Message, error)
                       EncodeFrameOnly(header Header, payload []byte) ([]byte, error); DecodeHeader(frame []byte) (Header, error) }
func NewCodec(encoding PayloadEncoding) *DefaultCodec        // EncodingKdbBinary | EncodingJSON
// (*DefaultCodec) WithWireFeatures(features) *DefaultCodec; BinaryFrames() bool; EncodePooled(msg) (frame, release, err)
type HandshakeNegotiator interface{ Negotiate(local, remote HandshakePayload) (HandshakeAckPayload, error) }   // DefaultHandshakeNegotiator
type CellKind byte         // SqlResult typed cells: String Null Long Double Bool JSON Bytes (SqlResultMessage.CellKinds)
type ErrorCode string      // BUSY UNAVAILABLE DEADLINE_EXCEEDED RESOURCE_EXHAUSTED CONFLICT SCHEMA_VIOLATION
                           // UNIQUE_VIOLATION UNAUTHORIZED UNSUPPORTED NAMESPACE_MISMATCH INTEGRITY NOT_HOME
                           // SHALLOW_BOUNDARY (peer sync only) NOT_FOUND INTERNAL
```

`SqlExecMessage{..., PageSize, Continuation}` / `SqlResultMessage{..., ContinuationToken, CellKinds}`
carry paged SELECTs and typed result cells (a row's cell values stay strings; `CellKinds` says how to
read each). `HandshakePayload` carries `User/Password/Token` (revocable access tokens), `Filter` and
`WireFeatures`.

Message families (one struct per `MessageType`, 0x01..0x49 and 0x50..0x51):

| Family | Types (MessageType hex range) |
|---|---|
| Session/SQL | `HandshakeMessage/AckMessage` (0x01, handshake), `SessionBeginMessage/AckMessage` (0x0E, 0x13), `SqlExecMessage`/`SqlResultMessage` (0x0F/0x10), `TxCommitMessage` (0x11), `TxRollbackMessage` (0x12) |
| Direct doc ops (Go-only) | `DocumentGetMessage/ResultMessage` (0x14/0x15), `UpsertMessage/ResultMessage` (0x16/0x17), `LockAcquire/Renew/Release/ResultMessage` (0x19-0x1C), `SearchMessage/ResultMessage` (0x1D/0x1E, in both trees), `HistoryList/Result` and `Revert/Result` (0x1F-0x22), `TxCommitMulti/Result` (0x23/0x24) |
| Stream | `DeltaCommitMessage` (0x02), `PositionAckMessage` (0x0C), `CompactionNoticeMessage` (0x08), `IceArchiveNoticeMessage` (0x09), `SchemaPushMessage` (0x0D), `TransactionReplayMessage` (0x06), `ConflictReportMessage` (0x07) |
| Peer sync v1 | `CommitFetchMessage` (0x03), `CommitPushMessage` (0x04), `CommitPushAckMessage` (0x18), `DagDiffMessage` (0x05), `SnapshotRequest/ResponseMessage` (0x0A/0x0B), `PeerErrorMessage` (0x25) |
| Peer sync v2 | `SyncHello/Ack` (0x26/0x27), `RefsRequest/Result` (0x28/0x29), `FetchRequest` + `PackPage` (0x2A/0x2B), `RefUpdate/Ack` (0x2C/0x2D), `SnapshotFetch` + `SnapshotPage` (0x2E/0x2F), `ProjectFetch/Page/Write/WriteResult` (0x30-0x33), `TreeNodes/Result` (0x34/0x35), `ObjectFetch/Result` (0x36/0x37), `DocFetch/Result` (0x38/0x39), `GraftPush/Result` (0x3A/0x3B), `MetaView/Result` (0x3C/0x3D), `HomeRequest/Result` (0x3E/0x3F), `SquashHead/Result` (0x40/0x41, squash catch-up; `SyncCapSquash`), `BlobFetch/Result` (0x42/0x43, ranged attachment blob reads with chunk proofs; `SyncCapAttachments`) |
| Attachments (client) | `AttachmentUpload/Result` (0x44/0x45), `AttachmentGet` + `AttachmentData` (0x46/0x47), `AttachmentList/Result` (0x48/0x49); always binary bodies, behind `WireFeatureAttachments` |
| Replica watch | `RefsWatchMessage` (0x50), `RefsChangedMessage` (0x51, reply and then unsolicited under the watch's correlation id; `SyncCapWatch`; 0x50-0x57 reserved for the replica) |

Results that can fail carry `Error *string` (legacy, prose) **and** `ErrorCode *ErrorCode` +
`RetryAfterMs *int` (additive, so old clients keep working). Mapping from server errors to codes is
in section 4.3. Wire errors: `wire.DecodeError{*kdberr.DecodeError}`, `FrameTooLargeError{Length, Max}`,
`InvalidCorrelationError`. Mapping helpers: `payload_mapper.go`, `op_mapper.go`, `transaction_codec.go`,
`commit_push_codec.go` (`EncodeCommitsWithPayloads` forwards logged payload bytes undecoded) convert
between wire DTOs and `document.*`; `binary_messages.go` / `binary_messages_more.go` hold the binary
forms; `snapshot_page_json.go` writes a snapshot page's JSON envelope pre-sized; `size.go`
(`EncodedStringLen`) bounds a reply before encoding. `wire.PayloadEnvelope` / `HandshakeDTO` are
exported aliases of internal DTOs for `kdb-inspect`.

#### 2.6.2 `stream`

**Purpose.** The push channel: a coordinator publishes committed deltas to subscribers; subscribers
can write back through transaction replay. Also defines the `Transport` port.

```go
type Transport interface{ Connect(uri string) (ConnectionHandle, error) }
type ConnectionHandle interface { Send(frame []byte) error; Incoming() <-chan []byte; Close() error; TryPoll() []byte }
type Coordinator interface { Start(SessionConfig) error; Stop() error; Publish(PublishedCommit) error
                             Subscribers() <-chan SubscriberState; SetTransactionReplayer(TransactionReplayer) }
type Subscriber interface { Connect(SubscriberConfig) (*Connection, error); Disconnect() error; Events() <-chan Event }
type SubscriberConfig struct{ NamespaceID, NodeID string; Mode ClientMode; CoordinatorURI string; ResumeFrom *codec.Hash
                              User, Password, Token *string; JSONOnly bool; ... }
type IndexHintApplier interface{ Apply(namespaceID string, hints []wire.IndexHint) }   // NoopHintApplier
func NewCoordinator(w wire.Codec, t Transport) Coordinator;  func NewSubscriber(w, t, hintApplier) Subscriber
type InMemoryTransport struct{...}; type Hub struct{ ServerHandler func(frame []byte) }; func HubFor(name) *Hub
```

`defaultCoordinator` (one `sync.Mutex`, `subscribers chan SubscriberState`) and `defaultSubscriber`
(one `readLoop` goroutine, `events chan Event`, `pendingReplays map[int]chan wire.Message`,
`handshakeAck chan wire.Message`). A subscriber offers `wire.SupportedWireFeatures` in its handshake
unless `JSONOnly`, so a hub that acks them sends `DeltaCommit` as binary bodies. `ClientMode`:
`ClientReadOnly | ClientWriteBack`. `Event` kinds:
`Connected, DeltaReceived, PositionUpdated, CompactionWarning, IceArchived, Disconnected, Error`.
In the server, the real broadcaster is `server.StreamHub` (`NewStreamHub(codec, ns, runtime)`,
`Publish(commit)`, `SetAllowAnonymous`), attached as `KdbServerRuntime.CommitListener`;
`stream.Coordinator` is the library-style equivalent used by tests and embedding.
Errors embed `*kdberr.DecodeError`: `DesyncError{ExpectedParent, ActualParent}`, `NotConnectedError`.

#### 2.6.3 `transport/core`, `transport/tcp`, `transport/ws`

```go
// core: shared framing + options
type TransportConnectOptions struct{ ConnectTimeoutMs, ReadTimeoutMs int64; MaxFrameBytes int
  ConnectHeaders map[string]string; TLS *TransportTlsSettings; Admitter FrameAdmitter; MaxConnections, IncomingQueueFrames int
  WriteTimeoutMs int64 }                                    // WriteTimeout(): 0 -> DefaultWriteTimeout (30s), <0 -> none
type FrameAdmitter func(header wire.Header) (rejection []byte, err error)    // pre-decode admission on header only
type TransportTlsSettings struct{ Enabled, InsecureSkipVerify bool; CAFile, CertFile, KeyFile, ServerName string; RequireClientAuth bool }
type FrameReader struct{...}         // NewFrameReader(r, maxFrameBytes, admit) / NewFrameReaderSize; Next() (frame, rejection, err)
type FrameStreamReader struct{...}   // reassembles frames from byte chunks; Feed/TakeRejections/Reset/BufferedBytes
type SynchronousSender interface{ SendCompletesSynchronously() bool }       // caller may reuse a pooled frame after Send
type WireConnectionAttributes struct{...}
// tcp
type Transport interface { stream.Transport
  Listen(ctx, uri string, handler func(stream.ConnectionHandle)) error
  ListenBound(uri string) (net.Listener, error);  Serve(ctx, ln net.Listener, handler func(stream.ConnectionHandle)) error }
// ws
type Transport interface { stream.Transport; ConnectWithOptions(uri, options); Listen(ctx, uri, options, handler)
  ListenBound(uri, options); Serve(ctx, ln, options, handler) }
func Upgrade(w http.ResponseWriter, r *http.Request, options core.TransportConnectOptions) (stream.ConnectionHandle, error)
```

`tcp.NewTransport(options)` / `ws.NewTransport(options)` return the `Transport` interfaces. Both
spawn per-connection reader goroutines feeding `Incoming() chan []byte` (bounded by
`IncomingQueueFrames`; `DefaultIncomingQueueFrames`), enforce `MaxFrameBytes`, bound each `Send` by
`WriteTimeout()`, and implement `SynchronousSender`. TCP reads through `core.FrameReader` (a 64 KiB
`bufio` reader, `DefaultReadBufferBytes`; bodies past 1 MiB grow as bytes arrive, so a declared
length alone cannot make the server allocate 16 MiB). The `FrameAdmitter` hook lets the server shed
an oversize/over-budget request **after reading 12 header bytes and before buffering the payload**
(`server.FrameHost.Admitter` -> `runtime.frameAdmitter`).

![Diagram 10](diagrams/class-10.svg)

<details><summary>Mermaid source</summary>

```mermaid
classDiagram
  class Codec {
    <<interface>>
    +Encode(msg) bytes
    +Decode(frame) Message
  }
  class DefaultCodec {
    +WithWireFeatures(features) DefaultCodec
    +BinaryFrames() bool
    +EncodePooled(msg) bytes
  }
  class Message {
    <<interface>>
    +Header() Header
  }
  class Transport {
    <<interface>>
    +Connect(uri) ConnectionHandle
  }
  class ConnectionHandle {
    <<interface>>
    +Send(frame)
    +Incoming() chan
    +Close()
  }
  class SynchronousSender {
    <<interface>>
    +SendCompletesSynchronously() bool
  }
  class FrameReader["core.FrameReader"] {
    +Next() frame
  }
  class FrameAdmitter["core.FrameAdmitter"] {
    <<function>>
  }
  class TcpTransport["tcp.Transport"] {
    +Listen(ctx, uri, handler)
    +Serve(ctx, ln, handler)
  }
  class WsTransport["ws.Transport"] {
    +Listen(ctx, uri, opts, handler)
    +Serve(ctx, ln, opts, handler)
  }
  class Coordinator {
    <<interface>>
    +Publish(commit)
  }
  class Subscriber {
    <<interface>>
    +Connect(cfg) Connection
  }
  Codec <|.. DefaultCodec
  DefaultCodec ..> Message
  Transport <|.. TcpTransport
  Transport <|.. WsTransport
  Transport <|.. InMemoryTransport
  TcpTransport ..> FrameReader : per connection
  FrameReader o-- FrameAdmitter
  WsTransport ..> FrameAdmitter
  ConnectionHandle <|.. socketConnection
  ConnectionHandle <|.. wsConnection
  SynchronousSender <|.. socketConnection
  SynchronousSender <|.. wsConnection
  Coordinator <|.. defaultCoordinator
  Subscriber <|.. defaultSubscriber
  defaultCoordinator o-- Codec
  defaultCoordinator o-- Transport
  defaultSubscriber o-- Transport
  defaultSubscriber ..> ConnectionHandle
```

</details>
### 2.7 Distribution: `peersync`, `replication`, `syncnode`

#### 2.7.1 `peersync`

**Purpose.** Replicates commit DAGs between nodes (Git-like: refs, want/have pack negotiation,
three-way merge with a configurable resolution chain, snapshots/grafts for shallow joins, Merkle
tree repair, projections). Two generations coexist on the wire:

* **v1** (`Client`/`Session`, `ConnectionHost`, `HostConfig`/`ClientConfig`): single-namespace
  `COMMIT_FETCH`/`COMMIT_PUSH` over `DagSyncPlan{CommonAncestor, LocalOnly, RemoteOnly}`.
* **v2** (`SyncV2`, `V2Host`, `V2HostConfig`/`V2ClientConfig`): `SYNC_HELLO` negotiates; refs
  exchange (`wire.NamespaceRefs{Namespace, Branches, Tags, HistoryFloorHex, Shallow, ResolutionHash, Access}`);
  paged want/have `FetchRequest`->`PackPage`; `RefUpdate` push; snapshot join; projections with
  optional write-back. The listener decides v1 vs v2 from the **first frame**
  (`server.peerSyncConnHandler.handle`). `V2Host.HandleFrame` serves `REFS`, `FETCH`, `REF_UPDATE`,
  `SNAPSHOT_FETCH`, `GRAFT_PUSH`, `TREE_NODES`, `OBJECT_FETCH`, `DOC_FETCH`, `PROJECT_FETCH/WRITE`,
  `META_VIEW`, `HOME_REQUEST`, `SQUASH_HEAD` (0x40), `BLOB_FETCH` (0x42), `ATTACHMENT_UPLOAD` (0x44)
  and `REFS_WATCH` (0x50; the host pushes coalesced `REFS_CHANGED` 0x51 under the watch's
  correlation id, `watch.go`, fed by `V2HostConfig.WatchHead`). Kotlin `kdb-peer-sync` now speaks v1
  and the core of v2 (see section 7).

Since 2026-10-11 the v2 path also carries:

* **Attachment blobs** (`attachments.go`): commits carry content-hash refs; a pull fetches missing
  blobs for each page (`v2Conn.fetchBlobs` -> `BLOB_FETCH`), a push uploads them first
  (`uploadBlobs` -> `ATTACHMENT_UPLOAD`); the host side keeps `hostUploads` per session. A peer that
  did not negotiate attachments is refused attachment commits (`AttachmentsUnsupportedError`).
* **Squash catch-up** (`squash.go`): `CatchUpRoute` (`CatchUpReplay` default, `CatchUpSquash`,
  `CatchUpAuto`; `ParseCatchUpRoute`), per-pattern `CatchUpOverride{Pattern, Route}` in
  `V2ClientConfig.CatchUp/CatchUpNamespaces`. A node far behind asks `RepairSession.SquashHead` and
  jumps to the peer's head via `InstallSquash(env, from, target, diff, bodies)`; fast-forward only.
  A namespace refuses to serve or take a squash when `IngestEnv.SquashRefused` is set
  (`server.CatchUpPolicy`).
* **Bounded sessions**: `pullGate` (`pull_gate.go`) makes pulls of one namespace from several peers
  take turns and share received tips; `foldFrontier` bounds haves/`ReceivedTips` to the history's
  width (`MaxPendingTips`); `pageSpool` stages snapshot and pushed-graft pages on disk (unlinked
  temp file under `IngestEnv.StagingDir`); `V2HostConfig.MaxGraftPushBytes` caps a pushed graft;
  `AdoptEveryBytes` adopts a long push/pull of main a chunk at a time; a large snapshot is served
  from an on-disk id-sorted copy of its bodies (`snapshot_source.go`, `sortedBodies`, built from
  `storage.TreeBodyStreamer`).
* **Resolution chain version 2** (`merge_semantics_v2.go`): `ResolutionChain.Version`
  (`ChainVersionOriginal`=1, `ChainVersionMergeBases`=2) selects true merge bases and per-field
  field-merge by origin; `EffectiveVersion/Normalize/Supported/ValidateRules`,
  `ErrUnsupportedChainVersion`.

The core is transport-independent and operates on an **`IngestEnv`** (everything the receiving side
needs, supplied by the server):

```go
type IngestEnv struct { DAG *dag.InMemoryCommitDag; Storage storage.Adapter; NamespaceID string
  Node LocalNode; Persist func(document.Commit) error; PersistAsync func(...) (wait func() error, err error)
  ApplyToStorage bool; Resolution ResolutionOptions; Conflicts *ConflictQueue; Peer, Self string
  CheckCommit func(document.Commit) error; CanInstallSnapshot func() error
  SnapshotInstalled func(document.Commit) error; ShallowRootsChanged func([]codec.Hash) error
  GraftRecorded func(root codec.Hash) error
  SquashInstalled func(root document.Commit, changed map[codec.Hash]string) error; SquashRefused string
  MaxClockSkew time.Duration; StagingDir string
  Attachments *attachstore.Store; AttachmentTableSeeded func(packed []byte) error }
type LocalNode interface { Exclusive(fn func() error) error; Advanced(step AdvanceStep) error }  // server.serverLocalNode
type BootstrapNode interface { ExclusiveBootstrap(fn func() error) error }  // optional on a LocalNode; snapshot install
type NamespaceProvider interface { List() []string; Env(ns string, create bool) (IngestEnv, error) } // server.peerNamespaceProvider
func Ingest(env, commits, incomingHead) (IngestResult, error)           // store + fast-forward / merge / conflict
func AdoptFastForward(env, incomingHead) (IngestResult, error)          // adopt an already-stored range
func ResolveDivergence(d, store, ns, localHead, incomingHead, ResolutionOptions) (CommitPushOutcome, error)
func InstallSnapshot(env, fetch) (document.Commit, error);  func Graft(env, root, fetch) (GraftResult, error)
func InstallSquash(env, from, target, diff, bodies) error
func Deepen(env, *RepairSession, root) (DeepenResult, error);  func DeepenAll(env, *RepairSession) ([]DeepenResult, error)
func ApplyRef(env, kind, name, proposed) (RefUpdateResult, error)
func SyncProjection(w, transport, ProjectionConfig) (ProjectionResult, error)
```

`LocalNode` is the key inversion: `Exclusive` runs peer ingest under the *same* write gate local
clients use (`serverLocalNode.Exclusive` -> `writeGate.acquire`), and `Advanced` lets the server
update indexes/unique keys/storage for ops that arrived from a peer (`AdvanceStep.HasApplied/
EachApplied`). A snapshot install probes for `BootstrapNode`; `serverLocalNode.ExclusiveBootstrap`
takes the gate as a *long hold* (`writeGate.beginLongHold`), so local writers are refused at once
with a retryable `BusyError` instead of queueing to their deadline.

Conflict handling types:

| Type | Role |
|---|---|
| `ResolutionChain{Rules []ResolutionRule, AllowUnrelated, Version}` | Ordered strategy list per namespace, content-hashed (`Hash()`), stored in the meta namespace and advertised in `NamespaceRefs.ResolutionHash`. Rule kinds: `RuleSourcePriority`, `RuleValidity`, `RuleFieldMerge`, `RuleLastWrite`, `RuleQueue`, `RuleProcedure`, `RuleAuthority`; `Pending` modes `PendingHold`, `PendingProvisional`. |
| `ResolutionOptions` | Runtime form of the chain: `Policy transaction.ConflictPolicy`, `Choose`, `Resolver transaction.ConflictResolver`, `Valid func(body) bool`, `Procedure func(name, sourceHash, ProcedureConflict) (Settlement, decided, error)`. |
| `Settlement{Body *string; Fork *ForkDoc}` | Outcome for one document; a `ForkDoc` preserves the losing version under a derived id (`ForkID`). |
| `ConflictQueue` (`NewConflictQueue(dir)`) | Durable per-namespace queue of `ConflictEntry` (kinds `ConflictDivergence`, `UniqueDuplicate`, `ForeignGroupPart`, `WriteBack`, `Provisional`, `UnrelatedHistory`); `Record/OnRecord/MarkDelivered/Get/List/Remove`. Resolved through `ResolveConflict(env, id, map[UUID]Choice)`. |
| `HeadUpdate` / `CommitPushOutcome` | `HeadFastForward | HeadAlreadyAncestor | HeadDiverged`; `OutcomeNoOp | FastForwarded | Merged | Conflict`. |
| `RepairSession` | Open v2 connection used for `Diff` (Merkle `TreeNodes`), `Fetch` (objects), `FetchDocs` (proven doc fetch with `TreeProof`), `FetchAncestors`/`FetchAncestorsPaged`, `RequestHome`, `SquashHead`. |

Errors: `peersync.Error{*kdberr.TransportErr}`, `RemoteError{Code wire.ErrorCode, Message}`,
`ClockSkewError`, `TreeMismatchError`, `UnrelatedHistoryError`, `NotGrantedError`,
`NamespaceMismatchError`, `UnsupportedFrameError`, `AttachmentsUnsupportedError`,
`ShallowBoundaryError` (pull needs a parent behind this node's snapshot root and fetching its state
failed), `PushBelowBoundaryError` (-> `SHALLOW_BOUNDARY`), `PeerBoundaryError` (push could not supply
state behind the peer's root), `ErrUnsupportedChainVersion`, sentinels `ErrNotEmpty`, `ErrNotShallow`,
`ErrPeerLacksHistory`, `ErrUnrelatedNotAllowed`, `ErrConflictNotFound`, `ErrUnproven`.

![Diagram 11](diagrams/class-11.svg)

<details><summary>Mermaid source</summary>

```mermaid
classDiagram
  class Client {
    <<interface>>
    +Connect(ClientConfig) Session
    +Disconnect()
  }
  class Session {
    <<interface>>
    +PullMissing() Result
    +PushCommits(commits) int
    +SyncBidirectional() Result
  }
  class Host {
    <<interface>>
    +Start(HostConfig)
    +HandleFrame(frame) bytes
  }
  class V2Host {
    +HandleFrame(frame) bytes
    -uploads hostUploads
    -watch refsWatch
  }
  class NamespaceProvider {
    <<interface>>
    +List() string[]
    +Env(ns, create) IngestEnv
  }
  class LocalNode {
    <<interface>>
    +Exclusive(fn)
    +Advanced(step)
  }
  class BootstrapNode {
    <<interface>>
    +ExclusiveBootstrap(fn)
  }
  class IngestEnv {
    +Attachments Store
    +SquashRefused string
    +StagingDir string
  }
  class RepairSession {
    +FetchDocs()
    +FetchAncestorsPaged()
    +SquashHead(ns, have) SquashHeadResult
  }
  class pullGate {
    -turn chan
    +share(tips)
  }
  class pageSpool {
    +add(docID, body)
    +each(maxBytes, limit, fn)
  }
  Host <|.. ConnectionHost
  Client <|.. defaultClient
  defaultClient ..> Session
  V2Host o-- NamespaceProvider
  V2Host ..> sortedBodies : large snapshot
  V2Host ..> pageSpool : graft push
  NamespaceProvider ..> IngestEnv : produces
  IngestEnv o-- LocalNode
  IngestEnv o-- ConflictQueue
  IngestEnv *-- ResolutionOptions
  IngestEnv o-- attachstoreStore : blobs
  ResolutionOptions o-- ResolutionChain
  ConnectionHost ..> IngestEnv
  RepairSession ..> IngestEnv : Deepen / Graft / InstallSquash
  v2Conn ..> pullGate : per namespace
  v2Conn ..> pageSpool : InstallSnapshot pages
  serverLocalNode ..|> LocalNode
  serverLocalNode ..|> BootstrapNode
  peerNamespaceProvider ..|> NamespaceProvider
```

</details>

#### 2.7.2 `replication`

**Purpose.** Background pull/push loops from this node to its configured peers.

```go
type PeerConfig struct{ Name, Addr string; Namespaces []string; Mode peersync.SyncMode /*SyncPull|SyncPush|SyncBoth*/
  Interval time.Duration; User, Password, Token *string; Credentials func() (auth.ConnectionContext, error)
  CreateLocal, PreferSnapshot bool; Filter string; WriteBack, ReadThrough, ScopedMeta, Deepen bool
  CatchUp peersync.CatchUpRoute; CatchUpNamespaces []peersync.CatchUpOverride /* catchup=, catchup-ns= */ }
func ParsePeer(spec string) (PeerConfig, error);  func ParsePeers(specs string) ([]PeerConfig, error)
type Config struct{ NodeID string; Local peersync.NamespaceProvider; Peers []PeerConfig; State *StateStore
  Transport func(PeerConfig) stream.Transport; TLS *core.TransportTlsSettings; Debounce, MaxBackoff, Timeout time.Duration
  MetaNamespace string; MetaView func(peer string, defs []wire.MetaDefinition) error
  Projections func(source, filter string, writeBack bool) (peersync.ProjectionTarget, error) }
func New(cfg Config) (*Replicator, error)
// (*Replicator) Start Stop OnLocalCommit SyncNow(name) Status() []PeerStatus SetPeerNamespaces AddNamespaces RemoveNamespaces
//               PeerFloor FetchBodies Compare OpenDocSession OpenRepairSessionTo WriteMetrics
type StateStore struct{...}   // JSON per-peer PeerState{Namespaces map[string]NamespaceState} under StateDir(dataRoot)
// NamespaceState{LocalMain, RemoteMain, LastSync, PendingTips, Pulled, Pushed, Overshoot, OpenConflicts, LastError}
```

`Replicator` owns one `peerLoop` per peer (goroutine each; `trigger chan struct{}` for debounced
`OnLocalCommit` wakeups; `cycleMu` allows one sync cycle per peer at a time; exponential backoff to
`MaxBackoff`; `nextAttempt`). Peers that bootstrap an empty namespace by snapshot sync first
(`snapshotFirst` chan closed once they finish), so the others pull deltas rather than the whole
history again. `NamespaceState.PendingTips` (built by `pendingFrontier` from a sync's
`ReceivedTips`, bounded by `peersync.MaxPendingTips`) resumes an interrupted pull; `Overshoot`
counts commits a peer re-sent that this node already held. `Replicator` satisfies `control.ReplicationSource`, `RepairSource`
and `DeepenSource`, and `embed.EmbeddedKdbRuntime.SetPeerRetentionFloor(floor)` is fed from
`PeerFloor` so log truncation never removes history a peer still needs.

#### 2.7.3 `syncnode`

**Purpose.** Assembles a multi-namespace, replicating node out of `embed.Host`, `server.NamespaceSet`
and `replication.Replicator` (the object behind `kdb-service --peer ...`).

```go
func Open(host *embed.Host, set *server.NamespaceSet, primary *server.KdbServerRuntime, cfg Config) (*Node, error)
// (*Node) Handler() Listen(addr, ...) Start() Replicator() Meta() MetaRuntime() Set() Conflicts() SyncNow(name)
//         StopSync() Handover/ForceHandover/RequestHome (single-home namespaces) CloseIdle() Prepare() Opener()
//         CloseInbound() Close()
type Config struct { Peers []replication.PeerConfig; DataDir string; TLS ...; Transport ...; PeerRetentionGrace time.Duration
  AuthorityExpiryInterval time.Duration; ConflictWebhook *server.ConflictWebhook; ScrubInterval time.Duration
  AuthorizePushedDocuments bool; HandoverPolicy server.HandoverPolicy; Idle *IdleConfig; Debounce, MaxBackoff, Timeout time.Duration
  MaxPeerConnections int /* -> KdbServerRuntime.PeerSyncMaxConnections; default 64 */ }
type IdleConfig struct{ MaxOpen int; IdleAfter, MinIdle, Interval time.Duration; OnClose func(*server.KdbServerRuntime) }
```

`Node` creates the meta namespace runtime (`server.MetaNamespace`; file-backed or in-memory) and
`server.MetaStore` (cluster-visible schema/index/resolution/procedure/home definitions), installs
`NamespaceSet.SetOpener` (lazy open of a namespace on first touch), the idle-close policy, the
conflict webhook/procedure loops, the scrub loop (`scrubLoop` goroutine) and attaches
`Replicator.OnLocalCommit` to each runtime's `CommitListener`. Its `mu`-guarded state tracks
listeners and live connections for orderly close. `CloseInbound()` (called by `service` before
closing runtimes, and by `Close`) sets `inboundClosed`, closes peer connections and listeners, and
waits on the `serving` `WaitGroup` so no inbound session is still applying when storage closes.

### 2.8 Runtime assembly: `embed`, `server`, `service`, `control`

#### 2.8.1 `embed`

**Purpose.** Build and own the storage + DAG stack of a namespace (`EmbeddedKdbRuntime`), the
process-wide `Host` that opens many namespaces over one data directory, the cross-namespace
transaction coordinator, and the maintenance scheduler.

```go
type EmbeddedKdbRuntime struct {            // public fields are what upper layers read directly
  Catalog string; DAG dag.CommitDAG; Storage storage.Adapter; Schema schema.KdbSchema
  DefaultNamespace string; WriteBaseVersion *codec.Hash; DataRoot string; ReadOnly bool
  /* unexported: attach attachmentState, deltaReader, shim, refresh, release, storageClose,
     closeHooks (closeHooksMu), maintain, checkpointNow, peerFloor atomic.Pointer, snapshot, txn, recovery */ }
func OpenMemoryRuntime(catalog, ns string, sch schema.KdbSchema) (*EmbeddedKdbRuntime, error)
func OpenFileRuntime / OpenReadOnlyFileRuntime / OpenFileRuntimeWithOptions(dataRoot, catalog, ns, sch, ...)
func OpenFileHost(dataRoot string, opts FileRuntimeOptions) (*Host, error)
// Host: Namespace / NamespaceWithOptions / ReopenNamespace / CloseNamespace / Namespaces / Adapter / Transactions / MemoryArbiter / Close
```

Methods on the runtime: `Maintain()`, `CompactHistory()`, `Refresh()` (read-only followers replay new
segments), `Close()`, `AssertWritable()` (`ErrReadOnly`), `ReclaimMode/SetReclaimMode`,
`HistoryMode/RetentionWindow/SetRetentionWindow/SetHistoryMode(to, reclaim) (HistoryModeSwitch, error)`,
`EligibleForCompaction()`, `AssertRetainsHistory`, `RestoreFromArchive(from,to)`,
`CanInstallSnapshot/PersistSnapshot/RecordShallowRoots/RecordGraft` (peer snapshot join),
`PersistSquash/PersistAttachmentSeed` (squash catch-up, attachment table seed),
`SetPeerRetentionFloor`, `AddCloseHook`, `CommitGraphStatus()`/`CompactCommitGraph()` (mapped
graph segments). `Maintain()` now also runs `CompactAttachments(DefaultNamespace)`.

Attachments (`attachments.go`, one `attachstore.Store` per namespace under `attachmentState.mu`):
`PutAttachment(ns, docID, r, PutAttachmentOptions{ID, Name, Mime}) (commit, AttachmentInfo, error)`,
`GetAttachment(ns, docID, AttachmentSelector, asOf) (*attachstore.Reader, AttachmentInfo, error)`,
`ListAttachments/CountAttachments/AttachmentInfoAt/AttachmentTableAt`, `RenameAttachment`,
`DeleteAttachment`, blob-level `PutAttachmentBlob/HasAttachmentBlob/ReadAttachmentBlob` (the
`transaction.AttachmentBlobSource` and `sql.AttachmentBlobs` seams), `CompactAttachments` (deltas
for replaced versions, GC of unreferenced blobs; `AttachmentMaintenance`), `SetAttachmentHistory
(AttachmentHistoryAll|AttachmentHistoryLatest)`, `SetAttachmentConfig(attachstore.Config)`,
`MigrateFileAttachments` (legacy file ops -> attachments; `FileMigration`). Selectors:
`AttachmentByID(id)`, `AttachmentByName(name)`. `AttachmentInfo{DocID, ID, Name, ContentHash, Size, Mime}`.
Open-time background work, both cancelled by close and re-run by the next open:
`startBackgroundCompaction` (an embedded runtime with no maintenance loop merges an SSTable backlog
in a goroutine; close merges at most `CloseCompactionBytes`) and `startVersionRefile` (re-files
document versions after `StorageOptions.DocumentCopies` changed). Free functions: `PutJSONDocument`, `DocumentAt`, `DocumentVersions`,
`RevertTo`, `DiffCommits`, `MaterializeCommit`, `MigrateHistoryMode/Strategy`, `LoadOrCreateNodeID`,
`ListNamespaces`, `ValidateNamespaceID`, `LockDataDir` (exclusive maintenance lock).

**Host lifecycle.** `OpenFileHost` takes the data-dir lock (two lock files, `.kdb.lock` shared
*attach* and `.kdb.write.lock` exclusive *writer*; `flock` on unix, `LockFileEx` on Windows
(`dir_lock_windows.go`); read-only hosts take only shared; `LockDataDir` takes the attach lock
exclusive for maintenance), builds the `PlatformIOShim` (optionally layering S3 replicas/archive via
`PrimaryWithReplicas`), opens the `TxnCoordinator` (decision log), and creates the `BudgetArbiter`
and `MultiplexAdapter`. `Namespace()` is idempotent per id (double-checked under `Host.mu`; a loser
of the race closes its entry), registers the adapter with the multiplexer and the engine with the
arbiter. `CloseNamespace`/`Close()` run each runtime's close hooks, close namespaces, then release
the lock.

Cross-namespace atomic commit:

```go
type GroupParticipant interface { Namespace() string; Runtime() *EmbeddedKdbRuntime; Lock() (unlock func(), err error)
  Prepare(tx document.Transaction) (PreparedPart, error); Fence(cause error) }
type PreparedPart interface{ Apply(message string) (document.Commit, error); Discard() }
func CommitGroup(coord *TxnCoordinator, parts []GroupPart) (GroupResult, func() error, error)
type TxnCoordinator struct{...}  // Begin(namespaces) (*TxnGroup, error); Epoch(); LocalHostID(); decision log (txn_decision_log.go)
type TxnGroup struct{ ID codec.UUID; Epoch uint64 }  // AddPart / Finish / Fail / Abandon / Done() <-chan / Err()
type GroupMarker struct{ Host string; Epoch uint64; Group codec.UUID; Parts []string }  // embedded in commit Message
```

`CommitGroup` rejects duplicate namespaces, sorts participants by namespace (a global lock order),
pins each base version, `Lock()`s every participant, calls `coord.Begin(namespaces)`, `Prepare`s each part
(stamping the group id as the transaction id; a failing `Prepare` discards earlier parts and abandons the
group), then `Apply`s each part; commits carry a `GroupMarker` in their message so recovery can tell a committed
group (decision log) from an abandoned one, and peer sync can flag `ConflictForeignGroupPart`.
`PersistingCommitDAG.barrier atomic.Pointer[TxnGroup]` records the latest group with a part in that log;
under `DurabilitySync`, later commits' durability waits chain on the group's decision (`chainOnBarrier`)
and fail with `GroupFailedError` if the group fails. `PublishObserver{PublishStarted/Finished}` lets the server delay stream
publication until the group is decided. Implementors of `GroupParticipant`: `server.serverParticipant`
and `driver.driverParticipant` (the test double is in `embed/group_commit_test.go`).

`PersistingCommitDAG` is the file-mode **decorator** for `dag.CommitDAG`: it delegates every call to
`*dag.InMemoryCommitDag` and adds durability (`Persist` sync, `PersistAsync` group-commit,
`Close`, `SetPersistListener`, `SetSealListener`, `SetAttachmentsCommitted` (ends upload holds once a
commit referencing them is durable), `AcknowledgesDurably()`); its `AppendCommit` appends in memory
and then persists synchronously (`Persist` = `PersistAsync` + `wait()`). The server deliberately bypasses
that method: it runs `transaction.Engine.Commit` against `Delegate()` and calls `PersistAsync` itself
so it can release the write gate *before* waiting for the fsync (section 1.3). It also forwards the
`HistoryNavigator` surface.

`MaintenanceScheduler` (`StartMaintenance(rt, MaintenanceOptions{Interval, Sweep, Busy func() bool,
MaxDefer, Reclaim, CompactTables, Logger})`): one goroutine ticks every `Interval`, runs
`rt.Maintain()` unless `Busy()` (deferred up to `MaxDefer` times, then forced), a faster `Sweep`
cadence when `ReclaimMode` says so, and `NotifySegmentSealed()` wakes it early. Runtime tuning:
`SetInterval/SetSweep/SetMaxDefer/ApplyReclaimMode`; stats via `Stats() MaintenanceStats`.

Typed errors in `embed` (plain `error` types, **not** `kdberr.Exception`): `TruncatedLogError`,
`SnapshotCheckpointMissingError`, `CommitOperationsUnavailableError`, `HistoryFeatureUnavailableError`,
`HistoryNotRetainedError`, `HistoryStrategyMismatchError`, `HistoryModeMismatchError`,
`HistoryModeStrategyConflictError`, `TreeHashMismatchError`, `GroupFailedError`, `GroupPartError`;
sentinels `ErrReadOnly`, `ErrCheckpointDeferred`, `ErrCommitLogClosed`, `ErrDecisionLogClosed`,
`ErrSnapshotNeedsFreshNamespace`, and for attachments `ErrNoAttachment`, `ErrNoSuchDocument`,
`ErrAttachmentNotRetained` (the server maps these three to wire `NOT_FOUND`).

![Diagram 12](diagrams/class-12.svg)

<details><summary>Mermaid source</summary>

```mermaid
classDiagram
  class Host {
    +Namespace(catalog, ns, schema) EmbeddedKdbRuntime
    +ReopenNamespace(...) ReopenResult
    +Close() error
  }
  class EmbeddedKdbRuntime {
    +DAG CommitDAG
    +Storage Adapter
    +Maintain() TruncationResult
    +SetHistoryMode(to, reclaim) HistoryModeSwitch
    +PutAttachment(ns, doc, r, opts) AttachmentInfo
    +GetAttachment(ns, doc, sel, asOf) Reader
    +CompactAttachments(ns) AttachmentMaintenance
    +CompactCommitGraph() error
    +AddCloseHook(f)
  }
  class attachmentState {
    -mu Mutex
    -stores map
    -history AttachmentHistory
  }
  class backgroundCompaction {
    -cancel CancelFunc
    -done chan
  }
  class commitLogWriter {
    -reqs chan logRequest
    -done chan struct
  }
  class TxnCoordinator {
    +Begin(namespaces) TxnGroup
  }
  class GroupParticipant {
    <<interface>>
    +Prepare(tx) PreparedPart
    +Lock() unlock
  }
  class PreparedPart {
    <<interface>>
    +Apply(msg) Commit
    +Discard()
  }
  class MaintenanceScheduler {
    +Stop()
    +NotifySegmentSealed()
    +Stats() MaintenanceStats
  }
  Host *-- TxnCoordinator
  Host *-- BudgetArbiter
  Host *-- MultiplexAdapter
  Host *-- EmbeddedKdbRuntime : per namespace
  EmbeddedKdbRuntime o-- PersistingCommitDAG
  PersistingCommitDAG *-- commitLogWriter
  PersistingCommitDAG o-- TxnGroup : barrier
  TxnCoordinator ..> TxnGroup
  GroupParticipant ..> PreparedPart
  MaintenanceScheduler o-- EmbeddedKdbRuntime
  EmbeddedKdbRuntime *-- attachmentState
  attachmentState *-- attachstoreStore : per namespace
  EmbeddedKdbRuntime ..> backgroundCompaction : open, cancelled at close
  EmbeddedKdbRuntime ..> versionRefile : open, cancelled at close
  PersistingCommitDAG ..> attachmentState : SetAttachmentsCommitted
  serverParticipant ..|> GroupParticipant
  driverParticipant ..|> GroupParticipant
```

</details>

#### 2.8.2 `server`

**Purpose.** The network-facing node: per-namespace `KdbServerRuntime` (commit path, sessions,
admission, locks, conflicts), `NamespaceSet` (many namespaces + cross-namespace commit), the
listeners (SQL wire over TCP/WS, stream, peer sync, admin), and the governance layer.

`KdbServerRuntime` (`server_runtime.go`), key public fields: `Runtime *embed.EmbeddedKdbRuntime`,
`TransactionEngine`, `UpsertEngine transaction.Engine`, `DocumentLocks *transaction.LockManager`,
`AuthEngine auth.Engine` (default `auth.AllowAll`), `CommitListener func(ns, commit)`, `WriteTimeout`
(default 5s), `UniqueKeys`, `Conflicts *peersync.ConflictQueue`, `ReadThrough`, `SessionWait`,
`ProjectionOf/ProjectionWriteBack/ProjectionFilter` (read-only projection replicas), `Meta`,
`Namespaces`, `NodeID`, `PeerCreateOnPush`, `PeerAuthorizeDocuments`, `PeerSyncMaxConnections`
(default `DefaultPeerSyncMaxConnections` = 64), `HandoverPolicy`, `MaxConnections`,
`MaxQueuedReplyBytesPerConn` (default `DefaultMaxQueuedReplyBytes` = 64 MiB), `ReplyWriteTimeout`.
Methods group as:

| Group | Methods |
|---|---|
| Writes | `Commit(ns, tx, sessionID, principal)`, `Upsert`, `Replay`, `PutJSON` (with `Expect{ContentHash, Absent}` preconditions -> `PreconditionFailedError`), `ReadForUpdate`, `CommitImmediate(principal, sessionID, build func(head) ([]Op, error))` (ops built under the write gate), `ExecImmediateUpdate` (queue claim: `UPDATE ... ORDER BY ... LIMIT ... RETURNING`, `immediate_update.go`), `ExecImmediateInsert` (`INSERT ... ON CONFLICT ... DO UPDATE` / `RETURNING`, `immediate_insert.go`) |
| Reads | `GetDocument`, `ReadDocument`, `SQLEngine()`, `SQLIndexProvider()`, `HeadTree`, `Schema()/SetSchema[Checked]`, `MaxResultRows/SetMaxResultRows`, `MaxResultBytes/SetMaxResultBytes` (default `DefaultMaxResultBytes`, 1 MiB under the frame cap), `MaxQueuedReplyBytes` |
| Governance | `SetMemoryLimit`, `SetMemoryBudget`, `Admission()` (`MaxQueryBytes`), `MemoryZone()`, `ShareGovernanceWith(primary)`; free `CachePoolBytes(cacheMB)` sizes the host cache pool from the cgroup limit or RAM (an eighth, at most 4 GiB) |
| Lifecycle | `Retain()/Release()` (ref count, `atomic.Int32`; `ServerRuntimeRegistry.GetOrOpen/Release` evicts at zero), `BeginDraining()`, `WaitForWritesToDrain(timeout)`, `ResumeAfterDrain`, `ReopenWith(...)`, `OpenIndexes`, `RebuildUniqueKeys` |
| Distribution | `PeerSyncNode()`, `PeerNamespaces()`, `PeerIngestEnv`, `SetHome/HomeOf/Handover/ForceHandover`, `ResolveConflict/DismissConflict/ResolveAll/AckConflict`, `SetResolutionChain/ResolutionChainOf/AdvertisedResolutionHash`, `SetProcedure*`, `SetCatchUpPolicy/CatchUpPolicyOf` (`CatchUpPolicy{RefuseSquash, Reason}`, a replicated meta definition), `WatchHead(fn) cancel` (REFS_WATCH), `Deepen`, `ShallowRoots`, `Scrub`, `ProjectionTarget`, `ApplyWriteBack` |
| Expiry | `SetDocumentExpiry`, `DocumentExpiry`, `SweepExpiredNow`, `ExpirySummary` (background sweeper; an ordered expiry index, `expiry_index.go`, answers which documents expired instead of reading every body) |

The write path and its gates are in section 1.3. The gate chain, outermost first:
`admitWrite` (cheap boolean checks) -> `Admission.Acquire` (memory grant, `semaphore.Weighted`
sized `effectiveLimit - reserve - nonGrantedFloor`, per-`OpClass`) -> `writeGate.acquire`
(single-flight, bounded queue; refused at once during a *long hold*, a peer snapshot install) ->
`transaction.Engine`. Since #339 an immediate UPDATE finds candidates before the gate
and, under it, re-checks only documents that `changeLog` (a ring of recent commits' written ids,
`change_log.go`) says changed since; keyed UPDATEs skip candidate search; index-store bookkeeping
runs after the gate is released.

Governance types:

| Type | Role |
|---|---|
| `MemoryGuard` | Samples `runtime/metrics` (total minus released heap) every 200 ms into a ring; exposes `CurrentZone()` lock-free: `ZoneNormal/Elevated/High/Critical`; `SetObserver`, `Stop`. |
| `Admission` | `Acquire(ctx, OpClass, estimateBytes) (*Grant, error)`; `OpClass`: `ClassPointRead/Scan/Write/Replication`; counts decisions in `AdmissionStats` (atomics); keeps a `RescueReserve`; errors `MemoryPressureError{Zone, Class, RetryAfterMs}`, `ResourceExhaustedError{Reason, EstimateBytes, CapacityBytes}`. |
| `CostModel` | Learns per-shape scan/point-read cost (`EstimateScan(ScanEstimateInput)`, `Observe*Actual`, `AccuracyP95`, `SnapshotState/RestoreState`, persisted by `service` to a state file). |
| `writeGate` | Two channels (`queued` bounded, `running` capacity 1) + EWMA service time (`serviceNanos`, shift 3); errors `BusyError`, `DeadlineExceededError`. `beginLongHold/endLongHold` (`holdMu`, `holdErr`, `holdWake` closed to wake queued writers) make `acquire` refuse during a snapshot install; `acquireThroughHold` is for internal drains. `KdbServerRuntime.WriteQueueDepth()`. |
| `AbortWatchdog` | Crash-only supervisor (`NewAbortWatchdog(runtime, listener, abortAfter)`): on *sustained* critical memory pressure it `BeginDraining`s (typed `UnavailableError` to new work), flushes and closes storage via the normal close path, and exits with `AbortExitCode = 75` (EX_TEMPFAIL) so a process supervisor restarts it. Never restarts itself. |

Sessions: `SessionManager` (per connection; `Begin/BeginOn/Get/End/ClearPending/PendingBuilder/All`),
`KdbSession{ID SessionID, NamespaceID, ReadConsistency (ReadCommitted|ReadYourWrites|Snapshot), Pending
*transaction.Builder, Principal}` with `TrackLease/UntrackLease/ClearLeases` so leases die with the
connection. Session tokens (`session_token.go`) let a client demand `MinCommit`, giving
`ReplicaBehindError` -> `BUSY` on lagging replicas.

Listeners: `Listener{ln, cancel, done}` with `Addr()/Close()`; `ListenSqlWire[TLS]`,
`ListenSqlWireWS[TLS]`, `ListenStream[TLS]` (returns `*StreamHub`), `ListenPeerSync[TLS]`,
`NewAdminServer(addr, rt)` (health/metrics, `SetReady`). `ListenPeerSync` caps accepted
connections at `PeerSyncMaxConnections`. A connection is a `FrameConn` served by
`FrameHost.Serve`; `sqlWireConnHandler.run` decodes frames, handles the handshake inline, then
dispatches each later frame on its own goroutine, bounded by `MaxInFlightFrames = 64` (a buffered
channel as semaphore), with a per-session FIFO ticket (`sessionTicket`, `sessionMu` over
`sessionTails`) preserving order of frames on one session and a `sendMu` serialising replies;
`dispatchRecovering` turns a handler panic into an INTERNAL error reply. A plain `SELECT` on a
non-`SNAPSHOT` session takes no ticket (`readsNoSessionState`), so one session's queries run
concurrently. The handshake agrees wire features (`wire.NegotiateWireFeatures`: `binary-frames`,
`binary-frames-2`, `zstd-frames`, `attachments`, `attachment-proofs`) and stores the agreed codec in
`negotiated atomic.Pointer[wire.DefaultCodec]`. Replies are bounded: `replyBudget` (a `sync.Cond`
over queued reply bytes) stops reading new frames when `MaxQueuedReplyBytesPerConn` is queued, and
a reply that cannot fit a frame or `MaxResultBytes` becomes `ReplyTooLargeError`
(`reply_bounds.go`). Paged SELECTs carry a continuation token (`sql_paging.go`: pinned commit,
offset or keyset `after` id, statement digest).

Attachment messages (`attachments_wire.go`): `handleAttachmentUpload` (`ATTACHMENT_UPLOAD`
0x44/0x45; uploads belong to the runtime, `KdbServerRuntime.attachUploads`, so a client resumes
from `Received` on a new connection; idle uploads are swept), `handleAttachmentGet` (0x46 -> paged
`ATTACHMENT_DATA` 0x47; each read pins its version per connection in `attachReads` until its last
page, `Close` or disconnect; `attachmentSuperseded` flags a newer version), `handleAttachmentList`
(0x48/0x49). `errCodeFor` maps `embed.ErrNoAttachment/ErrNoSuchDocument/ErrAttachmentNotRetained` ->
`NOT_FOUND`, `attachstore.ErrTooLarge` -> `RESOURCE_EXHAUSTED`, else `classifyError`. Projection
write-back carries attachment sets (`writeback_attachments.go`).

Multi-namespace: `NamespaceSet{WriteTimeout}` (`mu RWMutex` over `runtimes` and `system`
maps, `openMu` serialising lazy opens through `SetOpener`, `known` for lazily-listed names),
`Add/AddSystem/Get/Resolve/Namespaces/Runtimes/Snapshot`, `CommitAcross([]NamespaceTransaction)
(CrossNamespaceResult, error)` (wraps `embed.CommitGroup` with `serverParticipant`s),
`StartIdleClose(IdlePolicy)`. `MetaStore` keeps cluster definitions in `MetaNamespace`
(`RecordSchema/RecordIndex/SetResolution/SetProcedure/SetCatchUp/AssignHome/View/ApplyTo/ReconcileAll`;
indexes from one definition set are built in one scan).

Other server components: `RegistryIndexProvider` (index fan-out + hybrid `Search` via
`fusion.Fuse`; also `ExactCount`, `IndexKeyGroups`, `UnindexedDocs`, `CompositeBTrees` for the SQL
top-k/count paths, `Flush`, `Close`; restored index snapshots are caught up to head by tree diff,
`index_catchup.go`), `StreamHub` fan-out sharing (`stream_fanout.go`: a `fanoutCache` LRU of a
commit's ops and per-filter views, shared across subscribers), `headWatchers` (`WatchHead`), `SearchProvider` interface (`Search(ctx, wire.SearchMessage)`; `ErrSearchNotConfigured`),
`ReadThrough` (lazy remote fetch for shallow replicas, LRU of `Capacity`), `Home/NotHomeError/
StaleFenceError/HandoverPolicy` (single-home namespaces with fencing tokens), `ConflictWebhook`
(HMAC-signed `ConflictNotification` POSTs), `ConflictProcedure` (runs authority procedures),
`ScrubReport` (verify document bodies against tree hashes and repair from a peer via `BodyFetcher`).

![Diagram 13](diagrams/class-13.svg)

<details><summary>Mermaid source</summary>

```mermaid
classDiagram
  class KdbServerRuntime {
    +Runtime EmbeddedKdbRuntime
    +TransactionEngine Engine
    +DocumentLocks LockManager
    +AuthEngine Engine
    +Commit(ns, tx, session, principal) Commit
    +ExecImmediateUpdate(upd, qctx, principal, session, check) ImmediateUpdateResult
    +ExecImmediateInsert(ins, qctx, principal, session, check) ImmediateInsertResult
    +WatchHead(fn) cancel
    +SetCatchUpPolicy(p)
    +BeginDraining()
    +Retain() / Release()
  }
  class NamespaceSet {
    +Add(ns, rt)
    +Get(ns) KdbServerRuntime
    +CommitAcross(txs) CrossNamespaceResult
  }
  class writeGate {
    -queued chan
    -running chan
    -holdErr BusyError
    +beginLongHold(err)
  }
  class sqlWireConnHandler {
    -negotiated DefaultCodec
    -sessionTails map
    -replies replyBudget
    -attachReads attachmentReads
    +handleAttachmentUpload(msg)
    +handleAttachmentGet(msg)
    +handleAttachmentList(msg)
  }
  KdbServerRuntime *-- writeGate
  KdbServerRuntime *-- changeLog
  KdbServerRuntime *-- headWatchers
  KdbServerRuntime *-- attachmentUploads
  KdbServerRuntime o-- CatchUpPolicy
  sqlWireConnHandler *-- replyBudget
  sqlWireConnHandler *-- attachmentReads
  StreamHub *-- fanoutCache
  KdbServerRuntime *-- Admission
  Admission o-- MemoryGuard
  Admission o-- CostModel
  KdbServerRuntime o-- ConflictQueue
  KdbServerRuntime o-- MetaStore
  NamespaceSet o-- KdbServerRuntime
  ServerRuntimeRegistry o-- KdbServerRuntime : refcounted
  SessionManager o-- KdbServerRuntime
  SessionManager *-- KdbSession
  Listener ..> FrameHost
  FrameHost ..> sqlWireConnHandler
  sqlWireConnHandler *-- SessionManager
  sqlWireConnHandler o-- KdbServerRuntime
  KdbServerRuntime ..> StreamHub : CommitListener
  serverLocalNode o-- KdbServerRuntime
  serverLocalNode ..> writeGate : ExclusiveBootstrap long hold
  peerNamespaceProvider o-- NamespaceSet
  serverParticipant o-- KdbServerRuntime
  RegistryIndexProvider o-- KdbServerRuntime
```

</details>

Server errors (all plain error types, classified in section 4): `BusyError`, `DeadlineExceededError`,
`UnavailableError`, `MemoryPressureError`, `ResourceExhaustedError`, `ConflictError{Report, RetryAfterMs}`,
`SchemaError{Violations}` (`HasUniqueViolation()`), `AuthorizationError{Cause}`, `UnsupportedError`,
`NotHomeError{Namespace, Home}`, `StaleFenceError`, `ReplicaBehindError`, `PreconditionFailedError`,
`ProjectionReadOnlyError`, `HandoverRefusedError`, `ErrResolutionStale`, `ScrubUnrepairedError`,
`ReplyTooLargeError` (-> `RESOURCE_EXHAUSTED`),
`CrossNamespaceError` (= `embed.GroupPartError`), sentinels `ErrUnknownNamespace`,
`ErrSearchNotConfigured`, `ErrReadThroughForbidden`, `ErrProjectionWriteOp`.

#### 2.8.3 `service`

`service.Main()` is the whole `kdb-service` binary (`cmd/kdb-service`). Subcommands first
(`runSubcommand`): `kdb-service user create|role|assign|list` and `kdb-service token
create|revoke|list` edit the durable auth registry while the service is stopped. Otherwise it resolves
`config.ServiceSettings` (defaults < config file < env < flags), then assembles in order: logger and
GC memory limit (`ApplyGoMemoryLimit`), `memtune.Start` (GC paced to live heap + `--gc-headroom-percent`
once the live heap passes 256 MiB), storage (`embed.OpenMemoryRuntime` or `embed.OpenFileHost` +
`Host.Namespace`, the host's cache pool sized by `server.CachePoolBytes(--cache-mb)`; attachment and
`--document-copies` settings copied into `StorageOptions`), `server.NewKdbServerRuntime(rt)`, memory
governance and `CostModel` state restore, auth (`embed.OpenFileAuthRegistry` ->
`auth.NewRegistryAuthEngine`), `server.NewNamespaceSet(host.Transactions())`, `syncnode.Open`, the
listeners (SQL, WS, peer, stream, optional gRPC via `RegisterGRPCListener`), `AbortWatchdog`,
`AdminServer`, `control.New` (refused on a non-loopback `--control-addr` without `--rbac` unless
`--control-open`), `adoptRestoredDefinitions` (definitions a restored backup carried, into the meta
store), per-namespace `embed.StartMaintenance`. Namespaces opened on demand through the set's opener
also get a maintenance scheduler (`maintenanceRegistry.replace`) and, when they match
`--expire-namespaces`, the `--expire-field` policy. Then it blocks on `SIGINT/SIGTERM` and runs
drain: `BeginDraining` -> `WaitForWritesToDrain(DrainTimeout)` -> close listeners ->
`syncnode.Node.CloseInbound` -> close runtimes. The `GRPCListenerFactory` indirection keeps the
gRPC dependency out of the default build (`GRPCLinked()`).

#### 2.8.4 `control`

HTTP/JSON control plane and embedded UI (`control/ui`). `control.New(Options) (*Server, error)`;
`Options` carries `Addr`, `Runtime`, `Namespaces NamespaceSource`, `Settings []config.SettingDescriptor`,
`AllowWrites`, `ServeUI`, `StagingDir`, `BackupDir`, `AllowSettingsPersist`, `AllowPromotion`,
`RequestRestart`, `Replication ReplicationSource`, `Maintenance MaintenanceSource`, `Reopener
NamespaceReopener`. Ports it defines (satisfied by `service`/`syncnode`/`replication`):
`NamespaceSource{Namespaces(); Runtime(ns)}`, `ReplicationSource{Status(); SyncNow(name)}`,
`RepairSource{FetchBodies; Compare}`, `DeepenSource{OpenRepairSessionTo}`,
`NamespaceReopener{ReopenNamespace(ns, edit func(*embed.StorageOptions)); ReopenableNamespaces()}`,
`MaintenanceSource{Schedulers()}`. Authorisation: three wrappers (`nsRead`, `nsWrite`, `adminRead`)
over `auth.AdminAction`/document actions, scope constant `AdminScope`. ~76 routes under `/v1/...`
cover namespaces/status, log/commits/diff/refs (branches, tags), documents and history, SQL,
indexes/schema, retention (history mode, compact, restore from archive), integrity/verify,
checkpoints, backups/restore staging/promotion, conflicts/resolution/procedures/home and
catch-up policy (`GET/PUT /v1/ns/{ns}/catchup`) (distribution),
settings (read, drift, `PATCH` with live apply or namespace reopen per `config.Mutability`),
ops (runtime, locks, metrics, drain) and an SSE event stream. `Server.PublishCommit` feeds the
events. State is per-server `sync.Mutex`/`RWMutex`; restore runs in one goroutine (`runRestore`).

### 2.9 Edges: `client`, `driver`

#### 2.9.1 `client` (Go SDK over the wire)

```go
func Connect(ctx, addr, token string) (*Client, error);  func ConnectWithOptions(ctx, addr, token string, ConnectOptions) (*Client, error)
type ConnectOptions struct{ TLS *core.TransportTlsSettings; Namespaces []string; Transport stream.Transport
  JSONOnly bool /* offer no wire features: stay on the JSON envelope */ }
// (*Client): PutJSON ReplaceJSON GetJSON GetJSONWithHash Upsert Commit AppendEvent PutIfAbsent ReplaceIf ReplaceIfPresent
//            CompareAndSwap History Revert AcquireLock RenewLock ReleaseLock CommitAcross
//            Query QueryRaw QueryRawPages QueryValues QueryValuesPages Exec (-> rows affected) ExecReturning
//            Search NewSession Begin BeginTx WireFeatures Close
//   attachments: PutAttachment UploadAttachment ResumeUpload OpenAttachment OpenAttachmentRange ReadAttachment
//                ReadAttachmentAt ListAttachments RenameAttachment DeleteAttachment
type Tx struct{...} // Exec Query QueryRaw QueryValues Commit Rollback;  type Session struct{...} // Token Resume ...
type Router struct{...}   // NewRouter(ctx, addr, token, opts): follows NotHomeError to the home node; Primary/PutJSON/GetJSON/Commit/Upsert
type Transaction struct{ Namespace, BaseVersion string; Writes []DocWrite; Deletes []string }
type AttachmentRef struct{ ID, Name string }   // ByAttachmentID / ByAttachmentName
type AttachmentReader struct{...}              // io.ReadCloser over ATTACHMENT_DATA pages; Info() Commit() Superseded()
// OpenAttachmentRange(ctx, ns, docID, ref, asOf, offset, length) - verified range (attachment-proofs); *AttachmentRangeError
```

`Client` is one connection, one `readLoop` goroutine, a `pending map[int]chan wire.Message`
keyed by correlation id under `mu`, per-namespace session state (`namespaceState{dml RWMutex,
sessionID, head}`) under `nsMu`. It keeps `head` per namespace so `Commit` can send a base version;
`dml` keeps an `Exec` and its auto-commit back to back on the session. A finished `Tx` returns its
server session to `idleTxSessions` for the next `Tx`. The handshake offers every
`wire.SupportedWireFeatures` unless `JSONOnly`; the agreed codec sits in `encoder
atomic.Pointer[wire.DefaultCodec]` and `features`. The Go client has **no** per-namespace pool of
write sessions: that landed (#337) in the TS `@kdb/client` only (`ConnectOptions.sessionsPerNamespace`,
default 8), as did loopback zstd decline (`ConnectOptions.compression = "auto"`). Errors map from
wire codes (section 4.4).

#### 2.9.2 `driver` (`database/sql`)

`sql.Register("kdb", &Driver{})`; `driver.Open(raw)` returns `*sql.DB`. URL (`ParseURL`/`ParseDSN`)
`kdb://...` selects `ModeMemory` (shared by name in a process-wide `registry`, `ClearMemoryRegistries`)
or `ModeFile` (`DataRoot`, `Catalog`, `NamespaceID`, `ReadOnly`; Windows drive paths accepted).
Embedded, in-process: no network. SQL attachment statements work through it because the embedded
runtime is the `sql.AttachmentBlobs` source.

Internals: `registry{mu, dbs map[string]*database}`; `database{host *embed.Host, coord
*embed.TxnCoordinator, namespaces map[string]*namespace, refs}`; `namespace{rt, d, persister, engine
transaction.Engine, uniques, write sync.Mutex, schemaMu, sqlEngine, fenced atomic.Pointer[error]}`;
`conn{parsed, db, ns, tx *connTx}`; `connTx`/`txWrites` stage writes and commit through
`commitPart` / `driverParticipant` (so a `Tx` touching several tables-as-namespaces uses
`embed.CommitGroup`); `stmt`, `rows`, `txHandle`. Errors: `ErrConflict`, `ErrUnknownNamespace`,
`ConflictError{Namespace, Documents}` (with `Is`), `SchemaError`. The Kotlin analogue is `kdb-jdbc`.

### 2.10 Security and configuration: `auth`, `config`

#### 2.10.1 `auth`

```go
type Authenticator interface{ Authenticate(ctx, Credentials) (Principal, error) }
type Authorizer interface{ Authorize(ctx, Principal, Action) error }
type Engine interface{ Authenticator() Authenticator; Authorizer() Authorizer }
var AllowAll Engine                    // dev/tests; default of KdbServerRuntime.AuthEngine
func NewRegistryAuthEngine(store *RegistryAuthStore) Engine
type Principal struct{ ID string; Roles map[string]struct{}; Claims map[string]string }
type Action interface{ isAction() }    // SessionBeginAction SqlExecAction{ReadOnly} TxCommitAction PeerSync/Pull/PushAction
                                       // ConflictResolveAction StreamSubscribeAction DocumentWrite/Delete/ReadAction AdminAction
type ResourcePath struct{ Database, Collection, DocumentID string }
type RegistryAuthStore struct{...}     // users + roles stored as documents in UsersNamespace / RolesNamespace
```

`NewRegistryAuthStore(userDag, roleDag dag.CommitDAG, store storage.Adapter)` keeps users
(`UserRecord{ID, PasswordHash, PasswordSalt, Roles}`, `HashPassword`/`VerifyPassword`) and roles
(`RoleRecord{Name, Grants}`) as ordinary documents in two system namespaces (under
`embed.FileAuthRegistry`). Grants are permission strings matched by `PermissionMatchesPath`
against `ResourcePath.CandidatePaths()`. Helpers `AuthorizePeer` / `PeerDirections` express
pull/push rights. Errors `AuthenticationError`, `AuthorizationError` embed `*kdberr.TransportErr`.

Revocable access tokens (`tokens.go`): `RegistryAuthStore.CreateToken(user, label, ttl) (token,
TokenRecord, error)`, `RevokeToken(id)`, `ListTokens()`; `TokenRecord{Kind, TokenID, User, Label,
SecretSHA256, CreatedAt, ExpiresAt, Revoked}` is stored in the users namespace under a derived id
(`"token:<id>"`). A token string is `kdbt_<id>_<secret>` (`TokenPrefix`, `IsAccessToken`); only the
secret's SHA-256 is kept. The registry authenticator checks `Credentials.Token` or `.Password` for
the prefix first (`verifyToken`: unknown -> `ErrTokenNotFound`, revoked/expired refused) and yields a
`Principal` with the owning user's current roles.
Kotlin splits this into `kdb-auth` (`AuthEngine`, `CompositeAuthEngine`), `kdb-auth-static`
(config users) and `kdb-auth-store` (`RegistryAuthStore`, `UserStore`/`RoleStore`, `AccessTokens.kt`
with the same token format, `DynamicAuthEngine`).

#### 2.10.2 `config`

Pure data: `ServiceSettings` (resolved), `ServiceFile` (pointer-field mirror, decoded from a JSON file, so
"absent" is distinguishable from "zero"), `ServiceTLSFile`, `ProductConfig{Features, AuthPath, TLS}`. `ResolveService(file,
lookupEnv, flagWasSet, flags)` applies precedence default < file < env < flag and records provenance.
`Describe(...)` returns `[]SettingDescriptor{Key, Value, Default, Source, Scope, Mutability, Unit,
Sensitive, Help, Warnings}` (`Redact()` hides sensitive values) which the control plane serves and
uses to decide whether a `PATCH` applies live (`MutabilityLive`), to new connections, on namespace
reopen, only after restart, or never (`MutabilityImmutable`). New settings since 2026-10-11:
`CacheMB` (`--cache-mb`, 0 = sized from the machine), `AttachmentUploadGrace`,
`AttachmentDeltaStepMB`, `AttachmentDeltas`, `DocumentCopies`. Several flags are not settings at
all (`--expire-namespaces`, `--control-open`, `--gc-headroom-percent`; plain `flag` in `service`).
`SetInFile/WriteServiceFile`
persist a subset (`PersistableInFile`). `Parse{Durability,Compression,SyncMode,LogLevel}` bridge to
`storage`/`storage/io` enums.

### 2.11 Operations: `integrity`, `backup`, `recovery`

* **`integrity`**: `Verify(shim, ns, Options{Level L1|L2}) (*Report, error)` scans delta segments
  and classifies `Finding{Level, Segment, Offset, Classification, CommitHash}` as `TornTail`,
  `MidLogCorruption`, `MissingParent`, `SequenceGap`. `Repair(shim, report, opts)` returns
  `RepairResult{Steps []RepairStep}` with `Action` `TruncatedTornTail`, `RewroteSegmentPrefix`
  (after quarantining the original), `Refused`, `NoOp`. Helpers `ScanVerifiedCommits`,
  `VerifiedSegmentPrefix` (now returns the prefix bytes and its `[]delta.ScannedCommit`),
  `GenesisCommitHash(ns)`. Relies on `storage.DamageTolerantReader` ideas
  via `storage/delta`'s `ScanSkippingCorrupt`.
* **`backup`**: `ObjectStore{Put,Get,Delete,List(ctx,...)}` (implemented by `DirStore`; the
  `s3.BlobStore` method set is identical), `Create(shim, ns, store, baseBackupID) (*Manifest, error)`
  / `CreateWithOptions(..., CreateOptions{Definitions})` (incremental when a base id is given;
  `Manifest{Segments []SegmentEntry{Sequence, FileSha256, VerifiedPrefix}, Attachments
  []AttachmentSegment, Definitions json.RawMessage}`; attachment store segments are backed up as
  `AttachmentPart`s, `attachments.go`), `Verify`, `FetchToDir`, `ListBackups`; database-level
  `CreateDatabase/LoadDatabaseManifest/VerifyDatabase` (+ cross-namespace `TxnFile` state
  `RestoreTxnState`). A segment damaged in place is refused (`DamagedSegmentError{NamespaceID,
  Sequence, Offset, Reason, NextIntactFrame, Incremental, Err}`) rather than backed up as its
  prefix. Restored meta definitions are staged next to the namespace
  (`WriteRestoredDefinitions/ReadRestoredDefinitions/RemoveRestoredDefinitions`) for `service` to
  adopt at start.
* **`recovery`**: `HybridRestore(sources []Source, ns, comp, out)` merges verified commits from
  several sources; `StagePromotion`/`PendingPromotion`/`ApplyPromotion`/`AbandonPromotion`/
  `LastPromotion` implement the two-phase "stage a restored namespace, swap it in on restart, keep
  the superseded copy under `SupersededDir`" flow used by `control`.

None of these hold long-lived state; they operate on a `PlatformIOShim` or `ObjectStore`.

---

## 3. Interface catalogue

"Impls" lists production implementations unless marked (test). "Used by" names the main consumers.
Interfaces marked **sealed** have an unexported marker method, so the full set of implementations
is the closed list in the same package.

| Interface | Package | Impls | Used by |
|---|---|---|---|
| `Exception` | `error` | every `*kdberr.XxxError` via embedded `*base`; also `dag.*Error`, `document.*Error`, `wal.CorruptionError`, `io.PlatformIOError` (hand-written `Code()`) | `CodeOf/AsException`, `schema.Validate` (`Result[T]`) |
| `Value` (codec) / `Type` (codec/schema) / `Value` (json) / `LogicalAnnotation` | `codec`, `codec/schema`, `json` | **sealed** sum types (see 2.1.2, 2.1.3) | codec, json, document |
| `Op` | `document` | **sealed**: `WriteOp`, `DeleteOp`, `FileWriteOp`, `SchemaMigrationOp` | transaction, dag, storage, wire |
| `CommitDAG` | `dag` | `*InMemoryCommitDag`, `*embed.PersistingCommitDAG` | `EmbeddedKdbRuntime.DAG`, `auth.RegistryAuthStore`, `compaction`, `query/hybrid` |
| `HistoryNavigator` | `dag` | `*InMemoryCommitDag`, `*embed.PersistingCommitDAG` (explicit `var _` assertions) | `embed.RevertTo/DiffCommits`, server history, control |
| `DocumentTreeStore` | `dag` | `*engine.ServerEngine` | `InMemoryCommitDag.SetTreeStore` (in `Host.openNamespace`) |
| `TraversalEntry`, `DiffEntry`, `CommitRef` | `dag` | **sealed** (`FullEntry/StubbedEntry`; `DiffAdded/Removed/Modified`; `RefByHash/Branch/Tag/Time`) | history, diff, peer sync |
| `Adapter` | `storage` | `*engine.ServerEngine`, `*engine.MultiplexAdapter`, `*mem.InMemoryStorageAdapter` | transaction, sql, server, policy registry, auth store |
| `EvictableAdapter` | `storage` | `*engine.ServerEngine` | `engine.Handle.Adapter()` |
| `DamageTolerantReader`, `TreeResolver`, `TreeWalker`, `TreePinner`, `ForeignTreeStore` | `storage` | `delta.DefaultReader`; `ServerEngine` + `InMemoryStorageAdapter` (tree ones) | optional-interface probes in `embed`, `integrity`, `peersync` snapshot install, `server` pin |
| `DocumentCounter`, `OrderedScanner`, `HiddenDocumentLister` | `storage` | `ServerEngine`, `InMemoryStorageAdapter` (+ `MultiplexAdapter` for `OrderedScanner`); `server.expiryHidingAdapter` (all three) | SQL `COUNT(*)`, keyset paging, expiry |
| `SnapshotApplier`, `ThroughReader`, `TreeBodyStreamer` | `storage` | `ServerEngine` | `peersync` snapshot install (batched apply) and snapshot serving (`sortedBodies`), bounded reads |
| `DeltaDocumentReader` / `DeltaPayloadReader` / `DeltaHashStreamer` / `DeltaCommitStreamer` / `DeltaPayloadStreamer` / `DeltaSegmentRangeLister` / `DeltaSegmentSequenceLister` / `DeltaSegmentStamper` / `DeltaFingerprintMemo` | `storage` | `delta.DefaultReader` (explicit assertions for most) | engine cold loader, `embed` commit-ops loader and replay, peer-sync serving stored payloads |
| `DeltaSegmentWriter` / `Reader` / `CommitStreamer` / `Sequencer` | `storage` | `delta.DefaultWriter`; `delta.DefaultReader`; `DefaultReader`; `DefaultWriter` | `PersistingCommitDAG`, `embed` replay/truncate |
| `PlatformIOShim` | `storage` | `io.FileBackedPlatformIO`, `io.InMemoryPlatformIO` | wal, delta, sstable, checkpoints, integrity, backup |
| `SnapshotStreamer`, `SegmentRenamer`, `SegmentGenerationer`, `SegmentStamper`, `SegmentIntoReader`, `LocalPathResolver` | `storage` | `io.FileBackedPlatformIO` (`InMemoryPlatformIO` for renamer/generationer; `OSByteStore` streams snapshots) | streamed checkpoints, mapped files (`mmapfile`), prealloc stamps |
| `attachstore.SmallBlobs` | `storage/attachstore` | `ServerEngine`, `MultiplexAdapter`, `InMemoryStorageAdapter` (`ReadBlob`) | small attachments kept in the blob store |
| `document.AttachmentSetStore` | `document` | `embed` set stores (`engineSetStore`, `memSetStore`) | `AttachmentSetPool` (bounded set pool, spill) |
| `transaction.AttachmentBlobSource`; `BlobPresence` (optional on a storage) | `transaction` | `*embed.EmbeddedKdbRuntime` (`HasAttachmentBlob`); no production `HasBlob` yet, so preflight falls back to `ReadBlob` | commit preflight of attachment puts |
| `sql.AttachmentBlobs` | `sql` | `*embed.EmbeddedKdbRuntime` | `WITH ATTACHMENT`, `ATTACHMENT*()` functions (`sql.SetAttachmentBlobs`) |
| `sql.ImmediateUpdater`, `KeyedUpdater`, `ImmediateInserter`, `CandidateEstimator` | `sql` | `sql.defaultEngine`, `DMLExecutor` (estimator: `Executor`, `defaultEngine`) | `server.ExecImmediateUpdate/Insert`, admission sizing |
| `sql.ExactCounter`, `OrderedKeyIndex`, `CompositeOrderedIndex` | `sql` | `server.RegistryIndexProvider` (+ `index.Registry` for composite) | count and ORDER BY ... LIMIT top-k from indexes |
| `index.ParsedDocumentStore` | `index` | `MemoryStore`, `fulltext.Store`, `vector.Store` | one parse per document across stores |
| `core.SynchronousSender` | `transport/core` | `tcp` and `ws` connections | `wire` codec and `server` fan-out/replies: reuse a pooled encode buffer once `Send` returns |
| `BudgetParticipant` | `storage` | `embed.namespaceBudget` (wraps `ServerEngine` + DAG) | `storage.BudgetArbiter` |
| `RealizedStoreHandle` | `storage` | `manager.PooledRealizedHandle` | `storage/manager` only |
| `SegmentByteStore` | `storage/io` | `OSByteStore`, `PrimaryWithReplicas` | `FileBackedPlatformIO` |
| `ReplicaSink`, `SegmentFetcher` | `storage/io` | `s3.ReplicaSink` | `PrimaryWithReplicas` |
| `s3.BlobStore` / `backup.ObjectStore` | `storage/io/s3`, `backup` | `AWSBlobStore`, `MemoryBlobStore`, `PrefixedStore`; `backup.DirStore` | `s3.ReplicaSink`, `backup.*` |
| `WriteAheadLog`, `wal.Factory` | `storage/wal` | `DefaultWriteAheadLog`, `DefaultFactory` | `ServerEngine` |
| `SegmentCatalog` | `storage/wal` | none (declared, unused) | nothing |
| `memtable.Table` | `storage/memtable` | `SortedTable` (explicit assertion) | `memtable.Manager` |
| `sstable.Writer` / `Reader` | `storage/sstable` | `DefaultWriter` / `DefaultReader` | `LsmBlobStore`, `Compact` |
| `engine.Factory` / `Handle` / `Engine` | `storage/engine` | `DefaultFactory` / internal handle / `ServerEngine` | `embed.Host.openNamespace` |
| `storage/manager.Manager` | `storage/manager` | `DefaultManager` | process singleton (`Install/Get`); unused by server |
| `Engine` | `transaction` | `defaultEngine` (via `NewEngine[WithOptions]`) | `server.KdbServerRuntime`, `driver.namespace`, `peersync` (merge) |
| `Preparer` | `transaction` | `defaultEngine` (explicit assertion) | `server.serverParticipant.Prepare`, `driver.driverParticipant.Prepare` |
| `ConflictResolver` | `transaction` | `script.ConflictResolver` (explicit assertion), user supplied | `defaultEngine` (Custom policy), `peersync.ResolutionOptions` |
| `TransactionResult` | `transaction` | **sealed**: `ResultSuccess/Conflict/SchemaError/Aborted` | server, driver |
| `Store` / `DocumentStore` / `PreparedPut` / `StoreFactory` | `index` | `MemoryStore`; `fulltext.Store`, `vector.Store` (DocumentStore assertions); `StoreFactoryFunc`, `stores.NewFactory` | `index.Registry`, `server.RegistryIndexProvider` |
| `StatsProvider` | `index` | `fulltext.Store` | `SaveStoreSnapshot` manifest stats |
| `Key` | `index` | **sealed** key kinds | stores, registry |
| `sql.Engine`, `Parser`, `Planner` | `sql` | `defaultEngine`, `DefaultParser`, `DefaultPlanner` | server, driver, control |
| `sql.IndexProvider`, `IndexDDLExecutor` | `sql` | `server.RegistryIndexProvider` (both) | `sql.Executor`, `defaultEngine` |
| `Statement`, `Expr`, `Cell`, `Parameter`, `PhysicalPlan`, `Projection` | `sql` | **sealed** ASTs | sql |
| `hybrid.Engine`, `SQLParser`, `VersionResolver`, `PreparedQuery` | `query/hybrid` | internal engine, `DefaultSQLParser`, `DefaultVersionResolver` | tests only |
| `policy.Registry` | `policy` | `InMemoryRegistry`, `StorageBackedRegistry` | `compaction`, `hybrid`, `service` |
| `policy.Parser` / `Validator` / `Evaluator` | `policy` | `DefaultParser`; (validator impl internal); `defaultEvaluator` | policy loading, `compaction` |
| `compaction.Engine` / `Coordinator` | `compaction` | `defaultEngine` / `InProcessCoordinator` | tests only |
| `Authenticator`, `Authorizer`, `Engine` | `auth` | `AllowAll`; engine from `NewRegistryAuthEngine` | server, peersync host, syncnode, control |
| `Action` | `auth` | **sealed** action structs | `Authorizer.Authorize` |
| `wire.Codec` | `wire` | `*DefaultCodec` | server, client, stream, peersync |
| `wire.Message` | `wire` | ~76 `*Message` structs (75 `MessageType`s, up to `REFS_CHANGED` 0x51) | everywhere on the wire |
| `HandshakeNegotiator` | `wire` | `DefaultHandshakeNegotiator` | not used by the Go server (it negotiates in `handleHandshake`) |
| `stream.Transport`, `ConnectionHandle` | `stream` | `InMemoryTransport`; `tcp.Transport`; `ws.Transport` | client, peersync, replication, listeners |
| `stream.Coordinator` / `Subscriber` / `IndexHintApplier` | `stream` | `defaultCoordinator` / `defaultSubscriber` / `NoopHintApplier` | tests, embedders |
| `peersync.Client` / `Session` / `Host` | `peersync` | `defaultClient` (`NewClient`) + its session / `*ConnectionHost` (`NewHost`) | v1 peer sync |
| `peersync.LocalNode` | `peersync` | `server.serverLocalNode` | `IngestEnv` |
| `peersync.BootstrapNode` (optional on `LocalNode`) | `peersync` | `server.serverLocalNode` | snapshot install (`exclusiveBootstrap`) |
| `peersync.AttachmentProjectionTarget` (optional on `ProjectionTarget`) | `peersync` | `server.projectionTarget` | projection fetch/write-back with attachments |
| `peersync.NamespaceProvider` | `peersync` | `server.peerNamespaceProvider`, `replication.Config.Local` | `V2Host`, `SyncV2`, `Replicator` |
| `peersync.ProjectionTarget` / `WriteBackTarget` | `peersync` | `server.projectionTarget` (`KdbServerRuntime.ProjectionTarget()`) | `SyncProjection` |
| `embed.GroupParticipant` / `PreparedPart` | `embed` | `server.serverParticipant`, `driver.driverParticipant`, (test) `fakeParticipant` | `embed.CommitGroup` |
| `embed.PublishObserver` | `embed` | `server.serverParticipant` | `CommitGroup` |
| `script.Host` | `script` | `server.runtimeHost` (reads via the runtime, used with `ModeRead`) | `script.Runtime.Call/ResolveConflict` in `server.ConflictProcedure` |
| `server.SearchProvider` | `server` | `RegistryIndexProvider` | wire `SEARCH` handler |
| `server.FrameConn` | `server` | any `stream.ConnectionHandle` (it already has `Incoming()`/`Send()`) | `FrameHost.Serve` |
| `control.NamespaceSource`, `ReplicationSource`, `RepairSource`, `DeepenSource`, `NamespaceReopener`, `MaintenanceSource` | `control` | `SingleNamespace/StaticNamespaces`; `*replication.Replicator` (via `service.lazyReplication`); `*Replicator`; `*Replicator`; `service.namespaceReopener`; `service.maintenanceRegistry` | `control.Server` |
| `service.GRPCListener` / `GRPCListenerFactory` | `service` | registered by an optional build-tagged package | `service.Main` |
| `compute.Adapter` | `compute` | `CPUAdapter`, `webgpu.Adapter` | none in the Go engine |

Optional-interface idiom (Go "capability probing"): `Preparer`, `PublishObserver`,
`DamageTolerantReader`, `TreePinner`, `TreeResolver`, `IndexDDLExecutor`, `StatsProvider`, and since
2026-10-11 `BootstrapNode`, `AttachmentProjectionTarget`, `BlobPresence`, `SynchronousSender`, the
`storage` counters/scanners/streamers and `Delta*`/`Segment*` capabilities above are all
discovered by type assertion on a value that already satisfies a smaller interface. When adding a
wrapper around one of those values, forward the optional methods or the capability silently
disappears.

---

## 4. Error model

### 4.1 Three families

1. **`kdberr.Exception`** (`error` package): typed, with a stable numeric `Code`. Created through
   `NewXxx` constructors; hierarchy is by *embedding*, e.g. `index.NotFoundError` embeds
   `*kdberr.SchemaError`, `transaction.BaseNotFoundError` embeds `*kdberr.VersionNotFoundError`,
   `sql.ParseError` embeds `*kdberr.DecodeError`, `auth.AuthenticationError` embeds
   `*kdberr.TransportErr`. Detect with `errors.As`, or `kdberr.CodeOf(err)` (walks `Unwrap`).
2. **Package-local errors that implement `Code()` by hand**: `dag.{Consistency,BranchNotFound,
   TagNotFound,CompactionSafety,HeadConflict}Error`, `document.{Decode,CommitDecode}Error`,
   `wal.CorruptionError` and `io.PlatformIOError` (both `StorageTierError`).
3. **Plain error types and sentinels** with no numeric code: everything in `server` (`BusyError`,
   `ConflictError`, `SchemaError`, ...), `client`, `driver`, `embed`, `peersync`, `control`. These
   are classified at the boundary (4.3) rather than carrying a code. Note the name clashes this
   produces: `ConflictError` exists in `error`, `server`, `client`, `driver`; `SchemaError` in `error`,
   `server`, `driver`; `BusyError`, `UnsupportedError`, `NotHomeError` in `server` and `client`;
   `DecodeError` in `error`, `document`, `wire`, `schema`; `HistoryNotRetainedError` in `embed`
   and `query/hybrid`; `ResourceExhaustedError` in `server` and `client`; `DamagedSegmentError` in
   `backup` and `storage/delta`. Always import with an alias.

### 4.2 Numeric codes (`error/code.go`, never renumbered)

| Range | Codes (name = value) |
|---|---|
| 1xxx codec | `KdbDecodeError`=1001, `KdbEncodeError`=1002, `KdbSchemaError`=1005 |
| 2xxx json | `JSONPathError`=2001 |
| 3xxx schema/versions | `SchemaViolation`=3001, `SchemaMigrationFailed`=3002, `VersionNotFound`=3101, `IceStorage`=3102, `CompactionBoundary`=3103 |
| 4xxx concurrency/storage | `Conflict`=4001, `DocumentLocked`=4002, `StorageTierError`=4101, `DataDirectoryLocked`=4102, `NamespaceNotFound`=4201 |
| 5xxx index | `IndexCorruption`=5001 |
| 6xxx protocol/transport/auth | `UnsupportedProtocolVersion`=6001, `EncodingNegotiationFailure`=6002, `TransportError`=6101, `ComputeUnavailable`=6201, `ComputeDispatchError`=6202, `AuthenticationFailed`=6301, `AuthorizationFailed`=6302 |
| 7xxx | `ArchiveRestore`=7001 |

Codes with **no Go producer** found (grep of constructors/constants outside `error/`): `IndexCorruption`,
`ArchiveRestore`, `DataDirectoryLocked` (the data-dir lock failure is a bare
`fmt.Errorf("data directory locked: ...")`), `ComputeUnavailable`, `ComputeDispatchError`, and
`AuthenticationFailed`/`AuthorizationFailed` (the `auth` errors embed `TransportErr`, so they report
`TransportError`=6101). Kotlin's
`KdbErrorCode` additionally defines `TRANSACTION_ABORTED`=4003 and `SCRIPT_NOT_FOUND/COMPILE_ERROR/
TIMEOUT/RESOURCE_LIMIT/RUNTIME_ERROR`=8001-8005, which Go does not (Go's script errors are
sentinels in `script`).

### 4.3 Server error -> wire `ErrorCode` (`server/error_classification.go`, first match wins)

| Go error (`errors.As`) | `wire.ErrorCode` | `RetryAfterMs` |
|---|---|---|
| `attachstore.ErrCorrupt` (`errors.Is`; stored attachment bytes fail their hash) | `INTEGRITY` | none |
| `*BusyError` | `BUSY` | `RetryAfter()` |
| `*ReplicaBehindError` | `BUSY` | `RetryAfter()` |
| `*UnavailableError` | `UNAVAILABLE` | none |
| `*DeadlineExceededError` | `DEADLINE_EXCEEDED` | none |
| `*MemoryPressureError` | `BUSY` | zone-derived `RetryAfterMs` if > 0 |
| `*ResourceExhaustedError`, `*sql.ScanRowBudgetExceededError`, `*sql.RetainedLimitError`, `*ReplyTooLargeError`, `*wire.FrameTooLargeError`, `*sql.ResultRowLimitError` | `RESOURCE_EXHAUSTED` (never retry as-is) | none |
| `*ConflictError` | `CONFLICT` (re-read, then retry) | `conflictRetryAfterMs()` |
| `*AuthorizationError` | `UNAUTHORIZED` | none |
| `*UnsupportedError` | `UNSUPPORTED` | none |
| `*NotHomeError` | `NOT_HOME` (message carries `home=<addr>`) | none |
| `*StaleFenceError` | `CONFLICT` | none |
| `*SchemaError` with `HasUniqueViolation()` | `UNIQUE_VIOLATION` | none |
| `*SchemaError` otherwise | `SCHEMA_VIOLATION` | none |
| anything else | `INTERNAL` | none |

Attachment handlers classify first through `errCodeFor` (`server/attachments_wire.go`):
`embed.ErrNoAttachment`/`ErrNoSuchDocument`/`ErrAttachmentNotRetained` -> `NOT_FOUND` (new code),
`attachstore.ErrTooLarge` -> `RESOURCE_EXHAUSTED`, `*AuthorizationError` -> `UNAUTHORIZED`, then
`classifyError`. `wire.ErrorCode` now also has `SHALLOW_BOUNDARY` (peer sync) and `NOT_FOUND`.

Details that matter: `ConflictError` on the wire is delivered as a `ConflictReportMessage`
(`ReportBytes` = serialised `kdberr.ConflictReport`) for `TX_COMMIT`, and as `Error`+`ErrorCode`
on `UpsertResult`/`DocumentGetResult`/`SqlResult`; a transaction precondition failure is a
`ConflictItem` with `OperationType = PreconditionFailed` and `ActualContentHash` set. `PreconditionFailedError`
(from `server.PutJSON`'s `Expect`) is **not** in `classifyError`; only `control` consumes it (HTTP 409
`precondition_failed`). `peersync` hosts use `V2HostConfig.ClassifyError` / `HostConfig.ClassifyError`
(the server passes `classifyError` and treats `INTERNAL` as "not classified") and otherwise
fall back to `frameHandler.classify`: `AttachmentsUnsupportedError` -> `UNSUPPORTED`, `NamespaceMismatchError` -> `NAMESPACE_MISMATCH`,
`UnsupportedFrameError` -> `UNSUPPORTED`, `TreeMismatchError` -> `INTEGRITY`, `PushBelowBoundaryError` -> `SHALLOW_BOUNDARY`
(a pushed page needs parents behind the receiver's snapshot root; a pusher that knows the code sends their
state by `GRAFT_PUSH` and resends the page - `peersync/boundary.go`),
`auth.AuthorizationError` -> `UNAUTHORIZED`, else `INTERNAL`. Peer failures are sent as
`PeerErrorMessage{Code, Message}`; the receiving `peersync` client surfaces `RemoteError{Code, Message}`.

HTTP mapping for the control plane (`control/docwrite.go`, `data.go`): conflict ->409
`conflict` (or `precondition_failed` when any item has `ActualContentHash`), document lease ->409
`document_locked`, `server.SchemaError` ->400 `schema_violation`, read-only ->403
`read_only_runtime`, other ->500 `commit_failed`.

### 4.4 Wire `ErrorCode` -> client error (`client/client.go: classifiedError`)

| Code | Client type (Is-compatible sentinel) |
|---|---|
| `BUSY` | `*BusyError{RetryAfterMs}` (`ErrBusy`, `RetryAfter()`) |
| `RESOURCE_EXHAUSTED` | `*ResourceExhaustedError` (`ErrResourceExhausted`) |
| `NOT_FOUND` | wraps `ErrNotFound` (attachment or document absent) |
| `INTEGRITY` | wraps `ErrAttachmentCorrupt` (the only client-facing INTEGRITY; ranged reads also raise it client-side on a failed chunk proof) |
| `UNAVAILABLE` | `*UnavailableError` (`ErrUnavailable`) |
| `DEADLINE_EXCEEDED` | `*DeadlineExceededError` (`ErrDeadlineExceeded`) |
| `UNSUPPORTED` | `*UnsupportedError` (`ErrUnsupported`) |
| `NOT_HOME` | `*NotHomeError{Home}` (`ErrNotHome`; `Router` follows it) |
| `CONFLICT` | `*ConflictError{TransactionID, BaseHash, TargetHash, Conflicts, RetryAfterMs}` (`ErrConflict`); `*PreconditionError` (`ErrPreconditionFailed`) when an item is `PRECONDITION_FAILED` |
| lock refusal | `*LockError` (`ErrLockUnavailable`) |
| nil code (old server) or unknown code | plain `fmt.Errorf("kdb: %s")` |
| handshake rejection | wraps `ErrUnauthenticated`; connection loss wraps `ErrClosed`; transport faults `*TransportError` |
| attachment client-side | `*AttachmentRangeError` (`ErrAttachmentRange`, offset past end), `*UploadError` (resumable upload failure) |

`SCHEMA_VIOLATION`, `UNIQUE_VIOLATION`, `UNAUTHORIZED`, `SHALLOW_BOUNDARY` and
`NAMESPACE_MISMATCH` currently reach Go clients only as the prose fallback (no dedicated client type);
the driver instead surfaces `driver.ErrConflict` / `*driver.SchemaError` / `*driver.ConflictError`
directly from in-process results.

---

## 5. Configuration and option structs

| Struct | Package | Purpose / key fields |
|---|---|---|
| `ServiceSettings` / `ServiceFile` | `config` | Resolved process settings: `DataDir, Memory, Namespace, SQLAddr, PeerAddr, StreamAddr, WSAddr, GRPCAddr, AdminAddr, RBAC, MemoryBudgetMB, MemoryLimitMB, MemoryReserveMB, CacheMB, MaxConnections, ScanRowBudget, AbortAfter, DrainTimeout, MaintenanceInterval/Sweep/MaxDefer, Control*, TLS*, LogLevel, LogFormat, Durability, AsyncSyncIntervalMS, Compression, SyncMode, AttachmentUploadGrace, AttachmentDeltaStepMB, AttachmentDeltas, DocumentCopies`. `ServiceFile` = same with pointer fields + `ServiceTLSFile`. |
| `SettingDescriptor` | `config` | Provenance/mutability metadata for one setting, drives control-plane `GET/PATCH /v1/settings`. |
| `ProductConfig{Features, AuthPath, TLS}` | `config` | Feature toggles for the product shell (`LoadProduct`, `DefaultFeatures`). |
| `FileRuntimeOptions` | `embed` | `S3`, `S3Archive *s3.Config`, `ArchiveBlobs s3.BlobStore`, `ReplicationPolicy`, `Storage StorageOptions`, `ReadOnly`; `FileRuntimeOptionsFromEnv()`. |
| `StorageOptions` | `embed` | Per-namespace: `MemoryBudgetBytes, Durability, Compression *CompressionCodec, AsyncSyncIntervalMillis, HistoryStrategy, HistoryMode, Retain, DocumentCacheBytes, BoundedLiveBodies, LargeCommitBodyBytes, DocumentCopies, CommitOpsBytes, HistoryTreeCacheBytes, DeltaMaxSegmentBytes, TreeChainLimit, DisableCheckpoints, AttachmentUploadGrace, AttachmentDeltaStepBytes, DisableAttachmentDeltas, CloseCompactionBytes, DisableBackgroundCompaction, Graph dag.GraphSettings, TreeFile, TreeResidentBytes, TreeMaxSegments, AttachmentSetBudgetBytes, SyncMode, PreallocateSegments`. Changing it live = `Host.ReopenNamespace(..., sopts)`; `StorageOptionsOf(ns)` reads the active one. |
| `StorageEngineConfig` | `storage` | The engine-facing superset built from `StorageOptions` (adds `IOShim`, WAL settings, page sizes). |
| `dag.GraphSettings` | `dag` | `AncestryPruning, FileEnabled, RebuildCommits, MaxSegments, ChunkBytes, Memory GraphMemory, AnchorInterval`. Zero value is off, but `FileRuntimeOptionsFromEnv` turns `FileEnabled` on unless `KDB_GRAPH_FILE` says otherwise. |
| `MaintenanceOptions` | `embed` | `Interval, Sweep, Busy, MaxDefer, Reclaim, CompactTables, Logger`. |
| `attachstore.Config` | `storage/attachstore` | `SmallMaxBytes, MaxBytes, DeltaMaxBytes, DeltaIndexEntries, DeltaStepBytes, DisableDeltas, DeltaMaxDepth, HashWorkers, ReadLease, UploadGrace, PointerCacheBytes` (set via `EmbeddedKdbRuntime.SetAttachmentConfig`). |
| `memtune.Options` | `memtune` | `HeadroomPercent, MinHeadroomBytes, MaxHeadroomBytes, SmallHeapBytes, Interval` (GC pacing; `service` only). |
| `PlatformIOConfig` | `storage/io` | `RootDirectory, FsyncOnFlush, SyncMode, MaxAppendBytes, PreallocateBytes`. |
| `s3.Config` | `storage/io/s3` | `Bucket, Region, Prefix, Endpoint, PathStyle, EnsureBucket`. |
| `transaction.EngineOptions` | `transaction` | `UniqueKeys *UniqueKeyRegistry, Preconditions bool`. |
| `HostConfig`, `ClientConfig`, `V2HostConfig`, `V2ClientConfig`, `ResolutionOptions`, `ProjectionConfig`, `CatchUpOverride` | `peersync` | Callback-heavy configs; the callbacks are how `server` injects persistence, materialisation, classification, home handling, write-back and head watching (`V2HostConfig.WatchHead/Send`). Bounds added since 2026-10-11: `V2HostConfig.MaxGraftPushBytes`, `AdoptEveryBytes` (host and client); `V2ClientConfig.CatchUp/CatchUpNamespaces`. |
| `replication.Config`, `PeerConfig` | `replication` | Node id, local provider, peers (`name=..,addr=..,namespaces=..,mode=..,catchup=..,catchup-ns=..` parsed by `ParsePeer`), state store, debounce/backoff/timeout. |
| `syncnode.Config`, `IdleConfig` | `syncnode` | Wraps replication + retention grace, webhook, scrub, idle close, `MaxPeerConnections`. |
| `control.Options` | `control` | Listener, runtime/namespace source, settings, feature gates (`AllowWrites`, `AllowSettingsPersist`, `AllowPromotion`, `ServeUI`), dirs, hooks. |
| `ConnectOptions` | `client` | `TLS, Namespaces, Transport, JSONOnly`. |
| `ParsedURL` | `driver` | `Mode, Catalog, NamespaceID, ReadOnly, DataRoot, MemoryParams`. |
| `script.Limits` | `script` | `WallClock, MaxOutputBytes, MaxLogBytes, MaxHostCalls`. |
| `TransportConnectOptions`, `TransportTlsSettings` | `transport/core` | `ConnectTimeoutMs, ReadTimeoutMs, WriteTimeoutMs, MaxFrameBytes, ConnectHeaders, TLS, Admitter, MaxConnections, IncomingQueueFrames`. |
| `ConflictWebhook`, `ConflictProcedure`, `IdlePolicy` | `server` | Background-loop settings. |
| `CatchUpPolicy{RefuseSquash, Reason}` | `server` | Per-namespace replicated definition (meta kind `catchup`) feeding `IngestEnv.SquashRefused`. |
| `backup.CreateOptions{Definitions}` | `backup` | Meta definitions carried in a backup manifest. |
| `NamespacePolicy` | `policy` | Declarative per-namespace policy (largely unused by Go runtime, see 2.3.3). |

Convention: option structs are plain values with zero-value defaults resolved by the consumer
(for example `StorageOptions.MemoryBudgetBytes <= 0` means "share the host pool equally"); there
are no functional-options builders. Fields that turn a new on-disk behaviour on (`dag.GraphSettings`,
`StorageOptions.TreeFile`, `BoundedLiveBodies`) zero to *off*, so a caller that was never updated keeps
its old behaviour; `embed.FileRuntimeOptionsFromEnv` (used by `service` and `embed.OpenFileRuntime`) is where the
production defaults flip them *on* (`KDB_GRAPH_FILE`, `KDB_TREE_FILE`, `KDB_BOUNDED_LIVE_BODIES` opt out).
The `driver` opens its host with `embed.FileRuntimeOptions{}`, so all three stay off there (section 9).

---

## 6. Concurrency primitives

Counts below come from non-test source only. "Goroutines" are long-lived loops unless noted.

| Package / type | Locks | Atomics | Goroutines / channels | Notes |
|---|---|---|---|---|
| `dag.InMemoryCommitDag` | `mu sync.RWMutex` (+1 more RW in graph/ops retention) | `head atomic.Pointer[headSnapshot]` padded off `mu`; `pins`, versions | none | Lock-free head reads; every mutator must `publishHeadLocked`. `ancestryVersion` invalidates memoised ancestry. |
| `embed.PersistingCommitDAG` / `commitLogWriter` | `mu` (failure), `sendMu sync.RWMutex` (close vs send), `closeOnce` | `barrier atomic.Pointer[TxnGroup]` | one writer goroutine; `reqs chan *logRequest`, `done`, `closed` chans | Group commit; `wait()` funcs returned to callers. |
| `embed.Host` | `mu sync.Mutex` over `nss`, `closed` | none | arbiter ticker (in `BudgetArbiter`) | Double-checked open; loser closes its entry. |
| `embed.TxnCoordinator` / `TxnGroup` | `mu sync.Mutex` over `epoch, committed, inflight, dead, failed` | `foreignParts atomic.Int64` | `TxnGroup.done` chan closed on decision | `decisionLog`: 16-byte header + one fixed-size record (group id + CRC32C) per committed group; presence of the record is the commit point. |
| `embed.MaintenanceScheduler` | `mu` for the live cadence | | 1 loop goroutine; `stop`, `done`, `reconfigured` chans | `Busy()` defer/force logic; setters coalesce wakeups non-blockingly. |
| `embed` dir lock | OS `flock` x2 (attach, writer); `LockFileEx` on Windows | | | Not a Go lock: cross-process. |
| `embed` attachments / open-time jobs | `attachmentState.mu` (stores map), `closeHooksMu` | | `backgroundCompaction` and `versionRefile` goroutines (`cancel` + `done` chan), stopped by close | A cancelled merge writes nothing; the next open retries. |
| `storage/engine.ServerEngine` | `retainMu RW`, `treeMu`, `treesMu RW`, `treeRebuildMu`, `treeChainMu`, `pendingMu` | `bodiesExternal`, `replaying`, `latestTree atomic.Pointer`, `coldLoader`, counters | 1 async-sync goroutine (`asyncStop/asyncDone`) | Sharded doc/pending stores to cut contention; padding keeps `latestTree` off `treesMu`. |
| `storage/wal` | `DefaultWriteAheadLog.mu` | | `GroupCommitter` runs rounds (`runRounds` goroutine, `WaitGroup`) | Coalesces fsyncs. |
| `storage/io` | per-store mutexes (`OSByteStore` handle cache) | | | |
| `storage.BudgetArbiter` | mutex | | ticker goroutine, `Close()` | Rebalances `BudgetParticipant`s. |
| `transaction.LockManager` | `mu sync.Mutex` | | none | Leases expire lazily + `Sweep`. |
| `server.KdbServerRuntime` | `sqlEngineMu RW`, `searchProviderMu RW`, `schemaMu RW`, `closeMu`, `hookMu RW`, `inboundOnce`; `mu` in each of `changeLog`, `headWatchers`, `attachUploads` | `refCount`, `draining`, `lastUsed`, `sessionSeq`, `home`, `resolution`, `catchUp`, `fenced`, `groupPublishing`, `groupVersion`, `maxResultRows`, `maxResultBytes` | expiry sweeper loop, optional others | `foreignGroups sync.Map`; head watchers are called on the committing goroutine and must not block. |
| `server.writeGate` | `holdMu` (long hold only) | `serviceNanos atomic.Int64` | `queued chan` (bounded), `running chan` (cap 1), `holdWake chan` (closed to wake queued writers) | Channel-as-semaphore chosen so `ctx.Done()` can abort a wait. |
| `server.Admission` | `floorMu` | `outstandingBytes/Ops`, `lastZone`, `reserveLost`, `scanRowBudget`, stats counters | `semaphore.Weighted` | Floor is held as a long-lived reservation. |
| `server.MemoryGuard` | `mu`, `observerMu RW` | `zone`, `pressure` | sampler goroutine (`stop/done`, `stopOnce`) | 200 ms `runtime/metrics` ring. |
| `server.NamespaceSet` | `mu RW` (maps), `openMu` (lazy open), `globalMu` (benchmark serialise) | | idle-close loop; `finish()` goroutine per group commit | |
| `server.SessionManager` / `KdbSession` | `versionMu RW`, `leaseMu`, `crossMu`, `mu` | | none | Per-connection manager; leases released in `closeAllSessions`. |
| `server.sqlWireConnHandler` | `sendMu`, `authMu RW`, `sessionMu` (ticket map `sessionTails`), `replyBudget.mu` + `sync.Cond`, `attachReads.mu` | `negotiated atomic.Pointer[wire.DefaultCodec]` | 1 reader + up to `MaxInFlightFrames`=64 handler goroutines (`inFlight` chan semaphore, `WaitGroup`) | Session FIFO via per-session ticket chan, skipped by non-SNAPSHOT SELECTs; reader blocks on the reply byte budget. |
| `server.StreamHub`, `Listener`, `AdminServer` | mutexes over subscriber/conn sets; `fanoutCache.mu` | | accept loop + per-conn goroutines; `Listener.done` chan | `Close()` cancels ctx and waits. |
| `server.ConflictWebhook`, `ConflictProcedure`, authority expiry | | | one loop each; `Kick()` wakes, `Close()` stops | |
| `peersync.V2Host`, `ConnectionHost`, `ConflictQueue`, `RepairSession` | `mu` each; `V2Host` also `fetchMu`, `pushMu`, `hostUploads` mutex | `atomic.Int32` flags | none (driven by the connection goroutine), except a short-lived `sendChanges` goroutine per coalesced `REFS_CHANGED` | `LocalNode.Exclusive` takes the runtime write gate; `BootstrapNode.ExclusiveBootstrap` takes it as a long hold. |
| `peersync` pull gate, snapshot source | `pullGate` per namespace (`turn chan struct{}` token, waits at most `maxWait`); `sortedBodies` mutex + refcounted cursors | | snapshot-source build runs on its own goroutine | Pulls of one namespace take turns; pushes and other namespaces stay parallel. |
| `replication.Replicator` | `cycleMu`, `mu` per `peerLoop`; `StateStore.mu` | | 1 goroutine per peer, `trigger chan`, `stop chan`, `WaitGroup`, `once` | Debounced wakeups, backoff. |
| `syncnode.Node` | `mu`, `projectionPeers sync.Map` | `replicator atomic.Pointer` | scrub loop, expiry/idle loops | |
| `stream.defaultSubscriber` / `defaultCoordinator` | `mu` | | `readLoop` goroutine; `events`, `subscribers` chans | |
| `transport/tcp`, `transport/ws` | per-connection mutexes | `atomic.Int64` active-connection counter | accept goroutine, one handler goroutine and one `readLoop` per connection; bounded `Incoming` chan | Backpressure by not reading when the queue is full. |
| `client.Client` | `mu` (correlation, pending, closed), `nsMu` (namespaces, `idleTxSessions`), per-namespace `mu` and `dml sync.RWMutex` | `encoder atomic.Pointer[wire.DefaultCodec]` | 1 `readLoop`; `pending map[int]chan wire.Message` | Replies matched by correlation id; `Exec` takes `dml` exclusive, a self-contained commit shared. |
| `driver.namespace` | `write sync.Mutex` (commit serialisation), `schemaMu RW` | `fenced atomic.Pointer[error]` | none | Mirrors `writeGate` without a queue. |
| `index.Registry` | `mu RWMutex` | | none | Stores have their own locks. |
| `json` path cache | `pathCacheMu` | | | Process-wide memo. |
| `memtune` tuner | | | 1 goroutine (`run`, ctx-cancelled by `Stop`) | Adjusts `debug.SetGCPercent` to the live heap, with hysteresis. |
| `script.Runtime` | compile-cache mutex | | one short-lived watchdog goroutine per call | `Limits.WallClock` interrupts goja. |
| `control.Server` | `mu`/`RW` for settings revision, restore state | | SSE stream goroutines, one restore goroutine | |

Lock-ordering rules worth knowing: cross-namespace commits lock participants in **sorted namespace
order** (`CommitGroup`); the server takes admission grant before the write gate, never the reverse;
peer ingest enters through `LocalNode.Exclusive` = the same write gate as local writers, so there is
exactly one writer per namespace at any time.

---

## 7. Kotlin modules and Go packages

Kotlin is Kotlin Multiplatform (`commonMain` plus `jvmMain`/`nativeMain`/`jsMain`; `kdb-server`,
`kdb-jdbc`, `kdb-cli`, `kdb-auth-*`, `kdb-config`, `kdb-script`, `kdb-service`, `kdb-hibernate` are
JVM-only `src/main`; `kdb-replica` is multiplatform source with only a `jvm()` target so far). **Every storage/DAG/transaction/engine method in Kotlin is `suspend`**; the Go
equivalents are blocking and use goroutines. Mutual exclusion is `kotlinx.coroutines.sync.Mutex`
rather than `sync.Mutex`.

| Go package | Kotlin module | Central Kotlin names | Where they diverge |
|---|---|---|---|
| `error` | `kdb-error` | `KdbErrorCode` (enum), `KdbException` family (`ConflictException`, `DocumentLockedException`, `DataDirectoryLockedException`, ...), `ConflictReport/Item` | Kotlin adds `TRANSACTION_ABORTED` and `SCRIPT_*` codes and defines `DataDirectoryLockedException` (used by `kdb-jdbc`'s lock registry); Go has no `Result` use outside schema. |
| `codec`, `codec/schema` | `kdb-codec` | `KdbCodec`, `KdbValue`, `KdbType`, `KdbTypeRegistry`, `KdbHash`, `KdbUuid`, `KdbTimestamp` | Same wire format (shared golden vectors under `go/testdata/golden`); Go uses `codec.Value` interface + `schema.Registry`. |
| `json` | `kdb-json` | `JsonValue`, `JsonPath`, `JsonParser`, `JsonWriter`, `KdbJsonFunctionRegistry` | Kotlin has a function-descriptor registry (`KdbJsonFunctionDescriptor`) for JSON functions; Go exposes the functions as plain package funcs. |
| `document` | `kdb-document` | `KdbDocument`, `KdbCommit`, `KdbOp`, `KdbTransaction`, `DocumentTree` (+ `TrieNode`/`TrieLeaf`), `KdbBranch`, `KdbTag`, `CommitStub`, `AttachmentTable`, `AttachmentHash` | Attachment ops, tables and the Merkle chunk content hash are shared (golden-tested); Kotlin has no `Precondition`, `TreeNode`, `TreeProof` (Merkle repair is Go-only), and no tree.file / slab live tree. |
| `schema` | `kdb-schema` | `KdbSchema`, `SchemaEngine`, `SchemaMigration(+Builder)`, `KdbFieldType`, `UniqueConstraint` | Equivalent. |
| `dag` | `kdb-dag` | `CommitDag` (interface), `InMemoryCommitDag`, `CommitRef`, `TraversalEntry`, `CommitDiff`, `CheckpointRestore`, `AttachmentMerge`, `AttachmentTables` | Kotlin's interface is **wider** (tags, `commonAncestor`, `isAncestor`, `diff`, `squash`, `pin`, `appendMergeCommit` are all on `CommitDag`); Go's `CommitDAG` is deliberately narrow and the rest is on the concrete type / `HistoryNavigator`. Kotlin now restores from Go checkpoints (`restoreCheckpoint`, `putShallowCommit` for snapshot roots) but has no grafts, provisional commits, ops-retention budget, generation numbers or mapped on-disk graph. |
| `transaction` | `kdb-transaction` | `TransactionEngine`, `DefaultTransactionEngine`, `AuthorizingTransactionEngine`, `TransactionBuilder`, `DocumentLockManager`, `ConflictPolicy`, `ConflictResolver`, unique-key support | Kotlin wraps authorisation as a decorator engine; Go does RBAC in `KdbServerRuntime.admitWrite`. Go adds `Preparer`/`PreparedCommit`, preconditions and fenced leases. |
| `index` (+`index/*`) | `kdb-index`, `kdb-index-btree`, `-hash`, `-fulltext`, `-vector`, `-composite` | `DefaultIndexManager`, `DefaultIndexRegistry`, `DocumentIndexStore`, `DefaultBtreeIndexStore`, `DefaultHashIndexStore`, `DefaultFullTextIndexStore`, `DefaultVectorIndexStore`, `HnswGraph`, `CompositeIndexStoreFactory`, `FusionArm/Mode` | Go merges hash and btree into one `MemoryStore`; Go's registry has two-phase prepare/apply and replicated `Hint`s. |
| `storage` | `kdb-storage`, `kdb-storage-io` | `StorageAdapter`, `DeltaSegmentWriter/Reader`, `DeltaRecord`, `Durability`, `CompressionCodec`, `SegmentByteStore`, `Jvm/Native/BrowserSegmentByteStore` | Go adds `HistoryMode`, `HistoryStrategy`, `ReclaimMode`, `RetentionWindow`, `BudgetArbiter`, replica/archive sinks, `PrimaryWithReplicas`. |
| `storage/engine` | `kdb-storage-engine` | `StorageEngine`, `ServerStorageEngine`, `BrowserStorageEngine`, `InMemoryStorageEngine`, `GpuStorageEngine`, `DefaultStorageEngineFactory`, `ShardedDocStore`, `StageRecorder` | Go collapses targets into one `ServerEngine` (`BrowserEngine`/`InMemoryEngine` are type aliases; GPU target is a constant only). |
| `storage/wal`, `delta`, `memtable`, `sstable` | `kdb-storage-wal`, `-delta`, `-memtable`, `-sstable` | `DefaultWriteAheadLog`, `GroupCommitter`, `DefaultDeltaSegmentWriter/Reader`, `DeltaPageCodec`, `MemTableManager`, `LsmBlobStore`, `DefaultSsTableWriter/Reader`, `BlockCache` | Format-compatible; Go adds damage-tolerant scanning (`ScanSkippingCorrupt`) and preallocation. |
| `storage/manager`, `tier`, `compaction` | `kdb-storage-manager`, `kdb-storage-tier`, `kdb-storage-compaction`, `kdb-compaction` | `DefaultStorageManager`, `DefaultEnlistmentManager`, `DefaultEvictionManager`, `DefaultRebuildScheduler`; `DefaultStorageTierManager`, `IceBundleWriter`; `CompactionPlanner`; `DefaultCompactionEngine`, `DefaultSnapshotMaterializer`, `DefaultOrphanBlobGc` | Go's counterparts are minimal/unwired (2.3.5, 2.4.7). Go instead has log truncation + checkpoints. Kotlin-only: `kdb-storage-chunking` (`ChunkedStorageAdapter`, content-defined chunking). |
| `storage/attachstore`, `storage/deltaindex`, `storage/mmapfile`, `storage/bufpool` (new since 2026-10-11) | `kdb-embed` (`AttachmentBlobStore`, same on-disk layout incl. pointer index segments); none for the rest | `attachstore.Store`/`Reader`/`Config` (content-hashed blobs, Merkle 1 MiB chunks, deltas, pointer index, ref index, mark); `deltaindex.Index` (key -> delta frame `Location`, with on-disk sidecars); `mmapfile.Mapping`/`Region` (read-only mapped immutable files, `ResidentBytes`, `DropPages`); `bufpool.Get/Put` (size-classed read buffers) | Kotlin's attachment store is format-compatible but its pointer index is unbounded; the off-heap/mmap machinery (mapped graph, tree.file, deltaindex sidecars) is Go-only. |
| `memtune`, `fsx`, `internal/testtime` (new) | none | `memtune.Start(Options)` (GC pacing to live heap); `fsx.Open/OpenFile/Rename/Remove/RemoveAll/SyncDir` (POSIX-like semantics on Windows: `FILE_SHARE_DELETE`, retried renames); `testtime` (test wait scaling under `-race`) | Go-only process concerns. |
| `policy` | `kdb-namespace-policy` | `NamespacePolicy` registry, `DefaultNamespacePolicyParser`, `DefaultPolicyValidator`, `DefaultCompactionPolicyEvaluator`, `HistoryMode` | Policy drives compaction in Kotlin; in Go only document expiry consumes it. |
| `sql` | `kdb-sql` | `DefaultSqlEngine`, `DmlExecutor`, `DdlExecutor`, `DefaultQueryPlanner`, `DefaultVirtualViewEngine` | Go DML returns `[]document.Op`; Kotlin has virtual views and `ALTER TABLE ADD COLUMN`. |
| `query/hybrid` | `kdb-hybrid-query` | `DefaultHybridQueryEngine`, `HybridQueryRequest/Result`, `CheckoutStore`, `DefaultVersionResolver` | **Wired in Kotlin** (`EmbeddedKdbRuntime.hybrid`), unwired in Go. |
| `wire` | `kdb-wire` | `DefaultWireCodec`, `*Dto` types, `CommitPushCodec`, `DefaultHandshakeNegotiator`, `BinaryWireBody` (`WIRE_FEATURE_BINARY`), `AttachmentWireBody` | Kotlin covers handshake/SQL/session/stream/v1 peer sync, `SEARCH` + `errorCode` on results, binary frames and the attachment messages (0x44-0x49, verified ranges). Peer-sync v2 frames live in `kdb-peer-sync` (`SyncV2Frames`) and `kdb-replica` (`PeerFrames`). Go-only: lock, history/revert, multi-commit, home, meta view. |
| `stream` | `kdb-stream` | `DefaultStreamCoordinator`, `DefaultStreamSubscriber`, `StreamBroadcastHub`, `InMemoryWireTransport(Hub)` | Equivalent. |
| `transport/*` | `kdb-transport-core/-tcp/-ws` | `FrameFramer`, `FrameStreamReader/Writer`, `JvmTcpWireTransport`, `NativeTcpWireTransport`, `JsWebSocketWireTransport`, `JvmWebSocketWireTransport` | Go has no browser transport; Go adds the pre-decode `FrameAdmitter`. |
| `peersync` | `kdb-peer-sync` | `DefaultPeerSyncClient`, `DefaultPeerSyncHost`, `ConnectionPeerSyncHost`, `PeerSession`, `DagSyncPlan`, `HeadUpdate`; v2: `SyncV2Host`/`SyncV2HostSession`, `SyncV2Client` (`V2PeerSession`), `SyncV2Codec`, `PeerBlobStore`/`PeerBlobTransfer`, `PeerSnapshots` | v1 plus the core of v2: `HELLO`, `REFS`, `FETCH`/`PACK_PAGE`, `REF_UPDATE`, `SNAPSHOT_FETCH/PAGE` (bootstrap, both directions), `BLOB_FETCH` and `ATTACHMENT_UPLOAD`. Other v2 frames are answered `UNSUPPORTED`; no conflict queue, resolution chain, grafts, projections, repair, squash, `REFS_WATCH`. Kotlin merge outcomes are not deterministic across nodes, so procedure-based in-merge parity is out of scope. |
| `embed` | `kdb-embed` (+ `kdb-jdbc` wrapper) | `EmbeddedKdbRuntime` (with `putJson`, commit listeners, `hybrid`, attachments: `EmbedAttachments`, `AttachmentBlobStore` in Go's on-disk layout, `AttachmentDelta`), `KdbBrowser`, `StreamReconnectPolicy` | Kotlin's runtime is a *facade with behaviour* (put/get/query); Go's `EmbeddedKdbRuntime` is a *bundle of components* and behaviour lives in `server`/`driver`. No `Host`, `MaintenanceScheduler`, history modes or `TxnCoordinator` in Kotlin (cross-namespace is read-side only in `kdb-jdbc`: `CrossNamespaceDecisions`). |
| `server` | `kdb-server` | `KdbServerRuntime`, `SqlWireHost`, `SessionManager`, `KdbSession`, `WriteCoordinator`, `ServerRuntimeRegistry`, `ExpiryFilteringStorageAdapter`, `LockingTransactionBuilder`, `SqlAuthSupport`, `AttachmentsWire` | `WriteCoordinator` is the port of `writeGate` (same EWMA shift). `AttachmentsWire` serves the attachment messages and acks `attachments`/`attachment-proofs`. Kotlin has **no** `Admission`, `MemoryGuard`, `CostModel`, `NamespaceSet`, `Home`, `MetaStore`, `ReadThrough`, scrub, immediate UPDATE/INSERT, change log, reply budget. `SqlWireHost` = Go `FrameHost` + `sqlWireConnHandler`. |
| `auth` | `kdb-auth`, `kdb-auth-static`, `kdb-auth-store` | `AuthEngine`, `Authenticator`, `Authorizer`, `CompositeAuthEngine`, `AllowAllAuth`, `Principal`, `StaticAuthConfig`, `RegistryAuthStore`, `UserStore`, `RoleStore`, `PasswordHasher`, `TokenRecord` (`AccessTokens.kt`) | Go has one package; peer actions (`PeerPull/Push`) are Go-only. Revocable access tokens share format and storage on both sides. |
| `config` | `kdb-config`, `kdb-service` | `KdbProductConfig`, `KdbFeatures`, `KdbTlsConfig`; `ServiceConfig` | Go's settings provenance/mutability descriptors have no Kotlin equivalent. |
| `script` | `kdb-script` | `ProcedureRuntime`, `PureProcedureRuntime`, `GraalProcedureRuntime`, `ProcedureConflictResolver`, `ProcLimits` | goja (Go) vs GraalJS (Kotlin). |
| `integrity`, `recovery` | `kdb-integrity`, `kdb-recovery` | `Report`, `Finding`, `Level`, `Classification`, `RepairResult`; `Source`, `Result` | `backup`, promotion staging and the control plane are Go-only. |
| `compression`, `compute` | `kdb-compression`, `kdb-compute`, `-jvm`, `-webgpu` | `Crc32`, `ZstdUnavailableException`; `ComputeAdapter`, `ComputeBackend`, `GpuSegmentHandle` | Go has only the `Dot` seam. |
| `file` | `kdb-file` | `FileAttachmentIngest`, `FileMetadata`, `ZipArchive`, `BundleIngestResult` | Legacy file-op ingest. Named attachments are now a separate feature on both sides (Go `storage/attachstore` + `embed` attachments; Kotlin `kdb-embed`); Go's `file` is still a 30-line hash+describe, and `embed.MigrateFileAttachments` converts file ops to attachments. |
| `inspect` | `kdb-inspect` | `WireDebugHook`, `DeltaDebugHook`, `DeltaSegmentDump`, `BlobInspector` | Kotlin instruments via hooks; Go has `WireFrameInspector` + `cmd/kdb-inspect`. |
| `driver` | `kdb-jdbc` | `KdbDriver`, `KdbConnection`, `KdbStatement`, `KdbPreparedStatement`, `KdbResultSet`, `KdbJdbcUrl`, `DataDirectoryLock`, `DeltaCommitPersistence`, `DeltaNamespaceReplayer`, `KdbAttachments`; `file/`: `FileEmbeddedRuntime`, `CheckpointOpen` + `GoCheckpoint` (read Go checkpoints v2/4/5/6), `GoCheckpointWriter`/`NamespaceCheckpointer` (write v2 and v6, byte for byte), `FileSnapshotPersistence`, `FileSquashPersistence` | JDBC has `memory`/`file`/`remote` modes and keeps its own delta persistence + lock registry, but its file mode now opens from and writes Go-format checkpoints; Go's driver reuses `embed.Host`. |
| `client` | (none) | Kotlin clients use `kdb-jdbc` remote mode or `kdb-embed`'s `KdbBrowser` | Go SDK has Router/leases/conditional writes/history/search/attachments. TS `@kdb/client` (`packages/kdb-client`) is the other full SDK: binary frames, attachments, paged SELECTs, a per-namespace write-session pool (`sessionsPerNamespace`) and loopback zstd decline, neither of which the Go client has. |
| `control`, `replication`, `syncnode`, `service`, `metrics`, `version`, `backup` | `kdb-cli` (`KdbCli`, `CliSession`), `kdb-service` (`ServiceConfig`), `kdb-hibernate` (`KdbDialect`) | | Go-only runtime features; `StageRecorder` in `kdb-storage-engine` is the Kotlin analogue of `metrics.Recorder`. |
| (none: client replica) | `kdb-replica` (new) | `Replica`, `ReplicaOptions`, `NamespaceSpec`, `ReplicaNamespace` (memory) / `FileReplicaNamespace` (jvmMain, a `kdb-jdbc` file runtime), `ProjectionNamespace`, `ReplicaStore`/`MemoryReplicaStore`/`CachedReplicaStore`, `PeerConnection`, `syncNamespace`/`syncProjection`, `BlobStore` | A device-side replica kept in sync with a Go node over peer sync v2 (refs, fetch, snapshot, squash, `REFS_WATCH` live updates, attachments, filtered projections with write-back outbox). Port of TS `@kdb/replica` (`packages/kdb-replica`, IndexedDB store). The Go side is the ordinary `V2Host`; there is no Go replica. |
| (cross-language) | `kdb-integration`, `kdb-benchmark`, `kdb-browser-demo` | | Interop tests drive real Go and JVM peers (incl. Kotlin v2 peer sync, attachments, checkpoints); golden vectors in `go/testdata/golden`. TS packages: `@kdb/client`, `@kdb/replica`, `@kdb/embed`, `@kdb/codegen` under `packages/`. |

Rule of thumb for porting: a Kotlin class named `DefaultXxx` almost always corresponds to an
**unexported** Go struct behind an interface named `Xxx` (for example `DefaultTransactionEngine` ->
`transaction.defaultEngine`), because Go exports constructors, not implementations.

---

## 8. Design patterns that are actually present

* **Ports and adapters.** `storage.Adapter`, `PlatformIOShim`, `SegmentByteStore`, `stream.Transport`,
  `auth.Engine`, `sql.IndexProvider`, `peersync.LocalNode`/`NamespaceProvider`: the inner packages
  define the interface they need; outer packages (`server`, `embed`) supply adapters. This is why
  `peersync` and `driver` compile without `server`.
* **Factory.** `engine.Factory.Open` -> `Handle` (target switch), `wal.Factory.OpenOrCreate`,
  `delta.Factory.OpenWriter/OpenReader`, `FileBackedPlatformIOFactory.Open`, `index.StoreFactory`
  (switch on `IndexType`), `Host.Namespace`/`ServerRuntimeRegistry.GetOrOpen` (idempotent keyed
  factories with reference counting).
* **Strategy.** `ConflictPolicy` + `ConflictResolver` (+ `script.ConflictResolver`);
  `peersync.ResolutionChain` (ordered rule list); `storage.HistoryMode` (`Full`/`None`),
  `HistoryStrategy` (`Replay`/`Objects`: free-write/expensive-first-historical-read versus
  record-trees-as-you-go) and `ReclaimMode` (`Manual/Immediate/Balanced/Lazy`) are *runtime-switchable*
  strategies held as `*Live` fields on `ServerEngine` and consulted at maintenance time;
  `fusion.Mode` (RRF vs weighted); `Durability` (Sync/Async/MemoryOnly); `vector.Metric`;
  `peersync.CatchUpRoute` (replay/squash/auto per namespace pattern); `ResolutionChain.Version`
  (merge semantics, part of the chain hash); `embed.AttachmentHistory` (All/Latest).
* **Decorator.** `embed.PersistingCommitDAG` wraps `dag.CommitDAG`; `io.PrimaryWithReplicas` wraps a
  `SegmentByteStore`; `engine.MultiplexAdapter` is a router/composite over per-namespace adapters;
  `server.expiryHidingAdapter` wraps the `storage.Adapter` the SQL engine reads through (hides expired
  documents at head; Kotlin's twin is `ExpiryFilteringStorageAdapter`). Kotlin
  `AuthorizingTransactionEngine` has no Go twin (Go does RBAC in `KdbServerRuntime.admitWrite`).
* **Observer / listeners.** `KdbServerRuntime.CommitListener`, `PersistingCommitDAG.SetPersistListener/
  SetSealListener/SetAttachmentsCommitted`, `ConflictQueue.OnRecord`, `MemoryGuard.SetObserver`,
  `embed.PublishObserver`, `EmbeddedKdbRuntime.AddCloseHook`. Most are single-slot callbacks, not event
  buses (`WatchHead` is the exception: a list of cancellable watchers, which feeds `REFS_WATCH`); `syncnode` and the stream
  hub share the `CommitListener` slot by composing closures.
* **Sealed sum types.** Go idiom for closed hierarchies: an interface with an unexported marker method
  (`Op`, `TransactionResult`, `Statement`, `Expr`, `Cell`, `Key`, `Action`, `TraversalEntry`,
  `CommitRef`, `Message` kinds). Consumers `switch x := v.(type)`; adding a variant means touching those
  switches, and the compiler will not tell you.
* **Optional interface / capability probing.** See section 3 note.
* **Copy-on-write persistent data.** `DocumentTree.With/Without`, immutable `Commit`; mutation lives
  in the DAG maps and the storage engine, never in values handed to callers.
* **Command / two-phase commit.** `PreparedCommit` and `PreparedPart` (`Apply`/`Discard`) with a
  coordinator (`TxnCoordinator`, decision log) for cross-namespace atomicity; `index.PreparedWrite`
  and `PreparedPut` are the same shape one level down.
* **Singleton (limited).** `storage/manager.Install/Get`, `server.SetProcessNodeID`/`ProcessNodeID`,
  `auth.AllowAll`, `server.ProcedureRuntime()`, `json` path cache, and the process-wide weak
  registry `transaction.RegisterAttachmentBlobs` (adapter address -> owning runtime). Everything else
  is constructed and passed explicitly.
* **Actor-ish gating.** `writeGate` (capacity-1 channel) and the per-session ticket queue serialise
  work without holding a mutex across blocking I/O; `peersync.pullGate` (a token channel per namespace,
  bounded wait) does the same for concurrent pulls.
* **Spill to disk (Cassandra streaming / sort).** `peersync.pageSpool` (unlinked temp file of paged
  documents) and `sortedBodies` (external sort of snapshot bodies by id) keep whole-namespace
  transfers out of the heap; `attachstore` streams blobs of any size. Architectural optimisations
  since 2026-10 cite the Cassandra mechanism they copy (`bufpool` = BufferPool, `changeLog` = commit
  log, cache pool sizing, size-tiered merges).
* **Not present (despite the vocabulary):** dependency-injection containers, functional options,
  generics-based repositories, an event bus, inheritance of implementations (Go embeds only for error
  bases and in `engine.Engine`).

---

## 9. Gotchas, staleness, and open threads found while verifying

Re-checked against e79bb674 (2026-10-23).

1. **Unwired packages.** `compaction`, `query/hybrid`, `storage/manager`, `compute`,
   `wal.SegmentCatalog` and `wire.HandshakeNegotiator` still have no production consumer in Go. There
   is no Go `tier` package at all (earlier versions of this list named one). Check before assuming a
   Kotlin feature "exists in Go".
2. **Type switch on `EmbeddedKdbRuntime.DAG`.** Still true: `server.NewKdbServerRuntime` only understands
   `*dag.InMemoryCommitDag` and `*embed.PersistingCommitDAG`; another wrapper makes `KdbServerRuntime.dag`
   nil and every write fails with "requires an InMemoryCommitDag" (the check now also sits in
   `CommitImmediate` and `NamespaceSet`'s cross-namespace path).
3. **`PersistingCommitDAG.AppendCommit` vs the server path.** Still true: the server bypasses it
   (appends on `Delegate()` then `PersistAsync`) so persistence-affecting logic added only to
   `AppendCommit` will not run for server writes, nor for `CommitImmediate`. `driver` uses the same bypass.
4. **Duplicate enums.** Still: `policy.HistoryMode` vs `storage.HistoryMode`; `hybrid.ReadConsistency` vs
   `server.ReadConsistency`; `HistoryNotRetainedError` in `embed` and `hybrid`.
5. **Data-dir lock is not a typed error.** Still: `kdberr.DataDirectoryLocked` exists but `embed`
   (`dir_lock.go`, `dir_lock_other.go`, now also Windows) returns a plain `fmt.Errorf`; callers matching
   on code will miss it.
6. **LLD drift.** `docs/kdb-lld-components.md` is unchanged since before 2026-10-11: it (a) says
   `storage/manager` and `tier` "exist and are wired" (its section 1.20; `storage/manager` has no
   non-test importer and there is no Go `tier` package), (b) has no sections for `replication`,
   `syncnode`, `control`, `service`, peer-sync v2, history modes, `embed.Host`/`TxnCoordinator`, or
   anything since: attachments/`attachstore`, mapped graph, tree.file, `deltaindex`, `mmapfile`, client
   replica, access tokens, (c) describes `file` as attachment support whereas the Go package is a
   30-line hasher and attachments live in `storage/attachstore` + `embed`.
7. **Counting traps.** Package-level goroutine/lock counts are easy to inflate by including `_test.go`
   files; section 6 was recounted on non-test sources only.
8. **Driver opens with zero `FileRuntimeOptions`.** `driver/database.go` calls
   `embed.OpenFileHost(root, embed.FileRuntimeOptions{})`, not `FileRuntimeOptionsFromEnv()`, so an
   embedded `database/sql` user gets mapped graph segments, tree.file and bounded live bodies **off**
   (and no S3), while `kdb-service` and `embed.OpenFileRuntime` get them on. Memory figures measured
   on the service do not carry over to the driver.
9. **Go client has no write-session pool.** The per-namespace session pool (#337) and loopback zstd
   decline are TS `@kdb/client` only. A Go app's concurrent `Exec`s on one namespace serialise on
   `namespaceState.dml` and on the server's session ticket; open several `Client`s or `Tx`s for
   parallel writes.
10. **Attachment preflight is keyed by adapter address.** `transaction.hasBlob` finds the owning
    runtime through `RegisterAttachmentBlobs` by the adapter's pointer. A transaction run against a
    *wrapping* adapter (not the one the runtime registered) misses the registry and falls back to
    `BlobPresence` (no production implementor) then `ReadBlob`, which does not see large blobs in
    `attachstore`, so a valid attachment put is refused ("attachment blob not stored"). Latent:
    today's server and driver engines pass `rt.Storage` itself, the registered adapter.
11. **Long hold refuses, not queues.** During a peer snapshot install the namespace's writers get
    `BusyError` immediately (`writeGate.beginLongHold`); only `acquireThroughHold` callers wait. A
    client that treats BUSY as fatal fails throughout a bootstrap.
12. **Kotlin peer sync is partial v2.** A Go node peering with a Kotlin node gets `UNSUPPORTED` for
    grafts, repair (`TREE_NODES`/`OBJECT_FETCH`/`DOC_FETCH`), projections, squash, meta view, home and
    `REFS_WATCH` when the Kotlin node is the host; the interop suite covers only the supported set.
