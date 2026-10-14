# Attachments: named binary attachments on records

Status: built. PR 1 (#214: model, storage, GC, embed API) and the follow-up PR (SQL, wire,
server, Go/TS/Kotlin clients, peer sync, migration, backups, Kotlin engine parity, upgrade e2e)
implement everything below; what is still open is under "Known limits". It
replaces the "the JSON record holds a blobHash" design
(`docs/kdb-spec-layer1-component3b-file-attachments.md`). Old `FileWriteOp` and `blobHash` data
stays readable, and nothing new writes it.

## Goal

- A record (a document, addressed by namespace and `_id`) has any number of binary attachments.
- Each attachment has an **ID**: a UUID assigned when it is created, unique and never changed.
- Each attachment has a **name**, unique within the record. It defaults to the ID's string form.
- Either the ID or the name refers to an attachment. One attachment can be replaced, renamed or
  deleted without touching the others.
- A record's attachments can be listed and counted.
- Attachments never touch the record's JSON or its content hash.
- They commit atomically with the record and are versioned by the commit DAG, so an as-of read
  returns them as of that commit.
- SQL can write the bytes as a literal, a bound parameter, or a stream (PR 2).

## Decisions

These are the owner's decisions from 2026-10-13. Later decisions supersede earlier ones.

1. **Several attachments per record**, each with a permanent ID and a unique-per-record name;
   either one references it. (This superseded "one per record". Path-addressed files were rejected.)
2. **Content identity is a Merkle root over chunk hashes** (scheme 1, below).
   - Chunks are hashed in parallel while the bytes stream in.
   - Every chunk can be verified on its own: by a reader before it hands bytes out, and by a peer
     or client with an RFC 6962 inclusion proof.
   - A scheme byte travels with every ref, so later schemes share the format.
3. **No size limit.** Gigabytes to terabytes, like git. `MaxBytes` is opt-in.
4. **Hashing of the table.** A side table, the *attachment table*, maps each document to its set
   of attachments. It has its own Merkle root, and every commit records that root next to
   `DocumentTreeHash` as `AttachmentTreeHash` (commit payload field 10).
   - Keeping it out of the document trie means the slab, `leafHash`, tree segments and Kotlin's
     tree code are unchanged.
   - Keeping it in a hash peers compare means sync can see an attachment change.
5. **Storage follows the target system.**
   - Small blobs are packed into the ordinary blob store.
   - Large blobs are standalone segments.
   - S3 will get its own strategy later. Everything goes through the platform I/O shim, which
     already has an S3 implementation.
6. **Git-like deltas, any size.** The newest version is always whole. A replaced version is
   re-stored as a delta against its successor when that is under half its size - whatever its
   size, with memory bounded independent of it.
7. **History setting.** Keep all attachment history (`AttachmentHistoryAll`, the default) or only
   the latest (`AttachmentHistoryLatest`).
8. **Bytes are never held whole.** Reads stream chunks from storage. A read follows the same
   snapshot as the JSON read and pins the version it started on.
   - A send in progress (to a client, or to a peer during sync) finishes on that version even if a
     new one commits meanwhile.
   - The old version is disposed of only after the read completes.
   - The reader is told a newer version may exist (PR 3 and PR 5 wire frames).
9. **Permissions.** Reading an attachment uses the record's read permission, and changing one uses
   its write permission. There is no separate ACL.
10. **Kotlin parity.** Kotlin is fully compatible with Go, through a tracked PR (below).
11. **Negotiation.** The wire may break, but a wire feature (`attachments`) and a peer capability
    are negotiated anyway, so rolling upgrades keep working.

## Content identity: scheme 1 (done, `document/attachment_hash.go`)

```
chunk i  = bytes [i*1MiB, min((i+1)*1MiB, size))
leaf i   = SHA-256(0x00 || chunk i)
tree     = RFC 6962 Merkle tree hash over the leaves (n>1: SHA-256(0x01 || MTH(first k) || MTH(rest)),
           k = largest power of two below n); a zero-byte blob has one leaf, of the empty chunk
identity = SHA-256(0x02 || be64(size) || tree)
```

- Golden values are pinned in Go tests and checked against an independent implementation of the
  definition. Kotlin and TS must reproduce them.
- `MerkleBuilder` computes the tree with O(log n) state, so streams of any size hash in constant
  memory.
- `MerkleInclusionProof` and `VerifyMerkleInclusion` are the RFC 6962 audit path, for per-chunk
  verification across the wire.

## Model (done)

- **Ops.**
  - `AttachmentPutOp{DocID, AttachmentID, Name, Scheme, ContentHash, Size, Mime}` is union branch 4.
    It creates the attachment, or replaces its bytes and name.
  - `AttachmentDeleteOp{DocID, AttachmentID}` is branch 5.
  - Both exist in Go and Kotlin, with JSON and binary wire forms.
  - Ops always carry IDs. A reference by name is resolved to an ID when the change is staged, so
    replay is deterministic.
- **Apply rules** (`AttachmentTable.Apply`):
  - a put creates or replaces one attachment;
  - an attachment delete removes one;
  - a document `DeleteOp` removes all of the document's attachments;
  - a name another attachment of the record already has is refused;
  - a put on a document that will not exist after the transaction is refused.
- **Table** (`document/attachment_table.go`).
  - The document trie maps doc id → `AttachmentSet.Hash()`. A set is the document's attachments
    sorted by ID, hashed as `'S'` then, per attachment, ID(16) | be32 len(name) | name | ref hash.
  - The ref hash is SHA-256(`'A'` | scheme | contentHash | be64 size | be32 len(mime) | mime).
  - Changing one attachment re-hashes its document's set and O(depth) trie nodes.
  - `Root()` is nil for an empty table, and a nil root is omitted from the commit payload, so a
    commit without attachments hashes exactly as before.
  - API: `List`, `Count`, `Get(doc, id)`, `GetByName`, `Put`, `Delete`, `DeleteAll`, `Walk`. The
    reads return an error: a set can be on disk (below).
- **Set pool** (`document/attachment_set_pool.go`). Sets live in a pool keyed by their hash and
  shared by every table of a DAG.
  - Backed by a store, the pool keeps about a budget of sets in memory: 8 MiB by default,
    `StorageOptions.AttachmentSetBudgetBytes` / `KDB_ATTACHMENT_SET_BUDGET`. It writes a set out
    when it evicts it, the first time. A set it does not hold is read back and checked against its
    hash. A set the store lacks or has wrong is an error (`ErrAttachmentSetMissing`,
    `ErrAttachmentSetCorrupt`), never an empty or different set.
  - The file runtime's store is the namespace's blob store. Writes go to the memtable without a WAL
    record: every table a process holds was seeded or derived in that process, which put its sets
    first, so a crash loses nothing anyone reads. Read-only runtimes and engines without platform
    I/O keep every set in memory, as before.
  - Under history=none the blob store's merge keeps the sets of the branch, tag and seeded tables,
    and the sets written in the last two maintenance passes (a commit may still be on its way). The
    DAG then drops its cached tables of other commits, which are rebuilt from history if asked
    for. A set a new table names again is written again when evicted.
  - Seeded tables and transactions' tables join the DAG's pool, so what is derived from them is
    bounded too.
  - Kotlin keeps no store. Its tables share their sets through a persistent map (`UuidHamt`), so a
    cached version costs what changed. Before, every change copied the whole map, which cost time
    in every document and up to 64 full copies across cached tables.
- **The DAG derives every root** (`dag/attachments.go`).
  - A commit's table is its first parent's table with the commit's ops applied.
  - Tables are cached by root. A miss is rebuilt from the nearest cached ancestor, and each step is
    checked against the recorded root.
  - `Squash` refuses a window holding attachments.
- **The root survives every commit store**:
  - the resident graph tier (a side map plus a record flag);
  - graph file segments (version 5 adds an `ATTR` section; version 4 files still read);
  - packed checkpoint commits (flag bit 2);
  - checkpoints of a namespace with attachments, written as version 5 with the heads' tables and
    never as legacy version 2.
- **Transactions** (`transaction/attachments.go`).
  - Preflight checks: ID present, valid and unique name, blob stored, document present.
  - A change to one attachment conflicts with a concurrent change to the same attachment, or with a
    concurrent delete of the document.
  - A document delete conflicts with a concurrent change to any of its attachments.
  - Changes to different attachments of a record do not conflict, and neither do JSON-only writes.
  - Leases and authorization treat an attachment change as a write to its document.
- **`transaction.Builder`** has `Attach(doc, attachmentID, name, hash, size, mime)` and
  `Detach(doc, attachmentID)`.

## Storage (done, `storage/attachstore`)

- **Upload.** Bytes stream in 1 MiB chunks, hashed in parallel (`HashWorkers`, default GOMAXPROCS).
  - A blob is durable (fsynced) before `Finish` returns, and the commit is built after that.
  - Small blobs (≤ 256 KiB) go to the ordinary blob store under their content hash
    (`WriteKeyedBlob`).
  - Larger ones become a bytes segment plus a leaves segment (32 bytes per chunk).
- **Pointers.** A large blob is found through `h-<hash>.<generation>`, which names its bytes
  segment, its leaves segment, and for a delta its base. The shim has no rename, so a higher
  generation is written before a lower one is retired; a crash at any point leaves a readable blob.
- **Reads** (`Store.Open` → `*Reader`).
  - Sequential (`Read`) and random access (`ReadAt`, for ranged fetches).
  - Each chunk is checked against its stored leaf before it is returned. Leaves are checked once
    against the root by streaming them.
  - A delta is read through its instructions, so only the base ranges and literal bytes a read
    touches are fetched, and no version is rebuilt in memory. A format 2 delta's instructions are
    framed and indexed on disk: a read loads one frame (at most 4096 instructions) at a time.
- **Read pins.** An open reader pins every segment it resolved, including a delta's base chain.
  - Delta jobs and `Collect` queue a pinned segment for deletion, and the last reader to close
    deletes it.
  - Pins are leases (`ReadLease`, default one hour). A stalled reader is released by the next
    `Collect` and fails with `ErrReadLeaseExpired`.
- **Deltas** (`delta.go`, `deltajob.go`). Any size; chain depth is capped at 8 (`DeltaMaxDepth`).
  `DeltaMaxBytes` can still cap the size diffed; it is unlimited by default.
  - **Chunks first.** A target chunk whose leaf equals the base's leaf at the same offset is a copy
    instruction without reading a byte, so an in-place edit to a TB file costs the chunks touched.
  - **Index only what did not match.** Only base chunks with no equal target chunk at their
    offset are indexed, by block hash; the block grows with those bytes so the index stays under
    `DeltaIndexEntries` (default 1M, ~40 MB) whatever the size.
  - **Rolling hash where content shifted.** The target is scanned with a rolling hash through a
    few cached pages; a hit is extended backwards and forwards by page compares, and by whole
    chunks again where the offsets line up. Neither version is held whole.
  - **Format 2** (pointer flag 4): frames (literal bytes, then that frame's instructions), a
    32-byte-per-frame index and a footer (`KDBDLT2`). Format 1 deltas (written before this, at
    most 256 MiB) still read. The layout is documented in `delta.go`.
  - **Bounded, resumable steps.** A delta job runs in phases - index the base, encode, verify
    every chunk of the delta against the old version's leaves, swap the pointer - in steps of
    `DeltaStepBytes` (default 4 GiB; `--attachment-delta-step-mb` / `KDB_ATTACHMENT_DELTA_STEP_MB` /
    `attachmentDeltaStepMb` on the service). A maintenance pass that runs out of budget stops
    with the job pending and its mark unmoved; the next pass resumes it. Between steps it holds
    no pins or locks, only its index and progress.
  - **Crash safety.** Nothing replaces the whole copy until the delta has been read back and
    verified; the swap is the usual higher-generation pointer. A crash mid-job leaves only
    unnamed segments, which `Collect` retires, and the job starts over (progress is in memory).
- **GC.** `Collect` deletes what the keep-set rejects, keeps the bases of kept deltas, and deletes
  segments no pointer names (unfinished uploads).
- **Upload grace.** Every writer uploads first and commits after (client API, SQL `STREAM ?`, a
  replica's push), so `Collect` keeps a finished blob no commit references yet for `UploadGrace`
  (default one hour; `StorageOptions.AttachmentUploadGrace` in embed, `--attachment-upload-grace` /
  `KDB_ATTACHMENT_UPLOAD_GRACE` / `attachmentUploadGrace` on the service).
  - The finish time is in the pointer (format `KDBATP3`; `KDBATP2` still reads, with no grace), so
    the hold survives a restart between upload and commit.
  - A wire upload's `Expect` (the store already has the blob) and a re-upload of a stored blob
    restart the grace through `Store.Hold`. The pointer is rewritten only when its recorded time is
    half a grace old.
  - The hold ends once the commit referencing the blob is durable (`PersistingCommitDAG` calls
    `Store.Committed`), or a `Collect` sees it referenced; references alone decide after that, so a
    replaced version is not kept for an hour. After a restart, a blob whose recorded finish time
    is within the grace is held again until a `Collect` sees it referenced.
  - Kotlin (`AttachmentBlobStore`) holds the same way, default one hour,
    `setAttachmentUploadGrace` on the embed runtime. Its pointer format is unchanged: the hold is an
    empty `g-<hash>.<millis>` segment written before the pointer, since the shim cannot replace a
    segment atomically and an older build would read a new pointer format as torn and delete the
    blob. A pointer with no hold segment (stored before holds existed) has no grace. A re-upload
    restarts the grace (rewritten once half a grace old). The hold ends when `commitViaEngine`
    commits a put (the file runtime's commit is durable by then) or a collect sees the blob
    referenced; the hold segment is deleted then, so unlike Go a restart does not revive it.

## Embed API (done)

- `PutAttachment(ns, doc, reader, PutAttachmentOptions{ID, Name, Mime})`:
  - with an ID, creates or replaces that attachment;
  - with only a name, replaces the attachment of that name, or creates one with a fresh ID;
  - with neither, creates an attachment with a fresh ID named after the ID.
- `GetAttachment(ns, doc, AttachmentByID(id) | AttachmentByName(name), asOf)` returns a pinned,
  verifying reader.
- `ListAttachments`, `CountAttachments`, `AttachmentInfoAt`, `RenameAttachment`,
  `DeleteAttachment` and `AttachmentTableAt`.
- `SetAttachmentHistory` and `SetAttachmentConfig`.
- **`Maintain` calls `CompactAttachments`.** It re-stores replaced versions as deltas (all-history
  mode) and collects:
  - in latest mode, everything a branch or tag head does not reference;
  - in all-history mode, everything no reachable commit references.
- **Reference index** (`attachstore/ref_index.go`, segment `<ns>/attachment/r-<gen>`). All-history
  collection keeps a durable index: the content hashes the history below some heads references,
  those heads, and a token for where retained history ends (`dag.HistoryFloor`: the horizon and
  the number of stubbed commits).
  - A pass whose recorded heads are still below the current ones, on the same floor, reads only the
    commits since (`dag.CommitsAbove`, a generation-ordered walk).
  - Anything else rebuilds the index from retained history once: a truncation, an archive, a deleted
    or reset branch, or a lost or torn index.
  - A commit whose parent is not here (a shallow root, a truncated floor) contributes its whole
    table.
  - The index is read per pass and not held between passes. `AttachmentMaintenance` reports
    `Scanned` and `Rebuilt`.
- **Replacements come from the DAG.** A commit that puts an attachment whose first parent held a
  different version (same document and attachment ID) makes the pair, however the commit arrived:
  embedded `PutAttachment`, a client or SQL transaction, or peer sync. The store's mark segment
  (`<ns>/attachment/m-<gen>`) records the branch and tag heads of the last pass, so the next one,
  after a restart too, reads only the commits since.
- **Small blobs under history=none.** Small attachment blobs live in the engine's blob store, so its
  history=none compaction keeps them through `ServerEngine.SetExtraReachable`.

## Built in the follow-up PR

- **SQL** (`sql/attachments.go`): `INSERT ... WITH ATTACHMENT ['name' =] <bytes> [MIME ..]`,
  `UPDATE ... SET ATTACHMENT <sel> = <bytes>`, `DROP ATTACHMENT <sel>`, `RENAME ATTACHMENT <sel>
  TO 'n'` (`<sel>` a name or `ID '<uuid>'`); `_attachments`, `ATTACHMENT()`, `ATTACHMENT_COUNT()`,
  `ATTACHMENT_SIZE()`, `ATTACHMENT_MIME()`, `FROM_BASE64()`, `X'..'`; `STREAM ?` binds an upload.
  The engine reaches the attachment store through `sql.SetAttachmentBlobs`.
- **Wire** (`wire/attachment_ops.go`), feature `attachments`, always binary bodies, goldens pinned
  for TS and Kotlin: `ATTACHMENT_UPLOAD`/`_RESULT` 0x44/0x45 (resumable; `Expect` skips a blob
  the server holds), `ATTACHMENT_GET`/`DATA` 0x46/0x47 (paged, pinned `ReadID`, `Superseded` on the
  last page, `Close`), `ATTACHMENT_LIST`/`_RESULT` 0x48/0x49; `CellKindBytes` (6); parameters
  `{"$bytes": b64}` and `{"$attachment": {hash, size}}`; error code `NOT_FOUND`. Creating,
  replacing, renaming and deleting are ordinary transactions with attachment ops.
- **Damaged stored bytes** (`attachstore.ErrCorrupt`: a chunk, the leaves or a delta that no
  longer hash to what they are stored under) are refused with error code `INTEGRITY` on every
  reply that reads them - `ATTACHMENT_DATA` (first or later page), SQL `ATTACHMENT()`, and a
  `BLOB_FETCH` entry - logged at error level with namespace/doc/attachment/hash and counted in
  `kdb_attachment_corrupt_reads_total`. Clients type it: Go `client.ErrAttachmentCorrupt`, TS
  `KdbAttachmentCorruptError`, Kotlin `KdbAttachmentCorruptException` (SQLState `XX001`) - the
  same error as the client's own failed verification. The remedy is another replica or repair,
  never a retry against the same node.
- **Verified ranged reads**, feature `attachment-proofs`: `ATTACHMENT_GET` with `Proofs` (11) gets
  pages rounded to 1 MiB chunk boundaries (at most 4 MiB), and `ATTACHMENT_DATA` carries the blob's
  `Tree` (12), each chunk's RFC 6962 audit path (13, BLOB_FETCH's encoding) and `Scheme` (14). A
  client checks `AttachmentIdentity(Tree, Size) == Hash` and every chunk's proof before handing out
  any of its bytes, and trims the pages to the caller's range. An offset past the end fails
  `SCHEMA_VIOLATION` with `Info` still set (clients raise a typed range error); a `Close` reply
  carries `Superseded`, so a range that ends before the attachment does still learns it. Go
  `OpenAttachmentRange` / `ReadAttachmentAt`; TS `openAttachmentRange` / `readAttachmentAt`; Kotlin
  `KdbAttachments.openRange` / `readAt` / `getRange`, and `KdbAttachments.blob()`, a `java.sql.Blob`
  whose `getBytes(pos, len)` / `getBinaryStream(pos, len)` read verified ranges. The Kotlin server
  acks neither feature. A chunk failing its proof is the same corrupt error as below.
- **Server** (`server/attachments_wire.go`); **Go client** (`client/attachments.go`); **TS client**
  (`packages/kdb-client`); **Kotlin wire codec, client and JDBC** (`kdb-wire`, `kdb-jdbc`); the
  Kotlin server answers the messages `UNSUPPORTED`.
- **Peer sync** (`peersync/attachments.go`), capability `attachments`:
  - `BLOB_FETCH`/`_RESULT` 0x42/0x43, entries `(docId, attachmentId, commit, hash, offset,
    maxBytes)`, batched, paged by bytes, per-entry errors, each chunk with its RFC 6962 audit path;
  - a puller fetches what a page attaches before storing it; a pusher uploads over the sync
    session (`ATTACHMENT_UPLOAD` + `Expect`) before its `REF_UPDATE`;
  - `TREE_NODES` with `Tree: "attachments"` serves the attachment trie, with each listed document's
    attachments; `PROJECT_PAGE` writes carry their attachments;
  - snapshots carry the commit's packed table (graft and boundary extension too); squash catch-up
    is refused for a head with attachments (replay);
  - a peer without the capability is refused commits carrying attachments (typed `UNSUPPORTED`).
- **Merges** (`dag/attachment_merge.go`): every two-parent commit carries the attachment ops that
  three-way merge its parents' tables against their merge base (per attachment ID; a version beats
  a removal; ties by a fixed order; a contested name is kept by the smaller ID). Go and Kotlin
  produce the same merge commits.
- **Small blobs inline**: a blob up to `SmallMaxBytes` is stored in its pointer segment, so every
  attachment byte is in the attachment store's own segments (blobs an earlier build put in the
  engine's blob store are still read from there).
- **Backups and restore**: attachment segments are backed up after the log in content-addressed
  parts of at most 64 MiB (deduplicated across incremental backups); `FetchToDir` and
  `HybridRestore` bring them back.
- **`kdb file ls|put|get|rm|migrate`**; `MigrateFileAttachments` moves the earlier design's
  `blobHash` file records to attachments.
- **Kotlin engine parity** (`kdb-document`, `kdb-dag`, `kdb-transaction`, `kdb-embed`): hashes,
  tables, roots, merges, preflight and conflicts identical to Go (checked against Go fixtures); a
  Kotlin attachment store and embed API.
- **Write-back projections** (follow-up): `PROJECT_WRITE` docs carry a document's whole
  attachment set after the write (`attachmentsChanged`, `attachments`), guarded by the set the
  projection held (`attachmentsBase`), and attachment-only writes (`keepBody`); a refused write
  reports the source's attachments. A projection session may upload and `BLOB_FETCH`. Delta pages
  also list documents whose attachments alone changed. A JSON-only write-back no longer wipes the
  document's attachments at the source (it was a delete plus a write; now a replace). The Go
  projection node keeps attachments both ways.
- **Pushed grafts** (follow-up): the pusher uploads the root's blobs before `GRAFT_PUSH`; the host
  keeps the table from the last page, refuses a graft with attachments it lacks the table or bytes
  of, and seeds the table. Tables seeded from peers (graft, boundary, snapshot) are persisted
  (`kdb:attachment-seeds:<ns>`) and seeded again on open, so a crash before the first checkpoint
  keeps them.
- **Tests**: unit and golden tests per layer, two-node sync, TS live interop against Go, e2e
  (`test_attachments.py`), upgrade (`test_upgrade.py`: mixed mesh, rollback refused).

## Known limits

- **The store's pointer map.** `attachstore.Store` holds one pointer per stored blob in memory, so
  under all-history its size still grows with the number of versions. Paging it like the set pool is
  a follow-up.
- **A collection pass** still lists every stored blob (`Collect`), and a rebuild of the reference
  index reads all of retained history once.
- **Delta jobs restart after a crash.** Progress is in memory, so a restart mid-job redoes it from
  the start. Versions found not worth a delta are remembered per process only.
- **The Kotlin store** has its own on-disk layout, no deltas and no read leases; Kotlin does not
  read Go's v5 checkpoint and graph formats and has no blob peer transfer.
- **Downgrade** after attachments exist is impossible: older builds refuse the log.

## Known limits of PR 1 (still open unless listed above as built)

- **No peer blob transfer yet.** Commits and roots replicate, but blobs do not until PR 5.
- ~~All-history GC walks commit headers~~: replaced by the reference index.
- ~~Set pool held in memory~~: bounded and spilled to the blob store.
- **Backups.** Backups do not copy attachment segments yet, and a restored node must seed its
  tables (PR 5).
- **Delta size.** Lifted: versions of any size are delta-encoded (see Deltas above).
