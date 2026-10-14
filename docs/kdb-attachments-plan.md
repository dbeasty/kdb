# Attachments: one binary attachment per record

Status: in progress. PR 1 of 5 (model, storage, GC, embed API) implements the parts marked **done**.
It replaces the "the JSON record holds a blobHash" design
(`docs/kdb-spec-layer1-component3b-file-attachments.md`). Old `FileWriteOp` and `blobHash` data
stays readable, and nothing new writes it.

## Goal

A record (a document, addressed by namespace and `_id`) has at most one binary attachment.

- **The JSON is never touched.** Replacing the bytes leaves the document's JSON and its content hash
  unchanged.
- **Atomic and versioned.** The attachment commits together with the record and is versioned by the
  commit DAG, so an as-of read returns the attachment as of that commit.
- **SQL** can write the bytes as a literal, a bound parameter, or a stream (PR 2).

## Decisions

These are the owner's decisions, recorded in the order they were made (2026-10-13).

1. **Addressing.** One attachment per record, with no name. It is addressed by `(ns, _id)`, which
   never changes. Path-addressed files were considered and rejected.
2. **Hashing.** A side table, the *attachment table*, maps each document id to its attachment ref
   (content hash, size, mime). The table has its own Merkle root, and every commit records that root
   next to `DocumentTreeHash` as `AttachmentTreeHash` (commit payload field 10).
   - Keeping it out of the document trie means the slab, `leafHash`, tree segments and Kotlin's tree
     code are unchanged.
   - Keeping it in a hash peers compare means sync can see an attachment change.
3. **Storage follows the target system.**
   - Small blobs are packed into the ordinary blob store, which is already a packed, group-committed
     LSM.
   - Large blobs are standalone segments.
   - S3 will get its own strategy later. Everything goes through the platform I/O shim, which
     already has an S3 implementation.
4. **Git-like deltas, where small.** The newest version of an attachment is always stored whole. A
   replaced version is re-stored as a delta against its successor when that is under half its size.
   Chain depth and diff size are capped.
5. **History setting.** The engine either keeps all attachment history (`AttachmentHistoryAll`, the
   default) or only the latest (`AttachmentHistoryLatest`). In latest mode, a replaced or deleted
   attachment's bytes go at the next maintenance pass, and an as-of read of them fails with
   `ErrAttachmentNotRetained`.
6. **Permissions.** Reading an attachment uses the record's read permission, and changing one uses
   its write permission. There is no separate ACL.
7. **Negotiation.** The wire protocol may break, but a wire feature (`attachments`) and a peer
   capability are negotiated anyway, so rolling upgrades keep working.

## Model (done)

- **Ops.** `AttachmentPutOp{DocID, ContentHash, Size, Mime}` is union branch 4, and
  `AttachmentDeleteOp{DocID}` is branch 5. Both are in Go and Kotlin. They have JSON and binary wire
  forms (kinds `attachmentPut` and `attachmentDelete`; binary op kinds 5 and 6).
- **Apply rules** (`document.AttachmentTable.Apply`):
  - a put sets the attachment;
  - an attachment delete removes it;
  - a document `DeleteOp` removes it too;
  - a put on a document that will not exist after the transaction is rejected.
- **Table and root.** `AttachmentTable` reuses the document trie as its Merkle tree
  (doc id → `AttachmentRef.Hash()`), so changing one attachment costs O(depth) and versions share
  nodes. `Root()` is nil for an empty table, and a nil root is omitted from the payload (Go and
  Kotlin alike), so a commit with no attachments hashes exactly as before.
- **Ref hash.** `AttachmentRef.Hash` is SHA-256 of
  `'A' | contentHash(32) | be64 size | be32 len(mime) | mime`. It is pinned by a golden test; any
  other implementation must match it.
- **The DAG derives every root** (`dag/attachments.go`): a commit's table is its first parent's
  table with the commit's ops applied.
  - Every append path gets this for free: transactions, embedded puts, reverts, and merges whose ops
    carry the merged branch's net effect.
  - Tables are cached by root. A miss is rebuilt from the nearest cached ancestor, and every step is
    checked against the root the commit recorded.
  - `Squash` refuses a window holding attachments.
- **Storage tiers.** The root survives every place a commit is stored:
  - the resident graph tier: a side map plus a record flag;
  - graph file segments: version 5 adds an `ATTR` section, and version 4 files are still read;
  - packed checkpoint commits: flag bit 2.
- **Checkpoints** of a namespace with attachments are written as version 5. They carry the heads'
  attachment tables, so opening the namespace needs no history, and older builds refuse them instead
  of misreading them. Such a namespace is never written as legacy version 2.
- **Transactions** (`transaction/attachments.go`):
  - preflight: the blob must be stored, and the document must exist after the transaction;
  - conflicts: an attachment change conflicts with a concurrent change to the same attachment, and
    with a concurrent delete of the document; a document delete conflicts with a concurrent
    attachment change; a JSON-only write conflicts with neither;
  - leases and authorization treat an attachment change as a write to its document.
- **`transaction.Builder`** has `Attach` and `Detach`.

## Storage (done: `storage/attachstore`)

- **Writes are streamed.** An upload stays in memory up to `SmallMaxBytes` (256 KiB) and then
  spills to a segment chunk by chunk. `MaxBytes` defaults to 1 GiB. A blob is durable (fsynced)
  before `Finish` returns, and the commit that references it is built only after that.
- **Pointers.** The shim has no rename, so a large blob is found through a pointer segment
  `h-<hash>.<generation>` that names the segment holding its bytes, or a delta and its base. Moving
  a blob from whole to delta writes a higher generation before the old one is deleted, so a crash at
  any point leaves a readable blob.
- **Reads are streamed.** A read of a delta is materialized in memory (≤ `DeltaMaxBytes`) and checked
  against its hash.
- **The empty attachment** (`sha256("")`) is never stored and is always present.
- **GC** (`Collect`) deletes what the keep-set rejects, apart from the bases of kept deltas. It also
  deletes segments no pointer names, such as uploads that never finished.

## Embed API (done)

- `PutAttachment(ns, docID, io.Reader, mime)`, `GetAttachment(ns, docID, asOf)` (zero `asOf` means
  head), `DeleteAttachment`, `AttachmentInfoAt` and `AttachmentTableAt`.
- `SetAttachmentHistory` and `SetAttachmentConfig`.
- **`Maintain` calls `CompactAttachments`.** It re-stores replaced versions as deltas (all-history
  mode) and collects:
  - in latest mode, everything a branch or tag head does not reference;
  - in all-history mode, everything no reachable commit references.
- **Small blobs under history=none.** Small attachment blobs live in the engine's blob store, so its
  history=none compaction keeps them through `ServerEngine.SetExtraReachable`.

## Next PRs

- **PR 2, SQL.**
  - `INSERT ... WITH ATTACHMENT <bytes> [MIME ...]` (per row in multi-row inserts),
    `UPDATE ... SET ATTACHMENT = ...` and `DROP ATTACHMENT`.
  - The `_attachment` virtual column and `ATTACHMENT()`.
  - `<bytes>` is one of `X'..'`, `FROM_BASE64(..)`, `?` or `STREAM ?`.
  - `CellBytes`, `ParamBytes` and `MaxSqlLiteralBytes`.
- **PR 3, wire, server and Go client.**
  - Wire feature `attachments`, which requires `binary-frames`.
  - Bytes parameters on SqlExec.
  - `ATTACHMENT_BEGIN`/`CHUNK`/`END` uploads and `ATTACHMENT_GET`/`DATA` downloads, with codes from
    0x44 (0x42 and 0x43 are reserved for BLOB_FETCH).
  - `CellKindBytes`.
  - Client `PutAttachment`/`GetAttachment`/`DeleteAttachment` and `DocWrite.Attachment`.
- **PR 4, TS and Kotlin clients** (JDBC `setBytes`/`setBinaryStream`/`getBytes`/`getBinaryStream`).
- **PR 5, peer sync, migration and upgrade e2e.**
  - Commits already carry the ops and the root.
  - The receiver fetches missing blobs by hash (`BLOB_FETCH`, 0x42/0x43) before adopting, gated on
    peer capability `attachments`; a peer without it gets a typed refusal.
  - Squash, snapshot, graft and boundary transfers move state without replaying commits. They must
    diff the attachment root and fetch blobs before `PersistSquash` or the checkpoint.
  - `kdb file migrate <ns>` and upgrade e2e tests.

## Known limits of PR 1

- **No peer blob transfer yet.** A commit with attachments reaches peers, but its blobs do not until
  PR 5, so a read there fails with `ErrAttachmentNotRetained`.
- **All-history GC walks commit headers.** It loads ops only for commits whose root changed. A
  per-namespace attachment index would avoid the walk.
- **Backups** (`backup` package) do not copy attachment segments yet.
- **Kotlin** reads the ops and the root, but does not compute attachment tables, so it does not
  write attachments.
