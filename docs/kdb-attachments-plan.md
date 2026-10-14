# Attachments: named binary attachments on records

Status: in progress. PR 1 (model, storage, GC, embed API) implements the parts marked **done**. It
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
6. **Git-like deltas where small.** The newest version is always whole. A replaced version is
   re-stored as a delta against its successor when that is under half its size.
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
  - API: `List`, `Count`, `Get(doc, id)`, `GetByName`, `Put`, `Delete`, `DeleteAll`, `Walk`.
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
  - A delta is read through its instruction index, so only the base ranges and literal bytes a read
    touches are fetched, and no version is rebuilt in memory.
- **Read pins.** An open reader pins every segment it resolved, including a delta's base chain.
  - `StoreAsDelta` and `Collect` queue a pinned segment for deletion, and the last reader to close
    deletes it.
  - Pins are leases (`ReadLease`, default one hour). A stalled reader is released by the next
    `Collect` and fails with `ErrReadLeaseExpired`.
- **Deltas** (`delta.go`).
  - Encoding indexes the base by block hash, with the block size scaled so the index stays under
    about 1M entries, then rolls a hash over the target through 1 MiB page caches. Neither version
    is held whole.
  - A version is diffed only if its size is within `DeltaMaxBytes` (default 256 MiB, because that
    bounds the index), and chain depth is capped at 8.
  - Larger versions stay whole. A streaming rolling-hash delta for multi-GB files is a follow-up.
- **GC.** `Collect` deletes what the keep-set rejects, keeps the bases of kept deltas, and deletes
  segments no pointer names (unfinished uploads).

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
- **Small blobs under history=none.** Small attachment blobs live in the engine's blob store, so its
  history=none compaction keeps them through `ServerEngine.SetExtraReachable`.

## Next PRs

- **PR 2, SQL.**
  - `INSERT ... WITH ATTACHMENT [<name>] <bytes> [MIME ...]` (several per row allowed),
    `UPDATE ... SET ATTACHMENT <name|id> = <bytes>`, and `DROP ATTACHMENT <name|id>`.
  - The `_attachments` virtual column, which lists `{id, name, size, mime, hash}`.
  - `ATTACHMENT(<name|id>)` and `ATTACHMENT_COUNT()`.
  - `<bytes>` is one of `X'..'`, `FROM_BASE64(..)`, `?` or `STREAM ?`.
  - `CellBytes`, `ParamBytes` and `MaxSqlLiteralBytes`.
- **PR 3, wire, server and Go client.**
  - Wire feature `attachments`, which requires `binary-frames`.
  - `ATTACHMENT_BEGIN`/`CHUNK`/`END` uploads (resumable) and `ATTACHMENT_GET`/`DATA` downloads
    (ranged, verified per chunk, carrying a final `superseded` notice), with codes 0x44–0x4F.
  - A change-stream event for attachment changes.
  - `CellKindBytes`.
  - The Go client API.
- **PR 4, TS and Kotlin clients** (JDBC `setBytes`/`setBinaryStream`/`getBytes`/`getBinaryStream`).
- **PR 5, peer sync, migration and upgrade e2e.**
  - `BLOB_FETCH` is 0x42/0x43, gated on peer capability `attachments`. Requirements agreed with the
    client-replica session (Track D, codes 0x50–0x57):
    - **Entries:** each is `(docId, attachmentId, commit, hash, offset, maxBytes)`; every chunk
      carries the total size and its inclusion proof.
    - **Batched,** with results in request order and the response paged by bytes.
    - **Any readable old ref.** The server checks read permission on the document and that the table
      at the commit references the hash.
    - **Per-entry errors,** including "not retained" and "superseded".
    - **Tree diffs:** `TREE_NODES` gets a tree selector so the attachment table is diffed like the
      document tree.
  - Squash, snapshot, graft and boundary transfers diff the attachment root and fetch blobs before
    `PersistSquash` or the checkpoint.
  - Projected pages carry refs, and a replica can upload (both pending the owner's decision D7 in
    the replica plan).
  - `kdb file migrate <ns>`.
  - Upgrade e2e, covering an old binary meeting checkpoint v5, graph v5, the new op branches and a
    backup.
- **PR 6, Kotlin parity.** Kotlin attachment tables, the store, writes and the embed API, checked
  against the same goldens. `BLOB_FETCH` parity waits on Kotlin peer sync v2.

## Known limits of PR 1

- **No peer blob transfer yet.** Commits and roots replicate, but blobs do not until PR 5.
- **All-history GC walks commit headers.** It loads ops only for commits whose root changed. A
  per-namespace attachment index would avoid the walk.
- **Set pool.** The set pool (ref metadata only, never bytes) lives in memory and grows with
  attachment versions. Spilling it, or rebuilding sets on demand, is a follow-up for very large
  version counts.
- **Backups.** Backups do not copy attachment segments yet, and a restored node must seed its
  tables (PR 5).
- **Delta size.** Versions over `DeltaMaxBytes` are not delta-encoded.
