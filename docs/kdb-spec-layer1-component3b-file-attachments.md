# KDB Component Spec — Layer 1, Component 3b

# File Attachments (Metadata, Bundles, Compression)

# Package: `dev.kdb.document` (conventions) · `dev.kdb.file` (future helpers)

# Status: **Draft** — normative for new CLI / transaction / GC work

# Depends on: [Component 3 — Document + Commit Model](kdb-spec-layer1-component3-document-commit-model.md), Layer 0 Codec, Component 9 Storage Adapter, [Layer 8 File Persistence](kdb-spec-layer8-file-persistence-plan.md)

-----

## 1. Purpose

Defines how **opaque binary files** are stored in KDB alongside JSON documents:

- Every logical file has a stable **`fileId`** (`KdbUuid`, RFC 4122 GUID).
- **Discoverability** is via a **metadata JSON document** in the namespace document tree (queryable with SQL, replayed on reload).
- **Bytes** live in the content-addressed **blob store** (`writeBlob` / `readBlob`), never embedded in the delta log.
- Callers may store bytes **raw** or **ZIP-compressed** (single-member or multi-member archive).
- Multiple files may be stored **together** in a **file bundle** (shared `bundleId`, one archive blob or a manifest + member blobs).

`KdbOp.FileWrite(path, blobHash)` remains the low-level DAG op for namespace path → hash (optional, for git-style path history). **Primary retrieval** for applications is through **metadata documents** and `fileId`, not path replay alone.

-----

## 2. Dependencies

| Module | Role |
|--------|------|
| `dev.kdb.document` | `KdbDocument`, `KdbOp.Write`, `KdbOp.FileWrite`, `KdbUuid`, `KdbHash` |
| `dev.kdb.storage` | `StorageAdapter.writeBlob`, `readBlob` |
| `dev.kdb.transaction` | Atomic commit: blob + metadata (+ optional `FileWrite`) |
| `dev.kdb.schema` | Optional indexed fields on attachment metadata (Layer 2) |
| `dev.kdb.storage.compaction` / Component 19 | Blob reachability (§7) |

-----

## 3. Concepts

### 3.1 Three layers (do not conflate)

| Layer | What | Where |
|-------|------|--------|
| **Metadata** | JSON record: `fileId`, name, mime, size, `blobHash`, encoding, bundle membership | `KdbDocument` via `KdbOp.Write` → document tree + delta commit payload |
| **Blob** | Raw or ZIP bytes | Blob store keyed by SHA-256 (`KdbHash`) |
| **Path pointer** (optional) | Namespace path → `blobHash` at a commit | `KdbOp.FileWrite` in commit only |

### 3.2 Identifiers

| ID | Type | Scope | Notes |
|----|------|--------|-------|
| **`fileId`** | `KdbUuid` | One logical file for all time | Assigned at ingest; never changes across versions |
| **`bundleId`** | `KdbUuid` | One file bundle | Groups members; may be same as a “container” metadata doc id |
| **`docId`** | `KdbUuid` | Metadata document | Often **`docId == fileId`** for standalone files; bundle header uses `docId == bundleId` |
| **`blobHash`** | `KdbHash` | Content-addressed bytes | Hash of **stored** bytes (after ZIP if `encoding = zip`) |

### 3.3 Storage encoding

| `encoding` | Blob contents | Typical use |
|------------|---------------|-------------|
| **`raw`** | Exact uploaded bytes | Images, PDFs, pre-compressed assets |
| **`zip`** | ZIP archive (PKZIP / DEFLATE) | Single file (`report.pdf` one entry) or **bundle** (many entries) |

- ZIP is **not** a separate storage tier; it is an **ingest-time transform** before `writeBlob`.
- `blobHash` always refers to the **stored** blob (compressed or not).
- **`uncompressedSizeBytes`** and **`compressedSizeBytes`** are carried in metadata for display and quota; integrity of `raw` is `sizeBytes == blob length`; for `zip`, `compressedSizeBytes` equals blob length.

-----

## 4. JSON metadata shapes (normative)

All attachment metadata documents are ordinary `KdbDocument` JSON objects. They **must** include `"kdbKind"` so tooling can discriminate without schema.

### 4.1 Standalone file — `kdb.file`

```json
{
  "kdbKind": "kdb.file",
  "fileId": "550e8400-e29b-41d4-a716-446655440000",
  "name": "report.pdf",
  "path": "uploads/2024/report.pdf",
  "mimeType": "application/pdf",
  "encoding": "zip",
  "blobHash": "a1b2c3…64 hex…",
  "sizeBytes": 1048576,
  "compressedSizeBytes": 524288,
  "bundleId": null,
  "createdAt": "2026-05-16T12:00:00.000Z",
  "labels": { "department": "finance" }
}
```

| Field | Required | Type | Notes |
|-------|----------|------|-------|
| `kdbKind` | yes | string | Constant `"kdb.file"` |
| `fileId` | yes | UUID string | Same as document `id` when `docId == fileId` |
| `name` | yes | string | Display / original filename |
| `path` | no | string | Logical namespace path; if set, commit should include matching `FileWrite` |
| `mimeType` | no | string | IANA media type |
| `encoding` | yes | string | `"raw"` or `"zip"` |
| `blobHash` | yes | string | 64-char hex SHA-256 of blob store payload |
| `sizeBytes` | yes | number | Uncompressed logical size (single file or sum for bundle member) |
| `compressedSizeBytes` | if `encoding=zip` | number | Byte length of stored blob |
| `bundleId` | no | UUID string | Set when this file is a member of a bundle (§4.3) |
| `createdAt` | no | string | ISO-8601 timestamp |
| `labels` | no | object | Extension key/value map (not validated by schema unless declared) |

### 4.2 Bundle header — `kdb.file.bundle`

```json
{
  "kdbKind": "kdb.file.bundle",
  "bundleId": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
  "name": "quarterly-pack.zip",
  "encoding": "zip",
  "blobHash": "d4e5f6…",
  "sizeBytes": 5000000,
  "compressedSizeBytes": 1200000,
  "memberCount": 3,
  "members": [
    {
      "fileId": "550e8400-e29b-41d4-a716-446655440000",
      "name": "report.pdf",
      "pathInBundle": "report.pdf",
      "sizeBytes": 1048576
    },
    {
      "fileId": "6ba7b811-9dad-11d1-80b4-00c04fd430c8",
      "name": "data.csv",
      "pathInBundle": "data.csv",
      "sizeBytes": 2048
    }
  ],
  "createdAt": "2026-05-16T12:00:00.000Z"
}
```

| Field | Required | Notes |
|-------|----------|-------|
| `kdbKind` | yes | `"kdb.file.bundle"` |
| `bundleId` | yes | Stable bundle identity; prefer `docId == bundleId` |
| `encoding` | yes | **`zip`** for archive blob; **`raw`** only if bundle is manifest-only (§4.3b) |
| `blobHash` | yes* | *Required when `encoding=zip` (single archive holds all members) |
| `members` | yes | Ordered list; each entry has its own `fileId` |
| `memberCount` | yes | Redundant with `members.length` for indexed query |

Each **member** may also have its own **`kdb.file`** metadata document (same `fileId`, `bundleId` set) for per-file SQL and history.

### 4.3 Bundle storage modes

**Mode A — Archive blob (recommended for “files together in one blob”)**

1. Build ZIP in memory (or streaming) with one entry per member (`pathInBundle` = ZIP entry name).
2. `writeBlob(zipBytes)` → `bundleBlobHash`.
3. One `Write` for `kdb.file.bundle` header (§4.2) with `encoding: "zip"`.
4. Optional: one `Write` per member `kdb.file` with `bundleId` + same or per-member hashes (see Mode B).

**Mode B — Manifest + member blobs**

1. Each member: `writeBlob` (raw or zip per file) → per-file `blobHash`.
2. Bundle header `encoding: "raw"`, `blobHash` null or omitted; `members[]` lists each `fileId` + `blobHash`.
3. Suitable when members are deduplicated or updated independently.

**Mode C — Append to existing bundle (new commit)**

1. Read bundle header at HEAD (by `bundleId` / doc id).
2. If Mode A: read archive blob, unpack ZIP, add entries, re-zip, new `writeBlob`, new bundle `Write` (new version via document tree).
3. If Mode B: add member blob + patch bundle manifest `members` + new member `kdb.file` docs in **one transaction**.

Implementations **must** document which mode a CLI/API uses; default for **`kdb file put --bundle`** is **Mode A** when `--zip` and multiple inputs are given.

### 4.4 ZIP rules

- Format: standard ZIP (PKZIP); UTF-8 entry names when non-ASCII.
- **Single-file ingest with `--zip`:** archive contains **exactly one** entry; entry name defaults to `name` or basename of source path.
- **Bundle ingest:** one entry per input file; entry path = `pathInBundle` or basename; duplicate paths are rejected at ingest.
- Stored blob is the ZIP bytes; `blobHash` = SHA-256(ZIP bytes).

-----

## 5. Write path (transaction contract)

### 5.1 Single file ingest

**Preconditions:** `fileId` is new or caller intentionally versions by writing a new metadata doc (same `fileId`, new commit — document tree points to new content hash).

**Steps (one transaction):**

1. Read source bytes from caller.
2. If `zip: true`, compress to ZIP (single entry) → `payload`; else `payload = source`.
3. `blobHash = storage.writeBlob(payload)`.
4. Build `kdb.file` JSON; set sizes and `encoding`.
5. `KdbOp.Write(fileId, metadataJson)` (or `writeDocument`).
6. If `path` present: `KdbOp.FileWrite(path, blobHash)`.
7. `TransactionEngine.commit` → delta record + document tree update.

**Postconditions:**

- `readBlob(blobHash)` returns `payload`.
- `getDocument(fileId, HEAD)` returns metadata.
- Delta commit lists `Write` (+ optional `FileWrite`).

### 5.2 Bundle ingest

**Preconditions:** `bundleId` assigned; member `fileId`s assigned (new UUIDs unless replacing members).

**Steps (one transaction, Mode A):**

1. For each input file, assign `fileId`, `pathInBundle`, sizes.
2. Build ZIP containing all entries → `bundleBlobHash = writeBlob(zip)`.
3. `Write(bundleId, bundleHeaderJson)` with `kdb.file.bundle`.
4. For each member (optional but recommended): `Write(fileId, memberMetadataJson)` with `bundleId` set.
5. Optional `FileWrite` per namespace path.

### 5.3 Validation

| Check | When |
|-------|------|
| `blobHash` exists in storage before commit | Engine **should** reject commit if `readBlob(blobHash)` is null (Transaction Engine extension) |
| `fileId` / `bundleId` valid UUID | Ingest API |
| `encoding` ∈ {`raw`, `zip`} | Ingest API |
| ZIP parses and entry count matches `memberCount` | Optional integrity job; ingest should verify before commit |

-----

## 6. Read path

### 6.1 Resolve metadata

| Query by | Mechanism |
|----------|-----------|
| **`fileId`** | `getDocument(fileId, atCommit)` or SQL on indexed `fileId` |
| **`bundleId`** | `getDocument(bundleId, atCommit)` |
| **`path`** | SQL on metadata `path` at HEAD, or replay `FileWrite` ops (legacy) |

### 6.2 Load bytes

1. Read `blobHash` and `encoding` from metadata.
2. `bytes = readBlob(blobHash)`.
3. If `encoding == "raw"`, return `bytes`.
4. If `encoding == "zip"`, unzip:
   - **Standalone file:** return sole entry bytes (or named entry if `name` given).
   - **Bundle:** return full ZIP for download, or extract one member by `pathInBundle` / `fileId` → entry name map from `members`.

### 6.3 Historical version

Documents are versioned by commit DAG; `getDocument(fileId, atCommit)` returns metadata at that commit; blob may still be shared if content unchanged (`blobHash` deduplication).

-----

## 7. Reload, persistence, and GC (Stage B)

Extends [file persistence plan](kdb-spec-layer8-file-persistence-plan.md).

### 7.1 Reload

On namespace open:

1. Replay delta → apply all `KdbOp.Write` / `Delete` (metadata documents) — **already required** for documents.
2. Blob bytes recovered from WAL / SSTable via `readBlob` — **no** `FileWrite` replay required for metadata-first retrieval.
3. Optional: rebuild path index from `FileWrite` ops for `file get --path` CLI.

### 7.2 Blob reachability (GC)

`OrphanBlobGc` (Component 19) **must** treat a blob as reachable if any of:

- Referenced by **current or historical document tree** content hashes (document bodies that embed `blobHash` in JSON — scan or index), or
- Referenced in **`KdbOp.FileWrite`** of any commit retained in the DAG, or
- Listed in **`kdb.file` / `kdb.file.bundle`** metadata reachable from a retained document tree entry.

**v1 implementation note:** scanning commit payloads for `FileWrite` + parsing metadata JSON for 64-hex `blobHash` fields is acceptable; optimised index is follow-on.

### 7.3 Peer sync

Pushing commits **must** push referenced blobs (content fetch by hash) before or with commit application. Pull side **must** `writeBlob` before serving `readBlob` for metadata that references missing hashes.

-----

## 8. CLI surface (planned, Component 29 extension)

Not implemented in v1 CLI. Normative command shapes:

```bash
# Standalone file (assign or pass fileId)
kdb file put <namespace> <local-path> \
  --id <fileId-uuid> \
  [--zip] \
  [--path uploads/report.pdf] \
  [-m "message"]

# Returns: fileId, blobHash (stdout JSON or tab-separated)

# Bundle: multiple locals → one ZIP blob + bundle metadata
kdb file put <namespace> --bundle <bundleId-uuid> \
  [--zip] \
  file1.pdf file2.csv \
  [-m "bundle message"]

# Or add to existing bundle (Mode C)
kdb file put <namespace> --bundle <bundleId-uuid> --append \
  [--zip] extra.pdf

# Fetch bytes
kdb file get <namespace> --id <fileId-uuid> [-o output-path]
kdb file get <namespace> --bundle <bundleId-uuid> [-o bundle.zip]
kdb file get <namespace> --bundle <bundleId-uuid> --member <fileId-uuid> [-o out]

# Metadata only
kdb file meta <namespace> --id <fileId-uuid>
```

**`--zip`:** apply §4.4 before `writeBlob`. Default: **`raw`** for single file; **`zip`** default when `--bundle` with multiple inputs.

**`blob put`:** optional low-level command (hash only, no metadata) for tooling; not required for applications.

-----

## 9. Schema integration (optional)

Namespaces may declare schema for attachment collections:

```kotlin
namespace("myapp/files") {
    schema {
        field("fileId",   UuidType,   required = true, indexed = true, unique = true)
        field("bundleId", UuidType,   required = false, indexed = true)
        field("name",     StringType, required = true, indexed = true)
        field("path",     StringType, required = false, indexed = true)
        field("mimeType", StringType, required = false, indexed = false)
        field("encoding", StringType, required = true, indexed = false)
        field("blobHash", StringType, required = true, indexed = false)
    }
}
```

Extension fields and `labels` remain unindexed unless declared. **`_doc`** always contains full metadata JSON.

-----

## 10. Relationship to `KdbOp.FileWrite`

| Mechanism | Purpose |
|-----------|---------|
| **Metadata `kdb.file`** | Primary: SQL, reload, `fileId`, bundles, MIME, sizes |
| **`FileWrite(path, blobHash)`** | Optional: namespace path history, ice manifest `path`, compat with path-only tools |

If both are used, **`path` in metadata** and **`FileWrite.path`** **should** match for the same commit. Mismatch is undefined for path-based tools; metadata wins for `fileId` lookup.

-----

## 11. Test cases

| # | Name | Expected |
|---|------|----------|
| 1 | `ingest_raw_single` | `encoding=raw`; `readBlob` round-trip |
| 2 | `ingest_zip_single` | ZIP blob; get extracts original bytes |
| 3 | `ingest_bundle_zip_modeA` | One blob; bundle + member metadata; extract member by `fileId` |
| 4 | `commit_atomic_metadata_and_blob` | Blob missing → commit rejected (when validation enabled) |
| 5 | `reload_metadata_survives` | File mode reopen; `getDocument(fileId)` + `readBlob` work |
| 6 | `gc_retains_referenced_blob` | Metadata at HEAD; blob not collected |
| 7 | `gc_drops_orphan_blob` | Blob with no metadata / FileWrite reference collected |
| 8 | `bundle_append_modeC` | Second commit adds member; HEAD lists `memberCount` increased |
| 9 | `dedup_same_bytes` | Two files same content → same `blobHash`, different `fileId`s |

-----

## 12. Non-goals

- **Encrypted ZIP** or per-entry passwords — out of scope v1.
- **tar.gz / zstd** as ingest encodings — use `raw` blob + tier compression (master §12.1) for storage engine; ingest ZIP is explicit in metadata only.
- **Streaming multi-GB single ZIP build** — implementation may buffer; streaming API is follow-on.
- **Deduplication by `fileId`** across namespaces — blobs are global per storage adapter instance only.

-----

## 13. Implementation order

1. **Spec + JSON validators** (`kdbKind`, required fields) in `dev.kdb.document` or new `dev.kdb.file`.
2. **Ingest helpers:** `FileIngestOptions(zip, path, fileId, bundleId)`, ZIP builder.
3. **Transaction Engine:** optional `readBlob` preflight on `FileWrite` / metadata writes.
4. **File persistence replay:** ensure `Write` paths unchanged; document GC reachability (§7.2).
5. **CLI** (Component 29): `file put` / `file get` / `file meta`.
6. **Peer sync:** blob fetch by hash.

**Estimated NBNC:** ~1,200 (helpers 400, engine hooks 200, CLI 350, tests 250).

-----

## 14. Session instructions

When implementing, use this spec plus Component 3 and the master spec. Extract public ingest/read APIs into Section 17 after implementation.

Cross-reference from Component 3 §`FileWrite` and update [user guide](kdb-user-guide.md) when CLI lands.
