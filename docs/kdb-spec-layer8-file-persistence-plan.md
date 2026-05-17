# KDB Layer 8 — File Persistence Execution Plan

**Status:** Implemented  
**Master spec:** [`kdb-spec.md`](kdb-spec.md) §5.2, §0  
**Depends on:** Layer 4a (SERVER storage engine, delta segments), Layer 2 (`CommitDag`), Layer 8 Component 24 (JDBC), Layer 10 Component 29 (CLI)  
**Out of scope:** Browser snapshot / localStorage persistence (separate plan)

---

## Purpose

Enable JVM/backend **embedded file mode**: durable namespaces on local disk for `jdbc:kdb:file://`, `openFileRuntime`, and the `kdb` CLI. Browser storage uses a different model (realized-store snapshot + peer repair).

---

## On-disk layout

```
{dataRoot}/
  ns/{namespaceId}/
    meta.json
    delta/{segmentUuid}     # KDBP-framed commit payloads
    wal/...
    sstable/...
```

`SegmentNameBuilder` paths are relative to `dataRoot` (`ns/{namespaceId}/delta/...`).

---

## Open / write / reload

1. **Open:** `FileBackedPlatformIoShim` + `StorageEngineTarget.SERVER` → replay all delta segments into `inMemoryCommitDag` + apply `KdbOp`s to `ServerStorageEngine` → rebuild indexes at HEAD.
2. **Write:** `PersistingCommitDag` appends `DeltaRecord` after each `appendCommit` / `putCommit`.
3. **Reload:** Same as open; genesis in empty DAG is skipped (idempotent `putCommit`).

**v1 limitations:** Documents repopulated via delta replay only (not WAL doc recovery). Single-writer per data directory enforced via `{dataRoot}/.kdb.lock` ([`DataDirectoryLock`](../kdb-jdbc/src/main/kotlin/dev/kdb/jdbc/file/DataDirectoryLock.kt)); stale locks cleared with `kdb unlock`.

**Opaque files:** Retrieval is **metadata-first** — `KdbOp.Write` documents with `kdbKind` `kdb.file` / `kdb.file.bundle` (see [`kdb-spec-layer1-component3b-file-attachments.md`](kdb-spec-layer1-component3b-file-attachments.md)). Blob bytes live in the LSM blob store; `KdbOp.FileWrite` is optional path history. Reload **does not** require replaying `FileWrite` if metadata docs and blobs are present. GC must retain blobs referenced from metadata JSON and from `FileWrite` ops (Component 19 extension per 3b §7.2).

---

## Public API

- `dev.kdb.jdbc.openFileRuntime(dataRoot, catalog, namespaceId, schema)`
- `KdbDriver` accepts `jdbc:kdb:file:///path/to/data/catalog/table`
- `openCliRuntime` uses file runtime under `--data-dir`

---

## Tests

- `FilePersistenceTest` — write, reopen, SELECT
- `KdbCliTest.putGet_survivesReopen` — separate CLI processes
