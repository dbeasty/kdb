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

## Hybrid storage (local primary + S3 replica)

Go file runtime supports **local disk as primary** and an optional **S3-compatible replica** for sealed segments and snapshots. The LSM engine still appends only to local disk; after `MarkSealed`, the full segment is uploaded to S3.

| Role | Backend |
|------|---------|
| Live writes / reads / listing | Local `OSByteStore` under `{dataRoot}` |
| Backup / archive | S3 object store (AWS, LocalStack, MinIO) |

Single-writer lock remains on `{dataRoot}/.kdb.lock`. S3 is not a substitute for the data directory in v1.

### Environment variables

| Variable | Purpose |
|----------|---------|
| `KDB_S3_BUCKET` | Bucket name (unset = S3 disabled) |
| `KDB_S3_REGION` | AWS region (default `us-east-1`) |
| `KDB_S3_PREFIX` | Optional key prefix for all objects |
| `KDB_S3_ENDPOINT` | Custom endpoint (LocalStack: `http://localhost:4566`) |
| `KDB_S3_PATH_STYLE` | Force path-style URLs (`true`) |
| `KDB_S3_ENSURE_BUCKET` | Create bucket on open (`true`, default when endpoint set) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | Credentials (dummy `test`/`test` OK for LocalStack) |

Programmatic API: `embed.OpenFileRuntimeWithOptions` and `embed.FileRuntimeOptions` in Go.

### Local S3 emulation (LocalStack)

```bash
cd go && docker compose -f docker-compose.s3.yml up -d
export KDB_S3_ENDPOINT=http://localhost:4566
export KDB_S3_BUCKET=kdb-dev
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
# run CLI/service with --data-dir as usual
go test -tags=s3integration ./kdb/storage/io/s3/...
```

Same SDK code path as production; only endpoint and credentials differ.

## Tests

- `FilePersistenceTest` — write, reopen, SELECT
- `KdbCliTest.putGet_survivesReopen` — separate CLI processes
- Go `primary_replicas_test` / `s3` package unit tests (mocked S3)
- Optional `-tags=s3integration` against LocalStack
