/**
 * Commits: the canonical `dev.kdb.document.CommitPayload` record (go/kdb/document/wire.go) and
 * its hash, SHA-256 of the payload bytes (go/kdb/document/kdb_commit.go).
 *
 * A received commit keeps the bytes it arrived as, and its hash is derived from those bytes,
 * never from a re-encoding. That is how Go treats a pack (FromPayloadBytes), and it means a
 * field this decoder skips cannot change a hash. Encoding is only for commits the replica authors.
 */

import { fromHex, toHex, uuidBytes, uuidString } from "./bytes.ts";
import { DecodeError, expectTag, Reader, Tag, Writer } from "./codec.ts";
import { sha256 } from "./sha256.ts";

export type Op =
  | { kind: "write"; docId: string; json: string }
  | { kind: "delete"; docId: string }
  | { kind: "fileWrite"; path: string; blobHash: string }
  | { kind: "schemaMigration"; migrationId: string; payload: string };

export interface CommitFields {
  parents: string[]; // hex
  namespace: string;
  transactionId: string; // uuid
  timestampMicros: bigint;
  authorNodeId: string; // uuid
  ops: Op[];
  treeHash: string; // hex
  schemaHash?: string | undefined; // hex
  message: string;
}

export interface Commit extends CommitFields {
  hash: string;
  payload: Uint8Array;
}

const OP_WRITE = 0, OP_DELETE = 1, OP_FILE_WRITE = 2, OP_SCHEMA_MIGRATION = 3;

function hash32(hex: string, what: string): Uint8Array {
  const b = fromHex(hex);
  if (b.length !== 32) throw new Error(`kdb replica: ${what} must be 32 bytes, got ${b.length}`);
  return b;
}

function writeOp(w: Writer, op: Op): void {
  switch (op.kind) {
    case "write":
      w.byte(OP_WRITE).record((r) => {
        r.field(1, Tag.Fixed).raw(uuidBytes(op.docId));
        r.field(2, Tag.String).string(op.json);
      });
      return;
    case "delete":
      w.byte(OP_DELETE).record((r) => r.field(1, Tag.Fixed).raw(uuidBytes(op.docId)));
      return;
    case "fileWrite":
      w.byte(OP_FILE_WRITE).record((r) => {
        r.field(1, Tag.String).string(op.path);
        r.field(2, Tag.Fixed).raw(hash32(op.blobHash, "blobHash"));
      });
      return;
    case "schemaMigration":
      w.byte(OP_SCHEMA_MIGRATION).record((r) => {
        r.field(1, Tag.Fixed).raw(uuidBytes(op.migrationId));
        r.field(2, Tag.String).string(op.payload);
      });
      return;
  }
}

export function encodeCommitPayload(c: CommitFields): Uint8Array {
  const w = new Writer();
  w.record((r) => {
    r.field(1, Tag.Array).leb(c.parents.length);
    for (const p of c.parents) r.raw(hash32(p, "parent hash"));
    r.field(2, Tag.String).string(c.namespace);
    r.field(3, Tag.Fixed).raw(uuidBytes(c.transactionId));
    r.field(4, Tag.Int64).int64(c.timestampMicros);
    r.field(5, Tag.Fixed).raw(uuidBytes(c.authorNodeId));
    r.field(6, Tag.Array).leb(c.ops.length);
    for (const op of c.ops) writeOp(r, op);
    r.field(7, Tag.Fixed).raw(hash32(c.treeHash, "tree hash"));
    if (c.schemaHash !== undefined) r.field(8, Tag.Union).byte(1).raw(hash32(c.schemaHash, "schema hash"));
    r.field(9, Tag.String).string(c.message);
  });
  return w.bytes();
}

/** Builds a commit the replica authors: encodes it and derives its hash. */
export function buildCommit(c: CommitFields): Commit {
  const payload = encodeCommitPayload(c);
  return { ...c, hash: toHex(sha256(payload)), payload };
}

function readOp(r: Reader): Op {
  const branch = r.byte();
  let docId = "", json = "", path = "", blob = "", payload = "";
  switch (branch) {
    case OP_WRITE:
      r.record((id, tag, f) => {
        if (id === 1) { expectTag(id, tag, Tag.Fixed); docId = uuidString(f.raw(16)); return true; }
        if (id === 2) { expectTag(id, tag, Tag.String); json = f.string(); return true; }
        return false;
      });
      return { kind: "write", docId, json };
    case OP_DELETE:
      r.record((id, tag, f) => {
        if (id === 1) { expectTag(id, tag, Tag.Fixed); docId = uuidString(f.raw(16)); return true; }
        return false;
      });
      return { kind: "delete", docId };
    case OP_FILE_WRITE:
      r.record((id, tag, f) => {
        if (id === 1) { expectTag(id, tag, Tag.String); path = f.string(); return true; }
        if (id === 2) { expectTag(id, tag, Tag.Fixed); blob = toHex(f.raw(32)); return true; }
        return false;
      });
      return { kind: "fileWrite", path, blobHash: blob };
    case OP_SCHEMA_MIGRATION:
      r.record((id, tag, f) => {
        if (id === 1) { expectTag(id, tag, Tag.Fixed); docId = uuidString(f.raw(16)); return true; }
        if (id === 2) { expectTag(id, tag, Tag.String); payload = f.string(); return true; }
        return false;
      });
      return { kind: "schemaMigration", migrationId: docId, payload };
    default:
      throw new DecodeError(`unknown op branch ${branch}`);
  }
}

/** Decodes a commit from its canonical payload bytes; its hash is SHA-256 of exactly those bytes. */
export function decodeCommit(payload: Uint8Array): Commit {
  const r = new Reader(payload);
  const c: CommitFields = {
    parents: [], namespace: "", transactionId: "", timestampMicros: 0n, authorNodeId: "",
    ops: [], treeHash: "", message: "",
  };
  const seen = new Set<number>();
  r.record((id, tag, f) => {
    seen.add(id);
    switch (id) {
      case 1: {
        expectTag(id, tag, Tag.Array);
        const n = f.leb();
        for (let i = 0; i < n; i++) c.parents.push(toHex(f.raw(32)));
        return true;
      }
      case 2: expectTag(id, tag, Tag.String); c.namespace = f.string(); return true;
      case 3: expectTag(id, tag, Tag.Fixed); c.transactionId = uuidString(f.raw(16)); return true;
      case 4: expectTag(id, tag, Tag.Int64); c.timestampMicros = f.int64(); return true;
      case 5: expectTag(id, tag, Tag.Fixed); c.authorNodeId = uuidString(f.raw(16)); return true;
      case 6: {
        expectTag(id, tag, Tag.Array);
        const n = f.leb();
        for (let i = 0; i < n; i++) c.ops.push(readOp(f));
        return true;
      }
      case 7: expectTag(id, tag, Tag.Fixed); c.treeHash = toHex(f.raw(32)); return true;
      case 8: {
        expectTag(id, tag, Tag.Union);
        const present = f.byte();
        if (present === 1) c.schemaHash = toHex(f.raw(32));
        else if (present !== 0) throw new DecodeError("bad nullable marker");
        return true;
      }
      case 9: expectTag(id, tag, Tag.String); c.message = f.string(); return true;
      default:
        return false;
    }
  });
  if (!r.done) throw new DecodeError("trailing bytes after commit payload");
  for (const required of [1, 2, 3, 4, 5, 6, 7, 9]) {
    if (!seen.has(required)) throw new DecodeError(`commit payload is missing field ${required}`);
  }
  return { ...c, hash: toHex(sha256(payload)), payload };
}

/**
 * Splits a pack's `commitsPayload` (go/kdb/wire/commit_push_codec.go): `u32le count`, then per
 * commit `u32le len ++ payload`.
 */
export function decodeCommitList(bytes: Uint8Array): Commit[] {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (bytes.length < 4) return []; // as Go's DecodeCommits: an empty pack
  const count = view.getUint32(0, true);
  if (count > (bytes.length - 4) / 4) throw new DecodeError(`commit list declares ${count} commits, too many for its size`);
  const out: Commit[] = [];
  let off = 4;
  for (let i = 0; i < count; i++) {
    if (off + 4 > bytes.length) throw new DecodeError("truncated commit list");
    const len = view.getUint32(off, true);
    off += 4;
    if (off + len > bytes.length) throw new DecodeError("truncated commit in list");
    out.push(decodeCommit(bytes.slice(off, off + len)));
    off += len;
  }
  if (off !== bytes.length) throw new DecodeError("trailing bytes after commit list");
  return out;
}

export function encodeCommitList(commits: readonly { payload: Uint8Array }[]): Uint8Array {
  let n = 4;
  for (const c of commits) n += 4 + c.payload.length;
  const out = new Uint8Array(n);
  const view = new DataView(out.buffer);
  view.setUint32(0, commits.length, true);
  let off = 4;
  for (const c of commits) {
    view.setUint32(off, c.payload.length, true);
    out.set(c.payload, off + 4);
    off += 4 + c.payload.length;
  }
  return out;
}

/**
 * The commit every namespace starts at, on every node (go/kdb/dag NewInMemoryCommitDag): fixed
 * transaction and author ids, timestamp zero, no ops, the empty tree. A replica starts there too,
 * so what it writes before it ever syncs still shares a root with the host's history.
 */
export function genesisCommit(namespace: string): Commit {
  return buildCommit({
    parents: [],
    namespace,
    transactionId: "00000000-0000-4000-8000-000000000001",
    timestampMicros: 0n,
    authorNodeId: "00000000-0000-4000-8000-000000000002",
    ops: [],
    treeHash: "0".repeat(64),
    message: "genesis",
  });
}

/** Content hash of a document: SHA-256 of `DocumentBody{1: uuid id, 2: string json}`. */
export function contentHash(docId: string, json: string): string {
  const w = new Writer();
  w.record((r) => {
    r.field(1, Tag.Fixed).raw(uuidBytes(docId));
    r.field(2, Tag.String).string(json);
  });
  return toHex(sha256(w.bytes()));
}
