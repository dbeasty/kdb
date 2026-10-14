/**
 * The binary bodies (encoding tag 2) of the attachment messages a replica sends on a sync session
 * (go/kdb/wire/attachment_ops.go, binary.go): BLOB_FETCH / BLOB_FETCH_RESULT and
 * ATTACHMENT_UPLOAD / ATTACHMENT_UPLOAD_RESULT. They are always binary, so bytes stay bytes.
 *
 * A body is a sequence of fields, each `uvarint(field << 3 | wireType)` then the value: wire type
 * 0 a uvarint, 2 `uvarint(len) ++ bytes` (strings, bytes, hashes, UUIDs, nested bodies). Zero
 * values are omitted, as Go omits them.
 */

import { concat, fromHex, fromUtf8, toHex, utf8, uuidBytes, uuidString } from "./bytes.ts";
import { sha256 } from "./sha256.ts";

const WT_VARINT = 0, WT_FIXED64 = 1, WT_BYTES = 2;

class BinWriter {
  #parts: Uint8Array[] = [];

  #uvarint(v: number): void {
    if (!Number.isSafeInteger(v) || v < 0) throw new Error(`kdb replica: varint out of range: ${v}`);
    const out: number[] = [];
    do {
      let b = v % 128;
      v = Math.floor(v / 128);
      if (v > 0) b |= 0x80;
      out.push(b);
    } while (v > 0);
    this.#parts.push(Uint8Array.from(out));
  }

  #key(field: number, wt: number): void {
    this.#uvarint(field * 8 + wt);
  }

  int(field: number, v: number): this {
    if (v !== 0) {
      this.#key(field, WT_VARINT);
      this.#uvarint(v);
    }
    return this;
  }

  bool(field: number, v: boolean): this {
    return v ? this.int(field, 1) : this;
  }

  bytes(field: number, b: Uint8Array): this {
    if (b.length > 0) {
      this.#key(field, WT_BYTES);
      this.#uvarint(b.length);
      this.#parts.push(b);
    }
    return this;
  }

  string(field: number, s: string): this {
    return this.bytes(field, utf8(s));
  }

  nested(field: number, fn: (w: BinWriter) => void): this {
    const w = new BinWriter();
    fn(w);
    const body = w.finish();
    this.#key(field, WT_BYTES);
    this.#uvarint(body.length);
    this.#parts.push(body);
    return this;
  }

  finish(): Uint8Array {
    return concat(this.#parts);
  }
}

class BinReader {
  readonly #b: Uint8Array;
  #off = 0;

  constructor(b: Uint8Array) {
    this.#b = b;
  }

  #uvarint(): number {
    let v = 0, mul = 1;
    for (let i = 0; i < 10; i++) {
      if (this.#off >= this.#b.length) throw new Error("kdb replica: truncated binary body");
      const b = this.#b[this.#off++]!;
      v += (b & 0x7f) * mul;
      if ((b & 0x80) === 0) return v;
      mul *= 128;
    }
    throw new Error("kdb replica: varint too long in binary body");
  }

  /** The next field's number and wire type, or null at the end. */
  next(): { field: number; wt: number } | null {
    if (this.#off >= this.#b.length) return null;
    const key = this.#uvarint();
    return { field: Math.floor(key / 8), wt: key % 8 };
  }

  int(wt: number): number {
    if (wt !== WT_VARINT) throw new Error("kdb replica: binary field has the wrong wire type");
    return this.#uvarint();
  }

  bytes(wt: number): Uint8Array {
    if (wt !== WT_BYTES) throw new Error("kdb replica: binary field has the wrong wire type");
    const n = this.#uvarint();
    if (this.#off + n > this.#b.length) throw new Error("kdb replica: binary field runs past the end of the body");
    const out = this.#b.subarray(this.#off, this.#off + n);
    this.#off += n;
    return out;
  }

  string(wt: number): string {
    return fromUtf8(this.bytes(wt));
  }

  skip(wt: number): void {
    if (wt === WT_VARINT) this.#uvarint();
    else if (wt === WT_BYTES) this.bytes(wt);
    else if (wt === WT_FIXED64) this.#off += 8;
    else throw new Error(`kdb replica: unknown wire type ${wt} in binary body`);
  }
}

// --- BLOB_FETCH ----------------------------------------------------------------------------------

export interface BlobFetchEntry {
  docId: string;
  attachmentId: string;
  commitHex: string;
  hash: string;
  offset: number;
  maxBytes: number;
}

export function encodeBlobFetch(namespace: string, entries: readonly BlobFetchEntry[], maxBytes: number): Uint8Array {
  const w = new BinWriter().string(1, namespace);
  for (const e of entries) {
    w.nested(2, (n) => {
      n.bytes(1, uuidBytes(e.docId)).bytes(2, uuidBytes(e.attachmentId)).string(3, e.commitHex)
        .bytes(4, fromHex(e.hash)).int(5, e.offset).int(6, e.maxBytes);
    });
  }
  return w.int(3, maxBytes).finish();
}

export interface BlobChunkProof {
  leaf: number;
  path: Uint8Array[];
}

export interface BlobFetchPart {
  index: number;
  hash: string;
  size: number;
  tree: string;
  offset: number;
  data: Uint8Array;
  chunks: BlobChunkProof[];
  eof: boolean;
  superseded: boolean;
  error: string;
  errorCode: string;
}

export interface BlobFetchResult {
  namespace: string;
  parts: BlobFetchPart[];
  more: boolean;
  error: string;
  errorCode: string;
}

export function decodeBlobFetchResult(body: Uint8Array): BlobFetchResult {
  const r = new BinReader(body);
  const out: BlobFetchResult = { namespace: "", parts: [], more: false, error: "", errorCode: "" };
  for (let f = r.next(); f; f = r.next()) {
    switch (f.field) {
      case 1: out.namespace = r.string(f.wt); break;
      case 2: out.parts.push(decodePart(r.bytes(f.wt))); break;
      case 3: out.more = r.int(f.wt) !== 0; break;
      case 4: out.error = r.string(f.wt); break;
      case 5: out.errorCode = r.string(f.wt); break;
      default: r.skip(f.wt);
    }
  }
  return out;
}

function decodePart(body: Uint8Array): BlobFetchPart {
  const r = new BinReader(body);
  const p: BlobFetchPart = {
    index: 0, hash: "", size: 0, tree: "", offset: 0, data: new Uint8Array(), chunks: [],
    eof: false, superseded: false, error: "", errorCode: "",
  };
  for (let f = r.next(); f; f = r.next()) {
    switch (f.field) {
      case 1: p.index = r.int(f.wt); break;
      case 2: p.hash = toHex(r.bytes(f.wt)); break;
      case 3: p.size = r.int(f.wt); break;
      case 4: p.tree = toHex(r.bytes(f.wt)); break;
      case 5: p.offset = r.int(f.wt); break;
      case 6: p.data = r.bytes(f.wt).slice(); break;
      case 7: {
        const c = new BinReader(r.bytes(f.wt));
        const proof: BlobChunkProof = { leaf: 0, path: [] };
        for (let g = c.next(); g; g = c.next()) {
          if (g.field === 1) proof.leaf = c.int(g.wt);
          else if (g.field === 2) proof.path.push(c.bytes(g.wt).slice());
          else c.skip(g.wt);
        }
        p.chunks.push(proof);
        break;
      }
      case 8: p.eof = r.int(f.wt) !== 0; break;
      case 9: p.superseded = r.int(f.wt) !== 0; break;
      case 10: p.error = r.string(f.wt); break;
      case 11: p.errorCode = r.string(f.wt); break;
      default: r.skip(f.wt);
    }
  }
  return p;
}

// --- ATTACHMENT_UPLOAD ---------------------------------------------------------------------------

export interface UploadChunk {
  namespace: string;
  /** Only on a client session; a sync session has none. */
  sessionId?: string;
  uploadId: string;
  offset: number;
  data: Uint8Array;
  final: boolean;
  /** On a first chunk: the blob's hash, so a host that already holds it says so at once. */
  expect: string;
}

export function encodeAttachmentUpload(c: UploadChunk): Uint8Array {
  const w = new BinWriter().string(1, c.namespace).string(2, c.sessionId ?? "").string(3, c.uploadId).int(4, c.offset).bytes(5, c.data).bool(6, c.final);
  if (c.expect) w.bytes(7, fromHex(c.expect));
  return w.finish();
}

export interface UploadResult {
  namespace: string;
  uploadId: string;
  received: number;
  hash: string;
  size: number;
  error: string;
  errorCode: string;
}

export function decodeAttachmentUploadResult(body: Uint8Array): UploadResult {
  const r = new BinReader(body);
  const out: UploadResult = { namespace: "", uploadId: "", received: 0, hash: "", size: 0, error: "", errorCode: "" };
  for (let f = r.next(); f; f = r.next()) {
    switch (f.field) {
      case 1: out.namespace = r.string(f.wt); break;
      case 2: out.uploadId = r.string(f.wt); break;
      case 3: out.received = r.int(f.wt); break;
      case 4: out.hash = toHex(r.bytes(f.wt)); break;
      case 5: out.size = r.int(f.wt); break;
      case 6: out.error = r.string(f.wt); break;
      case 7: out.errorCode = r.string(f.wt); break;
      default: r.skip(f.wt);
    }
  }
  return out;
}

// --- proofs --------------------------------------------------------------------------------------

function node(l: Uint8Array, r: Uint8Array): Uint8Array {
  const b = new Uint8Array(65);
  b[0] = 0x01;
  b.set(l, 1);
  b.set(r, 33);
  return sha256(b);
}

/** The scheme-1 identity of a blob from its Merkle tree hash and size. */
export function blobIdentity(tree: Uint8Array, size: number): string {
  const b = new Uint8Array(41);
  b[0] = 0x02;
  new DataView(b.buffer).setBigUint64(1, BigInt(size));
  b.set(tree, 9);
  return toHex(sha256(b));
}

/** RFC 6962 §2.1.1: leaf is leaf index of a tree of n leaves whose hash is tree (go VerifyMerkleInclusion). */
export function verifyInclusion(leaf: Uint8Array, index: number, n: number, path: readonly Uint8Array[], tree: Uint8Array): boolean {
  if (index < 0 || index >= n) return false;
  let fn = index, sn = n - 1;
  let r = leaf;
  for (const p of path) {
    if (sn === 0) return false;
    if ((fn & 1) === 1 || fn === sn) {
      r = node(p, r);
      while ((fn & 1) === 0 && fn !== 0) {
        fn >>= 1;
        sn >>= 1;
      }
    } else {
      r = node(r, p);
    }
    fn >>= 1;
    sn >>= 1;
  }
  if (sn !== 0) return false;
  return r.length === tree.length && r.every((b, i) => b === tree[i]);
}

export function chunkLeaf(chunk: Uint8Array): Uint8Array {
  return sha256(concat([new Uint8Array([0x00]), chunk]));
}

export { uuidString };
