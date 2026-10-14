/**
 * Attachment messages (0x44-0x49), matching go/kdb/wire/attachment_ops.go, and the scheme-1
 * attachment content hash (go/kdb/document/attachment_hash.go).
 *
 * The six client messages are ALWAYS binary bodies (tag 2), whatever else the handshake agreed:
 * bytes stay bytes. They have no JSON envelope form at all, so the DTOs below name their fields
 * after the Go struct fields (camelCase) for this client's own use only. A client sends them only
 * to a server that acked WIRE_FEATURE_ATTACHMENTS.
 *
 * Field conventions are binary.ts's: an absent field is its zero value. UUIDs are their 16
 * big-endian bytes (omitted when zero, decoded as ""), hashes their 32 raw bytes (omitted when
 * zero, decoded as "") - both carried here as strings, a canonical UUID and lowercase hex.
 */

import { KdbProtocolError } from "../errors.ts";
import { malformed, Reader, Writer } from "./binary.ts";
import { fromHex, toHex } from "./bytes.ts";
import { MSG, type MessageKind } from "./messages.ts";
import { uuidFromBytes, uuidToBytes } from "./uuid.ts";

/** The wire feature: the peer speaks these messages, CellKindBytes and $bytes/$attachment params. */
export const WIRE_FEATURE_ATTACHMENTS = "attachments";
/**
 * The wire feature: the peer answers an ATTACHMENT_GET with `proofs` set with chunk-aligned pages
 * carrying each chunk's RFC 6962 audit path, so a range can be verified without reading the rest.
 * Only meaningful with WIRE_FEATURE_ATTACHMENTS.
 */
export const WIRE_FEATURE_ATTACHMENT_PROOFS = "attachment-proofs";

/** Scheme 1's chunk size, and the size of every upload chunk this client sends. */
export const ATTACHMENT_CHUNK_BYTES = 1 << 20;
/** The most one upload chunk or data page may carry (go: MaxAttachmentChunkBytes). */
export const MAX_ATTACHMENT_CHUNK_BYTES = 4 << 20;
/** The only content hash scheme so far: SHA-256 Merkle tree over 1 MiB chunks. */
export const ATTACHMENT_HASH_SCHEME_CHUNKED_1 = 1;

export interface AttachmentInfoDto {
  /** The attachment's ID, a UUID. */
  id: string;
  name: string;
  /** Scheme-1 content hash, lowercase hex. */
  hash: string;
  size: number;
  mime: string;
}

export interface AttachmentUploadDto {
  namespace: string;
  sessionId: string;
  /** Absent on the first chunk; the result names the upload. */
  uploadId?: string;
  /** Where this chunk starts: the bytes received so far. */
  offset?: number;
  data?: Uint8Array;
  final?: boolean;
}

export interface AttachmentUploadResultDto {
  namespace: string;
  uploadId: string;
  received: number;
  /** Set after the final chunk. */
  hash: string;
  size: number;
  error: string;
  errorCode: string;
}

export interface AttachmentGetDto {
  namespace: string;
  sessionId: string;
  docId: string;
  attachmentId?: string;
  name?: string;
  asOfHex?: string;
  offset?: number;
  maxBytes?: number;
  readId?: string;
  close?: boolean;
  /**
   * Ask for verifiable pages (WIRE_FEATURE_ATTACHMENT_PROOFS): the server rounds the offset down
   * and the page end up to 1 MiB chunk boundaries and proves each chunk.
   */
  proofs?: boolean;
}

/** One chunk's inclusion proof: its leaf index and audit path (hex hashes, bottom up). */
export interface ChunkProofDto {
  leaf: number;
  path: string[];
}

export interface AttachmentDataDto {
  namespace: string;
  readId: string;
  docId: string;
  info: AttachmentInfoDto;
  commitHex: string;
  offset: number;
  data: Uint8Array;
  eof: boolean;
  superseded: boolean;
  error: string;
  errorCode: string;
  /** Answering a get with proofs: the blob's Merkle tree hash, hex ("" when absent). */
  tree: string;
  /** Answering a get with proofs: one proof per whole chunk the page carries. */
  chunks: ChunkProofDto[];
  /** Answering a get with proofs: the content hash's scheme (1). */
  scheme: number;
}

export interface AttachmentListDto {
  namespace: string;
  sessionId: string;
  docId: string;
  asOfHex?: string;
}

export interface AttachmentListResultDto {
  namespace: string;
  commitHex: string;
  items: AttachmentInfoDto[];
  error: string;
  errorCode: string;
}

/** The attachment kinds, by type code. */
export const ATTACHMENT_KIND_BY_TYPE_CODE: ReadonlyMap<number, MessageKind> = new Map<number, MessageKind>([
  [MSG.ATTACHMENT_UPLOAD, "attachmentUpload"],
  [MSG.ATTACHMENT_UPLOAD_RESULT, "attachmentUploadResult"],
  [MSG.ATTACHMENT_GET, "attachmentGet"],
  [MSG.ATTACHMENT_DATA, "attachmentData"],
  [MSG.ATTACHMENT_LIST, "attachmentList"],
  [MSG.ATTACHMENT_LIST_RESULT, "attachmentListResult"],
]);

const ATTACHMENT_KINDS: ReadonlySet<MessageKind> = new Set(ATTACHMENT_KIND_BY_TYPE_CODE.values());

/** Whether `kind` is an (always binary) attachment message. */
export function isAttachmentKind(kind: MessageKind): boolean {
  return ATTACHMENT_KINDS.has(kind);
}

// --- encoding --------------------------------------------------------------------------------

type Dto = Record<string, unknown>;

const s = (v: unknown): string | undefined => (typeof v === "string" ? v : undefined);
const n = (v: unknown): number | undefined => (typeof v === "number" ? v : undefined);
const y = (v: unknown): Uint8Array | undefined => (v instanceof Uint8Array ? v : undefined);

function uuidField(w: Writer, field: number, id: unknown): void {
  if (typeof id !== "string" || id === "") return;
  const b = uuidToBytes(id);
  if (b.every((x) => x === 0)) return;
  w.bytesField(field, b);
}

function hashField(w: Writer, field: number, hex: unknown): void {
  if (typeof hex !== "string" || hex === "") return;
  const b = fromHex(hex);
  if (b.length !== 32) throw new KdbProtocolError(`kdb: attachment hash is not 32 bytes: ${hex}`);
  if (b.every((x) => x === 0)) return;
  w.bytesField(field, b);
}

function errFields(w: Writer, field: number, p: Dto): void {
  w.string(field, s(p.error));
  w.string(field + 1, s(p.errorCode));
}

function infoField(w: Writer, field: number, info: unknown): void {
  const i = (info ?? {}) as Dto;
  const sub = new Writer();
  uuidField(sub, 1, i.id);
  sub.string(2, s(i.name));
  hashField(sub, 3, i.hash);
  sub.int(4, n(i.size));
  sub.string(5, s(i.mime));
  // Go's appendNested always writes the field, even around an empty body.
  w.bytesField(field, sub.finish());
}

/** One nested field per chunk proof: 1 the leaf (omitted when 0), 2 each path hash's 32 raw bytes. */
function proofFields(w: Writer, field: number, chunks: unknown): void {
  for (const c of (chunks as ChunkProofDto[] | undefined) ?? []) {
    const sub = new Writer();
    sub.int(1, c.leaf);
    for (const h of c.path) {
      const b = fromHex(h);
      if (b.length !== 32) throw new KdbProtocolError(`kdb: proof hash is not 32 bytes: ${h}`);
      sub.bytesField(2, b);
    }
    w.bytesField(field, sub.finish());
  }
}

/** Encodes an attachment message's binary body; undefined for any other kind. */
export function encodeAttachmentBody(kind: MessageKind, payload: unknown): Uint8Array | undefined {
  const p = (payload ?? {}) as Dto;
  const w = new Writer();
  switch (kind) {
    case "attachmentUpload":
      w.string(1, s(p.namespace));
      w.string(2, s(p.sessionId));
      w.string(3, s(p.uploadId));
      w.int(4, n(p.offset));
      w.bytes(5, y(p.data));
      w.bool(6, p.final === true);
      break;
    case "attachmentUploadResult":
      w.string(1, s(p.namespace));
      w.string(2, s(p.uploadId));
      w.int(3, n(p.received));
      hashField(w, 4, p.hash);
      w.int(5, n(p.size));
      errFields(w, 6, p);
      break;
    case "attachmentGet":
      w.string(1, s(p.namespace));
      w.string(2, s(p.sessionId));
      uuidField(w, 3, p.docId);
      uuidField(w, 4, p.attachmentId);
      w.string(5, s(p.name));
      w.string(6, s(p.asOfHex));
      w.int(7, n(p.offset));
      w.int(8, n(p.maxBytes));
      w.string(9, s(p.readId));
      w.bool(10, p.close === true);
      w.bool(11, p.proofs === true);
      break;
    case "attachmentData":
      w.string(1, s(p.namespace));
      w.string(2, s(p.readId));
      uuidField(w, 3, p.docId);
      infoField(w, 4, p.info);
      w.string(5, s(p.commitHex));
      w.int(6, n(p.offset));
      w.bytes(7, y(p.data));
      w.bool(8, p.eof === true);
      w.bool(9, p.superseded === true);
      errFields(w, 10, p);
      hashField(w, 12, p.tree);
      proofFields(w, 13, p.chunks);
      w.int(14, n(p.scheme));
      break;
    case "attachmentList":
      w.string(1, s(p.namespace));
      w.string(2, s(p.sessionId));
      uuidField(w, 3, p.docId);
      w.string(4, s(p.asOfHex));
      break;
    case "attachmentListResult":
      w.string(1, s(p.namespace));
      w.string(2, s(p.commitHex));
      for (const item of (p.items as unknown[] | undefined) ?? []) infoField(w, 3, item);
      errFields(w, 4, p);
      break;
    default:
      return undefined;
  }
  return w.finish();
}

// --- decoding --------------------------------------------------------------------------------

function readUuid(r: Reader, wt: number): string {
  const b = r.bytes(wt);
  if (b.length !== 16) throw malformed("a UUID field is not 16 bytes");
  return uuidFromBytes(b);
}

function readHash(r: Reader, wt: number): string {
  const b = r.bytes(wt);
  if (b.length !== 32) throw malformed("a hash field is not 32 bytes");
  return toHex(b);
}

function readInfo(r: Reader, wt: number): AttachmentInfoDto {
  const sub = new Reader(r.bytes(wt));
  const info: AttachmentInfoDto = { id: "", name: "", hash: "", size: 0, mime: "" };
  while (!sub.done) {
    const { field, wireType } = sub.next();
    switch (field) {
      case 1:
        info.id = readUuid(sub, wireType);
        break;
      case 2:
        info.name = sub.string(wireType);
        break;
      case 3:
        info.hash = readHash(sub, wireType);
        break;
      case 4:
        info.size = sub.int(wireType);
        break;
      case 5:
        info.mime = sub.string(wireType);
        break;
      default:
        sub.skip(wireType);
    }
  }
  return info;
}

function readProof(r: Reader, wt: number): ChunkProofDto {
  const sub = new Reader(r.bytes(wt));
  const proof: ChunkProofDto = { leaf: 0, path: [] };
  while (!sub.done) {
    const { field, wireType } = sub.next();
    if (field === 1) proof.leaf = sub.int(wireType);
    else if (field === 2) {
      const b = sub.bytes(wireType);
      if (b.length !== 32) throw malformed("proof hash is not 32 bytes");
      proof.path.push(toHex(b));
    } else sub.skip(wireType);
  }
  return proof;
}

type FieldReader = (r: Reader, wt: number, p: Dto) => void;
const str = (key: string): FieldReader => (r, wt, p) => { p[key] = r.string(wt); };
const int = (key: string): FieldReader => (r, wt, p) => { p[key] = r.int(wt); };
const bool = (key: string): FieldReader => (r, wt, p) => { p[key] = r.int(wt) !== 0; };
const bytes = (key: string): FieldReader => (r, wt, p) => { p[key] = r.bytes(wt).slice(); };
const uuid = (key: string): FieldReader => (r, wt, p) => { p[key] = readUuid(r, wt); };
const hash = (key: string): FieldReader => (r, wt, p) => { p[key] = readHash(r, wt); };
const info = (key: string): FieldReader => (r, wt, p) => { p[key] = readInfo(r, wt); };
const infoList = (key: string): FieldReader => (r, wt, p) => { (p[key] as AttachmentInfoDto[]).push(readInfo(r, wt)); };
const proofList = (key: string): FieldReader => (r, wt, p) => { (p[key] as ChunkProofDto[]).push(readProof(r, wt)); };

const emptyInfo = (): AttachmentInfoDto => ({ id: "", name: "", hash: "", size: 0, mime: "" });

/** Per kind: the field readers, and a function giving every field's zero value. */
const SCHEMAS: Record<string, { fields: Record<number, FieldReader>; zero: () => Dto }> = {
  attachmentUpload: {
    fields: { 1: str("namespace"), 2: str("sessionId"), 3: str("uploadId"), 4: int("offset"), 5: bytes("data"), 6: bool("final") },
    zero: () => ({ namespace: "", sessionId: "", uploadId: "", offset: 0, data: new Uint8Array(0), final: false }),
  },
  attachmentUploadResult: {
    fields: {
      1: str("namespace"), 2: str("uploadId"), 3: int("received"), 4: hash("hash"), 5: int("size"),
      6: str("error"), 7: str("errorCode"),
    },
    zero: () => ({ namespace: "", uploadId: "", received: 0, hash: "", size: 0, error: "", errorCode: "" }),
  },
  attachmentGet: {
    fields: {
      1: str("namespace"), 2: str("sessionId"), 3: uuid("docId"), 4: uuid("attachmentId"), 5: str("name"),
      6: str("asOfHex"), 7: int("offset"), 8: int("maxBytes"), 9: str("readId"), 10: bool("close"),
      11: bool("proofs"),
    },
    zero: () => ({
      namespace: "", sessionId: "", docId: "", attachmentId: "", name: "", asOfHex: "", offset: 0,
      maxBytes: 0, readId: "", close: false, proofs: false,
    }),
  },
  attachmentData: {
    fields: {
      1: str("namespace"), 2: str("readId"), 3: uuid("docId"), 4: info("info"), 5: str("commitHex"),
      6: int("offset"), 7: bytes("data"), 8: bool("eof"), 9: bool("superseded"), 10: str("error"),
      11: str("errorCode"), 12: hash("tree"), 13: proofList("chunks"), 14: int("scheme"),
    },
    zero: () => ({
      namespace: "", readId: "", docId: "", info: emptyInfo(), commitHex: "", offset: 0,
      data: new Uint8Array(0), eof: false, superseded: false, error: "", errorCode: "",
      tree: "", chunks: [], scheme: 0,
    }),
  },
  attachmentList: {
    fields: { 1: str("namespace"), 2: str("sessionId"), 3: uuid("docId"), 4: str("asOfHex") },
    zero: () => ({ namespace: "", sessionId: "", docId: "", asOfHex: "" }),
  },
  attachmentListResult: {
    fields: { 1: str("namespace"), 2: str("commitHex"), 3: infoList("items"), 4: str("error"), 5: str("errorCode") },
    zero: () => ({ namespace: "", commitHex: "", items: [], error: "", errorCode: "" }),
  },
};

/**
 * Decodes an attachment message's binary body into its DTO, every field present (absent ones at
 * their zero value). Undefined for a type code that is not an attachment message.
 */
export function decodeAttachmentBody(
  typeCode: number,
  body: Uint8Array,
): { kind: MessageKind; payload: Dto } | undefined {
  const kind = ATTACHMENT_KIND_BY_TYPE_CODE.get(typeCode);
  if (kind === undefined) return undefined;
  const schema = SCHEMAS[kind]!;
  const p = schema.zero();
  const r = new Reader(body);
  while (!r.done) {
    const { field, wireType } = r.next();
    const read = schema.fields[field];
    if (read === undefined) r.skip(wireType);
    else read(r, wireType, p);
  }
  return { kind, payload: p };
}

// --- base64 ----------------------------------------------------------------------------------

/** Standard (padded) base64 of `bytes` - what CellKindBytes cells and $bytes parameters carry. */
export function bytesToBase64(bytes: Uint8Array): string {
  let out = "";
  // btoa takes a binary string; build it in slices so a large value does not blow the stack.
  for (let i = 0; i < bytes.length; i += 0x8000) {
    out += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return btoa(out);
}

export function base64ToBytes(text: string): Uint8Array {
  let bin: string;
  try {
    bin = atob(text);
  } catch (cause) {
    throw new KdbProtocolError("kdb: a bytes cell is not valid base64", { cause });
  }
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

// --- content hash (scheme 1) -----------------------------------------------------------------

async function sha256(parts: Uint8Array[]): Promise<Uint8Array> {
  const subtle = globalThis.crypto?.subtle;
  if (!subtle) {
    throw new KdbProtocolError(
      "kdb: no Web Crypto SubtleCrypto available - attachment hashes need globalThis.crypto.subtle",
    );
  }
  let len = 0;
  for (const p of parts) len += p.length;
  const buf = new Uint8Array(len);
  let off = 0;
  for (const p of parts) {
    buf.set(p, off);
    off += p.length;
  }
  return new Uint8Array(await subtle.digest("SHA-256", buf));
}

const LEAF = Uint8Array.of(0x00);
const NODE = Uint8Array.of(0x01);
const IDENTITY = Uint8Array.of(0x02);

/**
 * Computes a scheme-1 attachment content hash over bytes fed in order, in any slicing:
 *
 *     leaf i   = SHA-256(0x00 || chunk i)          (1 MiB chunks; a zero-byte blob has one, empty)
 *     tree     = RFC 6962 Merkle tree hash of the leaves, node = SHA-256(0x01 || L || R)
 *     identity = SHA-256(0x02 || be64(size) || tree)
 *
 * Holds at most one chunk plus O(log n) hashes (the roots of the perfect subtrees so far), like
 * Go's MerkleBuilder.
 */
export class AttachmentHasher {
  #pending: Uint8Array[] = [];
  #pendingLen = 0;
  #size = 0;
  #stack: Uint8Array[] = [];
  #sizes: number[] = [];
  #leaves = 0;
  #done = false;

  /** Bytes fed so far. */
  get size(): number {
    return this.#size;
  }

  async update(data: Uint8Array): Promise<void> {
    if (this.#done) throw new Error("kdb: AttachmentHasher used after digest");
    let off = 0;
    while (off < data.length) {
      const take = Math.min(ATTACHMENT_CHUNK_BYTES - this.#pendingLen, data.length - off);
      this.#pending.push(data.subarray(off, off + take));
      this.#pendingLen += take;
      this.#size += take;
      off += take;
      if (this.#pendingLen === ATTACHMENT_CHUNK_BYTES) await this.#flushLeaf();
    }
  }

  async #flushLeaf(): Promise<void> {
    const leaf = await sha256([LEAF, ...this.#pending]);
    this.#pending = [];
    this.#pendingLen = 0;
    this.#stack.push(leaf);
    this.#sizes.push(1);
    this.#leaves++;
    while (this.#sizes.length >= 2 && this.#sizes.at(-1) === this.#sizes.at(-2)) {
      const right = this.#stack.pop()!;
      const left = this.#stack.pop()!;
      const s = this.#sizes.pop()! * 2;
      this.#sizes.pop();
      this.#stack.push(await sha256([NODE, left, right]));
      this.#sizes.push(s);
    }
  }

  /** The content hash, lowercase hex. The hasher cannot be fed after this. */
  async digest(): Promise<string> {
    if (this.#done) throw new Error("kdb: AttachmentHasher digested twice");
    // A trailing partial chunk is a leaf; so is the empty chunk of a zero-byte blob.
    if (this.#pendingLen > 0 || this.#leaves === 0) await this.#flushLeaf();
    this.#done = true;
    let tree = this.#stack.at(-1)!;
    for (let i = this.#stack.length - 2; i >= 0; i--) tree = await sha256([NODE, this.#stack[i]!, tree]);
    const size = new Uint8Array(8);
    new DataView(size.buffer).setBigUint64(0, BigInt(this.#size), false);
    return toHex(await sha256([IDENTITY, size, tree]));
  }
}

/** The scheme-1 content hash of bytes held in memory. */
export async function attachmentContentHash(data: Uint8Array): Promise<string> {
  const h = new AttachmentHasher();
  await h.update(data);
  return h.digest();
}

// --- Merkle proofs (scheme 1, RFC 6962) -------------------------------------------------------

/** A chunk's leaf hash: SHA-256(0x00 || chunk). */
export function chunkLeafHash(chunk: Uint8Array): Promise<Uint8Array> {
  return sha256([LEAF, chunk]);
}

/** An inner node's hash: SHA-256(0x01 || left || right). */
export function merkleNodeHash(left: Uint8Array, right: Uint8Array): Promise<Uint8Array> {
  return sha256([NODE, left, right]);
}

/** The scheme-1 content hash (lowercase hex) of a blob of `size` bytes whose Merkle tree hash is `tree`. */
export async function attachmentIdentity(tree: Uint8Array, size: number): Promise<string> {
  const be = new Uint8Array(8);
  new DataView(be.buffer).setBigUint64(0, BigInt(size), false);
  return toHex(await sha256([IDENTITY, be, tree]));
}

/** How many scheme-1 chunks (so leaves) a blob of `size` bytes has: a zero-byte blob has one, empty. */
export function attachmentChunkCount(size: number): number {
  return size <= 0 ? 1 : Math.ceil(size / ATTACHMENT_CHUNK_BYTES);
}

function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

/**
 * Checks that `leaf` is leaf `index` of a tree of `n` leaves whose hash is `tree`, given its audit
 * path (sibling hashes, bottom up) - RFC 6962 §2.1.1, as go/kdb/document.VerifyMerkleInclusion.
 */
export async function verifyMerkleInclusion(
  leaf: Uint8Array,
  index: number,
  n: number,
  proof: readonly Uint8Array[],
  tree: Uint8Array,
): Promise<boolean> {
  if (!Number.isSafeInteger(index) || index < 0 || index >= n) return false;
  // Plain arithmetic rather than bitwise operators, which would truncate past 2^31 leaves.
  let fn = index;
  let sn = n - 1;
  let r = leaf;
  for (const p of proof) {
    if (sn === 0) return false;
    if (fn % 2 === 1 || fn === sn) {
      r = await merkleNodeHash(p, r);
      while (fn % 2 === 0 && fn !== 0) {
        fn = Math.floor(fn / 2);
        sn = Math.floor(sn / 2);
      }
    } else {
      r = await merkleNodeHash(r, p);
    }
    fn = Math.floor(fn / 2);
    sn = Math.floor(sn / 2);
  }
  return sn === 0 && sameBytes(r, tree);
}

/**
 * Every level of an RFC 6962 tree, so each audit path is O(log n) (go/kdb/document.MerkleTree).
 * Built bottom up: a level's unpaired last node is carried up unchanged, which gives the same tree
 * as RFC 6962's split at the largest power of two below n.
 */
export class MerkleTree {
  readonly #levels: Uint8Array[][];

  private constructor(levels: Uint8Array[][]) {
    this.#levels = levels;
  }

  /** The tree over `leaves` (none: the tree of one empty chunk). */
  static async build(leaves: readonly Uint8Array[]): Promise<MerkleTree> {
    let level = leaves.length > 0 ? [...leaves] : [await chunkLeafHash(new Uint8Array(0))];
    const levels = [level];
    while (level.length > 1) {
      const next: Uint8Array[] = [];
      for (let i = 0; i < level.length; i += 2) {
        next.push(i + 1 < level.length ? await merkleNodeHash(level[i]!, level[i + 1]!) : level[i]!);
      }
      levels.push(next);
      level = next;
    }
    return new MerkleTree(levels);
  }

  /** The leaf hashes of a blob held in memory, cut into 1 MiB chunks. */
  static async leavesOf(data: Uint8Array): Promise<Uint8Array[]> {
    const leaves: Uint8Array[] = [];
    for (let off = 0; off < data.length || off === 0; off += ATTACHMENT_CHUNK_BYTES) {
      leaves.push(await chunkLeafHash(data.subarray(off, off + ATTACHMENT_CHUNK_BYTES)));
      if (off + ATTACHMENT_CHUNK_BYTES >= data.length) break;
    }
    return leaves;
  }

  get leafCount(): number {
    return this.#levels[0]!.length;
  }

  /** The tree hash. */
  get root(): Uint8Array {
    return this.#levels.at(-1)![0]!;
  }

  /** Leaf `index`'s audit path, bottom up. */
  proof(index: number): Uint8Array[] {
    const path: Uint8Array[] = [];
    for (const level of this.#levels.slice(0, -1)) {
      const sib = index % 2 === 0 ? index + 1 : index - 1;
      if (sib < level.length) path.push(level[sib]!);
      index = Math.floor(index / 2);
    }
    return path;
  }
}
