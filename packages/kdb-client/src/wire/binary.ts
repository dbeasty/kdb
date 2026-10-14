/**
 * Binary frame bodies (encoding tag 2), matching go/kdb/wire/binary.go and binary_messages.go.
 *
 * A binary body is the message's fields in the protobuf wire format: each field is a key -
 * uvarint(fieldNumber << 3 | wireType) - then a uvarint (wire type 0) or a uvarint length and
 * that many raw bytes (wire type 2). Strings are UTF-8 and never escaped; byte fields are raw.
 * Unknown fields are skipped, so the format grows the way the JSON envelope did: additively.
 *
 * Conventions shared with the Go and Kotlin codecs:
 *   - an absent field is the zero value: "", 0, false, empty bytes, empty list;
 *   - an optional (nullable) field is present exactly when it is non-null, even if "";
 *   - integers are varints of their two's-complement int64 value.
 *
 * This module translates between binary bodies and the same DTO objects the JSON envelope
 * carries (messages.ts), so nothing above the codec knows which encoding a frame used.
 *
 * Neither side sends tag 2 until the handshake ack lists WIRE_FEATURE_BINARY. Tag 3 (zstd) is
 * never offered by this client: it would need a zstd decoder in every runtime it supports.
 */

import { KdbProtocolError } from "../errors.ts";
import { type MessageKind, MSG, TYPE_CODE_BY_KIND as TYPE_CODE } from "./messages.ts";

export const WIRE_FEATURE_BINARY = "binary-frames";
/**
 * The rest of the client message set in binary - for this client, the lease messages. A separate
 * feature because a peer that shipped with WIRE_FEATURE_BINARY alone cannot decode those bodies.
 */
export const WIRE_FEATURE_BINARY_2 = "binary-frames-2";
/** Tag-3 frames: a zstd frame of a tag-2 body. Offered only where a zstd decoder exists. */
export const WIRE_FEATURE_ZSTD = "zstd-frames";
export const ENCODING_BINARY = 0x02;
export const ENCODING_BINARY_ZSTD = 0x03;

const WT_VARINT = 0;
const WT_FIXED64 = 1;
const WT_BYTES = 2;
const WT_FIXED32 = 5;

/** Kinds binary-frames-2 adds, which go binary only when that feature was agreed. */
const BINARY_2_KINDS: ReadonlySet<MessageKind> = new Set<MessageKind>([
  "lockAcquire",
  "lockRenew",
  "lockRelease",
  "lockResult",
]);

/** Whether `kind` goes out binary on a connection that agreed `features`. */
export function hasBinaryForm(kind: MessageKind, features: ReadonlySet<string>): boolean {
  if (!features.has(WIRE_FEATURE_BINARY)) return false;
  if (BINARY_2_KINDS.has(kind)) return features.has(WIRE_FEATURE_BINARY_2);
  return BINARY_KIND_BY_TYPE_CODE.has(TYPE_CODE[kind]);
}

/** Kinds with a binary form, by type code. The handshake (0x01) never has one. */
export const BINARY_KIND_BY_TYPE_CODE: ReadonlyMap<number, MessageKind> = new Map<number, MessageKind>([
  [MSG.LOCK_ACQUIRE, "lockAcquire"],
  [MSG.LOCK_RENEW, "lockRenew"],
  [MSG.LOCK_RELEASE, "lockRelease"],
  [MSG.LOCK_RESULT, "lockResult"],
  [MSG.SESSION_BEGIN, "sessionBegin"],
  [MSG.SESSION_BEGIN_ACK, "sessionBeginAck"],
  [MSG.SQL_EXEC, "sqlExec"],
  [MSG.SQL_RESULT, "sqlResult"],
  [MSG.TX_COMMIT, "txCommit"],
  [MSG.TX_ROLLBACK, "txRollback"],
  [MSG.CONFLICT_REPORT, "conflictReport"],
  [MSG.DOCUMENT_GET, "documentGet"],
  [MSG.DOCUMENT_GET_RESULT, "documentGetResult"],
  [MSG.UPSERT, "upsert"],
  [MSG.UPSERT_RESULT, "upsertResult"],
]);

const textEncoder = new TextEncoder();
// Not fatal: Go's JSON encoder replaces invalid UTF-8 with U+FFFD, and so does this.
const textDecoder = new TextDecoder("utf-8");

class Writer {
  #buf = new Uint8Array(256);
  #len = 0;

  #reserve(n: number): void {
    if (this.#len + n <= this.#buf.length) return;
    let size = this.#buf.length * 2;
    while (size < this.#len + n) size *= 2;
    const next = new Uint8Array(size);
    next.set(this.#buf.subarray(0, this.#len));
    this.#buf = next;
  }

  uvarint(value: number | bigint): void {
    this.#reserve(10);
    if (typeof value === "number" && value >= 0 && value <= Number.MAX_SAFE_INTEGER) {
      let v = value;
      while (v >= 0x80) {
        this.#buf[this.#len++] = (v % 0x80) | 0x80;
        v = Math.floor(v / 0x80);
      }
      this.#buf[this.#len++] = v;
      return;
    }
    // Negative (or unsafe) integers: the 64-bit two's complement, as Go's uint64(int64(v)).
    let v = BigInt.asUintN(64, BigInt(value));
    while (v >= 0x80n) {
      this.#buf[this.#len++] = Number(v & 0x7fn) | 0x80;
      v >>= 7n;
    }
    this.#buf[this.#len++] = Number(v);
  }

  key(field: number, wireType: number): void {
    this.uvarint(field * 8 + wireType);
  }

  raw(bytes: Uint8Array): void {
    this.#reserve(bytes.length);
    this.#buf.set(bytes, this.#len);
    this.#len += bytes.length;
  }

  bytesField(field: number, bytes: Uint8Array): void {
    this.key(field, WT_BYTES);
    this.uvarint(bytes.length);
    this.raw(bytes);
  }

  /** Writes s unless it is empty (or absent). */
  string(field: number, s: string | null | undefined): void {
    if (s === undefined || s === null || s === "") return;
    this.bytesField(field, textEncoder.encode(s));
  }

  /** Writes s whenever it is non-null, even "" - an optional field, or a list element. */
  optString(field: number, s: string | null | undefined): void {
    if (s === undefined || s === null) return;
    this.bytesField(field, textEncoder.encode(s));
  }

  int(field: number, v: number | null | undefined): void {
    if (v === undefined || v === null || v === 0) return;
    this.key(field, WT_VARINT);
    this.uvarint(v);
  }

  optInt(field: number, v: number | null | undefined): void {
    if (v === undefined || v === null) return;
    this.key(field, WT_VARINT);
    this.uvarint(v);
  }

  bool(field: number, v: boolean | null | undefined): void {
    if (!v) return;
    this.key(field, WT_VARINT);
    this.uvarint(1);
  }

  bytes(field: number, v: Uint8Array | readonly number[] | null | undefined): void {
    if (v === undefined || v === null || v.length === 0) return;
    this.bytesField(field, v instanceof Uint8Array ? v : Uint8Array.from(v, (n) => n & 0xff));
  }

  finish(): Uint8Array {
    return this.#buf.subarray(0, this.#len);
  }
}

class Reader {
  readonly #b: Uint8Array;
  #off = 0;

  constructor(b: Uint8Array) {
    this.#b = b;
  }

  get done(): boolean {
    return this.#off >= this.#b.length;
  }

  /**
   * Up to 7 bytes (49 bits) is exact in a number. A longer varint - in practice a negative int64,
   * which Go writes as ten bytes - is re-read with BigInt and returned as its signed value.
   */
  uvarint(): number {
    const start = this.#off;
    let result = 0;
    let mul = 1;
    for (let i = 0; i < 7; i++) {
      if (this.#off >= this.#b.length) throw malformed("truncated varint");
      const byte = this.#b[this.#off++]!;
      result += (byte & 0x7f) * mul;
      if (byte < 0x80) return result;
      mul *= 0x80;
    }
    return this.#bigFrom(start);
  }

  /** Re-reads the varint starting at `start` with BigInt, as a signed int64. */
  #bigFrom(start: number): number {
    let v = 0n;
    let shift = 0n;
    let off = start;
    for (let i = 0; i < 10; i++) {
      if (off >= this.#b.length) throw malformed("truncated varint");
      const byte = this.#b[off++]!;
      v |= BigInt(byte & 0x7f) << shift;
      shift += 7n;
      if (byte < 0x80) {
        this.#off = off;
        return Number(BigInt.asIntN(64, v));
      }
    }
    throw malformed("varint longer than 10 bytes");
  }

  next(): { field: number; wireType: number } {
    const key = this.uvarint();
    const field = Math.floor(key / 8);
    if (field === 0) throw malformed("field number 0");
    return { field, wireType: key % 8 };
  }

  bytes(wireType: number): Uint8Array {
    if (wireType !== WT_BYTES) throw malformed("a bytes field has the wrong wire type");
    const n = this.uvarint();
    if (n > this.#b.length - this.#off) throw malformed("field runs past the end of the body");
    const out = this.#b.subarray(this.#off, this.#off + n);
    this.#off += n;
    return out;
  }

  string(wireType: number): string {
    return textDecoder.decode(this.bytes(wireType));
  }

  int(wireType: number): number {
    if (wireType !== WT_VARINT) throw malformed("an integer field has the wrong wire type");
    return this.uvarint();
  }

  skip(wireType: number): void {
    switch (wireType) {
      case WT_VARINT:
        this.uvarint();
        return;
      case WT_BYTES:
        this.bytes(wireType);
        return;
      case WT_FIXED64:
      case WT_FIXED32: {
        const n = wireType === WT_FIXED64 ? 8 : 4;
        if (this.#b.length - this.#off < n) throw malformed("field runs past the end of the body");
        this.#off += n;
        return;
      }
      default:
        throw malformed(`unknown wire type ${wireType}`);
    }
  }
}

function malformed(what: string): KdbProtocolError {
  return new KdbProtocolError(`kdb: malformed binary body: ${what}`);
}

type Dto = Record<string, unknown>;

const str = (v: unknown): string | undefined => (typeof v === "string" ? v : undefined);
const num = (v: unknown): number | undefined => (typeof v === "number" ? v : undefined);
const bytesOf = (v: unknown): Uint8Array | readonly number[] | undefined =>
  v instanceof Uint8Array || Array.isArray(v) ? (v as Uint8Array | number[]) : undefined;

/**
 * Encodes a DTO as a binary body. Returns undefined for a kind with no binary form, which then
 * goes out as a JSON envelope.
 */
export function encodeBinaryBody(kind: MessageKind, payload: unknown): Uint8Array | undefined {
  const p = (payload ?? {}) as Dto;
  const w = new Writer();
  switch (kind) {
    case "sessionBegin":
      w.string(1, str(p.namespace));
      w.optString(2, str(p.sessionId));
      w.string(3, str(p.readConsistency));
      w.optString(4, str(p.baseVersionHex));
      break;
    case "sessionBeginAck":
      w.string(1, str(p.namespace));
      w.string(2, str(p.sessionId));
      w.string(3, str(p.headHex));
      w.string(4, str(p.readConsistency));
      w.optString(5, str(p.error));
      break;
    case "sqlExec":
      w.string(1, str(p.namespace));
      w.string(2, str(p.sessionId));
      w.string(3, str(p.sql));
      w.optString(4, str(p.parametersJson));
      w.int(5, num(p.pageSize));
      w.string(6, str(p.continuation));
      break;
    case "sqlResult":
      encodeSqlResult(w, p);
      break;
    case "txCommit":
      w.string(1, str(p.namespace));
      w.string(2, str(p.sessionId));
      w.bytes(3, bytesOf(p.transactionBytes));
      break;
    case "txRollback":
      w.string(1, str(p.namespace));
      w.string(2, str(p.sessionId));
      break;
    case "conflictReport":
      w.string(1, str(p.namespace));
      w.bytes(2, bytesOf(p.reportBytes));
      w.optString(3, str(p.errorCode));
      w.optInt(4, num(p.retryAfterMs));
      break;
    case "documentGet":
      w.string(1, str(p.namespace));
      w.string(2, str(p.docId));
      w.string(3, str(p.minCommit));
      break;
    case "documentGetResult":
      w.string(1, str(p.namespace));
      w.string(2, str(p.docId));
      w.optString(3, str(p.json));
      w.string(4, str(p.commitHex));
      w.optString(5, str(p.error));
      w.optString(6, str(p.errorCode));
      w.optInt(7, num(p.retryAfterMs));
      break;
    case "upsert":
      w.string(1, str(p.namespace));
      w.string(2, str(p.docId));
      w.string(3, str(p.json));
      w.string(4, str(p.sessionId));
      break;
    case "upsertResult":
      w.string(1, str(p.namespace));
      w.string(2, str(p.commitHex));
      w.optString(3, str(p.error));
      w.optString(4, str(p.errorCode));
      w.optInt(5, num(p.retryAfterMs));
      break;
    case "lockAcquire":
    case "lockRenew":
    case "lockRelease":
      w.string(1, str(p.namespace));
      w.string(2, str(p.sessionId));
      w.string(3, str(p.docId));
      w.int(4, num(p.ttlMillis));
      break;
    case "lockResult":
      w.string(1, str(p.namespace));
      w.string(2, str(p.sessionId));
      w.string(3, str(p.docId));
      w.bool(4, p.granted === true);
      w.int(5, num(p.fence));
      w.int(6, num(p.expiresAtMillis));
      w.optString(7, str(p.holderSessionId));
      w.optString(8, str(p.error));
      w.optString(9, str(p.errorCode));
      break;
    default:
      return undefined;
  }
  return w.finish();
}

/** SqlResult field 4 is one row: its cells back to back, each a uvarint length and its bytes. */
function encodeSqlResult(w: Writer, p: Dto): void {
  w.string(1, str(p.namespace));
  w.string(2, str(p.sessionId));
  for (const c of (p.columns as string[] | null | undefined) ?? []) w.optString(3, c);
  for (const row of (p.rows as string[][] | null | undefined) ?? []) {
    const cells = row.map((c) => textEncoder.encode(c));
    let size = 0;
    for (const c of cells) size += uvarintLen(c.length) + c.length;
    w.key(4, WT_BYTES);
    w.uvarint(size);
    for (const c of cells) {
      w.uvarint(c.length);
      w.raw(c);
    }
  }
  w.int(5, num(p.rowsAffected));
  w.string(6, str(p.resolvedCommitHex));
  w.bool(7, p.readOnly === true);
  w.optString(8, str(p.error));
  for (const id of (p.generatedIds as string[] | null | undefined) ?? []) w.optString(9, id);
  w.optString(10, str(p.errorCode));
  w.optInt(11, num(p.retryAfterMs));
  w.string(12, str(p.continuation));
  // Field 13: every cell's kind, one byte each, row by row. The DTO carries them as the JSON
  // envelope does: one decimal digit per cell.
  const kinds = str(p.cellKinds);
  if (kinds) w.bytesField(13, Uint8Array.from(kinds, (c) => c.charCodeAt(0) - 48));
}

function uvarintLen(v: number): number {
  let n = 1;
  while (v >= 0x80) {
    v = Math.floor(v / 0x80);
    n++;
  }
  return n;
}

/**
 * Decodes a binary body into the DTO the JSON envelope would have carried for the same message:
 * the same keys, with absent optional fields as null where Go's JSON writes null, and byte
 * fields as Uint8Array (bytesFromWire accepts either form).
 */
export function decodeBinaryBody(typeCode: number, body: Uint8Array): { kind: MessageKind; payload: Dto } {
  const kind = BINARY_KIND_BY_TYPE_CODE.get(typeCode);
  if (kind === undefined) {
    throw new KdbProtocolError(`kdb: message type 0x${typeCode.toString(16)} has no binary form`);
  }
  const r = new Reader(body);
  const p: Dto = {};
  // Each table maps a field number to [key, kind of value]; the defaults below fill what an
  // absent field means.
  const fields = FIELDS[kind] ?? {};
  const lists: Record<string, unknown[]> = {};
  while (!r.done) {
    const { field, wireType } = r.next();
    const spec = fields[field];
    if (spec === undefined) {
      r.skip(wireType);
      continue;
    }
    const [key, type] = spec;
    switch (type) {
      case "s":
        p[key] = r.string(wireType);
        break;
      case "i":
        p[key] = r.int(wireType);
        break;
      case "b":
        p[key] = r.int(wireType) !== 0;
        break;
      case "y":
        p[key] = r.bytes(wireType).slice();
        break;
      case "ls":
        (lists[key] ??= []).push(r.string(wireType));
        break;
      case "row":
        (lists[key] ??= []).push(decodeRow(r.bytes(wireType)));
        break;
      case "kinds":
        p[key] = Array.from(r.bytes(wireType), (k) => String.fromCharCode(48 + Math.min(k, 9))).join("");
        break;
    }
  }
  Object.assign(p, lists);
  for (const [key, value] of Object.entries(DEFAULTS[kind] ?? {})) {
    if (key in p) continue;
    p[key] = Array.isArray(value) ? [] : value instanceof Uint8Array ? new Uint8Array(0) : value;
  }
  return { kind, payload: p };
}

function decodeRow(b: Uint8Array): string[] {
  const r = new Reader(b);
  const cells: string[] = [];
  while (!r.done) cells.push(r.string(WT_BYTES));
  return cells;
}

type FieldType = "s" | "i" | "b" | "y" | "ls" | "row" | "kinds";

const FIELDS: Record<string, Record<number, [string, FieldType]>> = {
  sessionBegin: { 1: ["namespace", "s"], 2: ["sessionId", "s"], 3: ["readConsistency", "s"], 4: ["baseVersionHex", "s"] },
  sessionBeginAck: { 1: ["namespace", "s"], 2: ["sessionId", "s"], 3: ["headHex", "s"], 4: ["readConsistency", "s"], 5: ["error", "s"] },
  sqlExec: { 1: ["namespace", "s"], 2: ["sessionId", "s"], 3: ["sql", "s"], 4: ["parametersJson", "s"], 5: ["pageSize", "i"], 6: ["continuation", "s"] },
  sqlResult: {
    1: ["namespace", "s"], 2: ["sessionId", "s"], 3: ["columns", "ls"], 4: ["rows", "row"],
    5: ["rowsAffected", "i"], 6: ["resolvedCommitHex", "s"], 7: ["readOnly", "b"], 8: ["error", "s"],
    9: ["generatedIds", "ls"], 10: ["errorCode", "s"], 11: ["retryAfterMs", "i"], 12: ["continuation", "s"],
    13: ["cellKinds", "kinds"],
  },
  txCommit: { 1: ["namespace", "s"], 2: ["sessionId", "s"], 3: ["transactionBytes", "y"] },
  txRollback: { 1: ["namespace", "s"], 2: ["sessionId", "s"] },
  conflictReport: { 1: ["namespace", "s"], 2: ["reportBytes", "y"], 3: ["errorCode", "s"], 4: ["retryAfterMs", "i"] },
  documentGet: { 1: ["namespace", "s"], 2: ["docId", "s"], 3: ["minCommit", "s"] },
  documentGetResult: {
    1: ["namespace", "s"], 2: ["docId", "s"], 3: ["json", "s"], 4: ["commitHex", "s"],
    5: ["error", "s"], 6: ["errorCode", "s"], 7: ["retryAfterMs", "i"],
  },
  upsert: { 1: ["namespace", "s"], 2: ["docId", "s"], 3: ["json", "s"], 4: ["sessionId", "s"] },
  upsertResult: { 1: ["namespace", "s"], 2: ["commitHex", "s"], 3: ["error", "s"], 4: ["errorCode", "s"], 5: ["retryAfterMs", "i"] },
  lockAcquire: { 1: ["namespace", "s"], 2: ["sessionId", "s"], 3: ["docId", "s"], 4: ["ttlMillis", "i"] },
  lockRenew: { 1: ["namespace", "s"], 2: ["sessionId", "s"], 3: ["docId", "s"], 4: ["ttlMillis", "i"] },
  lockRelease: { 1: ["namespace", "s"], 2: ["sessionId", "s"], 3: ["docId", "s"] },
  lockResult: {
    1: ["namespace", "s"], 2: ["sessionId", "s"], 3: ["docId", "s"], 4: ["granted", "b"], 5: ["fence", "i"],
    6: ["expiresAtMillis", "i"], 7: ["holderSessionId", "s"], 8: ["error", "s"], 9: ["errorCode", "s"],
  },
};

/**
 * What an absent field decodes to: the value the JSON envelope carries for the same zero field
 * (go/kdb/wire/payload_dto.go). Optional fields the JSON leaves out entirely when null
 * (omitempty) stay absent here too.
 */
const DEFAULTS: Record<string, Dto> = {
  sessionBegin: { namespace: "", sessionId: null, readConsistency: "", baseVersionHex: null },
  sessionBeginAck: { namespace: "", sessionId: "", headHex: "", readConsistency: "" },
  sqlExec: { namespace: "", sessionId: "", sql: "", parametersJson: null },
  sqlResult: {
    namespace: "", sessionId: "", columns: [], rows: [], rowsAffected: 0, resolvedCommitHex: "",
    readOnly: false, error: null, generatedIds: [],
  },
  txCommit: { namespace: "", sessionId: "", transactionBytes: new Uint8Array(0) },
  txRollback: { namespace: "", sessionId: "" },
  conflictReport: { namespace: "", reportBytes: new Uint8Array(0) },
  documentGet: { namespace: "", docId: "" },
  documentGetResult: { namespace: "", docId: "", commitHex: "" },
  upsert: { namespace: "", docId: "", json: "" },
  upsertResult: { namespace: "", commitHex: "" },
  lockAcquire: { namespace: "", sessionId: "", docId: "", ttlMillis: 0 },
  lockRenew: { namespace: "", sessionId: "", docId: "", ttlMillis: 0 },
  lockRelease: { namespace: "", sessionId: "", docId: "" },
  lockResult: { namespace: "", sessionId: "", docId: "", granted: false, fence: 0, expiresAtMillis: 0 },
};

/**
 * The runtime's zstd decoder, when it has one: Node's zlib.zstdDecompressSync (22.15+, 23.8+),
 * found without a static import so this module still loads in a browser. Null elsewhere - and
 * then the client does not offer WIRE_FEATURE_ZSTD.
 */
export const zstdDecompress: ((data: Uint8Array) => Uint8Array) | null = (() => {
  const proc = (globalThis as { process?: { getBuiltinModule?: (id: string) => unknown } }).process;
  const zlib = proc?.getBuiltinModule?.("node:zlib") as
    | { zstdDecompressSync?: (data: Uint8Array, options?: { maxOutputLength?: number }) => Uint8Array }
    | undefined;
  const fn = zlib?.zstdDecompressSync;
  if (typeof fn !== "function") return null;
  return (data: Uint8Array) => {
    const out = fn(data, { maxOutputLength: 16 * 1024 * 1024 });
    return new Uint8Array(out.buffer, out.byteOffset, out.byteLength);
  };
})();
