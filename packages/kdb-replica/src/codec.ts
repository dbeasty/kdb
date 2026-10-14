/**
 * The slice of Layer 0's canonical codec (go/kdb/codec/wirecodec.go) that commits use.
 *
 * A record is `leb128(len(body)) ++ body`, and its body is, for each present field in ascending
 * id order, `leb128(fieldId) ++ physicalTag ++ value`. Values:
 * - fixed: raw bytes;
 * - string: `leb128(len) ++ utf8`;
 * - int64 (and timestamps): 8 bytes little-endian;
 * - array: `leb128(count) ++ elements`;
 * - union and nullable: one branch byte, then the value (nullable: 0 = null, 1 = present).
 * A field whose schema default is null is omitted when null. That is why a commit without a
 * schema hash hashes as it does on the JVM.
 */

import { fromUtf8, utf8 } from "./bytes.ts";

/** Physical tags (go/kdb/codec/schema/physical.go). */
export const Tag = {
  Bool: 0x01,
  Int8: 0x02,
  Int16: 0x03,
  Int32: 0x04,
  Int64: 0x05,
  Float32: 0x06,
  Float64: 0x07,
  Bytes: 0x08,
  String: 0x09,
  Array: 0x0a,
  Map: 0x0b,
  Record: 0x0c,
  Enum: 0x0d,
  Union: 0x0e,
  Fixed: 0x0f,
} as const;

export class Writer {
  #buf = new Uint8Array(256);
  #len = 0;

  #grow(n: number): void {
    if (this.#len + n <= this.#buf.length) return;
    let cap = this.#buf.length * 2;
    while (cap < this.#len + n) cap *= 2;
    const next = new Uint8Array(cap);
    next.set(this.#buf.subarray(0, this.#len));
    this.#buf = next;
  }

  byte(b: number): this {
    this.#grow(1);
    this.#buf[this.#len++] = b;
    return this;
  }

  raw(b: Uint8Array): this {
    this.#grow(b.length);
    this.#buf.set(b, this.#len);
    this.#len += b.length;
    return this;
  }

  leb(n: number): this {
    if (!Number.isInteger(n) || n < 0 || n > 0xffffffff) throw new Error(`kdb replica: leb128 out of range: ${n}`);
    do {
      let b = n & 0x7f;
      n = Math.floor(n / 128);
      if (n > 0) b |= 0x80;
      this.byte(b);
    } while (n > 0);
    return this;
  }

  string(s: string): this {
    const b = utf8(s);
    return this.leb(b.length).raw(b);
  }

  int64(v: bigint): this {
    const b = new Uint8Array(8);
    new DataView(b.buffer).setBigInt64(0, v, true);
    return this.raw(b);
  }

  /** A field header: id, then the field's physical tag. */
  field(id: number, tag: number): this {
    return this.leb(id).byte(tag);
  }

  /** Writes body (built by fn) as a length-prefixed record. */
  record(fn: (w: Writer) => void): this {
    const inner = new Writer();
    fn(inner);
    const body = inner.bytes();
    return this.leb(body.length).raw(body);
  }

  bytes(): Uint8Array {
    return this.#buf.slice(0, this.#len);
  }
}

export class DecodeError extends Error {
  override name = "DecodeError";
}

export class Reader {
  readonly #b: Uint8Array;
  pos: number;
  readonly limit: number;

  constructor(b: Uint8Array, pos = 0, limit = b.length) {
    this.#b = b;
    this.pos = pos;
    this.limit = limit;
  }

  get done(): boolean {
    return this.pos >= this.limit;
  }

  byte(): number {
    if (this.pos >= this.limit) throw new DecodeError(`truncated at ${this.pos}`);
    return this.#b[this.pos++]!;
  }

  raw(n: number): Uint8Array {
    if (n < 0 || this.pos + n > this.limit) throw new DecodeError(`truncated: need ${n} bytes at ${this.pos}`);
    const out = this.#b.subarray(this.pos, this.pos + n);
    this.pos += n;
    return out;
  }

  leb(): number {
    let result = 0;
    let mul = 1;
    for (let i = 0; i < 10; i++) {
      const b = this.byte();
      result += (b & 0x7f) * mul;
      if ((b & 0x80) === 0) {
        if (result > Number.MAX_SAFE_INTEGER) throw new DecodeError("leb128 too large");
        return result;
      }
      mul *= 128;
    }
    throw new DecodeError("leb128 too long");
  }

  string(): string {
    return fromUtf8(this.raw(this.leb()));
  }

  int64(): bigint {
    const b = this.raw(8);
    return new DataView(b.buffer, b.byteOffset, 8).getBigInt64(0, true);
  }

  /**
   * Reads a length-prefixed record, calling onField for each field. onField returns false for a
   * field it does not know, which is then skipped as Go's skipTagged does (scalars only).
   */
  record(onField: (id: number, tag: number, r: Reader) => boolean): void {
    const len = this.leb();
    const end = this.pos + len;
    if (end > this.limit) throw new DecodeError("record overruns its container");
    const r = new Reader(this.#b, this.pos, end);
    while (!r.done) {
      const id = r.leb();
      const tag = r.byte();
      if (!onField(id, tag, r)) skipTagged(r, tag);
    }
    this.pos = end;
  }
}

function skipTagged(r: Reader, tag: number): void {
  switch (tag) {
    case Tag.Int32:
    case Tag.Float32:
      r.raw(4);
      return;
    case Tag.Int64:
    case Tag.Float64:
      r.raw(8);
      return;
    case Tag.String:
    case Tag.Bytes:
      r.raw(r.leb());
      return;
    case Tag.Bool:
    case Tag.Int8:
      r.raw(1);
      return;
    case Tag.Int16:
      r.raw(2);
      return;
    default:
      throw new DecodeError(`cannot skip unknown field with tag 0x${tag.toString(16)}`);
  }
}

export function expectTag(id: number, got: number, want: number): void {
  if (got !== want) {
    throw new DecodeError(`field ${id}: tag 0x${got.toString(16)}, expected 0x${want.toString(16)}`);
  }
}
