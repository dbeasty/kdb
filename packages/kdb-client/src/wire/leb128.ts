/**
 * LEB128 unsigned varints, matching `appendLeb128U32` / `readLeb128U32` in
 * go/kdb/codec/wire_numbers.go.
 *
 * Used only by the canonical value codec (hash.ts) - the wire *frame* format is fixed-width
 * little-endian and does not use varints at all.
 */

import { KdbProtocolError } from "../errors.ts";

/** Appends `value` to `out` as an unsigned LEB128 varint. */
export function writeLeb128U32(out: number[], value: number): void {
  if (!Number.isInteger(value) || value < 0 || value > 0xffffffff) {
    throw new KdbProtocolError(`kdb: leb128 value out of uint32 range: ${value}`);
  }
  // >>> keeps the shift unsigned; a plain >> would sign-extend past 2^31.
  let cur = value >>> 0;
  for (;;) {
    const chunk = cur & 0x7f;
    cur = cur >>> 7;
    if (cur === 0) {
      out.push(chunk);
      return;
    }
    out.push(chunk | 0x80);
  }
}

export interface Leb128Result {
  value: number;
  bytesRead: number;
}

/** Reads an unsigned LEB128 varint starting at `offset`. */
export function readLeb128U32(bytes: Uint8Array, offset: number): Leb128Result {
  let value = 0;
  let shift = 0;
  let pos = offset;
  for (;;) {
    if (pos >= bytes.length) {
      throw new KdbProtocolError("kdb: truncated leb128 varint");
    }
    const byte = bytes[pos]!;
    pos += 1;
    value += (byte & 0x7f) * Math.pow(2, shift);
    if ((byte & 0x80) === 0) break;
    shift += 7;
    if (shift > 28) {
      throw new KdbProtocolError("kdb: leb128 varint exceeds uint32 range");
    }
  }
  if (value > 0xffffffff) {
    throw new KdbProtocolError("kdb: leb128 varint exceeds uint32 range");
  }
  return { value, bytesRead: pos - offset };
}
