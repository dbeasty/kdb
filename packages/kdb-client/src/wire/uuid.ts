/**
 * UUID handling, matching `codec.UUIDFromString` / `codec.RandomUUID` in go/kdb/codec.
 *
 * Document ids are UUIDs, not free-form strings: `PutJSON` calls `codec.UUIDFromString(docID)`
 * and fails on anything else (go/kdb/client/client.go:434-437). The wire carries them as plain
 * strings, so this validation has nowhere else to live - without it an ordinary typo surfaces
 * as an opaque server error rather than as the argument error it is.
 */

import { KdbError } from "../errors.ts";

const UUID_RE =
  /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;

export function isUuid(value: string): boolean {
  return UUID_RE.test(value);
}

/** Throws a clear argument error unless `value` is a canonical UUID string. */
export function assertUuid(value: string, what = "docId"): string {
  if (!isUuid(value)) {
    throw new KdbError(
      `kdb: invalid ${what}: ${JSON.stringify(value)} is not a UUID ` +
        "(document ids are UUIDs - see go/kdb/codec's UUIDFromString)",
    );
  }
  return value.toLowerCase();
}

/**
 * The 16 raw bytes of a UUID, big-endian.
 *
 * Go stores a UUID as two int64s and writes them with writeBE64(MSB) then writeBE64(LSB)
 * (`uuidWire` in go/kdb/codec/wirecodec.go). That is byte-for-byte the canonical hex string
 * with its dashes removed, so parsing the string directly avoids reproducing the 64-bit split -
 * and avoids the precision hazard of routing 64-bit values through JS numbers.
 */
export function uuidToBytes(value: string): Uint8Array {
  const hex = assertUuid(value).replace(/-/g, "");
  const out = new Uint8Array(16);
  for (let i = 0; i < 16; i++) {
    out[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

export function uuidFromBytes(bytes: Uint8Array): string {
  if (bytes.length !== 16) {
    throw new KdbError(`kdb: a UUID is 16 bytes, got ${bytes.length}`);
  }
  let hex = "";
  for (const b of bytes) hex += b.toString(16).padStart(2, "0");
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20, 32),
  ].join("-");
}

/** A random v4 UUID, for transaction ids and the client's own author node id. */
export function randomUuid(): string {
  const c = globalThis.crypto;
  if (c && typeof c.randomUUID === "function") return c.randomUUID();
  if (c && typeof c.getRandomValues === "function") {
    const bytes = c.getRandomValues(new Uint8Array(16));
    bytes[6] = (bytes[6]! & 0x0f) | 0x40;
    bytes[8] = (bytes[8]! & 0x3f) | 0x80;
    return uuidFromBytes(bytes);
  }
  throw new KdbError(
    "kdb: no Web Crypto available - @kdb/client needs globalThis.crypto (Node 20+, browsers, Bun, Deno, Workers)",
  );
}
