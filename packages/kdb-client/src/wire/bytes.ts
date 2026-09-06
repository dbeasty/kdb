/**
 * The ByteArray-as-array-of-numbers convention (Component 63 §6.2).
 *
 * Every wire field whose Kotlin counterpart is a plain `ByteArray` - transactionBytes,
 * reportBytes, commitsPayload, snapshotBytes, schemaBytes, schemaDeltaBytes - travels as a JSON
 * array of numbers, e.g. [123, 34, 105], because that is kotlinx.serialization's default for a
 * ByteArray with no custom serializer.
 *
 * Getting this wrong is not a soft failure. The JVM's strict decoder throws
 * JsonDecodingException on an unexpected token and the connection is torn down. Go's
 * encoding/json defaults to a base64 *string* for []byte and had to be overridden
 * (go/kdb/wire/payload_dto.go:5-14, where the comment records that only a live cross-language
 * interop test caught it). JavaScript has its own distinct wrong answer: JSON.stringify on a
 * Uint8Array produces the *object* {"0":123,"1":34}, since a typed array is not an Array. So
 * every one of those fields goes through these two functions, never through the default.
 */

import { KdbProtocolError } from "../errors.ts";

/** Encodes bytes as the JSON array of numbers the wire expects. */
export function bytesToWire(bytes: Uint8Array): number[] {
  return Array.from(bytes);
}

/** Decodes the wire's array of numbers back to bytes. */
export function bytesFromWire(value: unknown, field: string): Uint8Array {
  if (!Array.isArray(value)) {
    throw new KdbProtocolError(
      `kdb: ${field} must be a JSON array of byte values, got ${describe(value)}`,
    );
  }
  const out = new Uint8Array(value.length);
  for (let i = 0; i < value.length; i++) {
    const n = value[i];
    if (typeof n !== "number" || !Number.isInteger(n) || n < -128 || n > 255) {
      throw new KdbProtocolError(
        `kdb: ${field}[${i}] is not a byte value: ${JSON.stringify(n)}`,
      );
    }
    // Kotlin's Byte is signed, so a JVM-encoded array can carry -1 where Go writes 255.
    // Both denote the same octet; & 0xff normalizes them.
    out[i] = n & 0xff;
  }
  return out;
}

/** Encodes bytes as UTF-8 JSON text. */
export function utf8Encode(text: string): Uint8Array {
  return new TextEncoder().encode(text);
}

export function utf8Decode(bytes: Uint8Array): string {
  return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
}

export function toHex(bytes: Uint8Array): string {
  let out = "";
  for (const b of bytes) out += b.toString(16).padStart(2, "0");
  return out;
}

export function fromHex(hex: string, field = "hash"): Uint8Array {
  if (hex.length % 2 !== 0 || /[^0-9a-fA-F]/.test(hex)) {
    throw new KdbProtocolError(`kdb: ${field} is not valid hex: ${hex}`);
  }
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

function describe(value: unknown): string {
  if (value === null) return "null";
  if (value instanceof Uint8Array) return "a Uint8Array (encode it with bytesToWire)";
  return typeof value;
}
