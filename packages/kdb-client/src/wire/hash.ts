/**
 * Document content hashes (Component 63 §6.3).
 *
 * This is the only part of the client that is not JSON, and it has to be exactly right.
 * `replaceIf` compares a content hash the client computes locally - it is never carried on the
 * wire, because it is defined as a pure function of (documentId, json)
 * (go/kdb/client/client.go:640-650). A TS client that computes it differently produces a value
 * the server can never match, and every conditional replace then fails with a precondition
 * error that reads exactly like a phantom concurrent write.
 *
 * The definition, from go/kdb/document/kdb_document.go:107-114:
 *
 *     ContentHash = SHA-256( EncodeBytes( DocumentBody{1: uuid(id), 2: string(json)} ) )
 *
 * where DocumentBody is the record registered in go/kdb/document/wire.go and EncodeBytes is
 * Layer 0's canonical codec (go/kdb/codec/wirecodec.go).
 *
 * Only a sliver of that codec is needed - one record, two fields, one UUID and one string - so
 * this is not a port of Layer 0. Record encoding (encodeRecord, wirecodec.go:132-166) is:
 *
 *     body  = for each field in ascending id order:
 *               leb128(fieldId) ++ [physicalTag] ++ encodedValue
 *     out   = leb128(len(body)) ++ body
 *
 * with physical tags from go/kdb/codec/schema/physical.go: FIXED = 0x0F (what a
 * LogicalUUID-annotated primitive resolves to), STRING = 0x09. A UUID encodes as its 16 raw
 * big-endian bytes; a string as leb128(byteLength) ++ UTF-8 bytes.
 *
 * Correctness here is established by differential testing against go/kdb/interop's golden
 * fixtures, not by re-reading the prose spec: a canonical encoder that is 99% right is 100%
 * broken, and only fixtures find the last 1%.
 */

import { toHex, utf8Encode } from "./bytes.ts";
import { writeLeb128U32 } from "./leb128.ts";
import { uuidToBytes } from "./uuid.ts";

/** Physical kind tags (go/kdb/codec/schema/physical.go). */
const PHYSICAL_STRING = 0x09;
const PHYSICAL_FIXED = 0x0f;

/** DocumentBody field ids (go/kdb/document/wire.go's RecordSchema). */
const FIELD_ID = 1;
const FIELD_JSON = 2;

/**
 * The canonical DocumentBody encoding whose SHA-256 is the content hash.
 *
 * Exported for the fixture tests, which check this encoding independently of the digest - when
 * a hash mismatches, knowing whether the bytes or the digest diverged is the whole debugging
 * story.
 */
export function encodeDocumentBody(docId: string, json: string): Uint8Array {
  const body: number[] = [];

  // Field 1: id, a FIXED(16) carrying the UUID's raw big-endian bytes.
  writeLeb128U32(body, FIELD_ID);
  body.push(PHYSICAL_FIXED);
  for (const b of uuidToBytes(docId)) body.push(b);

  // Field 2: json, a STRING - leb128 byte length then UTF-8. Note the length is in BYTES:
  // Go's appendLebString uses len(s), which is the UTF-8 byte count, not the character count.
  // Any non-ASCII document body would diverge if this used json.length.
  writeLeb128U32(body, FIELD_JSON);
  body.push(PHYSICAL_STRING);
  const jsonBytes = utf8Encode(json);
  writeLeb128U32(body, jsonBytes.length);
  for (const b of jsonBytes) body.push(b);

  const out: number[] = [];
  writeLeb128U32(out, body.length);
  return Uint8Array.from(out.concat(body));
}

/**
 * The content hash of a document, as the lowercase hex string the wire and the server's
 * EXPECT_CONTENT_HASH precondition both use.
 *
 * Async because crypto.subtle.digest is - which is also why getWithHash, replaceIf and
 * compareAndSwap are async for a reason that has nothing to do with the network, and stay
 * async in an embedded runtime where there is no round trip at all.
 */
export async function contentHash(docId: string, json: string): Promise<string> {
  const encoded = encodeDocumentBody(docId, json);
  const digest = await sha256(encoded);
  return toHex(digest);
}

async function sha256(data: Uint8Array): Promise<Uint8Array> {
  const subtle = globalThis.crypto?.subtle;
  if (!subtle) {
    throw new Error(
      "kdb: no Web Crypto SubtleCrypto available - content hashes need globalThis.crypto.subtle " +
        "(Node 20+, browsers over HTTPS/localhost, Bun, Deno, Workers)",
    );
  }
  // A fresh copy: some runtimes reject a view onto a SharedArrayBuffer, and slicing also
  // guarantees the exact byte range regardless of the view's offset.
  const buffer = data.slice().buffer as ArrayBuffer;
  return new Uint8Array(await subtle.digest("SHA-256", buffer));
}
