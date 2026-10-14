/**
 * Attachments: the public types of the attachment API on Client, and the helpers it shares.
 *
 * An attachment is a named blob on a document (several per document, each with an ID and a
 * name unique within the document). Bytes travel apart from transactions: an upload
 * (ATTACHMENT_UPLOAD) only stores them and returns their content hash and size, and an ordinary
 * transaction then references that blob with an attachmentPut operation - so creating,
 * replacing, renaming and deleting attachments follow the transaction's commit, conflict and lease
 * rules, and can share a commit with document writes (Transaction.attachments).
 */

import { KdbError, KdbNotFoundError, type KdbErrorOptions } from "./errors.ts";
import { ATTACHMENT_CHUNK_BYTES, bytesToBase64 } from "./wire/attachments.ts";
import type { CallOptions } from "./operations.ts";

/** One attachment of a document. */
export interface AttachmentInfo {
  /** The attachment's ID, a UUID: stable across replacing its bytes and renaming it. */
  id: string;
  /** Unique within the document. An attachment put with no name is named after its ID. */
  name: string;
  /** Scheme-1 content hash of its bytes, lowercase hex. */
  hash: string;
  size: number;
  /** Media type; "" when none was given. */
  mime: string;
}

/** An uploaded blob: what `uploadAttachment` returns and what an attachmentPut references. */
export interface AttachmentBlob {
  hash: string;
  size: number;
}

/**
 * Bytes to upload. Streams and iterables are read once, in order, and never held whole: the
 * upload sends them in 1 MiB chunks as they arrive.
 */
export type AttachmentSource =
  | Uint8Array
  | ArrayBuffer
  | Blob
  | ReadableStream<Uint8Array>
  | AsyncIterable<Uint8Array>
  | Iterable<Uint8Array>;

/** Which attachment: a string is a name; `{ id }` selects by ID, `{ name }` by name. */
export type AttachmentSelector = string | { id: string } | { name: string };

export interface UploadAttachmentOptions extends CallOptions {
  /**
   * Continue an interrupted upload: the `uploadId` and `received` a KdbUploadError reported. The
   * source must then supply the bytes from `received` on - only the rest is sent. A resumed
   * upload is not hashed locally (the client never saw its first part), so `verify` is moot.
   */
  resume?: { uploadId: string; received: number };
  /**
   * Hash the bytes locally while sending them and check the server's content hash against it.
   * Default true; costs one SHA-256 pass over the data.
   */
  verify?: boolean;
  /** Called after each chunk the server acknowledged, with the bytes it has received so far. */
  onProgress?: (received: number) => void;
}

export interface PutAttachmentOptions extends UploadAttachmentOptions {
  /** The attachment to create or replace, by ID. */
  id?: string;
  /**
   * Its name. With a name and no ID, an attachment of that name is replaced (keeping its ID), or
   * one is created with a fresh ID. With neither, a fresh ID is minted and is also the name.
   */
  name?: string;
  /** Media type. When omitted, a replaced attachment keeps the type it had. */
  mime?: string;
}

export interface ReadAttachmentOptions extends CallOptions {
  /** Read as of this commit (hex); default: the session's read head. */
  asOf?: string;
  /**
   * Start this many bytes in. Such a partial read is NOT hash-verified: for a verified part of an
   * attachment use `openAttachmentRange` / `readAttachmentAt`, which check every chunk against the
   * content hash with its Merkle inclusion proof.
   */
  offset?: number;
  /** The most bytes one page may carry; 0 or omitted lets the server choose. */
  pageBytes?: number;
  /**
   * Check the bytes against the attachment's content hash as they stream (from offset 0). The last
   * page is only handed over once the whole blob is verified; a mismatch throws
   * KdbIntegrityError. Default true.
   */
  verify?: boolean;
}

/** A verified ranged read (`openAttachmentRange`, `readAttachmentAt`). */
export interface AttachmentRangeOptions extends CallOptions {
  /** Read as of this commit (hex); default: the session's read head. */
  asOf?: string;
  /** Where the range starts; default 0. Past the attachment's end throws KdbAttachmentRangeError. */
  offset?: number;
  /** How many bytes; omitted or -1 reads to the end. A range running past the end stops there. */
  length?: number;
}

export interface ListAttachmentsOptions extends CallOptions {
  asOf?: string;
}

/**
 * One attachment (or, from `openAttachmentRange`, one range of it) being read, page by page. Iterate it (for await) or take `stream()`, once: the
 * read pins the version it started on, so every page comes from the same bytes even if a newer
 * version commits meanwhile - and `superseded`, once the read is done, says whether one did.
 */
export interface AttachmentReader extends AsyncIterable<Uint8Array> {
  readonly info: AttachmentInfo;
  /** The commit the read resolved to, hex. */
  readonly commit: string;
  /**
   * After the last page (of the range, for a ranged read): the document's attachment has changed
   * (or gone) since the read began.
   */
  readonly superseded: boolean;
  /** The bytes as a web ReadableStream. */
  stream(): ReadableStream<Uint8Array>;
  /** Ends the read early, telling the server to drop it. */
  cancel(): Promise<void>;
}

/** An upload failed part way. `uploadId` and `received` resume it (UploadAttachmentOptions.resume). */
export class KdbUploadError extends KdbError {
  readonly uploadId: string;
  readonly received: number;

  constructor(message: string, uploadId: string, received: number, options: KdbErrorOptions = {}) {
    super(message, options);
    this.uploadId = uploadId;
    this.received = received;
  }
}

/** Bytes did not match the content hash they were checked against. */
export class KdbIntegrityError extends KdbError {
  readonly expectedHash: string;
  readonly actualHash: string;

  constructor(message: string, expectedHash: string, actualHash: string) {
    super(message, { code: "INTEGRITY" });
    this.expectedHash = expectedHash;
    this.actualHash = actualHash;
  }
}

/**
 * A ranged read started outside the attachment: past its end, or at a negative offset. `size` is
 * the attachment's length, or -1 when the offset was refused before the server was asked.
 */
export class KdbAttachmentRangeError extends KdbError {
  readonly offset: number;
  readonly size: number;

  constructor(offset: number, size: number) {
    super(
      size < 0
        ? `kdb: attachment offset ${offset} is negative`
        : `kdb: attachment offset ${offset} is past its ${size} bytes`,
    );
    this.offset = offset;
    this.size = size;
  }
}

/** A document has no attachment matching the selector. */
export class KdbAttachmentNotFoundError extends KdbNotFoundError {
  readonly attachment: string;

  constructor(namespace: string, docId: string, attachment: string, options: KdbErrorOptions = {}) {
    super(namespace, docId, options);
    this.attachment = attachment;
    this.message = `kdb: not found: attachment ${attachment} of ${namespace}/${docId}`;
  }
}

/** A selector as the get message's (attachmentId, name) pair, and as text for errors. */
export function selectorParts(selector: AttachmentSelector): { id?: string; name?: string; text: string } {
  if (typeof selector === "string") return { name: selector, text: JSON.stringify(selector) };
  if ("id" in selector) return { id: selector.id, text: `ID ${selector.id}` };
  return { name: selector.name, text: JSON.stringify(selector.name) };
}

/** The attachment of `items` a selector picks, if any. */
export function findAttachment(items: AttachmentInfo[], selector: AttachmentSelector): AttachmentInfo | undefined {
  const { id, name } = selectorParts(selector);
  return items.find((i) => (id !== undefined ? i.id === id : i.name === name));
}

// --- SQL parameters ----------------------------------------------------------------------------

/** A SQL parameter binding bytes: `{"$bytes": "<base64>"}`. Uint8Array arguments convert to it on their own. */
export function sqlBytes(data: Uint8Array | ArrayBuffer): { $bytes: string } {
  return { $bytes: bytesToBase64(data instanceof Uint8Array ? data : new Uint8Array(data)) };
}

/**
 * A SQL parameter binding an uploaded blob: `{"$attachment": {"hash", "size"}}`. `STREAM ?`
 * requires one, so a statement never carries large bytes itself:
 *
 *     const blob = await client.uploadAttachment(ns, file.stream());
 *     await client.exec(ns, "UPDATE t SET ATTACHMENT 'scan.pdf' = STREAM ? WHERE id = ?", [sqlAttachment(blob), id]);
 */
export function sqlAttachment(blob: AttachmentBlob): { $attachment: { hash: string; size: number } } {
  return { $attachment: { hash: blob.hash, size: blob.size } };
}

/** Maps one SQL argument to its JSON parameter form: bytes become `$bytes`; the rest is unchanged. */
export function sqlParam(value: unknown): unknown {
  if (value instanceof Uint8Array || value instanceof ArrayBuffer) return sqlBytes(value);
  return value;
}

// --- sources -------------------------------------------------------------------------------------

/** The source's bytes, in whatever slices it yields them. */
async function* sourceSlices(source: AttachmentSource): AsyncGenerator<Uint8Array> {
  if (source instanceof Uint8Array) {
    yield source;
    return;
  }
  if (source instanceof ArrayBuffer) {
    yield new Uint8Array(source);
    return;
  }
  let src: unknown = source;
  if (typeof Blob !== "undefined" && src instanceof Blob) src = src.stream();
  if (typeof ReadableStream !== "undefined" && src instanceof ReadableStream) {
    // getReader rather than async iteration: not every browser's ReadableStream is iterable.
    const reader = (src as ReadableStream<Uint8Array>).getReader();
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) return;
        yield asBytes(value);
      }
    } finally {
      reader.releaseLock();
    }
  }
  if (src !== null && typeof src === "object" && Symbol.asyncIterator in src) {
    for await (const part of src as AsyncIterable<Uint8Array>) yield asBytes(part);
    return;
  }
  if (src !== null && typeof src === "object" && Symbol.iterator in src) {
    for (const part of src as Iterable<Uint8Array>) yield asBytes(part);
    return;
  }
  throw new TypeError("kdb: an attachment source must be bytes, a Blob, a ReadableStream or an (async) iterable of Uint8Array");
}

function asBytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  throw new TypeError("kdb: an attachment source yielded something other than a Uint8Array");
}

/**
 * The source re-cut into chunks of exactly `size` bytes (the last may be shorter), each marked
 * `last` - known by reading one chunk ahead, since the final upload chunk must say so. An empty
 * source is one empty last chunk.
 */
export async function* uploadChunks(
  source: AttachmentSource,
  size = ATTACHMENT_CHUNK_BYTES,
): AsyncGenerator<{ data: Uint8Array; last: boolean }> {
  let held: Uint8Array | undefined;
  for await (const chunk of exactChunks(sourceSlices(source), size)) {
    if (held !== undefined) yield { data: held, last: false };
    held = chunk;
  }
  yield { data: held ?? new Uint8Array(0), last: true };
}

async function* exactChunks(slices: AsyncIterable<Uint8Array>, size: number): AsyncGenerator<Uint8Array> {
  let parts: Uint8Array[] = [];
  let len = 0;
  for await (let slice of slices) {
    while (slice.length > 0) {
      if (len === 0 && slice.length >= size) {
        // Whole chunks straight from the slice, uncopied.
        yield slice.subarray(0, size);
        slice = slice.subarray(size);
        continue;
      }
      const take = Math.min(size - len, slice.length);
      parts.push(slice.subarray(0, take));
      len += take;
      slice = slice.subarray(take);
      if (len === size) {
        yield concat(parts, len);
        parts = [];
        len = 0;
      }
    }
  }
  if (len > 0) yield concat(parts, len);
}

function concat(parts: Uint8Array[], len: number): Uint8Array {
  if (parts.length === 1) return parts[0]!;
  const out = new Uint8Array(len);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}
