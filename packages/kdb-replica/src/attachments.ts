/**
 * Attachments (docs/kdb-attachments-plan.md; replica side in docs/kdb-client-replica-plan.md §9).
 *
 * A record has a set of named attachments. A commit carries attachment ops, and the root of its
 * attachment table: a second trie, hashed like the document trie, mapping each document id to the
 * hash of its attachment set. The replica keeps that table and verifies every root it is given, as
 * it does tree hashes. The bytes themselves are fetched lazily, by BLOB_FETCH.
 *
 * Hashes, exactly as go/kdb/document defines them (pinned by test/golden/attachments.json):
 * - refHash  = SHA-256('A' ‖ scheme ‖ contentHash ‖ be64 size ‖ be32 len(mime) ‖ mime)
 * - setHash  = SHA-256('S' ‖ for each attachment in id order: id(16) ‖ be32 len(name) ‖ name ‖ refHash)
 * - content hash, scheme 1: SHA-256(0x02 ‖ be64 size ‖ T), T the RFC 6962 Merkle tree hash over
 *   leaves SHA-256(0x00 ‖ chunk) of 1 MiB chunks, nodes SHA-256(0x01 ‖ L ‖ R); an empty blob has one
 *   leaf, of the empty chunk.
 */

import { compareBytes, concat, fromHex, fromUtf8, toHex, utf8, uuidBytes, uuidString } from "./bytes.ts";
import type { Op } from "./commit.ts";
import { sha256 } from "./sha256.ts";
import type { Batch, KvStore } from "./store.ts";
import { TrieEdit, TrieReader } from "./trie.ts";

export const ATTACHMENT_SCHEME_CHUNKED_1 = 1;
export const ATTACHMENT_CHUNK_BYTES = 1 << 20;

export interface AttachmentRef {
  scheme: number;
  contentHash: string;
  size: bigint;
  mime: string;
}

export interface Attachment extends AttachmentRef {
  id: string;
  name: string;
}

function be64(v: bigint): Uint8Array {
  const b = new Uint8Array(8);
  new DataView(b.buffer).setBigUint64(0, BigInt.asUintN(64, v));
  return b;
}

function be32(n: number): Uint8Array {
  const b = new Uint8Array(4);
  new DataView(b.buffer).setUint32(0, n >>> 0);
  return b;
}

export function refHash(r: AttachmentRef): string {
  const mime = utf8(r.mime);
  return toHex(sha256(concat([utf8("A"), new Uint8Array([r.scheme]), fromHex(r.contentHash), be64(r.size), be32(mime.length), mime])));
}

/** setHash of a set sorted by id (sortAttachments). */
export function setHash(set: readonly Attachment[]): string {
  const parts: Uint8Array[] = [utf8("S")];
  for (const a of set) {
    const name = utf8(a.name);
    parts.push(uuidBytes(a.id), be32(name.length), name, fromHex(refHash(a)));
  }
  return toHex(sha256(concat(parts)));
}

export function sortAttachments(set: Attachment[]): Attachment[] {
  return set.sort((a, b) => compareBytes(uuidBytes(a.id), uuidBytes(b.id)));
}

function merkleNode(l: Uint8Array, r: Uint8Array): Uint8Array {
  const b = new Uint8Array(65);
  b[0] = 0x01;
  b.set(l, 1);
  b.set(r, 33);
  return sha256(b);
}

/**
 * The scheme-1 content hash, computed as bytes arrive: chunks are hashed as they fill, holding
 * O(log n) subtree roots (go/kdb/document MerkleBuilder).
 */
export class ContentHasher {
  #stack: Uint8Array[] = [];
  #sizes: number[] = [];
  #chunk = new Uint8Array(ATTACHMENT_CHUNK_BYTES);
  #fill = 0;
  #size = 0n;
  #leaves = 0;

  update(data: Uint8Array): this {
    let off = 0;
    while (off < data.length) {
      if (this.#fill === ATTACHMENT_CHUNK_BYTES) this.#flush();
      const n = Math.min(data.length - off, ATTACHMENT_CHUNK_BYTES - this.#fill);
      this.#chunk.set(data.subarray(off, off + n), this.#fill);
      this.#fill += n;
      off += n;
    }
    this.#size += BigInt(data.length);
    return this;
  }

  #add(leaf: Uint8Array): void {
    this.#stack.push(leaf);
    this.#sizes.push(1);
    this.#leaves++;
    while (this.#sizes.length >= 2 && this.#sizes.at(-1) === this.#sizes.at(-2)) {
      const r = this.#stack.pop()!, l = this.#stack.pop()!;
      const s = this.#sizes.pop()! * 2;
      this.#sizes.pop();
      this.#stack.push(merkleNode(l, r));
      this.#sizes.push(s);
    }
  }

  #flush(): void {
    const b = new Uint8Array(1 + this.#fill);
    b[0] = 0x00;
    b.set(this.#chunk.subarray(0, this.#fill), 1);
    this.#add(sha256(b));
    this.#fill = 0;
  }

  /** The content hash of everything given; the hasher is spent afterwards. */
  digest(): string {
    // The last (or only, possibly empty) chunk is always a leaf.
    if (this.#fill > 0 || this.#leaves === 0) this.#flush();
    let h = this.#stack.at(-1)!;
    for (let i = this.#stack.length - 2; i >= 0; i--) h = merkleNode(this.#stack[i]!, h);
    const id = new Uint8Array(41);
    id[0] = 0x02;
    id.set(be64(this.#size), 1);
    id.set(h, 9);
    return toHex(sha256(id));
  }
}

export function attachmentContentHash(data: Uint8Array): string {
  return new ContentHasher().update(data).digest();
}

// --- the attachment table ------------------------------------------------------------------------

/** The table's trie lives beside the document trie; "%at" cannot occur in an encoded name. */
export function attachmentTrieKey(nsKey: string): string {
  return `${nsKey}%att`;
}

interface StoredAttachment {
  id: string;
  name: string;
  scheme: number;
  contentHash: string;
  size: string;
  mime: string;
}

export function encodeAttachmentSet(set: readonly Attachment[]): Uint8Array {
  return utf8(JSON.stringify(set.map((a): StoredAttachment => ({ ...a, size: a.size.toString() }))));
}

export function decodeAttachmentSet(b: Uint8Array): Attachment[] {
  return (JSON.parse(fromUtf8(b)) as StoredAttachment[]).map((a) => ({ ...a, size: BigInt(a.size) }));
}

export class AttachmentError extends Error {
  override name = "AttachmentError";
}

/**
 * An edit of one namespace's attachment table: ops applied in order (go/kdb/document
 * AttachmentTable.Apply - a put adds or replaces by id and refuses a name another attachment of
 * the document has; a delete removes one; a document delete removes them all), then the trie
 * updated and the root computed. Writes reach the store only through writeTo.
 */
export class AttachmentEdit {
  readonly #store: KvStore;
  readonly #nsKey: string;
  readonly #sets = new Map<string, Attachment[]>();
  #trie: TrieEdit | null = null;

  constructor(store: KvStore, nsKey: string) {
    this.#store = store;
    this.#nsKey = nsKey;
  }

  #key(docId: string): string {
    return `a/${this.#nsKey}/${docId.toLowerCase()}`;
  }

  async #load(docId: string): Promise<Attachment[]> {
    const id = docId.toLowerCase();
    let s = this.#sets.get(id);
    if (!s) {
      const b = await this.#store.get(this.#key(id));
      s = b ? decodeAttachmentSet(b) : [];
      this.#sets.set(id, s);
    }
    return s;
  }

  /** Applies the attachment effect of ops, in order. Other ops are ignored. */
  async apply(ops: Iterable<Op>): Promise<void> {
    for (const op of ops) {
      if (op.kind === "attachmentPut") {
        if (op.scheme !== ATTACHMENT_SCHEME_CHUNKED_1) throw new AttachmentError(`unknown content hash scheme ${op.scheme}`);
        const set = await this.#load(op.docId);
        const clash = set.find((a) => a.name === op.name && a.id !== op.attachmentId.toLowerCase());
        if (clash) throw new AttachmentError(`attachment name ${JSON.stringify(op.name)} is already used by attachment ${clash.id}`);
        const next = set.filter((a) => a.id !== op.attachmentId.toLowerCase());
        next.push({ id: op.attachmentId.toLowerCase(), name: op.name, scheme: op.scheme, contentHash: op.contentHash, size: op.size, mime: op.mime });
        this.#sets.set(op.docId.toLowerCase(), sortAttachments(next));
      } else if (op.kind === "attachmentDelete") {
        const set = await this.#load(op.docId);
        this.#sets.set(op.docId.toLowerCase(), set.filter((a) => a.id !== op.attachmentId.toLowerCase()));
      } else if (op.kind === "delete") {
        await this.#load(op.docId);
        this.#sets.set(op.docId.toLowerCase(), []);
      }
    }
  }

  /** Replaces a document's whole set (a squash or snapshot that states final sets). */
  set(docId: string, set: Attachment[]): void {
    this.#sets.set(docId.toLowerCase(), sortAttachments([...set]));
  }

  /** Every document the store holds a set for: what a whole-table replacement must clear. */
  async heldDocuments(): Promise<string[]> {
    const base = `a/${this.#nsKey}/`;
    return (await this.#store.scan(base, base + "\uffff")).map(([k]) => k.slice(base.length));
  }

  /** The table's root after the edit: undefined when the table is empty, as a commit states it. */
  async finish(): Promise<string | undefined> {
    const trie = new TrieEdit(this.#store, attachmentTrieKey(this.#nsKey));
    for (const [id, set] of this.#sets) {
      if (set.length === 0) await trie.delete(id);
      else await trie.set(id, setHash(set));
    }
    this.#trie = trie;
    const root = await trie.finish();
    return /^0+$/.test(root) ? undefined : root;
  }

  writeTo(batch: Batch): void {
    if (!this.#trie) throw new Error("kdb replica: finish the attachment edit before writing it");
    this.#trie.writeTo(batch);
    for (const [id, set] of this.#sets) batch.set(this.#key(id), set.length === 0 ? null : encodeAttachmentSet(set));
  }
}

/** A document's attachments as the replica holds them. */
export async function attachmentsOf(store: KvStore, nsKey: string, docId: string): Promise<Attachment[]> {
  const b = await store.get(`a/${nsKey}/${docId.toLowerCase()}`);
  return b ? decodeAttachmentSet(b) : [];
}

/** The attachment table's current root, undefined when empty. */
export async function attachmentRoot(store: KvStore, nsKey: string): Promise<string | undefined> {
  const r = await new TrieReader(store, attachmentTrieKey(nsKey)).rootHash();
  return /^0+$/.test(r) ? undefined : r;
}

/**
 * The first attachment table in a packed list (go/kdb/document AppendPackedAttachmentTables), as
 * a snapshot's last page carries the snapshot commit's: per table `uvarint(entries)`, then per
 * attachment docId(16) ‖ attachmentId(16) ‖ uvarint(len) name ‖ scheme ‖ contentHash(32) ‖
 * be64 size ‖ uvarint(len) mime.
 */
export function parsePackedAttachmentTable(packed: Uint8Array): Map<string, Attachment[]> {
  let off = 0;
  const view = new DataView(packed.buffer, packed.byteOffset, packed.byteLength);
  const uv = (): number => {
    let v = 0, mul = 1;
    for (let i = 0; i < 10; i++) {
      if (off >= packed.length) throw new AttachmentError("packed attachment tables are truncated");
      const b = packed[off++]!;
      v += (b & 0x7f) * mul;
      if ((b & 0x80) === 0) return v;
      mul *= 128;
    }
    throw new AttachmentError("packed attachment tables: bad varint");
  };
  const take = (n: number): Uint8Array => {
    if (off + n > packed.length) throw new AttachmentError("packed attachment tables are truncated");
    const b = packed.subarray(off, off + n);
    off += n;
    return b;
  };
  const out = new Map<string, Attachment[]>();
  if (packed.length === 0) return out;
  const tables = uv();
  if (tables === 0) return out;
  const entries = uv();
  for (let i = 0; i < entries; i++) {
    const doc = uuidString(take(16));
    const id = uuidString(take(16));
    const name = fromUtf8(take(uv()));
    const scheme = take(1)[0]!;
    const contentHash = toHex(take(32));
    const size = view.getBigInt64(off);
    off += 8;
    const mime = fromUtf8(take(uv()));
    const list = out.get(doc) ?? [];
    list.push({ id, name, scheme, contentHash, size, mime });
    out.set(doc, list);
  }
  for (const list of out.values()) sortAttachments(list);
  return out;
}
