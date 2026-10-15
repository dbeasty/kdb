/**
 * The document tree: KDB's 16-ary Merkle trie over document-id nibbles
 * (go/kdb/document/document_tree_trie.go), stored node by node so it never has to be resident.
 *
 * Hashing, exactly as Go defines it:
 * - leaf:     SHA-256(0x00 ‖ uuid(16) ‖ contentHash(32))
 * - internal: SHA-256(0x01 ‖ 16 × childHash(32)), all zeros for an absent child
 * - a subtree holding exactly one entry is stored as one leaf record at the shallowest depth where
 *   that is true, and its hash is the leaf hash folded up through the single-child internal nodes
 *   it stands for (`foldLeaf`). Go's tree hashes are therefore unchanged by the compression.
 * - the empty tree's hash is 32 zero bytes.
 *
 * Records are keyed by namespace and nibble prefix. A record exists only where a walk from the root
 * reaches it: when a subtree collapses to one entry, the records beneath it are deleted, so a
 * later split never finds a stale one.
 *
 * Edits go through a TrieEdit: an overlay over the store that loads records as it needs them and
 * hashes only at `finish`, deepest first. A batch of inserts therefore hashes each changed node
 * once, however many of the batch's documents pass through it. Nothing reaches the store until the
 * caller applies the edit's batch. A fast-forward computes the root first and checks it against the
 * commit's declared tree hash, so a mismatch writes nothing.
 */

import { bytesEqual, toHex, uuidBytes, uuidString, ZERO32 } from "./bytes.ts";
import { onlyChildHash, sha256 } from "./sha256.ts";
import type { Batch, KvStore } from "./store.ts";

export type TrieRecord =
  | { leaf: true; id: Uint8Array; content: Uint8Array; hash: Uint8Array | null }
  | { leaf: false; children: Uint8Array; hash: Uint8Array | null };

const HEXN = "0123456789abcdef";

function nibble(id: Uint8Array, depth: number): number {
  const b = id[depth >> 1]!;
  return depth & 1 ? b & 0x0f : b >> 4;
}

export function leafHash(id: Uint8Array, content: Uint8Array): Uint8Array {
  const buf = new Uint8Array(49);
  buf[0] = 0x00;
  buf.set(id, 1);
  buf.set(content, 17);
  return sha256(buf);
}

/** The hash of the subtree at depth that holds only (id, content). */
export function foldLeaf(id: Uint8Array, content: Uint8Array, depth: number): Uint8Array {
  let h = leafHash(id, content);
  for (let d = 31; d >= depth; d--) h = onlyChildHash(nibble(id, d), h);
  return h;
}

function internalHash(children: Uint8Array): Uint8Array {
  const buf = new Uint8Array(1 + 16 * 32);
  buf[0] = 0x01;
  buf.set(children, 1);
  return sha256(buf);
}

function childHash(children: Uint8Array, nib: number): Uint8Array {
  return children.subarray(nib * 32, nib * 32 + 32);
}

function isZero(h: Uint8Array): boolean {
  for (let i = 0; i < h.length; i++) if (h[i] !== 0) return false;
  return true;
}

// Record encoding. leaf: 0x00 ‖ id(16) ‖ content(32) ‖ hash(32).
// internal: 0x01 ‖ hash(32) ‖ u16 occupancy bitmap (big-endian) ‖ 32 bytes per present child.
export function encodeRecord(r: TrieRecord): Uint8Array {
  if (!r.hash) throw new Error("kdb replica: trie record written before it was hashed");
  if (r.leaf) {
    const out = new Uint8Array(81);
    out[0] = 0x00;
    out.set(r.id, 1);
    out.set(r.content, 17);
    out.set(r.hash, 49);
    return out;
  }
  let bitmap = 0, n = 0;
  for (let i = 0; i < 16; i++) if (!isZero(childHash(r.children, i))) { bitmap |= 1 << (15 - i); n++; }
  const out = new Uint8Array(35 + n * 32);
  out[0] = 0x01;
  out.set(r.hash, 1);
  out[33] = bitmap >> 8;
  out[34] = bitmap & 0xff;
  let off = 35;
  for (let i = 0; i < 16; i++) {
    if (bitmap & (1 << (15 - i))) { out.set(childHash(r.children, i), off); off += 32; }
  }
  return out;
}

export function decodeRecord(b: Uint8Array): TrieRecord {
  if (b[0] === 0x00 && b.length === 81) {
    return { leaf: true, id: b.slice(1, 17), content: b.slice(17, 49), hash: b.slice(49, 81) };
  }
  if (b[0] === 0x01 && b.length >= 35) {
    const bitmap = (b[33]! << 8) | b[34]!;
    const children = new Uint8Array(512);
    let off = 35;
    for (let i = 0; i < 16; i++) {
      if (bitmap & (1 << (15 - i))) { children.set(b.subarray(off, off + 32), i * 32); off += 32; }
    }
    if (off !== b.length) throw new Error("kdb replica: corrupt trie record");
    return { leaf: false, children, hash: b.slice(1, 33) };
  }
  throw new Error("kdb replica: corrupt trie record");
}

/** Trie records of one namespace live under this key prefix. */
export function trieKeyPrefix(nsKey: string): string {
  return `t/${nsKey}/`;
}

/** Reads a namespace's trie: the root hash and records by nibble prefix. */
export class TrieReader {
  readonly #store: KvStore;
  readonly #prefix: string;

  constructor(store: KvStore, nsKey: string) {
    this.#store = store;
    this.#prefix = trieKeyPrefix(nsKey);
  }

  async record(prefix: string): Promise<TrieRecord | null> {
    const b = await this.#store.get(this.#prefix + prefix);
    return b ? decodeRecord(b) : null;
  }

  async rootHash(): Promise<string> {
    const r = await this.record("");
    return toHex(r?.hash ?? ZERO32);
  }

  /** The content hash of id, or null when the tree does not hold it. */
  async lookup(id: string): Promise<string | null> {
    const key = uuidBytes(id);
    let prefix = "";
    for (let depth = 0; depth <= 32; depth++) {
      const r = await this.record(prefix);
      if (!r) return null;
      if (r.leaf) return bytesEqual(r.id, key) ? toHex(r.content) : null;
      prefix += HEXN[nibble(key, depth)];
    }
    return null;
  }

  /** Visits every (id, contentHash) in id order. */
  async walk(visit: (id: string, content: string) => void | Promise<void>, prefix = ""): Promise<void> {
    const r = await this.record(prefix);
    if (!r) return;
    if (r.leaf) {
      await visit(uuidString(r.id), toHex(r.content));
      return;
    }
    for (let i = 0; i < 16; i++) {
      if (!isZero(childHash(r.children, i))) await this.walk(visit, prefix + HEXN[i]);
    }
  }
}

/** A batch of tree changes over a store, hashed and written as one. */
export class TrieEdit {
  readonly #store: KvStore;
  readonly #prefix: string;
  readonly #overlay = new Map<string, TrieRecord | null>();
  readonly #dirty = new Set<string>();
  #finished = false;

  constructor(store: KvStore, nsKey: string) {
    this.#store = store;
    this.#prefix = trieKeyPrefix(nsKey);
  }

  async #load(prefix: string): Promise<TrieRecord | null> {
    if (this.#overlay.has(prefix)) return this.#overlay.get(prefix)!;
    const b = await this.#store.get(this.#prefix + prefix);
    const r = b ? decodeRecord(b) : null;
    this.#overlay.set(prefix, r);
    return r;
  }

  #put(prefix: string, r: TrieRecord | null): void {
    if (r) r.hash = null;
    this.#overlay.set(prefix, r);
    this.#dirty.add(prefix);
  }

  #touch(prefix: string, r: TrieRecord): void {
    r.hash = null;
    this.#dirty.add(prefix);
  }

  /** Puts (or replaces) id with contentHash. */
  async set(id: string, contentHashHex: string): Promise<void> {
    this.#check();
    const key = uuidBytes(id);
    const content = hex32(contentHashHex);
    let prefix = "";
    // depth 32 is a full id: only a leaf can be there, and only an empty position is reached.
    for (let depth = 0; depth <= 32; depth++) {
      const r = await this.#load(prefix);
      if (!r) {
        this.#put(prefix, { leaf: true, id: key, content, hash: null });
        return;
      }
      if (r.leaf) {
        if (bytesEqual(r.id, key)) {
          if (!bytesEqual(r.content, content)) this.#put(prefix, { leaf: true, id: key, content, hash: null });
          return;
        }
        // Split: this position now holds two entries. The old one moves down one level (its
        // child position is free: nothing exists beneath a leaf), and the loop carries on down
        // for the new one, splitting again while the two share nibbles.
        this.#put(prefix, { leaf: false, children: new Uint8Array(512), hash: null });
        this.#put(prefix + HEXN[nibble(r.id, depth)], { leaf: true, id: r.id, content: r.content, hash: null });
      } else {
        this.#touch(prefix, r);
      }
      prefix += HEXN[nibble(key, depth)];
    }
    throw new Error("kdb replica: trie deeper than a UUID");
  }

  /** Removes id; a no-op when the tree does not hold it. */
  async delete(id: string): Promise<void> {
    this.#check();
    await this.#remove("", 0, uuidBytes(id));
  }

  async #present(prefix: string, r: TrieRecord & { leaf: false }, nib: number): Promise<boolean> {
    const child = prefix + HEXN[nib];
    if (this.#overlay.has(child)) return this.#overlay.get(child) !== null;
    return !isZero(childHash(r.children, nib));
  }

  async #remove(prefix: string, depth: number, key: Uint8Array): Promise<boolean> {
    const r = await this.#load(prefix);
    if (!r) return false;
    if (r.leaf) {
      if (!bytesEqual(r.id, key)) return false;
      this.#put(prefix, null);
      return true;
    }
    if (!(await this.#remove(prefix + HEXN[nibble(key, depth)], depth + 1, key))) return false;
    this.#touch(prefix, r);
    let only = -1, count = 0;
    for (let i = 0; i < 16; i++) {
      if (await this.#present(prefix, r, i)) { only = i; count++; }
    }
    if (count === 0) {
      this.#put(prefix, null);
    } else if (count === 1) {
      // A subtree down to one entry is stored as that entry's leaf, here: collapse, and delete the
      // record it came from so nothing stale is left beneath a leaf.
      const child = await this.#load(prefix + HEXN[only]);
      if (child?.leaf) {
        this.#put(prefix, { leaf: true, id: child.id, content: child.content, hash: null });
        this.#put(prefix + HEXN[only], null);
      }
    }
    return true;
  }

  #check(): void {
    if (this.#finished) throw new Error("kdb replica: trie edit already finished");
  }

  /** Hashes every changed record, deepest first, and returns the new root hash. */
  async finish(): Promise<string> {
    if (!this.#finished) {
      const order = [...this.#dirty].sort((a, b) => b.length - a.length);
      for (const p of order) {
        const r = this.#overlay.get(p);
        if (!r) continue;
        if (r.leaf) {
          r.hash = foldLeaf(r.id, r.content, p.length);
          continue;
        }
        for (let i = 0; i < 16; i++) {
          const cp = p + HEXN[i];
          if (this.#overlay.has(cp)) r.children.set(this.#overlay.get(cp)?.hash ?? ZERO32, i * 32);
        }
        r.hash = internalHash(r.children);
      }
      this.#finished = true;
    }
    const root = await this.#load("");
    return toHex(root?.hash ?? ZERO32);
  }

  /** Adds the edit's record writes and deletes to batch. Call after finish(). */
  writeTo(batch: Batch): void {
    if (!this.#finished) throw new Error("kdb replica: finish the trie edit before writing it");
    for (const p of this.#dirty) {
      const r = this.#overlay.get(p);
      batch.set(this.#prefix + p, r ? encodeRecord(r) : null);
    }
  }
}

function hex32(hex: string): Uint8Array {
  if (!/^[0-9a-f]{64}$/.test(hex)) throw new Error(`kdb replica: not a 32-byte hex hash: ${hex}`);
  const out = new Uint8Array(32);
  for (let i = 0; i < 32; i++) out[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return out;
}
