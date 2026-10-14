/**
 * Attachment bytes on the replica (docs/kdb-client-replica-plan.md §9): fetched lazily, on first
 * read, by BLOB_FETCH - every chunk checked against the blob's content hash with its RFC 6962 proof
 * - and kept by content hash in 1 MiB chunks, so a blob shared by several attachments is stored
 * once and a read can resume. A blob the replica wrote is pinned until the host holds it.
 *
 * Fetched bytes are a cache, not the replica's data: the host keeps every blob a commit references,
 * so what is evicted here is fetched again on the next read. The cache is bounded by bytes
 * (cacheBytes; 0 for no bound) and evicts the least recently used blobs first - never a pinned
 * one, whose bytes exist nowhere else yet. It is trimmed after every fetch and every unpin, and on
 * demand (evict).
 */

import { ATTACHMENT_CHUNK_BYTES, attachmentContentHash } from "./attachments.ts";
import { blobIdentity, chunkLeaf, verifyInclusion, type BlobFetchEntry } from "./binwire.ts";
import { concat, fromHex, fromUtf8, utf8 } from "./bytes.ts";
import type { PeerConnection } from "./peer.ts";
import { prefixEnd, type Batch, type KvStore } from "./store.ts";

export class BlobUnavailableError extends Error {
  override name = "BlobUnavailableError";
}

interface BlobMeta {
  size: number;
  /** Chunks held, by index. */
  have: number[];
  /** Written here and not yet confirmed by the host. */
  pinned: boolean;
  /** When the blob was last written or read here (ms since the epoch), for eviction. */
  used?: number;
}

/** What the blob cache holds. */
export interface BlobCacheStats {
  /** Bytes of every blob held, pinned or not. */
  bytes: number;
  blobs: number;
  /** Bytes of pinned blobs: written here, not yet uploaded - never evicted. */
  pinnedBytes: number;
  /** The bound on unpinned bytes; 0 for none. */
  limit: number;
}

/** Default bound on the bytes of fetched attachments kept. */
export const DEFAULT_BLOB_CACHE_BYTES = 256 * 1024 * 1024;

/** A read refreshes a blob's recency at most this often, so reading is not a write each time. */
const TOUCH_EVERY_MS = 60_000;

function chunkCount(size: number): number {
  return Math.max(1, Math.ceil(size / ATTACHMENT_CHUNK_BYTES));
}

/** The bytes a blob's held chunks take (every chunk is 1 MiB but the last). */
function heldBytes(m: BlobMeta): number {
  const n = chunkCount(m.size);
  let bytes = 0;
  for (const i of m.have) bytes += i === n - 1 ? m.size - i * ATTACHMENT_CHUNK_BYTES : ATTACHMENT_CHUNK_BYTES;
  return bytes;
}

export class BlobStore {
  readonly #store: KvStore;
  readonly #limit: number;
  /** Serializes evictions, so two concurrent fetches do not evict the same blob twice. */
  #evicting: Promise<unknown> = Promise.resolve();

  /** cacheBytes bounds the unpinned bytes kept (0: no bound). */
  constructor(store: KvStore, cacheBytes = DEFAULT_BLOB_CACHE_BYTES) {
    this.#store = store;
    this.#limit = Math.max(0, cacheBytes);
  }

  #metaKey(hash: string): string {
    return `bm/${hash}`;
  }

  #chunkKey(hash: string, i: number): string {
    return `b/${hash}/${i.toString().padStart(8, "0")}`;
  }

  async meta(hash: string): Promise<BlobMeta | undefined> {
    const b = await this.#store.get(this.#metaKey(hash));
    return b ? (JSON.parse(fromUtf8(b)) as BlobMeta) : undefined;
  }

  async complete(hash: string): Promise<boolean> {
    const m = await this.meta(hash);
    return m !== undefined && m.have.length === chunkCount(m.size);
  }

  /** Stores bytes written on this replica, pinned until the host has them. Returns the hash. */
  async putLocal(data: Uint8Array): Promise<string> {
    const hash = attachmentContentHash(data);
    const batch: Batch = new Map();
    const n = chunkCount(data.length);
    for (let i = 0; i < n; i++) batch.set(this.#chunkKey(hash, i), data.slice(i * ATTACHMENT_CHUNK_BYTES, (i + 1) * ATTACHMENT_CHUNK_BYTES));
    const meta: BlobMeta = { size: data.length, have: Array.from({ length: n }, (_, i) => i), pinned: true, used: Date.now() };
    batch.set(this.#metaKey(hash), utf8(JSON.stringify(meta)));
    await this.#store.apply(batch);
    return hash;
  }

  /** The host holds the blob now: it becomes a cache entry like any fetched one. */
  async unpin(hash: string): Promise<void> {
    const m = await this.meta(hash);
    if (m?.pinned) {
      await this.#store.apply(new Map([[this.#metaKey(hash), utf8(JSON.stringify({ ...m, pinned: false, used: Date.now() }))]]));
      await this.trim();
    }
  }

  async chunk(hash: string, i: number): Promise<Uint8Array | undefined> {
    return this.#store.get(this.#chunkKey(hash, i));
  }

  /** The whole blob, when every chunk is held. */
  async read(hash: string): Promise<Uint8Array | undefined> {
    const m = await this.meta(hash);
    if (!m || m.have.length !== chunkCount(m.size)) return undefined;
    const parts: Uint8Array[] = [];
    for (let i = 0; i < chunkCount(m.size); i++) {
      const c = await this.chunk(hash, i);
      if (!c) return undefined; // evicted under the read: the caller fetches it again
      parts.push(c);
    }
    const now = Date.now();
    if (!m.pinned && now - (m.used ?? 0) > TOUCH_EVERY_MS) {
      await this.#store.apply(new Map([[this.#metaKey(hash), utf8(JSON.stringify({ ...m, used: now }))]]));
    }
    return concat(parts);
  }

  async #metas(): Promise<[string, BlobMeta][]> {
    const base = "bm/";
    return (await this.#store.scan(base, prefixEnd(base))).map(([k, v]) => [k.slice(base.length), JSON.parse(fromUtf8(v)) as BlobMeta]);
  }

  /** What the cache holds. */
  async stats(): Promise<BlobCacheStats> {
    const out: BlobCacheStats = { bytes: 0, blobs: 0, pinnedBytes: 0, limit: this.#limit };
    for (const [, m] of await this.#metas()) {
      const held = heldBytes(m);
      out.bytes += held;
      out.blobs++;
      if (m.pinned) out.pinnedBytes += held;
    }
    return out;
  }

  /**
   * Evicts unpinned blobs, least recently used first, until the unpinned bytes are at most target
   * (default: the cache's bound; with no bound, nothing). Returns the bytes freed.
   */
  evict(target?: number): Promise<number> {
    return this.#serialized(() => this.#evict(target ?? (this.#limit > 0 ? this.#limit : Infinity), undefined));
  }

  /** Trims to the bound, keeping keep - the blob just fetched, which its reader is about to use. */
  trim(keep?: string): Promise<number> {
    if (this.#limit === 0) return Promise.resolve(0);
    return this.#serialized(() => this.#evict(this.#limit, keep));
  }

  #serialized(fn: () => Promise<number>): Promise<number> {
    const run = this.#evicting.then(fn);
    this.#evicting = run.catch(() => undefined);
    return run;
  }

  async #evict(target: number, keep: string | undefined): Promise<number> {
    const unpinnedMetas = (await this.#metas()).filter(([, m]) => !m.pinned);
    let unpinned = unpinnedMetas.reduce((n, [, m]) => n + heldBytes(m), 0);
    if (unpinned <= target) return 0;
    const candidates = unpinnedMetas.filter(([h]) => h !== keep).sort(([, a], [, b]) => (a.used ?? 0) - (b.used ?? 0));
    let freed = 0;
    for (const [hash, m] of candidates) {
      if (unpinned <= target) break;
      const batch: Batch = new Map([[this.#metaKey(hash), null]]);
      for (const i of m.have) batch.set(this.#chunkKey(hash, i), null);
      await this.#store.apply(batch);
      const n = heldBytes(m);
      unpinned -= n;
      freed += n;
    }
    return freed;
  }

  /**
   * Fetches what is missing of the blob `hash` - attachment (docId, attachmentId) as of commitHex -
   * from the host, verifying each chunk, and returns the whole blob.
   */
  async fetch(conn: PeerConnection, namespace: string, entry: Omit<BlobFetchEntry, "offset" | "maxBytes">, size: number): Promise<Uint8Array> {
    const hash = entry.hash;
    const n = chunkCount(size);
    let meta = (await this.meta(hash)) ?? { size, have: [], pinned: false };
    const have = new Set(meta.have);
    for (let guard = 0; have.size < n; guard++) {
      if (guard > n + 16) throw new BlobUnavailableError(`kdb replica: blob ${hash} did not arrive`);
      let first = 0;
      while (have.has(first)) first++;
      const res = await conn.blobFetch(namespace, [{ ...entry, offset: first * ATTACHMENT_CHUNK_BYTES, maxBytes: 0 }]);
      if (res.error) throw new BlobUnavailableError(`kdb replica: blob fetch refused: ${res.errorCode}: ${res.error}`);
      const part = res.parts[0];
      if (!part) {
        if (res.more) continue;
        throw new BlobUnavailableError(`kdb replica: the host sent no part of blob ${hash}`);
      }
      if (part.error) throw new BlobUnavailableError(`kdb replica: blob ${hash}: ${part.errorCode}: ${part.error}`);
      // The part's tree and size must be the blob's identity; every chunk must prove into the tree.
      const tree = fromHex(part.tree);
      if (part.hash !== hash || part.size !== size || blobIdentity(tree, part.size) !== hash) {
        throw new BlobUnavailableError(`kdb replica: blob ${hash}: the host's tree and size do not make its hash`);
      }
      const batch: Batch = new Map();
      let off = 0;
      for (const proof of part.chunks) {
        const len = Math.min(ATTACHMENT_CHUNK_BYTES, size - proof.leaf * ATTACHMENT_CHUNK_BYTES);
        const data = part.data.subarray(off, off + Math.max(0, len));
        off += data.length;
        if (!verifyInclusion(chunkLeaf(data), proof.leaf, n, proof.path, tree)) {
          throw new BlobUnavailableError(`kdb replica: blob ${hash}: chunk ${proof.leaf} fails its proof`);
        }
        batch.set(this.#chunkKey(hash, proof.leaf), data.slice());
        have.add(proof.leaf);
      }
      if (part.chunks.length === 0) throw new BlobUnavailableError(`kdb replica: blob ${hash}: an empty part`);
      meta = { ...meta, size, have: [...have].sort((a, b) => a - b), used: Date.now() };
      batch.set(this.#metaKey(hash), utf8(JSON.stringify(meta)));
      await this.#store.apply(batch);
    }
    const out = await this.read(hash);
    if (!out) throw new BlobUnavailableError(`kdb replica: blob ${hash} is incomplete`);
    await this.trim(hash);
    return out;
  }

  /**
   * Uploads a pinned blob on a sync session, before the commit or write-back that references it:
   * the first chunk names the blob, so a host that already holds it says so at once.
   */
  async upload(conn: PeerConnection, namespace: string, hash: string): Promise<void> {
    const m = await this.meta(hash);
    if (!m) throw new BlobUnavailableError(`kdb replica: blob ${hash} to upload is not held here`);
    const n = chunkCount(m.size);
    let uploadId = "";
    let offset = 0;
    for (let i = 0; ; ) {
      const data = (await this.chunk(hash, i)) ?? new Uint8Array();
      const final = i === n - 1;
      const r = await conn.upload({ namespace, uploadId, offset, data, final, expect: uploadId ? "" : hash });
      if (r.error) throw new BlobUnavailableError(`kdb replica: upload of ${hash} refused: ${r.errorCode}: ${r.error}`);
      if (r.hash) {
        if (r.hash !== hash || r.size !== m.size) throw new BlobUnavailableError(`kdb replica: the host stored ${r.hash}, not ${hash}`);
        return;
      }
      if (final) throw new BlobUnavailableError(`kdb replica: the host did not confirm upload of ${hash}`);
      uploadId = r.uploadId;
      offset = r.received;
      i = Math.floor(offset / ATTACHMENT_CHUNK_BYTES);
      if (offset % ATTACHMENT_CHUNK_BYTES !== 0) throw new BlobUnavailableError("kdb replica: the host received part of a chunk");
    }
  }
}
