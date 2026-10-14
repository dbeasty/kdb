/**
 * Attachment bytes on the replica (docs/kdb-client-replica-plan.md §9): fetched lazily, on first
 * read, by BLOB_FETCH - every chunk checked against the blob's content hash with its RFC 6962 proof
 * - and kept by content hash in 1 MiB chunks, so a blob shared by several attachments is stored
 * once and a read can resume. A blob the replica wrote is pinned until the host holds it.
 */

import { ATTACHMENT_CHUNK_BYTES, attachmentContentHash } from "./attachments.ts";
import { blobIdentity, chunkLeaf, verifyInclusion, type BlobFetchEntry } from "./binwire.ts";
import { concat, fromHex, fromUtf8, utf8 } from "./bytes.ts";
import type { PeerConnection } from "./peer.ts";
import type { Batch, KvStore } from "./store.ts";

export class BlobUnavailableError extends Error {
  override name = "BlobUnavailableError";
}

interface BlobMeta {
  size: number;
  /** Chunks held, by index. */
  have: number[];
  /** Written here and not yet confirmed by the host. */
  pinned: boolean;
}

function chunkCount(size: number): number {
  return Math.max(1, Math.ceil(size / ATTACHMENT_CHUNK_BYTES));
}

export class BlobStore {
  readonly #store: KvStore;

  constructor(store: KvStore) {
    this.#store = store;
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
    const meta: BlobMeta = { size: data.length, have: Array.from({ length: n }, (_, i) => i), pinned: true };
    batch.set(this.#metaKey(hash), utf8(JSON.stringify(meta)));
    await this.#store.apply(batch);
    return hash;
  }

  async unpin(hash: string): Promise<void> {
    const m = await this.meta(hash);
    if (m?.pinned) await this.#store.apply(new Map([[this.#metaKey(hash), utf8(JSON.stringify({ ...m, pinned: false }))]]));
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
      if (!c) return undefined;
      parts.push(c);
    }
    return concat(parts);
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
      meta = { ...meta, size, have: [...have].sort((a, b) => a - b) };
      batch.set(this.#metaKey(hash), utf8(JSON.stringify(meta)));
      await this.#store.apply(batch);
    }
    const out = await this.read(hash);
    if (!out) throw new BlobUnavailableError(`kdb replica: blob ${hash} is incomplete`);
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
