/**
 * A filtered namespace (docs/kdb-client-replica-plan.md §4.2): the documents of a source namespace
 * that match a KDB-SQL filter and that the principal may read, kept by PROJECT_FETCH. With write
 * back, local writes go to the source by PROJECT_WRITE.
 *
 * Unlike a full namespace it holds no commits: the host decides what the replica may see, so the
 * replica cannot verify source commits it only partly receives (plan D3). It keeps the documents,
 * the last complete source commit, any transfer in progress, and an outbox.
 *
 * - Pages apply atomically with the transfer's cursor, so an interrupted transfer resumes. Every
 *   page carries final states.
 * - A reset (the host could not relate the replica's position to its head) replaces the projection.
 *   Each page covers the ids from the cursor to its `next`, and local documents in that range that
 *   the page does not deliver are removed.
 * - Local writes apply at once (optimistic) and queue in the outbox, oldest first. Each entry has
 *   a transaction id fixed when it was written, so a resend after a lost reply is recognised at the
 *   source rather than applied twice. Its base hash is the document's content hash before the
 *   write, as the source will hold it once the entries ahead of it are applied.
 * - A document with a write still in the outbox keeps its local state against incoming pages,
 *   until the source decides. A conflicting or refused write puts the source's state back.
 * - A write may change a document's attachments: the entry then carries the document's whole set
 *   after it, and the hash of the set it replaced (the source applies it only if it still holds
 *   that set). An attachment-only write keeps the body (keepBody) and is still guarded by its
 *   base hash. The blobs are uploaded before the entry is sent (sync.ts).
 */

import { fromUtf8, toHex, utf8 } from "./bytes.ts";
import { contentHash } from "./commit.ts";
import { sha256 } from "./sha256.ts";
import { decodeAttachmentSet, encodeAttachmentSet, setHash, sortAttachments, type Attachment } from "./attachments.ts";
import { prefixEnd, type Batch, type KvStore } from "./store.ts";

/** The local name Go gives a projection (peersync.ProjectionNamespace). */
export function projectionNamespace(ns: string, filter: string): string {
  return `${ns}.projection-${toHex(sha256(utf8(filter.trim()))).slice(0, 8)}`;
}

/** An attachment as PROJECT_WRITE, PROJECT_PAGE and TREE_NODES list it. */
export interface WireAttachment {
  id: string;
  name: string;
  hash: string;
  size: number;
  mime?: string;
}

export interface WriteBackDoc {
  docId: string;
  body?: string;
  deleted?: boolean;
  baseHash?: string;
  /** The write left the body alone: only the attachments changed. */
  keepBody?: boolean;
  /** attachments is the document's whole set after the write; attachmentsBase the set it replaced. */
  attachmentsChanged?: boolean;
  attachments?: WireAttachment[];
  attachmentsBase?: string;
}

/** One document of a local write: json null deletes it, undefined keeps its body; attachments, when
 * given, is its whole attachment set after the write. */
export interface LocalWrite {
  docId: string;
  json?: string | null;
  attachments?: Attachment[];
}

export function toWireAttachments(set: readonly Attachment[]): WireAttachment[] {
  return set.map((a) => ({ id: a.id, name: a.name, hash: a.contentHash, size: Number(a.size), mime: a.mime }));
}

export function fromWireAttachments(items: readonly WireAttachment[] | undefined): Attachment[] {
  return sortAttachments((items ?? []).map((a) => ({
    id: a.id.toLowerCase(), name: a.name, scheme: 1, contentHash: a.hash.toLowerCase(), size: BigInt(a.size), mime: a.mime ?? "",
  })));
}

/** The hash a write-back names a document's attachment set by: "" for none. */
function baseSetHash(set: readonly Attachment[]): string {
  return set.length === 0 ? "" : setHash(sortAttachments([...set]));
}

export interface OutboxEntry {
  seq: number;
  txId: string;
  docs: WriteBackDoc[];
}

export interface ProjectionMeta {
  source: string;
  filter: string;
  writeBack: boolean;
  /** The source commit the last complete transfer reached; "" before the first. */
  lastSource: string;
  /** A transfer in progress: resumed with the same from, at and cursor. */
  transfer?: { from: string; at: string; after: string } | undefined;
  nextSeq: number;
}

export interface ProjectPage {
  atHex: string;
  reset: boolean;
  writes: { id: string; body: string }[];
  deletes: string[];
  next: string;
  done: boolean;
  /** Every written document's whole attachment set at atHex; a written document absent has none. */
  attachments?: Record<string, WireAttachment[]>;
}

export class ReadOnlyProjectionError extends Error {
  override name = "ReadOnlyProjectionError";
}

class Lock {
  #tail: Promise<unknown> = Promise.resolve();
  run<T>(fn: () => Promise<T>): Promise<T> {
    const next = this.#tail.then(fn, fn);
    this.#tail = next.catch(() => undefined);
    return next;
  }
}

export class ProjectionNamespace {
  readonly name: string;
  readonly key: string;
  readonly #store: KvStore;
  readonly #lock = new Lock();
  #meta: ProjectionMeta;

  private constructor(store: KvStore, meta: ProjectionMeta) {
    this.#store = store;
    this.#meta = meta;
    this.name = meta.source;
    this.key = encodeURIComponent(projectionNamespace(meta.source, meta.filter));
  }

  static metaKey(source: string, filter: string): string {
    return `m/proj/${encodeURIComponent(projectionNamespace(source, filter))}`;
  }

  static async open(store: KvStore, source: string, filter: string, writeBack: boolean): Promise<ProjectionNamespace> {
    const key = ProjectionNamespace.metaKey(source, filter);
    const raw = await store.get(key);
    let meta: ProjectionMeta;
    if (raw) {
      meta = { ...(JSON.parse(fromUtf8(raw)) as ProjectionMeta), writeBack };
    } else {
      meta = { source, filter: filter.trim(), writeBack, lastSource: "", nextSeq: 1 };
      await store.apply(new Map([[key, utf8(JSON.stringify(meta))]]));
    }
    return new ProjectionNamespace(store, meta);
  }

  get filter(): string {
    return this.#meta.filter;
  }

  get writeBack(): boolean {
    return this.#meta.writeBack;
  }

  get lastSource(): string {
    return this.#meta.lastSource;
  }

  get transfer(): ProjectionMeta["transfer"] {
    return this.#meta.transfer;
  }

  #docKey(id: string): string {
    return `d/${this.key}/${id.toLowerCase()}`;
  }

  #attachKey(id: string): string {
    return `a/${this.key}/${id.toLowerCase()}`;
  }

  /** The document's attachments as of the last transfer (refs; bytes are fetched on read). */
  async attachments(id: string): Promise<Attachment[]> {
    const b = await this.#store.get(this.#attachKey(id));
    return b ? decodeAttachmentSet(b) : [];
  }

  #pendingKey(id: string): string {
    return `p/${this.key}/${id.toLowerCase()}`;
  }

  #outboxKey(seq: number): string {
    return `o/${this.key}/${seq.toString().padStart(16, "0")}`;
  }

  async #commit(batch: Batch, meta: ProjectionMeta): Promise<void> {
    batch.set(ProjectionNamespace.metaKey(this.#meta.source, this.#meta.filter), utf8(JSON.stringify(meta)));
    await this.#store.apply(batch);
    this.#meta = meta;
  }

  async get(id: string): Promise<string | undefined> {
    const b = await this.#store.get(this.#docKey(id));
    return b ? fromUtf8(b) : undefined;
  }

  async scan(opts: { after?: string; before?: string; limit?: number } = {}): Promise<{ id: string; json: string }[]> {
    const base = `d/${this.key}/`;
    const start = opts.after ? base + opts.after.toLowerCase() + "\u0000" : base;
    const end = opts.before ? base + opts.before.toLowerCase() : prefixEnd(base);
    const rows = await this.#store.scan(start, end, opts.limit);
    return rows.map(([k, v]) => ({ id: k.slice(base.length), json: fromUtf8(v) }));
  }

  async #pendingCount(id: string): Promise<number> {
    const b = await this.#store.get(this.#pendingKey(id));
    return b ? Number(fromUtf8(b)) : 0;
  }

  /** Writes locally and queues the write for the source, as one transaction. */
  write(docs: LocalWrite[], txId: string): Promise<OutboxEntry> {
    return this.#lock.run(async () => {
      if (!this.#meta.writeBack) {
        throw new ReadOnlyProjectionError(`kdb replica: ${this.name} is filtered without write-back, so it is read-only here`);
      }
      const batch: Batch = new Map();
      const entry: OutboxEntry = { seq: this.#meta.nextSeq, txId, docs: [] };
      const counts = new Map<string, number>();
      // A document written twice in one call: the second write's base is the first's result.
      const staged = new Map<string, string | null>();
      const stagedSets = new Map<string, Attachment[]>();
      for (const d of docs) {
        const id = d.docId.toLowerCase();
        const before = staged.has(id) ? (staged.get(id) ?? undefined) : await this.get(id);
        const json = d.json === undefined ? (before ?? null) : d.json;
        staged.set(id, json);
        const wb: WriteBackDoc = { docId: id };
        if (before !== undefined) wb.baseHash = contentHash(id, before);
        if (d.json === undefined) {
          if (before === undefined) throw new Error(`kdb replica: ${this.name}/${id} is not here to attach to`);
          wb.keepBody = true;
        } else if (d.json === null) {
          wb.deleted = true;
          batch.set(this.#docKey(id), null);
          batch.set(this.#attachKey(id), null); // a deleted document keeps no attachments
          stagedSets.set(id, []);
        } else {
          wb.body = d.json;
          batch.set(this.#docKey(id), utf8(d.json));
        }
        if (d.attachments !== undefined && d.json !== null) {
          const prev = stagedSets.get(id) ?? (await this.attachments(id));
          const next = sortAttachments([...d.attachments]);
          wb.attachmentsChanged = true;
          wb.attachments = toWireAttachments(next);
          wb.attachmentsBase = baseSetHash(prev);
          stagedSets.set(id, next);
          batch.set(this.#attachKey(id), next.length > 0 ? encodeAttachmentSet(next) : null);
        }
        entry.docs.push(wb);
        counts.set(id, (counts.get(id) ?? (await this.#pendingCount(id))) + 1);
      }
      for (const [id, n] of counts) batch.set(this.#pendingKey(id), utf8(String(n)));
      batch.set(this.#outboxKey(entry.seq), utf8(JSON.stringify(entry)));
      await this.#commit(batch, { ...this.#meta, nextSeq: entry.seq + 1 });
      return entry;
    });
  }

  /** The outbox, oldest first. */
  async outbox(): Promise<OutboxEntry[]> {
    const base = `o/${this.key}/`;
    return (await this.#store.scan(base, prefixEnd(base))).map(([, v]) => JSON.parse(fromUtf8(v)) as OutboxEntry);
  }

  async outboxSize(): Promise<number> {
    return (await this.outbox()).length;
  }

  /**
   * Records the source's decision on entry. Applied: the entry leaves the outbox. Not applied:
   * its documents go back to the source's state (current), and the entry leaves the outbox.
   */
  resolve(entry: OutboxEntry, current: { docId: string; body?: string; absent?: boolean; attachments?: WireAttachment[] }[] | null): Promise<void> {
    return this.#lock.run(async () => {
      const batch: Batch = new Map([[this.#outboxKey(entry.seq), null]]);
      for (const d of entry.docs) {
        const n = (await this.#pendingCount(d.docId)) - 1;
        batch.set(this.#pendingKey(d.docId), n > 0 ? utf8(String(n)) : null);
      }
      for (const c of current ?? []) {
        const gone = c.absent || c.body === undefined;
        batch.set(this.#docKey(c.docId), gone ? null : utf8(c.body!));
        // The source's attachments go back with its body (bytes fetched on read).
        const set = gone ? [] : fromWireAttachments(c.attachments);
        batch.set(this.#attachKey(c.docId), set.length > 0 ? encodeAttachmentSet(set) : null);
      }
      await this.#commit(batch, this.#meta);
    });
  }

  /** Starts (or returns) the transfer to resume. */
  async beginTransfer(): Promise<{ from: string; at: string; after: string }> {
    return this.#meta.transfer ?? { from: this.#meta.lastSource, at: "", after: "" };
  }

  /** Applies one page of a transfer from `from`, with its cursor, atomically. */
  applyPage(from: string, after: string, page: ProjectPage): Promise<string[]> {
    return this.#lock.run(async () => {
      const batch: Batch = new Map();
      const changed: string[] = [];
      const delivered = new Set<string>();
      const listed = new Map(Object.entries(page.attachments ?? {}).map(([k, v]) => [k.toLowerCase(), v]));
      for (const w of page.writes) {
        const id = w.id.toLowerCase();
        delivered.add(id);
        if ((await this.#pendingCount(id)) > 0) continue;
        if ((await this.get(id)) !== w.body) changed.push(id);
        batch.set(this.#docKey(id), utf8(w.body));
        // Every written document comes with its whole attachment set; absent means none.
        const set = fromWireAttachments(listed.get(id));
        batch.set(this.#attachKey(id), set.length > 0 ? encodeAttachmentSet(set) : null);
      }
      for (const raw of page.deletes) {
        const id = raw.toLowerCase();
        if ((await this.#pendingCount(id)) > 0) continue;
        if ((await this.get(id)) !== undefined) changed.push(id);
        batch.set(this.#docKey(id), null);
        batch.set(this.#attachKey(id), null);
      }
      if (page.reset) {
        // The page stands for every id after the cursor up to its next (to the end when done):
        // what it does not deliver there is no longer in the projection.
        const base = `d/${this.key}/`;
        const start = after ? base + after + "\u0000" : base;
        const end = page.done ? prefixEnd(base) : base + page.next + "\u0000";
        for (const [k] of await this.#store.scan(start, end)) {
          const id = k.slice(base.length);
          if (delivered.has(id) || (await this.#pendingCount(id)) > 0) continue;
          batch.set(k, null);
          batch.set(this.#attachKey(id), null);
          changed.push(id);
        }
      }
      const meta: ProjectionMeta = page.done
        ? { ...this.#meta, lastSource: page.atHex, transfer: undefined }
        : { ...this.#meta, transfer: { from, at: page.atHex, after: page.next } };
      await this.#commit(batch, meta);
      return changed;
    });
  }
}
