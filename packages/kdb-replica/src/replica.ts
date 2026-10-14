/**
 * openReplica: namespaces held locally and kept in sync with a KDB node
 * (docs/kdb-client-replica-plan.md §3.2).
 *
 * Reads and writes are local and never wait for the network. Sync runs in rounds - connect, hello,
 * each namespace pushed then pulled, disconnect - on a poll interval (plan D5: polling until
 * REFS_WATCH exists), right after a local write, and on demand. A failed round backs off and
 * retries; nothing local is lost by a failure, because local writes are durable commits before
 * they are acknowledged.
 */

import type { Transport } from "@kdb/client";
import { fromUtf8, randomUuid, utf8 } from "./bytes.ts";
import type { Op } from "./commit.ts";
import { ReplicaNamespace } from "./namespace.ts";
import { PeerConnection, type ConflictReport } from "./peer.ts";
import { CachedStore, type KvStore } from "./store.ts";
import { ProjectionNamespace } from "./projection.ts";
import { syncNamespace, syncProjection, type CatchUp, type NamespaceSyncResult, type ProjectionSyncResult } from "./sync.ts";

export interface ReplicaOptions {
  store: KvStore;
  peer: {
    url: string;
    /** The bearer token, or a function asked on every connect (so a refreshed token is used). */
    token?: string | (() => string | Promise<string>);
    /** Opens the transport; WebSocket by default. Tests and Node TCP callers pass their own. */
    connect?: (url: string) => Promise<Transport>;
  };
  /**
   * The namespaces held. "full" (the default) holds the whole namespace with real commits;
   * "filtered" holds what matches filter and the token may read, and is read-only unless writeBack.
   */
  namespaces: ({ ns: string; mode?: "full" } | { ns: string; mode: "filtered"; filter: string; writeBack?: boolean })[];
  catchUp?: CatchUp;
  /** Bytes of trie nodes and document bodies kept in memory; the rest is read from the store. */
  memory?: { cacheBytes?: number };
  /**
   * Milliseconds between sync rounds; 0 syncs only on writes, changes and syncNow(). Default 5000,
   * or 60000 while the host is reporting changes (live).
   */
  pollMs?: number;
  /**
   * Keep the connection open between rounds and ask the host to report changes (REFS_WATCH), so
   * remote writes arrive without waiting for a poll. Default true; a host without the watch
   * capability is polled.
   */
  live?: boolean;
  /** Page size asked of the host, in bytes. */
  pageBytes?: number;
  /** Start syncing at once (default true). */
  autoSync?: boolean;
  /**
   * In a browser, ask for persistent storage (navigator.storage.persist) so the browser does not
   * evict the replica - and with it any writes not yet synced - under storage pressure. Default
   * true; status().storagePersisted reports the answer.
   */
  persistStorage?: boolean;
}

export interface ChangeEvent {
  namespace: string;
  ids: string[];
  source: "local" | "remote";
}

export interface ConflictEvent {
  namespace: string;
  report: ConflictReport | undefined;
  /** Full mode: the local commits the host refused, still held. Filtered: none (the write was undone). */
  pending: readonly string[];
  /** Filtered mode: the source's decision ("conflict" or "refused") and why. */
  outcome?: string;
  reason?: string;
}

export interface ReplicaStatus {
  connected: boolean;
  lastSyncAt: number | null;
  lastError: string | null;
  namespaces: Record<string, { mode: "full" | "filtered"; head: string | null; pending: number; converging: boolean; lastSource?: string }>;
  /** Bytes the read cache holds now; never more than memory.cacheBytes. */
  residentBytes: number;
  /**
   * Whether the browser granted persistent storage: false means it may evict the replica, losing
   * unsynced writes; null outside a browser or when not asked.
   */
  storagePersisted: boolean | null;
}

type Listener<T> = (e: T) => void;

interface Events {
  change: ChangeEvent;
  conflict: ConflictEvent;
  sync: NamespaceSyncResult[];
}

export class Replica {
  readonly #opts: ReplicaOptions;
  readonly #store: CachedStore;
  readonly #ns = new Map<string, ReplicaNamespace>();
  readonly #proj = new Map<string, ProjectionNamespace>();
  readonly #outboxSizes = new Map<string, number>();
  readonly #listeners: { [K in keyof Events]: Set<Listener<Events[K]>> } = { change: new Set(), conflict: new Set(), sync: new Set() };
  #timer: ReturnType<typeof setTimeout> | null = null;
  #running: Promise<NamespaceSyncResult[]> | null = null;
  #again = false;
  #closed = false;
  #failures = 0;
  #lastProjections: ProjectionSyncResult[] = [];
  /** What the last round did to the filtered namespaces. */
  get lastProjectionResults(): readonly ProjectionSyncResult[] {
    return this.#lastProjections;
  }
  #status = { connected: false, lastSyncAt: null as number | null, lastError: null as string | null };

  private constructor(opts: ReplicaOptions, store: CachedStore) {
    this.#opts = opts;
    this.#store = store;
  }

  static async open(opts: ReplicaOptions): Promise<Replica> {
    const store = new CachedStore(opts.store, opts.memory?.cacheBytes ?? 32 << 20);
    const r = new Replica(opts, store);
    let nodeId = await store.get("m/node");
    if (!nodeId) {
      nodeId = utf8(randomUuid());
      await store.apply(new Map([["m/node", nodeId]]));
    }
    for (const spec of opts.namespaces) {
      if (r.#ns.has(spec.ns) || r.#proj.has(spec.ns)) throw new Error(`kdb replica: ${spec.ns} is listed twice`);
      if (spec.mode === "filtered") {
        const p = await ProjectionNamespace.open(store, spec.ns, spec.filter, spec.writeBack ?? false);
        r.#proj.set(spec.ns, p);
        r.#outboxSizes.set(spec.ns, await p.outboxSize());
      } else {
        r.#ns.set(spec.ns, await ReplicaNamespace.open(store, spec.ns, fromUtf8(nodeId)));
      }
    }
    r.#nodeId = fromUtf8(nodeId);
    const storage = (globalThis as { navigator?: { storage?: { persist?: () => Promise<boolean> } } }).navigator?.storage;
    if (opts.persistStorage !== false && typeof storage?.persist === "function") {
      r.#persisted = await storage.persist().catch(() => false);
    }
    if (opts.autoSync !== false) r.#schedule(0);
    return r;
  }

  #nodeId = "";
  #persisted: boolean | null = null;

  /** A full-mode namespace. */
  namespace(name: string): ReplicaNamespace {
    const ns = this.#ns.get(name);
    if (!ns) throw new Error(`kdb replica: ${name} is not one of this replica's full namespaces`);
    return ns;
  }

  /** A filtered namespace. */
  projection(name: string): ProjectionNamespace {
    const p = this.#proj.get(name);
    if (!p) throw new Error(`kdb replica: ${name} is not one of this replica's filtered namespaces`);
    return p;
  }

  #local(name: string): ReplicaNamespace | ProjectionNamespace {
    const n = this.#ns.get(name) ?? this.#proj.get(name);
    if (!n) throw new Error(`kdb replica: ${name} is not one of this replica's namespaces`);
    return n;
  }

  // --- local reads and writes --------------------------------------------------------------------

  get(ns: string, id: string): Promise<string | undefined> {
    return this.#local(ns).get(id);
  }

  scan(ns: string, opts?: { after?: string; before?: string; limit?: number }) {
    return this.#local(ns).scan(opts);
  }

  /**
   * Writes documents locally, durable before it resolves, and schedules a sync. Full mode: one
   * local commit, pushed as is. Filtered with write-back: one write-back transaction. Returns the
   * commit hash or the write-back transaction id.
   */
  async write(ns: string, ops: Op[], message?: string): Promise<string> {
    const p = this.#proj.get(ns);
    if (p) {
      const docs = ops.map((o) => {
        if (o.kind === "write") return { docId: o.docId, json: o.json };
        if (o.kind === "delete") return { docId: o.docId, json: null };
        throw new Error(`kdb replica: a filtered namespace takes document writes and deletes only, not ${o.kind}`);
      });
      const entry = await p.write(docs, randomUuid());
      this.#outboxSizes.set(ns, (this.#outboxSizes.get(ns) ?? 0) + 1);
      this.#emit("change", { namespace: ns, ids: docs.map((d) => d.docId), source: "local" });
      this.#schedule(50);
      return entry.txId;
    }
    const c = await this.namespace(ns).commitLocal(ops, { transactionId: randomUuid(), message });
    this.#emit("change", { namespace: ns, ids: ops.flatMap((o) => (o.kind === "write" || o.kind === "delete" ? [o.docId] : [])), source: "local" });
    this.#schedule(50);
    return c.hash;
  }

  put(ns: string, id: string, json: string | object): Promise<string> {
    return this.write(ns, [{ kind: "write", docId: id, json: typeof json === "string" ? json : JSON.stringify(json) }]);
  }

  delete(ns: string, id: string): Promise<string> {
    return this.write(ns, [{ kind: "delete", docId: id }]);
  }

  // --- events and status -------------------------------------------------------------------------

  /** Subscribes to an event; returns the unsubscribe function. */
  on<K extends keyof Events>(event: K, fn: Listener<Events[K]>): () => void {
    const set = this.#listeners[event] as Set<Listener<Events[K]>>;
    set.add(fn);
    return () => set.delete(fn);
  }

  #emit<K extends keyof Events>(event: K, e: Events[K]): void {
    for (const fn of this.#listeners[event] as Set<Listener<Events[K]>>) {
      try {
        fn(e);
      } catch {
        // A listener's failure is the listener's.
      }
    }
  }

  status(): ReplicaStatus {
    const namespaces: ReplicaStatus["namespaces"] = {};
    for (const [name, ns] of this.#ns) {
      namespaces[name] = { mode: "full", head: ns.head, pending: ns.pending.length, converging: ns.target !== undefined };
    }
    for (const [name, p] of this.#proj) {
      namespaces[name] = {
        mode: "filtered", head: null, pending: this.#outboxSizes.get(name) ?? 0,
        converging: p.transfer !== undefined, lastSource: p.lastSource,
      };
    }
    return { ...this.#status, namespaces, residentBytes: this.#store.residentBytes, storagePersisted: this.#persisted };
  }

  // --- sync --------------------------------------------------------------------------------------

  /** Runs a sync round now (or joins the one running) and resolves with what it did. */
  syncNow(): Promise<NamespaceSyncResult[]> {
    if (this.#running) {
      this.#again = true;
      return this.#running;
    }
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = null;
    this.#running = this.#round().finally(() => {
      this.#running = null;
      if (this.#closed) return;
      if (this.#again) {
        this.#again = false;
        this.#schedule(0);
      } else {
        const poll = this.#opts.pollMs ?? (this.#watching ? 60_000 : 5000);
        const backoff = this.#failures > 0 ? Math.min(60_000, 500 * 2 ** Math.min(this.#failures, 7)) : 0;
        if (backoff > 0) this.#schedule(backoff);
        else if (poll > 0) this.#schedule(poll);
      }
    });
    return this.#running;
  }

  #schedule(ms: number): void {
    if (this.#closed || this.#opts.autoSync === false) return;
    if (this.#running) {
      this.#again = true;
      return;
    }
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = setTimeout(() => {
      this.#timer = null;
      this.syncNow().catch(() => undefined);
    }, ms);
  }

  /** The open connection, reused across rounds while live; null when there is none. */
  #conn: PeerConnection | null = null;
  #watching = false;

  async #connect(): Promise<{ conn: PeerConnection; fresh: boolean }> {
    if (this.#conn && !this.#conn.closed) return { conn: this.#conn, fresh: false };
    const token = typeof this.#opts.peer.token === "function" ? await this.#opts.peer.token() : this.#opts.peer.token;
    // Filtered namespaces' sources are named too: where the token may also sync one, the host
    // grants it, and the replica can watch it.
    const names = [...new Set([...this.#ns.keys(), ...this.#proj.keys()])];
    const conn = await PeerConnection.open(this.#opts.peer.url, {
      nodeId: this.#nodeId, token, namespaces: names, connect: this.#opts.peer.connect,
    });
    conn.onClose(() => {
      if (this.#conn === conn) {
        this.#conn = null;
        this.#watching = false;
        this.#status.connected = false;
        this.#schedule(this.#closed ? 0 : 1000);
      }
    });
    return { conn, fresh: true };
  }

  get #live(): boolean {
    return this.#opts.live !== false && this.#opts.autoSync !== false;
  }

  async #round(): Promise<NamespaceSyncResult[]> {
    let conn: PeerConnection | null = null;
    let keep = false;
    try {
      const c = await this.#connect();
      conn = c.conn;
      // A reused connection's hello refs are stale: ask again.
      if (!c.fresh) conn.refs = await conn.refs_(conn.refs.map((r) => r.namespace));
      this.#status.connected = true;
      const results: NamespaceSyncResult[] = [];
      const projections: ProjectionSyncResult[] = [];
      const errors: string[] = [];
      for (const [name, ns] of this.#ns) {
        try {
          const r = await syncNamespace(conn, ns, conn.refs.find((x) => x.namespace === name), {
            catchUp: this.#opts.catchUp ?? "auto",
            pageBytes: this.#opts.pageBytes,
          });
          results.push(r);
          if (r.changed.length > 0) this.#emit("change", { namespace: name, ids: r.changed, source: "remote" });
          if (r.pushOutcome === "conflict") this.#emit("conflict", { namespace: name, report: r.conflict, pending: ns.pending });
        } catch (e) {
          errors.push(`${name}: ${e instanceof Error ? e.message : String(e)}`);
        }
      }
      for (const [name, p] of this.#proj) {
        try {
          const r = await syncProjection(conn, p, { catchUp: "auto", pageBytes: this.#opts.pageBytes });
          projections.push(r);
          this.#outboxSizes.set(name, await p.outboxSize());
          if (r.changed.length > 0) this.#emit("change", { namespace: name, ids: [...new Set(r.changed)], source: "remote" });
          for (const rej of r.rejected) {
            this.#emit("conflict", {
              namespace: name, pending: [], outcome: rej.outcome, reason: rej.reason,
              report: {
                transactionId: rej.txId, baseHash: "", targetHash: "",
                conflicts: rej.docs.map((d) => {
                  const cur = rej.current.find((c) => c.docId === d.docId);
                  return {
                    documentId: d.docId, operationType: "CONCURRENT_WRITE",
                    ...(d.deleted ? {} : { localDoc: d.body }),
                    ...(cur && !cur.absent ? { incomingDoc: cur.body } : {}),
                  };
                }),
              },
            });
          }
          if (r.deferred) errors.push(`${name}: write-back deferred: ${r.deferred}`);
        } catch (e) {
          errors.push(`${name}: ${e instanceof Error ? e.message : String(e)}`);
        }
      }
      this.#status.lastSyncAt = Date.now();
      this.#status.lastError = errors.length > 0 ? errors.join("; ") : null;
      this.#failures = errors.length > 0 ? this.#failures + 1 : 0;
      this.#lastProjections = projections;
      if (this.#live && conn.has("watch")) {
        keep = true;
        this.#conn = conn;
        if (!this.#watching && conn.refs.length > 0) {
          this.#watching = true;
          await conn.watch(conn.refs.map((r) => r.namespace), () => this.#schedule(0));
        }
      }
      this.#emit("sync", results);
      if (errors.length > 0) throw new Error(`kdb replica: sync failed for ${errors.join("; ")}`);
      return results;
    } catch (e) {
      this.#status.lastError = e instanceof Error ? e.message : String(e);
      if (!this.#status.lastError.startsWith("kdb replica: sync failed")) this.#failures++;
      throw e;
    } finally {
      if (!keep) {
        if (this.#conn === conn) this.#conn = null;
        this.#watching = false;
        this.#status.connected = false;
        await conn?.close();
      }
    }
  }

  async close(): Promise<void> {
    this.#closed = true;
    if (this.#timer) clearTimeout(this.#timer);
    await this.#running?.catch(() => undefined);
    const conn = this.#conn;
    this.#conn = null;
    await conn?.close();
    await this.#store.close();
  }
}

export function openReplica(opts: ReplicaOptions): Promise<Replica> {
  return Replica.open(opts);
}
