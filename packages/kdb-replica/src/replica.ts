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
import { syncNamespace, type CatchUp, type NamespaceSyncResult } from "./sync.ts";

export interface ReplicaOptions {
  store: KvStore;
  peer: {
    url: string;
    /** The bearer token, or a function asked on every connect (so a refreshed token is used). */
    token?: string | (() => string | Promise<string>);
    /** Opens the transport; WebSocket by default. Tests and Node TCP callers pass their own. */
    connect?: (url: string) => Promise<Transport>;
  };
  /** Full-mode namespaces (filtered ones arrive with write-back, plan PR 3). */
  namespaces: { ns: string; mode?: "full" }[];
  catchUp?: CatchUp;
  /** Bytes of trie nodes and document bodies kept in memory; the rest is read from the store. */
  memory?: { cacheBytes?: number };
  /** Milliseconds between sync rounds; 0 syncs only on writes and syncNow(). Default 5000. */
  pollMs?: number;
  /** Page size asked of the host, in bytes. */
  pageBytes?: number;
  /** Start syncing at once (default true). */
  autoSync?: boolean;
}

export interface ChangeEvent {
  namespace: string;
  ids: string[];
  source: "local" | "remote";
}

export interface ConflictEvent {
  namespace: string;
  report: ConflictReport | undefined;
  /** The local commits the host refused, still held. */
  pending: readonly string[];
}

export interface ReplicaStatus {
  connected: boolean;
  lastSyncAt: number | null;
  lastError: string | null;
  namespaces: Record<string, { head: string | null; pending: number; converging: boolean }>;
  /** Bytes the read cache holds now; never more than memory.cacheBytes. */
  residentBytes: number;
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
  readonly #listeners: { [K in keyof Events]: Set<Listener<Events[K]>> } = { change: new Set(), conflict: new Set(), sync: new Set() };
  #timer: ReturnType<typeof setTimeout> | null = null;
  #running: Promise<NamespaceSyncResult[]> | null = null;
  #again = false;
  #closed = false;
  #failures = 0;
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
    for (const { ns } of opts.namespaces) {
      r.#ns.set(ns, await ReplicaNamespace.open(store, ns, fromUtf8(nodeId)));
    }
    if (opts.autoSync !== false) r.#schedule(0);
    return r;
  }

  namespace(name: string): ReplicaNamespace {
    const ns = this.#ns.get(name);
    if (!ns) throw new Error(`kdb replica: ${name} is not one of this replica's namespaces`);
    return ns;
  }

  // --- local reads and writes --------------------------------------------------------------------

  get(ns: string, id: string): Promise<string | undefined> {
    return this.namespace(ns).get(id);
  }

  scan(ns: string, opts?: { after?: string; before?: string; limit?: number }) {
    return this.namespace(ns).scan(opts);
  }

  /** Writes documents in one local commit, durable before it resolves, and schedules a push. */
  async write(ns: string, ops: Op[], message?: string): Promise<string> {
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
      namespaces[name] = { head: ns.head, pending: ns.pending.length, converging: ns.target !== undefined };
    }
    return { ...this.#status, namespaces, residentBytes: this.#store.residentBytes };
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
        const poll = this.#opts.pollMs ?? 5000;
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

  async #round(): Promise<NamespaceSyncResult[]> {
    const token = typeof this.#opts.peer.token === "function" ? await this.#opts.peer.token() : this.#opts.peer.token;
    const nodeId = [...this.#ns.values()][0]?.nodeId ?? randomUuid();
    let conn: PeerConnection | null = null;
    try {
      conn = await PeerConnection.open(this.#opts.peer.url, {
        nodeId, token, namespaces: [...this.#ns.keys()], connect: this.#opts.peer.connect,
      });
      this.#status.connected = true;
      const results: NamespaceSyncResult[] = [];
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
      this.#status.lastSyncAt = Date.now();
      this.#status.lastError = errors.length > 0 ? errors.join("; ") : null;
      this.#failures = errors.length > 0 ? this.#failures + 1 : 0;
      this.#emit("sync", results);
      if (errors.length > 0) throw new Error(`kdb replica: sync failed for ${errors.join("; ")}`);
      return results;
    } catch (e) {
      this.#status.lastError = e instanceof Error ? e.message : String(e);
      if (!this.#status.lastError.startsWith("kdb replica: sync failed")) this.#failures++;
      throw e;
    } finally {
      this.#status.connected = false;
      await conn?.close();
    }
  }

  async close(): Promise<void> {
    this.#closed = true;
    if (this.#timer) clearTimeout(this.#timer);
    await this.#running?.catch(() => undefined);
    await this.#store.close();
  }
}

export function openReplica(opts: ReplicaOptions): Promise<Replica> {
  return Replica.open(opts);
}
