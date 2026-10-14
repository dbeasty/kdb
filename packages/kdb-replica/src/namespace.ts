/**
 * One namespace held by the replica: its documents, its trie, its head commit and the local commits
 * not yet pushed. It holds no other history.
 *
 * The replica never merges. It applies only fast-forwards: given the commits between its head and
 * a target, it topo-sorts them, keeps each document's last op, applies those, and requires the
 * tree it built to equal the target's declared tree hash. That is go/kdb/peersync/ingest.go's
 * fastForward → netEffect → applyAndVerify. A merge commit carries every document its parents
 * disagree on, so the last op in any topological order is the merged state. Everything a step
 * changes (documents, trie, head) is one store transaction, written only after the tree hash
 * checks out.
 */

import { fromHex, fromUtf8, toHex, utf8, uuidBytes } from "./bytes.ts";
import { buildCommit, contentHash, decodeCommit, type Commit, type Op } from "./commit.ts";
import { prefixEnd, type Batch, type KvStore } from "./store.ts";
import { TrieEdit, TrieReader } from "./trie.ts";

export class TreeMismatchError extends Error {
  override name = "TreeMismatchError";
  readonly commit: string;
  readonly declared: string;
  readonly built: string;
  constructor(commit: string, declared: string, built: string) {
    super(`kdb replica: commit ${commit} declares tree ${declared}, applying its range built ${built}`);
    this.commit = commit;
    this.declared = declared;
    this.built = built;
  }
}

export class NotFastForwardError extends Error {
  override name = "NotFastForwardError";
}

export interface NamespaceMeta {
  name: string;
  /** The replica's own node id: the author of its commits. */
  nodeId: string;
  head: string | null;
  /** Local commits not yet confirmed by the host, oldest first. */
  pending: string[];
  /** The highest commit timestamp seen or made, so local timestamps never go backwards. */
  lastMicros: string;
}

export function namespaceKey(name: string): string {
  return encodeURIComponent(name);
}

/** A simple async mutex: one operation on a namespace at a time. */
class Lock {
  #tail: Promise<unknown> = Promise.resolve();
  run<T>(fn: () => Promise<T>): Promise<T> {
    const next = this.#tail.then(fn, fn);
    this.#tail = next.catch(() => undefined);
    return next;
  }
}

/** Topological order of commits by their parents within the set; ties broken by hash. */
export function topoSort(commits: readonly Commit[]): Commit[] {
  const byHash = new Map(commits.map((c) => [c.hash, c]));
  const indegree = new Map<string, number>();
  const children = new Map<string, string[]>();
  for (const c of commits) {
    let n = 0;
    for (const p of c.parents) {
      if (!byHash.has(p)) continue;
      n++;
      const list = children.get(p) ?? [];
      list.push(c.hash);
      children.set(p, list);
    }
    indegree.set(c.hash, n);
  }
  const ready = commits.filter((c) => indegree.get(c.hash) === 0).map((c) => c.hash).sort();
  const out: Commit[] = [];
  while (ready.length > 0) {
    const h = ready.shift()!;
    out.push(byHash.get(h)!);
    for (const child of children.get(h) ?? []) {
      const n = indegree.get(child)! - 1;
      indegree.set(child, n);
      if (n === 0) {
        // keep `ready` sorted so the order is deterministic
        let i = 0;
        while (i < ready.length && ready[i]! < child) i++;
        ready.splice(i, 0, child);
      }
    }
  }
  if (out.length !== commits.length) throw new NotFastForwardError("kdb replica: commit range has a cycle");
  return out;
}

/** The last op per document over commits in order: what a fast-forward applies. */
export function netEffect(ordered: readonly Commit[]): Map<string, Op> {
  const final = new Map<string, Op>();
  for (const c of ordered) {
    for (const op of c.ops) {
      if (op.kind === "write" || op.kind === "delete") final.set(op.docId, op);
    }
  }
  return final;
}

export class ReplicaNamespace {
  readonly name: string;
  readonly key: string;
  readonly #store: KvStore;
  readonly #trie: TrieReader;
  readonly #lock = new Lock();
  #meta: NamespaceMeta;

  private constructor(store: KvStore, meta: NamespaceMeta) {
    this.#store = store;
    this.name = meta.name;
    this.key = namespaceKey(meta.name);
    this.#trie = new TrieReader(store, this.key);
    this.#meta = meta;
  }

  static metaKey(name: string): string {
    return `m/ns/${namespaceKey(name)}`;
  }

  /** Opens namespace name in store, creating its (empty) metadata on first use. */
  static async open(store: KvStore, name: string, nodeId: string): Promise<ReplicaNamespace> {
    const raw = await store.get(ReplicaNamespace.metaKey(name));
    if (raw) return new ReplicaNamespace(store, JSON.parse(fromUtf8(raw)) as NamespaceMeta);
    const meta: NamespaceMeta = { name, nodeId, head: null, pending: [], lastMicros: "0" };
    await store.apply(new Map([[ReplicaNamespace.metaKey(name), utf8(JSON.stringify(meta))]]));
    return new ReplicaNamespace(store, meta);
  }

  get head(): string | null {
    return this.#meta.head;
  }

  get pending(): readonly string[] {
    return this.#meta.pending;
  }

  #docKey(id: string): string {
    return `d/${this.key}/${id.toLowerCase()}`;
  }

  #commitKey(hash: string): string {
    return `c/${this.key}/${hash}`;
  }

  #writeMeta(batch: Batch, meta: NamespaceMeta): void {
    batch.set(ReplicaNamespace.metaKey(this.name), utf8(JSON.stringify(meta)));
  }

  async rootHash(): Promise<string> {
    return this.#trie.rootHash();
  }

  async commit(hash: string): Promise<Commit | undefined> {
    const b = await this.#store.get(this.#commitKey(hash));
    return b ? decodeCommit(b) : undefined;
  }

  async headCommit(): Promise<Commit | undefined> {
    return this.#meta.head ? this.commit(this.#meta.head) : undefined;
  }

  /** The document's JSON, or undefined. */
  async get(id: string): Promise<string | undefined> {
    const b = await this.#store.get(this.#docKey(id));
    return b ? fromUtf8(b.subarray(32)) : undefined;
  }

  /** The document's content hash, or undefined. */
  async contentHashOf(id: string): Promise<string | undefined> {
    const b = await this.#store.get(this.#docKey(id));
    return b ? toHex(b.subarray(0, 32)) : undefined;
  }

  /** Documents in id order, after `after` (exclusive), at most limit. */
  async scan(opts: { after?: string; before?: string; limit?: number } = {}): Promise<{ id: string; json: string }[]> {
    const base = `d/${this.key}/`;
    const start = opts.after ? base + opts.after.toLowerCase() + "\u0000" : base;
    const end = opts.before ? base + opts.before.toLowerCase() : prefixEnd(base);
    const rows = await this.#store.scan(start, end, opts.limit);
    return rows.map(([k, v]) => ({ id: k.slice(base.length), json: fromUtf8(v.subarray(32)) }));
  }

  /** Stages ops into batch and a trie edit; returns the edit (not yet finished). */
  async #stage(ops: Iterable<Op>, batch: Batch): Promise<TrieEdit> {
    const edit = new TrieEdit(this.#store, this.key);
    for (const op of ops) {
      if (op.kind === "write") {
        uuidBytes(op.docId);
        const ch = contentHash(op.docId, op.json);
        await edit.set(op.docId, ch);
        const json = utf8(op.json);
        const v = new Uint8Array(32 + json.length);
        v.set(fromHex(ch), 0);
        v.set(json, 32);
        batch.set(this.#docKey(op.docId), v);
      } else if (op.kind === "delete") {
        await edit.delete(op.docId);
        batch.set(this.#docKey(op.docId), null);
      }
    }
    return edit;
  }

  /**
   * Fast-forwards to target, given the commits between the current head and target (any order).
   * The range must reach the current head; the tree built must be target's. Returns the ids of the
   * documents that changed.
   */
  fastForward(commits: readonly Commit[], target: string): Promise<string[]> {
    return this.#lock.run(async () => {
      if (this.#meta.pending.length > 0) {
        throw new NotFastForwardError("kdb replica: local commits are not yet pushed; push before pulling");
      }
      if (target === this.#meta.head) return [];
      const byHash = new Map(commits.map((c) => [c.hash, c]));
      const tip = byHash.get(target);
      if (!tip) throw new NotFastForwardError(`kdb replica: the range does not contain its target ${target}`);
      this.#checkReachesHead(byHash, target);
      const ordered = topoSort(commits);
      const effect = netEffect(ordered);
      const batch: Batch = new Map();
      const edit = await this.#stage(effect.values(), batch);
      const built = await edit.finish();
      if (built !== tip.treeHash) throw new TreeMismatchError(target, tip.treeHash, built);
      edit.writeTo(batch);
      this.#replaceHead(batch, tip, maxMicros(this.#meta.lastMicros, ordered));
      await this.#store.apply(batch);
      this.#meta = JSON.parse(fromUtf8(batch.get(ReplicaNamespace.metaKey(this.name))!)) as NamespaceMeta;
      return [...effect.keys()];
    });
  }

  #checkReachesHead(byHash: Map<string, Commit>, target: string): void {
    const head = this.#meta.head;
    const seen = new Set<string>();
    const queue = [target];
    let reached = false, roots = false;
    while (queue.length > 0) {
      const h = queue.pop()!;
      if (seen.has(h)) continue;
      seen.add(h);
      const c = byHash.get(h);
      if (!c) {
        if (h === head) reached = true;
        continue;
      }
      if (c.parents.length === 0) roots = true;
      queue.push(...c.parents);
    }
    if (head === null) {
      // A replica with no head starts from a genesis chain; a later root comes by snapshot.
      if (!roots || [...seen].some((h) => !byHash.has(h))) {
        throw new NotFastForwardError("kdb replica: an empty namespace needs the whole history or a snapshot");
      }
    } else if (!reached) {
      throw new NotFastForwardError(`kdb replica: ${target} does not descend from the head ${head}`);
    }
  }

  #replaceHead(batch: Batch, tip: Commit, lastMicros: string): void {
    const old = this.#meta.head;
    if (old && old !== tip.hash) batch.set(this.#commitKey(old), null);
    batch.set(this.#commitKey(tip.hash), tip.payload);
    this.#writeMeta(batch, { ...this.#meta, head: tip.hash, lastMicros });
  }

  /**
   * Commits ops locally on top of the head and queues the commit for push. Returns the commit.
   * Its timestamp is the wall clock, but never at or below one the replica has already seen or
   * made, so a device with a slow clock still orders after the host's history.
   */
  commitLocal(ops: Op[], opts: { transactionId: string; message?: string; nowMicros?: bigint }): Promise<Commit> {
    return this.#lock.run(async () => {
      const batch: Batch = new Map();
      const edit = await this.#stage(ops, batch);
      const tree = await edit.finish();
      edit.writeTo(batch);
      const now = opts.nowMicros ?? BigInt(Date.now()) * 1000n;
      const last = BigInt(this.#meta.lastMicros);
      const ts = now > last ? now : last + 1n;
      const c = buildCommit({
        parents: this.#meta.head ? [this.#meta.head] : [],
        namespace: this.name,
        transactionId: opts.transactionId,
        timestampMicros: ts,
        authorNodeId: this.#meta.nodeId,
        ops,
        treeHash: tree,
        message: opts.message ?? "",
      });
      batch.set(this.#commitKey(c.hash), c.payload);
      // The head's own payload stays: a pending commit's parent may be needed for a push.
      this.#writeMeta(batch, {
        ...this.#meta, head: c.hash, pending: [...this.#meta.pending, c.hash], lastMicros: ts.toString(),
      });
      await this.#store.apply(batch);
      this.#meta = JSON.parse(fromUtf8(batch.get(ReplicaNamespace.metaKey(this.name))!)) as NamespaceMeta;
      return c;
    });
  }

  /** Every document as (id, contentHash), in id order, from the trie. */
  walkTree(visit: (id: string, contentHash: string) => void | Promise<void>): Promise<void> {
    return this.#trie.walk(visit);
  }
}

function maxMicros(last: string, commits: readonly Commit[]): string {
  let m = BigInt(last);
  for (const c of commits) if (c.timestampMicros > m) m = c.timestampMicros;
  return m.toString();
}
