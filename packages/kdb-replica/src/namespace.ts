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

import { fromHex, fromUtf8, toHex, utf8, uuidBytes, uuidString } from "./bytes.ts";
import { buildCommit, contentHash, decodeCommit, genesisCommit, type Commit, type Op } from "./commit.ts";
import { prefixEnd, type Batch, type KvStore } from "./store.ts";
import { foldLeaf, TrieEdit, TrieReader } from "./trie.ts";
import { AttachmentEdit, AttachmentError, attachmentRoot, attachmentsOf, type Attachment } from "./attachments.ts";

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

/** A commit's attachment root is not the table the replica built (or could build) for it. */
export class AttachmentRootMismatchError extends Error {
  override name = "AttachmentRootMismatchError";
  readonly commit: string;
  constructor(commit: string, declared: string | undefined, built: string | undefined) {
    super(`kdb replica: commit ${commit} declares attachment root ${declared ?? "(none)"}, the replica has ${built ?? "(none)"}`);
    this.commit = commit;
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
  /**
   * A head this namespace is converging to by snapshot or tree diff, set while documents are
   * being installed in pages. While it is set, the documents are a mix of the old head's and the
   * target's, so the namespace takes no local writes; a sync resumes the convergence, and the
   * target becomes the head only once the built tree is the target's.
   */
  target?: { hash: string; payloadHex: string; via: "snapshot" | "diff"; after: string } | undefined;
  /** The host's main as of the last sync: the oldHex a push proposes to move. */
  remoteMain?: string | undefined;
}

export class ReplicaBusyError extends Error {
  override name = "ReplicaBusyError";
}

/** One position of the trie, as TREE_NODES reports it (go/kdb/document/document_tree_node.go). */
export interface TreeNodeView {
  prefix: string;
  /** "" when the subtree is empty. */
  hash: string;
  /** 16 child hashes, "" for an empty child; empty when the subtree is empty. */
  children: string[];
  /** id -> content hash, when the subtree holds no more than the entry limit. */
  entries: Map<string, string> | null;
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
    const genesis = genesisCommit(name);
    const meta: NamespaceMeta = { name, nodeId, head: genesis.hash, pending: [], lastMicros: "0" };
    await store.apply(new Map([
      [ReplicaNamespace.metaKey(name), utf8(JSON.stringify(meta))],
      [`c/${namespaceKey(name)}/${genesis.hash}`, genesis.payload],
    ]));
    return new ReplicaNamespace(store, meta);
  }

  get head(): string | null {
    return this.#meta.head;
  }

  /** Whether the namespace is still at its genesis, holding nothing: what a snapshot bootstraps. */
  get isEmpty(): boolean {
    return this.#meta.head === genesisCommit(this.name).hash && this.#meta.pending.length === 0 && !this.#meta.target;
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

  /** The document's attachments, by id. */
  attachments(id: string): Promise<Attachment[]> {
    return attachmentsOf(this.#store, this.key, id);
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
      if (this.#meta.target) throw new NotFastForwardError("kdb replica: finish converging before fast-forwarding");
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
      // Attachments: every op of the range in order. A commit's table is its first parent's plus
      // its own ops, so this is right for a linear range; across a merge it is checked like the
      // tree - against the root the target declares - and a mismatch writes nothing.
      const att = new AttachmentEdit(this.#store, this.key);
      let attRoot: string | undefined;
      try {
        for (const c of ordered) await att.apply(c.ops);
        attRoot = await att.finish();
      } catch (e) {
        if (!(e instanceof AttachmentError)) throw e;
        throw new AttachmentRootMismatchError(target, tip.attachmentRoot, `unappliable (${e.message})`);
      }
      if (attRoot !== tip.attachmentRoot) throw new AttachmentRootMismatchError(target, tip.attachmentRoot, attRoot);
      att.writeTo(batch);
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
      if (!roots || [...seen].some((h) => !byHash.has(h))) {
        throw new NotFastForwardError("kdb replica: a namespace with no head needs the whole history or a snapshot");
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
      if (this.#meta.target) {
        throw new ReplicaBusyError(`kdb replica: ${this.name} is being brought up to date; retry once it has synced`);
      }
      const batch: Batch = new Map();
      const edit = await this.#stage(ops, batch);
      const tree = await edit.finish();
      edit.writeTo(batch);
      const att = new AttachmentEdit(this.#store, this.key);
      await att.apply(ops);
      const attachmentRoot = await att.finish();
      att.writeTo(batch);
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
        attachmentRoot,
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

  get target(): NamespaceMeta["target"] {
    return this.#meta.target;
  }

  get remoteMain(): string | undefined {
    return this.#meta.remoteMain;
  }

  get nodeId(): string {
    return this.#meta.nodeId;
  }

  async #commitMeta(batch: Batch, meta: NamespaceMeta): Promise<void> {
    this.#writeMeta(batch, meta);
    await this.#store.apply(batch);
    this.#meta = meta;
  }

  /** Records the host's main as of this sync. */
  setRemoteMain(hash: string | undefined): Promise<void> {
    return this.#lock.run(() => this.#commitMeta(new Map(), { ...this.#meta, remoteMain: hash }));
  }

  /**
   * Starts converging to target by `via`; a no-op if already converging to it. Refused while
   * local commits are unpushed: a replica never drops them.
   */
  beginConverge(target: Commit, via: "snapshot" | "diff"): Promise<void> {
    return this.#lock.run(async () => {
      if (this.#meta.target?.hash === target.hash && this.#meta.target.via === via) return;
      if (this.#meta.pending.length > 0) {
        throw new NotFastForwardError("kdb replica: local commits are not yet pushed; push before converging");
      }
      await this.#commitMeta(new Map(), {
        ...this.#meta, target: { hash: target.hash, payloadHex: toHex(target.payload), via, after: "" },
      });
    });
  }

  /**
   * Applies one page of a convergence: final states for some documents (null deletes), and the
   * cursor to resume after. Unverified until finishConverge, but durable, so an interrupted
   * convergence resumes rather than restarts.
   */
  applyConvergePage(docs: Iterable<[string, string | null]>, after: string): Promise<void> {
    return this.#lock.run(async () => {
      const t = this.#meta.target;
      if (!t) throw new Error("kdb replica: not converging");
      const ops: Op[] = [];
      for (const [id, json] of docs) {
        ops.push(json === null ? { kind: "delete", docId: id } : { kind: "write", docId: id, json });
      }
      const batch: Batch = new Map();
      const edit = await this.#stage(ops, batch);
      await edit.finish();
      edit.writeTo(batch);
      await this.#commitMeta(batch, { ...this.#meta, target: { ...t, after } });
    });
  }

  /**
   * Completes a convergence: the built tree must be the target's. Then the target is the head, a
   * root this replica holds without its history. Throws TreeMismatchError otherwise, and stays
   * converging - a diff against the target's tree then repairs whatever differs.
   */
  finishConverge(): Promise<void> {
    return this.#lock.run(async () => {
      const t = this.#meta.target;
      if (!t) return;
      const tip = decodeCommit(fromHex(t.payloadHex));
      const built = await this.#trie.rootHash();
      if (built !== tip.treeHash) throw new TreeMismatchError(tip.hash, tip.treeHash, built);
      const attRoot = await attachmentRoot(this.#store, this.key);
      if (attRoot !== tip.attachmentRoot) throw new AttachmentRootMismatchError(tip.hash, tip.attachmentRoot, attRoot);
      const batch: Batch = new Map();
      this.#replaceHead(batch, tip, maxMicros(this.#meta.lastMicros, [tip]));
      const meta = JSON.parse(fromUtf8(batch.get(ReplicaNamespace.metaKey(this.name))!)) as NamespaceMeta;
      meta.target = undefined;
      await this.#commitMeta(batch, meta);
    });
  }

  /** Marks the oldest n pending commits as held by the host. */
  markPushed(n: number, remoteMain: string): Promise<void> {
    return this.#lock.run(async () => {
      const pending = this.#meta.pending.slice(n);
      const batch: Batch = new Map();
      // Pushed commits other than the head are history the replica does not keep.
      for (const h of this.#meta.pending.slice(0, n)) if (h !== this.#meta.head) batch.set(this.#commitKey(h), null);
      await this.#commitMeta(batch, { ...this.#meta, pending, remoteMain });
    });
  }

  /** The unpushed local commits, oldest first. */
  async pendingCommits(): Promise<Commit[]> {
    const out: Commit[] = [];
    for (const h of this.#meta.pending) {
      const c = await this.commit(h);
      if (!c) throw new Error(`kdb replica: pending commit ${h} is missing from the store`);
      out.push(c);
    }
    return out;
  }

  /**
   * The subtree at prefix, as Go's DocumentTree.Node reports it, so a diff against a host's
   * TREE_NODES compares like with like - including for a subtree stored folded into one leaf.
   */
  async treeNode(prefix: string, entryLimit: number): Promise<TreeNodeView> {
    const empty: TreeNodeView = { prefix, hash: "", children: [], entries: null };
    let at = "";
    let rec = await this.#trie.record(at);
    while (rec && !rec.leaf && at.length < prefix.length) {
      at = prefix.slice(0, at.length + 1);
      rec = await this.#trie.record(at);
    }
    if (!rec) return empty;
    const depth = prefix.length;
    if (rec.leaf) {
      const idHex = toHex(rec.id);
      if (!idHex.startsWith(prefix)) return empty;
      const children = new Array<string>(16).fill("");
      if (depth < 32) children[Number.parseInt(idHex[depth]!, 16)] = toHex(foldLeaf(rec.id, rec.content, depth + 1));
      return {
        prefix,
        hash: toHex(foldLeaf(rec.id, rec.content, depth)),
        children,
        entries: entryLimit >= 1 ? new Map([[uuidString(rec.id), toHex(rec.content)]]) : null,
      };
    }
    const children: string[] = [];
    for (let i = 0; i < 16; i++) {
      const h = toHex(rec.children.subarray(i * 32, i * 32 + 32));
      children.push(/^0+$/.test(h) ? "" : h);
    }
    let entries: Map<string, string> | null = null;
    if (entryLimit > 0) {
      entries = new Map();
      const found = entries;
      const stop = new Error("stop");
      try {
        await this.#trie.walk((id, ch) => {
          found.set(id, ch);
          if (found.size > entryLimit) throw stop;
        }, prefix);
      } catch (e) {
        if (e !== stop) throw e;
        entries = null;
      }
    }
    return { prefix, hash: toHex(rec.hash!), children, entries };
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
