/**
 * One sync of one full-mode namespace (docs/kdb-client-replica-plan.md §4.1), over an open v2
 * connection. In order:
 *
 * 1. Finish a convergence left half-done (by snapshot pages or by tree diff).
 * 2. Push local commits (REF_UPDATE). The host merges them if it moved meanwhile; the replica
 *    never merges. A conflict stops here: the commits stay pending and the app is told.
 * 3. Pull to the host's main:
 *    - an empty namespace bootstraps from a snapshot (SNAPSHOT_FETCH), not from history;
 *    - otherwise SQUASH_HEAD says how far behind the replica is, and the catch-up route decides:
 *      replay the commits (FETCH/PACK_PAGE, then fast-forward) or converge by tree diff
 *      (TREE_NODES + OBJECT_FETCH, the C7 squash), as go/kdb/peersync/squash.go's trySquash does.
 */

import { fromHex } from "./bytes.ts";
import { contentHash, decodeCommit, type Commit } from "./commit.ts";
import { ReplicaNamespace, type TreeNodeView } from "./namespace.ts";
import type { ProjectionNamespace, WriteBackDoc } from "./projection.ts";

export class SingleHomeError extends Error {
  override name = "SingleHomeError";
}
import {
  DEFAULT_TREE_ENTRY_LIMIT,
  MAX_OBJECT_FETCH_ITEMS,
  MAX_TREE_NODE_PREFIXES,
  ProtocolError,
  RemoteError,
  type ConflictReport,
  type ProjectWriteResult,
  type NamespaceRefs,
  type PeerConnection,
  type WireTreeNode,
} from "./peer.ts";

export type CatchUp = "replay" | "squash" | "auto";

/** go/kdb/peersync/squash.go: auto squashes from this backlog, at this many commits per change. */
export const AUTO_SQUASH_MIN_BACKLOG = 1000;
export const AUTO_SQUASH_BACKLOG_PER_CHANGE = 2;

export interface SyncOptions {
  catchUp: CatchUp;
  pageBytes?: number;
}

export interface NamespaceSyncResult {
  namespace: string;
  pushed: number;
  /** "fast-forwarded", "merged", "created", "noop" or "conflict", when something was pushed. */
  pushOutcome?: string;
  conflict?: ConflictReport;
  /** How the namespace caught up, if it moved. */
  route?: "snapshot" | "replay" | "squash";
  changed: string[];
  head: string | null;
}

const HEX = "0123456789abcdef";

export async function syncNamespace(
  conn: PeerConnection,
  ns: ReplicaNamespace,
  remote: NamespaceRefs | undefined,
  opts: SyncOptions,
): Promise<NamespaceSyncResult> {
  const res: NamespaceSyncResult = { namespace: ns.name, pushed: 0, changed: [], head: ns.head };
  if (!remote) throw new ProtocolError(`kdb replica: the host did not grant ${ns.name}`);
  let remoteMain = remote.branches?.main ?? "";

  if (ns.target) {
    res.route = ns.target.via === "snapshot" ? "snapshot" : "squash";
    res.changed.push(...(await resumeConverge(conn, ns, opts)));
  }

  if (ns.pending.length > 0) {
    if (remote.access === "pull") {
      throw new ProtocolError(`kdb replica: ${ns.name} is read-only for this token; its local commits cannot be pushed`);
    }
    let ack;
    try {
      ack = await push(conn, ns, remoteMain, opts);
    } catch (e) {
      // NOT_HOME, or - once the host knows the handover point - its refusal of a commit made after
      // it by a node that is not the home (server.StaleFenceError, sent as CONFLICT; the wire has
      // no code of its own for it, so it is recognised by its message).
      if (e instanceof RemoteError && (e.code === "NOT_HOME" || (e.code === "CONFLICT" && /home is now at fence/.test(e.message)))) {
        // Plan D6: a single-home namespace takes writes only at its home, so a replica that wants
        // to write it holds it filtered with write-back, which queues writes until the home is
        // reachable. Its local commits here can never be pushed.
        throw new SingleHomeError(
          `kdb replica: ${ns.name} is single-home (${e.message}); open it with mode "filtered" and writeBack to write to it`,
        );
      }
      throw e;
    }
    res.pushOutcome = ack.outcome;
    if (ack.outcome === "conflict") {
      res.conflict = ack.conflict;
      res.head = ns.head;
      return res;
    }
    res.pushed = ns.pending.length;
    remoteMain = ack.headHex || ns.head || remoteMain;
    await ns.markPushed(ns.pending.length, remoteMain);
  }

  if (remote.access !== "push" && remoteMain && remoteMain !== ns.head) {
    if (ns.isEmpty) {
      res.route = "snapshot";
      res.changed.push(...(await bootstrap(conn, ns, remoteMain, opts)));
    } else {
      const caught = await catchUp(conn, ns, remoteMain, opts);
      res.route = caught.route;
      res.changed.push(...caught.changed);
    }
  }
  await ns.setRemoteMain(ns.head ?? (remoteMain || undefined));
  res.head = ns.head;
  return res;
}

async function push(conn: PeerConnection, ns: ReplicaNamespace, remoteMain: string, opts: SyncOptions) {
  const commits = await ns.pendingCommits();
  const limit = opts.pageBytes ?? 4 << 20;
  let page: Commit[] = [];
  let size = 0;
  for (let i = 0; i < commits.length; i++) {
    const c = commits[i]!;
    page.push(c);
    size += c.payload.length;
    const last = i === commits.length - 1;
    if (!last && size < limit) continue;
    const ack = await conn.refUpdate(ns.name, remoteMain, ns.head!, page, !last);
    if (last) return ack;
    page = [];
    size = 0;
  }
  throw new Error("kdb replica: nothing to push");
}

/** An empty namespace takes the host's state at main from a snapshot, paged by document id. */
async function bootstrap(conn: PeerConnection, ns: ReplicaNamespace, at: string, opts: SyncOptions): Promise<string[]> {
  const first = await conn.snapshot(ns.name, at, "", opts.pageBytes);
  await ns.beginConverge(first.commit, "snapshot");
  return resumeConverge(conn, ns, opts, first);
}

async function resumeConverge(
  conn: PeerConnection,
  ns: ReplicaNamespace,
  opts: SyncOptions,
  firstPage?: Awaited<ReturnType<PeerConnection["snapshot"]>>,
): Promise<string[]> {
  const t = ns.target!;
  const changed: string[] = [];
  if (t.via === "snapshot") {
    let page = firstPage;
    let after = t.after;
    for (;;) {
      page ??= await conn.snapshot(ns.name, t.hash, after, opts.pageBytes);
      if (page.commit.hash !== t.hash) {
        throw new ProtocolError(`kdb replica: snapshot page is of ${page.commit.hash}, not ${t.hash}`);
      }
      const docs = page.docs.map((d) => [d.id, d.body] as [string, string]);
      after = page.done ? after : page.next;
      await ns.applyConvergePage(docs, after);
      changed.push(...docs.map(([id]) => id));
      if (page.done) break;
      page = undefined;
    }
    try {
      await ns.finishConverge();
      return changed;
    } catch {
      // A snapshot laid over what an earlier, interrupted attempt left can disagree with the
      // target; a diff against the target's tree repairs exactly that.
    }
  }
  const tree = decodeCommit(fromHex(ns.target!.payloadHex)).treeHash;
  changed.push(...(await converge(conn, ns, tree)));
  await ns.finishConverge();
  return changed;
}

async function catchUp(conn: PeerConnection, ns: ReplicaNamespace, remoteMain: string, opts: SyncOptions): Promise<{ route: "replay" | "squash"; changed: string[] }> {
  if (opts.catchUp !== "replay" && conn.has("squash") && conn.has("repair")) {
    const head = await conn.squashHead(ns.name, ns.head!);
    const target = head.commit;
    // Refused: the host wants every replica to hold full history, so replay. Not descending: the
    // host's main does not build on this replica's head (it rolled back, or squashed away), and
    // only converging can follow it - this replica has no unpushed commits to lose by then.
    if (!head.refused && target) {
      let diff: Diff | undefined;
      let squash = opts.catchUp === "squash" || !head.descends;
      if (!squash && head.backlog >= AUTO_SQUASH_MIN_BACKLOG) {
        diff = await diffTrees(conn, ns, target.treeHash);
        squash = head.backlog >= AUTO_SQUASH_BACKLOG_PER_CHANGE * diff.changes.length;
      }
      if (squash) {
        await ns.beginConverge(target, "diff");
        const changed = await converge(conn, ns, target.treeHash, diff);
        await ns.finishConverge();
        return { route: "squash", changed };
      }
    }
  }
  return { route: "replay", changed: await replay(conn, ns, remoteMain, opts) };
}

/** FETCH/PACK_PAGE from the head to want, fast-forwarding whenever a page closes on one tip. */
async function replay(conn: PeerConnection, ns: ReplicaNamespace, want: string, opts: SyncOptions): Promise<string[]> {
  const changed = new Set<string>();
  let held: Commit[] = [];
  for (;;) {
    const haves = [ns.head!, ...tipsOf(held)];
    const page = await conn.fetch(ns.name, [want], haves, opts.pageBytes);
    held.push(...page.commits);
    const tips = tipsOf(held);
    const finished = page.done || page.commits.length === 0;
    if (tips.length === 1) {
      for (const id of await ns.fastForward(held, tips[0]!)) changed.add(id);
      held = [];
    }
    if (finished) break;
  }
  if (held.length > 0 || ns.head !== want) {
    throw new ProtocolError(`kdb replica: replay of ${ns.name} ended at ${ns.head}, not at ${want}`);
  }
  return [...changed];
}

/** Commits no other commit in the set names as a parent. */
function tipsOf(commits: readonly Commit[]): string[] {
  const parents = new Set<string>();
  for (const c of commits) for (const p of c.parents) parents.add(p);
  return commits.filter((c) => !parents.has(c.hash)).map((c) => c.hash);
}

// --- tree diff (go/kdb/document/document_tree_node.go DiffTrees) --------------------------------

interface Diff {
  treeHex: string;
  /** id -> the host's content hash, or "" where the host does not hold it. */
  changes: [string, string][];
}

function fromWire(n: WireTreeNode): TreeNodeView {
  return {
    prefix: n.prefix,
    hash: n.hash ?? "",
    children: n.children ?? [],
    entries: n.hasEntries ? new Map(Object.entries(n.entries ?? {})) : null,
  };
}

async function diffTrees(conn: PeerConnection, ns: ReplicaNamespace, treeHex: string): Promise<Diff> {
  const changes: [string, string][] = [];
  let frontier = [""];
  while (frontier.length > 0) {
    const next: string[] = [];
    for (let i = 0; i < frontier.length; i += MAX_TREE_NODE_PREFIXES) {
      const batch = frontier.slice(i, i + MAX_TREE_NODE_PREFIXES);
      const remote = (await conn.treeNodes(ns.name, treeHex, batch)).nodes.map(fromWire);
      if (remote.length !== batch.length) throw new ProtocolError("kdb replica: TREE_NODES answered the wrong number of nodes");
      for (let j = 0; j < batch.length; j++) {
        const prefix = batch[j]!;
        const l = await ns.treeNode(prefix, DEFAULT_TREE_ENTRY_LIMIT);
        const r = remote[j]!;
        if (l.hash === r.hash) continue;
        if ((l.entries || l.hash === "") && (r.entries || r.hash === "")) {
          const le = l.entries ?? new Map<string, string>();
          const re = r.entries ?? new Map<string, string>();
          for (const [id, h] of le) if (re.get(id) !== h) changes.push([id, re.get(id) ?? ""]);
          for (const [id, h] of re) if (!le.has(id)) changes.push([id, h]);
          continue;
        }
        if (prefix.length === 32) throw new ProtocolError(`kdb replica: subtree ${prefix} differs but lists no entries`);
        for (let nib = 0; nib < 16; nib++) {
          if ((l.children[nib] ?? "") !== (r.children[nib] ?? "")) next.push(prefix + HEX[nib]);
        }
      }
    }
    frontier = next;
  }
  changes.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
  return { treeHex, changes };
}

/**
 * Makes the namespace's documents equal the host's tree treeHex: diff, then fetch the changed
 * bodies (each checked against its content hash) and apply them a chunk at a time.
 */
async function converge(conn: PeerConnection, ns: ReplicaNamespace, treeHex: string, known?: Diff): Promise<string[]> {
  const diff = known ?? (await diffTrees(conn, ns, treeHex));
  for (let i = 0; i < diff.changes.length; i += MAX_OBJECT_FETCH_ITEMS) {
    const chunk = diff.changes.slice(i, i + MAX_OBJECT_FETCH_ITEMS);
    const wanted = chunk.filter(([, h]) => h !== "");
    const bodies = new Map<string, string>();
    if (wanted.length > 0) {
      const got = await conn.objectFetch(ns.name, wanted.map(([id, h]) => ({ id, contentHash: h, treeHex })));
      for (const d of got.docs) bodies.set(d.id.toLowerCase(), d.body);
      if (got.missing.length > 0) {
        throw new ProtocolError(`kdb replica: the host could not supply ${got.missing.length} changed documents of ${ns.name}`);
      }
    }
    const page: [string, string | null][] = [];
    for (const [id, h] of chunk) {
      if (h === "") {
        page.push([id, null]);
        continue;
      }
      const body = bodies.get(id);
      if (body === undefined || contentHash(id, body) !== h) {
        throw new ProtocolError(`kdb replica: the host's body for ${id} does not match its content hash`);
      }
      page.push([id, body]);
    }
    await ns.applyConvergePage(page, chunk.at(-1)![0]);
  }
  return diff.changes.map(([id]) => id);
}

// --- filtered namespaces (plan §4.2) ------------------------------------------------------------

export interface ProjectionSyncResult {
  namespace: string;
  filter: string;
  /** Outbox entries the source applied this round. */
  applied: number;
  /** Outbox entries the source did not apply: their documents went back to the source's state. */
  rejected: { txId: string; outcome: string; reason: string; docs: WriteBackDoc[]; current: ProjectWriteResult["current"] }[];
  /** Why write-back stopped early (a transient refusal such as NOT_HOME); the rest stays queued. */
  deferred?: string;
  reset: boolean;
  changed: string[];
  lastSource: string;
}

export async function syncProjection(conn: PeerConnection, p: ProjectionNamespace, opts: SyncOptions): Promise<ProjectionSyncResult> {
  const res: ProjectionSyncResult = { namespace: p.name, filter: p.filter, applied: 0, rejected: [], reset: false, changed: [], lastSource: p.lastSource };
  if (p.writeBack) {
    if (!conn.has("writeback")) throw new ProtocolError(`kdb replica: the host does not take write-back for ${p.name}`);
    for (const entry of await p.outbox()) {
      let r: ProjectWriteResult;
      try {
        r = await conn.projectWrite(p.name, entry.txId, entry.docs);
      } catch (e) {
        if (e instanceof RemoteError) {
          // Transient at the source (not its home, busy): keep the queue, in order, for next time.
          res.deferred = `${e.code}: ${e.message}`;
          break;
        }
        throw e;
      }
      if (r.outcome === "applied") {
        await p.resolve(entry, null);
        res.applied++;
      } else {
        await p.resolve(entry, r.current);
        res.rejected.push({ txId: entry.txId, outcome: r.outcome, reason: r.reason, docs: entry.docs, current: r.current });
        res.changed.push(...r.current.map((c) => c.docId));
      }
    }
  }
  const t = await p.beginTransfer();
  let at = t.at, after = t.after;
  for (;;) {
    const page = await conn.projectFetch(p.name, p.filter, t.from, at, after, opts.pageBytes);
    if (at === "") at = page.atHex;
    if (page.atHex !== at) throw new ProtocolError(`kdb replica: projection page is at ${page.atHex}, not ${at}`);
    res.reset ||= page.reset;
    res.changed.push(...(await p.applyPage(t.from, after, page)));
    if (page.done) break;
    after = page.next;
  }
  res.lastSource = p.lastSource;
  return res;
}
