import assert from "node:assert/strict";
import { test } from "node:test";
import { fromHex } from "../src/bytes.ts";
import { buildCommit, decodeCommit, type Commit } from "../src/commit.ts";
import { NotFastForwardError, ReplicaNamespace, TreeMismatchError } from "../src/namespace.ts";
import { MemoryStore } from "../src/store.ts";
import { load, type ChainFixture } from "./golden.ts";

const chain = load<ChainFixture>("chain.json");
const byHash = new Map<string, Commit>(chain.commits.map((f) => [f.hash, decodeCommit(fromHex(f.payloadHex))]));
const NODE = "99999999-9999-4999-8999-999999999999";

async function docsOf(ns: ReplicaNamespace): Promise<Record<string, string>> {
  const out: Record<string, string> = {};
  for (const d of await ns.scan()) out[d.id] = d.json;
  return out;
}

test("fast-forwarding Go's chain step by step, through a merge, reaches every declared tree", async () => {
  const store = new MemoryStore();
  const ns = await ReplicaNamespace.open(store, chain.namespace, NODE);
  for (const step of chain.steps) {
    assert.equal(ns.head, step.from);
    await ns.fastForward(step.commits.map((h) => byHash.get(h)!), step.to);
    assert.equal(ns.head, step.to);
    assert.equal(await ns.rootHash(), byHash.get(step.to)!.treeHash);
    assert.deepEqual(await docsOf(ns), step.docs);
  }
  // Reopening reads the same state back from the store.
  const again = await ReplicaNamespace.open(store, chain.namespace, NODE);
  assert.equal(again.head, chain.steps.at(-1)!.to);
  assert.deepEqual(await docsOf(again), chain.steps.at(-1)!.docs);
  assert.equal((await again.headCommit())?.hash, again.head);
});

test("one fast-forward over the whole chain equals the steps", async () => {
  const ns = await ReplicaNamespace.open(new MemoryStore(), chain.namespace, NODE);
  const last = chain.steps.at(-1)!;
  await ns.fastForward([...byHash.values()].reverse(), last.to);
  assert.deepEqual(await docsOf(ns), last.docs);
});

test("a commit whose tree does not match its ops writes nothing", async () => {
  const store = new MemoryStore();
  const ns = await ReplicaNamespace.open(store, chain.namespace, NODE);
  const first = chain.steps[0]!;
  await ns.fastForward(first.commits.map((h) => byHash.get(h)!), first.to);
  const real = byHash.get(chain.steps[1]!.to)!;
  // The same commit with one body changed: a valid commit with its own hash, whose ops no longer
  // build the tree it declares.
  const forged = buildCommit({
    ...real,
    ops: real.ops.map((op) => (op.kind === "write" ? { ...op, json: `{"forged":1}` } : op)),
  });
  const applies = store.applies;
  await assert.rejects(ns.fastForward([forged], forged.hash), TreeMismatchError);
  assert.equal(store.applies, applies, "nothing written");
  assert.equal(ns.head, first.to);
  assert.deepEqual(await docsOf(ns), first.docs);
  await ns.fastForward([real], real.hash);
  assert.deepEqual(await docsOf(ns), chain.steps[1]!.docs);
});

test("a range that does not descend from the head is refused", async () => {
  const ns = await ReplicaNamespace.open(new MemoryStore(), chain.namespace, NODE);
  const first = chain.steps[0]!;
  await ns.fastForward(first.commits.map((h) => byHash.get(h)!), first.to);
  const last = chain.steps.at(-1)!;
  await assert.rejects(ns.fastForward(last.commits.map((h) => byHash.get(h)!), last.to), NotFastForwardError);
  await assert.rejects(
    (await ReplicaNamespace.open(new MemoryStore(), "x", NODE)).fastForward([byHash.get(chain.steps[1]!.to)!], chain.steps[1]!.to),
    NotFastForwardError,
  );
});

test("local commits chain on the head, verify like any commit, and block pulls until pushed", async () => {
  const store = new MemoryStore();
  const ns = await ReplicaNamespace.open(store, chain.namespace, NODE);
  const first = chain.steps[0]!;
  await ns.fastForward(first.commits.map((h) => byHash.get(h)!), first.to);
  const id = "0b1c2d3e-0000-4000-8000-000000000001";
  const c = await ns.commitLocal([{ kind: "write", docId: id, json: `{"local":true}` }], {
    transactionId: "0b1c2d3e-0000-4000-8000-0000000000aa",
    nowMicros: 5n, // a slow clock: the commit still orders after what the replica has seen
  });
  assert.deepEqual(c.parents, [first.to]);
  assert.ok(c.timestampMicros > byHash.get(first.to)!.timestampMicros);
  assert.equal(c.treeHash, await ns.rootHash());
  assert.equal(decodeCommit(c.payload).hash, c.hash);
  assert.deepEqual(ns.pending, [c.hash]);
  assert.equal(await ns.get(id), `{"local":true}`);
  // A second replica fast-forwarding through the local commit builds the same tree.
  const other = await ReplicaNamespace.open(new MemoryStore(), chain.namespace, NODE);
  await other.fastForward([...first.commits.map((h) => byHash.get(h)!), c], c.hash);
  assert.equal(await other.rootHash(), c.treeHash);
  const second = chain.steps[1]!;
  await assert.rejects(ns.fastForward(second.commits.map((h) => byHash.get(h)!), second.to), NotFastForwardError);
});

test("scan pages by id", async () => {
  const ns = await ReplicaNamespace.open(new MemoryStore(), chain.namespace, NODE);
  const last = chain.steps.at(-1)!;
  await ns.fastForward([...byHash.values()], last.to);
  const ids = Object.keys(last.docs).sort();
  const page1 = await ns.scan({ limit: 2 });
  assert.deepEqual(page1.map((d) => d.id), ids.slice(0, 2));
  const page2 = await ns.scan({ after: page1.at(-1)!.id, limit: 100 });
  assert.deepEqual(page2.map((d) => d.id), ids.slice(2));
});
