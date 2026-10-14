/**
 * Live interop: a TS replica syncing with a real Go node (go/cmd/kdb-replicahost) over WebSocket,
 * with the same peer sync v2 messages Go nodes use. Skipped when there is no Go toolchain.
 */

import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import { randomUuid } from "../src/bytes.ts";
import { openReplica, type Replica, type ReplicaOptions } from "../src/replica.ts";
import { MemoryStore } from "../src/store.ts";
import { goAvailable, startHost, type Host } from "./host.ts";

const skip = !goAvailable() && "no Go toolchain";

async function docsOf(r: Replica, ns: string): Promise<Record<string, string>> {
  const out: Record<string, string> = {};
  for (const d of await r.scan(ns)) out[d.id] = d.json;
  return out;
}

describe("replica against a Go node", { skip }, () => {
  let host: Host;
  const opened: Replica[] = [];
  before(async () => {
    host = await startHost();
  });
  after(async () => {
    for (const r of opened) await r.close();
    await host?.stop();
  });

  async function replica(ns: string, extra: Partial<ReplicaOptions> = {}): Promise<Replica> {
    const r = await openReplica({
      store: new MemoryStore(),
      peer: { url: host.url, token: "rw-token" },
      namespaces: [{ ns }],
      autoSync: false,
      ...extra,
    });
    opened.push(r);
    return r;
  }

  test("an empty replica bootstraps from a snapshot, then replays new commits", async () => {
    const ns = "app/u/boot";
    for (let i = 0; i < 25; i++) await host.put(ns, randomUuid(), { i, pad: "x".repeat(i * 10) });
    const r = await replica(ns, { pageBytes: 512 }); // several snapshot pages
    const [first] = await r.syncNow();
    assert.equal(first!.route, "snapshot");
    assert.equal(r.namespace(ns).head, (await host.head(ns)).head);
    assert.deepEqual(await docsOf(r, ns), await host.docs(ns));

    const id = randomUuid();
    await host.put(ns, id, { late: true });
    const [second] = await r.syncNow();
    assert.equal(second!.route, "replay");
    assert.deepEqual(second!.changed, [id]);
    assert.equal(await r.get(ns, id), `{"late":true}`);
    assert.equal(r.namespace(ns).head, (await host.head(ns)).head);
  });

  test("a long-offline replica catches up by squash, not by replaying every commit", async () => {
    const ns = "app/u/squash";
    await host.put(ns, randomUuid(), { start: 1 });
    const r = await replica(ns);
    await r.syncNow();
    await host.cmd({ op: "seed", ns, commits: 1500, docs: 10 }); // a rewrite-heavy backlog
    const [res] = await r.syncNow();
    assert.equal(res!.route, "squash");
    assert.equal(res!.changed.length, 10);
    assert.equal(r.namespace(ns).head, (await host.head(ns)).head);
    assert.equal(await r.namespace(ns).rootHash(), (await host.head(ns)).tree);
    assert.deepEqual(await docsOf(r, ns), await host.docs(ns));
    // After a squash the replica is an ordinary peer again: the next commit replays.
    await host.put(ns, randomUuid(), { after: "squash" });
    const [next] = await r.syncNow();
    assert.equal(next!.route, "replay");
    assert.deepEqual(await docsOf(r, ns), await host.docs(ns));
  });

  test("catchUp=replay replays the same backlog", async () => {
    const ns = "app/u/replay";
    await host.put(ns, randomUuid(), { start: 1 });
    const r = await replica(ns, { catchUp: "replay", pageBytes: 4096 });
    await r.syncNow();
    await host.cmd({ op: "seed", ns, commits: 300, docs: 7 });
    const [res] = await r.syncNow();
    assert.equal(res!.route, "replay");
    assert.equal(r.namespace(ns).head, (await host.head(ns)).head);
    assert.deepEqual(await docsOf(r, ns), await host.docs(ns));
  });

  test("local writes push as real commits with the host's hashes", async () => {
    const ns = "app/u/push";
    await host.put(ns, randomUuid(), { seed: 1 });
    const r = await replica(ns);
    await r.syncNow();
    const id = randomUuid();
    const local = await r.put(ns, id, { from: "replica" });
    const [res] = await r.syncNow();
    assert.equal(res!.pushOutcome, "fast-forwarded");
    assert.equal((await host.head(ns)).head, local, "the host's head is the replica's commit, byte for byte");
    assert.equal((await host.docs(ns))[id], `{"from":"replica"}`);
    assert.equal(r.status().namespaces[ns]!.pending, 0);
  });

  test("concurrent writes on both sides: the host merges, the replica fast-forwards to the merge", async () => {
    const ns = "app/u/merge";
    await host.put(ns, randomUuid(), { seed: 1 });
    const r = await replica(ns);
    await r.syncNow();
    const mine = randomUuid(), theirs = randomUuid();
    await r.put(ns, mine, { side: "replica" });
    await host.put(ns, theirs, { side: "host" });
    const [res] = await r.syncNow();
    assert.equal(res!.pushOutcome, "merged");
    assert.equal(r.namespace(ns).head, (await host.head(ns)).head);
    const docs = await docsOf(r, ns);
    assert.equal(docs[mine], `{"side":"replica"}`);
    assert.equal(docs[theirs], `{"side":"host"}`);
    assert.deepEqual(docs, await host.docs(ns));
  });

  test("two replicas converge through the host", async () => {
    const ns = "app/u/two";
    const a = await replica(ns), b = await replica(ns);
    await a.put(ns, randomUuid(), { by: "a" });
    await b.put(ns, randomUuid(), { by: "b" });
    await a.syncNow();
    await b.syncNow();
    await a.syncNow();
    assert.equal(a.namespace(ns).head, b.namespace(ns).head);
    assert.deepEqual(await docsOf(a, ns), await docsOf(b, ns));
    assert.equal(Object.keys(await docsOf(a, ns)).length, 2);
  });

  test("a replica reopened from its store keeps its state and syncs on", async () => {
    const ns = "app/u/reopen";
    await host.put(ns, randomUuid(), { v: 1 });
    const store = new MemoryStore();
    const r1 = await replica(ns, { store });
    await r1.syncNow();
    const offline = randomUuid();
    await r1.put(ns, offline, { offline: true });
    await r1.close();
    opened.splice(opened.indexOf(r1), 1);
    const r2 = await replica(ns, { store });
    assert.equal(await r2.get(ns, offline), `{"offline":true}`);
    assert.equal(r2.status().namespaces[ns]!.pending, 1);
    await r2.syncNow();
    assert.equal((await host.docs(ns))[offline], `{"offline":true}`);
  });

  test("a bad token is refused and reported; a read-only token pulls but cannot push", async () => {
    const ns = "app/u/auth";
    await host.put(ns, randomUuid(), { v: 1 });
    const bad = await replica(ns, { peer: { url: host.url, token: "nope" } });
    await assert.rejects(bad.syncNow(), /unauthenticated|refused/i);
    assert.match(bad.status().lastError ?? "", /unauthenticated|refused/i);

    const ro = await replica(ns, { peer: { url: host.url, token: () => "ro-token" } });
    await ro.syncNow();
    assert.deepEqual(await docsOf(ro, ns), await host.docs(ns));
    await ro.put(ns, randomUuid(), { denied: true });
    await assert.rejects(ro.syncNow(), /read-only/);
  });

  test("live: a host write reaches the replica by REFS_WATCH, with no poll", async () => {
    const ns = "app/u/live";
    await host.put(ns, randomUuid(), { v: 1 });
    const r = await replica(ns, { autoSync: true, pollMs: 3_600_000 });
    const until = async (what: string, ok: () => Promise<boolean>) => {
      for (let i = 0; i < 100; i++) {
        if (await ok()) return;
        await new Promise((res) => setTimeout(res, 50));
      }
      assert.fail(`timed out waiting for ${what}`);
    };
    await until("the first sync", async () => r.namespace(ns).head === (await host.head(ns)).head);
    const id = randomUuid();
    await host.put(ns, id, { pushed: "live" });
    await until("the host's write to arrive", async () => (await r.get(ns, id)) !== undefined);
    assert.equal(r.status().connected, true, "the connection stays open while live");
    // And a local write goes up without a poll either.
    const mine = randomUuid();
    await r.put(ns, mine, { from: "replica" });
    await until("the replica's write to reach the host", async () => (await host.docs(ns))[mine] !== undefined);
  });

  test("a namespace larger than the cache syncs within the cache's budget", async () => {
    const ns = "app/u/big";
    const body = { pad: "y".repeat(300) };
    for (let i = 0; i < 40; i++) {
      await host.cmd({ op: "commit", ns, ops: Array.from({ length: 50 }, () => ({ id: randomUuid(), json: JSON.stringify(body) })) });
    }
    const budget = 64 * 1024; // the namespace is ~700 KB of bodies alone
    const r = await replica(ns, { memory: { cacheBytes: budget } });
    await r.syncNow();
    assert.equal(Object.keys(await docsOf(r, ns)).length, 2000);
    assert.ok(r.status().residentBytes <= budget, `resident ${r.status().residentBytes} > ${budget}`);
    assert.equal(await r.namespace(ns).rootHash(), (await host.head(ns)).tree);
  });
});
