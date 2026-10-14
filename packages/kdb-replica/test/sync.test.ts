/**
 * Live interop: a TS replica syncing with a real Go node (go/cmd/kdb-replicahost) over WebSocket,
 * with the same peer sync v2 messages Go nodes use. Skipped when there is no Go toolchain.
 */

import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import { attachmentContentHash } from "../src/attachments.ts";
import { randomUuid, utf8 } from "../src/bytes.ts";
import { PeerConnection } from "../src/peer.ts";
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

  test("fetched attachment bytes are a bounded cache: least recently used are evicted and fetched again; pinned bytes stay", async () => {
    const ns = "app/u/evict";
    const MiB = 1024 * 1024;
    const docs = [randomUuid(), randomUuid(), randomUuid()];
    const texts = docs.map((_, i) => String.fromCharCode(97 + i).repeat(3 * MiB)); // 3 MiB each, distinct
    for (const [i, id] of docs.entries()) {
      await host.put(ns, id, { i });
      await host.cmd({ op: "attach", ns, id, name: "f.txt", mime: "text/plain", text: texts[i] });
    }
    const r = await replica(ns, { blobCacheBytes: 7 * MiB });
    await r.syncNow();
    const read = async (i: number) => new TextDecoder().decode(await r.readAttachment(ns, docs[i]!, { name: "f.txt" }));
    assert.equal(await read(0), texts[0]);
    assert.equal(await read(1), texts[1]);
    assert.equal((await r.blobCache()).bytes, 6 * MiB);
    assert.equal(await read(2), texts[2]); // 9 MiB fetched: the least recently used goes
    let stats = await r.blobCache();
    assert.ok(stats.bytes <= 7 * MiB, `cache holds ${stats.bytes} bytes, over its bound`);
    assert.equal(stats.blobs, 2);
    assert.equal(await read(0), texts[0], "an evicted blob is fetched again, verified, on read");

    // Bytes written here and not yet uploaded are never evicted; once the host holds them they are.
    const mine = new Uint8Array(2 * MiB).fill(7);
    const local = await replica(ns, { blobCacheBytes: 7 * MiB, autoSync: false });
    await local.syncNow();
    await local.putAttachment(ns, docs[0]!, "mine.bin", mine);
    assert.equal(await local.evictBlobs(0), 0);
    assert.equal((await local.blobCache()).pinnedBytes, 2 * MiB);
    await local.syncNow(); // uploads, then unpins
    stats = await local.blobCache();
    assert.equal(stats.pinnedBytes, 0);
    assert.equal(await local.evictBlobs(0), 2 * MiB);
    assert.equal((await local.blobCache()).bytes, 0);
    assert.deepEqual(await local.readAttachment(ns, docs[0]!, { name: "mine.bin" }), mine, "fetched back from the host");
  });

  test("a diff convergence takes the attachment table by TREE_NODES, each set checked against its hash", async () => {
    const ns = "app/u/att-converge";
    const docs = Array.from({ length: 6 }, () => randomUuid());
    for (const d of docs) await host.put(ns, d, { d });
    await host.cmd({ op: "attach", ns, id: docs[0], name: "keep.txt", mime: "text/plain", text: "kept" });
    await host.cmd({ op: "attach", ns, id: docs[1], name: "gone.txt", mime: "text/plain", text: "removed later" });
    const r = await replica(ns);
    await r.syncNow();
    await host.cmd({ op: "detach", ns, id: docs[1], name: "gone.txt" });
    await host.cmd({ op: "attach", ns, id: docs[0], name: "keep.txt", mime: "text/plain", text: "replaced" });
    for (const [i, d] of docs.slice(2).entries()) await host.cmd({ op: "attach", ns, id: d, name: `n${i}.txt`, mime: "text/plain", text: `new ${i}` });
    // Started by hand, as the repair after an interrupted snapshot starts one: the host refuses to
    // squash a namespace with attachments, so no ordinary sync takes this route.
    const conn = await PeerConnection.open(host.url, { nodeId: randomUuid(), token: "rw-token", namespaces: [ns] });
    const target = (await conn.snapshot(ns, (await host.head(ns)).head, "", 1024)).commit;
    await conn.close();
    await r.namespace(ns).beginConverge(target, "diff");
    await r.syncNow();
    assert.equal(r.namespace(ns).head, (await host.head(ns)).head);
    for (const d of docs) {
      const atHost = ((await host.cmd({ op: "attachments", ns, id: d })).attachments as { name: string; hash: string }[])
        .map((a) => [a.name, a.hash]).sort();
      assert.deepEqual((await r.attachments(ns, d)).map((a) => [a.name, a.contentHash]).sort(), atHost, `document ${d}`);
    }
    assert.equal(new TextDecoder().decode(await r.readAttachment(ns, docs[0]!, { name: "keep.txt" })), "replaced");
  });

  test("attachment refs replay with their commits, and every attachment root is verified", async () => {
    const ns = "app/u/attach";
    const doc = randomUuid(), other = randomUuid();
    await host.put(ns, doc, { title: "with files" });
    await host.put(ns, other, { title: "other" });
    const r = await replica(ns);
    await r.syncNow(); // a snapshot of a namespace with no attachments yet
    await host.cmd({ op: "attach", ns, id: doc, name: "a.txt", mime: "text/plain", text: "hello" });
    await host.cmd({ op: "attach", ns, id: doc, name: "b.txt", mime: "text/plain", text: "world" });
    await host.cmd({ op: "attach", ns, id: other, name: "c.txt", mime: "text/plain", text: "!" });
    await host.cmd({ op: "detach", ns, id: doc, name: "b.txt" });
    const [res] = await r.syncNow();
    assert.equal(res!.route, "replay");
    const atts = await r.attachments(ns, doc);
    assert.deepEqual(atts.map((a) => [a.name, a.mime, a.size]), [["a.txt", "text/plain", 5n]]);
    assert.equal(atts[0]!.contentHash, attachmentContentHash(utf8("hello")));
    // Bytes are lazy: the first read fetches them by BLOB_FETCH, verified chunk by chunk.
    assert.equal(new TextDecoder().decode(await r.readAttachment(ns, doc, { name: "a.txt" })), "hello");
    assert.equal((await r.attachments(ns, other)).length, 1);
    // A document delete takes its attachments with it, in Go and here.
    await host.cmd({ op: "commit", ns, ops: [{ id: other, delete: true }] });
    await r.syncNow();
    assert.deepEqual(await r.attachments(ns, other), []);
    assert.equal(r.namespace(ns).head, (await host.head(ns)).head);
  });

  test("a snapshot bootstrap takes the attachment table; reads fetch large blobs in verified chunks", async () => {
    const ns = "app/u/attach-boot";
    const doc = randomUuid();
    await host.put(ns, doc, { title: "big" });
    const big = "0123456789abcdef".repeat(200_000); // 3.2 MB: four chunks, the last short
    await host.cmd({ op: "attach", ns, id: doc, name: "big.bin", mime: "application/octet-stream", text: big });
    await host.cmd({ op: "attach", ns, id: doc, name: "small.txt", mime: "text/plain", text: "tiny" });
    const r = await replica(ns);
    const [res] = await r.syncNow();
    assert.equal(res!.route, "snapshot");
    const head = await r.namespace(ns).headCommit();
    assert.ok(head?.attachmentRoot, "the snapshot commit has attachments");
    assert.equal(await r.namespace(ns).attachmentRoot(), head!.attachmentRoot);
    assert.deepEqual((await r.attachments(ns, doc)).map((a) => a.name).sort(), ["big.bin", "small.txt"]);
    const bytes = await r.readAttachment(ns, doc, { name: "big.bin" });
    assert.equal(bytes.length, big.length);
    assert.equal(new TextDecoder().decode(bytes), big);
  });

  test("an attachment written on the replica uploads before its commit, and the host holds it", async () => {
    const ns = "app/u/attach-up";
    const doc = randomUuid();
    await host.put(ns, doc, { title: "upload" });
    const r = await replica(ns);
    await r.syncNow();
    const data = utf8("from the replica ".repeat(100_000)); // 1.7 MB: two chunks
    const local = await r.putAttachment(ns, doc, "note.txt", data, "text/plain");
    const [res] = await r.syncNow();
    assert.equal(res!.pushOutcome, "fast-forwarded");
    assert.equal((await host.head(ns)).head, local);
    const listed = (await host.cmd({ op: "attachments", ns, id: doc })).attachments as { name: string; hash: string; size: number }[];
    assert.deepEqual(listed.map((a) => [a.name, a.hash, a.size]), [["note.txt", attachmentContentHash(data), data.length]]);
    // A second replica reads it from the host.
    const other = await replica(ns);
    await other.syncNow();
    assert.deepEqual(await other.readAttachment(ns, doc, { name: "note.txt" }), data);
    // And deletes travel the same way.
    await r.deleteAttachment(ns, doc, { name: "note.txt" });
    await r.syncNow();
    assert.deepEqual((await host.cmd({ op: "attachments", ns, id: doc })).attachments, []);
  });

  test("concurrent attachment and document writes merge with both", async () => {
    const ns = "app/u/attach-merge";
    const doc = randomUuid();
    await host.put(ns, doc, { title: "base" });
    const r = await replica(ns);
    await r.syncNow();
    await host.cmd({ op: "attach", ns, id: doc, name: "a.txt", mime: "text/plain", text: "hello" });
    await r.syncNow();
    for (let round = 0; round < 4; round++) { // both parent orders, whichever hashes first
      await r.put(ns, doc, { title: `edited ${round}` });
      await host.cmd({ op: "attach", ns, id: doc, name: `d${round}.txt`, mime: "text/plain", text: `merged ${round}` });
      const [merged] = await r.syncNow();
      assert.equal(merged!.pushOutcome, "merged");
      assert.equal(r.namespace(ns).head, (await host.head(ns)).head);
    }
    assert.deepEqual((await r.attachments(ns, doc)).map((a) => a.name).sort(), ["a.txt", "d0.txt", "d1.txt", "d2.txt", "d3.txt"]);
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
