/**
 * Live interop for filtered namespaces (plan §4.2, §7): projections of a Go node's namespace through
 * a filter and the token's read rights, with write-back, conflicts and single-home namespaces.
 */

import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import { randomUuid } from "../src/bytes.ts";
import { openReplica, type ConflictEvent, type Replica, type ReplicaOptions } from "../src/replica.ts";
import { MemoryStore } from "../src/store.ts";
import { goAvailable, startHost, type Host } from "./host.ts";

const skip = !goAvailable() && "no Go toolchain";
const EU = "region = 'EU'";

async function docsOf(r: Replica, ns: string): Promise<Record<string, string>> {
  const out: Record<string, string> = {};
  for (const d of await r.scan(ns)) out[d.id] = d.json;
  return out;
}

function onlyEU(docs: Record<string, string>, unreadable: string[] = []): Record<string, string> {
  return Object.fromEntries(
    Object.entries(docs).filter(([id, j]) => (JSON.parse(j) as { region?: string }).region === "EU" && !unreadable.includes(id)),
  );
}

describe("filtered replica against a Go node", { skip }, () => {
  let host: Host;
  const opened: Replica[] = [];
  before(async () => {
    host = await startHost();
  });
  after(async () => {
    for (const r of opened) await r.close();
    await host?.stop();
  });

  async function filtered(ns: string, extra: Partial<ReplicaOptions> & { writeBack?: boolean; filter?: string } = {}): Promise<Replica> {
    const r = await openReplica({
      store: new MemoryStore(),
      peer: { url: host.url, token: "rw-token" },
      namespaces: [{ ns, mode: "filtered", filter: extra.filter ?? EU, writeBack: extra.writeBack ?? true }],
      autoSync: false,
      ...extra,
    });
    opened.push(r);
    return r;
  }

  test("holds exactly the matching documents the token may read, and follows them out", async () => {
    const ns = "shop/orders";
    const ids = Array.from({ length: 12 }, () => randomUuid());
    for (const [i, id] of ids.entries()) await host.put(ns, id, { region: i % 3 === 0 ? "US" : "EU", i });
    const secret = ids[1]!;
    await host.cmd({ op: "denyRead", ns, id: secret });
    const r = await filtered(ns, { pageBytes: 64 }); // a page per document or so
    await r.syncNow();
    assert.deepEqual(await docsOf(r, ns), onlyEU(await host.docs(ns), [secret]));
    assert.equal(await r.get(ns, secret), undefined, "a document the token may not read never arrives");

    // Out of the filter, deleted at the source, and losing read access: each leaves the replica.
    await host.put(ns, ids[2]!, { region: "US", moved: true });
    await host.cmd({ op: "commit", ns, ops: [{ id: ids[4]!, delete: true }] });
    await host.cmd({ op: "denyRead", ns, id: ids[5]! });
    await host.put(ns, ids[5]!, { region: "EU", changed: true });
    const id = randomUuid();
    await host.put(ns, id, { region: "EU", fresh: true });
    await r.syncNow();
    const res = r.lastProjectionResults[0]!;
    assert.equal(res.reset, false, "a catch-up is a delta, not a reset");
    assert.deepEqual(await docsOf(r, ns), onlyEU(await host.docs(ns), [secret, ids[5]!]));
    assert.equal(await r.get(ns, ids[2]!), undefined);
    assert.equal(await r.get(ns, ids[4]!), undefined);
    assert.equal(await r.get(ns, ids[5]!), undefined);
    assert.equal(await r.get(ns, id), `{"region":"EU","fresh":true}`);
  });

  test("an id-range filter holds one slice of the namespace", async () => {
    const ns = "shop/slices";
    const ids = Array.from({ length: 40 }, () => randomUuid()).sort();
    for (const i of ids) await host.put(ns, i, { region: "EU" });
    const lo = ids[10]!, hi = ids[30]!;
    const r = await filtered(ns, { filter: `kdb_id >= '${lo}' AND kdb_id < '${hi}'`, writeBack: false });
    await r.syncNow();
    assert.deepEqual(Object.keys(await docsOf(r, ns)).sort(), ids.slice(10, 30));
  });

  test("a filtered namespace holds its documents' attachment refs and reads their bytes", async () => {
    const ns = "shop/attached";
    const eu = randomUuid(), us = randomUuid();
    await host.put(ns, eu, { region: "EU" });
    await host.put(ns, us, { region: "US" });
    await host.cmd({ op: "attach", ns, id: eu, name: "invoice.pdf", mime: "application/pdf", text: "%PDF eu" });
    await host.cmd({ op: "attach", ns, id: us, name: "invoice.pdf", mime: "application/pdf", text: "%PDF us" });
    const r = await filtered(ns, { writeBack: false });
    await r.syncNow();
    assert.deepEqual((await r.attachments(ns, eu)).map((a) => a.name), ["invoice.pdf"]);
    assert.deepEqual(await r.attachments(ns, us), [], "a document outside the filter brings no refs");
    assert.equal(new TextDecoder().decode(await r.readAttachment(ns, eu, { name: "invoice.pdf" })), "%PDF eu");
    // A document leaving the filter takes its refs with it.
    await host.put(ns, eu, { region: "US" });
    await r.syncNow();
    assert.deepEqual(await r.attachments(ns, eu), []);
  });

  test("an interrupted transfer resumes where it stopped", async () => {
    const ns = "shop/resume";
    for (let i = 0; i < 30; i++) await host.put(ns, randomUuid(), { region: "EU", i, pad: "z".repeat(50) });
    const store = new MemoryStore();
    const r = await filtered(ns, { store, pageBytes: 200 });
    let applies = 0;
    const real = store.apply.bind(store);
    store.apply = async (b) => {
      if (++applies === 4) throw new Error("disk full");
      return real(b);
    };
    await assert.rejects(r.syncNow(), /disk full/);
    const midway = r.projection(ns).transfer;
    assert.ok(midway && midway.after !== "", "the transfer's cursor was kept");
    await r.syncNow();
    assert.equal(r.projection(ns).transfer, undefined);
    assert.deepEqual(await docsOf(r, ns), onlyEU(await host.docs(ns)));
  });

  test("offline writes queue, apply at the source on reconnect, and come back unchanged", async () => {
    const ns = "shop/writes";
    const existing = randomUuid();
    await host.put(ns, existing, { region: "EU", qty: 1 });
    const r = await filtered(ns);
    await r.syncNow();
    const created = randomUuid();
    await r.put(ns, existing, { region: "EU", qty: 2 });
    await r.put(ns, created, { region: "EU", qty: 9 });
    await r.put(ns, existing, { region: "EU", qty: 3 }); // a second write on top of the first
    assert.equal(r.status().namespaces[ns]!.pending, 3);
    await r.syncNow();
    assert.equal(r.lastProjectionResults[0]!.applied, 3);
    assert.equal(r.status().namespaces[ns]!.pending, 0);
    const hostDocs = await host.docs(ns);
    assert.equal(hostDocs[existing], `{"region":"EU","qty":3}`);
    assert.equal(hostDocs[created], `{"region":"EU","qty":9}`);
    await r.syncNow();
    assert.deepEqual(await docsOf(r, ns), onlyEU(hostDocs));
  });

  test("a write the source changed meanwhile is a conflict: the source's state comes back, and the app hears", async () => {
    const ns = "shop/conflict";
    const id = randomUuid();
    await host.put(ns, id, { region: "EU", owner: "nobody" });
    const r = await filtered(ns);
    await r.syncNow();
    const events: ConflictEvent[] = [];
    r.on("conflict", (e) => events.push(e));
    await r.put(ns, id, { region: "EU", owner: "replica" });
    await host.put(ns, id, { region: "EU", owner: "host" });
    await r.syncNow();
    assert.equal(await r.get(ns, id), `{"region":"EU","owner":"host"}`);
    assert.equal(events.length, 1);
    assert.equal(events[0]!.outcome, "conflict");
    assert.equal(events[0]!.report?.conflicts[0]?.localDoc, `{"region":"EU","owner":"replica"}`);
    assert.equal(events[0]!.report?.conflicts[0]?.incomingDoc, `{"region":"EU","owner":"host"}`);
    assert.equal((await host.docs(ns))[id], `{"region":"EU","owner":"host"}`);
  });

  test("a single-home namespace: writes wait while the home is elsewhere, and land once it is reachable", async () => {
    const ns = "shop/homed";
    await host.put(ns, randomUuid(), { region: "EU" });
    const r = await filtered(ns);
    await r.syncNow();
    await host.cmd({ op: "setHome", ns });
    const id = randomUuid();
    await r.put(ns, id, { region: "EU", queued: true });
    await assert.rejects(r.syncNow(), /deferred.*NOT_HOME/);
    assert.equal(r.status().namespaces[ns]!.pending, 1, "the write stays queued");
    assert.equal(await r.get(ns, id), `{"region":"EU","queued":true}`, "and visible locally");
    await host.cmd({ op: "clearHome", ns });
    await r.syncNow();
    assert.equal(r.status().namespaces[ns]!.pending, 0);
    assert.equal((await host.docs(ns))[id], `{"region":"EU","queued":true}`);
  });

  test("a full replica of a single-home namespace is told to hold it filtered", async () => {
    const ns = "shop/homed-full";
    await host.put(ns, randomUuid(), { region: "EU" });
    const r = await openReplica({ store: new MemoryStore(), peer: { url: host.url, token: "rw-token" }, namespaces: [{ ns }], autoSync: false });
    opened.push(r);
    await r.syncNow();
    await host.cmd({ op: "setHome", ns });
    await r.put(ns, randomUuid(), { region: "EU" });
    await assert.rejects(r.syncNow(), /single-home.*filtered/);
    await host.cmd({ op: "clearHome", ns });
  });

  test("a read-only filtered namespace refuses local writes; a refused write-back is undone", async () => {
    const ns = "shop/ro";
    const id = randomUuid();
    await host.put(ns, id, { region: "EU", v: 1 });
    const ro = await filtered(ns, { writeBack: false });
    await ro.syncNow();
    await assert.rejects(ro.put(ns, id, { region: "EU", v: 2 }), /read-only/);

    const wb = await filtered(ns, { peer: { url: host.url, token: "ro-token" } });
    await wb.syncNow();
    const events: ConflictEvent[] = [];
    wb.on("conflict", (e) => events.push(e));
    await wb.put(ns, id, { region: "EU", v: 3 });
    await wb.syncNow();
    assert.equal(events[0]?.outcome, "refused");
    assert.equal(await wb.get(ns, id), `{"region":"EU","v":1}`, "the source's state is back");
    assert.equal(wb.status().namespaces[ns]!.pending, 0);
  });
});
