import assert from "node:assert/strict";
import { test } from "node:test";
import { ProjectionNamespace, projectionNamespace } from "../src/projection.ts";
import { MemoryStore } from "../src/store.ts";

const id = (n: number) => `${n.toString(16).padStart(8, "0")}-0000-4000-8000-000000000000`;

async function docs(p: ProjectionNamespace): Promise<Record<string, string>> {
  return Object.fromEntries((await p.scan()).map((d) => [d.id, d.json]));
}

test("the local name is Go's (peersync.ProjectionNamespace)", () => {
  // sha256("region = 'EU'")[:4] in hex, as Go computes it
  assert.match(projectionNamespace("shop/orders", "  region = 'EU' "), /^shop\/orders\.projection-[0-9a-f]{8}$/);
  assert.equal(projectionNamespace("a", "x"), projectionNamespace("a", " x "));
});

test("a reset replaces the projection page by page, removing what each page's range no longer holds", async () => {
  const p = await ProjectionNamespace.open(new MemoryStore(), "ns", "TRUE", true);
  await p.applyPage("", "", { atHex: "a1", reset: false, writes: [1, 2, 3, 4, 5, 6].map((n) => ({ id: id(n), body: `{"n":${n}}` })), deletes: [], next: "", done: true });
  // A reset delivering 2 and 5, in two pages: (start, id(3)] then (id(3), end).
  await p.applyPage("", "", { atHex: "b2", reset: true, writes: [{ id: id(2), body: `{"n":22}` }], deletes: [], next: id(3), done: false });
  assert.deepEqual(Object.keys(await docs(p)), [id(2), id(4), id(5), id(6)], "the first page's range is replaced, the rest untouched yet");
  assert.equal(p.transfer?.after, id(3));
  await p.applyPage("", id(3), { atHex: "b2", reset: true, writes: [{ id: id(5), body: `{"n":55}` }], deletes: [], next: "", done: true });
  assert.deepEqual(await docs(p), { [id(2)]: `{"n":22}`, [id(5)]: `{"n":55}` });
  assert.equal(p.lastSource, "b2");
  assert.equal(p.transfer, undefined);
});

test("a document with a queued write keeps its local state until the source decides", async () => {
  const p = await ProjectionNamespace.open(new MemoryStore(), "ns", "TRUE", true);
  await p.applyPage("", "", { atHex: "a", reset: false, writes: [{ id: id(1), body: `{"v":1}` }], deletes: [], next: "", done: true });
  const e = await p.write([{ docId: id(1), json: `{"v":"mine"}` }], "11111111-1111-4111-8111-111111111111");
  assert.equal(e.docs[0]!.baseHash?.length, 64, "the base is the state the source holds");
  await p.applyPage("a", "", { atHex: "b", reset: false, writes: [{ id: id(1), body: `{"v":"theirs"}` }], deletes: [], next: "", done: true });
  assert.equal(await p.get(id(1)), `{"v":"mine"}`);
  await p.applyPage("b", "", { atHex: "c", reset: true, writes: [], deletes: [], next: "", done: true });
  assert.equal(await p.get(id(1)), `{"v":"mine"}`, "not even a reset removes it");
  await p.resolve(e, [{ docId: id(1), body: `{"v":"theirs"}` }]);
  assert.equal(await p.get(id(1)), `{"v":"theirs"}`);
  assert.equal(await p.outboxSize(), 0);
  await p.applyPage("c", "", { atHex: "d", reset: false, writes: [], deletes: [id(1)], next: "", done: true });
  assert.equal(await p.get(id(1)), undefined, "once decided, pages apply again");
});

test("the outbox survives reopening, in order", async () => {
  const store = new MemoryStore();
  const p = await ProjectionNamespace.open(store, "ns", "TRUE", true);
  for (let i = 0; i < 12; i++) await p.write([{ docId: id(i), json: `{"i":${i}}` }], `00000000-0000-4000-8000-${i.toString().padStart(12, "0")}`);
  const again = await ProjectionNamespace.open(store, "ns", "TRUE", true);
  assert.deepEqual((await again.outbox()).map((e) => e.docs[0]!.docId), Array.from({ length: 12 }, (_, i) => id(i)));
});

test("a document written twice in one write: the second's base is the first's result", async () => {
  const p = await ProjectionNamespace.open(new MemoryStore(), "ns", "TRUE", true);
  const e = await p.write([{ docId: id(1), json: `{"v":1}` }, { docId: id(1), json: `{"v":2}` }], "22222222-2222-4222-8222-222222222222");
  const { contentHash } = await import("../src/commit.ts");
  assert.equal(e.docs[0]!.baseHash, undefined, "a create");
  assert.equal(e.docs[1]!.baseHash, contentHash(id(1), `{"v":1}`));
  assert.equal(await p.get(id(1)), `{"v":2}`);
});
