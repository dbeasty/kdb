import assert from "node:assert/strict";
import { test } from "node:test";
import { MemoryStore } from "../src/store.ts";
import { TrieEdit, TrieReader } from "../src/trie.ts";
import { load, type TreeEditFixture, type TreeFixture } from "./golden.ts";

for (const f of load<TreeFixture[]>("trees.json")) {
  test(`tree ${f.name}: built entry by entry, the root is Go's`, async () => {
    const store = new MemoryStore();
    const edit = new TrieEdit(store, "ns");
    for (const e of f.entries) await edit.set(e.id, e.contentHash);
    assert.equal(await edit.finish(), f.root);
    const batch = new Map();
    edit.writeTo(batch);
    await store.apply(batch);
    const reader = new TrieReader(store, "ns");
    assert.equal(await reader.rootHash(), f.root);
    const seen: string[] = [];
    await reader.walk((id) => void seen.push(id));
    assert.deepEqual(seen, f.entries.map((e) => e.id), "walk visits ids in order");
    for (const e of f.entries.slice(0, 50)) assert.equal(await reader.lookup(e.id), e.contentHash);
  });
}

for (const f of load<TreeEditFixture[]>("tree-edits.json")) {
  test(`tree edits ${f.name}: every step's root is Go's, persisted between steps`, async () => {
    const store = new MemoryStore();
    for (const [i, step] of f.steps.entries()) {
      const edit = new TrieEdit(store, "ns");
      for (const e of step.edits) {
        if (e.contentHash) await edit.set(e.id, e.contentHash);
        else await edit.delete(e.id);
      }
      assert.equal(await edit.finish(), step.root, `step ${i}`);
      const batch = new Map();
      edit.writeTo(batch);
      await store.apply(batch);
      assert.equal(await new TrieReader(store, "ns").rootHash(), step.root, `step ${i} reread`);
    }
  });
}

test("deleting everything leaves no records behind", async () => {
  const f = load<TreeFixture[]>("trees.json").find((t) => t.name === "random300")!;
  const store = new MemoryStore();
  let edit = new TrieEdit(store, "ns");
  for (const e of f.entries) await edit.set(e.id, e.contentHash);
  await edit.finish();
  let batch = new Map();
  edit.writeTo(batch);
  await store.apply(batch);
  edit = new TrieEdit(store, "ns");
  for (const e of f.entries) await edit.delete(e.id);
  assert.equal(await edit.finish(), "0".repeat(64));
  batch = new Map();
  edit.writeTo(batch);
  await store.apply(batch);
  assert.deepEqual(await store.scan("t/", "t/￿"), []);
});
