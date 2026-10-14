import assert from "node:assert/strict";
import { test } from "node:test";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { utf8 } from "../src/bytes.ts";
import { IndexedDbStore } from "../src/idb.ts";
import { CachedStore, MemoryStore, prefixEnd, type KvStore } from "../src/store.ts";

async function exercise(store: KvStore): Promise<void> {
  await store.apply(new Map([["a/1", utf8("x")], ["a/2", utf8("y")], ["a/3", utf8("z")], ["b/1", utf8("w")]]));
  assert.deepEqual((await store.scan("a/", prefixEnd("a/"))).map(([k]) => k), ["a/1", "a/2", "a/3"]);
  assert.deepEqual((await store.scan("a/2", prefixEnd("a/"), 1)).map(([k]) => k), ["a/2"]);
  await store.apply(new Map([["a/2", null], ["a/4", utf8("v")]]));
  assert.equal(await store.get("a/2"), undefined);
  assert.deepEqual((await store.getMany(["a/1", "a/2", "a/4"])).map((v) => v?.length), [1, undefined, 1]);
  assert.deepEqual((await store.scan("a/", prefixEnd("a/"))).map(([k]) => k), ["a/1", "a/3", "a/4"]);
}

test("the memory store reads, scans and applies in order", async () => {
  await exercise(new MemoryStore());
});

test("the IndexedDB store behaves the same, and keeps data across reopen", async () => {
  const factory = { factory: new IDBFactory(), keyRange: IDBKeyRange };
  const store = await IndexedDbStore.open("replica-test", factory);
  await exercise(store);
  await store.close();
  const again = await IndexedDbStore.open("replica-test", factory);
  assert.deepEqual((await again.scan("a/", prefixEnd("a/"))).map(([k]) => k), ["a/1", "a/3", "a/4"]);
  await again.close();
  await IndexedDbStore.destroy("replica-test", factory);
  const fresh = await IndexedDbStore.open("replica-test", factory);
  assert.deepEqual(await fresh.scan("", "￿"), []);
  await fresh.close();
});

test("the cache never holds more than its budget, and serves repeated reads", async () => {
  const inner = new MemoryStore();
  const batch = new Map<string, Uint8Array>();
  for (let i = 0; i < 2000; i++) batch.set(`k/${i}`, new Uint8Array(500));
  await inner.apply(batch);
  const cached = new CachedStore(inner, 64 * 1024);
  for (let round = 0; round < 3; round++) {
    for (let i = 0; i < 2000; i++) {
      await cached.get(`k/${i}`);
      assert.ok(cached.residentBytes <= 64 * 1024);
    }
  }
  const missesBefore = cached.misses;
  for (let i = 0; i < 20; i++) await cached.get("k/1999");
  assert.equal(cached.misses, missesBefore, "a hot key stays cached");
  await cached.apply(new Map([["k/1999", null]]));
  assert.equal(await cached.get("k/1999"), undefined, "a delete is not served stale from the cache");
});
