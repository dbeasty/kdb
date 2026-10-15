/**
 * Durability of `openEmbedded({ persist })`: the real file-backed Go engine running on the
 * persistent filesystem. "Reload" is simply opening the same backend again; "crash" is opening a
 * snapshot of the backend taken while the first database was still running and never closed.
 */

import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { KdbNotFoundError } from "@limidus/kdb-client";
import { MemoryBackend, openEmbedded } from "../src/index.ts";

const NS = "app/users";
const uuid = (n: number): string => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
let names = 0;
const fresh = () => ({ name: `db-${++names}`, backend: new MemoryBackend() });

describe("persistent embedded database", () => {
  it("is empty the first time, and keeps documents, SQL schema and rows across a close and reopen", async () => {
    const { name, backend } = fresh();
    let db = await openEmbedded({ namespace: NS, persist: { name, backend } });
    await assert.rejects(db.get(NS, uuid(1)), KdbNotFoundError);
    await db.putJSON(NS, uuid(1), { name: "ada", n: 1 });
    await db.putJSON(NS, uuid(2), { name: "grace" });
    await db.delete(NS, uuid(2));
    await db.exec(NS, "CREATE TABLE users (name TEXT NOT NULL, n INTEGER)");
    await db.exec(NS, "INSERT INTO users (name, n) VALUES (?, ?)", ["bob", 7]);
    await db.close();

    db = await openEmbedded({ namespace: NS, persist: { name, backend } });
    assert.deepEqual((await db.get(NS, uuid(1))).body, { name: "ada", n: 1 });
    await assert.rejects(db.get(NS, uuid(2)), KdbNotFoundError);
    assert.deepEqual(await db.query(NS, "SELECT name, n FROM users WHERE n = 7"), [{ name: "bob", n: "7" }]);
    // The rows survived, but a CREATE TABLE schema does not: the engine (the Go service too) opens every
    // namespace with no schema, so a persistent app re-applies it on start. Pinned so it cannot change
    // unnoticed.
    await db.exec(NS, "INSERT INTO users (n) VALUES (555)"); // accepted: no constraint is installed yet
    await db.exec(NS, "DELETE FROM users WHERE n = 555");
    await db.exec(NS, "CREATE TABLE users (name TEXT NOT NULL, n INTEGER)"); // re-install it
    await assert.rejects(db.exec(NS, "INSERT INTO users (n) VALUES (555)"), /required/);
    // And it keeps working: new writes after a reopen land and persist again.
    await db.putJSON(NS, uuid(3), { name: "third" });
    await db.close();
    db = await openEmbedded({ namespace: NS, persist: { name, backend } });
    assert.deepEqual((await db.get(NS, uuid(3))).body, { name: "third" });
    assert.deepEqual((await db.get(NS, uuid(1))).body, { name: "ada", n: 1 });
    await db.close();
  });

  it("keeps each namespace's data separate across a reopen", async () => {
    const { name, backend } = fresh();
    const namespace = ["a/one", "b/two"];
    let db = await openEmbedded({ namespace, persist: { name, backend } });
    await db.putJSON("a/one", uuid(1), { in: "one" });
    await db.putJSON("b/two", uuid(1), { in: "two" });
    await db.close();
    db = await openEmbedded({ namespace, persist: { name, backend } });
    assert.deepEqual((await db.get("a/one", uuid(1))).body, { in: "one" });
    assert.deepEqual((await db.get("b/two", uuid(1))).body, { in: "two" });
    await db.close();
  });

  it("persists large documents that span many storage chunks", async () => {
    const { name, backend } = fresh();
    const blob = Array.from({ length: 300_000 }, (_, i) => String.fromCharCode(97 + (i % 26))).join("");
    let db = await openEmbedded({ namespace: NS, persist: { name, backend } });
    await db.putJSON(NS, uuid(1), { blob });
    await db.close();
    db = await openEmbedded({ namespace: NS, persist: { name, backend } });
    assert.equal(((await db.get(NS, uuid(1))).body as { blob: string }).blob, blob);
    await db.close();
  });

  it("an acknowledged commit is already in the backend: a crash right after loses nothing", async () => {
    const { name, backend } = fresh();
    const db = await openEmbedded({ namespace: NS, persist: { name, backend } });
    const acked: number[] = [];
    for (let i = 1; i <= 25; i++) {
      await db.putJSON(NS, uuid(i), { i });
      acked.push(i);
      if (i % 8 === 0) {
        // Snapshot the "disk" at this instant, with the database still running and never closed.
        const crashed = backend.clone();
        const recovered = await openEmbedded({ namespace: NS, persist: { name: `${name}-crash-${i}`, backend: crashed } });
        for (const n of acked) assert.deepEqual((await recovered.get(NS, uuid(n))).body, { i: n }, `commit ${n} lost after crash at ${i}`);
        await recovered.close();
      }
    }
    await db.close();
  });

  it("recovers a crash in the middle of concurrent writes to a consistent prefix", async () => {
    const { name, backend } = fresh();
    const db = await openEmbedded({ namespace: NS, persist: { name, backend } });
    const writes = Array.from({ length: 40 }, (_, i) => db.putJSON(NS, uuid(100 + i), { i }));
    await new Promise((r) => setTimeout(r, 15));
    const crashed = backend.clone();
    await Promise.all(writes);
    const recovered = await openEmbedded({ namespace: NS, persist: { name: `${name}-crash`, backend: crashed } });
    // Whatever subset made it must be intact documents, never torn ones.
    let found = 0;
    for (let i = 0; i < 40; i++) {
      try {
        assert.deepEqual((await recovered.get(NS, uuid(100 + i))).body, { i });
        found++;
      } catch (error) {
        assert.ok(error instanceof KdbNotFoundError, `unexpected failure for ${i}: ${error}`);
      }
    }
    assert.ok(found <= 40);
    // And the recovered database is fully usable.
    await recovered.putJSON(NS, uuid(999), { after: "crash" });
    assert.deepEqual((await recovered.get(NS, uuid(999))).body, { after: "crash" });
    await recovered.close();
    await db.close();
  });

  it("never persists the engine's lock file, so a crashed database reopens", async () => {
    const { name, backend } = fresh();
    const db = await openEmbedded({ namespace: NS, persist: { name, backend } });
    await db.putJSON(NS, uuid(1), { a: 1 });
    const crashed = backend.clone();
    for (const key of (await crashed.loadAll()).keys()) assert.ok(!key.includes(".lock"), `lock persisted: ${key}`);
    const recovered = await openEmbedded({ namespace: NS, persist: { name: `${name}-2`, backend: crashed } });
    assert.deepEqual((await recovered.get(NS, uuid(1))).body, { a: 1 });
    await recovered.close();
    await db.close();
  });

  it("refuses to open a name twice at once, and allows it again after close", async () => {
    const { name, backend } = fresh();
    const first = await openEmbedded({ namespace: NS, persist: { name, backend } });
    await assert.rejects(
      openEmbedded({ namespace: NS, persist: { name, backend } }),
      /already open/,
    );
    // A different name is independent.
    const other = await openEmbedded({ namespace: NS, persist: { name: `${name}-other`, backend: new MemoryBackend() } });
    await other.close();
    await first.close();
    const again = await openEmbedded({ namespace: NS, persist: { name, backend } });
    await again.close();
  });

  it("surfaces a storage failure instead of acknowledging a commit that is not durable", async () => {
    class Quota extends MemoryBackend {
      full = false;
      override async apply(puts: Map<string, Uint8Array>, deletes: string[]): Promise<void> {
        if (this.full) throw new Error("QuotaExceededError");
        return super.apply(puts, deletes);
      }
    }
    const backend = new Quota();
    const db = await openEmbedded({ namespace: NS, persist: { name: "quota", backend } });
    await db.putJSON(NS, uuid(1), { ok: true });
    backend.full = true;
    await assert.rejects(db.putJSON(NS, uuid(2), { ok: false }));
    await db.close().catch(() => undefined);
  });

  it("does not leave a memory-only database persisted, and validates its options", async () => {
    const backend = new MemoryBackend();
    const memory = await openEmbedded({ namespace: NS });
    await memory.putJSON(NS, uuid(1), { a: 1 });
    await memory.close();
    assert.equal(backend.size, 0);
    await assert.rejects(openEmbedded({ namespace: NS, persist: { name: "", backend } }), /persist.name is required/);
    await assert.rejects(openEmbedded({ namespace: NS, persist: { name: "x" } }), /no indexedDB/);
  });
});
