import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import {
  KdbConflictError,
  KdbNotFoundError,
  KdbPreconditionError,
  KdbClosedError,
  type KdbOperations,
} from "@kdb/client";
import { openEmbedded, type EmbeddedKdb } from "../src/index.ts";
import { compileEngine } from "../src/engine.ts";

const NS = "app/users";
const uuid = (n: number): string => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;

describe("embedded engine", () => {
  let db: EmbeddedKdb;
  before(async () => {
    db = await openEmbedded({ namespace: NS });
  });
  after(async () => {
    await db.close();
  });

  it("is a KdbOperations, so code written against the interface runs on it", () => {
    const ops: KdbOperations = db;
    assert.equal(typeof ops.putJSON, "function");
  });

  it("round-trips documents, including non-ASCII and nesting", async () => {
    const body = { name: "Zoë 日本語 🚀", nested: { a: [1, 2, { b: null }] }, n: 1.5 };
    const commit = await db.putJSON(NS, uuid(1), body);
    assert.match(commit, /^[0-9a-f]{64}$/);
    const got = await db.get(NS, uuid(1));
    assert.deepEqual(got.body, body);
    assert.equal(got.commit, commit);
  });

  it("reports a missing document as KdbNotFoundError, like a server does", async () => {
    await assert.rejects(db.get(NS, uuid(404)), KdbNotFoundError);
  });

  it("deletes", async () => {
    await db.putJSON(NS, uuid(2), { x: 1 });
    await db.delete(NS, uuid(2));
    await assert.rejects(db.get(NS, uuid(2)), KdbNotFoundError);
  });

  it("enforces conditional-write preconditions", async () => {
    await db.putIfAbsent(NS, uuid(3), { v: 1 });
    await assert.rejects(db.putIfAbsent(NS, uuid(3), { v: 2 }), KdbPreconditionError);
    const { contentHash } = await db.getWithHash(NS, uuid(3));
    await db.replaceIf(NS, uuid(3), { v: 2 }, contentHash);
    await assert.rejects(db.replaceIf(NS, uuid(3), { v: 3 }, contentHash), KdbPreconditionError);
    await db.replaceIfPresent(NS, uuid(3), { v: 4 });
    await assert.rejects(db.replaceIfPresent(NS, uuid(999), { v: 1 }), KdbPreconditionError);
  });

  it("serializes concurrent compareAndSwap without losing an update", async () => {
    await db.putJSON(NS, uuid(4), { n: 0 });
    await Promise.all(
      Array.from({ length: 20 }, () =>
        db.compareAndSwap(NS, uuid(4), (cur) => ({ n: ((cur as { n: number }).n ?? 0) + 1 }), { attempts: 100 }),
      ),
    );
    assert.deepEqual((await db.get(NS, uuid(4))).body, { n: 20 });
  });

  it("commits multi-document transactions atomically, and surfaces a stale baseVersion as a conflict", async () => {
    const { commit: base } = await db.get(NS, uuid(4));
    await db.commit({ namespace: NS, baseVersion: base, writes: [{ docId: uuid(5), body: { a: 1 } }, { docId: uuid(6), body: { b: 2 } }] });
    assert.deepEqual((await db.get(NS, uuid(5))).body, { a: 1 });
    assert.deepEqual((await db.get(NS, uuid(6))).body, { b: 2 });
    // `base` is no longer the head and uuid(5) changed since: a second commit against it must not silently win.
    await db.putJSON(NS, uuid(5), { a: 2 });
    await assert.rejects(
      db.commit({ namespace: NS, baseVersion: base, writes: [{ docId: uuid(5), body: { a: 99 } }] }),
      (e: unknown) => e instanceof KdbConflictError,
    );
    assert.deepEqual((await db.get(NS, uuid(5))).body, { a: 2 });
  });

  it("upserts and appends", async () => {
    await db.upsert(NS, uuid(7), { v: 1 });
    await db.upsert(NS, uuid(7), { v: 2 });
    assert.deepEqual((await db.get(NS, uuid(7))).body, { v: 2 });
  });

  it("serves SQL: DDL, DML with parameters, queries", async () => {
    const sql = await openEmbedded({ namespace: "sql/ns" });
    try {
      await sql.exec("sql/ns", "CREATE TABLE t (name TEXT NOT NULL, n INTEGER)");
      await sql.exec("sql/ns", "INSERT INTO t (name, n) VALUES (?, ?)", ["a", 1]);
      await sql.exec("sql/ns", "INSERT INTO t (name, n) VALUES (?, ?)", ["b", 2]);
      assert.deepEqual(await sql.query("sql/ns", "SELECT name, n FROM t WHERE n > ? ORDER BY n", [0]), [
        { name: "a", n: "1" },
        { name: "b", n: "2" },
      ]);
      assert.deepEqual((await sql.queryRaw("sql/ns", "SELECT 1")).rows, [["1"]]);
      await assert.rejects(sql.queryRaw("sql/ns", "SELECT a FROM 1"), /expected identifier/);
      await assert.rejects(sql.exec("sql/ns", "INSERT INTO t (n) VALUES (1)"), /required/);
      // Still serving after errors.
      assert.equal((await sql.query("sql/ns", "SELECT name FROM t")).length, 2);
    } finally {
      await sql.close();
    }
  });

  it("holds and releases leases like a server", async () => {
    const lease = await db.acquireLock(NS, uuid(8), 60_000);
    assert.equal(lease.docId, uuid(8));
    const renewed = await db.renewLock(NS, uuid(8), 60_000);
    assert.equal(renewed.docId, uuid(8));
    await db.releaseLock(NS, uuid(8));
  });

  it("handles large documents and many concurrent requests", async () => {
    const big = { blob: "x".repeat(2_000_000) };
    await db.putJSON(NS, uuid(9), big);
    assert.equal(((await db.get(NS, uuid(9))).body as typeof big).blob.length, 2_000_000);
    await Promise.all(Array.from({ length: 100 }, (_, i) => db.putJSON(NS, uuid(1000 + i), { i })));
    const bodies = await Promise.all(Array.from({ length: 100 }, (_, i) => db.get(NS, uuid(1000 + i))));
    assert.deepEqual(bodies.map((b) => (b.body as { i: number }).i), Array.from({ length: 100 }, (_, i) => i));
  });

  it("rejects a client-side invalid id without touching the engine", async () => {
    await assert.rejects(db.putJSON(NS, "not-a-uuid", {}), /uuid/i);
  });
});

describe("embedded lifecycle and isolation", () => {
  it("keeps databases isolated from each other", async () => {
    const [a, b] = await Promise.all([openEmbedded({ namespace: NS }), openEmbedded({ namespace: NS })]);
    try {
      await a.putJSON(NS, uuid(1), { who: "a" });
      await assert.rejects(b.get(NS, uuid(1)), KdbNotFoundError);
      await b.putJSON(NS, uuid(1), { who: "b" });
      assert.deepEqual((await a.get(NS, uuid(1))).body, { who: "a" });
      assert.deepEqual((await b.get(NS, uuid(1))).body, { who: "b" });
    } finally {
      await Promise.all([a.close(), b.close()]);
    }
  });

  it("compiles the module once and reuses it", async () => {
    const module = await compileEngine();
    const a = await openEmbedded({ namespace: NS, wasm: module });
    const b = await openEmbedded({ namespace: NS, wasm: module });
    await a.putJSON(NS, uuid(1), { ok: true });
    await a.close();
    await b.close();
    assert.equal(await compileEngine(), module, "default module is cached");
  });

  it("rejects use after close, and close is idempotent", async () => {
    const db = await openEmbedded({ namespace: NS });
    await db.putJSON(NS, uuid(1), { a: 1 });
    await db.close();
    await db.close();
    await assert.rejects(db.get(NS, uuid(1)), KdbClosedError);
  });

  it("refuses a namespace it was not opened with, rather than aliasing it onto another", async () => {
    const db = await openEmbedded({ namespace: NS });
    try {
      await assert.rejects(db.putJSON("other/ns", uuid(1), { a: 1 }), /unknown namespace: other\/ns/);
      // Nothing leaked into the real namespace, and it is still healthy afterwards.
      await assert.rejects(db.get(NS, uuid(1)), KdbNotFoundError);
      await db.putJSON(NS, uuid(1), { a: 1 });
    } finally {
      await db.close();
    }
  });

  it("serves several namespaces as independent stores", async () => {
    const db = await openEmbedded({ namespace: ["a/one", "b/two"] });
    try {
      await db.putJSON("a/one", uuid(1), { in: "one" });
      await assert.rejects(db.get("b/two", uuid(1)), KdbNotFoundError);
      await db.putJSON("b/two", uuid(1), { in: "two" });
      assert.deepEqual((await db.get("a/one", uuid(1))).body, { in: "one" });
      assert.deepEqual((await db.get("b/two", uuid(1))).body, { in: "two" });

      // Each namespace has its own schema, too: a table in one is invisible in the other.
      await db.exec("a/one", "CREATE TABLE t (n INTEGER NOT NULL)");
      await db.exec("a/one", "INSERT INTO t (n) VALUES (1)");
      await db.exec("b/two", "CREATE TABLE t (name TEXT NOT NULL)");
      await db.exec("b/two", "INSERT INTO t (name) VALUES ('x')");
      assert.deepEqual(await db.query("a/one", "SELECT n FROM t WHERE n = 1"), [{ n: "1" }]);
      assert.deepEqual(await db.query("b/two", "SELECT name FROM t WHERE name = 'x'"), [{ name: "x" }]);
    } finally {
      await db.close();
    }
  });

  it("validates its options", async () => {
    await assert.rejects(openEmbedded({ namespace: "" }), /namespace is required/);
    await assert.rejects(openEmbedded({ namespace: [] }), /namespace is required/);
    await assert.rejects(openEmbedded({ namespace: ["a/b", "a/b"] }), /twice/);
    await assert.rejects(openEmbedded({ namespace: NS, wasm: new URL("file:///nonexistent/kdb.wasm") }));
  });
});
