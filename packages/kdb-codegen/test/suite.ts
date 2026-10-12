/**
 * The end-to-end suite for the generated, typed layer. It takes any KdbOperations - a network
 * client talking to the Go server, or the embedded WASM engine - so one body of assertions holds
 * both to the same behaviour. That is the point of embedding the same engine behind the same
 * interface (Component 63 §11.3).
 */

import assert from "node:assert/strict";
import { describe, it, before, after } from "node:test";
import type { KdbOperations } from "@kdb/client";
import { bind } from "./generated/kdb.ts";

export interface Target {
  ops: KdbOperations;
  close(): Promise<void>;
}

const uuid = (n: number): string => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;

export function typedLayerSuite(label: string, open: () => Promise<Target>, options: { skip?: string | false } = {}): void {
  describe(`generated types + SQL builder against ${label}`, { skip: options.skip ?? false }, () => {
    let target: Target;
    let db: ReturnType<typeof bind>;

    before(async () => {
      target = await open();
      db = bind(target.ops);
      await db.users.create();
      const rows = [
        { name: "ann", age: 30, balance: 9007199254740993n, score: 1.5, active: true, joined: "2024-01-02T03:04:05Z", nickname: "annie", tier: "pro" as const },
        { name: "bob", age: 25, balance: 5n, score: 2.5, active: false, tier: "free" as const },
        { name: "cy", age: 40, active: true },
        { name: "dee", score: 4, active: true, nickname: "d" },
      ];
      for (const r of rows) await db.users.insert(r).execute();
    });

    after(async () => {
      await target.close();
    });

    it("returns real types, not strings", async () => {
      const [ann] = await db.users.select("name", "age", "balance", "score", "active", "joined", "tier").where("name", "=", "ann").execute();
      assert.deepEqual(ann, {
        name: "ann", age: 30, balance: 9007199254740993n, score: 1.5, active: true,
        joined: "2024-01-02T03:04:05Z", tier: "pro",
      });
      assert.equal(typeof ann!.age, "number");
      assert.equal(typeof ann!.balance, "bigint");
    });

    it("keeps a BIGINT above 2^53 exact, in a result and as a filter", async () => {
      const exact = await db.users.select("name").where("balance", "=", 9007199254740993n).execute();
      assert.deepEqual(exact, [{ name: "ann" }]);
      // The neighbour that Number() would have collapsed it onto must NOT match.
      const neighbour = await db.users.select("name").where("balance", "=", 9007199254740992n).execute();
      assert.deepEqual(neighbour, []);
    });

    it("tells false from absent for booleans, and absent from zero for numbers", async () => {
      const rows = await db.users.select("name", "active", "age").orderBy("name").execute();
      assert.deepEqual(rows.map((r) => [r.name, r.active, r.age]), [
        ["ann", true, 30],
        ["bob", false, 25],
        ["cy", true, 40],
        ["dee", true, null],
      ]);
    });

    it("reads an absent optional string as the empty string", async () => {
      const rows = await db.users.select("name", "nickname").orderBy("name").execute();
      assert.deepEqual(rows.map((r) => r.nickname), ["annie", "", "", "d"]);
    });

    it("filters, orders, limits and offsets", async () => {
      const older = await db.users.select("name", "age").where("age", ">", 26).orderBy("age", "desc").limit(2).execute();
      assert.deepEqual(older, [{ name: "cy", age: 40 }, { name: "ann", age: 30 }]);

      const page = await db.users.select("name").orderBy("name").limit(2).offset(1).execute();
      assert.deepEqual(page, [{ name: "bob" }, { name: "cy" }]);

      const grouped = await db.users
        .select("name")
        .where((eb) => eb.and(eb.or(eb.cmp("age", "=", 25), eb.cmp("age", "=", 30)), eb.cmp("active", "=", true)))
        .execute();
      assert.deepEqual(grouped, [{ name: "ann" }]);

      assert.deepEqual(await db.users.select("name").where((eb) => eb.in("age", [25, 30])).orderBy("name").execute(), [{ name: "ann" }, { name: "bob" }]);
      assert.deepEqual(await db.users.select("name").where("name", "like", "%e%").execute(), [{ name: "dee" }]);
      assert.deepEqual(await db.users.select("name").where((eb) => eb.isNull("age")).execute(), [{ name: "dee" }]);
      assert.deepEqual(await db.users.select("name").where("tier", "=", "pro").execute(), [{ name: "ann" }]);
      assert.equal((await db.users.select("name").where("name", "=", "nobody").first()), undefined);
    });

    it("treats hostile input as data", async () => {
      const evil = `x'; DELETE FROM users; --`;
      assert.deepEqual(await db.users.select("name").where("name", "=", evil).execute(), []);
      assert.equal(await db.users.count().execute(), 4);
    });

    it("counts", async () => {
      assert.equal(await db.users.count().execute(), 4);
      assert.equal(await db.users.count().where("active", "=", true).execute(), 3);
    });

    it("exposes the document id as kdb_id", async () => {
      const [row] = await db.users.select("kdb_id", "name").where("name", "=", "ann").execute();
      assert.match(row!.kdb_id, /^[0-9a-f-]{36}$/);
      const same = await db.users.select("name").where("kdb_id", "=", row!.kdb_id).execute();
      assert.deepEqual(same, [{ name: "ann" }]);
    });

    it("updates and deletes with parameters, and leaves other rows alone", async () => {
      await db.users.insert({ name: "eve", age: 20, active: true }).execute();
      await db.users.update({ age: 21, active: false }).where("name", "=", "eve").execute();
      assert.deepEqual(await db.users.select("age", "active").where("name", "=", "eve").execute(), [{ age: 21, active: false }]);
      await db.users.deleteFrom().where("name", "=", "eve").execute();
      assert.equal(await db.users.count().execute(), 4);
    });

    it("rejects what the schema rejects, on the server", async () => {
      // `as never` defeats the compile-time check on purpose: this is the server-side backstop.
      await assert.rejects(db.users.insert({ name: "bad", age: "old" as never }).execute(), /schema violation/);
      // The builder refuses a missing required column itself; raw SQL shows the server does too.
      await assert.rejects(db.users.insert({ name: null as never }).execute(), /missing required column/);
      await assert.rejects(target.ops.exec("app/users", "INSERT INTO users (age) VALUES (1)"), /required/);
    });

    it("round-trips typed documents, including fields that have no SQL column", async () => {
      const id = uuid(1);
      const commit = await db.users.put(id, { name: "zed", age: 7, profile: { theme: "dark", n: [1, 2] }, tags: ["a", "b"] });
      assert.match(commit, /^[0-9a-f]{64}$/);
      const got = await db.users.get(id);
      assert.deepEqual(got.body, { name: "zed", age: 7, profile: { theme: "dark", n: [1, 2] }, tags: ["a", "b"] });
      // ...and it is visible through SQL, since both are views of the same documents.
      assert.deepEqual(await db.users.select("name", "age").where("kdb_id", "=", id).execute(), [{ name: "zed", age: 7 }]);
    });

    it("keeps the commit visible: a stale baseVersion is a conflict, a fresh one commits", async () => {
      const id = uuid(2);
      await db.users.put(id, { name: "base", age: 1 });
      const first = await db.users.get(id);
      // Someone else commits first.
      await db.users.put(id, { name: "base", age: 2 });
      await assert.rejects(
        db.users.commit({ baseVersion: first.commit, writes: [{ docId: id, body: { name: "base", age: 99 } }] }),
        (error: unknown) => error instanceof Error && /conflict|precondition|base/i.test(`${error.name} ${error.message}`),
      );
      const fresh = await db.users.get(id);
      await db.users.commit({ baseVersion: fresh.commit, writes: [{ docId: id, body: { name: "base", age: 3 } }] });
      assert.equal((await db.users.get(id)).body.age, 3);
    });

    it("compareAndSwap is typed and atomic under contention", async () => {
      const id = uuid(3);
      await db.users.put(id, { name: "counter", age: 0 });
      await Promise.all(
        Array.from({ length: 8 }, () => db.users.compareAndSwap(id, (cur) => ({ ...cur!, age: (cur!.age ?? 0) + 1 }), { attempts: 50 })),
      );
      assert.equal((await db.users.get(id)).body.age, 8);
    });

    it("conditional writes by content hash", async () => {
      const id = uuid(4);
      await db.users.putIfAbsent(id, { name: "once" });
      await assert.rejects(db.users.putIfAbsent(id, { name: "twice" }), /precondition|exist/i);
      const { contentHash } = await db.users.getWithHash(id);
      await db.users.replaceIf(id, { name: "once", age: 1 }, contentHash);
      await assert.rejects(db.users.replaceIf(id, { name: "once", age: 2 }, contentHash), /precondition|hash|changed/i);
    });
  });
}
