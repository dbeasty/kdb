import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { KdbOperations } from "@kdb/client";
import { KdbCoercionError, coerceValue } from "../src/index.ts";
import { users } from "./generated/kdb.ts";

/** Records what reached the "server" and answers queryRaw from a canned result. */
function fakeOps(result: { columns: string[]; rows: string[][] } = { columns: [], rows: [] }) {
  const calls: Array<{ kind: string; sql: string; args: unknown[] }> = [];
  const ops = {
    queryRaw: async (_ns: string, sql: string, args: unknown[] = []) => {
      calls.push({ kind: "query", sql, args });
      return result;
    },
    exec: async (_ns: string, sql: string, args: unknown[] = []) => {
      calls.push({ kind: "exec", sql, args });
    },
  } as unknown as KdbOperations;
  return { ops, calls };
}

describe("SQL compilation", () => {
  const { ops } = fakeOps();
  const t = users.bind(ops);

  it("selects with where / order / limit / offset, binding every value", () => {
    const q = t.select("name", "age").where("age", ">", 26).where("name", "like", "a%").orderBy("age", "desc").limit(2).offset(1);
    assert.deepEqual(q.toSQL(), {
      sql: "SELECT name, age FROM users WHERE age > ? AND name LIKE ? ORDER BY age DESC LIMIT 2 OFFSET 1",
      args: [26, "a%"],
    });
  });

  it("composes or/and groups with parentheses", () => {
    const q = t.select("name").where((eb) => eb.and(eb.or(eb.cmp("age", "=", 25), eb.cmp("age", "=", 30)), eb.cmp("active", "=", true)));
    assert.deepEqual(q.toSQL(), { sql: "SELECT name FROM users WHERE (age = ? OR age = ?) AND active = ?", args: [25, 30, true] });
  });

  it("supports IN and IS [NOT] NULL", () => {
    const q = t.select("name").where((eb) => eb.in("tier", ["free", "pro"])).where((eb) => eb.isNotNull("age"));
    assert.deepEqual(q.toSQL(), { sql: "SELECT name FROM users WHERE tier IN (?, ?) AND age IS NOT NULL", args: ["free", "pro"] });
  });

  it("inlines a bigint as digits (JSON cannot carry one) and never inlines anything else", () => {
    const q = t.select("name").where("balance", ">", 9007199254740993n).where("name", "=", "x'; DROP TABLE users; --");
    assert.deepEqual(q.toSQL(), {
      sql: "SELECT name FROM users WHERE balance > 9007199254740993 AND name = ?",
      args: ["x'; DROP TABLE users; --"],
    });
  });

  it("compiles count, insert, update and delete", () => {
    assert.deepEqual(t.count().where("active", "=", true).toSQL(), { sql: "SELECT COUNT(*) AS n FROM users WHERE active = ?", args: [true] });
    assert.deepEqual(t.insert({ name: "ann", age: 3, tier: "pro" }).toSQL(), {
      sql: "INSERT INTO users (name, age, tier) VALUES (?, ?, ?)",
      args: ["ann", 3, "pro"],
    });
    assert.deepEqual(t.update({ age: 4, active: null }).where("name", "=", "ann").toSQL(), {
      sql: "UPDATE users SET age = ?, active = ? WHERE name = ?",
      args: [4, null, "ann"],
    });
    assert.deepEqual(t.deleteFrom().where("kdb_id", "=", "id-1").toSQL(), { sql: "DELETE FROM users WHERE kdb_id = ?", args: ["id-1"] });
  });

  it("selectAll lists SQL columns only", () => {
    assert.equal(t.selectAll().toSQL().sql, "SELECT name, age, balance, score, active, joined, nickname, tier FROM users");
  });

  it("refuses what would silently do the wrong thing", async () => {
    const bad = (fn: () => unknown, pattern: RegExp) => assert.throws(fn, (e: unknown) => e instanceof Error && e.name === "BuilderError" && pattern.test(e.message));
    // Unfiltered writes need an explicit .all().
    await assert.rejects(t.update({ age: 1 }).execute(), /every row/);
    await assert.rejects(t.deleteFrom().execute(), /every row/);
    // = NULL never matches in SQL.
    bad(() => t.select("name").where("age", "=", null as never).toSQL(), /isNull/);
    // Object/array fields have no SQL column; unknown columns are not guessed at.
    bad(() => t.select("profile" as never).toSQL(), /no SQL column/);
    bad(() => t.select("nope" as never).toSQL(), /unknown column/);
    bad(() => t.select("name").where("age", "like", "1%" as never).toSQL(), /LIKE needs a text column/);
    bad(() => t.select("name").where("age", "==" as never, 1).toSQL(), /unsupported comparison/);
    bad(() => t.insert({ age: 1 } as never).toSQL(), /missing required column "name"/);
    bad(() => t.select("name").where((eb) => eb.in("age", [])).toSQL(), /at least one value/);
    bad(() => t.select("name").limit(-1), /non-negative/);
    bad(() => t.select("name").where("name", "=", undefined as never).toSQL(), /undefined/);
    bad(() => t.select("name").where("name", "=", { x: 1 } as never).toSQL(), /scalar/);
  });

  it("allows an explicit whole-table write", async () => {
    const { ops: o, calls } = fakeOps();
    await users.bind(o).deleteFrom().all().execute();
    await users.bind(o).update({ age: 1 }).all().execute();
    assert.deepEqual(calls.map((c) => c.sql), ["DELETE FROM users", "UPDATE users SET age = ?"]);
  });
});

describe("result coercion", () => {
  it("coerces each kind per the schema", async () => {
    const { ops } = fakeOps({
      columns: ["name", "age", "balance", "score", "active", "joined", "tier", "nickname"],
      rows: [
        ["ann", "30", "9007199254740993", "1.5", "1", "2024-01-02T03:04:05Z", "pro", "annie"],
        ["bob", "", "", "", "", "", "", ""],
      ],
    });
    const rows = await users.bind(ops).select("name", "age", "balance", "score", "active", "joined", "tier", "nickname").execute();
    assert.deepEqual(rows[0], {
      name: "ann", age: 30, balance: 9007199254740993n, score: 1.5, active: true,
      joined: "2024-01-02T03:04:05Z", tier: "pro", nickname: "annie",
    });
    // Absent optional values are null - except strings, where SQL cannot tell NULL from "".
    assert.deepEqual(rows[1], { name: "bob", age: null, balance: null, score: null, active: null, joined: null, tier: null, nickname: "" });
  });

  it("keeps bigint exact where Number would corrupt it", () => {
    assert.equal(coerceValue("balance", { kind: "int64", required: false }, "9007199254740993"), 9007199254740993n);
    assert.notEqual(BigInt(Number("9007199254740993")), 9007199254740993n);
  });

  it("treats false/0 as false, not as absent", () => {
    const def = { kind: "bool", required: false } as const;
    assert.equal(coerceValue("active", def, "0"), false);
    assert.equal(coerceValue("active", def, "false"), false);
    assert.equal(coerceValue("active", def, ""), null);
  });

  it("fails loudly when the server disagrees with the generated schema", () => {
    const fails = (def: Parameters<typeof coerceValue>[1], raw: string, pattern: RegExp) =>
      assert.throws(() => coerceValue("c", def, raw), (e: unknown) => e instanceof KdbCoercionError && pattern.test(e.message));
    fails({ kind: "int32", required: true }, "", /required/);
    fails({ kind: "int32", required: false }, "abc", /integer/);
    fails({ kind: "int32", required: false }, "1.5", /integer/);
    fails({ kind: "float64", required: false }, "abc", /number/);
    fails({ kind: "int64", required: false }, "1.5", /64-bit/);
    fails({ kind: "bool", required: false }, "maybe", /boolean/);
    fails({ kind: "enum", required: false, values: ["a", "b"] }, "c", /one of a, b/);
  });

  it("rejects a column the schema does not know", async () => {
    const { ops } = fakeOps({ columns: ["surprise"], rows: [["1"]] });
    await assert.rejects(users.bind(ops).select("name").execute(), /unexpected column "surprise"/);
  });

  it("count parses and validates", async () => {
    const { ops } = fakeOps({ columns: ["n"], rows: [["7"]] });
    assert.equal(await users.bind(ops).count().execute(), 7);
    const bad = fakeOps({ columns: ["n"], rows: [["x"]] });
    await assert.rejects(users.bind(bad.ops).count().execute(), /COUNT returned "x"/);
  });

  it("first() adds LIMIT 1 and returns undefined on no rows", async () => {
    const { ops, calls } = fakeOps({ columns: ["name"], rows: [] });
    assert.equal(await users.bind(ops).select("name").first(), undefined);
    assert.match(calls[0]!.sql, /LIMIT 1$/);
  });
});
