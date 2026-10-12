/**
 * Compile-time assertions. This file is never run: `npm run typecheck` fails if any `@ts-expect-error`
 * stops erroring (a type got looser) or any plain line starts erroring (a type got tighter).
 */
import type { KdbOperations } from "@kdb/client";
import { bind, type User } from "./generated/kdb.ts";

declare const ops: KdbOperations;
const db = bind(ops);

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
function assertType<_T extends true>(): void {}

export async function checks(): Promise<void> {
  const rows = await db.users.select("name", "age", "balance", "active", "tier", "nickname").execute();
  const row = rows[0]!;
  assertType<Equal<typeof row.name, string>>();
  assertType<Equal<typeof row.age, number | null>>();
  assertType<Equal<typeof row.balance, bigint | null>>();
  assertType<Equal<typeof row.active, boolean | null>>();
  assertType<Equal<typeof row.tier, "free" | "pro" | null>>();
  // Optional strings read as "" (SQL cannot tell NULL from ""), so they are not `| null`.
  assertType<Equal<typeof row.nickname, string>>();

  // Only the selected columns exist on the row.
  // @ts-expect-error score was not selected
  void row.score;

  // Object/array fields have no SQL column.
  // @ts-expect-error profile is not SQL-addressable
  db.users.select("profile");
  // @ts-expect-error unknown column
  db.users.select("nope");

  // Values are checked against the column type.
  db.users.select("name").where("age", ">", 3);
  // @ts-expect-error age is a number
  db.users.select("name").where("age", ">", "3");
  // @ts-expect-error enum values are a closed set
  db.users.select("name").where("tier", "=", "gold");
  db.users.select("name").where("balance", ">", 5n);
  db.users.select("kdb_id").where("kdb_id", "=", "x");

  // Inserts require the required columns and nothing unknown.
  db.users.insert({ name: "a" });
  // @ts-expect-error name is required
  db.users.insert({ age: 1 });
  // @ts-expect-error unknown column
  db.users.insert({ name: "a", nope: 1 });
  // @ts-expect-error profile has no SQL column
  db.users.insert({ name: "a", profile: {} });
  db.users.update({ age: null });
  // @ts-expect-error required column cannot be set to null
  db.users.update({ name: null });

  // Document bodies are the generated interface, both ways.
  const got = await db.users.get("id");
  assertType<Equal<typeof got.body, User>>();
  await db.users.put("id", { name: "a", profile: { x: 1 }, tags: [1] });
  // @ts-expect-error name is required on a document
  await db.users.put("id", { age: 1 });
  // @ts-expect-error wrong field type
  await db.users.put("id", { name: "a", age: "x" });
  await db.users.compareAndSwap("id", (cur) => (cur === null ? { name: "seed" } : { ...cur, age: 1 }));

  const n: number = await db.users.count().execute();
  void n;
}
