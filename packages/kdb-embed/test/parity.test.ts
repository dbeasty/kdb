/**
 * Differential test: one script, two ways of reaching the Go engine - over a real WebSocket to a
 * server process, and in-process through WASM - must produce the same observable results.
 *
 * Embedding is only worth anything if "it works locally" and "it works against the server" are the
 * same statement. One deliberate gap: go/cmd/kdb-tsinterop serves a bare single runtime, which
 * aliases any namespace name onto its one store, while the embedded engine routes through a
 * NamespaceSet like the production service and refuses unknown namespaces (embed.test.ts covers
 * that). Commit hashes are excluded (they embed timestamps and node ids); everything a
 * caller can branch on - bodies, row values, error classes and messages - is compared.
 */

import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { connect, type KdbOperations } from "@limidus/kdb-client";
import { goAvailable, startGoServer } from "../../kdb-codegen/test/server.ts";
import { openEmbedded } from "../src/index.ts";

const NS = "app/users";
const uuid = (n: number): string => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;

type Step = readonly [label: string, run: (ops: KdbOperations) => Promise<unknown>];

/** Runs a step and records its result - or, for a failure, the error's class and message. */
async function observe(run: () => Promise<unknown>): Promise<unknown> {
  try {
    return { ok: await run() };
  } catch (error) {
    const e = error as Error;
    return { error: e.constructor.name, message: e.message };
  }
}

const SCRIPT: Step[] = [
  ["put", (o) => o.putJSON(NS, uuid(1), { name: "ann", n: 1 }).then(() => "done")],
  ["get", (o) => o.get(NS, uuid(1)).then((r) => r.body)],
  ["get missing", (o) => o.get(NS, uuid(2))],
  ["putIfAbsent twice", async (o) => {
    await o.putIfAbsent(NS, uuid(3), { v: 1 });
    return o.putIfAbsent(NS, uuid(3), { v: 2 });
  }],
  ["replaceIfPresent missing", (o) => o.replaceIfPresent(NS, uuid(4), { v: 1 })],
  ["replaceIf stale hash", async (o) => {
    const { contentHash } = await o.getWithHash(NS, uuid(3));
    await o.replaceIf(NS, uuid(3), { v: 2 }, contentHash);
    return o.replaceIf(NS, uuid(3), { v: 3 }, contentHash);
  }],
  ["content hash is identical", async (o) => (await o.getWithHash(NS, uuid(3))).contentHash],
  ["cas x10", async (o) => {
    await o.putJSON(NS, uuid(5), { n: 0 });
    await Promise.all(Array.from({ length: 10 }, () => o.compareAndSwap(NS, uuid(5), (c) => ({ n: ((c as { n: number }).n ?? 0) + 1 }), { attempts: 100 })));
    return (await o.get(NS, uuid(5))).body;
  }],
  ["delete then get", async (o) => {
    await o.putJSON(NS, uuid(6), { a: 1 });
    await o.delete(NS, uuid(6));
    return o.get(NS, uuid(6));
  }],
  ["unicode + nesting", async (o) => {
    const body = { s: "Zoë 日本語 🚀", deep: { a: [1, { b: [null, true, 2.5] }] }, empty: {}, none: [] };
    await o.putJSON(NS, uuid(7), body);
    return (await o.get(NS, uuid(7))).body;
  }],
  ["create table", (o) => o.exec(NS, "CREATE TABLE people (name TEXT NOT NULL, age INTEGER, ok BOOLEAN, big BIGINT, score REAL)")],
  ["insert rows", async (o) => {
    for (const [n, a, ok, big, s] of [["a", 30, true, "9007199254740993", 1.5], ["b", 25, false, null, null], ["c", null, null, null, 3]]) {
      await o.exec(NS, "INSERT INTO people (name, age, ok, big, score) VALUES (?, ?, ?, ?, ?)", [n, a, ok, big === null ? null : Number.isSafeInteger(Number(big)) ? Number(big) : 9007199254740992, s]);
    }
  }],
  ["select all", (o) => o.queryRaw(NS, "SELECT name, age, ok, score FROM people ORDER BY name")],
  ["select filtered", (o) => o.queryRaw(NS, "SELECT name FROM people WHERE age > ? AND ok = ? ORDER BY name", [20, true])],
  ["select in/like/limit", (o) => o.queryRaw(NS, "SELECT name FROM people WHERE name IN ('a','b','c') AND name LIKE '%' ORDER BY name DESC LIMIT 2 OFFSET 1")],
  ["count", (o) => o.queryRaw(NS, "SELECT COUNT(*) AS n FROM people")],
  ["update + delete", async (o) => {
    await o.exec(NS, "UPDATE people SET age = ? WHERE name = ?", [31, "a"]);
    await o.exec(NS, "DELETE FROM people WHERE name = ?", ["b"]);
    return o.queryRaw(NS, "SELECT name, age FROM people ORDER BY name");
  }],
  ["schema violation: type", (o) => o.exec(NS, "INSERT INTO people (name, age) VALUES ('x', 'old')")],
  ["schema violation: required", (o) => o.exec(NS, "INSERT INTO people (age) VALUES (1)")],
  ["parse error", (o) => o.queryRaw(NS, "SELECT a FROM 1")],
  ["SELECT 1", (o) => o.queryRaw(NS, "SELECT 1")],
  ["documents are visible through SQL", async (o) => (await o.queryRaw(NS, "SELECT name FROM people ORDER BY name")).rows],
  ["after errors: still serving", (o) => o.queryRaw(NS, "SELECT name FROM people ORDER BY name")],
];

describe("embedded engine behaves like the Go server", { skip: goAvailable() ? false : "go toolchain not found" }, () => {
  it(`produces identical results for a ${SCRIPT.length}-step script`, async () => {
    const server = await startGoServer(NS);
    const remote = await connect(server.uri);
    const local = await openEmbedded({ namespace: NS });
    try {
      for (const [label, run] of SCRIPT) {
        const [fromServer, fromEmbed] = [await observe(() => run(remote)), await observe(() => run(local))];
        assert.deepEqual(fromEmbed, fromServer, `step "${label}" differs between embedded and server`);
      }
    } finally {
      await local.close();
      await remote.close();
      await server.stop();
    }
  });
});
