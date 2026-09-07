/**
 * Live interop (Component 63 §9.2): the TypeScript client against a real Go server, over a real
 * WebSocket.
 *
 * This is the test the golden fixtures cannot replace. They prove the TS codec reproduces Go's
 * bytes; the fake server proves the client's own logic. Only this proves a TypeScript process
 * can actually hold a conversation with a Go server - handshake, session, commit, read - over a
 * socket, which is the claim the package makes.
 *
 * It is also the test that could not exist at all until the Go WebSocket listener replaced its
 * HTTP 501 stub, and ws:// is the only transport a browser can open, so this is the browser
 * story's proof too.
 *
 * Skipped when the Go toolchain is unavailable, so `npm test` still works without it.
 */

import assert from "node:assert/strict";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { execFileSync } from "node:child_process";
import { after, before, describe, it } from "node:test";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { connect, type Client } from "../src/index.ts";
import { KdbNotFoundError, KdbPreconditionError } from "../src/errors.ts";
import { randomUuid } from "../src/wire/uuid.ts";

const goDir = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..", "go");
const NS = "app/users";

function goAvailable(): boolean {
  try {
    execFileSync("go", ["version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

const enabled = goAvailable();

describe("live interop against the Go server", { skip: enabled ? false : "go toolchain not found" }, () => {
  let server: ChildProcessWithoutNullStreams;
  let client: Client;

  before(async () => {
    server = spawn("go", ["run", "./cmd/kdb-tsinterop", "-namespace", NS], {
      cwd: goDir,
      stdio: ["pipe", "pipe", "pipe"],
    });

    const uri = await new Promise<string>((resolve, reject) => {
      let stdout = "";
      let stderr = "";
      // `go run` compiles first, so the first line can take a while on a cold build cache.
      const timer = setTimeout(
        () => reject(new Error(`server did not become ready in 120s\nstderr:\n${stderr}`)),
        120_000,
      );
      server.stdout.on("data", (chunk: Buffer) => {
        stdout += chunk.toString();
        const match = /^ready (ws:\/\/\S+)$/m.exec(stdout);
        if (match) {
          clearTimeout(timer);
          resolve(match[1]!);
        }
      });
      server.stderr.on("data", (chunk: Buffer) => {
        stderr += chunk.toString();
      });
      server.on("exit", (code) => {
        clearTimeout(timer);
        reject(new Error(`server exited early (${code})\nstderr:\n${stderr}`));
      });
    });

    client = await connect(uri, { requestTimeoutMs: 15_000 });
  });

  after(async () => {
    await client?.close().catch(() => undefined);
    server?.stdin.end();
    server?.kill("SIGTERM");
  });

  it("completes the handshake a Go server accepts", () => {
    // Reaching `after` at all means the handshake succeeded - including the [] / {} encoding of
    // namespaces and localHeads, which a null would have failed on.
    assert.ok(client);
  });

  it("round trips a document through putJSON and get", async () => {
    const docId = randomUuid();
    const commit = await client.putJSON(NS, docId, { name: "ada", score: 42 });
    assert.match(commit, /^[0-9a-f]{64}$/);

    const read = await client.get(NS, docId);
    assert.deepEqual(read.body, { name: "ada", score: 42 });
    assert.equal(read.commit, commit);
  });

  it("round trips non-ASCII content byte-for-byte", async () => {
    const docId = randomUuid();
    const body = { name: "Ada Lovelace ✨", city: "Köln", note: 'she said "hello"' };
    await client.putJSON(NS, docId, body);
    assert.deepEqual((await client.get(NS, docId)).body, body);
  });

  it("throws KdbNotFoundError for a document that does not exist", async () => {
    await assert.rejects(client.get(NS, randomUuid()), KdbNotFoundError);
  });

  it("upserts, creating then replacing", async () => {
    const docId = randomUuid();
    await client.upsert(NS, docId, { v: 1 });
    assert.deepEqual((await client.get(NS, docId)).body, { v: 1 });
    await client.upsert(NS, docId, { v: 2 });
    assert.deepEqual((await client.get(NS, docId)).body, { v: 2 });
  });

  it("agrees with the server on content hashes", async () => {
    // The end-to-end proof for wire/hash.ts. The hash never crosses the wire, so the only way
    // to find out whether the TS canonical encoder matches the server's is to hand one back as
    // a precondition and see whether the server accepts it.
    const docId = randomUuid();
    await client.putJSON(NS, docId, { v: 1 });

    const { contentHash } = await client.getWithHash(NS, docId);
    const commit = await client.replaceIf(NS, docId, { v: 2 }, contentHash);
    assert.match(commit, /^[0-9a-f]{64}$/);
    assert.deepEqual((await client.get(NS, docId)).body, { v: 2 });
  });

  it("rejects a conditional replace carrying a stale content hash", async () => {
    const docId = randomUuid();
    await client.putJSON(NS, docId, { v: 1 });
    const { contentHash } = await client.getWithHash(NS, docId);

    await client.putJSON(NS, docId, { v: 2 });

    // A hash that was correct a moment ago must now be refused - the whole point of the
    // precondition. If the TS encoder produced hashes the server never matched, this would
    // pass for the wrong reason, which is why the accepting case above is tested first.
    await assert.rejects(
      client.replaceIf(NS, docId, { v: 3 }, contentHash),
      KdbPreconditionError,
    );
  });

  it("refuses putIfAbsent on an existing document", async () => {
    const docId = randomUuid();
    await client.putJSON(NS, docId, { v: 1 });
    await assert.rejects(client.putIfAbsent(NS, docId, { v: 2 }), KdbPreconditionError);
  });

  it("converges under concurrent compare-and-swap", async () => {
    const docId = randomUuid();
    await client.putJSON(NS, docId, { count: 0 });

    const writers = 5;
    await Promise.all(
      Array.from({ length: writers }, () =>
        client.compareAndSwap(
          NS,
          docId,
          (current) => ({ count: ((current as { count: number }).count ?? 0) + 1 }),
          { attempts: 25 },
        ),
      ),
    );

    const final = (await client.get(NS, docId)).body as { count: number };
    // Every increment landed: no lost updates, which is what CAS is for.
    assert.equal(final.count, writers);
  });

  it("commits several documents as one transaction", async () => {
    const first = randomUuid();
    const second = randomUuid();
    const seed = await client.putJSON(NS, first, { v: 0 });

    await client.commit({
      namespace: NS,
      baseVersion: seed,
      writes: [
        { docId: first, body: { v: 1 } },
        { docId: second, body: { v: 2 } },
      ],
    });

    assert.deepEqual((await client.get(NS, first)).body, { v: 1 });
    assert.deepEqual((await client.get(NS, second)).body, { v: 2 });
  });

  it("keeps concurrent requests on one connection matched to their callers", async () => {
    const ids = Array.from({ length: 8 }, () => randomUuid());
    await Promise.all(ids.map((id, i) => client.upsert(NS, id, { n: i })));

    const bodies = await Promise.all(ids.map((id) => client.get(NS, id)));
    bodies.forEach((read, i) => {
      // A mismatch is the correlation-id bug: replies that decode fine but answer someone
      // else's question.
      assert.deepEqual(read.body, { n: i });
    });
  });

  it("runs SQL over the same connection", async () => {
    // Real statements against a real table: KDB's SQL requires a FROM clause, so `SELECT 1` is
    // not a usable smoke test here (see the next case for what it does do).
    await client.exec(NS, "CREATE TABLE players (name VARCHAR NOT NULL, level VARCHAR NOT NULL)");
    await client.exec(NS, "INSERT INTO players (name, level) VALUES ('Alice', '7')");
    await client.exec(NS, "INSERT INTO players (name, level) VALUES ('Bob', '3')");

    const rows = await client.query<{ name: string; level: string }>(
      NS,
      "SELECT name, level FROM players",
    );
    const byName = Object.fromEntries(rows.map((r) => [r.name, r.level]));
    assert.equal(byName["Alice"], "7");
    assert.equal(byName["Bob"], "3");
    // Values stay strings: this layer does not guess at types (see operations.ts on query).
    assert.equal(typeof byName["Alice"], "string");
  });

  it("answers SELECT 1, the connectivity probe", async () => {
    // The statement that used to panic the server's parser and kill the whole process - every
    // connection, every namespace - is now what it should always have been: a one-row answer.
    // This is what a connection pool sends to check the link is alive.
    const { rows } = await client.queryRaw(NS, "SELECT 1");
    assert.equal(rows.length, 1);
    assert.deepEqual(rows[0], ["1"]);

    const aliased = await client.query<{ ping: string }>(NS, "SELECT 1 AS ping");
    assert.deepEqual(aliased, [{ ping: "1" }]);
  });

  it("reports a malformed statement as a parse error, not an outage", async () => {
    await assert.rejects(client.queryRaw(NS, "SELECT a FROM 1"), (error: unknown) => {
      assert.ok(error instanceof Error);
      assert.match(error.message, /expected identifier/);
      assert.match(error.message, /found "1"/);
      return true;
    });

    // Same connection, still serving. (Asserted with SQL rather than a document write because
    // the earlier CREATE TABLE installs a schema on this shared namespace, so an arbitrary
    // document would now be refused - correctly - for missing required fields.)
    const rows = await client.query<{ name: string }>(NS, "SELECT name FROM players");
    assert.ok(rows.length >= 2);
  });

  it("keeps serving after a sweep of malformed statements", async () => {
    for (const sql of [
      "SELECT",
      "SELECT * FROM",
      "SELECT FROM players",
      "",
      "DROP TABLE players",
      "SELECT 1 GARBAGE",
    ]) {
      await assert.rejects(client.queryRaw(NS, sql), `expected ${JSON.stringify(sql)} to fail`);
    }
    const rows = await client.query<{ name: string }>(NS, "SELECT name FROM players");
    assert.ok(rows.length >= 2);
  });
});
