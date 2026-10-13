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
import { execFile, execFileSync } from "node:child_process";
import { after, before, describe, it } from "node:test";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { connect, type Client, WIRE_FEATURE_BINARY } from "../src/index.ts";
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

// Every case runs twice: over binary frames, which a current Go server agrees to by default, and
// over the JSON envelope, which every older server and the JVM one still speak.
for (const jsonOnly of [false, true]) describe(`live interop against the Go server (${jsonOnly ? "JSON" : "binary"} frames)`, { skip: enabled ? false : "go toolchain not found" }, () => {
  let server: ChildProcessWithoutNullStreams;
  let client: Client;
  let serverUri = "";

  before(async () => {
    server = spawn("go", ["run", "./cmd/kdb-tsinterop", "-namespace", NS, "-max-result-rows", "50"], {
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

    serverUri = uri;
    client = await connect(uri, { requestTimeoutMs: 15_000, jsonOnly });
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

  it("agrees binary frames unless asked not to", () => {
    assert.deepEqual(client.wireFeatures, jsonOnly ? [] : [WIRE_FEATURE_BINARY]);
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

  it("deletes a document by key", async () => {
    const docId = randomUuid();
    await client.putJSON(NS, docId, { v: 1 });
    const commit = await client.delete(NS, docId);
    assert.match(commit, /^[0-9a-f]{64}$/);
    await assert.rejects(client.get(NS, docId), KdbNotFoundError);
  });

  it("deletes inside a transaction alongside writes", async () => {
    const keep = randomUuid();
    const drop = randomUuid();
    const added = randomUuid();
    await client.putJSON(NS, keep, { v: 1 });
    const seed = await client.putJSON(NS, drop, { v: 1 });

    await client.commit({
      namespace: NS,
      baseVersion: seed,
      writes: [{ docId: added, body: { v: 2 } }],
      deletes: [drop],
    });

    assert.deepEqual((await client.get(NS, keep)).body, { v: 1 });
    assert.deepEqual((await client.get(NS, added)).body, { v: 2 });
    await assert.rejects(client.get(NS, drop), KdbNotFoundError);
  });

  it("deleting a document that is not there is not an error", async () => {
    await client.delete(NS, randomUuid());
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

  it("replaces a document wholesale, dropping keys a merge would keep", async () => {
    const docId = randomUuid();
    await client.putJSON(NS, docId, { a: 1, b: 2, c: 3 });
    // The merge default is unchanged: naming only d keeps a, b and c.
    await client.putJSON(NS, docId, { d: 4 });
    assert.deepEqual((await client.get(NS, docId)).body, { a: 1, b: 2, c: 3, d: 4 });

    await client.replace(NS, docId, { a: 9 });
    assert.deepEqual((await client.get(NS, docId)).body, { a: 9 });
  });

  it("replaceIf and compareAndSwap can remove a field", async () => {
    const docId = randomUuid();
    await client.putJSON(NS, docId, { keep: 1, drop: 2 });
    const { contentHash } = await client.getWithHash(NS, docId);
    await client.replaceIf(NS, docId, { keep: 1 }, contentHash);
    assert.deepEqual((await client.get(NS, docId)).body, { keep: 1 });

    await client.compareAndSwap(NS, docId, () => ({ other: true }));
    assert.deepEqual((await client.get(NS, docId)).body, { other: true });
  });

  it("lets exactly one of two OS processes win a claim on one document", async () => {
    const docId = randomUuid();
    await client.putJSON(NS, docId, { owner: "" });
    const { contentHash } = await client.getWithHash(NS, docId);

    const worker = join(dirname(fileURLToPath(import.meta.url)), "claim-worker.ts");
    const startAt = String(Date.now() + 1500);
    const run = (name: string) =>
      new Promise<string>((resolve, reject) => {
        execFile(
          process.execPath,
          [worker, serverUri, NS, docId, contentHash, name, startAt],
          { timeout: 60_000 },
          (error, stdout, stderr) =>
            error ? reject(new Error(`${name}: ${error.message}\n${stderr}`)) : resolve(stdout.trim()),
        );
      });
    const results = await Promise.all([run("p1"), run("p2")]);
    assert.deepEqual([...results].sort(), ["lost", "won"]);

    const owner = ((await client.get(NS, docId)).body as { owner: string }).owner;
    assert.ok(owner === "p1" || owner === "p2");
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
      // Was "SELECT 1 GARBAGE", which Layer 16 made legal: the server's parser gained implicit
      // aliases, so that reads as `SELECT 1 AS GARBAGE`, exactly as Postgres and MySQL parse
      // it. Replaced with input that is still genuinely malformed.
      "SELECT 1 GARBAGE GARBAGE",
    ]) {
      await assert.rejects(client.queryRaw(NS, sql), `expected ${JSON.stringify(sql)} to fail`);
    }
    const rows = await client.query<{ name: string }>(NS, "SELECT name FROM players");
    assert.ok(rows.length >= 2);
  });

  it("serializes 50 concurrent DELETEs on one namespace and reports rows affected", async () => {
    for (let i = 0; i < 50; i++) {
      await client.exec(NS, `INSERT INTO players (name, level) VALUES ('del${i}', '1')`);
    }
    const counts = await Promise.all(
      Array.from({ length: 50 }, (_, i) =>
        client.exec(NS, `DELETE FROM players WHERE name = 'del${i}'`),
      ),
    );
    assert.deepEqual(counts, new Array(50).fill(1));
    assert.equal(await client.exec(NS, "DELETE FROM players WHERE name = 'never-existed'"), 0);
    const left = await client.query<{ name: string }>(NS, "SELECT name FROM players");
    assert.ok(left.every((r) => !r.name.startsWith("del")));
  });

  it("streams a result larger than the server's result bound in pages", async () => {
    const total = 130; // the server in this suite caps a reply at 50 rows
    for (let i = 0; i < total; i++) {
      await client.exec(NS, `INSERT INTO players (name, level) VALUES ('page${String(i).padStart(3, "0")}', '1')`);
    }
    await assert.rejects(client.query(NS, "SELECT name FROM players WHERE name LIKE 'page%'"), /50/);

    const rows = await client.queryAll<{ name: string }>(
      NS,
      "SELECT name FROM players WHERE name LIKE 'page%' ORDER BY name",
      [],
      40,
    );
    assert.equal(rows.length, total);
    assert.deepEqual(
      rows.map((r) => r.name),
      Array.from({ length: total }, (_, i) => `page${String(i).padStart(3, "0")}`),
    );

    let pages = 0;
    for await (const page of client.queryRawPages(NS, "SELECT name FROM players WHERE name LIKE 'page%'", [], 0)) {
      pages++;
      assert.ok(page.rows.length <= 50);
    }
    assert.equal(pages, 3); // 130 rows at the clamped page size of 49
  });
});
