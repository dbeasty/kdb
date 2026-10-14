/**
 * Live interop for attachments: the TypeScript client against a real Go server (kdb-tsinterop),
 * over a real WebSocket with binary frames.
 *
 * Two layers, gated separately:
 *   - SQL: $bytes parameters, CellKindBytes cells, WITH ATTACHMENT / SET / RENAME / DROP. Needs
 *     only the SQL attachment support and the wire's bytes cells and params.
 *   - The attachment messages (ATTACHMENT_UPLOAD / GET / LIST, 0x44-0x49) and attachmentPut /
 *     attachmentDelete operations in a client transaction. These need the server's handlers for
 *     those messages; a server without them (the wire contract landed before its handlers) does
 *     not answer, and those cases skip with a note rather than fail. Set KDB_ATTACHMENTS_LIVE=1 to
 *     make a missing handler a failure instead.
 *
 * Skipped entirely when the Go toolchain is unavailable, like interop.test.ts.
 */

import assert from "node:assert/strict";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { execFileSync } from "node:child_process";
import { after, before, describe, it, type TestContext } from "node:test";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  attachmentContentHash,
  connect,
  KdbAttachmentNotFoundError,
  KdbUnsupportedError,
  sqlAttachment,
  WIRE_FEATURE_ATTACHMENTS,
  type Client,
} from "../src/index.ts";
import { KdbError } from "../src/errors.ts";
import { randomUuid } from "../src/wire/uuid.ts";

const goDir = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..", "go");
const NS = "app/files";
const REQUIRE_HANDLERS = process.env["KDB_ATTACHMENTS_LIVE"] === "1";

function goAvailable(): boolean {
  try {
    execFileSync("go", ["version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

function gen(n: number, seed = 0): Uint8Array {
  const b = new Uint8Array(n);
  for (let i = 0; i < n; i++) b[i] = (i * 7 + Math.floor(i / 251) + seed) & 0xff;
  return b;
}

const utf8 = (s: string) => new TextEncoder().encode(s);

describe("live attachments against the Go server", { skip: goAvailable() ? false : "go toolchain not found" }, () => {
  let server: ChildProcessWithoutNullStreams;
  let client: Client;
  let serverUri = "";
  /** Why the attachment-message cases cannot run, or "" when they can. */
  let messagesMissing = "";

  before(async () => {
    server = spawn("go", ["run", "./cmd/kdb-tsinterop", "-namespace", NS], { cwd: goDir, stdio: ["pipe", "pipe", "pipe"] });
    const uri = await new Promise<string>((resolve, reject) => {
      let stdout = "";
      let stderr = "";
      const timer = setTimeout(() => reject(new Error(`server did not become ready in 120s\nstderr:\n${stderr}`)), 120_000);
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
    client = await connect(uri, { requestTimeoutMs: 15_000 });

    // Probe the message handlers: a list of a document that does not exist is an empty list.
    try {
      await client.listAttachments(NS, randomUuid(), { timeoutMs: 3_000 });
    } catch (error) {
      if (!(error instanceof KdbError)) throw error;
      messagesMissing = `the server does not serve the attachment messages yet (${error.message})`;
    }
  });

  after(async () => {
    await client?.close().catch(() => undefined);
    server?.stdin.end();
    server?.kill("SIGTERM");
  });

  function needsMessages(t: TestContext): boolean {
    if (!messagesMissing) return true;
    if (REQUIRE_HANDLERS) assert.fail(messagesMissing);
    t.skip(messagesMissing);
    return false;
  }

  async function docIdOf(table: string, title: string): Promise<string> {
    const rows = await client.query<{ kdb_id: string }>(NS, `SELECT kdb_id FROM ${table} WHERE title = ?`, [title]);
    assert.equal(rows.length, 1);
    return rows[0]!.kdb_id;
  }

  it("agrees the attachments wire feature", () => {
    assert.ok(client.wireFeatures.includes(WIRE_FEATURE_ATTACHMENTS), client.wireFeatures.join(","));
  });

  it("writes attachments through SQL with $bytes parameters and reads them back as bytes", async () => {
    const big = gen(3 * 1024 * 1024 + 11); // several hash chunks
    await client.exec(
      NS,
      "INSERT INTO files (title) VALUES ('sql-doc') WITH ATTACHMENT 'a.txt' = ? MIME 'text/plain', ATTACHMENT 'big.bin' = ?",
      [utf8("hello"), big],
    );
    const { rows } = await client.queryValues(
      NS,
      "SELECT ATTACHMENT('a.txt'), ATTACHMENT('big.bin'), ATTACHMENT_COUNT(), ATTACHMENT('none') FROM files WHERE title = 'sql-doc'",
    );
    assert.equal(rows.length, 1);
    const [a, b, count, none] = rows[0]!;
    assert.deepEqual(a, utf8("hello"));
    assert.ok(b instanceof Uint8Array);
    assert.equal(Buffer.compare(Buffer.from(b), Buffer.from(big)), 0);
    assert.equal(count, 2);
    assert.equal(none, null);

    await client.exec(NS, "UPDATE files RENAME ATTACHMENT 'a.txt' TO 'greeting.txt', DROP ATTACHMENT 'big.bin' WHERE title = 'sql-doc'");
    const listing = await client.queryValues(NS, "SELECT _attachments FROM files WHERE title = 'sql-doc'");
    const items = listing.rows[0]![0] as Array<{ name: string; size: number; mime: string; hash: string }>;
    assert.deepEqual(items.map((i) => [i.name, i.size, i.mime]), [["greeting.txt", 5, "text/plain"]]);
    // The server's content hash is the one this client computes.
    assert.equal(items[0]!.hash, await attachmentContentHash(utf8("hello")));
  });

  it("commits hand-built attachmentPut / attachmentDelete operations the server applies", async () => {
    // No attachment messages needed: SQL stores the blob, and the client's transaction then
    // references it from another document - the op JSON is what is under test.
    await client.exec(NS, "INSERT INTO files (title) VALUES ('blob-source') WITH ATTACHMENT 'src' = ?", [utf8("shared bytes")]);
    await client.exec(NS, "INSERT INTO files (title) VALUES ('blob-target')");
    const listing = await client.queryValues(NS, "SELECT _attachments FROM files WHERE title = 'blob-source'");
    const [src] = listing.rows[0]![0] as Array<{ hash: string; size: number }>;
    const target = await docIdOf("files", "blob-target");

    const attId = randomUuid();
    await client.commit({
      namespace: NS,
      baseVersion: (await client.get(NS, target)).commit,
      writes: [],
      attachments: [{ kind: "put", docId: target, id: attId, name: "copy", hash: src!.hash, size: src!.size, mime: "text/plain" }],
    });
    let read = await client.queryValues(NS, "SELECT ATTACHMENT('copy'), ATTACHMENT_MIME('copy') FROM files WHERE title = 'blob-target'");
    assert.deepEqual(read.rows[0], [utf8("shared bytes"), "text/plain"]);

    // Delete by ID needs no lookup, so it is a transaction alone.
    await client.deleteAttachment(NS, target, { id: attId });
    read = await client.queryValues(NS, "SELECT ATTACHMENT_COUNT() FROM files WHERE title = 'blob-target'");
    assert.equal(read.rows[0]![0], 0);
  });

  it("uploads a blob and binds it with STREAM ?", async (t) => {
    if (!needsMessages(t)) return;
    const data = gen(2 * 1024 * 1024 + 5, 3);
    const blob = await client.uploadAttachment(NS, data);
    assert.deepEqual(blob, { hash: await attachmentContentHash(data), size: data.length });
    await client.exec(NS, "INSERT INTO files (title) VALUES ('streamed') WITH ATTACHMENT 'scan' = STREAM ?", [sqlAttachment(blob)]);
    const { rows } = await client.queryValues(NS, "SELECT ATTACHMENT_SIZE('scan') FROM files WHERE title = 'streamed'");
    assert.equal(rows[0]![0], data.length);
  });

  it("puts, lists, reads, renames and deletes through the attachment API", async (t) => {
    if (!needsMessages(t)) return;
    await client.exec(NS, "INSERT INTO files (title) VALUES ('api-doc')");
    const docId = await docIdOf("files", "api-doc");

    const data = gen(1024 * 1024 + 77, 9);
    const put = await client.putAttachment(NS, docId, data, { name: "photo.bin", mime: "application/octet-stream" });
    assert.match(put.commit, /^[0-9a-f]{64}$/);
    assert.equal(put.attachment.hash, await attachmentContentHash(data));

    // Replacing by name keeps the ID and the media type.
    const again = await client.putAttachment(NS, docId, utf8("small now"), { name: "photo.bin" });
    assert.equal(again.attachment.id, put.attachment.id);
    const anon = await client.putAttachment(NS, docId, (async function* () {
      yield utf8("streamed ");
      yield utf8("source");
    })());

    const listed = await client.listAttachments(NS, docId);
    assert.deepEqual(listed.map((i) => i.name).sort(), [anon.attachment.id, "photo.bin"].sort());
    assert.deepEqual(listed.find((i) => i.name === "photo.bin"), {
      ...again.attachment,
      mime: "application/octet-stream",
    });
    assert.equal(await client.countAttachments(NS, docId), 2);

    const read = await client.readAttachment(NS, docId, "photo.bin");
    assert.equal(new TextDecoder().decode(read.data), "small now");
    assert.equal(read.superseded, false);
    assert.equal(new TextDecoder().decode((await client.readAttachment(NS, docId, { id: anon.attachment.id })).data), "streamed source");

    // SQL sees what the API wrote.
    const viaSql = await client.queryValues(NS, "SELECT ATTACHMENT('photo.bin') FROM files WHERE title = 'api-doc'");
    assert.deepEqual(viaSql.rows[0]![0], utf8("small now"));

    await client.renameAttachment(NS, docId, "photo.bin", "renamed.bin");
    await client.deleteAttachment(NS, docId, { id: anon.attachment.id });
    assert.deepEqual((await client.listAttachments(NS, docId)).map((i) => i.name), ["renamed.bin"]);
    await assert.rejects(client.readAttachment(NS, docId, "photo.bin"), (e: unknown) => e instanceof KdbError);
    await assert.rejects(client.deleteAttachment(NS, docId, "photo.bin"), KdbAttachmentNotFoundError);
  });

  it("reads a large attachment page by page, verified, and pins the version it started on", async (t) => {
    if (!needsMessages(t)) return;
    await client.exec(NS, "INSERT INTO files (title) VALUES ('paged')");
    const docId = await docIdOf("files", "paged");
    const data = gen(5 * 1024 * 1024 + 3, 1);
    await client.putAttachment(NS, docId, data, { name: "v" });

    const reader = await client.getAttachment(NS, docId, "v", { pageBytes: 1024 * 1024 });
    const parts: Uint8Array[] = [];
    let replaced = false;
    for await (const part of reader) {
      parts.push(part);
      if (!replaced) {
        replaced = true;
        await client.putAttachment(NS, docId, utf8("newer"), { name: "v" });
      }
    }
    assert.ok(parts.length >= 5, `${parts.length} pages`);
    assert.equal(Buffer.compare(Buffer.concat(parts), Buffer.from(data)), 0);
    assert.equal(reader.superseded, true);
  });

  it("commits attachment operations in one transaction with a document write", async (t) => {
    if (!needsMessages(t)) return;
    const docId = randomUuid();
    await client.putJSON(NS, docId, { title: "tx" });
    const op = await client.prepareAttachment(NS, docId, utf8("in a tx"), { name: "t.txt" });
    const head = (await client.get(NS, docId)).commit;
    await client.commit({ namespace: NS, baseVersion: head, writes: [{ docId, body: { title: "tx2" } }], attachments: [op] });
    assert.deepEqual((await client.get(NS, docId)).body, { title: "tx2" });
    assert.deepEqual((await client.readAttachment(NS, docId, "t.txt")).data, utf8("in a tx"));
  });

  it("names the missing feature on a JSON-only connection", async () => {
    const json = await connect(serverUri, { jsonOnly: true, requestTimeoutMs: 15_000 });
    try {
      await assert.rejects(json.listAttachments(NS, randomUuid()), KdbUnsupportedError);
    } finally {
      await json.close();
    }
  });
});
