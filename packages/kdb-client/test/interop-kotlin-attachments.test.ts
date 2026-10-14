/**
 * Live interop for attachments: the TypeScript client against a real Kotlin server (kdb-service,
 * kdb-server's AttachmentsWire.kt), over a real WebSocket with binary frames - the attachment
 * messages (ATTACHMENT_UPLOAD / GET / LIST) and attachment operations in client transactions.
 * The Kotlin SQL engine has no attachment SQL, so unlike interop-attachments.test.ts (the Go
 * server) nothing here goes through SQL.
 *
 * Needs a running Kotlin service at KDB_JVM_SQL_WS_URI (namespace KDB_JVM_SQL_NAMESPACE, default
 * demo/interop) - CI's go-interop job starts one; locally:
 *
 *   ./gradlew :kdb-service:runService --args="--listen-sql-ws kdb-ws://127.0.0.1:17444/kdb?bind=true --namespace demo/interop"
 *   KDB_JVM_SQL_WS_URI=ws://127.0.0.1:17444/kdb node --test test/interop-kotlin-attachments.test.ts
 *
 * Skipped when the variable is not set.
 */

import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";

import {
  attachmentContentHash,
  connect,
  KdbAttachmentNotFoundError,
  KdbAttachmentRangeError,
  WIRE_FEATURE_ATTACHMENT_PROOFS,
  WIRE_FEATURE_ATTACHMENTS,
  type Client,
} from "../src/index.ts";
import { randomUuid } from "../src/wire/uuid.ts";

const URI = process.env["KDB_JVM_SQL_WS_URI"] ?? "";
const NS = process.env["KDB_JVM_SQL_NAMESPACE"] ?? "demo/interop";
const MIB = 1024 * 1024;

function gen(n: number, seed = 0): Uint8Array {
  const b = new Uint8Array(n);
  for (let i = 0; i < n; i++) b[i] = (i * 7 + Math.floor(i / 251) + seed) & 0xff;
  return b;
}

const utf8 = (s: string) => new TextEncoder().encode(s);

describe("live attachments against the Kotlin server", { skip: URI ? false : "KDB_JVM_SQL_WS_URI not set" }, () => {
  let client: Client;

  before(async () => {
    // The Kotlin server reports no serverVersion (the JDBC driver tells the two servers' SQL
    // dialects apart by it), which this client refuses unless told to skip the check.
    client = await connect(URI, { requestTimeoutMs: 30_000, skipVersionCheck: true });
  });

  after(async () => {
    await client?.close().catch(() => undefined);
  });

  async function newDoc(): Promise<string> {
    const docId = randomUuid();
    await client.putJSON(NS, docId, { title: "ts-kotlin-attachments" });
    return docId;
  }

  it("agrees the attachment wire features", () => {
    assert.ok(client.wireFeatures.includes(WIRE_FEATURE_ATTACHMENTS), client.wireFeatures.join(","));
    assert.ok(client.wireFeatures.includes(WIRE_FEATURE_ATTACHMENT_PROOFS), client.wireFeatures.join(","));
  });

  it("puts, lists, reads, renames and deletes", async () => {
    const docId = await newDoc();
    const data = gen(MIB + 77, 9);
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
    assert.deepEqual(listed.find((i) => i.name === "photo.bin"), { ...again.attachment, mime: "application/octet-stream" });
    assert.equal(await client.countAttachments(NS, docId), 2);
    const read = await client.readAttachment(NS, docId, "photo.bin");
    assert.equal(new TextDecoder().decode(read.data), "small now");
    assert.equal(read.superseded, false);
    assert.equal(new TextDecoder().decode((await client.readAttachment(NS, docId, { id: anon.attachment.id })).data), "streamed source");
    // The first version, as of its commit.
    assert.deepEqual((await client.readAttachment(NS, docId, "photo.bin", { asOf: put.commit })).data, data);

    await client.renameAttachment(NS, docId, "photo.bin", "renamed.bin");
    await client.deleteAttachment(NS, docId, { id: anon.attachment.id });
    assert.deepEqual((await client.listAttachments(NS, docId)).map((i) => i.name), ["renamed.bin"]);
    await assert.rejects(client.readAttachment(NS, docId, "photo.bin"), KdbAttachmentNotFoundError);
    await assert.rejects(client.deleteAttachment(NS, docId, "photo.bin"), KdbAttachmentNotFoundError);
  });

  it("reads a large attachment page by page, verified, pinned to the version it started on", async () => {
    const docId = await newDoc();
    const data = gen(5 * MIB + 3, 1);
    await client.putAttachment(NS, docId, data, { name: "v" });
    const reader = await client.getAttachment(NS, docId, "v", { pageBytes: MIB });
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

  it("reads verified ranges, checked chunk by chunk", async () => {
    const docId = await newDoc();
    // go/kdb/document's range-proof golden attachment: five chunks, the last 5 bytes.
    const data = new Uint8Array(4 * MIB + 5);
    for (let i = 0; i < data.length; i++) data[i] = i % 251;
    const size = data.length;
    const put = await client.putAttachment(NS, docId, data, { name: "r.bin" });
    assert.equal(put.attachment.hash, "1a57b9052a9bae5f2bbaa720791a30f99ea84caecaa0898551c84d1746d3ae27");
    const cases: Array<[number, number]> = [
      [0, 10],
      [MIB + 100, 50],
      [MIB - 10, 20],
      [100, 4 * MIB + 7 - 100],
      [2 * MIB - 3, MIB + 6],
      [3 * MIB, -1],
      [size - 1, 1],
      [2 * MIB + 1, 0],
      [size, -1],
    ];
    for (const [offset, length] of cases) {
      const reader = await client.openAttachmentRange(NS, docId, "r.bin", { offset, length });
      const parts: Uint8Array[] = [];
      for await (const part of reader) parts.push(part);
      const end = length >= 0 ? Math.min(size, offset + length) : size;
      assert.equal(Buffer.compare(Buffer.concat(parts), Buffer.from(data.subarray(offset, end))), 0, `${offset}+${length}`);
      assert.equal(reader.commit, put.commit);
      assert.equal(reader.superseded, false);
    }
    const past = await client.readAttachmentAt(NS, docId, "r.bin", size + 1, 1).then(
      () => assert.fail("read past the end"),
      (e: unknown) => e,
    );
    assert.ok(past instanceof KdbAttachmentRangeError, String(past));
    assert.equal(past.size, size);
    // Refused opens, and ranges released at their end, leave no read pinned: far more than the
    // connection's 64 reads still work.
    for (let i = 0; i < 70; i++) {
      await assert.rejects(client.readAttachmentAt(NS, docId, "r.bin", size + 1, 1), KdbAttachmentRangeError);
      assert.equal((await client.readAttachmentAt(NS, docId, "r.bin", i, 1))[0], i % 251);
    }
    // A range pins its version and reports a newer one at its end.
    const reader = await client.openAttachmentRange(NS, docId, "r.bin", { offset: MIB - 1, length: 2 * MIB });
    const parts: Uint8Array[] = [];
    let replaced = false;
    for await (const part of reader) {
      parts.push(part);
      if (!replaced) {
        replaced = true;
        await client.putAttachment(NS, docId, utf8("replaced"), { name: "r.bin" });
      }
    }
    assert.equal(Buffer.compare(Buffer.concat(parts), Buffer.from(data.subarray(MIB - 1, 3 * MIB - 1))), 0);
    assert.equal(reader.superseded, true);
    // A zero-byte attachment proves its one empty chunk.
    await client.putAttachment(NS, docId, new Uint8Array(0), { name: "empty" });
    for (const length of [-1, 0, 10]) {
      const r = await client.openAttachmentRange(NS, docId, "empty", { length });
      assert.equal(r.info.size, 0);
      for await (const part of r) assert.equal(part.length, 0);
    }
  });

  it("commits attachment operations in one transaction with a document write", async () => {
    const docId = await newDoc();
    const op = await client.prepareAttachment(NS, docId, utf8("in a tx"), { name: "t.txt" });
    const head = (await client.get(NS, docId)).commit;
    await client.commit({ namespace: NS, baseVersion: head, writes: [{ docId, body: { title: "tx2" } }], attachments: [op] });
    assert.deepEqual((await client.get(NS, docId)).body, { title: "tx2" });
    assert.deepEqual((await client.readAttachment(NS, docId, "t.txt")).data, utf8("in a tx"));
  });
});
