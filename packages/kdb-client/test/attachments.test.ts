/**
 * Attachments: the 0x44-0x49 codec against go/kdb/wire/attachment_ops_test.go's golden frames,
 * the scheme-1 content hash against go/kdb/document's, and the client's attachment API against
 * an in-memory attachment server (FakeServer).
 */

import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { Client } from "../src/client.ts";
import {
  KdbAttachmentNotFoundError,
  KdbIntegrityError,
  KdbUploadError,
  sqlAttachment,
  sqlBytes,
  uploadChunks,
} from "../src/attachments.ts";
import { KdbAttachmentCorruptError, KdbUnsupportedError } from "../src/errors.ts";
import {
  ATTACHMENT_CHUNK_BYTES,
  attachmentContentHash,
  AttachmentHasher,
  base64ToBytes,
  bytesToBase64,
  decodeAttachmentBody,
  encodeAttachmentBody,
  WIRE_FEATURE_ATTACHMENTS,
  type AttachmentInfoDto,
} from "../src/wire/attachments.ts";
import { bytesFromWire } from "../src/wire/bytes.ts";
import { decodeMessage, encodeMessage } from "../src/wire/envelope.ts";
import { FRAME_HEADER_SIZE } from "../src/wire/frame.ts";
import type { MessageKind, OpDto, TransactionDto } from "../src/wire/messages.ts";
import { randomUuid } from "../src/wire/uuid.ts";
import { FakeServer, HEAD_ONE, sessionAck, type Request } from "./fake-server.ts";
import { bytesToHex, hexToBytes } from "./golden.ts";

// --- codec goldens -----------------------------------------------------------------------------

const DOC = "6f9619ff-8b86-d011-b42d-00cf4fc964ff";
const ATT = "0b6f3c1e-2a4d-4e8f-9a1b-3c5d7e9f1a2b";
const rep = (b: string) => b.repeat(32);
const COMMIT = "03".repeat(32);
const INFO: AttachmentInfoDto = { id: ATT, name: "photo.png", hash: rep("07"), size: 2 ** 33, mime: "image/png" };

/** go/kdb/wire/attachment_ops_test.go attachmentSamples, the client messages (BLOB_FETCH is peer-only). */
const SAMPLES: Array<{ kind: MessageKind; correlationId: number; payload: Record<string, unknown> }> = [
  {
    kind: "attachmentUpload",
    correlationId: 1,
    payload: { namespace: "app/data", sessionId: "s1", uploadId: "u1", offset: 2 ** 20, data: Uint8Array.of(0, 1, 2, 255), final: true },
  },
  {
    kind: "attachmentUploadResult",
    correlationId: 1,
    payload: { namespace: "app/data", uploadId: "u1", received: 2 ** 20 + 4, hash: rep("07"), size: 2 ** 20 + 4 },
  },
  {
    kind: "attachmentUploadResult",
    correlationId: 1,
    payload: { namespace: "app/data", error: "offset 5 does not follow 4", errorCode: "CONFLICT" },
  },
  {
    kind: "attachmentGet",
    correlationId: 2,
    payload: {
      namespace: "app/data", sessionId: "s1", docId: DOC, attachmentId: ATT, name: "photo.png", asOfHex: COMMIT,
      offset: 2 ** 20, maxBytes: 4 * 2 ** 20, readId: "r9", close: true,
    },
  },
  {
    kind: "attachmentData",
    correlationId: 2,
    payload: {
      namespace: "app/data", readId: "r9", docId: DOC, info: INFO, commitHex: COMMIT, offset: 2 ** 20,
      data: new TextEncoder().encode("bytes"), eof: true, superseded: true,
    },
  },
  {
    kind: "attachmentList",
    correlationId: 3,
    payload: { namespace: "app/data", sessionId: "s1", docId: DOC, asOfHex: "" },
  },
  {
    kind: "attachmentListResult",
    correlationId: 3,
    payload: {
      namespace: "app/data", commitHex: COMMIT,
      items: [INFO, { id: DOC, name: "b", hash: rep("08"), size: 0, mime: "" }],
    },
  },
];

/** attachmentGoldenFrames, the same order. */
const GOLDEN = [
  "2b0000004400010001000000020a086170702f64617461120273311a027531208080402a04000102ff3001",
  "450000004500010001000000020a086170702f6461746112027531188480402220070707070707070707070707070707070707070707070707070707070707070728848040",
  "3d0000004500010001000000020a086170702f64617461321a6f6666736574203520646f6573206e6f7420666f6c6c6f7720343a08434f4e464c494354",
  "9b0000004600010002000000020a086170702f64617461120273311a106f9619ff8b86d011b42d00cf4fc964ff22100b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b2a0970686f746f2e706e673240303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333880804040808080024a0272395001",
  "d00000004700010002000000020a086170702f64617461120272391a106f9619ff8b86d011b42d00cf4fc964ff22500a100b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b120970686f746f2e706e671a2007070707070707070707070707070707070707070707070707070707070707072080808080202a09696d6167652f706e672a4030333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033308080403a05627974657340014801",
  "2d0000004800010003000000020a086170702f64617461120273311a106f9619ff8b86d011b42d00cf4fc964ff",
  "e40000004900010003000000020a086170702f646174611240303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330331a500a100b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b120970686f746f2e706e671a2007070707070707070707070707070707070707070707070707070707070707072080808080202a09696d6167652f706e671a370a106f9619ff8b86d011b42d00cf4fc964ff1201621a200808080808080808080808080808080808080808080808080808080808080808",
];

describe("attachment messages", () => {
  it("encodes each Go sample to exactly its golden frame, always binary", () => {
    for (const [i, sample] of SAMPLES.entries()) {
      // No wire features at all: these go binary regardless, like Go's attachmentBinaryForm.
      const frame = encodeMessage(sample.kind, sample.correlationId, sample.payload);
      assert.equal(frame[FRAME_HEADER_SIZE], 2, sample.kind);
      assert.equal(bytesToHex(frame), GOLDEN[i], `${i} ${sample.kind}`);
    }
  });

  it("decodes each golden frame back to its sample", () => {
    for (const [i, sample] of SAMPLES.entries()) {
      const message = decodeMessage(hexToBytes(GOLDEN[i]!));
      assert.equal(message.kind, sample.kind);
      assert.equal(message.correlationId, sample.correlationId);
      const payload = message.payload as Record<string, unknown>;
      for (const [key, want] of Object.entries(sample.payload)) {
        assert.deepEqual(payload[key], want, `${i} ${sample.kind}.${key}`);
      }
      // And back again: decode then encode is the identity on the frame.
      assert.equal(bytesToHex(encodeMessage(message.kind, message.correlationId, payload)), GOLDEN[i]);
    }
  });

  it("decodes absent fields to their zero values", () => {
    const decoded = decodeAttachmentBody(0x47, new Uint8Array(0))!;
    assert.equal(decoded.kind, "attachmentData");
    assert.deepEqual(decoded.payload, {
      namespace: "", readId: "", docId: "", info: { id: "", name: "", hash: "", size: 0, mime: "" }, commitHex: "",
      offset: 0, data: new Uint8Array(0), eof: false, superseded: false, error: "", errorCode: "",
    });
    assert.equal(decodeAttachmentBody(0x10, new Uint8Array(0)), undefined);
    assert.equal(encodeAttachmentBody("sqlExec", {}), undefined);
  });

  it("carries an INTEGRITY refusal through the binary frame", () => {
    const error = "attachstore: stored bytes do not match their content hash: chunk 2 of ab";
    const body = encodeAttachmentBody("attachmentData", { namespace: "ns", readId: "r1", error, errorCode: "INTEGRITY" })!;
    const decoded = decodeAttachmentBody(0x47, body)!.payload as { error: string; errorCode: string };
    assert.equal(decoded.errorCode, "INTEGRITY");
    assert.equal(decoded.error, error);
  });

  it("skips unknown fields and rejects a malformed UUID", () => {
    // Field 15 (varint 1) then namespace: a newer peer's field is ignored.
    const body = Uint8Array.of(0x78, 0x01, 0x0a, 0x01, 0x61);
    assert.equal((decodeAttachmentBody(0x48, body)!.payload as { namespace: string }).namespace, "a");
    assert.throws(() => decodeAttachmentBody(0x48, Uint8Array.of(0x1a, 0x02, 1, 2)), /UUID field is not 16 bytes/);
  });
});

// --- content hash ------------------------------------------------------------------------------

/** go/kdb/document.AttachmentContentHash of gen(n), computed with the Go implementation. */
const HASH_GOLDENS: Array<[number, string]> = [
  [0, "c0e50f0d90d6e2fea1e6f53d4f758ed5fc9035a399a8eb215c29c6ff3ea05425"],
  [1, "ee8acb9cb20eaf4b807ef586e552774d803cc3dd837bf26e9f509500cc2604bd"],
  [5, "65e4588e7bc4125fec95f1885045568667ebc2bb27811b4ca35d6bab2175a8ef"],
  [1 << 20, "61a9891c279fdf7fee76a3738e91b520f45ed3f06c177b4d369953b48c732e0d"],
  [(1 << 20) + 1, "e6ebf3a816a3ecdfd40fdb1990ed7801712153800043c4c4f66de686e3f91825"],
  [3 << 20, "f26068ae7cd852ccf63771c5276eeb17f783a5e235ed2e9b48e11b66742de997"],
  [(5 << 20) + 17, "ddaebc33b7f6f036b95ad7225511a2f273d82b78e5da4030236034bfcea99ed2"],
];

/** gen(n) from the Go generator: byte(i*7 + i/251). */
function gen(n: number): Uint8Array {
  const b = new Uint8Array(n);
  for (let i = 0; i < n; i++) b[i] = (i * 7 + Math.floor(i / 251)) & 0xff;
  return b;
}

describe("attachment content hash (scheme 1)", () => {
  it('hashes "hello" to the documented golden', async () => {
    assert.equal(
      await attachmentContentHash(new TextEncoder().encode("hello")),
      "cf1b6c96f0fcdb59fffdc5a353309421c73530cf0639f595d3a42546131c168b",
    );
  });

  it("matches Go at chunk boundaries and across several chunks", async () => {
    for (const [n, want] of HASH_GOLDENS) {
      assert.equal(await attachmentContentHash(gen(n)), want, `size ${n}`);
    }
  });

  it("does not depend on how the bytes are sliced", async () => {
    const data = gen((3 << 20) + 5);
    const h = new AttachmentHasher();
    for (let off = 0, step = 1; off < data.length; off += step, step = step * 3 + 7) {
      await h.update(data.subarray(off, off + step));
    }
    assert.equal(h.size, data.length);
    assert.equal(await h.digest(), await attachmentContentHash(data));
  });
});

// --- helpers -------------------------------------------------------------------------------------

describe("attachment helpers", () => {
  it("re-cuts any source into 1 MiB upload chunks, marking the last", async () => {
    const data = gen(2 * ATTACHMENT_CHUNK_BYTES + 10);
    async function* drip() {
      for (let off = 0; off < data.length; off += 300_000) yield data.subarray(off, off + 300_000);
    }
    for (const source of [data, drip(), new Blob([data as Uint8Array<ArrayBuffer>]).stream(), [data.subarray(0, 5), data.subarray(5)]]) {
      const chunks = [];
      for await (const c of uploadChunks(source)) chunks.push(c);
      assert.deepEqual(chunks.map((c) => [c.data.length, c.last]), [
        [ATTACHMENT_CHUNK_BYTES, false],
        [ATTACHMENT_CHUNK_BYTES, false],
        [10, true],
      ]);
      assert.deepEqual(Buffer.concat(chunks.map((c) => c.data)), Buffer.from(data));
    }
    const empty = [];
    for await (const c of uploadChunks(new Uint8Array(0))) empty.push(c);
    assert.deepEqual(empty.map((c) => [c.data.length, c.last]), [[0, true]]);
  });

  it("round trips base64 and builds the SQL parameter forms", () => {
    const bytes = gen(100_000);
    assert.deepEqual(base64ToBytes(bytesToBase64(bytes)), bytes);
    assert.equal(bytesToBase64(Uint8Array.of(0, 1, 2, 255)), "AAEC/w==");
    assert.deepEqual(sqlBytes(Uint8Array.of(104, 105)), { $bytes: "aGk=" });
    assert.deepEqual(sqlAttachment({ hash: rep("ab"), size: 9 }), { $attachment: { hash: rep("ab"), size: 9 } });
  });
});

// --- client against a fake attachment server ----------------------------------------------------

const NS = "app/files";

/** An in-memory attachment store speaking the attachment messages, and the txCommit ops. */
class AttachmentStore {
  readonly blobs = new Map<string, Uint8Array>();
  readonly docs = new Map<string, AttachmentInfoDto[]>();
  readonly uploads = new Map<string, Uint8Array[]>();
  readonly reads = new Map<string, { data: Uint8Array; info: AttachmentInfoDto }>();
  readonly txs: TransactionDto[] = [];
  readonly sql: Array<string | null> = [];
  pageBytes = 4;
  /** Corrupts the bytes served (not the info): verification must catch it. */
  corrupt = false;
  /** Fails the upload chunk at this offset once. */
  failAt = -1;
  /** Refuses the page at this offset (and SQL) as a server whose stored copy is damaged does. */
  damagedAt = -1;
  #ids = 0;
  features = ["binary-frames", "binary-frames-2", WIRE_FEATURE_ATTACHMENTS];

  readonly server = new FakeServer((r) => this.#respond(r));

  async client(): Promise<Client> {
    const client = new Client(this.server);
    await client.handshake({ skipVersionCheck: true });
    return client;
  }

  async #respond(r: Request): Promise<{ kind: MessageKind; payload: unknown } | undefined> {
    const p = r.payload;
    switch (r.kind) {
      case "handshake":
        return {
          kind: "handshakeAck",
          payload: { accepted: true, negotiatedEncoding: "JSON", protocolVersion: 1, remoteHeads: {}, wireFeatures: this.features },
        };
      case "sessionBegin":
        return sessionAck(NS);
      case "attachmentUpload": {
        let id = p.uploadId as string;
        if (!id) {
          id = `up-${++this.#ids}`;
          this.uploads.set(id, []);
        }
        const parts = this.uploads.get(id)!;
        const received = parts.reduce((n, b) => n + b.length, 0);
        if (p.offset !== received) {
          return { kind: "attachmentUploadResult", payload: { namespace: NS, uploadId: id, received, error: `offset ${p.offset} does not follow ${received}`, errorCode: "CONFLICT" } };
        }
        if (this.failAt === received) {
          this.failAt = -1;
          return { kind: "attachmentUploadResult", payload: { namespace: NS, uploadId: id, received, error: "disk on fire", errorCode: "UNAVAILABLE" } };
        }
        parts.push(p.data as Uint8Array);
        const total = received + (p.data as Uint8Array).length;
        if (!p.final) return { kind: "attachmentUploadResult", payload: { namespace: NS, uploadId: id, received: total } };
        const all = Buffer.concat(parts);
        const hash = await attachmentContentHash(all);
        this.blobs.set(hash, all);
        this.uploads.delete(id);
        return { kind: "attachmentUploadResult", payload: { namespace: NS, uploadId: id, received: total, hash, size: total } };
      }
      case "attachmentList":
        return {
          kind: "attachmentListResult",
          payload: { namespace: NS, commitHex: HEAD_ONE, items: [...(this.docs.get(p.docId as string) ?? [])].sort((a, b) => a.name.localeCompare(b.name)) },
        };
      case "attachmentGet": {
        let readId = p.readId as string;
        if (readId && p.close) {
          this.reads.delete(readId);
          return { kind: "attachmentData", payload: { namespace: NS, readId } };
        }
        if (!readId) {
          const items = this.docs.get(p.docId as string) ?? [];
          const info = items.find((i) => (p.attachmentId ? i.id === p.attachmentId : i.name === p.name));
          if (!info) return { kind: "attachmentData", payload: { namespace: NS, error: "no such attachment", errorCode: "NOT_FOUND" } };
          let data = this.blobs.get(info.hash)!;
          if (this.corrupt) data = data.map((b, i) => (i === 0 ? b ^ 1 : b));
          readId = `r-${++this.#ids}`;
          this.reads.set(readId, { data, info });
        }
        const read = this.reads.get(readId)!;
        const offset = p.offset as number;
        if (this.damagedAt >= 0 && offset <= this.damagedAt && this.damagedAt < offset + this.pageBytes) {
          this.reads.delete(readId);
          return {
            kind: "attachmentData",
            payload: {
              namespace: NS, readId, docId: p.docId,
              error: `attachstore: stored bytes do not match their content hash: chunk 0 of ${read.info.hash}`,
              errorCode: "INTEGRITY",
            },
          };
        }
        const data = read.data.subarray(offset, offset + this.pageBytes);
        const eof = offset + data.length >= read.data.length;
        if (eof) this.reads.delete(readId);
        return {
          kind: "attachmentData",
          payload: { namespace: NS, readId, docId: p.docId, info: read.info, commitHex: HEAD_ONE, offset, data, eof },
        };
      }
      case "txCommit": {
        const tx = JSON.parse(new TextDecoder().decode(bytesFromWire(p.transactionBytes, "transactionBytes"))) as TransactionDto;
        this.txs.push(tx);
        for (const op of tx.operations) this.#apply(op);
        return { kind: "sqlResult", payload: { namespace: NS, sessionId: "sess-1", columns: [], rows: [], rowsAffected: 1, resolvedCommitHex: HEAD_ONE, readOnly: false, generatedIds: [] } };
      }
      case "sqlExec":
        this.sql.push(p.parametersJson as string | null);
        if (this.damagedAt >= 0) {
          return {
            kind: "sqlResult",
            payload: {
              namespace: NS, sessionId: "sess-1", columns: [], rows: [], rowsAffected: 0, resolvedCommitHex: "", readOnly: true,
              generatedIds: [], error: "ATTACHMENT(\"x\"): attachstore: stored bytes do not match their content hash", errorCode: "INTEGRITY",
            },
          };
        }
        return {
          kind: "sqlResult",
          payload: {
            namespace: NS, sessionId: "sess-1", columns: ["a", "n"], rows: [["aGVsbG8=", "1"], ["", ""]],
            rowsAffected: 0, resolvedCommitHex: HEAD_ONE, readOnly: true, generatedIds: [], cellKinds: "6211",
          },
        };
      default:
        return undefined;
    }
  }

  #apply(op: OpDto): void {
    const items = this.docs.get(op.docId!) ?? [];
    if (op.kind === "attachmentPut") {
      assert.ok(this.blobs.has(op.blobHashHex!), "put references an uploaded blob");
      assert.equal(op.scheme, 1);
      const info = { id: op.attachmentId!, name: op.name!, hash: op.blobHashHex!, size: op.size!, mime: op.mime! };
      this.docs.set(op.docId!, [...items.filter((i) => i.id !== info.id), info]);
    } else if (op.kind === "attachmentDelete") {
      this.docs.set(op.docId!, items.filter((i) => i.id !== op.attachmentId));
    }
  }
}

const DOC_A = "11111111-2222-4333-8444-555555555555";

describe("client attachments", () => {
  it("uploads in 1 MiB chunks with offsets and the upload ID, verifying the hash", async () => {
    const store = new AttachmentStore();
    const client = await store.client();
    const data = gen(2 * ATTACHMENT_CHUNK_BYTES + 3);
    const progress: number[] = [];
    const blob = await client.uploadAttachment(NS, data, { onProgress: (n) => progress.push(n) });
    assert.deepEqual(blob, { hash: await attachmentContentHash(data), size: data.length });
    const uploads = store.server.requests.filter((r) => r.kind === "attachmentUpload").map((r) => r.payload);
    assert.deepEqual(
      uploads.map((u) => [u.uploadId, u.offset, (u.data as Uint8Array).length, u.final, u.sessionId]),
      [
        ["", 0, ATTACHMENT_CHUNK_BYTES, false, "sess-1"],
        ["up-1", ATTACHMENT_CHUNK_BYTES, ATTACHMENT_CHUNK_BYTES, false, "sess-1"],
        ["up-1", 2 * ATTACHMENT_CHUNK_BYTES, 3, true, "sess-1"],
      ],
    );
    assert.deepEqual(progress, [ATTACHMENT_CHUNK_BYTES, 2 * ATTACHMENT_CHUNK_BYTES, data.length]);
  });

  it("uploads an empty blob as one final chunk", async () => {
    const store = new AttachmentStore();
    const client = await store.client();
    assert.deepEqual(await client.uploadAttachment(NS, new Uint8Array(0)), { hash: HASH_GOLDENS[0]![1], size: 0 });
  });

  it("reports where a failed upload stopped, and resumes from there", async () => {
    const store = new AttachmentStore();
    store.failAt = ATTACHMENT_CHUNK_BYTES;
    const client = await store.client();
    const data = gen(ATTACHMENT_CHUNK_BYTES + 100);
    const error = await client.uploadAttachment(NS, data).then(
      () => assert.fail("upload succeeded"),
      (e: unknown) => e,
    );
    assert.ok(error instanceof KdbUploadError);
    assert.equal(error.uploadId, "up-1");
    assert.equal(error.received, ATTACHMENT_CHUNK_BYTES);
    assert.equal(error.code, "UNAVAILABLE");
    const blob = await client.uploadAttachment(NS, data.subarray(error.received), {
      resume: { uploadId: error.uploadId, received: error.received },
    });
    assert.deepEqual(blob, { hash: await attachmentContentHash(data), size: data.length });
  });

  it("puts, lists, reads, renames and deletes, resolving names to IDs", async () => {
    const store = new AttachmentStore();
    const client = await store.client();

    // No id, no name: a fresh ID, which is also the name. No list needed.
    const anon = await client.putAttachment(NS, DOC_A, Uint8Array.of(1, 2, 3));
    assert.equal(anon.attachment.name, anon.attachment.id);
    assert.equal(store.server.requests.filter((r) => r.kind === "attachmentList").length, 0);

    // By name: created, then replaced in place - same ID, same media type.
    const first = await client.putAttachment(NS, DOC_A, new TextEncoder().encode("hello world"), { name: "a.txt", mime: "text/plain" });
    const second = await client.putAttachment(NS, DOC_A, new TextEncoder().encode("HELLO"), { name: "a.txt" });
    assert.equal(second.attachment.id, first.attachment.id);
    assert.equal(second.attachment.mime, "text/plain");
    assert.equal(second.commit, HEAD_ONE);

    const op = store.txs.at(-1)!.operations[0]!;
    assert.deepEqual(op, {
      kind: "attachmentPut", docId: DOC_A, attachmentId: first.attachment.id, name: "a.txt", scheme: 1,
      blobHashHex: second.attachment.hash, size: 5, mime: "text/plain",
    });

    assert.equal(await client.countAttachments(NS, DOC_A), 2);
    const listed = await client.listAttachments(NS, DOC_A);
    assert.deepEqual(listed.find((i) => i.name === "a.txt"), second.attachment);

    const read = await client.readAttachment(NS, DOC_A, "a.txt");
    assert.equal(new TextDecoder().decode(read.data), "HELLO");
    assert.deepEqual(read.info, second.attachment);
    assert.equal(read.superseded, false);
    const byId = await client.readAttachment(NS, DOC_A, { id: anon.attachment.id });
    assert.deepEqual(byId.data, Uint8Array.of(1, 2, 3));

    await client.renameAttachment(NS, DOC_A, "a.txt", "b.txt");
    const renamed = await client.listAttachments(NS, DOC_A);
    assert.deepEqual(renamed.find((i) => i.id === first.attachment.id)?.name, "b.txt");

    await client.deleteAttachment(NS, DOC_A, "b.txt");
    await client.deleteAttachment(NS, DOC_A, { id: anon.attachment.id });
    assert.equal(await client.countAttachments(NS, DOC_A), 0);
    await assert.rejects(client.deleteAttachment(NS, DOC_A, "nope"), KdbAttachmentNotFoundError);
    await assert.rejects(client.readAttachment(NS, DOC_A, "nope"), KdbAttachmentNotFoundError);
  });

  it("streams pages with the read ID and verifies before handing over the last", async () => {
    const store = new AttachmentStore();
    const client = await store.client();
    const data = gen(10);
    await client.putAttachment(NS, DOC_A, data, { name: "x" });

    const reader = await client.getAttachment(NS, DOC_A, "x");
    assert.equal(reader.info.size, 10);
    const pages: number[] = [];
    for await (const page of reader) pages.push(page.length);
    assert.deepEqual(pages, [4, 4, 2]);
    const gets = store.server.requests.filter((r) => r.kind === "attachmentGet").map((r) => r.payload);
    assert.deepEqual(gets.map((g) => [g.readId, g.offset, g.name]), [["", 0, "x"], ["r-2", 4, ""], ["r-2", 8, ""]]);

    // The same bytes through a web stream.
    const viaStream = await new Response((await client.getAttachment(NS, DOC_A, "x")).stream()).arrayBuffer();
    assert.deepEqual(new Uint8Array(viaStream), data);

    store.corrupt = true;
    const bad = await client.getAttachment(NS, DOC_A, "x");
    const seen: number[] = [];
    await assert.rejects(
      (async () => {
        for await (const page of bad) seen.push(page.length);
      })(),
      KdbIntegrityError,
    );
    assert.deepEqual(seen, [4, 4], "the unverified last page is withheld");
    // Unverified reads (and partial ones) hand everything over.
    assert.equal((await client.readAttachment(NS, DOC_A, "x", { verify: false })).data.length, 10);
    assert.equal((await client.readAttachment(NS, DOC_A, "x", { offset: 3 })).data.length, 7);
  });

  it("types a server's refusal of its damaged stored copy as KdbAttachmentCorruptError", async () => {
    const store = new AttachmentStore();
    const client = await store.client();
    await client.putAttachment(NS, DOC_A, gen(10), { name: "x" });

    // The first page.
    store.damagedAt = 0;
    await assert.rejects(client.readAttachment(NS, DOC_A, "x"), (e: unknown) => {
      assert.ok(e instanceof KdbAttachmentCorruptError, String(e));
      assert.ok(!(e instanceof KdbIntegrityError), "not this client's verification");
      assert.equal(e.code, "INTEGRITY");
      return true;
    });

    // A later page, after an intact first one.
    store.damagedAt = 5;
    const reader = await client.getAttachment(NS, DOC_A, "x");
    const seen: number[] = [];
    await assert.rejects(
      (async () => {
        for await (const page of reader) seen.push(page.length);
      })(),
      KdbAttachmentCorruptError,
    );
    assert.deepEqual(seen, [4]);

    // SQL ATTACHMENT().
    await assert.rejects(client.queryValues(NS, "SELECT ATTACHMENT('x') FROM t"), KdbAttachmentCorruptError);

    // This client's own verification failing is the same kind of error.
    store.damagedAt = -1;
    store.corrupt = true;
    await assert.rejects(client.readAttachment(NS, DOC_A, "x"), KdbAttachmentCorruptError);
  });

  it("closes a read broken off early", async () => {
    const store = new AttachmentStore();
    const client = await store.client();
    await client.putAttachment(NS, DOC_A, gen(20), { name: "x" });
    const reader = await client.getAttachment(NS, DOC_A, "x");
    for await (const _ of reader) break;
    const last = store.server.requests.at(-1)!;
    assert.equal(last.kind, "attachmentGet");
    assert.equal(last.payload.close, true);
    assert.equal(store.reads.size, 0);
  });

  it("joins attachment operations to a transaction, after writes and deletes", async () => {
    const store = new AttachmentStore();
    const client = await store.client();
    const put = await client.prepareAttachment(NS, DOC_A, Uint8Array.of(9), { name: "n", mime: "x/y" });
    assert.equal(store.txs.length, 0, "prepare commits nothing");
    const attId = randomUuid();
    await client.commit({
      namespace: NS,
      baseVersion: HEAD_ONE,
      writes: [{ docId: DOC_A, body: { title: "t" } }],
      deletes: [DOC],
      attachments: [put, { kind: "delete", docId: DOC_A, id: attId }],
    });
    const ops = store.txs[0]!.operations;
    assert.deepEqual(ops.map((o) => o.kind), ["write", "delete", "attachmentPut", "attachmentDelete"]);
    assert.deepEqual(ops[3], { kind: "attachmentDelete", docId: DOC_A, attachmentId: attId });
    assert.equal(ops[2]!.mime, "x/y");
  });

  it("refuses attachment calls on a connection without the feature", async () => {
    const store = new AttachmentStore();
    store.features = ["binary-frames"];
    const client = await store.client();
    assert.ok(!client.wireFeatures.includes(WIRE_FEATURE_ATTACHMENTS));
    await assert.rejects(client.listAttachments(NS, DOC_A), KdbUnsupportedError);
    await assert.rejects(client.uploadAttachment(NS, Uint8Array.of(1)), KdbUnsupportedError);
  });

  it("offers the attachments feature in the handshake", async () => {
    const store = new AttachmentStore();
    const client = await store.client();
    assert.ok((store.server.requests[0]!.payload.wireFeatures as string[]).includes(WIRE_FEATURE_ATTACHMENTS));
    assert.ok(client.wireFeatures.includes(WIRE_FEATURE_ATTACHMENTS));
  });

  it("sends bytes parameters as $bytes and reads bytes cells as Uint8Array", async () => {
    const store = new AttachmentStore();
    const client = await store.client();
    const { rows } = await client.queryValues(NS, "SELECT ATTACHMENT('a.txt'), ? FROM t", [
      Uint8Array.of(104, 105),
      sqlAttachment({ hash: rep("ab"), size: 2 }),
      "s",
    ]);
    assert.deepEqual(JSON.parse(store.sql[0]!), [
      { $bytes: "aGk=" },
      { $attachment: { hash: rep("ab"), size: 2 } },
      "s",
    ]);
    assert.deepEqual(rows, [[new TextEncoder().encode("hello"), 1], [null, null]]);
  });
});
