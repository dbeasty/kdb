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
  KdbAttachmentRangeError,
  KdbIntegrityError,
  KdbUploadError,
  sqlAttachment,
  sqlBytes,
  uploadChunks,
} from "../src/attachments.ts";
import { KdbUnsupportedError } from "../src/errors.ts";
import {
  ATTACHMENT_CHUNK_BYTES,
  attachmentChunkCount,
  attachmentContentHash,
  AttachmentHasher,
  attachmentIdentity,
  base64ToBytes,
  bytesToBase64,
  decodeAttachmentBody,
  encodeAttachmentBody,
  MAX_ATTACHMENT_CHUNK_BYTES,
  MerkleTree,
  verifyMerkleInclusion,
  WIRE_FEATURE_ATTACHMENT_PROOFS,
  WIRE_FEATURE_ATTACHMENTS,
  type AttachmentInfoDto,
} from "../src/wire/attachments.ts";
import { bytesFromWire, toHex } from "../src/wire/bytes.ts";
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
  // A verified ranged read (attachment-proofs): the get asks for proofs, the page covers whole
  // chunks and proves each - leaf 0 with an empty-field leaf, leaf 1 short.
  {
    kind: "attachmentGet",
    correlationId: 5,
    payload: { namespace: "app/data", docId: DOC, name: "photo.png", offset: 2 ** 20 + 7, maxBytes: 100, proofs: true },
  },
  {
    kind: "attachmentData",
    correlationId: 5,
    payload: {
      namespace: "app/data", readId: "r10", docId: DOC,
      info: { id: ATT, name: "photo.png", hash: rep("07"), size: 2 ** 20 + 2, mime: "" }, commitHex: COMMIT,
      data: Uint8Array.of(4, 5, 6), eof: true, tree: rep("06"), scheme: 1,
      chunks: [{ leaf: 0, path: [rep("01")] }, { leaf: 1, path: [rep("02")] }],
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
  // Go indices 7 and 8 are BLOB_FETCH / BLOB_FETCH_RESULT (peer-only); these are 9 and 10.
  "3c0000004600010005000000020a086170702f646174611a106f9619ff8b86d011b42d00cf4fc964ff2a0970686f746f2e706e673887804040645801",
  "2a0100004700010005000000020a086170702f6461746112037231301a106f9619ff8b86d011b42d00cf4fc964ff22430a100b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b120970686f746f2e706e671a200707070707070707070707070707070707070707070707070707070707070707208280402a40303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333a030405064001622006060606060606060606060606060606060606060606060606060606060606066a22122001010101010101010101010101010101010101010101010101010101010101016a240801122002020202020202020202020202020202020202020202020202020202020202027001",
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
      tree: "", chunks: [], scheme: 0,
    });
    assert.equal(decodeAttachmentBody(0x10, new Uint8Array(0)), undefined);
    assert.equal(encodeAttachmentBody("sqlExec", {}), undefined);
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
  readonly reads = new Map<string, { data: Uint8Array; info: AttachmentInfoDto; docId: string }>();
  readonly trees = new Map<string, MerkleTree>();
  readonly txs: TransactionDto[] = [];
  readonly sql: Array<string | null> = [];
  pageBytes = 4;
  /** Corrupts the bytes served (not the info): verification must catch it. */
  corrupt = false;
  /** Fails the upload chunk at this offset once. */
  failAt = -1;
  /** Flips a byte of this chunk in the bytes served after the proofs were computed (-1: none). */
  tamperChunk = -1;
  #ids = 0;
  features = ["binary-frames", "binary-frames-2", WIRE_FEATURE_ATTACHMENTS, WIRE_FEATURE_ATTACHMENT_PROOFS];

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
          const read = this.reads.get(readId);
          this.reads.delete(readId);
          if (!read) return { kind: "attachmentData", payload: { namespace: NS, readId } };
          return {
            kind: "attachmentData",
            payload: { namespace: NS, readId, info: read.info, commitHex: HEAD_ONE, eof: true, superseded: this.#superseded(read) },
          };
        }
        if (!readId) {
          const items = this.docs.get(p.docId as string) ?? [];
          const info = items.find((i) => (p.attachmentId ? i.id === p.attachmentId : i.name === p.name));
          if (!info) return { kind: "attachmentData", payload: { namespace: NS, error: "no such attachment", errorCode: "NOT_FOUND" } };
          let data = this.blobs.get(info.hash)!;
          if (this.corrupt) data = data.map((b, i) => (i === 0 ? b ^ 1 : b));
          if (this.tamperChunk >= 0) data = data.map((b, i) => (i === this.tamperChunk * ATTACHMENT_CHUNK_BYTES + 3 ? b ^ 0xff : b));
          readId = `r-${++this.#ids}`;
          this.reads.set(readId, { data, info, docId: p.docId as string });
        }
        const read = this.reads.get(readId)!;
        const size = read.info.size;
        let offset = p.offset as number;
        if (offset < 0 || offset > size) {
          // As the Go server: the read is dropped, but the info stays on the error.
          this.reads.delete(readId);
          return {
            kind: "attachmentData",
            payload: { namespace: NS, info: read.info, commitHex: HEAD_ONE, error: `offset ${offset} outside the attachment's ${size} bytes`, errorCode: "SCHEMA_VIOLATION" },
          };
        }
        let page = Math.min(p.proofs ? (p.maxBytes as number) || MAX_ATTACHMENT_CHUNK_BYTES : this.pageBytes, MAX_ATTACHMENT_CHUNK_BYTES);
        const proof: Record<string, unknown> = {};
        if (p.proofs) {
          // Go's alignment rule: whole chunks, at least one, at most 4 MiB, never past the end.
          const chunk = ATTACHMENT_CHUNK_BYTES;
          let end = Math.ceil((offset + page) / chunk) * chunk;
          offset -= offset % chunk;
          end = Math.min(size, Math.max(end, offset + chunk), offset + MAX_ATTACHMENT_CHUNK_BYTES);
          page = end - offset;
          const tree = await this.#tree(read.info.hash);
          const chunks = [];
          for (let i = offset / chunk; i < attachmentChunkCount(size) && (i * chunk < end || size === 0); i++) {
            chunks.push({ leaf: i, path: tree.proof(i).map(toHex) });
          }
          Object.assign(proof, { tree: toHex(tree.root), chunks, scheme: 1 });
        }
        const data = read.data.subarray(offset, offset + page);
        const eof = offset + data.length >= read.data.length;
        if (eof) this.reads.delete(readId);
        return {
          kind: "attachmentData",
          payload: {
            namespace: NS, readId, docId: p.docId, info: read.info, commitHex: HEAD_ONE, offset, data, eof,
            superseded: eof && this.#superseded(read), ...proof,
          },
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

  /** Whether the read's attachment has since been replaced or removed. */
  #superseded(read: { info: AttachmentInfoDto; docId: string }): boolean {
    const now = (this.docs.get(read.docId) ?? []).find((i) => i.id === read.info.id);
    return now?.hash !== read.info.hash;
  }

  async #tree(hash: string): Promise<MerkleTree> {
    let tree = this.trees.get(hash);
    if (!tree) {
      tree = await MerkleTree.build(await MerkleTree.leavesOf(this.blobs.get(hash)!));
      this.trees.set(hash, tree);
    }
    return tree;
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

// --- verified ranged reads -----------------------------------------------------------------------

/** The range-proof golden attachment (go/kdb/document/attachment_proof_test.go): 4 MiB + 5 bytes, byte i = i % 251. */
function rangeBytes(): Uint8Array {
  const b = new Uint8Array(4 * ATTACHMENT_CHUNK_BYTES + 5);
  for (let i = 0; i < b.length; i++) b[i] = i % 251;
  return b;
}

const RANGE_GOLDEN_HASH = "1a57b9052a9bae5f2bbaa720791a30f99ea84caecaa0898551c84d1746d3ae27";
const RANGE_GOLDEN_TREE = "b6b9d1a58b2d9dc4d118b3347037b67ed5ba981c51684aad96da01b56091d758";
const RANGE_GOLDEN_LEAF = [
  "f4e53704c07aef05b5b12a89d6c1e54292fa7d9d8c3ee431c40b6f6ef6d19280",
  "23124c5e2cfe3dc827a1b85edc1a2da44b2797cfaccc2120830b0663a818c044",
  "eff5a4ce2d94f327c9dc3df962c7a293b9dd39731f7ad77cf9c000ae46893633",
  "4e4be56a1af3ce8199934fa45f00954b81a1789f1bdd37fd0c9a176bb84e9c61",
  "80c33e07ae030ed1db9a08ac474659b0bb3a4c92abf785750c4030adbc37c9b3",
];
const RANGE_GOLDEN_PATH = [
  [RANGE_GOLDEN_LEAF[1]!, "43d36e796ccb572050e71018e752c7b68495fdc5b025bf606d7d249205fc80fb", RANGE_GOLDEN_LEAF[4]!],
  [RANGE_GOLDEN_LEAF[0]!, "43d36e796ccb572050e71018e752c7b68495fdc5b025bf606d7d249205fc80fb", RANGE_GOLDEN_LEAF[4]!],
  [RANGE_GOLDEN_LEAF[3]!, "1661cb740559458c2a1dce898f8f510ea07d28501f983c733aec692565131eb7", RANGE_GOLDEN_LEAF[4]!],
  [RANGE_GOLDEN_LEAF[2]!, "1661cb740559458c2a1dce898f8f510ea07d28501f983c733aec692565131eb7", RANGE_GOLDEN_LEAF[4]!],
  ["b70f08b29c8bc80f288233ea601ba8396567682f18ca787267d5c695ec190f78"],
];

describe("attachment Merkle proofs (RFC 6962)", () => {
  it("reproduces Go's range-proof goldens: tree, leaves and audit paths, each verifying", async () => {
    const data = rangeBytes();
    const leaves = await MerkleTree.leavesOf(data);
    const tree = await MerkleTree.build(leaves);
    assert.equal(await attachmentContentHash(data), RANGE_GOLDEN_HASH);
    assert.equal(toHex(tree.root), RANGE_GOLDEN_TREE);
    assert.equal(await attachmentIdentity(tree.root, data.length), RANGE_GOLDEN_HASH);
    assert.equal(tree.leafCount, 5);
    assert.deepEqual(leaves.map(toHex), RANGE_GOLDEN_LEAF);
    for (const [i, leaf] of leaves.entries()) {
      const path = tree.proof(i);
      assert.deepEqual(path.map(toHex), RANGE_GOLDEN_PATH[i], `leaf ${i}`);
      assert.ok(await verifyMerkleInclusion(leaf, i, 5, path, tree.root), `leaf ${i} verifies`);
      // Any other position, count or path fails.
      assert.ok(!(await verifyMerkleInclusion(leaf, (i + 1) % 5, 5, path, tree.root)), `leaf ${i} elsewhere`);
      assert.ok(!(await verifyMerkleInclusion(leaf, i, 9, path, tree.root)), `leaf ${i} of 9`);
      assert.ok(!(await verifyMerkleInclusion(leaf, i, 5, path.slice(1), tree.root)), `leaf ${i} short path`);
    }
    assert.deepEqual([0, 1, ATTACHMENT_CHUNK_BYTES, ATTACHMENT_CHUNK_BYTES + 1].map(attachmentChunkCount), [1, 1, 1, 2]);
  });

  it("builds the RFC 6962 tree for every leaf count, each path verifying", async () => {
    for (let n = 1; n <= 20; n++) {
      const leaves = await Promise.all(Array.from({ length: n }, (_, i) => MerkleTree.leavesOf(Uint8Array.of(i, n)).then((l) => l[0]!)));
      const tree = await MerkleTree.build(leaves);
      for (let i = 0; i < n; i++) {
        assert.ok(await verifyMerkleInclusion(leaves[i]!, i, n, tree.proof(i), tree.root), `n=${n} leaf ${i}`);
      }
    }
    // The empty tree is the tree of one empty chunk, whose identity is the zero-byte content hash.
    const empty = await MerkleTree.build([]);
    assert.equal(await attachmentIdentity(empty.root, 0), HASH_GOLDENS[0]![1]);
  });
});

describe("client verified ranged reads", () => {
  const MIB = ATTACHMENT_CHUNK_BYTES;

  async function setup(data = rangeBytes(), name = "r.bin") {
    const store = new AttachmentStore();
    const client = await store.client();
    await client.putAttachment(NS, DOC_A, data, { name });
    return { store, client, data };
  }

  it("offers attachment-proofs in the handshake", async () => {
    const { store, client } = await setup(Uint8Array.of(1));
    assert.ok((store.server.requests[0]!.payload.wireFeatures as string[]).includes(WIRE_FEATURE_ATTACHMENT_PROOFS));
    assert.ok(client.wireFeatures.includes(WIRE_FEATURE_ATTACHMENT_PROOFS));
  });

  it("reads verified ranges anywhere in the attachment", async () => {
    const { client, data } = await setup();
    const size = data.length;
    const cases: Array<[string, number, number]> = [
      ["start", 0, 10],
      ["mid-chunk", MIB + 100, 50],
      ["across a chunk boundary", MIB - 10, 20],
      ["across pages", 100, 4 * MIB + 7 - 100],
      ["whole", 0, -1],
      ["from a chunk boundary to the end", 3 * MIB, -1],
      ["into the short last chunk", 4 * MIB - 3, -1],
      ["last byte", size - 1, 1],
      ["past the end is cut", size - 2, 100],
      ["empty mid-way", 2 * MIB + 1, 0],
      ["at the end", size, -1],
      ["at the end, with a length", size, 5],
    ];
    for (const [what, offset, length] of cases) {
      const reader = await client.openAttachmentRange(NS, DOC_A, "r.bin", { offset, length });
      const parts: Uint8Array[] = [];
      for await (const part of reader) parts.push(part);
      const end = length >= 0 ? Math.min(size, offset + length) : size;
      assert.equal(Buffer.compare(Buffer.concat(parts), Buffer.from(data.subarray(offset, end))), 0, what);
      assert.equal(reader.info.size, size, what);
      assert.equal(reader.superseded, false, what);
      assert.equal(reader.commit, HEAD_ONE, what);
    }
  });

  it("asks for chunk-aligned pages of at most 4 MiB, with proofs", async () => {
    const { store, client, data } = await setup();
    const got = await client.readAttachmentAt(NS, DOC_A, "r.bin", 100, 4 * MIB + 7 - 100);
    assert.equal(Buffer.compare(Buffer.from(got), Buffer.from(data.subarray(100, 4 * MIB + 7))), 0);
    const gets = store.server.requests.filter((r) => r.kind === "attachmentGet").map((r) => r.payload);
    assert.deepEqual(
      gets.map((g) => [g.readId, g.offset, g.maxBytes, g.proofs, g.close]),
      [
        // The range [100, 4 MiB + 7) is cut at the attachment's end, 4 MiB + 5, from the second page on.
        ["", 100, 4 * MIB + 7 - 100, true, false],
        ["r-2", 4 * MIB, 5, true, false],
      ],
    );
    assert.equal(store.reads.size, 0);
  });

  it("releases the pinned read at the range's end, reporting superseded from the close", async () => {
    const { store, client, data } = await setup();
    const reader = await client.openAttachmentRange(NS, DOC_A, "r.bin", { offset: MIB - 1, length: 2 * MIB });
    assert.equal(store.reads.size, 1);
    await client.putAttachment(NS, DOC_A, utf8("replaced"), { name: "r.bin" });
    const parts: Uint8Array[] = [];
    for await (const part of reader) parts.push(part);
    assert.equal(Buffer.compare(Buffer.concat(parts), Buffer.from(data.subarray(MIB - 1, 3 * MIB - 1))), 0);
    assert.equal(reader.superseded, true);
    const last = store.server.requests.at(-1)!;
    assert.equal(last.kind, "attachmentGet");
    assert.equal(last.payload.close, true);
    assert.equal(store.reads.size, 0);

    // Unchanged: a range short of the end is closed too, and reports false.
    const { store: s2, client: c2 } = await setup();
    const quiet = await c2.openAttachmentRange(NS, DOC_A, "r.bin", { offset: 5, length: 5 });
    for await (const _ of quiet) void _;
    assert.equal(quiet.superseded, false);
    assert.equal(s2.server.requests.at(-1)!.payload.close, true);
    assert.equal(s2.reads.size, 0);

    // A range reaching the end takes superseded from the EOF page; no close is sent.
    const { store: s3, client: c3, data: d3 } = await setup();
    const tail = await c3.openAttachmentRange(NS, DOC_A, "r.bin", { offset: d3.length - 3 });
    await c3.putAttachment(NS, DOC_A, utf8("newer"), { name: "r.bin" });
    for await (const _ of tail) void _;
    assert.equal(tail.superseded, false, "the EOF page came with the open, before the replace");
    assert.ok(!s3.server.requests.some((r) => r.kind === "attachmentGet" && r.payload.close));
  });

  it("cancel closes the read", async () => {
    const { store, client } = await setup();
    const reader = await client.openAttachmentRange(NS, DOC_A, "r.bin", { offset: 0, length: -1 });
    await reader.cancel();
    assert.equal(store.server.requests.at(-1)!.payload.close, true);
    assert.equal(store.reads.size, 0);
  });

  it("reads like an io.ReaderAt: short at the tail, empty at the end, a typed error past it", async () => {
    const { store, client, data } = await setup();
    const size = data.length;
    assert.deepEqual(await client.readAttachmentAt(NS, DOC_A, "r.bin", 2 * MIB - 5, 10), data.subarray(2 * MIB - 5, 2 * MIB + 5));
    assert.deepEqual(await client.readAttachmentAt(NS, DOC_A, "r.bin", size - 3, 10), data.subarray(size - 3));
    assert.equal((await client.readAttachmentAt(NS, DOC_A, "r.bin", size, 10)).length, 0);

    const past = await client.readAttachmentAt(NS, DOC_A, "r.bin", size + 10, 1).then(
      () => assert.fail("read past the end"),
      (e: unknown) => e,
    );
    assert.ok(past instanceof KdbAttachmentRangeError);
    assert.equal(past.offset, size + 10);
    assert.equal(past.size, size);
    assert.equal(store.reads.size, 0, "the refused read is not left pinned");

    const before = store.server.requests.length;
    const negative = await client.openAttachmentRange(NS, DOC_A, "r.bin", { offset: -1 }).then(
      () => assert.fail("negative offset"),
      (e: unknown) => e,
    );
    assert.ok(negative instanceof KdbAttachmentRangeError);
    assert.equal(negative.size, -1);
    assert.equal(store.server.requests.length, before, "a negative offset is refused without asking");
    await assert.rejects(client.openAttachmentRange(NS, DOC_A, "nope", { offset: 0 }), KdbAttachmentNotFoundError);
  });

  it("proves the one empty chunk of a zero-byte attachment", async () => {
    const { client } = await setup(new Uint8Array(0), "empty");
    for (const length of [-1, 0, 10]) {
      const reader = await client.openAttachmentRange(NS, DOC_A, "empty", { length });
      assert.equal(reader.info.size, 0);
      assert.equal((await collect(reader)).length, 0, `length ${length}`);
    }
    await assert.rejects(client.readAttachmentAt(NS, DOC_A, "empty", 1, 1), KdbAttachmentRangeError);
  });

  it("rejects a tampered chunk before handing out any byte of its page", async () => {
    const data = rangeBytes().subarray(0, 2 * MIB + 10); // three chunks
    const { store, client } = await setup(data);
    store.tamperChunk = 1;
    // Chunk 0 is sound, so a range inside it reads.
    assert.deepEqual(await client.readAttachmentAt(NS, DOC_A, "r.bin", 5, 100), data.subarray(5, 105));
    // A range touching chunk 1 fails at the open: no bytes at all, not even chunk 0's.
    for (const [offset, length] of [[MIB + 100, 10], [MIB - 10, 20], [0, -1]] as const) {
      await assert.rejects(client.openAttachmentRange(NS, DOC_A, "r.bin", { offset, length }), KdbIntegrityError, `${offset}+${length}`);
    }
    assert.equal(store.reads.size, 0, "the failed opens closed their reads");
  });

  it("fails a later page over a tampered chunk, having handed out only the sound pages", async () => {
    const { store, client } = await setup(); // five chunks: pages of chunks 0-3, then 4
    store.tamperChunk = 4;
    const pages = await client.openAttachmentRange(NS, DOC_A, "r.bin", { offset: 10 });
    const seen: number[] = [];
    await assert.rejects(
      (async () => {
        for await (const part of pages) seen.push(part.length);
      })(),
      KdbIntegrityError,
    );
    assert.deepEqual(seen, [4 * MIB - 10]);
    assert.equal(store.reads.size, 0);
  });

  it("rejects a tree that does not hash to the content hash", async () => {
    const { store, client } = await setup();
    const real = store.trees;
    const fake = await MerkleTree.build([await MerkleTree.leavesOf(utf8("other")).then((l) => l[0]!)]);
    for (const k of store.blobs.keys()) real.set(k, fake);
    await assert.rejects(client.openAttachmentRange(NS, DOC_A, "r.bin", { offset: 0, length: 1 }), KdbIntegrityError);
  });

  it("needs the attachment-proofs feature", async () => {
    const store = new AttachmentStore();
    store.features = ["binary-frames", WIRE_FEATURE_ATTACHMENTS];
    const client = await store.client();
    await assert.rejects(client.openAttachmentRange(NS, DOC_A, "r.bin"), KdbUnsupportedError);
    await assert.rejects(client.readAttachmentAt(NS, DOC_A, "r.bin", 0, 1), KdbUnsupportedError);
    // The unverified API is unaffected.
    await client.putAttachment(NS, DOC_A, Uint8Array.of(1, 2, 3), { name: "x" });
    assert.deepEqual((await client.readAttachment(NS, DOC_A, "x")).data, Uint8Array.of(1, 2, 3));
  });
});

const utf8 = (text: string) => new TextEncoder().encode(text);

async function collect(reader: AsyncIterable<Uint8Array>): Promise<Uint8Array> {
  const parts: Uint8Array[] = [];
  for await (const part of reader) parts.push(part);
  return new Uint8Array(Buffer.concat(parts));
}
