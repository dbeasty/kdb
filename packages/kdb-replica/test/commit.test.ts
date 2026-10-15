import assert from "node:assert/strict";
import { test } from "node:test";
import { fromHex, toHex } from "../src/bytes.ts";
import {
  buildCommit,
  contentHash,
  decodeCommit,
  decodeCommitList,
  encodeCommitList,
  encodeCommitPayload,
  type CommitFields,
} from "../src/commit.ts";
import { DecodeError } from "../src/codec.ts";
import { load, opFromFixture, type CommitFixture } from "./golden.ts";

const commits = [
  ...load<CommitFixture[]>("commits.json"),
  ...load<{ commits: CommitFixture[] }>("attachments.json").commits,
];

function fields(f: CommitFixture): CommitFields {
  return {
    parents: f.parents,
    namespace: f.namespace,
    transactionId: f.transactionId,
    timestampMicros: BigInt(f.timestampMicros),
    authorNodeId: f.authorNodeId,
    ops: f.ops.map((o) => opFromFixture(o)),
    treeHash: f.treeHash,
    schemaHash: f.schemaHash,
    message: f.message,
    attachmentRoot: f.attachmentRoot,
  };
}

for (const f of commits) {
  test(`commit ${f.name}: decodes Go's payload and derives Go's hash`, () => {
    const c = decodeCommit(fromHex(f.payloadHex));
    assert.equal(c.hash, f.hash);
    const { hash: _h, payload: _p, ...got } = c;
    const want = fields(f);
    if (want.schemaHash === undefined) delete want.schemaHash;
    if (want.attachmentRoot === undefined) delete want.attachmentRoot;
    assert.deepEqual(got, want);
  });

  test(`commit ${f.name}: encodes byte-identically to Go`, () => {
    assert.equal(toHex(encodeCommitPayload(fields(f))), f.payloadHex);
    assert.equal(buildCommit(fields(f)).hash, f.hash);
  });
}

test("a commit list round-trips, and an empty one decodes as no commits", () => {
  const list = commits.map((f) => decodeCommit(fromHex(f.payloadHex)));
  const back = decodeCommitList(encodeCommitList(list));
  assert.deepEqual(back.map((c) => c.hash), commits.map((f) => f.hash));
  assert.deepEqual(decodeCommitList(new Uint8Array()), []);
});

test("corrupt payloads are refused, not misread", () => {
  const good = fromHex(commits[1]!.payloadHex);
  assert.throws(() => decodeCommit(good.subarray(0, good.length - 1)), DecodeError);
  assert.throws(() => decodeCommit(new Uint8Array([...good, 0])), DecodeError);
  const huge = new Uint8Array([0xff, 0xff, 0xff, 0x0f]);
  assert.throws(() => decodeCommitList(huge), DecodeError);
});

test("content hashes match Go's", () => {
  // the hashes.json case @limidus/kdb-client checks too
  const golden = JSON.parse(
    (globalThis as { process: { getBuiltinModule(id: string): { readFileSync(p: URL, e: string): string } } })
      .process.getBuiltinModule("node:fs")
      .readFileSync(new URL("../../kdb-client/test/golden/hashes.json", import.meta.url), "utf8"),
  ) as { docId: string; json: string; contentHash: string }[];
  for (const h of golden) assert.equal(contentHash(h.docId, h.json), h.contentHash);
});
