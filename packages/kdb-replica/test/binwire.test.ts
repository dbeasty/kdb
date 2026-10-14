import assert from "node:assert/strict";
import { test } from "node:test";
import { encodeFrameBody } from "@kdb/client";
import {
  chunkLeaf,
  decodeAttachmentUploadResult,
  decodeBlobFetchResult,
  encodeAttachmentUpload,
  encodeBlobFetch,
  verifyInclusion,
} from "../src/binwire.ts";
import { concat, fromHex, toHex } from "../src/bytes.ts";
import { sha256 } from "../src/sha256.ts";

// go/kdb/wire/attachment_ops_test.go attachmentGoldenFrames (the frames of attachmentSamples).
const golden = {
  upload: "2b0000004400010001000000020a086170702f64617461120273311a027531208080402a04000102ff3001",
  uploadResult: "450000004500010001000000020a086170702f6461746112027531188480402220070707070707070707070707070707070707070707070707070707070707070728848040",
  uploadError: "3d0000004500010001000000020a086170702f64617461321a6f6666736574203520646f6573206e6f7420666f6c6c6f7720343a08434f4e464c494354",
  blobFetch: "b00000004200010004000000020a086170702f646174611291010a106f9619ff8b86d011b42d00cf4fc964ff12100b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b1a4030333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033222007070707070707070707070707070707070707070707070707070707070707072880808001308080401880808004",
  blobFetchResult: "d50000004300010004000000020a086170702f64617461129c0112200707070707070707070707070707070707070707070707070707070707070707188080c001222006060606060606060606060606060606060606060606060606060606060606062880808001320209093a46080212200101010101010101010101010101010101010101010101010101010101010101122002020202020202020202020202020202020202020202020202020202020202024001121b0801520c6e6f742072657461696e65645a094e4f545f464f554e441801",
};
const rep = (b: number) => toHex(new Uint8Array(32).fill(b));
const body = (frameHex: string) => fromHex(frameHex).subarray(13);

test("BLOB_FETCH and ATTACHMENT_UPLOAD encode to Go's golden frames", () => {
  const fetch = encodeBlobFetch("app/data", [{
    docId: "6f9619ff-8b86-d011-b42d-00cf4fc964ff", attachmentId: "0b6f3c1e-2a4d-4e8f-9a1b-3c5d7e9f1a2b",
    commitHex: rep(3), hash: rep(7), offset: 2 << 20, maxBytes: 1 << 20,
  }], 8 << 20);
  assert.equal(toHex(encodeFrameBody(0x42, 4, 2, fetch)), golden.blobFetch);
  const upload = encodeAttachmentUpload({ namespace: "app/data", sessionId: "s1", uploadId: "u1", offset: 1 << 20, data: new Uint8Array([0, 1, 2, 255]), final: true, expect: "" });
  assert.equal(toHex(encodeFrameBody(0x44, 1, 2, upload)), golden.upload);
});

test("BLOB_FETCH_RESULT and ATTACHMENT_UPLOAD_RESULT decode Go's golden frames", () => {
  const r = decodeBlobFetchResult(body(golden.blobFetchResult));
  assert.equal(r.namespace, "app/data");
  assert.equal(r.more, true);
  const [ok, failed] = r.parts;
  assert.deepEqual(
    { ...ok!, data: [...ok!.data], chunks: ok!.chunks.map((c) => ({ leaf: c.leaf, path: c.path.map(toHex) })) },
    { index: 0, hash: rep(7), size: 3 << 20, tree: rep(6), offset: 2 << 20, data: [9, 9], chunks: [{ leaf: 2, path: [rep(1), rep(2)] }], eof: true, superseded: false, error: "", errorCode: "" },
  );
  assert.equal(failed!.index, 1);
  assert.equal(failed!.errorCode, "NOT_FOUND");
  assert.equal(failed!.error, "not retained");
  assert.deepEqual(decodeAttachmentUploadResult(body(golden.uploadResult)), {
    namespace: "app/data", uploadId: "u1", received: (1 << 20) + 4, hash: rep(7), size: (1 << 20) + 4, error: "", errorCode: "",
  });
  const e = decodeAttachmentUploadResult(body(golden.uploadError));
  assert.equal(e.errorCode, "CONFLICT");
  assert.equal(e.error, "offset 5 does not follow 4");
});

test("RFC 6962 inclusion proofs verify for every leaf of trees of 1 to 17 leaves, and only there", () => {
  const node = (l: Uint8Array, r: Uint8Array) => sha256(concat([new Uint8Array([1]), l, r]));
  const mth = (leaves: Uint8Array[]): Uint8Array => {
    if (leaves.length === 1) return leaves[0]!;
    let k = 1;
    while (k * 2 < leaves.length) k *= 2;
    return node(mth(leaves.slice(0, k)), mth(leaves.slice(k)));
  };
  const proof = (leaves: Uint8Array[], i: number): Uint8Array[] => {
    if (leaves.length <= 1) return [];
    let k = 1;
    while (k * 2 < leaves.length) k *= 2;
    return i < k ? [...proof(leaves.slice(0, k), i), mth(leaves.slice(k))] : [...proof(leaves.slice(k), i - k), mth(leaves.slice(0, k))];
  };
  for (let n = 1; n <= 17; n++) {
    const leaves = Array.from({ length: n }, (_, i) => chunkLeaf(new Uint8Array([i, n])));
    const tree = mth(leaves);
    for (let i = 0; i < n; i++) {
      assert.ok(verifyInclusion(leaves[i]!, i, n, proof(leaves, i), tree), `n=${n} i=${i}`);
      if (n > 1) assert.ok(!verifyInclusion(leaves[(i + 1) % n]!, i, n, proof(leaves, i), tree), `wrong leaf n=${n} i=${i}`);
    }
  }
});
