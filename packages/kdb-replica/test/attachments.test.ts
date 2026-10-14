import assert from "node:assert/strict";
import { test } from "node:test";
import { AttachmentEdit, attachmentContentHash, ContentHasher, refHash, setHash, type Attachment } from "../src/attachments.ts";
import { utf8 } from "../src/bytes.ts";
import { MemoryStore } from "../src/store.ts";
import { load, opFromFixture } from "./golden.ts";

interface Ref { scheme: number; contentHash: string; size: number; mime: string; refHash: string }
const f = load<{
  refs: Ref[];
  sets: { entries: { id: string; name: string; ref: Ref }[]; setHash: string }[];
  contentHashes: { name: string; size: number; seed: number; hash: string; helloText?: string }[];
  table: { ops: Record<string, unknown>[]; root: string }[];
}>("attachments.json");

const ref = (r: Ref) => ({ scheme: r.scheme, contentHash: r.contentHash, size: BigInt(r.size), mime: r.mime });

test("ref and set hashes are Go's (the pinned goldens among them)", () => {
  for (const r of f.refs) assert.equal(refHash(ref(r)), r.refHash);
  assert.equal(f.refs[0]!.refHash, "365f1bd0c0b790d6812664a83229589447f5a9a3156bfa137b65320621ff0a68");
  for (const s of f.sets) {
    const set: Attachment[] = s.entries.map((e) => ({ id: e.id, name: e.name, ...ref(e.ref) }));
    assert.equal(setHash(set), s.setHash);
  }
  assert.equal(f.sets[0]!.setHash, "dd3ca4b42f1408538c2e5a1c84c888b6670fb9da99b2029a80eb9c8d411630d4");
});

test("scheme-1 content hashes are Go's, whole and streamed in odd pieces", () => {
  for (const c of f.contentHashes) {
    const data = c.helloText !== undefined ? utf8(c.helloText) : new Uint8Array(c.size).map((_, i) => (i * 31 + c.seed * 7 + (i >> 8)) & 0xff);
    assert.equal(attachmentContentHash(data), c.hash, c.name);
    const h = new ContentHasher();
    for (let off = 0; off < data.length; off += 300_007) h.update(data.subarray(off, off + 300_007));
    assert.equal(h.digest(), c.hash, `${c.name} streamed`);
  }
  assert.equal(attachmentContentHash(utf8("hello")), "cf1b6c96f0fcdb59fffdc5a353309421c73530cf0639f595d3a42546131c168b");
});

test("the attachment table's root after every step is Go's", async () => {
  const store = new MemoryStore();
  for (const [i, step] of f.table.entries()) {
    const edit = new AttachmentEdit(store, "ns");
    await edit.apply(step.ops.map(opFromFixture));
    assert.equal((await edit.finish()) ?? "", step.root, `step ${i}`);
    const batch = new Map();
    edit.writeTo(batch);
    await store.apply(batch);
  }
});
