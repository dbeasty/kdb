import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { test } from "node:test";
import { toHex, utf8 } from "../src/bytes.ts";
import { onlyChildHash, onlyChildHashPortable, sha256, sha256Portable } from "../src/sha256.ts";

test("the portable SHA-256 matches the NIST vectors", () => {
  assert.equal(toHex(sha256Portable(utf8(""))), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  assert.equal(toHex(sha256Portable(utf8("abc"))), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  assert.equal(
    toHex(sha256Portable(utf8("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"))),
    "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
  );
  assert.equal(
    toHex(sha256Portable(utf8("a".repeat(1_000_000)))),
    "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0",
  );
});

test("the portable SHA-256 agrees with node:crypto at every padding boundary", () => {
  for (let n = 0; n < 300; n++) {
    const data = new Uint8Array(n).map((_, i) => (i * 31 + n) & 0xff);
    assert.equal(toHex(sha256Portable(data)), createHash("sha256").update(data).digest("hex"), `length ${n}`);
    assert.equal(toHex(sha256(data)), toHex(sha256Portable(data)));
  }
});

test("the single-child hash, native and portable midstate, is the full preimage's", () => {
  for (let i = 0; i < 2000; i++) {
    const nib = i % 16;
    const child = createHash("sha256").update(String(i)).digest();
    const full = new Uint8Array(1 + 16 * 32);
    full[0] = 0x01;
    full.set(child, 1 + nib * 32);
    const want = toHex(createHash("sha256").update(full).digest());
    assert.equal(toHex(onlyChildHash(nib, child)), want, `nib ${nib}`);
    assert.equal(toHex(onlyChildHashPortable(nib, child)), want, `portable, nib ${nib}`);
  }
});

test("the portable SHA-256 agrees with node:crypto at every length around the block boundaries", () => {
  for (let n = 0; n <= 600; n++) {
    const m = new Uint8Array(n).map((_, i) => (i * 31 + n) & 0xff);
    assert.equal(toHex(sha256Portable(m)), createHash("sha256").update(m).digest("hex"), `length ${n}`);
  }
});
