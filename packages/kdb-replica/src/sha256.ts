/**
 * Synchronous SHA-256.
 *
 * Every hash KDB defines is SHA-256: commits, content hashes and every node of the document trie.
 * The trie is why this is synchronous. Inserting one document hashes up to 33 nodes (a leaf folded
 * up to its depth, then the path to the root), so a Web Crypto promise per hash would cost more
 * in scheduling than in hashing. On Node the native `node:crypto` digest is used. Elsewhere this is
 * a plain implementation of FIPS 180-4, checked against the NIST vectors in test/sha256.test.ts.
 */

type NodeCrypto = {
  createHash(alg: string): { update(d: Uint8Array): { digest(): Uint8Array } };
};

const nodeCrypto: NodeCrypto | undefined = (() => {
  const proc = (globalThis as { process?: { getBuiltinModule?: (id: string) => unknown } }).process;
  return proc?.getBuiltinModule?.("node:crypto") as NodeCrypto | undefined;
})();

const K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);

const W = new Uint32Array(64);

const H0 = new Int32Array([
  0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
]);

/** Folds the 64-byte block at off of view into state (eight words, modified in place). */
function compress(state: Int32Array, view: DataView, off: number): void {
  for (let i = 0; i < 16; i++) W[i] = view.getUint32(off + i * 4);
  for (let i = 16; i < 64; i++) {
    const w15 = W[i - 15]!, w2 = W[i - 2]!;
    const s0 = ((w15 >>> 7) | (w15 << 25)) ^ ((w15 >>> 18) | (w15 << 14)) ^ (w15 >>> 3);
    const s1 = ((w2 >>> 17) | (w2 << 15)) ^ ((w2 >>> 19) | (w2 << 13)) ^ (w2 >>> 10);
    W[i] = (W[i - 16]! + s0 + W[i - 7]! + s1) | 0;
  }
  let a = state[0]!, b = state[1]!, c = state[2]!, d = state[3]!;
  let e = state[4]!, f = state[5]!, g = state[6]!, h = state[7]!;
  for (let i = 0; i < 64; i++) {
    const S1 = ((e >>> 6) | (e << 26)) ^ ((e >>> 11) | (e << 21)) ^ ((e >>> 25) | (e << 7));
    const ch = (e & f) ^ (~e & g);
    const t1 = (h + S1 + ch + K[i]! + W[i]!) | 0;
    const S0 = ((a >>> 2) | (a << 30)) ^ ((a >>> 13) | (a << 19)) ^ ((a >>> 22) | (a << 10));
    const maj = (a & b) ^ (a & c) ^ (b & c);
    const t2 = (S0 + maj) | 0;
    h = g; g = f; f = e; e = (d + t1) | 0;
    d = c; c = b; b = a; a = (t1 + t2) | 0;
  }
  state[0] = (state[0]! + a) | 0; state[1] = (state[1]! + b) | 0;
  state[2] = (state[2]! + c) | 0; state[3] = (state[3]! + d) | 0;
  state[4] = (state[4]! + e) | 0; state[5] = (state[5]! + f) | 0;
  state[6] = (state[6]! + g) | 0; state[7] = (state[7]! + h) | 0;
}

/**
 * Finishes a SHA-256 whose first prefixLen bytes (whole blocks) are already folded into state,
 * hashing data[from:] as the rest. With H0 and no prefix it is the plain digest; with a midstate
 * it skips a constant prefix's blocks.
 */
function resume(state: Int32Array, prefixLen: number, data: Uint8Array, from: number): Uint8Array {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let off = from;
  for (; data.length - off >= 64; off += 64) compress(state, view, off);
  const rest = data.length - off;
  const tail = new Uint8Array(rest + 9 <= 64 ? 64 : 128);
  tail.set(data.subarray(off), 0);
  tail[rest] = 0x80;
  const tv = new DataView(tail.buffer);
  const bitLen = (prefixLen + data.length - from) * 8;
  tv.setUint32(tail.length - 8, Math.floor(bitLen / 0x100000000));
  tv.setUint32(tail.length - 4, bitLen >>> 0);
  for (let t = 0; t < tail.length; t += 64) compress(state, tv, t);
  const out = new Uint8Array(32);
  const ov = new DataView(out.buffer);
  for (let i = 0; i < 8; i++) ov.setUint32(i * 4, state[i]!);
  return out;
}

/** SHA-256 in pure TS, whatever the runtime. Exported so tests check it, not just the native one. */
export function sha256Portable(data: Uint8Array): Uint8Array {
  return resume(H0.slice(), 0, data, 0);
}

export function sha256(data: Uint8Array): Uint8Array {
  if (nodeCrypto) {
    const d = nodeCrypto.createHash("sha256").update(data).digest();
    return new Uint8Array(d.buffer, d.byteOffset, d.byteLength);
  }
  return sha256Portable(data);
}

/**
 * The trie's single-child internal hash: SHA-256 over 0x01 and sixteen 32-byte slots with only
 * nib's set. On Node it is one native digest of the full preimage: the call costs more than its
 * nine blocks (a prefixed hash's copy() is no cheaper than createHash). The pure-TS digest instead
 * resumes from the state after the constant prefix's whole blocks (everything before nib's slot),
 * skipping 3.5 of the nine blocks on average: a browser's fold is about a third faster. Either
 * way the result is the full preimage's hash byte for byte, as Go and Kotlin compute it.
 */
export function onlyChildHash(nib: number, child: Uint8Array): Uint8Array {
  if (!nodeCrypto) return onlyChildHashPortable(nib, child);
  onlyChildBuf.fill(0);
  onlyChildBuf[0] = 0x01;
  onlyChildBuf.set(child, 1 + nib * 32);
  return sha256(onlyChildBuf);
}

/** The pure-TS path of onlyChildHash, whatever the runtime (tests check it on Node too). */
export function onlyChildHashPortable(nib: number, child: Uint8Array): Uint8Array {
  const skip = ONLY_CHILD_SKIP[nib]!;
  onlyChildBuf.fill(0, skip);
  if (skip === 0) onlyChildBuf[0] = 0x01;
  onlyChildBuf.set(child, 1 + nib * 32);
  return resume(ONLY_CHILD_MIDSTATE[nib]!.slice(), skip, onlyChildBuf, skip);
}

const ONLY_CHILD_SKIP = Array.from({ length: 16 }, (_, nib) => Math.floor((1 + nib * 32) / 64) * 64);
const onlyChildBuf = new Uint8Array(1 + 16 * 32);

/** The state after nib's constant prefix: 0x01, then zeros, in whole blocks. */
const ONLY_CHILD_MIDSTATE: Int32Array[] = ONLY_CHILD_SKIP.map((skip) => {
  const state = H0.slice();
  const prefix = new Uint8Array(skip);
  if (skip > 0) prefix[0] = 0x01;
  const view = new DataView(prefix.buffer);
  for (let off = 0; off < skip; off += 64) compress(state, view, off);
  return state;
});
