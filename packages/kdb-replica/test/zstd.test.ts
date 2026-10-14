import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import zlib from "node:zlib";
import { fromHex, utf8 } from "../src/bytes.ts";
import { zstdDecompress } from "../src/zstd.ts";

const goldenDir = join(dirname(fileURLToPath(import.meta.url)), "../../../go/testdata/golden/physical");

/** Deterministic bytes from a 32-bit LCG. */
function lcgBytes(n: number, seed: number): Uint8Array {
  const out = new Uint8Array(n);
  let x = seed >>> 0;
  for (let i = 0; i < n; i++) {
    x = (Math.imul(x, 1664525) + 1013904223) >>> 0;
    out[i] = x >>> 24;
  }
  return out;
}

/**
 * The zstd golden corpus payload: JSON-like records drawn from a 32-bit LCG, truncated to exactly
 * size bytes. Byte-identical to zstdCorpusJSON (go/kdb/interop/zstd_physical_golden_test.go) and
 * ZstdTestCorpus.json (kdb-compression).
 */
function corpusJson(size: number): Uint8Array {
  let x = 0x12345678;
  const next = (): number => {
    x = (Math.imul(x, 1664525) + 1013904223) >>> 0;
    return x >>> 16;
  };
  let s = "";
  for (let i = 0; s.length < size; i++) {
    const name = next() % 1000;
    const t1 = next() % 17;
    const t2 = next() % 17;
    const score = next() % 100000;
    const active = next() % 2 === 0;
    s += `{"id":${i},"name":"user-${name}","tags":["t${t1}","t${t2}"],"score":${score},"active":${active}}\n`;
  }
  return utf8(s.slice(0, size));
}

function corpusPayload(spec: string): Uint8Array {
  if (spec === "empty") return new Uint8Array(0);
  const m = /^json(\d+)k$/.exec(spec);
  assert.ok(m, `unknown corpus payload ${spec}`);
  return corpusJson(Number(m[1]) * 1000);
}

function nodeZstd(data: Uint8Array, level: number, checksum: boolean): Uint8Array {
  const params: Record<number, number> = { [zlib.constants.ZSTD_c_compressionLevel]: level };
  if (checksum) params[zlib.constants.ZSTD_c_checksumFlag] = 1;
  return new Uint8Array(zlib.zstdCompressSync(data, { params }));
}

function assertBytes(got: Uint8Array, want: Uint8Array, what: string): void {
  assert.equal(got.length, want.length, `${what}: length`);
  assert.ok(Buffer.from(got).equals(Buffer.from(want)), `${what}: bytes differ`);
}

test("decodes what Node's native zstd writes, at every level, with and without a checksum", () => {
  const text = utf8("The quick brown fox jumps over the lazy dog. ");
  const repetitive = utf8("kdb replica zstd differential test line, mostly the same each time. ".repeat(16000)); // ~1 MB
  const inputs: [string, Uint8Array][] = [
    ["empty", new Uint8Array(0)],
    ["1 byte", new Uint8Array([0x42])],
    ["short text", text],
    ["100 KB random", lcgBytes(100_000, 1)],
    ["1 MB repetitive", repetitive],
    ["json 300k", corpusJson(300_000)],
    ["all zero 200k", new Uint8Array(200_000)],
  ];
  for (const d of [-3, -1, 0, 1, 3]) {
    const n = 128 * 1024 + d;
    inputs.push([`json ${n}`, corpusJson(n)]);
    inputs.push([`random ${n}`, lcgBytes(n, n)]);
  }
  for (const [name, data] of inputs) {
    for (const level of [1, 3, 9, 19]) {
      for (const checksum of [false, true]) {
        const frame = nodeZstd(data, level, checksum);
        assertBytes(zstdDecompress(frame, data.length), data, `${name} level ${level} checksum ${checksum}`);
      }
    }
  }
});

/** Compresses through Node's streaming encoder in 64 KiB writes: no pledged size, so no content size field. */
async function nodeZstdStreamed(data: Uint8Array, checksum: boolean): Promise<Uint8Array> {
  const enc = zlib.createZstdCompress(checksum ? { params: { [zlib.constants.ZSTD_c_checksumFlag]: 1 } } : {});
  const parts: Buffer[] = [];
  enc.on("data", (c: Buffer) => parts.push(c));
  const done = new Promise<void>((resolve, reject) => {
    enc.on("end", resolve);
    enc.on("error", reject);
  });
  for (let i = 0; i < data.length; i += 65536) enc.write(data.subarray(i, i + 65536));
  enc.end();
  await done;
  return new Uint8Array(Buffer.concat(parts));
}

test("decodes Node's streamed frames, which carry no content size", async () => {
  const data = corpusJson(500_000);
  const frame = await nodeZstdStreamed(data, true);
  assert.equal(frame[4]! >>> 6, 0, "no frame content size field");
  assertBytes(zstdDecompress(frame, data.length), data, "streamed");
});

test("decodes concatenated frames and skips skippable frames", () => {
  const a = corpusJson(10_000);
  const b = lcgBytes(5_000, 7);
  const skippable = new Uint8Array([0x5a, 0x2a, 0x4d, 0x18, 3, 0, 0, 0, 9, 9, 9]);
  const input = Buffer.concat([nodeZstd(a, 3, true), skippable, nodeZstd(b, 1, false), skippable]);
  assertBytes(zstdDecompress(input, a.length + b.length), Buffer.concat([a, b]), "concatenated");
  assert.equal(zstdDecompress(new Uint8Array(0), 0).length, 0, "no frames at all");
});

// The original single-payload interop fixtures (go/testdata/golden/physical/<side>/zstd_body.hex):
// Go's is one raw block, Kotlin's (zstd-jni) one compressed block; both empty bodies.
const GO_ZSTD_BODY =
  "28b52ffd04001102006b6462207a73746420696e7465726f702066697874757265207061796c6f61642c206c6f6e6720656e6f75676820746f2061637475616c6c7920636f6d7072657373b2229fd1";
const KOTLIN_ZSTD_BODY =
  "28b52ffd2042ed010022440e14a0b70d5c48da518afd6aa397ed54556363ffb30629213ac58957a6a925e6af87a2aee8a24f0367cf65c788d41a46f039c255e3db7438382200";
const KOTLIN_ZSTD_EMPTY_BODY = "28b52ffd2000010000";
// The pure-Kotlin encoder's empty frame (kotlin/zstd_corpus_empty_purekotlin.hex).
const PURE_KOTLIN_EMPTY = "28b52ffd240001000099e9d851";

test("decodes the Go and Kotlin zstd_body golden vectors", () => {
  const want = utf8("kdb zstd interop fixture payload, long enough to actually compress");
  assertBytes(zstdDecompress(fromHex(GO_ZSTD_BODY), 1 << 20), want, "go/zstd_body.hex");
  assertBytes(zstdDecompress(fromHex(KOTLIN_ZSTD_BODY), 1 << 20), want, "kotlin/zstd_body.hex");
  assert.equal(zstdDecompress(fromHex(KOTLIN_ZSTD_EMPTY_BODY), 1 << 20).length, 0);
  assert.equal(zstdDecompress(fromHex(PURE_KOTLIN_EMPTY), 1 << 20).length, 0);
});

test("the corpus payload generator matches Go and Kotlin", () => {
  const want = `{"id":0,"name":"user-19","tags":["t15","t1"],"score":19292,"active":true}\n`;
  assert.equal(Buffer.from(corpusJson(want.length)).toString(), want);
});

test("decodes every frame of the zstd golden corpus (klauspost, zstd-jni, pure-Kotlin encoder)", (t) => {
  const required = [
    "go/zstd_corpus_json20k_l1.hex", "go/zstd_corpus_json20k_l3.hex", "go/zstd_corpus_json20k_l7.hex",
    "go/zstd_corpus_json20k_l19.hex", "go/zstd_corpus_json140k_l3.hex",
    "kotlin/zstd_corpus_json20k_l1.hex", "kotlin/zstd_corpus_json20k_l3.hex", "kotlin/zstd_corpus_json20k_l19.hex",
    "kotlin/zstd_corpus_json20k_stream.hex", "kotlin/zstd_corpus_json140k_l3.hex",
    "kotlin/zstd_corpus_json20k_purekotlin.hex", "kotlin/zstd_corpus_json140k_purekotlin.hex",
    "kotlin/zstd_corpus_empty_purekotlin.hex",
  ];
  if (!existsSync(goldenDir)) {
    t.skip(`no golden fixtures at ${goldenDir}`);
    return;
  }
  for (const name of required) {
    const frame = fromHex(readFileSync(join(goldenDir, name), "utf8").trim());
    const want = corpusPayload(name.replace(/^.*zstd_corpus_/, "").split("_")[0]!);
    assertBytes(zstdDecompress(frame, want.length), want, name);
  }
});

test("rejects truncated frames, bad magic, oversized output and corruption", async () => {
  const data = corpusJson(50_000);
  const frame = nodeZstd(data, 3, true);
  const throwsZstd = (f: () => unknown, re: RegExp, what: string): void =>
    assert.throws(f, (e: unknown) => e instanceof Error && e.message.startsWith("kdb replica: zstd: ") && re.test(e.message), what);

  for (const cut of [1, 3, 4, 5, 6, 10, frame.length >> 1, frame.length - 5, frame.length - 1]) {
    throwsZstd(() => zstdDecompress(frame.subarray(0, cut), data.length), /./, `truncated to ${cut}`);
  }
  const bad = frame.slice();
  bad[0] = 0x29;
  throwsZstd(() => zstdDecompress(bad, data.length), /bad frame magic/, "bad magic");
  throwsZstd(() => zstdDecompress(frame, data.length - 1), /exceeds max/, "declared size over max");
  // Without a declared size, the limit is still enforced while decoding.
  const streamed = await nodeZstdStreamed(data, false);
  assert.equal(streamed[4]! >>> 6, 0);
  throwsZstd(() => zstdDecompress(streamed, data.length - 1), /exceeds max/, "streamed over max");
  // A flipped checksum byte, and a flipped byte in the middle of the compressed data.
  const badSum = frame.slice();
  badSum[badSum.length - 1]! ^= 1;
  throwsZstd(() => zstdDecompress(badSum, data.length), /checksum/, "checksum mismatch");
  let mangledRejected = 0;
  for (let i = 20; i < frame.length - 4; i += 97) {
    const m = frame.slice();
    m[i]! ^= 0x10;
    try {
      const out = zstdDecompress(m, data.length);
      assert.fail(`flipping byte ${i} decoded ${out.length} bytes despite the checksum`);
    } catch (e) {
      assert.ok(e instanceof Error && e.message.startsWith("kdb replica: zstd: "), `byte ${i}: ${String(e)}`);
      mangledRejected++;
    }
  }
  assert.ok(mangledRejected > 10);
  // Dictionaries are refused: a frame header descriptor naming a 1-byte dictionary id.
  throwsZstd(() => zstdDecompress(fromHex("28b52ffd210701000000"), 100), /dictionar/, "dictionary");
  throwsZstd(() => zstdDecompress(fromHex("28b52ffd"), 100), /truncated/, "magic only");
  throwsZstd(() => zstdDecompress(fromHex("28b52f"), 100), /trailing bytes/, "short magic");
});

test("throughput: a 4 MiB JSON payload", () => {
  const data = corpusJson(4 << 20);
  const frame = nodeZstd(data, 3, true);
  for (let i = 0; i < 2; i++) zstdDecompress(frame, data.length); // warm up
  const runs = 5;
  const t0 = performance.now();
  let out: Uint8Array = new Uint8Array(0);
  for (let i = 0; i < runs; i++) out = zstdDecompress(frame, data.length);
  const ms = (performance.now() - t0) / runs;
  assertBytes(out, data, "4 MiB");
  const t1 = performance.now();
  for (let i = 0; i < runs; i++) zlib.zstdDecompressSync(frame);
  const nativeMs = (performance.now() - t1) / runs;
  console.log(
    `zstd decode: ${frame.length} -> ${data.length} bytes, ${ms.toFixed(1)} ms = ${(data.length / 1e6 / (ms / 1000)).toFixed(0)} MB/s` +
      ` (native ${(data.length / 1e6 / (nativeMs / 1000)).toFixed(0)} MB/s)`,
  );
});
