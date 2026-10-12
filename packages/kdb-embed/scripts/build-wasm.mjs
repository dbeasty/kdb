// Builds go/wasm/embed into wasm/kdb.wasm and copies Go's matching wasm_exec.js glue next to it.
// The glue is version-locked to the compiler that produced the binary, so it is copied from the
// same toolchain at build time rather than vendored.
import { execFileSync } from "node:child_process";
import { copyFileSync, mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { brotliCompressSync, constants, gzipSync } from "node:zlib";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const goDir = join(here, "..", "..", "..", "go");
const out = join(here, "..", "wasm");
mkdirSync(out, { recursive: true });

// kdb_lite swaps the AWS S3 client for a stub (go/kdb/storage/io/s3/client_lite.go): the persistent
// engine links it, and it alone is ~21 MB of the binary, for a network path a browser never takes.
const env = { ...process.env, GOOS: "js", GOARCH: "wasm" };
execFileSync("go", ["build", "-tags", "kdb_lite", "-trimpath", "-ldflags=-s -w", "-o", join(out, "kdb.wasm"), "./wasm/embed"], {
  cwd: goDir,
  env,
  stdio: "inherit",
});

const goroot = execFileSync("go", ["env", "GOROOT"], { encoding: "utf8" }).trim();
const candidates = [join(goroot, "lib", "wasm", "wasm_exec.js"), join(goroot, "misc", "wasm", "wasm_exec.js")];
const glue = candidates.find((p) => {
  try {
    return statSync(p).isFile();
  } catch {
    return false;
  }
});
if (glue === undefined) throw new Error(`wasm_exec.js not found under ${goroot}`);
copyFileSync(glue, join(out, "wasm_exec.js"));

// Ship a precompressed copy. The wire size is what a browser pays, and brotli beats gzip by about a
// third on this binary; a static host can serve kdb.wasm.br with `Content-Encoding: br`.
const wasm = readFileSync(join(out, "kdb.wasm"));
const br = brotliCompressSync(wasm, {
  params: { [constants.BROTLI_PARAM_QUALITY]: 11, [constants.BROTLI_PARAM_LGWIN]: 24, [constants.BROTLI_PARAM_SIZE_HINT]: wasm.length },
});
writeFileSync(join(out, "kdb.wasm.br"), br);
const mb = (n) => (n / 1e6).toFixed(1);
console.log(`built kdb.wasm: ${mb(wasm.length)} MB raw, ${mb(gzipSync(wasm, { level: 9 }).length)} MB gzip, ${mb(br.length)} MB brotli (kdb.wasm.br)`);
