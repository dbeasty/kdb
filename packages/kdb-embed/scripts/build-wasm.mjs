// Builds go/wasm/embed into wasm/kdb.wasm and copies Go's matching wasm_exec.js glue next to it.
// The glue is version-locked to the compiler that produced the binary, so it is copied from the
// same toolchain at build time rather than vendored.
import { execFileSync } from "node:child_process";
import { copyFileSync, mkdirSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const goDir = join(here, "..", "..", "..", "go");
const out = join(here, "..", "wasm");
mkdirSync(out, { recursive: true });

const env = { ...process.env, GOOS: "js", GOARCH: "wasm" };
execFileSync("go", ["build", "-trimpath", "-ldflags=-s -w", "-o", join(out, "kdb.wasm"), "./wasm/embed"], {
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
console.log(`built ${join(out, "kdb.wasm")} (${(statSync(join(out, "kdb.wasm")).size / 1e6).toFixed(1)} MB)`);
