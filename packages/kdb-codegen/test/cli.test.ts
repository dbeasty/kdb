import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { copyFileSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const cli = join(root, "dist", "cli.js");
const schema = join(root, "test", "fixtures", "kdb.schema.json");

const run = (...args: string[]) => spawnSync(process.execPath, [cli, ...args], { encoding: "utf8" });

describe("kdb-codegen CLI", () => {
  const dir = mkdtempSync(join(tmpdir(), "kdb-codegen-"));
  const out = join(dir, "nested", "kdb.ts");

  it("writes the file, creating directories", () => {
    const r = run("--schema", schema, "--out", out);
    assert.equal(r.status, 0, r.stderr);
    assert.match(readFileSync(out, "utf8"), /export interface User/);
  });

  it("produces exactly the committed generated file (so test/generated cannot go stale)", () => {
    assert.equal(readFileSync(out, "utf8"), readFileSync(join(root, "test", "generated", "kdb.ts"), "utf8"));
  });

  it("--check passes when current, fails when stale or missing", () => {
    assert.equal(run("--schema", schema, "--out", out, "--check").status, 0);
    writeFileSync(out, readFileSync(out, "utf8") + "// edited\n");
    const stale = run("--schema", schema, "--out", out, "--check");
    assert.equal(stale.status, 1);
    assert.match(stale.stderr, /stale/);
    const missing = run("--schema", schema, "--out", join(dir, "nope.ts"), "--check");
    assert.equal(missing.status, 1);
    assert.match(missing.stderr, /missing/);
  });

  it("--check detects schema drift: a changed schema makes the old file stale", () => {
    const changed = join(dir, "changed.json");
    const doc = JSON.parse(readFileSync(schema, "utf8"));
    doc.namespaces["app/users"].fields.push({ name: "extra", type: "string" });
    writeFileSync(changed, JSON.stringify(doc));
    copyFileSync(join(root, "test", "generated", "kdb.ts"), join(dir, "current.ts"));
    assert.equal(run("--schema", schema, "--out", join(dir, "current.ts"), "--check").status, 0);
    assert.equal(run("--schema", changed, "--out", join(dir, "current.ts"), "--check").status, 1);
  });

  it("reports a bad schema with exit 2 and a useful message", () => {
    const bad = join(dir, "bad.json");
    writeFileSync(bad, JSON.stringify({ namespaces: { "a/b": { fields: [{ name: "x", type: "decimal" }] } } }));
    const r = run("--schema", bad, "--out", join(dir, "x.ts"));
    assert.equal(r.status, 2);
    assert.match(r.stderr, /unknown schema field type "decimal"/);
  });

  it("requires its arguments", () => {
    assert.equal(run().status, 2);
    assert.equal(run("--help").status, 0);
  });
});
