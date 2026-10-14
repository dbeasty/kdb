/**
 * A real Go node for the live interop tests: go/cmd/kdb-replicahost, built once per process and
 * spawned per test. It serves peer sync v2 over WebSocket with token auth, and answers JSON commands
 * on stdin so a test can write to the host and read its state back.
 */

import { execFileSync, spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const goDir = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..", "go");

export function goAvailable(): boolean {
  try {
    execFileSync("go", ["version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

let built: string | undefined;

function build(): string {
  if (built === undefined) {
    const out = join(mkdtempSync(join(tmpdir(), "kdb-replicahost-")), "kdb-replicahost");
    execFileSync("go", ["build", "-o", out, "./cmd/kdb-replicahost"], { cwd: goDir, stdio: "inherit" });
    built = out;
  }
  return built;
}

export interface Host {
  url: string;
  cmd(c: Record<string, unknown>): Promise<Record<string, unknown>>;
  put(ns: string, id: string, json: unknown): Promise<string>;
  head(ns: string): Promise<{ head: string; tree: string }>;
  docs(ns: string): Promise<Record<string, string>>;
  stop(): Promise<void>;
}

/**
 * Starts a host on a fresh data directory: file-backed like a deployed node, which matters - the
 * generation numbers squash's backlog comes from are a file-graph feature.
 */
export async function startHost(args: string[] = []): Promise<Host> {
  const dir = mkdtempSync(join(tmpdir(), "kdb-replicahost-data-"));
  const proc: ChildProcessWithoutNullStreams = spawn(build(), ["-dir", dir, ...args], { stdio: ["pipe", "pipe", "pipe"] });
  let stderr = "";
  proc.stderr.on("data", (c: Buffer) => (stderr += c.toString()));
  let buf = "";
  const waiting: ((line: string) => void)[] = [];
  const lines: string[] = [];
  proc.stdout.on("data", (c: Buffer) => {
    buf += c.toString();
    let i;
    while ((i = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, i);
      buf = buf.slice(i + 1);
      const w = waiting.shift();
      if (w) w(line);
      else lines.push(line);
    }
  });
  const nextLine = () =>
    new Promise<string>((resolve, reject) => {
      const l = lines.shift();
      if (l !== undefined) return resolve(l);
      const timer = setTimeout(() => reject(new Error(`kdb-replicahost: no answer in 60s\n${stderr}`)), 60_000);
      waiting.push((line) => {
        clearTimeout(timer);
        resolve(line);
      });
    });
  const ready = await nextLine();
  const m = /^ready (ws:\/\/\S+)$/.exec(ready);
  if (!m) throw new Error(`kdb-replicahost: unexpected first line ${ready}\n${stderr}`);
  let queue = Promise.resolve();
  const cmd = (c: Record<string, unknown>) => {
    const run = queue.then(async () => {
      proc.stdin.write(JSON.stringify(c) + "\n");
      const res = JSON.parse(await nextLine()) as Record<string, unknown>;
      if (typeof res.error === "string") throw new Error(`kdb-replicahost ${String(c.op)}: ${res.error}`);
      return res;
    });
    queue = run.then(() => undefined, () => undefined);
    return run;
  };
  return {
    url: m[1]!,
    cmd,
    put: async (ns, id, json) => String((await cmd({ op: "put", ns, id, json: typeof json === "string" ? json : JSON.stringify(json) })).head),
    head: async (ns) => (await cmd({ op: "head", ns })) as { head: string; tree: string },
    docs: async (ns) => (await cmd({ op: "docs", ns })).docs as Record<string, string>,
    stop: () =>
      new Promise<void>((resolve) => {
        proc.once("exit", () => {
          rmSync(dir, { recursive: true, force: true });
          resolve();
        });
        proc.stdin.end();
        setTimeout(() => proc.kill("SIGKILL"), 5000).unref();
      }),
  };
}
