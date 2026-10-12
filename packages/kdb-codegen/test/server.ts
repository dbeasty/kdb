import { spawn, execFileSync, type ChildProcessWithoutNullStreams } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

export const goDir = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..", "go");

export function goAvailable(): boolean {
  try {
    execFileSync("go", ["version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

let built: string | undefined;

/** Builds go/cmd/kdb-tsinterop once per process, so each suite does not pay for `go run`'s compile. */
function buildServer(): string {
  if (built === undefined) {
    const out = join(mkdtempSync(join(tmpdir(), "kdb-tsinterop-")), "kdb-tsinterop");
    execFileSync("go", ["build", "-o", out, "./cmd/kdb-tsinterop"], { cwd: goDir, stdio: "inherit" });
    built = out;
  }
  return built;
}

export interface RunningServer {
  uri: string;
  stop(): Promise<void>;
}

/** Starts an in-memory Go server with a WebSocket listener for one namespace. */
export async function startGoServer(namespace: string): Promise<RunningServer> {
  const server: ChildProcessWithoutNullStreams = spawn(buildServer(), ["-namespace", namespace], { stdio: ["pipe", "pipe", "pipe"] });
  const uri = await new Promise<string>((resolve, reject) => {
    let stdout = "";
    let stderr = "";
    const timer = setTimeout(() => reject(new Error(`server not ready in 30s\n${stderr}`)), 30_000);
    server.stdout.on("data", (chunk: Buffer) => {
      stdout += chunk.toString();
      const match = /^ready (ws:\/\/\S+)$/m.exec(stdout);
      if (match) {
        clearTimeout(timer);
        resolve(match[1]!);
      }
    });
    server.stderr.on("data", (chunk: Buffer) => (stderr += chunk.toString()));
    server.on("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`server exited early (${code})\n${stderr}`));
    });
  });
  return {
    uri,
    stop: () =>
      new Promise<void>((resolve) => {
        server.removeAllListeners("exit");
        server.on("exit", () => resolve());
        server.stdin.end();
        server.kill();
      }),
  };
}
