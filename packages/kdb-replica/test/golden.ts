/**
 * Loads the fixtures written by `cd go && go run ./cmd/kdb-tsfixtures` (go/cmd/kdb-tsfixtures/replica.go).
 * They come from the Go code a node runs, so agreeing with them is agreeing with a live host.
 */

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import type { Op } from "../src/commit.ts";

const here = dirname(fileURLToPath(import.meta.url));

export interface CommitFixture {
  name: string;
  payloadHex: string;
  hash: string;
  parents: string[];
  namespace: string;
  transactionId: string;
  timestampMicros: string;
  authorNodeId: string;
  ops: (Op & Record<string, unknown>)[];
  treeHash: string;
  schemaHash?: string;
  attachmentRoot?: string;
  message: string;
}

export interface TreeEntry {
  id: string;
  contentHash: string;
}

export interface TreeFixture {
  name: string;
  entries: TreeEntry[];
  root: string;
}

export interface TreeEditFixture {
  name: string;
  steps: { edits: TreeEntry[]; root: string }[];
}

export interface ChainFixture {
  namespace: string;
  commits: CommitFixture[];
  steps: { from: string; to: string; commits: string[]; docs: Record<string, string> }[];
}

/** A fixture op (go/cmd/kdb-tsfixtures replicaOpFixture) as an Op. */
export function opFromFixture(o: Record<string, unknown>): Op {
  const str = (k: string) => String(o[k] ?? "");
  switch (o.kind) {
    case "write": return { kind: "write", docId: str("docId"), json: str("json") };
    case "delete": return { kind: "delete", docId: str("docId") };
    case "fileWrite": return { kind: "fileWrite", path: str("path"), blobHash: str("blobHash") };
    case "schemaMigration": return { kind: "schemaMigration", migrationId: str("migrationId"), payload: str("payload") };
    case "attachmentPut":
      return {
        kind: "attachmentPut", docId: str("docId"), attachmentId: str("attachmentId"), name: str("name"),
        scheme: Number(o.scheme ?? 0), contentHash: str("contentHash"), size: BigInt(Number(o.size ?? 0)), mime: str("mime"),
      };
    case "attachmentDelete": return { kind: "attachmentDelete", docId: str("docId"), attachmentId: str("attachmentId") };
  }
  throw new Error(`unknown fixture op ${String(o.kind)}`);
}

export function load<T>(file: string): T {
  return JSON.parse(readFileSync(join(here, "golden", file), "utf8")) as T;
}
