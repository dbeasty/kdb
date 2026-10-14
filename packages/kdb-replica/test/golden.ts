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

export function load<T>(file: string): T {
  return JSON.parse(readFileSync(join(here, "golden", file), "utf8")) as T;
}
