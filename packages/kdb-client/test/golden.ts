/**
 * Loads the golden fixtures written by `cd go && go run ./cmd/kdb-tsfixtures`.
 *
 * These are produced by the real Go encoder, so a disagreement here is a disagreement with a
 * live server - which is the only way a third hand-written implementation of this wire format
 * can honestly claim to be compatible with the other two.
 */

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));

export interface FrameFixture {
  name: string;
  kind: string;
  typeCode: number;
  correlationId: number;
  frameHex: string;
  envelopeJson: string;
}

export interface HashFixture {
  name: string;
  docId: string;
  json: string;
  encodedHex: string;
  contentHash: string;
}

export interface TransactionFixture {
  name: string;
  json: string;
  wireForm: number[];
}

function load<T>(file: string): T {
  return JSON.parse(readFileSync(join(here, "golden", file), "utf8")) as T;
}

export const frameFixtures = load<FrameFixture[]>("frames.json");
export const hashFixtures = load<HashFixture[]>("hashes.json");
export const transactionFixtures = load<TransactionFixture[]>("transactions.json");

export function hexToBytes(hex: string): Uint8Array {
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

export function bytesToHex(bytes: Uint8Array): string {
  let out = "";
  for (const b of bytes) out += b.toString(16).padStart(2, "0");
  return out;
}
