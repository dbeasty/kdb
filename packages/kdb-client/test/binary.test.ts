/**
 * Binary frames (encoding tag 2), checked against go/cmd/kdb-tsfixtures' binary-frames.json:
 * every message with a binary form, as the Go encoder writes it, next to the same message as a
 * JSON envelope.
 */

import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { decodeBinaryBody, encodeBinaryBody } from "../src/wire/binary.ts";
import { decodeMessage, encodeMessage } from "../src/wire/envelope.ts";
import { FRAME_HEADER_SIZE } from "../src/wire/frame.ts";
import type { MessageKind } from "../src/wire/messages.ts";
import { KdbProtocolError } from "../src/errors.ts";
import { binaryFrameFixtures, bytesToHex, hexToBytes } from "./golden.ts";

/** A decoded binary payload with its byte fields in the JSON envelope's number[] form. */
function asJsonShape(payload: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(payload)) out[k] = v instanceof Uint8Array ? Array.from(v) : v;
  return out;
}

/** The JSON envelope's payload, without the keys Go leaves out of a binary body when zero. */
function jsonPayload(fixture: { kind: string; envelopeJson: string }): Record<string, unknown> {
  const envelope = JSON.parse(fixture.envelopeJson) as Record<string, Record<string, unknown>>;
  return envelope[fixture.kind]!;
}

describe("binary frames", () => {
  it("has a fixture for every kind with a binary form", () => {
    const kinds = new Set(binaryFrameFixtures.map((f) => f.kind));
    for (const kind of [
      "sessionBegin", "sessionBeginAck", "sqlExec", "sqlResult", "txCommit", "txRollback",
      "conflictReport", "documentGet", "documentGetResult", "upsert", "upsertResult",
    ]) {
      assert.ok(kinds.has(kind), kind);
    }
  });

  it("decodes each Go binary frame to the payload its JSON envelope carries", () => {
    for (const fixture of binaryFrameFixtures) {
      const frame = hexToBytes(fixture.frameHex);
      assert.equal(frame[FRAME_HEADER_SIZE], 2, fixture.name);
      const message = decodeMessage(frame);
      assert.equal(message.kind, fixture.kind, fixture.name);
      assert.equal(message.correlationId, fixture.correlationId, fixture.name);
      const want = jsonPayload(fixture);
      const got = asJsonShape(message.payload as Record<string, unknown>);
      // Go's JSON writes some zero fields (an empty byte field is [], a nil list is null) that the
      // binary decoder fills from its defaults; compare on what the JSON says.
      for (const [key, value] of Object.entries(want)) {
        const normalized = value === null && Array.isArray(got[key]) && (got[key] as unknown[]).length === 0 ? [] : value;
        assert.deepEqual(got[key] ?? null, normalized ?? null, `${fixture.name}.${key}`);
      }
    }
  });

  it("encodes each JSON payload to the Go binary frame byte for byte", () => {
    for (const fixture of binaryFrameFixtures) {
      const frame = encodeMessage(fixture.kind as MessageKind, fixture.correlationId, jsonPayload(fixture), undefined, true);
      assert.equal(bytesToHex(frame), fixture.frameHex, fixture.name);
    }
  });

  it("keeps the JSON envelope for kinds without a binary form", () => {
    const frame = encodeMessage("lockAcquire", 3, { namespace: "n", sessionId: "s", docId: "d", ttlMillis: 1 }, undefined, true);
    assert.equal(frame[FRAME_HEADER_SIZE], 1);
    assert.equal(encodeBinaryBody("handshake", {}), undefined);
  });

  it("round trips integers the way Go's int64 varints do", () => {
    for (const retryAfterMs of [0, 1, 127, 128, 300, 2 ** 31, 2 ** 49 + 3, -1, -5000]) {
      const body = encodeBinaryBody("upsertResult", { namespace: "n", commitHex: "c", retryAfterMs })!;
      const { payload } = decodeBinaryBody(0x17, body);
      assert.equal(payload.retryAfterMs, retryAfterMs);
    }
    // -1 is ten bytes on the wire, as Go writes it.
    const body = encodeBinaryBody("upsertResult", { retryAfterMs: -1 })!;
    assert.equal(body.length, 1 + 10);
  });

  it("skips fields it does not know", () => {
    const known = encodeBinaryBody("upsertResult", { namespace: "n", commitHex: "c" })!;
    // field 99 varint 5; field 100 bytes "abc"; field 101 fixed64; field 102 fixed32
    const extra = [0x98, 0x06, 5, 0xa2, 0x06, 3, 97, 98, 99, 0xa9, 0x06, 1, 2, 3, 4, 5, 6, 7, 8, 0xb5, 0x06, 1, 2, 3, 4];
    const body = new Uint8Array([...known, ...extra]);
    const { payload } = decodeBinaryBody(0x17, body);
    assert.equal(payload.namespace, "n");
    assert.equal(payload.commitHex, "c");
  });

  it("fails cleanly on every truncation and corruption of a body", () => {
    for (const fixture of binaryFrameFixtures) {
      const body = hexToBytes(fixture.frameHex).subarray(FRAME_HEADER_SIZE + 1);
      for (let n = 0; n < body.length; n++) {
        try {
          decodeBinaryBody(fixture.typeCode, body.subarray(0, n));
        } catch (error) {
          assert.ok(error instanceof KdbProtocolError, `${fixture.name}[:${n}]: ${String(error)}`);
        }
      }
      for (let i = 0; i < body.length; i++) {
        for (const b of [0x00, 0x7f, 0x80, 0xff]) {
          const bad = body.slice();
          bad[i] = b;
          try {
            decodeBinaryBody(fixture.typeCode, bad);
          } catch (error) {
            assert.ok(error instanceof KdbProtocolError, `${fixture.name}[${i}]=${b}: ${String(error)}`);
          }
        }
      }
    }
  });
});
