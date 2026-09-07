/**
 * Conformance tests (Component 63 §9.1): frames, envelopes, byte arrays and content hashes,
 * every one of them checked against fixtures the Go implementation produced.
 */

import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { bytesFromWire, bytesToWire } from "../src/wire/bytes.ts";
import { decodeMessage, encodeMessage } from "../src/wire/envelope.ts";
import {
  DEFAULT_MAX_FRAME_BYTES,
  FRAME_HEADER_SIZE,
  FrameReader,
  decodeFrame,
  decodeHeader,
  encodeFrame,
} from "../src/wire/frame.ts";
import { contentHash, encodeDocumentBody } from "../src/wire/hash.ts";
import { readLeb128U32, writeLeb128U32 } from "../src/wire/leb128.ts";
import { MSG, TYPE_CODE_BY_KIND, type MessageKind } from "../src/wire/messages.ts";
import { uuidFromBytes, uuidToBytes } from "../src/wire/uuid.ts";
import { KdbProtocolError } from "../src/errors.ts";
import {
  bytesToHex,
  frameFixtures,
  hashFixtures,
  hexToBytes,
  transactionFixtures,
} from "./golden.ts";

describe("frame header", () => {
  it("decodes every golden frame's header exactly", () => {
    assert.ok(frameFixtures.length > 0, "fixtures missing - run: cd go && go run ./cmd/kdb-tsfixtures");
    for (const fixture of frameFixtures) {
      const frame = hexToBytes(fixture.frameHex);
      const header = decodeHeader(frame);
      assert.equal(header.typeCode, fixture.typeCode, fixture.name);
      assert.equal(header.correlationId, fixture.correlationId, fixture.name);
      assert.equal(header.protocolVersion, 1, fixture.name);
      assert.equal(header.frameLength, frame.length, fixture.name);
      assert.equal(header.payloadLength, frame.length - FRAME_HEADER_SIZE, fixture.name);
    }
  });

  it("parses each golden frame's envelope into the payload the fixture recorded", () => {
    for (const fixture of frameFixtures) {
      const { envelope } = decodeFrame(hexToBytes(fixture.frameHex));
      assert.deepEqual(envelope, JSON.parse(fixture.envelopeJson), fixture.name);
      assert.equal(envelope["kind"], fixture.kind, fixture.name);
    }
  });

  it("re-encodes each golden frame byte-for-byte", () => {
    for (const fixture of frameFixtures) {
      const original = hexToBytes(fixture.frameHex);
      const envelope = JSON.parse(fixture.envelopeJson) as Record<string, unknown>;
      const reencoded = encodeFrame(fixture.typeCode, fixture.correlationId, envelope);
      // Byte equality holds only because JSON.stringify preserves key insertion order and
      // JSON.parse preserves it from the source text - which is exactly the property that
      // makes a round trip meaningful rather than merely "semantically equal".
      assert.equal(bytesToHex(reencoded), fixture.frameHex, fixture.name);
    }
  });
});

describe("frame validation", () => {
  it("rejects a frame shorter than the header", () => {
    assert.throws(() => decodeHeader(new Uint8Array(11)), KdbProtocolError);
  });

  it("rejects a frame whose declared length exceeds the buffer", () => {
    // The hazard frame.go:41-47 records: a WebSocket message arrives whole and unvalidated, so
    // a truncated or hostile prefix reaches the decoder directly. Must be a decode error, never
    // a RangeError out of DataView.
    const frame = new Uint8Array(20);
    new DataView(frame.buffer).setInt32(0, 4096, true);
    assert.throws(
      () => decodeHeader(frame),
      (error: unknown) => error instanceof KdbProtocolError && /declared length/.test(String(error)),
    );
  });

  it("rejects a frame length below the header size or above the maximum", () => {
    const tooSmall = new Uint8Array(16);
    new DataView(tooSmall.buffer).setInt32(0, 4, true);
    assert.throws(() => decodeHeader(tooSmall), KdbProtocolError);

    const tooBig = new Uint8Array(16);
    new DataView(tooBig.buffer).setInt32(0, DEFAULT_MAX_FRAME_BYTES + 1, true);
    assert.throws(() => decodeHeader(tooBig), KdbProtocolError);
  });

  it("rejects an unknown payload encoding tag rather than misparsing it", () => {
    const frame = encodeFrame(MSG.SQL_EXEC, 1, { kind: "sqlExec", sqlExec: {} });
    frame[FRAME_HEADER_SIZE] = 0x7f;
    assert.throws(() => decodeFrame(frame), /payload encoding tag/);
  });

  it("rejects a payload that is not a JSON object", () => {
    const body = new TextEncoder().encode("[1,2,3]");
    const frame = new Uint8Array(FRAME_HEADER_SIZE + 1 + body.length);
    const view = new DataView(frame.buffer);
    view.setInt32(0, frame.length, true);
    view.setUint16(4, MSG.SQL_EXEC, true);
    view.setInt16(6, 1, true);
    view.setInt32(8, 1, true);
    frame[FRAME_HEADER_SIZE] = 0x01;
    frame.set(body, FRAME_HEADER_SIZE + 1);
    assert.throws(() => decodeFrame(frame), /not a JSON object/);
  });
});

describe("FrameReader", () => {
  const frames = frameFixtures.slice(0, 4).map((f) => hexToBytes(f.frameHex));

  it("reassembles frames split across arbitrary chunk boundaries", () => {
    const stream = concat(frames);
    for (const chunkSize of [1, 3, 7, 64, stream.length]) {
      const reader = new FrameReader();
      const seen: Uint8Array[] = [];
      for (let i = 0; i < stream.length; i += chunkSize) {
        seen.push(...reader.push(stream.subarray(i, Math.min(i + chunkSize, stream.length))));
      }
      assert.equal(seen.length, frames.length, `chunkSize ${chunkSize}`);
      for (let i = 0; i < frames.length; i++) {
        assert.equal(bytesToHex(seen[i]!), bytesToHex(frames[i]!), `chunkSize ${chunkSize}`);
      }
    }
  });

  it("emits several coalesced frames from one chunk", () => {
    const reader = new FrameReader();
    assert.equal(reader.push(concat(frames)).length, frames.length);
  });

  it("holds back an incomplete trailing frame", () => {
    const reader = new FrameReader();
    const partial = frames[0]!.subarray(0, frames[0]!.length - 2);
    assert.equal(reader.push(partial).length, 0);
    assert.equal(reader.push(frames[0]!.subarray(frames[0]!.length - 2)).length, 1);
  });
});

describe("envelope", () => {
  it("round trips every client message kind", () => {
    for (const kind of Object.keys(TYPE_CODE_BY_KIND) as MessageKind[]) {
      const payload = { namespace: "app/users", marker: kind };
      const decoded = decodeMessage(encodeMessage(kind, 42, payload));
      assert.equal(decoded.kind, kind);
      assert.equal(decoded.correlationId, 42);
      assert.deepEqual(decoded.payload, payload);
    }
  });

  it("stamps handshakeAck with the same type code as handshake", () => {
    // The asymmetry that breaks a decoder switching on typeCode alone: every other
    // request/reply pair has distinct codes, this one does not.
    assert.equal(TYPE_CODE_BY_KIND.handshakeAck, TYPE_CODE_BY_KIND.handshake);
    assert.equal(TYPE_CODE_BY_KIND.handshakeAck, MSG.HANDSHAKE);

    const ack = frameFixtures.find((f) => f.name === "handshakeAckAccepted");
    assert.ok(ack, "handshakeAckAccepted fixture missing");
    assert.equal(ack.typeCode, MSG.HANDSHAKE);
    // ...and the Go server really does send it that way, so dispatch must use `kind`.
    assert.equal(decodeMessage(hexToBytes(ack.frameHex)).kind, "handshakeAck");
  });

  it("rejects an envelope with no kind", () => {
    assert.throws(() => decodeMessage(encodeFrame(MSG.SQL_EXEC, 1, { sqlExec: {} })), /no `kind`/);
  });

  it("rejects an envelope whose payload key is missing", () => {
    assert.throws(
      () => decodeMessage(encodeFrame(MSG.SQL_EXEC, 1, { kind: "sqlExec" })),
      /carries no "sqlExec" payload/,
    );
  });
});

describe("leb128", () => {
  it("round trips values across the byte-count boundaries", () => {
    for (const value of [0, 1, 127, 128, 255, 256, 16383, 16384, 400, 0xffffffff]) {
      const out: number[] = [];
      writeLeb128U32(out, value);
      const decoded = readLeb128U32(Uint8Array.from(out), 0);
      assert.equal(decoded.value, value);
      assert.equal(decoded.bytesRead, out.length);
    }
  });

  it("uses the multi-byte form exactly where Go does", () => {
    const one: number[] = [];
    writeLeb128U32(one, 127);
    assert.deepEqual(one, [0x7f]);
    const two: number[] = [];
    writeLeb128U32(two, 128);
    assert.deepEqual(two, [0x80, 0x01]);
  });

  it("rejects a truncated varint", () => {
    assert.throws(() => readLeb128U32(Uint8Array.from([0x80]), 0), /truncated/);
  });
});

describe("ByteArray wire convention", () => {
  it("encodes bytes as an array of numbers, never base64 and never an object", () => {
    const encoded = bytesToWire(new Uint8Array([123, 34, 105, 0, 255]));
    assert.ok(Array.isArray(encoded));
    assert.deepEqual(encoded, [123, 34, 105, 0, 255]);
    // The three wrong answers this convention exists to rule out.
    assert.equal(JSON.stringify(encoded), "[123,34,105,0,255]");
    assert.notEqual(JSON.stringify(new Uint8Array([123])), "[123]");
  });

  it("encodes empty bytes as []", () => {
    assert.deepEqual(bytesToWire(new Uint8Array(0)), []);
  });

  it("accepts the signed values a Kotlin Byte produces for the same octets", () => {
    // Kotlin's Byte is signed, so a JVM peer writes -1 where Go writes 255.
    assert.deepEqual(Array.from(bytesFromWire([-1, -128, 127], "test")), [255, 128, 127]);
  });

  it("rejects a base64 string, which is what a naive Go encoder would have sent", () => {
    assert.throws(() => bytesFromWire("eyJhIjoxfQ==", "reportBytes"), /must be a JSON array/);
  });

  it("round trips through the transaction fixtures' wire form", () => {
    for (const fixture of transactionFixtures) {
      const bytes = bytesFromWire(fixture.wireForm, fixture.name);
      assert.equal(new TextDecoder().decode(bytes), fixture.json, fixture.name);
      assert.deepEqual(bytesToWire(bytes), fixture.wireForm, fixture.name);
    }
  });
});

describe("uuid", () => {
  it("round trips through raw bytes", () => {
    const id = "6f9619ff-8b86-d011-b42d-00cf4fc964ff";
    assert.equal(uuidFromBytes(uuidToBytes(id)), id);
  });

  it("produces the big-endian bytes the canonical encoder embeds", () => {
    assert.equal(
      bytesToHex(uuidToBytes("6f9619ff-8b86-d011-b42d-00cf4fc964ff")),
      "6f9619ff8b86d011b42d00cf4fc964ff",
    );
  });

  it("rejects a non-UUID document id with an argument error, not a server round trip", () => {
    assert.throws(() => uuidToBytes("not-a-uuid"), /is not a UUID/);
  });
});

describe("content hash", () => {
  it("reproduces the Go canonical DocumentBody encoding byte-for-byte", () => {
    assert.ok(hashFixtures.length > 0, "fixtures missing - run: cd go && go run ./cmd/kdb-tsfixtures");
    for (const fixture of hashFixtures) {
      assert.equal(
        bytesToHex(encodeDocumentBody(fixture.docId, fixture.json)),
        fixture.encodedHex,
        fixture.name,
      );
    }
  });

  it("reproduces the Go content hash", async () => {
    for (const fixture of hashFixtures) {
      assert.equal(await contentHash(fixture.docId, fixture.json), fixture.contentHash, fixture.name);
    }
  });

  it("measures the JSON length in UTF-8 bytes, not characters", () => {
    // The specific way a from-scratch encoder diverges: Go's appendLebString uses len(s), the
    // byte count. Using json.length would pass every ASCII fixture and fail on this one.
    const nonAscii = hashFixtures.find((f) => f.name === "nonAscii");
    assert.ok(nonAscii, "nonAscii fixture missing");
    const json = nonAscii.json;
    assert.notEqual(json.length, new TextEncoder().encode(json).length, "fixture is not multi-byte");
    assert.equal(bytesToHex(encodeDocumentBody(nonAscii.docId, json)), nonAscii.encodedHex);
  });
});

function concat(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((sum, c) => sum + c.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}
