/**
 * Frame framing, matching go/kdb/wire/frame.go and codec.go.
 *
 * Layout, little-endian throughout:
 *
 *   offset 0  size 4  frameLength (i32) - TOTAL frame size, header included
 *   offset 4  size 2  typeCode (u16)
 *   offset 6  size 2  protocolVersion (i16)
 *   offset 8  size 4  correlationId (i32)
 *   offset 12 size 1  payload encoding tag
 *   offset 13 size n  JSON envelope, UTF-8
 */

import { KdbProtocolError } from "../errors.ts";
import { utf8Decode, utf8Encode } from "./bytes.ts";

export const FRAME_HEADER_SIZE = 12;
export const DEFAULT_MAX_FRAME_BYTES = 16 * 1024 * 1024;
export const KDB_WIRE_PROTOCOL_VERSION = 1;
export const MIN_SUPPORTED_WIRE_PROTOCOL_VERSION = 1;

/** Payload encoding tags (`encodingTag` in go/kdb/wire/codec.go). */
export const ENCODING_JSON = 0x01;

export interface FrameHeader {
  frameLength: number;
  typeCode: number;
  protocolVersion: number;
  correlationId: number;
  payloadLength: number;
}

export interface DecodedFrame {
  header: FrameHeader;
  encodingTag: number;
  /** The parsed JSON envelope. */
  envelope: Record<string, unknown>;
}

/** Builds a complete frame: header, encoding tag, then the JSON envelope. */
export function encodeFrame(
  typeCode: number,
  correlationId: number,
  envelope: unknown,
  options: { protocolVersion?: number; maxFrameBytes?: number } = {},
): Uint8Array {
  const protocolVersion = options.protocolVersion ?? KDB_WIRE_PROTOCOL_VERSION;
  const maxFrameBytes = options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES;

  const body = utf8Encode(JSON.stringify(envelope));
  const payloadLength = 1 + body.length;
  const frameLength = FRAME_HEADER_SIZE + payloadLength;

  if (frameLength > maxFrameBytes) {
    throw new KdbProtocolError(
      `kdb: frame of ${frameLength} bytes exceeds the ${maxFrameBytes}-byte limit`,
    );
  }

  const frame = new Uint8Array(frameLength);
  const view = new DataView(frame.buffer);
  view.setInt32(0, frameLength, true);
  view.setUint16(4, typeCode, true);
  view.setInt16(6, protocolVersion, true);
  view.setInt32(8, correlationId, true);
  frame[FRAME_HEADER_SIZE] = ENCODING_JSON;
  frame.set(body, FRAME_HEADER_SIZE + 1);
  return frame;
}

/**
 * Parses the fixed 12-byte header.
 *
 * Both length checks that DecodeHeader performs are reproduced, and the second one is not
 * belt-and-braces. The comment at go/kdb/wire/frame.go:41-47 records why: a WebSocket message
 * arrives whole and unvalidated, and captured frames fed to kdb-inspect can be truncated by
 * whatever wrote them, so a buffer whose prefix claims more bytes than it carries genuinely
 * reaches this code with attacker- or corruption-controlled contents. In Go that was a
 * slice-bounds panic. Here it would be a RangeError out of DataView, which is just as wrong a
 * thing to hand a caller - so validate first, then read.
 */
export function decodeHeader(
  frame: Uint8Array,
  maxFrameBytes = DEFAULT_MAX_FRAME_BYTES,
): FrameHeader {
  if (frame.length < FRAME_HEADER_SIZE) {
    throw new KdbProtocolError("kdb: frame shorter than header");
  }
  const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
  const frameLength = view.getInt32(0, true);
  if (frameLength < FRAME_HEADER_SIZE || frameLength > maxFrameBytes) {
    throw new KdbProtocolError(
      `kdb: frame length ${frameLength} outside [${FRAME_HEADER_SIZE}, ${maxFrameBytes}]`,
    );
  }
  if (frame.length < frameLength) {
    throw new KdbProtocolError("kdb: frame shorter than its declared length");
  }
  return {
    frameLength,
    typeCode: view.getUint16(4, true),
    protocolVersion: view.getInt16(6, true),
    correlationId: view.getInt32(8, true),
    payloadLength: frameLength - FRAME_HEADER_SIZE,
  };
}

/** Parses a whole frame: header, encoding tag, and the JSON envelope. */
export function decodeFrame(
  frame: Uint8Array,
  maxFrameBytes = DEFAULT_MAX_FRAME_BYTES,
): DecodedFrame {
  const header = decodeHeader(frame, maxFrameBytes);
  if (frame.length < FRAME_HEADER_SIZE + 1) {
    throw new KdbProtocolError("kdb: frame too short for payload");
  }
  const encodingTag = frame[FRAME_HEADER_SIZE]!;
  if (encodingTag !== ENCODING_JSON) {
    throw new KdbProtocolError(
      `kdb: unsupported payload encoding tag 0x${encodingTag.toString(16)} ` +
        "(this client negotiates JSON only)",
    );
  }
  const body = frame.subarray(FRAME_HEADER_SIZE + 1, header.frameLength);

  let parsed: unknown;
  try {
    parsed = JSON.parse(utf8Decode(body));
  } catch (cause) {
    throw new KdbProtocolError("kdb: payload is not valid JSON", { cause });
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new KdbProtocolError("kdb: payload envelope is not a JSON object");
  }
  return { header, encodingTag, envelope: parsed as Record<string, unknown> };
}

/**
 * Splits a byte stream into whole frames.
 *
 * Only the TCP transport needs this: a WebSocket message is delivered whole, so its transport
 * hands each message straight to decodeFrame. A stream, by contrast, can split or coalesce
 * frames at any byte boundary.
 */
export class FrameReader {
  // Explicitly widened: an incoming chunk is Uint8Array<ArrayBufferLike>, which TypeScript 5.7+
  // will not assign to the Uint8Array<ArrayBuffer> that `new Uint8Array(0)` infers.
  #buffer: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  readonly #maxFrameBytes: number;

  constructor(maxFrameBytes = DEFAULT_MAX_FRAME_BYTES) {
    this.#maxFrameBytes = maxFrameBytes;
  }

  /** Appends `chunk` and returns every complete frame now available. */
  push(chunk: Uint8Array): Uint8Array[] {
    if (this.#buffer.length === 0) {
      this.#buffer = chunk;
    } else {
      const merged = new Uint8Array(this.#buffer.length + chunk.length);
      merged.set(this.#buffer, 0);
      merged.set(chunk, this.#buffer.length);
      this.#buffer = merged;
    }

    const frames: Uint8Array[] = [];
    for (;;) {
      if (this.#buffer.length < FRAME_HEADER_SIZE) break;
      const view = new DataView(
        this.#buffer.buffer,
        this.#buffer.byteOffset,
        this.#buffer.byteLength,
      );
      const frameLength = view.getInt32(0, true);
      if (frameLength < FRAME_HEADER_SIZE || frameLength > this.#maxFrameBytes) {
        throw new KdbProtocolError(
          `kdb: frame length ${frameLength} outside [${FRAME_HEADER_SIZE}, ${this.#maxFrameBytes}]`,
        );
      }
      if (this.#buffer.length < frameLength) break;
      frames.push(this.#buffer.slice(0, frameLength));
      this.#buffer = this.#buffer.subarray(frameLength);
    }
    return frames;
  }
}
