/**
 * The payload envelope: `{"kind": "<name>", "<name>": {...}}`.
 *
 * Matches `payloadEnvelope` in go/kdb/wire/payload_dto.go, where the `kind` string and the key
 * holding the payload are always the same name.
 */

import { KdbProtocolError } from "../errors.ts";
import { decodeFrame, encodeFrame, type FrameHeader } from "./frame.ts";
import { type MessageKind, TYPE_CODE_BY_KIND } from "./messages.ts";

export interface WireMessage<K extends MessageKind = MessageKind, P = unknown> {
  kind: K;
  payload: P;
  correlationId: number;
  header?: FrameHeader;
}

/** Builds a frame carrying one message. */
export function encodeMessage(
  kind: MessageKind,
  correlationId: number,
  payload: unknown,
  maxFrameBytes?: number,
): Uint8Array {
  const envelope: Record<string, unknown> = { kind, [kind]: payload };
  const options: { maxFrameBytes?: number } = {};
  if (maxFrameBytes !== undefined) options.maxFrameBytes = maxFrameBytes;
  return encodeFrame(TYPE_CODE_BY_KIND[kind], correlationId, envelope, options);
}

/**
 * Parses a frame into a message.
 *
 * Dispatch is on the envelope's `kind`, never on the header's type code - see
 * TYPE_CODE_BY_KIND's note on handshakeAck sharing 0x01 with handshake.
 */
export function decodeMessage(frame: Uint8Array, maxFrameBytes?: number): WireMessage {
  const { header, envelope } = decodeFrame(frame, maxFrameBytes);
  const kind = envelope["kind"];
  if (typeof kind !== "string" || kind === "") {
    throw new KdbProtocolError("kdb: payload envelope has no `kind`");
  }
  const payload = envelope[kind];
  if (payload === undefined) {
    throw new KdbProtocolError(`kdb: envelope of kind "${kind}" carries no "${kind}" payload`);
  }
  return {
    kind: kind as MessageKind,
    payload,
    correlationId: header.correlationId,
    header,
  };
}

/**
 * True for the kinds this client can act on.
 *
 * Peer-sync, stream and DAG messages (0x02-0x0D, 0x18) belong to a peer's vocabulary, not a
 * client's. One arriving here is not a protocol violation worth tearing the connection down
 * for - the server may legitimately broadcast to a connection it also serves SQL on - so an
 * unknown kind is dropped with a warning rather than thrown.
 */
export function isClientKind(kind: string): kind is MessageKind {
  return Object.hasOwn(TYPE_CODE_BY_KIND, kind);
}
