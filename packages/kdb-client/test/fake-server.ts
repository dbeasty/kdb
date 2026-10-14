/**
 * An in-memory Transport that plays the server side, so client behaviour can be tested without
 * a live server.
 *
 * It speaks the real codec in both directions - requests are decoded from real frames and
 * replies encoded back into them - so it exercises everything except the socket. What it cannot
 * prove is cross-language agreement; that is what the golden fixtures in wire.test.ts are for,
 * and ultimately what the live-server tests in Component 63 §9.2 are for.
 */

import { decodeMessage, encodeMessage } from "../src/wire/envelope.ts";
import type { MessageKind } from "../src/wire/messages.ts";
import type { Transport } from "../src/transport/types.ts";

export interface Request {
  kind: MessageKind;
  payload: Record<string, unknown>;
  correlationId: number;
}

type Reply = { kind: MessageKind; payload: unknown } | undefined;

/** May answer asynchronously (a Promise), e.g. to hash what it was sent. */
export type Responder = (request: Request) => Reply | Promise<Reply>;

export class FakeServer implements Transport {
  readonly requests: Request[] = [];
  #frameHandlers: Array<(frame: Uint8Array) => void> = [];
  #closeHandlers: Array<(cause?: Error) => void> = [];
  #responder: Responder;
  #closed = false;

  constructor(responder: Responder) {
    this.#responder = responder;
  }

  /** Replaces the responder mid-test, for scripted multi-phase scenarios. */
  respondWith(responder: Responder): void {
    this.#responder = responder;
  }

  onFrame(handler: (frame: Uint8Array) => void): void {
    this.#frameHandlers.push(handler);
  }

  onClose(handler: (cause?: Error) => void): void {
    this.#closeHandlers.push(handler);
  }

  send(frame: Uint8Array): void {
    const message = decodeMessage(frame);
    const request: Request = {
      kind: message.kind,
      payload: message.payload as Record<string, unknown>,
      correlationId: message.correlationId,
    };
    this.requests.push(request);

    const reply = this.#responder(request);
    if (reply instanceof Promise) {
      void reply.then((r) => this.#reply(r, message.correlationId));
      return;
    }
    this.#reply(reply, message.correlationId);
  }

  #reply(reply: Reply, correlationId: number): void {
    // undefined means "the server says nothing" - the silent-drop failure mode that makes a
    // per-request deadline a correctness requirement rather than a nicety.
    if (!reply) return;

    // Reply asynchronously, as a real socket would: replying inline would resolve the pending
    // promise before the caller had registered it.
    queueMicrotask(() => {
      if (this.#closed) return;
      const replyFrame = encodeMessage(reply.kind, correlationId, reply.payload);
      for (const handler of this.#frameHandlers) handler(replyFrame);
    });
  }

  close(): Promise<void> {
    this.#closed = true;
    for (const handler of this.#closeHandlers) handler();
    return Promise.resolve();
  }

  /** Pushes an unsolicited frame at the client, as a broadcasting server would. */
  deliver(frame: Uint8Array): void {
    for (const handler of this.#frameHandlers) handler(frame);
  }

  /** Simulates the connection dropping under in-flight requests. */
  drop(cause: Error): void {
    this.#closed = true;
    for (const handler of this.#closeHandlers) handler(cause);
  }
}

export const HEAD_ZERO = "00".repeat(32);
export const HEAD_ONE = "11".repeat(32);

/** The stock replies for the session handshake every operation goes through. */
export function sessionAck(namespace: string, headHex = HEAD_ZERO) {
  return {
    kind: "sessionBeginAck" as const,
    payload: {
      namespace,
      sessionId: "sess-1",
      headHex,
      readConsistency: "READ_COMMITTED",
    },
  };
}

export function sqlOk(
  namespace: string,
  resolvedCommitHex = HEAD_ONE,
  readOnly = false,
  rowsAffected = 1,
) {
  return {
    kind: "sqlResult" as const,
    payload: {
      namespace,
      sessionId: "sess-1",
      columns: [],
      rows: [],
      rowsAffected,
      resolvedCommitHex,
      readOnly,
      generatedIds: [],
    },
  };
}

/** Builds a conflictReport payload with reportBytes in the array-of-numbers wire form. */
export function conflictReport(
  namespace: string,
  conflicts: Array<{ documentId: string; operationType: string; actualContentHash?: string }>,
  retryAfterMs?: number,
) {
  const body = {
    transactionId: "11111111-2222-3333-4444-555555555555",
    baseHash: HEAD_ZERO,
    targetHash: HEAD_ONE,
    conflicts: conflicts.map((c) => ({
      documentId: c.documentId,
      operationType: c.operationType,
      actualContentHash: c.actualContentHash ?? "",
    })),
  };
  const payload: Record<string, unknown> = {
    namespace,
    reportBytes: Array.from(new TextEncoder().encode(JSON.stringify(body))),
  };
  if (retryAfterMs !== undefined) payload["retryAfterMs"] = retryAfterMs;
  return { kind: "conflictReport" as const, payload };
}
