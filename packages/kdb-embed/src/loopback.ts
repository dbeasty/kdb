/**
 * The loopback transport: wire frames between @kdb/client and the in-process engine.
 *
 * It carries exactly the frames a WebSocket would. That is the design (Component 63 §11): the
 * embedded engine is the Go server's own FrameHost, so the client above it and the handshake,
 * sessions, SQL and commit semantics below it are the networked ones, not lookalikes.
 */

import { KdbTransportError, type Transport } from "@kdb/client";

/** What the WASM module hands back from `KdbEmbed.open`. */
export interface EngineHandle {
  send(frame: Uint8Array): unknown;
  close(): void;
}

export class LoopbackTransport implements Transport {
  readonly #frameHandlers: Array<(frame: Uint8Array) => void> = [];
  readonly #closeHandlers: Array<(cause?: Error) => void> = [];
  #engine: EngineHandle | undefined;
  #closed = false;

  attach(engine: EngineHandle): void {
    this.#engine = engine;
  }

  onFrame(handler: (frame: Uint8Array) => void): void {
    this.#frameHandlers.push(handler);
  }

  onClose(handler: (cause?: Error) => void): void {
    this.#closeHandlers.push(handler);
  }

  /**
   * Called by the engine for each reply frame. Delivery is deferred to a microtask: the call
   * arrives while Go is still inside its own callback, and running client code (which may send
   * again) re-entrantly inside that would re-enter the Go runtime mid-call.
   */
  deliver(frame: Uint8Array): void {
    if (this.#closed) return;
    queueMicrotask(() => {
      if (this.#closed) return;
      for (const handler of this.#frameHandlers) handler(frame);
    });
  }

  send(frame: Uint8Array): void {
    if (this.#closed || this.#engine === undefined) {
      throw new KdbTransportError("kdb: embedded engine is closed");
    }
    const result = this.#engine.send(frame) as { error?: string } | undefined;
    if (result !== undefined && result !== null && typeof result === "object" && "error" in result) {
      throw new KdbTransportError(`kdb: embedded engine refused a frame: ${String(result.error)}`);
    }
  }

  /** The engine stopped on its own (the Go runtime exited or panicked). */
  fail(cause: Error): void {
    this.#finish(cause);
  }

  async close(): Promise<void> {
    this.#finish();
  }

  #finish(cause?: Error): void {
    if (this.#closed) return;
    this.#closed = true;
    try {
      this.#engine?.close();
    } catch {
      // The engine may already be gone; closing is best-effort.
    }
    for (const handler of this.#closeHandlers) handler(cause);
  }
}
