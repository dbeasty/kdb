/**
 * WebSocket transport - the browser path, and also fine on Node 22+, Bun, Deno and Workers,
 * all of which expose a global WebSocket.
 *
 * A WebSocket message is delivered whole, so every message is exactly one frame and no
 * re-framing is needed. That is also why frame.ts validates a frame's declared length against
 * the buffer actually received: unlike a stream reader, nothing upstream of here has checked it.
 *
 * Note the server-side prerequisite (Component 63 §2.3): the Go server's WebSocket listener is
 * still an HTTP 501 stub (go/kdb/transport/ws/transport.go:128). This transport is correct and
 * tested against the wire format, but until that listener lands it has nothing to talk to on
 * the Go deployment target.
 */

import { KdbTransportError } from "../errors.ts";
import type { Transport, TransportConnectOptions } from "./types.ts";

const DEFAULT_CONNECT_TIMEOUT_MS = 30_000;

export function connectWebSocket(
  uri: string,
  options: TransportConnectOptions = {},
): Promise<Transport> {
  const WS = globalThis.WebSocket;
  if (typeof WS !== "function") {
    return Promise.reject(
      new KdbTransportError(
        "kdb: no global WebSocket available - use Node 22+, a browser, Bun, Deno or a Workers " +
          "runtime, or connect over tcp:// via @kdb/client/tcp on Node",
      ),
    );
  }

  return new Promise<Transport>((resolve, reject) => {
    let socket: WebSocket;
    try {
      socket = new WS(uri);
    } catch (cause) {
      reject(new KdbTransportError(`kdb: cannot open WebSocket to ${uri}`, { cause }));
      return;
    }
    socket.binaryType = "arraybuffer";

    const frameHandlers: Array<(frame: Uint8Array) => void> = [];
    const closeHandlers: Array<(cause?: Error) => void> = [];
    let settled = false;
    let closed = false;

    const timeoutMs = options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      try {
        socket.close();
      } catch {
        // The socket never opened; closing it is best-effort.
      }
      reject(new KdbTransportError(`kdb: WebSocket connect to ${uri} timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    const onAbort = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try {
        socket.close();
      } catch {
        // Best-effort.
      }
      reject(new KdbTransportError("kdb: WebSocket connect aborted"));
    };
    options.signal?.addEventListener("abort", onAbort, { once: true });

    const finishClose = (cause?: Error) => {
      if (closed) return;
      closed = true;
      clearTimeout(timer);
      options.signal?.removeEventListener("abort", onAbort);
      for (const handler of closeHandlers) handler(cause);
    };

    socket.onopen = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      options.signal?.removeEventListener("abort", onAbort);
      resolve({
        onFrame(handler) {
          frameHandlers.push(handler);
        },
        onClose(handler) {
          closeHandlers.push(handler);
        },
        send(frame) {
          if (socket.readyState !== 1 /* OPEN */) {
            throw new KdbTransportError("kdb: WebSocket is not open");
          }
          // Copy onto a standalone ArrayBuffer: a subarray view would send the whole
          // underlying buffer on some runtimes.
          socket.send(frame.slice().buffer as ArrayBuffer);
        },
        close() {
          return new Promise<void>((done) => {
            if (socket.readyState === 3 /* CLOSED */) {
              finishClose();
              done();
              return;
            }
            closeHandlers.push(() => done());
            socket.close();
          });
        },
      });
    };

    socket.onmessage = (event: MessageEvent) => {
      const frame = toBytes(event.data);
      if (!frame) return;
      for (const handler of frameHandlers) handler(frame);
    };

    socket.onerror = () => {
      const error = new KdbTransportError(`kdb: WebSocket error on ${uri}`);
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        reject(error);
        return;
      }
      finishClose(error);
    };

    socket.onclose = (event: CloseEvent) => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        reject(
          new KdbTransportError(
            `kdb: WebSocket to ${uri} closed before opening (code ${event.code})`,
          ),
        );
        return;
      }
      finishClose(
        event.wasClean
          ? undefined
          : new KdbTransportError(`kdb: WebSocket closed uncleanly (code ${event.code})`),
      );
    };
  });
}

function toBytes(data: unknown): Uint8Array | null {
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data)) {
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  }
  // A text frame is not something this protocol ever sends; ignore rather than misparse.
  return null;
}
