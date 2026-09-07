/**
 * The transport seam.
 *
 * A transport moves whole frames. Framing a byte stream back into frames is the transport's
 * job, not the client's: a WebSocket already delivers messages whole, while a TCP stream can
 * split or coalesce at any byte boundary, and only the transport knows which it is.
 */

export interface Transport {
  /** Fires once per whole frame. */
  onFrame(handler: (frame: Uint8Array) => void): void;
  /** Fires once when the connection ends, with a cause if it ended badly. */
  onClose(handler: (cause?: Error) => void): void;
  send(frame: Uint8Array): void;
  close(): Promise<void>;
}

export interface TransportConnectOptions {
  /** Milliseconds to wait for the connection to be established. */
  connectTimeoutMs?: number;
  signal?: AbortSignal;
}

export type TransportScheme = "ws" | "wss" | "tcp" | "tcps";

export function schemeOf(uri: string): TransportScheme | null {
  const match = /^([a-z][a-z0-9+.-]*):\/\//i.exec(uri);
  if (!match) return null;
  const scheme = match[1]!.toLowerCase();
  return scheme === "ws" || scheme === "wss" || scheme === "tcp" || scheme === "tcps"
    ? scheme
    : null;
}
