/**
 * TCP transport - Node only, behind the `@kdb/client/tcp` subpath export.
 *
 * This file is the reason for the subpath split: it is the only part of the package that
 * touches a Node built-in, and keeping it out of the main entry point is what lets the core
 * load unchanged on Workers and other runtimes with no `node:net`. The imports are dynamic so
 * that even a bundler that follows this module does not pull `node:net` into a browser bundle
 * unless it is actually reached.
 *
 * Unlike a WebSocket, a stream splits and coalesces at arbitrary byte boundaries, so this
 * transport re-frames with FrameReader.
 */

import { KdbTransportError } from "../errors.ts";
import { FrameReader } from "../wire/frame.ts";
import type { Transport, TransportConnectOptions } from "./types.ts";

const DEFAULT_CONNECT_TIMEOUT_MS = 30_000;

export interface TcpConnectOptions extends TransportConnectOptions {
  /** Passed through to node:tls for a tcps:// URI. */
  tls?: {
    ca?: string | Uint8Array | Array<string | Uint8Array>;
    cert?: string | Uint8Array;
    key?: string | Uint8Array;
    servername?: string;
    /** Disables certificate verification. For local testing only. */
    rejectUnauthorized?: boolean;
  };
}

/**
 * The subset of node:net / node:tls sockets this transport uses.
 *
 * Declared structurally rather than imported as a type so the package typechecks the same way
 * whether or not @types/node is installed in the consumer's project.
 */
interface NodeSocketLike {
  on(event: "data", handler: (chunk: Uint8Array) => void): unknown;
  on(event: "error", handler: (error: Error) => void): unknown;
  on(event: "close", handler: () => void): unknown;
  once(event: string, handler: (...args: never[]) => void): unknown;
  write(data: Uint8Array): boolean;
  end(): unknown;
  destroy(): unknown;
  setNoDelay(enable: boolean): unknown;
  removeListener(event: string, handler: (...args: never[]) => void): unknown;
}

export async function connectTcp(
  uri: string,
  options: TcpConnectOptions = {},
): Promise<Transport> {
  const parsed = parseTcpUri(uri);
  const timeoutMs = options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS;

  if (parsed.secure && !options.tls) {
    throw new KdbTransportError(
      `kdb: ${parsed.scheme}:// requires TLS settings - refusing to fall back to plaintext`,
    );
  }

  const socket = await openSocket(parsed, options, timeoutMs);
  socket.setNoDelay(true);

  const reader = new FrameReader();
  const frameHandlers: Array<(frame: Uint8Array) => void> = [];
  const closeHandlers: Array<(cause?: Error) => void> = [];
  let closed = false;
  let pendingCause: Error | undefined;

  const finishClose = (cause?: Error) => {
    if (closed) return;
    closed = true;
    for (const handler of closeHandlers) handler(cause);
  };

  socket.on("data", (chunk: Uint8Array) => {
    let frames: Uint8Array[];
    try {
      frames = reader.push(chunk);
    } catch (cause) {
      // A frame prefix this reader cannot trust means the stream is desynchronized; there is
      // no safe resynchronization point, so tear the connection down rather than guess.
      const error =
        cause instanceof Error ? cause : new KdbTransportError("kdb: malformed frame stream");
      pendingCause = error;
      socket.destroy();
      return;
    }
    for (const frame of frames) {
      for (const handler of frameHandlers) handler(frame);
    }
  });

  socket.on("error", (error: Error) => {
    pendingCause = new KdbTransportError(`kdb: TCP error on ${uri}`, { cause: error });
  });

  socket.on("close", () => finishClose(pendingCause));

  return {
    onFrame(handler) {
      frameHandlers.push(handler);
    },
    onClose(handler) {
      closeHandlers.push(handler);
    },
    send(frame) {
      if (closed) throw new KdbTransportError("kdb: socket is closed");
      socket.write(frame);
    },
    close() {
      return new Promise<void>((resolve) => {
        if (closed) {
          resolve();
          return;
        }
        closeHandlers.push(() => resolve());
        socket.end();
      });
    },
  };
}

interface ParsedTcpUri {
  scheme: "tcp" | "tcps";
  host: string;
  port: number;
  secure: boolean;
}

export function parseTcpUri(uri: string): ParsedTcpUri {
  const withScheme = /:\/\//.test(uri) ? uri : `tcp://${uri}`;
  const match = /^(tcps?):\/\/([^/?#]+)/i.exec(withScheme);
  if (!match) {
    throw new KdbTransportError(`kdb: not a tcp:// or tcps:// URI: ${uri}`);
  }
  const scheme = match[1]!.toLowerCase() as "tcp" | "tcps";
  const authority = match[2]!;

  // An IPv6 literal is bracketed, so the last ':' outside the brackets is the port separator.
  const ipv6 = /^\[([^\]]+)\]:(\d+)$/.exec(authority);
  if (ipv6) {
    return { scheme, host: ipv6[1]!, port: Number(ipv6[2]), secure: scheme === "tcps" };
  }
  const lastColon = authority.lastIndexOf(":");
  if (lastColon < 0) {
    throw new KdbTransportError(`kdb: no port in ${uri}`);
  }
  const host = authority.slice(0, lastColon);
  const port = Number(authority.slice(lastColon + 1));
  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    throw new KdbTransportError(`kdb: invalid port in ${uri}`);
  }
  return { scheme, host, port, secure: scheme === "tcps" };
}

async function openSocket(
  parsed: ParsedTcpUri,
  options: TcpConnectOptions,
  timeoutMs: number,
): Promise<NodeSocketLike> {
  const modulePath = parsed.secure ? "node:tls" : "node:net";
  let mod: Record<string, unknown>;
  try {
    mod = (await import(/* @vite-ignore */ modulePath)) as Record<string, unknown>;
  } catch (cause) {
    throw new KdbTransportError(
      `kdb: ${modulePath} is unavailable - the TCP transport is Node-only; use ws:// elsewhere`,
      { cause },
    );
  }

  return await new Promise<NodeSocketLike>((resolve, reject) => {
    let settled = false;
    const settleError = (error: Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      options.signal?.removeEventListener("abort", onAbort);
      try {
        socket.destroy();
      } catch {
        // Best-effort.
      }
      reject(error);
    };

    const timer = setTimeout(
      () =>
        settleError(
          new KdbTransportError(
            `kdb: connect to ${parsed.host}:${parsed.port} timed out after ${timeoutMs}ms`,
          ),
        ),
      timeoutMs,
    );
    const onAbort = () => settleError(new KdbTransportError("kdb: connect aborted"));
    options.signal?.addEventListener("abort", onAbort, { once: true });

    const onReady = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      options.signal?.removeEventListener("abort", onAbort);
      socket.removeListener("error", settleError as (...args: never[]) => void);
      resolve(socket);
    };

    let socket: NodeSocketLike;
    if (parsed.secure) {
      const connect = mod["connect"] as (opts: unknown, cb: () => void) => NodeSocketLike;
      socket = connect(
        {
          host: parsed.host,
          port: parsed.port,
          servername: options.tls?.servername ?? parsed.host,
          ca: options.tls?.ca,
          cert: options.tls?.cert,
          key: options.tls?.key,
          rejectUnauthorized: options.tls?.rejectUnauthorized ?? true,
        },
        onReady,
      );
    } else {
      const connect = mod["connect"] as (opts: unknown, cb: () => void) => NodeSocketLike;
      socket = connect({ host: parsed.host, port: parsed.port }, onReady);
    }
    socket.once("error", settleError as (...args: never[]) => void);
  });
}
