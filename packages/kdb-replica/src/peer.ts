/**
 * A peer sync v2 connection, from the client side: the same frames and messages a Go node's
 * v2 client sends (go/kdb/peersync/v2_client.go, go/kdb/wire/sync_v2_ops.go).
 *
 * v2 frames carry the JSON envelope (tag 1) in the 12-byte frame of @kdb/client's wire layer, and
 * that framing is reused here. Two things differ from the client protocol, though:
 * - v2 is Go-only, so `[]byte` fields are standard base64 strings, not the integer arrays Kotlin
 *   needs;
 * - every request gets exactly one reply, and a failure comes back as PEER_ERROR (0x25), never
 *   as silence.
 */

import { connectWebSocket, decodeFrameBody, encodeFrame, encodeFrameBody, type Transport } from "@kdb/client";
import type { TreeAttachmentWire } from "./sync.ts";
import {
  decodeAttachmentUploadResult,
  decodeBlobFetchResult,
  encodeAttachmentUpload,
  encodeBlobFetch,
  type BlobFetchEntry,
  type BlobFetchResult,
  type UploadChunk,
  type UploadResult,
} from "./binwire.ts";
import { decodeCommitList, encodeCommitList, type Commit } from "./commit.ts";
import type { ProjectPage, WriteBackDoc } from "./projection.ts";
import { zstdDecompress as zstdDecompressPortable } from "./zstd.ts";

export const MSG = {
  PEER_ERROR: 0x25,
  SYNC_HELLO: 0x26,
  SYNC_HELLO_ACK: 0x27,
  REFS_REQUEST: 0x28,
  REFS_RESULT: 0x29,
  FETCH_REQUEST: 0x2a,
  PACK_PAGE: 0x2b,
  REF_UPDATE: 0x2c,
  REF_UPDATE_ACK: 0x2d,
  SNAPSHOT_FETCH: 0x2e,
  SNAPSHOT_PAGE: 0x2f,
  TREE_NODES: 0x34,
  TREE_NODES_RESULT: 0x35,
  OBJECT_FETCH: 0x36,
  OBJECT_FETCH_RESULT: 0x37,
  PROJECT_FETCH: 0x30,
  PROJECT_PAGE: 0x31,
  PROJECT_WRITE: 0x32,
  PROJECT_WRITE_RESULT: 0x33,
  SQUASH_HEAD: 0x40,
  SQUASH_HEAD_RESULT: 0x41,
  /** Client replica codes (0x50-0x57 reserved): watch refs, and the host's reports. */
  REFS_WATCH: 0x50,
  REFS_CHANGED: 0x51,
  /** Attachments (docs/kdb-attachments-plan.md), always binary bodies. */
  BLOB_FETCH: 0x42,
  BLOB_FETCH_RESULT: 0x43,
  ATTACHMENT_UPLOAD: 0x44,
  ATTACHMENT_UPLOAD_RESULT: 0x45,
} as const;

const ENCODING_BINARY = 0x02;

const KIND_BY_CODE: Record<number, string> = {
  [MSG.SYNC_HELLO]: "syncHello",
  [MSG.REFS_REQUEST]: "refsRequest",
  [MSG.FETCH_REQUEST]: "fetchRequest",
  [MSG.REF_UPDATE]: "refUpdate",
  [MSG.SNAPSHOT_FETCH]: "snapshotFetch",
  [MSG.TREE_NODES]: "treeNodes",
  [MSG.OBJECT_FETCH]: "objectFetch",
  [MSG.SQUASH_HEAD]: "squashHead",
  [MSG.PROJECT_FETCH]: "projectFetch",
  [MSG.PROJECT_WRITE]: "projectWrite",
  [MSG.REFS_WATCH]: "refsWatch",
};

export const SYNC_PROTOCOL_VERSION = 2;

/** Limits the host enforces (go/kdb/peersync/repair.go, refs.go). */
export const MAX_TREE_NODE_PREFIXES = 4096;
export const MAX_OBJECT_FETCH_ITEMS = 1024;
export const DEFAULT_TREE_ENTRY_LIMIT = 32;
export const DEFAULT_PAGE_BYTES = 4 << 20;

export interface NamespaceRefs {
  namespace: string;
  branches: Record<string, string>;
  historyFloorHex?: string;
  shallow?: string[];
  /** "" both directions, "pull" or "push" only. */
  access?: string;
}

export class RemoteError extends Error {
  override name = "RemoteError";
  readonly code: string;
  readonly namespace: string;
  constructor(code: string, message: string, namespace: string) {
    super(`kdb replica: peer refused: ${code}: ${message}`);
    this.code = code;
    this.namespace = namespace;
  }
}

export class ProtocolError extends Error {
  override name = "ProtocolError";
}

// --- base64 (Go's encoding/json form for []byte) -------------------------------------------------

export function toBase64(b: Uint8Array): string {
  let s = "";
  for (let i = 0; i < b.length; i += 0x8000) s += String.fromCharCode(...b.subarray(i, i + 0x8000));
  return btoa(s);
}

export function fromBase64(s: string | undefined | null): Uint8Array {
  if (!s) return new Uint8Array();
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

// --- zstd ----------------------------------------------------------------------------------------

/**
 * zstd for packs: Node's native decoder when the runtime has one (22.15+), else the TS port of
 * kdb-compression's decoder (zstd.ts), so browsers get compressed packs too (plan D11).
 */
const nativeZstd: ((data: Uint8Array) => Uint8Array) | null = (() => {
  const proc = (globalThis as { process?: { getBuiltinModule?: (id: string) => unknown } }).process;
  const zlib = proc?.getBuiltinModule?.("node:zlib") as
    | { zstdDecompressSync?: (d: Uint8Array, o?: { maxOutputLength?: number }) => Uint8Array }
    | undefined;
  const fn = zlib?.zstdDecompressSync;
  if (typeof fn !== "function") return null;
  return (d) => {
    const out = fn(d, { maxOutputLength: MAX_PACK_PAYLOAD });
    return new Uint8Array(out.buffer, out.byteOffset, out.byteLength);
  };
})();

/** The most a pack's commits may inflate to (go/kdb/wire maxPackPayload). */
const MAX_PACK_PAYLOAD = 256 << 20;

const zstdDecompress = nativeZstd ?? ((d: Uint8Array) => zstdDecompressPortable(d, MAX_PACK_PAYLOAD));


function commitsFrom(payload: string | undefined, zstd: boolean | undefined): Commit[] {
  let bytes = fromBase64(payload);
  if (zstd) {
    bytes = zstdDecompress(bytes);
  }
  return decodeCommitList(bytes);
}

// --- connection ----------------------------------------------------------------------------------

export interface ConnectOptions {
  nodeId: string;
  token?: string | undefined;
  /** Namespace names (or patterns) the session covers. */
  namespaces: string[];
  timeoutMs?: number;
  /** Opens the transport; defaults to WebSocket for ws:// and wss://. */
  connect?: (url: string) => Promise<Transport>;
}

type Envelope = Record<string, unknown> & { kind: string };

export class PeerConnection {
  readonly #t: Transport;
  readonly #timeoutMs: number;
  readonly #pending = new Map<number, { resolve: (e: Envelope) => void; reject: (e: Error) => void; timer: ReturnType<typeof setTimeout> }>();
  #next = 1;
  #watch: { id: number; onChange: (refs: NamespaceRefs[]) => void } | null = null;
  #closeHandlers: ((cause: Error) => void)[] = [];
  #closed: Error | null = null;
  caps: string[] = [];
  remoteNodeId = "";
  refs: NamespaceRefs[] = [];

  private constructor(t: Transport, timeoutMs: number) {
    this.#t = t;
    this.#timeoutMs = timeoutMs;
    t.onFrame((frame) => this.#onFrame(frame));
    t.onClose((cause) => this.#fail(cause ?? new ProtocolError("kdb replica: connection closed")));
  }

  /** Dials url and completes the SYNC_HELLO handshake. */
  static async open(url: string, opts: ConnectOptions): Promise<PeerConnection> {
    const t = await (opts.connect ?? ((u) => connectWebSocket(u)))(url);
    const conn = new PeerConnection(t, opts.timeoutMs ?? 30_000);
    try {
      const caps = ["snapshot", "repair", "squash", "filter", "writeback", "watch", "zstd-pack", "attachments"];
      const hello: Record<string, unknown> = {
        nodeId: opts.nodeId, protocol: SYNC_PROTOCOL_VERSION, capabilities: caps, namespaces: opts.namespaces,
      };
      if (opts.token !== undefined) hello.token = opts.token;
      const ack = await conn.request(MSG.SYNC_HELLO, hello, "syncHelloAck");
      if (!ack.accepted) throw new RemoteError("REFUSED", String(ack.reason ?? "hello refused"), "");
      conn.caps = (ack.capabilities as string[] | undefined) ?? [];
      conn.remoteNodeId = String(ack.nodeId ?? "");
      conn.refs = (ack.refs as NamespaceRefs[] | undefined) ?? [];
      return conn;
    } catch (e) {
      await conn.close();
      throw e;
    }
  }

  #onFrame(frame: Uint8Array): void {
    let decoded: { header: { correlationId: number; typeCode: number }; envelope: Envelope };
    try {
      const { header, encodingTag, body } = decodeFrameBody(frame);
      // JSON envelopes for v2 messages; binary bodies for the attachment messages, which are
      // handed over undecoded with their type code.
      const envelope: Envelope = encodingTag === ENCODING_BINARY
        ? { kind: "binary", typeCode: header.typeCode, body: body.slice() }
        : (JSON.parse(new TextDecoder().decode(body)) as Envelope);
      decoded = { header, envelope };
    } catch (e) {
      this.#fail(e instanceof Error ? e : new ProtocolError(String(e)));
      return;
    }
    const p = this.#pending.get(decoded.header.correlationId);
    if (!p) {
      // An unasked REFS_CHANGED, under the watch's correlation id.
      const env = decoded.envelope as Envelope;
      if (this.#watch && decoded.header.correlationId === this.#watch.id && env.kind === "refsChanged") {
        const refs = ((env.refsChanged as { refs?: NamespaceRefs[] } | undefined)?.refs) ?? [];
        try {
          this.#watch.onChange(refs);
        } catch {
          // the listener's failure is its own
        }
      }
      return;
    }
    this.#pending.delete(decoded.header.correlationId);
    clearTimeout(p.timer);
    p.resolve(decoded.envelope as Envelope);
  }

  #fail(err: Error): void {
    if (this.#closed) return;
    this.#closed = err;
    for (const p of this.#pending.values()) {
      clearTimeout(p.timer);
      p.reject(err);
    }
    this.#pending.clear();
    for (const h of this.#closeHandlers) h(err);
  }

  /** Whether the connection has ended. */
  get closed(): boolean {
    return this.#closed !== null;
  }

  /** Called once when the connection ends, for whatever reason. */
  onClose(handler: (cause: Error) => void): void {
    if (this.#closed) handler(this.#closed);
    else this.#closeHandlers.push(handler);
  }

  #exchange(what: string, frame: (id: number) => Uint8Array): Promise<Envelope> {
    if (this.#closed) return Promise.reject(this.#closed);
    const id = this.#next++;
    return new Promise<Envelope>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#pending.delete(id);
        reject(new ProtocolError(`kdb replica: no reply to ${what} within ${this.#timeoutMs} ms`));
      }, this.#timeoutMs);
      this.#pending.set(id, { resolve, reject, timer });
      try {
        this.#t.send(frame(id));
      } catch (e) {
        clearTimeout(timer);
        this.#pending.delete(id);
        reject(e);
      }
    });
  }

  /** Sends a binary-bodied request and returns the binary body of its reply, of type `expect`. */
  async requestBinary(code: number, body: Uint8Array, expect: number): Promise<Uint8Array> {
    const env = await this.#exchange(`0x${code.toString(16)}`, (id) => encodeFrameBody(code, id, ENCODING_BINARY, body));
    if (env.kind === "peerError") {
      const pe = (env.peerError ?? {}) as { code?: string; message?: string; namespace?: string };
      throw new RemoteError(pe.code ?? "UNKNOWN", pe.message ?? "", pe.namespace ?? "");
    }
    if (env.kind !== "binary" || env.typeCode !== expect) {
      throw new ProtocolError(`kdb replica: expected binary 0x${expect.toString(16)}, got ${env.kind}`);
    }
    return env.body as Uint8Array;
  }

  /** Sends one request and returns the body of its reply, which must be of kind `expect`. */
  request(code: number, body: Record<string, unknown>, expect: string): Promise<Record<string, unknown>> {
    const kind = KIND_BY_CODE[code];
    if (!kind) return Promise.reject(new ProtocolError(`kdb replica: no request kind for 0x${code.toString(16)}`));
    return this.#exchange(kind, (id) => encodeFrame(code, id, { kind, [kind]: body })).then((env) => {
      if (env.kind === "peerError") {
        const pe = (env.peerError ?? {}) as { code?: string; message?: string; namespace?: string };
        throw new RemoteError(pe.code ?? "UNKNOWN", pe.message ?? "", pe.namespace ?? "");
      }
      if (env.kind !== expect) throw new ProtocolError(`kdb replica: expected ${expect}, got ${env.kind}`);
      return (env[expect] ?? {}) as Record<string, unknown>;
    });
  }

  async close(): Promise<void> {
    this.#fail(new ProtocolError("kdb replica: connection closed"));
    await this.#t.close().catch(() => undefined);
  }

  has(cap: string): boolean {
    return this.caps.includes(cap);
  }

  // --- typed requests ----------------------------------------------------------------------------

  async refs_(namespaces: string[]): Promise<NamespaceRefs[]> {
    const r = await this.request(MSG.REFS_REQUEST, { namespaces }, "refsResult");
    return (r.refs as NamespaceRefs[] | undefined) ?? [];
  }

  /**
   * Watches namespaces (REFS_WATCH): returns their refs now, then calls onChange with the refs of
   * those that move, until the connection ends. Replaces any earlier watch on this connection.
   */
  async watch(namespaces: string[], onChange: (refs: NamespaceRefs[]) => void): Promise<NamespaceRefs[]> {
    const id = this.#next; // the request's correlation id, which the host's reports reuse
    this.#watch = { id, onChange };
    const r = await this.request(MSG.REFS_WATCH, { namespaces }, "refsChanged");
    return (r.refs as NamespaceRefs[] | undefined) ?? [];
  }

  async fetch(namespace: string, wants: string[], haves: string[], maxBytes = DEFAULT_PAGE_BYTES): Promise<{ commits: Commit[]; done: boolean }> {
    const r = await this.request(MSG.FETCH_REQUEST, { namespace, wants, haves, maxBytes }, "packPage");
    if (Array.isArray(r.stubs) && r.stubs.length > 0) {
      throw new ProtocolError("kdb replica: the peer sent archived commit stubs, which a replica cannot apply");
    }
    return { commits: commitsFrom(r.commitsPayload as string, r.commitsZstd as boolean), done: Boolean(r.done) };
  }

  async snapshot(namespace: string, atHex: string, after: string, maxBytes = DEFAULT_PAGE_BYTES): Promise<SnapshotPage> {
    const r = await this.request(MSG.SNAPSHOT_FETCH, { namespace, atHex, after, maxBytes }, "snapshotPage");
    const commits = decodeCommitList(fromBase64(r.commitPayload as string));
    if (commits.length !== 1) throw new ProtocolError("kdb replica: a snapshot page must carry exactly one commit");
    return {
      commit: commits[0]!,
      docs: ((r.docs as { id: string; body: string }[] | undefined) ?? []),
      next: String(r.next ?? ""),
      done: Boolean(r.done),
      total: Number(r.total ?? 0),
      attachmentTables: fromBase64(r.attachmentTables as string | undefined),
    };
  }

  async squashHead(namespace: string, have: string): Promise<SquashHead> {
    const r = await this.request(MSG.SQUASH_HEAD, { namespace, have }, "squashHeadResult");
    const payload = r.commitPayload as string | undefined;
    const commit = payload ? decodeCommitList(fromBase64(payload))[0] : undefined;
    return {
      commit,
      descends: Boolean(r.descends),
      backlog: Number(r.backlog ?? 0),
      refused: String(r.refused ?? ""),
    };
  }

  /**
   * TREE_NODES: the document tree treeHex, or with tree "attachments" the attachment table at
   * commitHex (entries map document ids to set hashes, and attachments lists their sets).
   */
  async treeNodes(namespace: string, treeHex: string, prefixes: string[], entryLimit = DEFAULT_TREE_ENTRY_LIMIT, opts: { tree?: "attachments"; commitHex?: string } = {}): Promise<{ treeHex: string; nodes: WireTreeNode[]; attachments: Record<string, TreeAttachmentWire[]> }> {
    const r = await this.request(MSG.TREE_NODES, { namespace, treeHex, prefixes, entryLimit, ...opts }, "treeNodesResult");
    return {
      treeHex: String(r.treeHex ?? ""),
      nodes: (r.nodes as WireTreeNode[] | undefined) ?? [],
      attachments: (r.attachments as Record<string, TreeAttachmentWire[]> | undefined) ?? {},
    };
  }

  /** BLOB_FETCH: parts of attachment blobs, each with chunk proofs (verified by the caller). */
  async blobFetch(namespace: string, entries: readonly BlobFetchEntry[], maxBytes = 8 << 20): Promise<BlobFetchResult> {
    return decodeBlobFetchResult(await this.requestBinary(MSG.BLOB_FETCH, encodeBlobFetch(namespace, entries, maxBytes), MSG.BLOB_FETCH_RESULT));
  }

  /** ATTACHMENT_UPLOAD: one chunk of a blob upload on the sync session. */
  async upload(chunk: UploadChunk): Promise<UploadResult> {
    return decodeAttachmentUploadResult(await this.requestBinary(MSG.ATTACHMENT_UPLOAD, encodeAttachmentUpload(chunk), MSG.ATTACHMENT_UPLOAD_RESULT));
  }

  async objectFetch(namespace: string, items: { id: string; contentHash: string; treeHex?: string }[]): Promise<{ docs: { id: string; body: string }[]; missing: string[] }> {
    const r = await this.request(MSG.OBJECT_FETCH, { namespace, items }, "objectFetchResult");
    return {
      docs: (r.docs as { id: string; body: string }[] | undefined) ?? [],
      missing: (r.missing as string[] | undefined) ?? [],
    };
  }

  async projectFetch(namespace: string, filter: string, fromHex: string, atHex: string, after: string, maxBytes = DEFAULT_PAGE_BYTES): Promise<ProjectPage> {
    const r = await this.request(MSG.PROJECT_FETCH, { namespace, filter, fromHex, atHex, after, maxBytes }, "projectPage");
    return {
      atHex: String(r.atHex ?? ""),
      reset: Boolean(r.reset),
      writes: (r.writes as { id: string; body: string }[] | undefined) ?? [],
      deletes: (r.deletes as string[] | undefined) ?? [],
      next: String(r.next ?? ""),
      done: Boolean(r.done),
      attachments: (r.attachments as Record<string, TreeAttachmentWire[]> | undefined) ?? {},
    };
  }

  async projectWrite(namespace: string, txId: string, docs: WriteBackDoc[]): Promise<ProjectWriteResult> {
    const r = await this.request(MSG.PROJECT_WRITE, { namespace, txId, docs }, "projectWriteResult");
    return {
      outcome: String(r.outcome ?? ""),
      commitHex: String(r.commitHex ?? ""),
      reason: String(r.reason ?? ""),
      current: (r.current as ProjectWriteResult["current"] | undefined) ?? [],
    };
  }

  async refUpdate(namespace: string, oldHex: string, newHex: string, commits: readonly Commit[], more: boolean): Promise<RefUpdateAck> {
    const r = await this.request(MSG.REF_UPDATE, {
      namespace, kind: "branch", ref: "main", oldHex, newHex,
      commitsPayload: toBase64(encodeCommitList(commits)), more,
    }, "refUpdateAck");
    return {
      outcome: String(r.outcome ?? ""),
      headHex: String(r.headHex ?? ""),
      stored: Number(r.stored ?? 0),
      conflict: r.conflictBytes ? JSON.parse(new TextDecoder().decode(fromBase64(r.conflictBytes as string))) as ConflictReport : undefined,
    };
  }
}

export interface SnapshotPage {
  commit: Commit;
  docs: { id: string; body: string }[];
  next: string;
  done: boolean;
  total: number;
  /** On the last page of a commit with attachments: its packed attachment table. */
  attachmentTables: Uint8Array;
}

export interface SquashHead {
  commit: Commit | undefined;
  descends: boolean;
  backlog: number;
  refused: string;
}

export interface WireTreeNode {
  prefix: string;
  hash?: string;
  children?: string[];
  hasEntries?: boolean;
  entries?: Record<string, string>;
}

/** go/kdb/error/payloads.go ConflictReport. */
export interface ConflictReport {
  transactionId: string;
  baseHash: string;
  targetHash: string;
  conflicts: {
    documentId: string;
    operationType: string;
    localDoc?: string;
    incomingDoc?: string;
    actualContentHash?: string;
  }[];
}

export interface ProjectWriteResult {
  /** "applied", "conflict" or "refused". */
  outcome: string;
  commitHex: string;
  reason: string;
  /** The source's state of every document named, at commitHex - attachments included. */
  current: { docId: string; body?: string; absent?: boolean; attachments?: { id: string; name: string; hash: string; size: number; mime?: string }[] }[];
}

export interface RefUpdateAck {
  outcome: string;
  headHex: string;
  stored: number;
  conflict: ConflictReport | undefined;
}
