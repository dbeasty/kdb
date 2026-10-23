/**
 * The KDB client: connection, sessions, correlation, and the operation set.
 *
 * Mirrors go/kdb/client method for method, translated to idiomatic TypeScript - promises rather
 * than (value, error) tuples, AbortSignal rather than context.Context, thrown typed errors
 * rather than sentinel returns.
 */

import { checkServerCompatible } from "./version.ts";
import { SessionLock } from "./session-lock.ts";
import {
  KdbAbortedError,
  KdbClosedError,
  KdbConflictError,
  KdbError,
  KdbLockError,
  KdbNotFoundError,
  KdbPreconditionError,
  KdbProtocolError,
  KdbTransportError,
  KdbUnauthenticatedError,
  KdbUnsupportedError,
  classifiedError,
  type ConflictDetail,
  type ErrorCode,
} from "./errors.ts";
import {
  KdbAttachmentNotFoundError,
  KdbAttachmentRangeError,
  KdbIntegrityError,
  KdbUploadError,
  findAttachment,
  selectorParts,
  sqlParam,
  uploadChunks,
  type AttachmentBlob,
  type AttachmentInfo,
  type AttachmentRangeOptions,
  type AttachmentReader,
  type AttachmentSelector,
  type AttachmentSource,
  type ListAttachmentsOptions,
  type PutAttachmentOptions,
  type ReadAttachmentOptions,
  type UploadAttachmentOptions,
} from "./attachments.ts";
import type {
  AttachmentOp,
  AttachmentPutOp,
  CallOptions,
  CommitHash,
  CompareAndSwapOptions,
  KdbOperations,
  Row,
  Transaction,
} from "./operations.ts";
import { connectWebSocket } from "./transport/websocket.ts";
import { schemeOf, type Transport } from "./transport/types.ts";
import { bytesFromWire, bytesToWire, fromHex, toHex, utf8Decode, utf8Encode } from "./wire/bytes.ts";
import {
  WIRE_FEATURE_BINARY,
  WIRE_FEATURE_BINARY_2,
  WIRE_FEATURE_ZSTD,
  zstdDecompress,
} from "./wire/binary.ts";
import { decodeMessage, encodeMessage, isClientKind, type WireMessage } from "./wire/envelope.ts";
import { DEFAULT_MAX_FRAME_BYTES, KDB_WIRE_PROTOCOL_VERSION } from "./wire/frame.ts";
import { contentHash } from "./wire/hash.ts";
import {
  ATTACHMENT_CHUNK_BYTES,
  ATTACHMENT_HASH_SCHEME_CHUNKED_1,
  AttachmentHasher,
  attachmentChunkCount,
  attachmentIdentity,
  base64ToBytes,
  chunkLeafHash,
  MAX_ATTACHMENT_CHUNK_BYTES,
  verifyMerkleInclusion,
  WIRE_FEATURE_ATTACHMENT_PROOFS,
  WIRE_FEATURE_ATTACHMENTS,
  type AttachmentDataDto,
  type AttachmentInfoDto,
  type AttachmentListResultDto,
  type AttachmentUploadResultDto,
} from "./wire/attachments.ts";
import {
  CLIENT_MODE_SQL,
  READ_COMMITTED,
  type ConflictReportBody,
  type ConflictReportDto,
  type DocumentGetResultDto,
  type HandshakeAckDto,
  type HandshakeDto,
  type LockResultDto,
  type MessageKind,
  type OpDto,
  type PreconditionDto,
  type SessionBeginAckDto,
  type SqlResultDto,
  type TransactionDto,
  type UpsertResultDto,
} from "./wire/messages.ts";
import { assertUuid, randomUuid } from "./wire/uuid.ts";

export const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

/** Backoff bounds for compareAndSwap, matching go/kdb/client/conditional.go:182-185. */
const BACKOFF_BASE_MS = 2;
const BACKOFF_CAP_MS = 250;

export interface TlsOptions {
  ca?: string | Uint8Array | Array<string | Uint8Array>;
  cert?: string | Uint8Array;
  key?: string | Uint8Array;
  servername?: string;
  rejectUnauthorized?: boolean;
}

/** What the handshake offers: binary frames, and zstd frames where this runtime can read them. */
const OFFERED_WIRE_FEATURES: string[] =
  zstdDecompress === null
    ? [WIRE_FEATURE_BINARY, WIRE_FEATURE_BINARY_2, WIRE_FEATURE_ATTACHMENTS, WIRE_FEATURE_ATTACHMENT_PROOFS]
    : [
        WIRE_FEATURE_BINARY,
        WIRE_FEATURE_BINARY_2,
        WIRE_FEATURE_ZSTD,
        WIRE_FEATURE_ATTACHMENTS,
        WIRE_FEATURE_ATTACHMENT_PROOFS,
      ];

/**
 * One typed SQL cell (queryValues): null for NULL, a number for an integer that fits one exactly
 * (a bigint otherwise) or a double, a boolean, a string, the parsed value of a JSON cell, or the
 * bytes of a binary cell (an ATTACHMENT() read, an X'..' literal).
 */
export type SqlValue = null | number | bigint | boolean | string | JsonValue | Uint8Array;

/** A parsed JSON value. */
export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export interface ConnectOptions {
  /** "user:secret", matching wire.HandshakePayload.Token. Omit against a server with no RBAC. */
  token?: string;
  /**
   * Sent in the handshake so the server authorizes the connection against the first entry.
   * An authorization-scoping hint only: per-namespace sessions are still opened lazily and
   * re-authorized on sessionBegin regardless.
   */
  namespaces?: string[];
  /** Default deadline for every request. See the note on `#request`. */
  requestTimeoutMs?: number;
  connectTimeoutMs?: number;
  signal?: AbortSignal;
  /** Required for tcps:// and wss://. */
  tls?: TlsOptions;
  maxFrameBytes?: number;
  /**
   * The handshake fails when the server's release is not the one this package was published
   * for (or reports none). Set this to talk to a server regardless: a build from source that
   * carries no release number, or a deliberate version skew you have tested yourself.
   */
  skipVersionCheck?: boolean;
  /**
   * Keep the connection on the JSON envelope: the handshake offers no wire features, so neither
   * side sends binary frames. By default the client offers binary frames and uses them when the
   * server acks them (an older server, or the JVM one, acks none and the connection stays JSON).
   */
  jsonOnly?: boolean;
  /**
   * Whether the server may zstd-compress the frames it sends. "auto" (the default) declines it on
   * a loopback address - 127.0.0.0/8, ::1, localhost - where compressing costs the server CPU and
   * saves no network, and accepts it anywhere else, as Cassandra leaves compression to each
   * connection's client. qtask's server spent 20% of its CPU compressing replies to a client on
   * the same host. "zstd" and "none" decide it regardless of the address. Ignored with jsonOnly.
   */
  compression?: "auto" | "zstd" | "none";
  /**
   * How many server sessions one namespace may use for writes at once (default 8; 1 runs every
   * write on the namespace's one session, as before). The server orders the frames of one session,
   * and an exec holds its session until its auto-commit lands, so with a single session one app
   * process ran one write at a time: each waited for the one before it, fsync included, and none
   * shared a group commit. Self-contained writes - an exec and its auto-commit, a commit carrying
   * its own transaction - take a session from a small pool instead, the way Cassandra's and
   * MongoDB's drivers keep many requests in flight per process. Leases, queries and transaction
   * control stay on the namespace's own session, and while this client holds a lease in a
   * namespace every write there does too: the server checks a lease against its holder's session.
   */
  sessionsPerNamespace?: number;
}

const DEFAULT_SESSIONS_PER_NAMESPACE = 8;

/** The write sessions of one namespace beyond its own (ConnectOptions.sessionsPerNamespace). */
interface SessionPool {
  idle: string[];
  open: number;
  waiters: Array<{ resolve: (sessionId: string) => void; reject: (error: unknown) => void }>;
}

/** Whether sql is transaction control, which only the namespace's own session may carry. */
function isTransactionControl(sql: string): boolean {
  return /^\s*(BEGIN|START\s+TRANSACTION|COMMIT|ROLLBACK|END)\b/i.test(sql);
}

/**
 * Whether uri names a loopback host. Only literal loopback addresses and "localhost" count: a
 * name that merely resolves to one is not looked up, so it keeps compression on.
 */
export function isLoopbackAddress(uri: string): boolean {
  let host: string;
  try {
    host = new URL(uri).hostname;
  } catch {
    return false;
  }
  host = host.replace(/^\[|\]$/g, "").toLowerCase();
  return host === "localhost" || host === "::1" || /^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(host);
}

/** The wire features a handshake offers under options, for a connection to uri when known. */
function offeredWireFeatures(options: ConnectOptions, uri: string | undefined): string[] {
  const compression = options.compression ?? "auto";
  const decline = compression === "none" || (compression === "auto" && uri !== undefined && isLoopbackAddress(uri));
  return decline ? OFFERED_WIRE_FEATURES.filter((f) => f !== WIRE_FEATURE_ZSTD) : OFFERED_WIRE_FEATURES;
}

interface NamespaceState {
  sessionId: string;
  head: string;
}

interface PendingRequest {
  resolve: (message: WireMessage) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
  cleanup: () => void;
}

/**
 * Dials `addr` and performs the wire handshake.
 *
 * `addr` accepts ws://, wss://, tcp://, tcps:// or a bare host:port (which means TCP, and is
 * therefore Node-only).
 */
export async function connect(addr: string, options: ConnectOptions = {}): Promise<Client> {
  const uri = /:\/\//.test(addr) ? addr : `tcp://${addr}`;
  const scheme = schemeOf(uri);
  if (!scheme) {
    throw new KdbTransportError(
      `kdb: unsupported scheme in ${JSON.stringify(addr)} ` +
        "(expected ws://, wss://, tcp://, tcps://, or a bare host:port)",
    );
  }

  const transportOptions: { connectTimeoutMs?: number; signal?: AbortSignal; tls?: TlsOptions } = {};
  if (options.connectTimeoutMs !== undefined) {
    transportOptions.connectTimeoutMs = options.connectTimeoutMs;
  }
  if (options.signal !== undefined) transportOptions.signal = options.signal;
  if (options.tls !== undefined) transportOptions.tls = options.tls;

  let transport: Transport;
  if (scheme === "ws" || scheme === "wss") {
    transport = await connectWebSocket(uri, transportOptions);
  } else {
    // Dynamic so the Node-only transport never enters a browser or edge bundle unless a
    // tcp:// URI actually reaches this line.
    const { connectTcp } = await import("./transport/tcp.ts");
    transport = await connectTcp(uri, transportOptions);
  }

  const client = new Client(transport, options, uri);
  try {
    await client.handshake(options);
  } catch (error) {
    await transport.close().catch(() => undefined);
    throw error;
  }
  return client;
}

export class Client implements KdbOperations {
  readonly #transport: Transport;
  readonly #maxFrameBytes: number;
  /** The wire features the handshake agreed; requests go out in their encodings. */
  #features: ReadonlySet<string> = new Set();
  readonly #requestTimeoutMs: number;
  readonly #authorNodeId: string;
  readonly #pending = new Map<number, PendingRequest>();
  readonly #namespaces = new Map<string, NamespaceState>();
  readonly #sessionsInFlight = new Map<string, Promise<NamespaceState>>();
  readonly #sessionLocks = new Map<string, SessionLock>();
  readonly #maxSessions: number;
  readonly #pools = new Map<string, SessionPool>();
  /** The documents this client holds a lease on, per namespace. */
  readonly #leases = new Map<string, Set<string>>();

  #correlation = 1;
  /** The address connect() dialled, when it did; "auto" compression decides by it. */
  #uri: string | undefined;
  #closed = false;
  #closeCause: Error | undefined;

  /** uri is the address the transport is connected to, when known; see ConnectOptions.compression. */
  constructor(transport: Transport, options: ConnectOptions = {}, uri?: string) {
    this.#transport = transport;
    this.#uri = uri;
    this.#maxSessions = Math.max(1, Math.floor(options.sessionsPerNamespace ?? DEFAULT_SESSIONS_PER_NAMESPACE));
    this.#maxFrameBytes = options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES;
    this.#requestTimeoutMs = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
    this.#authorNodeId = randomUuid();

    transport.onFrame((frame) => this.#onFrame(frame));
    transport.onClose((cause) => this.#onClose(cause));
  }

  /** Performs the handshake. Called by `connect`; public so a custom transport can be driven. */
  async handshake(options: ConnectOptions = {}): Promise<void> {
    const request: HandshakeDto = {
      nodeId: "kdb-client-ts",
      // These two must be [] and {}, never null. Kotlin's HandshakeDto declares them as
      // non-nullable List<String> / Map<String, String> with no default, so a JSON null makes
      // the JVM's strict decoder throw and the connection dies with no response at all -
      // not a clean rejection, just silence. Go hit exactly this and fixed it the same way
      // (go/kdb/wire/payload_mapper.go:31-45).
      namespaces: options.namespaces ?? [],
      localHeads: {},
      supportsZstd: false,
      supportsIndexHints: false,
      supportsDirectDeltaIngest: false,
      maxFrameBytes: this.#maxFrameBytes,
      // JSON only, honestly advertised: this client implements no other payload codec, so
      // negotiating KDB_BINARY would be a promise it cannot keep.
      preferredEncodings: ["JSON"],
      clientMode: CLIENT_MODE_SQL,
      protocolVersion: KDB_WIRE_PROTOCOL_VERSION,
    };
    if (options.token !== undefined && options.token !== "") request.token = options.token;
    if (!options.jsonOnly) request.wireFeatures = offeredWireFeatures(options, this.#uri);

    const callOptions: CallOptions = {};
    if (options.signal !== undefined) callOptions.signal = options.signal;
    if (options.connectTimeoutMs !== undefined) callOptions.timeoutMs = options.connectTimeoutMs;

    const reply = await this.#request("handshake", request, callOptions);
    const ack = this.#expect<HandshakeAckDto>(reply, "handshakeAck");
    if (!ack.accepted) {
      throw new KdbUnauthenticatedError(
        `kdb: handshake rejected: ${ack.rejectionReason ?? "no reason given"}`,
      );
    }
    if (!options.skipVersionCheck) {
      const problem = checkServerCompatible(ack.serverVersion);
      if (problem) throw new KdbProtocolError(`kdb: ${problem}`);
    }
    if (ack.negotiatedEncoding !== "JSON") {
      throw new KdbProtocolError(
        `kdb: server negotiated ${ack.negotiatedEncoding}, but this client implements JSON only`,
      );
    }
    // Binary frames only once the server has said it reads them. The ack itself is JSON.
    const acked = new Set(options.jsonOnly ? [] : (ack.wireFeatures ?? []));
    this.#features = acked.has(WIRE_FEATURE_BINARY)
      ? new Set(OFFERED_WIRE_FEATURES.filter((f) => acked.has(f)))
      : new Set();
  }

  /** The wire features the handshake agreed; empty when the connection is on the JSON envelope. */
  get wireFeatures(): string[] {
    return [...this.#features];
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    await this.#transport.close();
    this.#onClose();
  }

  // --- documents ----------------------------------------------------------------------------

  async putJSON(
    ns: string,
    docId: string,
    body: unknown,
    options: CallOptions = {},
  ): Promise<CommitHash> {
    return this.#writeTransaction(ns, docId, body, undefined, options);
  }

  async delete(ns: string, docId: string, options: CallOptions = {}): Promise<CommitHash> {
    assertUuid(docId);
    const state = await this.#ensureNamespace(ns, options);
    const tx = this.#buildTransaction(state.head, [{ kind: "delete", docId }], undefined);
    return this.#commitTransaction(ns, state, tx, options);
  }

  async get(
    ns: string,
    docId: string,
    options: CallOptions = {},
  ): Promise<{ body: unknown; commit: CommitHash }> {
    const reply = await this.#request("documentGet", { namespace: ns, docId }, options, "get");
    const result = this.#expect<DocumentGetResultDto>(reply, "documentGetResult");
    this.#throwIfError(result.error, result.errorCode, result.retryAfterMs);
    if (result.json === undefined || result.json === null) {
      throw new KdbNotFoundError(ns, docId);
    }
    return { body: JSON.parse(result.json), commit: result.commitHex };
  }

  async getWithHash(
    ns: string,
    docId: string,
    options: CallOptions = {},
  ): Promise<{ body: unknown; contentHash: string }> {
    const { body } = await this.get(ns, docId, options);
    // Hashed from the canonical form this client would write, not from the server's byte-exact
    // text: the server compares against a hash of what it stores, and re-serializing here is
    // what makes the round trip idempotent for a caller that reads, edits and writes back.
    const json = JSON.stringify(body);
    return { body, contentHash: await contentHash(assertUuid(docId), json) };
  }

  async upsert(
    ns: string,
    docId: string,
    body: unknown,
    options: CallOptions = {},
  ): Promise<CommitHash> {
    assertUuid(docId);
    const state = await this.#ensureNamespace(ns, options);
    const reply = await this.#request(
      "upsert",
      {
        namespace: ns,
        docId,
        json: JSON.stringify(body),
        // Always sent. Upsert needs no session of its own, but this is how the server tells a
        // lease holder's own upsert from a stranger's, and an empty value reads as "not the
        // holder" (go/kdb/wire/document_ops.go:41-47).
        sessionId: state.sessionId,
      },
      options,
      "upsert",
    );
    const result = this.#expect<UpsertResultDto>(reply, "upsertResult");
    this.#throwIfError(result.error, result.errorCode, result.retryAfterMs);
    return result.commitHex;
  }

  async appendEvent(
    ns: string,
    docId: string,
    body: unknown,
    options: CallOptions = {},
  ): Promise<void> {
    await this.upsert(ns, docId, body, options);
  }

  // --- conditional writes -------------------------------------------------------------------

  async replace(
    ns: string,
    docId: string,
    body: unknown,
    options: CallOptions = {},
  ): Promise<CommitHash> {
    return this.#writeTransaction(ns, docId, body, undefined, options, true);
  }

  async putIfAbsent(
    ns: string,
    docId: string,
    body: unknown,
    options: CallOptions = {},
  ): Promise<CommitHash> {
    return this.#writeTransaction(ns, docId, body, { opIndex: 0, kind: "EXPECT_ABSENT" }, options);
  }

  async replaceIf(
    ns: string,
    docId: string,
    body: unknown,
    expectedContentHash: string,
    options: CallOptions = {},
  ): Promise<CommitHash> {
    return this.#writeTransaction(
      ns,
      docId,
      body,
      { opIndex: 0, kind: "EXPECT_CONTENT_HASH", contentHashHex: expectedContentHash },
      options,
      true,
    );
  }

  async replaceIfPresent(
    ns: string,
    docId: string,
    body: unknown,
    options: CallOptions = {},
  ): Promise<CommitHash> {
    return this.#writeTransaction(
      ns,
      docId,
      body,
      { opIndex: 0, kind: "EXPECT_PRESENT" },
      options,
      true,
    );
  }

  async compareAndSwap(
    ns: string,
    docId: string,
    mutate: (current: unknown | null) => unknown | null | Promise<unknown | null>,
    options: CompareAndSwapOptions = {},
  ): Promise<CommitHash> {
    const attempts = options.attempts && options.attempts > 0 ? options.attempts : 5;
    let lastError: KdbError | undefined;

    for (let attempt = 0; attempt < attempts; attempt++) {
      this.#throwIfAborted(options.signal);

      let current: unknown | null = null;
      let hash = "";
      try {
        const read = await this.getWithHash(ns, docId, options);
        current = read.body;
        hash = read.contentHash;
      } catch (error) {
        if (!(error instanceof KdbNotFoundError)) throw error;
      }

      const next = await mutate(current);
      if (next === null || next === undefined) {
        throw new KdbAbortedError();
      }

      try {
        return hash === ""
          ? await this.putIfAbsent(ns, docId, next, options)
          : await this.replaceIf(ns, docId, next, hash, options);
      } catch (error) {
        // Only a lost race is worth another attempt. A schema violation or a unique-constraint
        // collision fails identically next time and just burns the attempt budget.
        if (!(error instanceof KdbPreconditionError) && !(error instanceof KdbConflictError)) {
          throw error;
        }
        lastError = error;
        if (attempt < attempts - 1) {
          await this.#waitBackoff(attempt, error, options.signal);
        }
      }
    }

    throw new KdbError(
      `kdb: compare-and-swap on ${docId} gave up after ${attempts} attempts`,
      { cause: lastError },
    );
  }

  async commit(tx: Transaction, options: CallOptions = {}): Promise<CommitHash> {
    const state = await this.#ensureNamespace(tx.namespace, options);
    const operations: OpDto[] = [
      ...tx.writes.map((write): OpDto => ({
        kind: "write",
        docId: assertUuid(write.docId),
        patch: JSON.stringify(write.body),
        ...(write.replace ? { replace: true } : {}),
      })),
      ...(tx.deletes ?? []).map((docId): OpDto => ({ kind: "delete", docId: assertUuid(docId) })),
      ...(tx.attachments ?? []).map(attachmentOpDto),
    ];
    const preconditions: PreconditionDto[] | undefined = tx.preconditions?.length
      ? tx.preconditions.map((p) =>
          p.kind === "EXPECT_CONTENT_HASH"
            ? { opIndex: p.opIndex, kind: p.kind, contentHashHex: p.contentHashHex }
            : { opIndex: p.opIndex, kind: p.kind },
        )
      : undefined;

    return this.#commitTransaction(
      tx.namespace,
      state,
      this.#buildTransaction(tx.baseVersion, operations, preconditions),
      options,
    );
  }

  // --- SQL ----------------------------------------------------------------------------------

  async query<T = Row>(
    ns: string,
    sql: string,
    args: unknown[] = [],
    options: CallOptions = {},
  ): Promise<T[]> {
    const { columns, rows } = await this.queryRaw(ns, sql, args, options);
    return rows.map((row) => {
      const out: Record<string, string> = {};
      for (let i = 0; i < columns.length; i++) {
        const column = columns[i];
        if (column !== undefined) out[column] = row[i] ?? "";
      }
      return out as T;
    });
  }

  async queryRaw(
    ns: string,
    sql: string,
    args: unknown[] = [],
    options: CallOptions = {},
  ): Promise<{ columns: string[]; rows: string[][] }> {
    const result = await this.#execSql(ns, sql, args, options);
    return { columns: result.columns, rows: result.rows };
  }

  /**
   * Runs a SELECT and returns typed cells (SqlValue), which tells a NULL from "" and a number from
   * text. The types come from the server; one that predates them sends none, and then every cell
   * is a string, NULL included as "".
   */
  async queryValues(
    ns: string,
    sql: string,
    args: unknown[] = [],
    options: CallOptions = {},
  ): Promise<{ columns: string[]; rows: SqlValue[][] }> {
    const result = await this.#execSql(ns, sql, args, options);
    return { columns: result.columns, rows: typedRows(result.rows, result.cellKinds) };
  }

  async *queryRawPages(
    ns: string,
    sql: string,
    args: unknown[] = [],
    pageSize = 0,
    options: CallOptions = {},
  ): AsyncGenerator<{ columns: string[]; rows: string[][] }, void, undefined> {
    // 0 asks for the largest page the server allows; it clamps an oversized request itself.
    const size = pageSize > 0 ? Math.floor(pageSize) : 2 ** 30;
    let continuation: string | undefined;
    do {
      const page = await this.#execSql(ns, sql, args, options, { pageSize: size, continuation });
      yield { columns: page.columns, rows: page.rows };
      continuation = page.continuation;
    } while (continuation !== undefined);
  }

  /**
   * queryRawPages with typed cells, as queryValues returns them: a SELECT read in pages of at most
   * pageSize rows (0: the largest the server allows), every page from the same snapshot, with
   * NULLs, numbers, booleans and JSON decoded.
   */
  async *queryValuesPages(
    ns: string,
    sql: string,
    args: unknown[] = [],
    pageSize = 0,
    options: CallOptions = {},
  ): AsyncGenerator<{ columns: string[]; rows: SqlValue[][] }, void, undefined> {
    const size = pageSize > 0 ? Math.floor(pageSize) : 2 ** 30;
    let continuation: string | undefined;
    do {
      const page = await this.#execSql(ns, sql, args, options, { pageSize: size, continuation });
      yield { columns: page.columns, rows: typedRows(page.rows, page.cellKinds) };
      continuation = page.continuation;
    } while (continuation !== undefined);
  }

  async queryAll<T = Row>(
    ns: string,
    sql: string,
    args: unknown[] = [],
    pageSize = 0,
    options: CallOptions = {},
  ): Promise<T[]> {
    const out: T[] = [];
    for await (const { columns, rows } of this.queryRawPages(ns, sql, args, pageSize, options)) {
      for (const row of rows) {
        const obj: Record<string, string> = {};
        for (let i = 0; i < columns.length; i++) {
          const column = columns[i];
          if (column !== undefined) obj[column] = row[i] ?? "";
        }
        out.push(obj as T);
      }
    }
    return out;
  }

  async exec(
    ns: string,
    sql: string,
    args: unknown[] = [],
    options: CallOptions = {},
  ): Promise<number> {
    const result = await this.#execAutoCommit(ns, sql, args, options);
    return result.rowsAffected;
  }

  /**
   * Runs `UPDATE ... [ORDER BY ...] [LIMIT n] RETURNING ...` and returns the rows RETURNING
   * produced - one per updated document, as it is after the update - with typed cells, as
   * queryValues types them. It is the atomic queue claim (MongoDB's findOneAndUpdate):
   *
   *     UPDATE docs SET status = 'processing', attempts = attempts + 1
   *      WHERE _c = 'jobs' AND status = 'pending' AND attempts < 3
   *      ORDER BY createdAt LIMIT 1 RETURNING kdb_id, _doc
   *
   * The server selects the documents and commits the update in one step under its write gate, so
   * concurrent claims never take the same document and never fail with a version conflict because
   * another claim committed first. No match returns no rows and writes nothing. Any other DML
   * statement is run and auto-committed as exec runs it, returning no rows.
   */
  async execReturning(
    ns: string,
    sql: string,
    args: unknown[] = [],
    options: CallOptions = {},
  ): Promise<{ columns: string[]; rows: SqlValue[][]; rowsAffected: number }> {
    const result = await this.#execAutoCommit(ns, sql, args, options);
    return {
      columns: result.columns,
      rows: typedRows(result.rows, result.cellKinds),
      rowsAffected: result.rowsAffected,
    };
  }

  /** exec and execReturning: one statement, and its commit when the server left one pending. */
  async #execAutoCommit(
    ns: string,
    sql: string,
    args: unknown[],
    options: CallOptions,
  ): Promise<{ columns: string[]; rows: string[][]; cellKinds: string | undefined; rowsAffected: number }> {
    // The sqlExec and its auto-commit are one unit of work on a session that holds a single
    // pending transaction, so nothing else may use that session between them: a pooled session
    // is this unit's alone, the namespace's own is held exclusively (see SessionLock).
    return this.#withWriteSession(ns, options, "exclusive", !isTransactionControl(sql), async (sessionId) => {
      const result = await this.#execSql(ns, sql, args, options, undefined, sessionId);
      if (result.readOnly) {
        if (result.rowsAffected > 0 && result.resolvedCommitHex) {
          // A write the server committed on its own (an UPDATE with ORDER BY, LIMIT or
          // RETURNING): the commit it made is the head this client now knows.
          const state = await this.#ensureNamespace(ns, options);
          state.head = result.resolvedCommitHex;
          this.#namespaces.set(ns, state);
        }
        return result;
      }

      // A write statement is not durable until it is committed, and this call is one
      // client-visible unit of work rather than two, so the auto-commit happens here. The
      // txCommit carries no transactionBytes: it commits whatever the session accumulated.
      // Matching go/kdb/client/query.go:44-72. Omitting this is the bug that passes every unit
      // test and loses writes in production.
      const state = await this.#ensureNamespace(ns, options);
      const reply = await this.#request(
        "txCommit",
        { namespace: ns, sessionId, transactionBytes: [] },
        options,
      );
      this.#handleCommitReply(ns, state, reply);
      // The count comes from the statement, not the commit: the commit reports the operations
      // it applied, which is not the number of rows a DELETE matched.
      return result;
    });
  }

  // --- leases -------------------------------------------------------------------------------

  async acquireLock(
    ns: string,
    docId: string,
    ttlMs = 0,
    options: CallOptions = {},
  ): Promise<Lease> {
    return this.#lockRequest("lockAcquire", ns, docId, ttlMs, options);
  }

  async renewLock(ns: string, docId: string, ttlMs = 0, options: CallOptions = {}): Promise<Lease> {
    return this.#lockRequest("lockRenew", ns, docId, ttlMs, options);
  }

  async releaseLock(ns: string, docId: string, options: CallOptions = {}): Promise<void> {
    const state = await this.#ensureNamespace(ns, options);
    const reply = await this.#request(
      "lockRelease",
      { namespace: ns, sessionId: state.sessionId, docId },
      options,
      "releaseLock",
    );
    this.#leases.get(ns)?.delete(docId);
    const result = this.#expect<LockResultDto>(reply, "lockResult");
    if (!result.granted && result.error) {
      throw new KdbLockError(docId, result.holderSessionId ?? undefined, `kdb: ${result.error}`);
    }
  }

  // --- attachments --------------------------------------------------------------------------

  /**
   * Stores bytes in the namespace's attachment store and returns their content hash and size -
   * nothing references them yet. Commit a reference with `commit` (Transaction.attachments) or
   * bind it in SQL with `sqlAttachment`; `putAttachment` does both steps. Sent in 1 MiB chunks,
   * one in flight at a time; the blob is durable once this resolves.
   *
   * A failure part way throws KdbUploadError, whose `uploadId` and `received` resume the upload
   * (options.resume) with the source re-opened at `received`.
   */
  async uploadAttachment(
    ns: string,
    data: AttachmentSource,
    options: UploadAttachmentOptions = {},
  ): Promise<AttachmentBlob> {
    this.#requireAttachments("uploadAttachment");
    const state = await this.#ensureNamespace(ns, options);
    let uploadId = options.resume?.uploadId ?? "";
    let received = options.resume?.received ?? 0;
    const hasher = options.verify === false || options.resume ? undefined : new AttachmentHasher();

    for await (const { data: chunk, last } of uploadChunks(data)) {
      let result: AttachmentUploadResultDto;
      try {
        const reply = await this.#request(
          "attachmentUpload",
          {
            namespace: ns,
            sessionId: state.sessionId,
            uploadId,
            offset: received,
            data: chunk,
            final: last,
          },
          options,
          "uploadAttachment",
        );
        result = this.#expect<AttachmentUploadResultDto>(reply, "attachmentUploadResult");
      } catch (cause) {
        throw new KdbUploadError(
          `kdb: attachment upload interrupted after ${received} bytes`,
          uploadId,
          received,
          { cause, ...(cause instanceof KdbError && cause.code ? { code: cause.code } : {}) },
        );
      }
      if (result.uploadId) uploadId = result.uploadId;
      if (result.error) {
        const cause = classifiedError(result.error, (result.errorCode || undefined) as ErrorCode | undefined);
        throw new KdbUploadError(
          `kdb: attachment upload failed: ${result.error}`,
          uploadId,
          result.received || received,
          { cause, ...(cause.code ? { code: cause.code } : {}) },
        );
      }
      if (result.received !== received + chunk.length) {
        throw new KdbUploadError(
          `kdb: attachment upload: server has ${result.received} bytes, expected ${received + chunk.length}`,
          uploadId,
          result.received,
        );
      }
      received = result.received;
      await hasher?.update(chunk);
      options.onProgress?.(received);
      if (last) {
        if (!result.hash || result.size !== received) {
          throw new KdbProtocolError(
            `kdb: attachment upload finished without a content hash (size ${result.size}, sent ${received})`,
          );
        }
        if (hasher) {
          const local = await hasher.digest();
          if (local !== result.hash) {
            throw new KdbIntegrityError(
              `kdb: uploaded attachment hashes to ${result.hash} on the server, ${local} here`,
              local,
              result.hash,
            );
          }
        }
        return { hash: result.hash, size: result.size };
      }
    }
    throw new KdbProtocolError("kdb: attachment upload ended without a final chunk");
  }

  /**
   * Uploads `data` and resolves which attachment of `docId` it is to become, returning the
   * operation for Transaction.attachments - so attachments can share a commit with document
   * writes. Nothing is committed. See PutAttachmentOptions for how `id` / `name` pick it.
   */
  async prepareAttachment(
    ns: string,
    docId: string,
    data: AttachmentSource,
    options: PutAttachmentOptions = {},
  ): Promise<AttachmentPutOp> {
    assertUuid(docId);
    if (options.id !== undefined) assertUuid(options.id, "attachment id");
    this.#requireAttachments("prepareAttachment");
    // Resolve first: a failed lookup should not cost an upload.
    let id = options.id;
    let name = options.name;
    let mime = options.mime;
    if (id === undefined && name === undefined) {
      id = randomUuid();
      name = id;
    } else if (id === undefined || name === undefined || mime === undefined) {
      const items = await this.listAttachments(ns, docId, options);
      const existing = findAttachment(items, id !== undefined ? { id } : { name: name! });
      id ??= existing?.id ?? randomUuid();
      name ??= existing?.name ?? id;
      mime ??= existing?.mime;
    }
    const blob = await this.uploadAttachment(ns, data, options);
    return { kind: "put", docId, id, name, hash: blob.hash, size: blob.size, mime: mime ?? "" };
  }

  /**
   * Creates or replaces one attachment of a document, as its own commit. With `name` alone an
   * existing attachment of that name is replaced (keeping its ID and, unless `mime` is given, its
   * media type); with neither `id` nor `name` a new attachment is created, named after its ID.
   */
  async putAttachment(
    ns: string,
    docId: string,
    data: AttachmentSource,
    options: PutAttachmentOptions = {},
  ): Promise<{ commit: CommitHash; attachment: AttachmentInfo }> {
    const op = await this.prepareAttachment(ns, docId, data, options);
    const commit = await this.#commitAttachmentOps(ns, [op], options);
    return {
      commit,
      attachment: { id: op.id, name: op.name, hash: op.hash, size: op.size, mime: op.mime ?? "" },
    };
  }

  /** A document's attachments, sorted by name. Empty for a document with none (or no document). */
  async listAttachments(
    ns: string,
    docId: string,
    options: ListAttachmentsOptions = {},
  ): Promise<AttachmentInfo[]> {
    assertUuid(docId);
    this.#requireAttachments("listAttachments");
    const state = await this.#ensureNamespace(ns, options);
    const reply = await this.#request(
      "attachmentList",
      { namespace: ns, sessionId: state.sessionId, docId, asOfHex: options.asOf ?? "" },
      options,
      "listAttachments",
    );
    const result = this.#expect<AttachmentListResultDto>(reply, "attachmentListResult");
    this.#throwIfAttachmentError(result.error, result.errorCode, ns, docId);
    return result.items.map(infoFromDto);
  }

  async countAttachments(ns: string, docId: string, options: ListAttachmentsOptions = {}): Promise<number> {
    return (await this.listAttachments(ns, docId, options)).length;
  }

  /**
   * Opens a read of one attachment and returns once its first page has arrived - so a missing
   * attachment throws KdbAttachmentNotFoundError here, and `info` is known before any byte is
   * consumed. The rest is read page by page as the reader is iterated.
   */
  async getAttachment(
    ns: string,
    docId: string,
    selector: AttachmentSelector,
    options: ReadAttachmentOptions = {},
  ): Promise<AttachmentReader> {
    assertUuid(docId);
    this.#requireAttachments("getAttachment");
    const state = await this.#ensureNamespace(ns, options);
    const { id, name, text } = selectorParts(selector);
    const offset = Math.max(0, Math.floor(options.offset ?? 0));
    const maxBytes = options.pageBytes && options.pageBytes > 0 ? Math.floor(options.pageBytes) : 0;
    const fetchPage = async (readId: string, at: number, close = false): Promise<AttachmentDataDto> => {
      const reply = await this.#request(
        "attachmentGet",
        readId
          ? { namespace: ns, sessionId: state.sessionId, docId, readId, offset: at, maxBytes, close }
          : {
              namespace: ns,
              sessionId: state.sessionId,
              docId,
              attachmentId: id ?? "",
              name: name ?? "",
              asOfHex: options.asOf ?? "",
              offset: at,
              maxBytes,
            },
        options,
        "getAttachment",
      );
      const page = this.#expect<AttachmentDataDto>(reply, "attachmentData");
      if (!close) this.#throwIfAttachmentError(page.error, page.errorCode, ns, docId, text);
      return page;
    };
    const first = await fetchPage("", offset);
    return new PagedAttachmentReader(first, offset, options.verify !== false && offset === 0, fetchPage);
  }

  /** Reads one attachment whole. Verified against its content hash unless options.verify is false. */
  async readAttachment(
    ns: string,
    docId: string,
    selector: AttachmentSelector,
    options: ReadAttachmentOptions = {},
  ): Promise<{ info: AttachmentInfo; data: Uint8Array; commit: CommitHash; superseded: boolean }> {
    const reader = await this.getAttachment(ns, docId, selector, options);
    const data = await collectBytes(reader);
    return { info: reader.info, data, commit: reader.commit, superseded: reader.superseded };
  }

  /**
   * Opens a verified read of part of one attachment: `options.length` bytes from `options.offset`
   * (length omitted or -1: to the end; a range running past the end stops there). Every 1 MiB
   * chunk the range touches is checked against the attachment's content hash with its Merkle
   * inclusion proof before any of its bytes is handed out, so tampering throws KdbIntegrityError
   * with no bad byte delivered. Returns once the first page has arrived and checked out.
   *
   * An offset past the end (or negative) throws KdbAttachmentRangeError. The server must have
   * agreed the "attachment-proofs" wire feature, else KdbUnsupportedError. The read pins the
   * version it started on; once the range is consumed it is released, and `superseded` says
   * whether a newer version committed meanwhile.
   */
  async openAttachmentRange(
    ns: string,
    docId: string,
    selector: AttachmentSelector,
    options: AttachmentRangeOptions = {},
  ): Promise<AttachmentReader> {
    assertUuid(docId);
    this.#requireAttachments("openAttachmentRange");
    if (!this.#features.has(WIRE_FEATURE_ATTACHMENT_PROOFS)) {
      throw new KdbUnsupportedError(
        "openAttachmentRange",
        `the connection did not agree wire feature "${WIRE_FEATURE_ATTACHMENT_PROOFS}" (an older server)`,
      );
    }
    const offset = options.offset ?? 0;
    const length = options.length ?? -1;
    if (!Number.isSafeInteger(offset)) throw new RangeError(`kdb: invalid attachment range offset ${offset}`);
    if (!Number.isSafeInteger(length) || length < -1) {
      throw new RangeError(`kdb: invalid attachment range length ${length}`);
    }
    if (offset < 0) throw new KdbAttachmentRangeError(offset, -1);
    const state = await this.#ensureNamespace(ns, options);
    const { id, name, text } = selectorParts(selector);

    const getPage = async (readId: string, at: number, maxBytes: number, close: boolean): Promise<AttachmentDataDto> => {
      const reply = await this.#request(
        "attachmentGet",
        readId
          ? { namespace: ns, sessionId: state.sessionId, docId, readId, offset: at, maxBytes, close, proofs: !close }
          : {
              namespace: ns,
              sessionId: state.sessionId,
              docId,
              attachmentId: id ?? "",
              name: name ?? "",
              asOfHex: options.asOf ?? "",
              offset: at,
              maxBytes,
              proofs: true,
            },
        options,
        "openAttachmentRange",
      );
      return this.#expect<AttachmentDataDto>(reply, "attachmentData");
    };
    const fetchPage = async (readId: string, at: number, maxBytes: number, close = false): Promise<AttachmentDataDto> => {
      const page = await getPage(readId, at, maxBytes, close);
      if (!close) this.#throwIfAttachmentError(page.error, page.errorCode, ns, docId, text);
      return page;
    };

    const maxBytes = length >= 0 ? Math.min(Math.max(length, 1), MAX_ATTACHMENT_CHUNK_BYTES) : MAX_ATTACHMENT_CHUNK_BYTES;
    const first = await getPage("", offset, maxBytes, false);
    // Past the end the server refuses the get but still says how long the attachment is.
    if (first.error && first.info.id !== "" && offset > first.info.size) {
      throw new KdbAttachmentRangeError(offset, first.info.size);
    }
    this.#throwIfAttachmentError(first.error, first.errorCode, ns, docId, text);
    const reader = new RangedAttachmentReader(first, offset, length, fetchPage);
    try {
      await reader.takeFirst(first);
    } catch (error) {
      await reader.cancel();
      throw error;
    }
    return reader;
  }

  /**
   * Reads `length` bytes of one attachment from `offset` (-1: to the end), verified chunk by
   * chunk as `openAttachmentRange` does - like an io.ReaderAt: fewer bytes come back at the end,
   * none at exactly the end, and an offset past it throws KdbAttachmentRangeError. Each call is its
   * own read; to read one version across calls, pass a commit as `options.asOf` (a reader's
   * `commit`, say).
   */
  async readAttachmentAt(
    ns: string,
    docId: string,
    selector: AttachmentSelector,
    offset: number,
    length: number,
    options: Omit<AttachmentRangeOptions, "offset" | "length"> = {},
  ): Promise<Uint8Array> {
    const reader = await this.openAttachmentRange(ns, docId, selector, { ...options, offset, length });
    return collectBytes(reader);
  }

  /** Renames one attachment, keeping its ID and bytes, as its own commit. */
  async renameAttachment(
    ns: string,
    docId: string,
    selector: AttachmentSelector,
    newName: string,
    options: CallOptions = {},
  ): Promise<CommitHash> {
    const existing = await this.#resolveAttachment(ns, docId, selector, options);
    return this.#commitAttachmentOps(
      ns,
      [{ kind: "put", docId, id: existing.id, name: newName, hash: existing.hash, size: existing.size, mime: existing.mime }],
      options,
    );
  }

  /**
   * Removes one attachment, as its own commit. By ID it commits the delete directly; by name it
   * looks the ID up first, and throws KdbAttachmentNotFoundError when there is none of that name.
   */
  async deleteAttachment(
    ns: string,
    docId: string,
    selector: AttachmentSelector,
    options: CallOptions = {},
  ): Promise<CommitHash> {
    assertUuid(docId);
    const { id } = selectorParts(selector);
    const attachmentId =
      id !== undefined
        ? assertUuid(id, "attachment id")
        : (await this.#resolveAttachment(ns, docId, selector, options)).id;
    return this.#commitAttachmentOps(ns, [{ kind: "delete", docId, id: attachmentId }], options);
  }

  async #resolveAttachment(
    ns: string,
    docId: string,
    selector: AttachmentSelector,
    options: CallOptions,
  ): Promise<AttachmentInfo> {
    const found = findAttachment(await this.listAttachments(ns, docId, options), selector);
    if (!found) throw new KdbAttachmentNotFoundError(ns, docId, selectorParts(selector).text);
    return found;
  }

  async #commitAttachmentOps(ns: string, ops: AttachmentOp[], options: CallOptions): Promise<CommitHash> {
    const state = await this.#ensureNamespace(ns, options);
    const tx = this.#buildTransaction(state.head, ops.map(attachmentOpDto), undefined);
    return this.#commitTransaction(ns, state, tx, options);
  }

  #requireAttachments(operation: string): void {
    if (!this.#features.has(WIRE_FEATURE_ATTACHMENTS)) {
      throw new KdbUnsupportedError(
        operation,
        `the connection did not agree wire feature "${WIRE_FEATURE_ATTACHMENTS}" ` +
          "(an older or JVM server, or a jsonOnly connection)",
      );
    }
  }

  #throwIfAttachmentError(error: string, code: string, ns: string, docId: string, attachment?: string): void {
    if (!error) return;
    if (code === "NOT_FOUND") {
      throw attachment !== undefined
        ? new KdbAttachmentNotFoundError(ns, docId, attachment)
        : new KdbNotFoundError(ns, docId);
    }
    throw classifiedError(error, (code || undefined) as ErrorCode | undefined);
  }

  // --- internals ----------------------------------------------------------------------------

  async #lockRequest(
    kind: "lockAcquire" | "lockRenew",
    ns: string,
    docId: string,
    ttlMs: number,
    options: CallOptions,
  ): Promise<Lease> {
    const state = await this.#ensureNamespace(ns, options);
    const reply = await this.#request(
      kind,
      {
        namespace: ns,
        sessionId: state.sessionId,
        docId,
        // Zero asks the server for its configured default rather than for a lease that never
        // expires - a client-held lock with no expiry is the failure mode leases exist to remove.
        ttlMillis: ttlMs > 0 ? ttlMs : 0,
      },
      options,
      kind,
    );
    const result = this.#expect<LockResultDto>(reply, "lockResult");
    if (result.granted) {
      let held = this.#leases.get(ns);
      if (!held) this.#leases.set(ns, (held = new Set()));
      held.add(docId);
    }
    if (!result.granted) {
      throw new KdbLockError(
        docId,
        result.holderSessionId ?? undefined,
        `kdb: document lock unavailable: ${result.error ?? "held by another session"}`,
        result.errorCode ? { code: result.errorCode as ErrorCode } : {},
      );
    }
    return {
      namespace: ns,
      docId,
      fence: result.fence,
      expiresAtMs: result.expiresAtMillis,
    };
  }

  async #writeTransaction(
    ns: string,
    docId: string,
    body: unknown,
    precondition: PreconditionDto | undefined,
    options: CallOptions,
    replace = false,
  ): Promise<CommitHash> {
    assertUuid(docId);
    const state = await this.#ensureNamespace(ns, options);
    const operations: OpDto[] = [
      replace
        ? { kind: "write", docId, patch: JSON.stringify(body), replace: true }
        : { kind: "write", docId, patch: JSON.stringify(body) },
    ];
    const tx = this.#buildTransaction(
      state.head,
      operations,
      precondition ? [precondition] : undefined,
    );
    return this.#commitTransaction(ns, state, tx, options);
  }

  #buildTransaction(
    baseVersionHex: string,
    operations: OpDto[],
    preconditions: PreconditionDto[] | undefined,
  ): TransactionDto {
    const tx: TransactionDto = {
      id: randomUuid(),
      baseVersionHex,
      timestampMicros: Date.now() * 1000,
      authorNodeId: this.#authorNodeId,
      operations,
    };
    // Only when non-empty. The Go encoder tags preconditions omitempty so a transaction
    // without any encodes exactly as it did before the field existed; emitting [] would break
    // that compatibility guarantee for an older peer.
    if (preconditions && preconditions.length > 0) tx.preconditions = preconditions;
    return tx;
  }

  async #commitTransaction(
    ns: string,
    state: NamespaceState,
    tx: TransactionDto,
    options: CallOptions,
  ): Promise<CommitHash> {
    // On the namespace's own session: shared with other byte-carrying commits, exclusive against
    // exec - the server clears the session's pending transaction as this lands, so it must not
    // land inside an exec. On a pooled one it is alone.
    const reply = await this.#withWriteSession(ns, options, "shared", true, (sessionId) =>
      this.#request(
        "txCommit",
        {
          namespace: ns,
          sessionId,
          transactionBytes: bytesToWire(utf8Encode(JSON.stringify(tx))),
        },
        options,
      ),
    );
    const commit = this.#handleCommitReply(ns, state, reply);
    if (commit === undefined) {
      throw new KdbProtocolError("kdb: commit succeeded but returned no commit hash");
    }
    return commit;
  }

  #handleCommitReply(
    ns: string,
    state: NamespaceState,
    reply: WireMessage,
  ): CommitHash | undefined {
    if (reply.kind === "conflictReport") {
      throw decodeConflictError(reply.payload as ConflictReportDto);
    }
    const result = this.#expect<SqlResultDto>(reply, "sqlResult");
    this.#throwIfError(result.error, result.errorCode, result.retryAfterMs);
    if (result.resolvedCommitHex) {
      // Advancing the cached head is what keeps putJSON a single round trip. Getting its
      // invalidation wrong shows up as spurious conflicts on the next write.
      state.head = result.resolvedCommitHex;
      this.#namespaces.set(ns, state);
    }
    return result.resolvedCommitHex || undefined;
  }

  async #execSql(
    ns: string,
    sql: string,
    args: unknown[],
    options: CallOptions,
    paging?: { pageSize: number; continuation?: string },
    sessionId?: string,
  ): Promise<{
    columns: string[];
    rows: string[][];
    readOnly: boolean;
    rowsAffected: number;
    continuation: string | undefined;
    cellKinds: string | undefined;
    resolvedCommitHex: string | undefined;
  }> {
    const state = await this.#ensureNamespace(ns, options);
    const reply = await this.#request(
      "sqlExec",
      {
        namespace: ns,
        sessionId: sessionId ?? state.sessionId,
        sql,
        parametersJson: args.length > 0 ? JSON.stringify(args.map(sqlParam)) : null,
        ...(paging
          ? {
              pageSize: paging.pageSize,
              ...(paging.continuation ? { continuation: paging.continuation } : {}),
            }
          : {}),
      },
      options,
    );
    const result = this.#expect<SqlResultDto>(reply, "sqlResult");
    this.#throwIfError(result.error, result.errorCode, result.retryAfterMs);
    return {
      columns: result.columns ?? [],
      rows: result.rows ?? [],
      readOnly: result.readOnly,
      rowsAffected: result.rowsAffected ?? 0,
      continuation: result.continuation || undefined,
      cellKinds: result.cellKinds || undefined,
      resolvedCommitHex: result.resolvedCommitHex || undefined,
    };
  }

  /**
   * Runs one self-contained write on a session of its own from the namespace's pool, or, when
   * pooling is off, the statement must stay on the namespace's session, or this client holds a
   * lease in the namespace, on the namespace's own session under its lock (mode).
   */
  async #withWriteSession<T>(
    ns: string,
    options: CallOptions,
    mode: "exclusive" | "shared",
    poolable: boolean,
    run: (sessionId: string) => Promise<T>,
  ): Promise<T> {
    const state = await this.#ensureNamespace(ns, options);
    if (this.#maxSessions <= 1 || !poolable || (this.#leases.get(ns)?.size ?? 0) > 0) {
      const lock = this.#sessionLock(ns);
      return mode === "exclusive" ? lock.exclusive(() => run(state.sessionId)) : lock.shared(() => run(state.sessionId));
    }
    const sessionId = await this.#acquirePooled(ns, options);
    let healthy = false;
    try {
      const out = await run(sessionId);
      healthy = true;
      return out;
    } finally {
      // A unit that failed in any way may have left half of itself on the session - an exec
      // whose commit never landed - so the session is not reused. Opening another is one round
      // trip, and failures are rare.
      this.#releasePooled(ns, sessionId, healthy);
    }
  }

  async #acquirePooled(ns: string, options: CallOptions): Promise<string> {
    let pool = this.#pools.get(ns);
    if (!pool) this.#pools.set(ns, (pool = { idle: [], open: 0, waiters: [] }));
    const idle = pool.idle.pop();
    if (idle !== undefined) return idle;
    // The namespace's own session counts towards the limit; the pool holds the rest.
    if (pool.open < this.#maxSessions - 1) {
      pool.open++;
      try {
        return await this.#beginSession(ns, options).then((ack) => ack.sessionId);
      } catch (error) {
        pool.open--;
        throw error;
      }
    }
    return new Promise<string>((resolve, reject) => pool.waiters.push({ resolve, reject }));
  }

  #releasePooled(ns: string, sessionId: string, healthy: boolean): void {
    const pool = this.#pools.get(ns);
    if (!pool) return;
    const waiter = pool.waiters.shift();
    if (healthy) {
      if (waiter) waiter.resolve(sessionId);
      else pool.idle.push(sessionId);
      return;
    }
    // Dropped: its place in the pool goes to a fresh session for whoever waits, if anyone does.
    if (!waiter) {
      pool.open--;
      return;
    }
    this.#beginSession(ns, {}).then(
      (ack) => waiter.resolve(ack.sessionId),
      (error: unknown) => {
        pool.open--;
        waiter.reject(error);
      },
    );
  }


  #sessionLock(ns: string): SessionLock {
    let lock = this.#sessionLocks.get(ns);
    if (!lock) {
      lock = new SessionLock();
      this.#sessionLocks.set(ns, lock);
    }
    return lock;
  }

  /**
   * Resolves (and caches) the session for `ns`.
   *
   * The in-flight map matters under concurrency: without it, two operations racing on a
   * namespace with no session yet each open one, and the loser's cached head is stale from the
   * moment it lands.
   */
  async #ensureNamespace(ns: string, options: CallOptions): Promise<NamespaceState> {
    const cached = this.#namespaces.get(ns);
    if (cached) return cached;

    const inFlight = this.#sessionsInFlight.get(ns);
    if (inFlight) return inFlight;

    const promise = (async () => {
      const ack = await this.#beginSession(ns, options);
      const state: NamespaceState = { sessionId: ack.sessionId, head: ack.headHex };
      this.#namespaces.set(ns, state);
      return state;
    })();

    this.#sessionsInFlight.set(ns, promise);
    try {
      return await promise;
    } finally {
      this.#sessionsInFlight.delete(ns);
    }
  }

  /** Opens a READ_COMMITTED session on ns. */
  async #beginSession(ns: string, options: CallOptions): Promise<{ sessionId: string; headHex: string }> {
    const reply = await this.#request(
      "sessionBegin",
      {
        namespace: ns,
        sessionId: null,
        readConsistency: READ_COMMITTED,
        baseVersionHex: null,
      },
      options,
    );
    const ack = this.#expect<SessionBeginAckDto>(reply, "sessionBeginAck");
    if (!ack.sessionId) {
      throw new KdbUnauthenticatedError(
        `kdb: session begin rejected for namespace ${ns}` + (ack.error ? `: ${ack.error}` : ""),
      );
    }
    return { sessionId: ack.sessionId, headHex: ack.headHex };
  }

  /**
   * Sends one request and waits for the reply with a matching correlation id.
   *
   * Every request has a deadline, and that is a correctness requirement rather than a nicety:
   * some server paths answer an unrecognized message with nothing at all rather than an error
   * frame (finish-up plan item 4.H, `peersync/host.go:224-226`, `wire_listen.go:142-144`), so
   * without a timeout a client talking to a server that lacks an operation hangs forever.
   * `unsupportedOp`, when given, turns that timeout into the error that actually explains it.
   */
  async #request(
    kind: MessageKind,
    payload: unknown,
    options: CallOptions,
    unsupportedOp?: string,
  ): Promise<WireMessage> {
    if (this.#closed) {
      throw this.#closeCause ?? new KdbClosedError();
    }
    this.#throwIfAborted(options.signal);

    const correlationId = this.#nextCorrelation();
    const timeoutMs = options.timeoutMs ?? this.#requestTimeoutMs;

    return await new Promise<WireMessage>((resolve, reject) => {
      const onAbort = () => {
        this.#settle(correlationId, () => reject(new KdbAbortedError()));
      };

      const timer = setTimeout(() => {
        this.#settle(correlationId, () =>
          reject(
            unsupportedOp
              ? new KdbUnsupportedError(
                  unsupportedOp,
                  kind.startsWith("attachment")
                    ? "the server did not answer - it agreed the attachments feature but does not " +
                        "serve the 0x44-0x49 attachment messages"
                    : "the server did not answer - the JVM kdb-server implements only " +
                        "sessionBegin/sqlExec/txCommit, not the 0x14-0x1C document, upsert and lease messages",
                )
              : new KdbError(`kdb: request "${kind}" timed out after ${timeoutMs}ms`, {
                  code: "DEADLINE_EXCEEDED",
                }),
          ),
        );
      }, timeoutMs);

      options.signal?.addEventListener("abort", onAbort, { once: true });

      this.#pending.set(correlationId, {
        resolve,
        reject,
        timer,
        cleanup: () => options.signal?.removeEventListener("abort", onAbort),
      });

      try {
        this.#transport.send(encodeMessage(kind, correlationId, payload, this.#maxFrameBytes, this.#features));
      } catch (error) {
        this.#settle(correlationId, () =>
          reject(
            error instanceof KdbError
              ? error
              : new KdbTransportError("kdb: send failed", { cause: error }),
          ),
        );
      }
    });
  }

  /**
   * Removes the pending entry for `correlationId` and clears its timer and abort listener,
   * returning it so the caller can resolve or reject. Returns undefined if it was already
   * settled - a late reply after a timeout lands here, and must be a no-op rather than a
   * second settlement of the same promise.
   */
  #take(correlationId: number): PendingRequest | undefined {
    const pending = this.#pending.get(correlationId);
    if (!pending) return undefined;
    this.#pending.delete(correlationId);
    clearTimeout(pending.timer);
    pending.cleanup();
    return pending;
  }

  #settle(correlationId: number, action: (pending: PendingRequest) => void): void {
    const pending = this.#take(correlationId);
    if (pending) action(pending);
  }

  #onFrame(frame: Uint8Array): void {
    let message: WireMessage;
    try {
      message = decodeMessage(frame, this.#maxFrameBytes);
    } catch {
      // A frame this client cannot parse is not necessarily fatal for the connection - it may
      // be a peer-sync broadcast from a newer server. Dropping it is safer than tearing down
      // a connection with live requests on it.
      return;
    }

    // Peer-sync, stream and DAG messages are a peer's vocabulary, not a client's. Dropped
    // rather than treated as a protocol violation.
    if (!isClientKind(message.kind)) return;

    const pending = this.#take(message.correlationId);
    if (!pending) return;
    pending.resolve(message);
  }

  #onClose(cause?: Error): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#closeCause = cause ?? new KdbClosedError();
    // Fail every in-flight request rather than leaving callers waiting on a dead socket until
    // their individual deadlines expire.
    for (const correlationId of [...this.#pending.keys()]) {
      const pending = this.#take(correlationId);
      pending?.reject(this.#closeCause);
    }
    // Sessions die with the connection: so do the pools and the leases they were checked against.
    for (const pool of this.#pools.values()) {
      for (const waiter of pool.waiters.splice(0)) waiter.reject(this.#closeCause);
    }
    this.#pools.clear();
    this.#leases.clear();
  }

  #nextCorrelation(): number {
    const next = this.#correlation;
    // Wrap before the i32 the header carries would overflow.
    this.#correlation = next >= 0x7fffffff ? 1 : next + 1;
    return next;
  }

  #expect<T>(message: WireMessage, kind: MessageKind): T {
    if (message.kind !== kind) {
      throw new KdbProtocolError(`kdb: expected ${kind}, got ${message.kind}`);
    }
    return message.payload as T;
  }

  #throwIfError(
    error: string | null | undefined,
    code: string | null | undefined,
    retryAfterMs: number | null | undefined,
  ): void {
    if (!error) return;
    throw classifiedError(
      error,
      (code ?? undefined) as ErrorCode | undefined,
      retryAfterMs ?? undefined,
    );
  }

  #throwIfAborted(signal: AbortSignal | undefined): void {
    if (signal?.aborted) throw new KdbAbortedError();
  }

  async #waitBackoff(attempt: number, error: KdbError, signal?: AbortSignal): Promise<void> {
    const delay = backoffDelay(attempt, error);
    if (delay <= 0) return;
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        signal?.removeEventListener("abort", onAbort);
        resolve();
      }, delay);
      const onAbort = () => {
        clearTimeout(timer);
        reject(new KdbAbortedError());
      };
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  }
}

export interface Lease {
  namespace: string;
  docId: string;
  /**
   * The lease's monotonic fence token. A client need not interpret it, but a fence that
   * CHANGED across a renew means the lease lapsed and was re-taken rather than extended - any
   * work done in between was based on a document this client no longer owned.
   *
   * The wire carries a uint64. Values beyond 2^53 lose precision as a JS number; fences are
   * small monotonic counters in practice, and comparing them for equality is all this client
   * does with them.
   */
  fence: number;
  /** Epoch milliseconds, or 0 when the server sent no expiry. */
  expiresAtMs: number;
}

/**
 * Turns a conflictReport into the right error.
 *
 * The PRECONDITION_FAILED scan comes first and that ordering is load-bearing: a failed
 * assertion and a lost race arrive on the same wire message but mean different things to the
 * caller. Reported first, exactly as go/kdb/client/client.go:596-608 does it - if any
 * operation's explicit assertion failed, that is why the transaction was refused, whatever
 * else the report also lists.
 */
export function decodeConflictError(dto: ConflictReportDto): KdbError {
  let body: ConflictReportBody;
  try {
    body = JSON.parse(utf8Decode(bytesFromWire(dto.reportBytes, "reportBytes")));
  } catch {
    return new KdbConflictError(
      { transactionId: "", baseHash: "", targetHash: "", conflicts: [] },
      dto.retryAfterMs != null ? { retryAfterMs: dto.retryAfterMs } : {},
    );
  }

  const retryAfterMs = dto.retryAfterMs ?? 0;
  const conflicts: ConflictDetail[] = (body.conflicts ?? []).map((c) => ({
    documentId: c.documentId,
    operationType: c.operationType,
    actualContentHash: c.actualContentHash,
  }));

  for (const conflict of conflicts) {
    if (conflict.operationType === "PRECONDITION_FAILED") {
      return new KdbPreconditionError(conflict.documentId, conflict.actualContentHash ?? "", {
        retryAfterMs,
      });
    }
  }

  return new KdbConflictError(
    {
      transactionId: body.transactionId ?? "",
      baseHash: body.baseHash ?? "",
      targetHash: body.targetHash ?? "",
      conflicts,
    },
    { retryAfterMs },
  );
}

/**
 * How long to wait before the next compare-and-swap attempt.
 *
 * Prefers the server's own hint - it can see the whole queue and has already jittered per
 * response, which no client can do for itself - and otherwise draws uniformly from
 * [0, min(base*2^attempt, cap)]: full jitter, per AWS's "Exponential Backoff and Jitter".
 * Full jitter rather than exponential-plus-noise because only the former fully decorrelates
 * clients that started together, which under a lockstep workload is all of them.
 *
 * Exported so the schedule can be tested without sleeping through it.
 */
export function backoffDelay(attempt: number, error: KdbError): number {
  const hint = error.retryAfterMs ?? 0;
  if (hint > 0) return hint;
  let capMs = BACKOFF_CAP_MS;
  if (attempt < 24) {
    const scaled = BACKOFF_BASE_MS * Math.pow(2, attempt);
    if (scaled < capMs) capMs = scaled;
  }
  return Math.floor(Math.random() * (capMs + 1));
}

function infoFromDto(d: AttachmentInfoDto): AttachmentInfo {
  return { id: d.id, name: d.name, hash: d.hash, size: d.size, mime: d.mime };
}

/** An attachment operation as the transaction JSON carries it (go/kdb/wire/op_mapper.go). */
function attachmentOpDto(op: AttachmentOp): OpDto {
  const docId = assertUuid(op.docId);
  const attachmentId = assertUuid(op.id, "attachment id");
  if (op.kind === "delete") return { kind: "attachmentDelete", docId, attachmentId };
  if (!/^[0-9a-f]{64}$/.test(op.hash)) {
    throw new TypeError(`kdb: attachment hash must be 64 lowercase hex digits, got ${JSON.stringify(op.hash)}`);
  }
  if (!Number.isSafeInteger(op.size) || op.size < 0) {
    throw new TypeError(`kdb: attachment size must be a non-negative integer, got ${op.size}`);
  }
  // Every field, mime included even when "": Go rejects an attachmentPut missing any.
  return {
    kind: "attachmentPut",
    docId,
    attachmentId,
    name: op.name,
    scheme: ATTACHMENT_HASH_SCHEME_CHUNKED_1,
    blobHashHex: op.hash,
    size: op.size,
    mime: op.mime ?? "",
  };
}

/**
 * AttachmentReader over ATTACHMENT_GET pages. The first page is already in hand; each later one
 * is asked for with the read's ID (which pins the version) from where the last ended.
 */
class PagedAttachmentReader implements AttachmentReader {
  readonly info: AttachmentInfo;
  readonly commit: string;
  #superseded: boolean;
  #first: AttachmentDataDto | undefined;
  #readId: string;
  #offset: number;
  #eof: boolean;
  #started = false;
  readonly #hasher: AttachmentHasher | undefined;
  readonly #fetch: (readId: string, at: number, close?: boolean) => Promise<AttachmentDataDto>;

  constructor(
    first: AttachmentDataDto,
    offset: number,
    verify: boolean,
    fetch: (readId: string, at: number, close?: boolean) => Promise<AttachmentDataDto>,
  ) {
    this.info = infoFromDto(first.info);
    this.commit = first.commitHex;
    this.#superseded = first.superseded;
    this.#first = first;
    this.#readId = first.readId;
    this.#offset = offset;
    this.#eof = first.eof;
    this.#hasher = verify ? new AttachmentHasher() : undefined;
    this.#fetch = fetch;
  }

  get superseded(): boolean {
    return this.#superseded;
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<Uint8Array, void, undefined> {
    if (this.#started) throw new KdbError("kdb: an attachment reader can be read only once");
    this.#started = true;
    let finished = false;
    try {
      for (;;) {
        let page = this.#first;
        this.#first = undefined;
        if (page === undefined) {
          page = await this.#fetch(this.#readId, this.#offset);
          if (page.readId) this.#readId = page.readId;
        }
        if (page.offset !== this.#offset) {
          throw new KdbProtocolError(`kdb: attachment page starts at ${page.offset}, expected ${this.#offset}`);
        }
        this.#offset += page.data.length;
        this.#eof = page.eof;
        if (page.eof) this.#superseded = page.superseded;
        if (this.#hasher) {
          await this.#hasher.update(page.data);
          // The last page is handed over only once the whole blob checks out.
          if (page.eof) await this.#verify();
        }
        if (page.eof) {
          finished = true;
          if (page.data.length > 0) yield page.data;
          return;
        }
        if (page.data.length === 0) {
          throw new KdbProtocolError("kdb: attachment page carried no data and did not end the read");
        }
        yield page.data;
      }
    } finally {
      // Broken off early (or failed): tell the server to drop the read, best effort.
      if (!finished && !this.#eof) await this.cancel();
    }
  }

  async #verify(): Promise<void> {
    const actual = await this.#hasher!.digest();
    if (this.#offset !== this.info.size || actual !== this.info.hash) {
      throw new KdbIntegrityError(
        `kdb: attachment ${this.info.id} read ${this.#offset} bytes hashing to ${actual}; ` +
          `expected ${this.info.size} bytes hashing to ${this.info.hash}`,
        this.info.hash,
        actual,
      );
    }
  }

  stream(): ReadableStream<Uint8Array> {
    return readerStream(this);
  }

  async cancel(): Promise<void> {
    if (this.#eof || !this.#readId) return;
    this.#eof = true;
    this.#first = undefined;
    await this.#fetch(this.#readId, this.#offset, true).catch(() => undefined);
  }
}

/** A reader's pages as a web ReadableStream; cancelling the stream cancels the read. */
function readerStream(reader: AttachmentReader): ReadableStream<Uint8Array> {
  const it = reader[Symbol.asyncIterator]();
  return new ReadableStream<Uint8Array>({
    pull: async (controller) => {
      try {
        const { done, value } = await it.next();
        if (done) controller.close();
        else controller.enqueue(value);
      } catch (error) {
        controller.error(error);
      }
    },
    cancel: async () => {
      await it.return?.(undefined);
      await reader.cancel();
    },
  });
}

/** Every byte a reader yields, in one array. */
async function collectBytes(reader: AsyncIterable<Uint8Array>): Promise<Uint8Array> {
  const parts: Uint8Array[] = [];
  let len = 0;
  for await (const part of reader) {
    parts.push(part);
    len += part.length;
  }
  if (parts.length === 1) return parts[0]!;
  const data = new Uint8Array(len);
  let off = 0;
  for (const p of parts) {
    data.set(p, off);
    off += p.length;
  }
  return data;
}

type RangeFetch = (readId: string, at: number, maxBytes: number, close?: boolean) => Promise<AttachmentDataDto>;

/**
 * A verified ranged read (openAttachmentRange). Pages are whole chunks from a chunk-aligned offset,
 * each proved against the blob's Merkle tree, whose hash (with the size) must be the content hash;
 * only [offset, end) of them is handed out, and only once the whole page has checked out.
 */
class RangedAttachmentReader implements AttachmentReader {
  readonly info: AttachmentInfo;
  readonly commit: string;
  #superseded = false;
  #readId: string;
  readonly #offset: number;
  /** Where the range ends: offset + length, cut at the attachment's end. */
  readonly #end: number;
  /** Where the next page starts: always a chunk boundary, or the attachment's end. */
  #pageOff: number;
  #tree: Uint8Array = new Uint8Array(0);
  #treeHex = "";
  /** The verified, trimmed bytes of the last page, not yet handed out. */
  #pending: Uint8Array | undefined;
  /** The server ended the read (EOF page), or it was closed. */
  #eof = false;
  #started = false;
  readonly #fetch: RangeFetch;

  constructor(first: AttachmentDataDto, offset: number, length: number, fetch: RangeFetch) {
    this.info = infoFromDto(first.info);
    this.commit = first.commitHex;
    this.#readId = first.readId;
    this.#offset = offset;
    this.#end = length >= 0 ? Math.min(first.info.size, offset + length) : first.info.size;
    this.#pageOff = offset - (offset % ATTACHMENT_CHUNK_BYTES);
    this.#fetch = fetch;
  }

  get superseded(): boolean {
    return this.#superseded;
  }

  /** Checks the first page: the hash scheme, the tree against the content hash, then its chunks. */
  async takeFirst(page: AttachmentDataDto): Promise<void> {
    this.#eof = page.eof;
    if (page.scheme !== ATTACHMENT_HASH_SCHEME_CHUNKED_1) {
      throw new KdbProtocolError(`kdb: attachment content hash scheme ${page.scheme} is not supported`);
    }
    if (page.tree === "") throw new KdbProtocolError("kdb: attachment page carries no Merkle tree hash");
    this.#treeHex = page.tree;
    this.#tree = fromHex(page.tree);
    const identity = await attachmentIdentity(this.#tree, this.info.size);
    if (identity !== this.info.hash) {
      throw new KdbIntegrityError(
        `kdb: attachment ${this.info.id}'s Merkle tree and size hash to ${identity}, not its content hash ${this.info.hash}`,
        this.info.hash,
        identity,
      );
    }
    await this.#take(page);
  }

  async #take(page: AttachmentDataDto): Promise<void> {
    if (page.offset !== this.#pageOff || page.tree !== this.#treeHex) {
      throw new KdbProtocolError(`kdb: attachment page at ${page.offset}, expected ${this.#pageOff}`);
    }
    await this.#verifyChunks(page);
    this.#pageOff += page.data.length;
    if (page.data.length === 0 && !page.eof) {
      throw new KdbProtocolError(`kdb: empty attachment page at ${page.offset} before the end`);
    }
    if (page.eof && this.#pageOff !== this.info.size) {
      throw new KdbProtocolError(`kdb: attachment read ended at ${this.#pageOff} of ${this.info.size} bytes`);
    }
    const from = Math.max(0, this.#offset - page.offset);
    const to = Math.min(this.#end, this.#pageOff) - page.offset;
    this.#pending = from < to ? page.data.subarray(from, to) : undefined;
    this.#eof = page.eof;
    if (page.eof) this.#superseded = page.superseded;
  }

  /** The page's data is whole chunks from its (aligned) offset, each proved, and nothing more. */
  async #verifyChunks(page: AttachmentDataDto): Promise<void> {
    if (page.offset % ATTACHMENT_CHUNK_BYTES !== 0) {
      throw new KdbProtocolError(`kdb: attachment page at ${page.offset} is not chunk-aligned`);
    }
    const size = this.info.size;
    const leaves = attachmentChunkCount(size);
    const first = page.offset / ATTACHMENT_CHUNK_BYTES;
    let data = page.data;
    for (const [i, proof] of page.chunks.entries()) {
      const index = first + i;
      if (proof.leaf !== index) {
        throw new KdbProtocolError(`kdb: attachment proof for chunk ${proof.leaf} where ${index} was expected`);
      }
      const n = Math.min(ATTACHMENT_CHUNK_BYTES, size - index * ATTACHMENT_CHUNK_BYTES);
      if (n < 0 || data.length < n) throw this.#corrupt(`chunk ${index} is short`);
      const leaf = await chunkLeafHash(data.subarray(0, n));
      const path = proof.path.map((h) => fromHex(h));
      if (!(await verifyMerkleInclusion(leaf, index, leaves, path, this.#tree))) {
        throw this.#corrupt(`chunk ${index} (leaf ${toHex(leaf)}) is not in its Merkle tree`);
      }
      data = data.subarray(n);
    }
    if (data.length !== 0) throw this.#corrupt(`${data.length} bytes at the page's end carry no proof`);
  }

  #corrupt(what: string): KdbIntegrityError {
    return new KdbIntegrityError(
      `kdb: attachment ${this.info.id} does not match its content hash ${this.info.hash}: ${what}`,
      this.info.hash,
      "",
    );
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<Uint8Array, void, undefined> {
    if (this.#started) throw new KdbError("kdb: an attachment reader can be read only once");
    this.#started = true;
    let finished = false;
    try {
      for (;;) {
        const part = this.#pending;
        this.#pending = undefined;
        if (part !== undefined) yield part;
        if (this.#eof) {
          finished = true;
          return;
        }
        if (this.#pageOff >= this.#end) {
          // The range is read, short of the attachment's end: release the pinned read, learning
          // from the close whether a newer version committed meanwhile.
          finished = true;
          await this.#close(true);
          return;
        }
        const page = await this.#fetch(
          this.#readId,
          this.#pageOff,
          Math.min(this.#end - this.#pageOff, MAX_ATTACHMENT_CHUNK_BYTES),
        );
        if (page.readId) this.#readId = page.readId;
        await this.#take(page);
      }
    } finally {
      if (!finished) await this.cancel();
    }
  }

  stream(): ReadableStream<Uint8Array> {
    return readerStream(this);
  }

  async cancel(): Promise<void> {
    await this.#close(false);
  }

  async #close(report: boolean): Promise<void> {
    if (this.#eof || !this.#readId) return;
    this.#eof = true;
    this.#pending = undefined;
    const reply = await this.#fetch(this.#readId, this.#pageOff, 0, true).catch((error: unknown) => {
      if (report) throw error;
      return undefined;
    });
    if (reply !== undefined && !reply.error) this.#superseded = reply.superseded;
  }
}

/**
 * Converts text cells to the values their kinds name. `kinds` is one decimal digit per cell, row
 * by row (SqlResult's cellKinds); undefined leaves every cell a string.
 */
function typedRows(rows: string[][], kinds: string | undefined): SqlValue[][] {
  if (kinds === undefined) return rows;
  let k = 0;
  return rows.map((row) =>
    row.map((cell) => {
      const kind = kinds.charCodeAt(k++) - 48;
      switch (kind) {
        case 1: // NULL
          return null;
        case 2: {
          // int64: exact as a number while it fits one, a bigint past that.
          const n = Number(cell);
          return Number.isSafeInteger(n) ? n : BigInt(cell);
        }
        case 3:
          return Number(cell);
        case 4:
          return cell === "1" || cell === "true"; // Go writes 1/0, the JVM server true/false
        case 5:
          return JSON.parse(cell) as JsonValue;
        case 6:
          return base64ToBytes(cell);
        default:
          return cell;
      }
    }),
  );
}
