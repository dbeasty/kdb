/**
 * The KDB client: connection, sessions, correlation, and the operation set.
 *
 * Mirrors go/kdb/client method for method, translated to idiomatic TypeScript - promises rather
 * than (value, error) tuples, AbortSignal rather than context.Context, thrown typed errors
 * rather than sentinel returns.
 */

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
import type {
  CallOptions,
  CommitHash,
  CompareAndSwapOptions,
  KdbOperations,
  Row,
  Transaction,
} from "./operations.ts";
import { connectWebSocket } from "./transport/websocket.ts";
import { schemeOf, type Transport } from "./transport/types.ts";
import { bytesFromWire, bytesToWire, utf8Decode, utf8Encode } from "./wire/bytes.ts";
import { decodeMessage, encodeMessage, isClientKind, type WireMessage } from "./wire/envelope.ts";
import { DEFAULT_MAX_FRAME_BYTES, KDB_WIRE_PROTOCOL_VERSION } from "./wire/frame.ts";
import { contentHash } from "./wire/hash.ts";
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

  const client = new Client(transport, options);
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
  readonly #requestTimeoutMs: number;
  readonly #authorNodeId: string;
  readonly #pending = new Map<number, PendingRequest>();
  readonly #namespaces = new Map<string, NamespaceState>();
  readonly #sessionsInFlight = new Map<string, Promise<NamespaceState>>();

  #correlation = 1;
  #closed = false;
  #closeCause: Error | undefined;

  constructor(transport: Transport, options: ConnectOptions = {}) {
    this.#transport = transport;
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
    if (ack.negotiatedEncoding !== "JSON") {
      throw new KdbProtocolError(
        `kdb: server negotiated ${ack.negotiatedEncoding}, but this client implements JSON only`,
      );
    }
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
    );
  }

  async replaceIfPresent(
    ns: string,
    docId: string,
    body: unknown,
    options: CallOptions = {},
  ): Promise<CommitHash> {
    return this.#writeTransaction(ns, docId, body, { opIndex: 0, kind: "EXPECT_PRESENT" }, options);
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
    const operations: OpDto[] = tx.writes.map((write) => ({
      kind: "write",
      docId: assertUuid(write.docId),
      patch: JSON.stringify(write.body),
    }));
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

  async exec(
    ns: string,
    sql: string,
    args: unknown[] = [],
    options: CallOptions = {},
  ): Promise<void> {
    const result = await this.#execSql(ns, sql, args, options);
    if (result.readOnly) return;

    // A write statement is not durable until it is committed, and this call is one
    // client-visible unit of work rather than two, so the auto-commit happens here. The
    // txCommit carries no transactionBytes: it commits whatever the session accumulated.
    // Matching go/kdb/client/query.go:44-72. Omitting this is the bug that passes every unit
    // test and loses writes in production.
    const state = await this.#ensureNamespace(ns, options);
    const reply = await this.#request(
      "txCommit",
      { namespace: ns, sessionId: state.sessionId, transactionBytes: [] },
      options,
    );
    this.#handleCommitReply(ns, state, reply);
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
    const result = this.#expect<LockResultDto>(reply, "lockResult");
    if (!result.granted && result.error) {
      throw new KdbLockError(docId, result.holderSessionId ?? undefined, `kdb: ${result.error}`);
    }
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
  ): Promise<CommitHash> {
    assertUuid(docId);
    const state = await this.#ensureNamespace(ns, options);
    const operations: OpDto[] = [{ kind: "write", docId, patch: JSON.stringify(body) }];
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
    const reply = await this.#request(
      "txCommit",
      {
        namespace: ns,
        sessionId: state.sessionId,
        transactionBytes: bytesToWire(utf8Encode(JSON.stringify(tx))),
      },
      options,
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
  ): Promise<{ columns: string[]; rows: string[][]; readOnly: boolean }> {
    const state = await this.#ensureNamespace(ns, options);
    const reply = await this.#request(
      "sqlExec",
      {
        namespace: ns,
        sessionId: state.sessionId,
        sql,
        parametersJson: args.length > 0 ? JSON.stringify(args) : null,
      },
      options,
    );
    const result = this.#expect<SqlResultDto>(reply, "sqlResult");
    this.#throwIfError(result.error, result.errorCode, result.retryAfterMs);
    return {
      columns: result.columns ?? [],
      rows: result.rows ?? [],
      readOnly: result.readOnly,
    };
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
          `kdb: session begin rejected for namespace ${ns}` +
            (ack.error ? `: ${ack.error}` : ""),
        );
      }
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
                  "the server did not answer - the JVM kdb-server implements only " +
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
        this.#transport.send(encodeMessage(kind, correlationId, payload, this.#maxFrameBytes));
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
