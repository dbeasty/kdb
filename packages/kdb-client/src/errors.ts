/**
 * The error taxonomy from Component 63 §8, mapped one-to-one onto `go/kdb/client`'s.
 *
 * Every error carries the optional `code`/`retryAfterMs` pair that `sqlResult`,
 * `upsertResult`, `documentGetResult` and `conflictReport` all carry additively on the wire,
 * so a caller can tell "wait 50ms" from "never retry this" without parsing a message string.
 */

/** Server-side error classification (`go/kdb/wire/error_code.go`). */
export type ErrorCode =
  | "BUSY"
  | "UNAVAILABLE"
  | "DEADLINE_EXCEEDED"
  | "RESOURCE_EXHAUSTED"
  | "CONFLICT"
  | "SCHEMA_VIOLATION"
  | "UNIQUE_VIOLATION"
  | "UNAUTHORIZED"
  | "INTERNAL";

export interface KdbErrorOptions {
  code?: ErrorCode;
  retryAfterMs?: number;
  cause?: unknown;
}

export class KdbError extends Error {
  readonly code?: ErrorCode;
  readonly retryAfterMs?: number;

  constructor(message: string, options: KdbErrorOptions = {}) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = new.target.name;
    if (options.code !== undefined) this.code = options.code;
    if (options.retryAfterMs !== undefined) this.retryAfterMs = options.retryAfterMs;
  }
}

/** One entry of a server conflict report. */
export interface ConflictDetail {
  documentId: string;
  operationType: string;
  actualContentHash?: string;
}

/**
 * A transaction was refused because something else committed against the same baseVersion
 * first. No partial write happened - the transaction is all-or-nothing.
 */
export class KdbConflictError extends KdbError {
  readonly transactionId: string;
  readonly baseHash: string;
  readonly targetHash: string;
  readonly conflicts: ConflictDetail[];

  constructor(
    fields: {
      transactionId: string;
      baseHash: string;
      targetHash: string;
      conflicts: ConflictDetail[];
    },
    options: KdbErrorOptions = {},
  ) {
    super(
      `kdb: version conflict (${fields.conflicts.length} document(s) conflicted)`,
      { code: "CONFLICT", ...options },
    );
    this.transactionId = fields.transactionId;
    this.baseHash = fields.baseHash;
    this.targetHash = fields.targetHash;
    this.conflicts = fields.conflicts;
  }
}

/**
 * An explicit precondition failed - the document was absent when EXPECT_PRESENT was asserted,
 * present when EXPECT_ABSENT was, or carried a different content hash than EXPECT_CONTENT_HASH
 * named.
 *
 * Distinct from KdbConflictError even though both arrive on a `conflictReport` frame: a lost
 * race is worth retrying, a failed assertion generally is not. Conflating them makes
 * compareAndSwap spin on a write that can never succeed.
 */
export class KdbPreconditionError extends KdbError {
  readonly documentId: string;
  readonly actualHash: string;

  constructor(documentId: string, actualHash: string, options: KdbErrorOptions = {}) {
    super(`kdb: precondition failed for document ${documentId}`, options);
    this.documentId = documentId;
    this.actualHash = actualHash;
  }
}

export class KdbNotFoundError extends KdbError {
  readonly namespace: string;
  readonly docId: string;

  constructor(namespace: string, docId: string, options: KdbErrorOptions = {}) {
    super(`kdb: not found: ${namespace}/${docId}`, options);
    this.namespace = namespace;
    this.docId = docId;
  }
}

export class KdbUnauthenticatedError extends KdbError {}

export class KdbBusyError extends KdbError {
  constructor(message: string, options: KdbErrorOptions = {}) {
    super(message, { code: "BUSY", ...options });
  }
}

export class KdbUnavailableError extends KdbError {
  constructor(message: string, options: KdbErrorOptions = {}) {
    super(message, { code: "UNAVAILABLE", ...options });
  }
}

export class KdbDeadlineExceededError extends KdbError {
  constructor(message: string, options: KdbErrorOptions = {}) {
    super(message, { code: "DEADLINE_EXCEEDED", ...options });
  }
}

export class KdbLockError extends KdbError {
  readonly docId: string;
  readonly holderSessionId?: string;

  constructor(docId: string, holderSessionId: string | undefined, message: string, options: KdbErrorOptions = {}) {
    super(message, options);
    this.docId = docId;
    if (holderSessionId !== undefined && holderSessionId !== "") {
      this.holderSessionId = holderSessionId;
    }
  }
}

/** A transport-level failure: the socket or WebSocket, not the protocol. */
export class KdbTransportError extends KdbError {}

/** A frame that could not be decoded, or a reply of an unexpected kind. */
export class KdbProtocolError extends KdbError {}

export class KdbClosedError extends KdbError {
  constructor(message = "kdb: connection closed", options: KdbErrorOptions = {}) {
    super(message, options);
  }
}

export class KdbAbortedError extends KdbError {
  constructor(message = "kdb: operation aborted by caller", options: KdbErrorOptions = {}) {
    super(message, options);
  }
}

/**
 * The connected server does not implement this operation.
 *
 * No Go counterpart: the Go client has only ever talked to servers implementing the full
 * message set. A TS client will meet the JVM `kdb-server`, which implements sessionBegin /
 * sqlExec / txCommit and none of the 0x14-0x1C document, upsert and lease messages
 * (`go/kdb/wire/lock_ops.go`: "Go-only for now ... no Kotlin counterpart exists yet").
 *
 * Some server paths currently answer an unrecognized message with nothing at all rather than
 * an error frame (finish-up plan item 4.H), so this is also what a request deadline turns into
 * when the operation is one a server is known not to implement.
 */
export class KdbUnsupportedError extends KdbError {
  readonly operation: string;

  constructor(operation: string, hint?: string) {
    super(
      `kdb: server does not implement ${operation}` + (hint ? ` - ${hint}` : ""),
    );
    this.operation = operation;
  }
}

/**
 * Turns a server-sent (message, errorCode, retryAfterMs) triple into the right error class.
 * Mirrors `classifiedError` in go/kdb/client/client.go.
 */
export function classifiedError(
  message: string,
  code?: ErrorCode,
  retryAfterMs?: number,
): KdbError {
  const options: KdbErrorOptions = {};
  if (code !== undefined) options.code = code;
  if (retryAfterMs !== undefined) options.retryAfterMs = retryAfterMs;

  switch (code) {
    case "BUSY":
    case "RESOURCE_EXHAUSTED":
      return new KdbBusyError(`kdb: server busy: ${message}`, options);
    case "UNAVAILABLE":
      return new KdbUnavailableError(`kdb: server unavailable: ${message}`, options);
    case "DEADLINE_EXCEEDED":
      return new KdbDeadlineExceededError(`kdb: deadline exceeded: ${message}`, options);
    case "UNAUTHORIZED":
      return new KdbUnauthenticatedError(`kdb: unauthorized: ${message}`, options);
    default:
      return new KdbError(`kdb: ${message}`, options);
  }
}
