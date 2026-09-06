/**
 * @kdb/client - the KDB network driver.
 *
 * This entry point touches no Node built-ins, so it loads unchanged in browsers, Bun, Deno and
 * Workers-style runtimes. The TCP transport lives behind the `@kdb/client/tcp` subpath and is
 * reached lazily by `connect` for a tcp:// or tcps:// URI.
 */

export { connect, Client, backoffDelay, decodeConflictError, DEFAULT_REQUEST_TIMEOUT_MS } from "./client.ts";
export type { ConnectOptions, Lease, TlsOptions } from "./client.ts";

export type {
  CallOptions,
  CommitHash,
  CompareAndSwapOptions,
  DocWrite,
  KdbOperations,
  Precondition,
  Row,
  Transaction,
} from "./operations.ts";

export {
  KdbError,
  KdbAbortedError,
  KdbBusyError,
  KdbClosedError,
  KdbConflictError,
  KdbDeadlineExceededError,
  KdbLockError,
  KdbNotFoundError,
  KdbPreconditionError,
  KdbProtocolError,
  KdbTransportError,
  KdbUnauthenticatedError,
  KdbUnavailableError,
  KdbUnsupportedError,
} from "./errors.ts";
export type { ConflictDetail, ErrorCode } from "./errors.ts";

// The wire layer is exported for tooling (kdb-inspect-style frame dumps, conformance tests)
// and for @kdb/embed, which shares the transaction and content-hash encodings.
export { contentHash, encodeDocumentBody } from "./wire/hash.ts";
export { decodeFrame, decodeHeader, encodeFrame, FrameReader } from "./wire/frame.ts";
export { decodeMessage, encodeMessage } from "./wire/envelope.ts";
export { MSG } from "./wire/messages.ts";
export type { MessageKind, TransactionDto } from "./wire/messages.ts";
export { isUuid, randomUuid } from "./wire/uuid.ts";
export type { Transport } from "./transport/types.ts";
