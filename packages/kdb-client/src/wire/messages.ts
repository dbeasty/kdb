/**
 * Message type codes and payload shapes, transcribed from go/kdb/wire/types.go and
 * payload_dto.go.
 *
 * The JSON key names below ARE the wire contract - they are what the JVM's strict decoder
 * matches on, and a misspelling here is a connection teardown, not a missing field. They are
 * copied from payload_dto.go's struct tags rather than inferred from the Go field names.
 */

/** Message type codes (go/kdb/wire/types.go:30-73). */
export const MSG = {
  HANDSHAKE: 0x01,
  DELTA_COMMIT: 0x02,
  COMMIT_FETCH: 0x03,
  COMMIT_PUSH: 0x04,
  DAG_DIFF: 0x05,
  TRANSACTION_REPLAY: 0x06,
  CONFLICT_REPORT: 0x07,
  COMPACTION_NOTICE: 0x08,
  ICE_ARCHIVE_NOTICE: 0x09,
  SNAPSHOT_REQUEST: 0x0a,
  SNAPSHOT_RESPONSE: 0x0b,
  POSITION_ACK: 0x0c,
  SCHEMA_PUSH: 0x0d,
  SESSION_BEGIN: 0x0e,
  SQL_EXEC: 0x0f,
  SQL_RESULT: 0x10,
  TX_COMMIT: 0x11,
  TX_ROLLBACK: 0x12,
  SESSION_BEGIN_ACK: 0x13,
  DOCUMENT_GET: 0x14,
  DOCUMENT_GET_RESULT: 0x15,
  UPSERT: 0x16,
  UPSERT_RESULT: 0x17,
  COMMIT_PUSH_ACK: 0x18,
  LOCK_ACQUIRE: 0x19,
  LOCK_RENEW: 0x1a,
  LOCK_RELEASE: 0x1b,
  LOCK_RESULT: 0x1c,
} as const;

/**
 * The envelope `kind` discriminant. Note handshakeAck has no code of its own - see
 * TYPE_CODE_BY_KIND.
 */
export type MessageKind =
  | "handshake"
  | "handshakeAck"
  | "sessionBegin"
  | "sessionBeginAck"
  | "sqlExec"
  | "sqlResult"
  | "txCommit"
  | "txRollback"
  | "conflictReport"
  | "documentGet"
  | "documentGetResult"
  | "upsert"
  | "upsertResult"
  | "lockAcquire"
  | "lockRenew"
  | "lockRelease"
  | "lockResult";

/**
 * Which type code to stamp on an outbound frame of each kind.
 *
 * `handshakeAck` maps to MSG.HANDSHAKE deliberately: the server sends an ack under the SAME
 * type code as the request it answers (go/kdb/server/wire_listen.go:240 and
 * stream/coordinator.go:138 both construct it that way), and the two are told apart only by
 * the envelope's `kind`. Every other request/reply pair has distinct codes, which is exactly
 * what makes this one easy to miss - a decoder switching on typeCode alone reads an ack as a
 * request and fails on the first field. The type code is a routing hint; `kind` is the
 * discriminant.
 */
export const TYPE_CODE_BY_KIND: Record<MessageKind, number> = {
  handshake: MSG.HANDSHAKE,
  handshakeAck: MSG.HANDSHAKE,
  sessionBegin: MSG.SESSION_BEGIN,
  sessionBeginAck: MSG.SESSION_BEGIN_ACK,
  sqlExec: MSG.SQL_EXEC,
  sqlResult: MSG.SQL_RESULT,
  txCommit: MSG.TX_COMMIT,
  txRollback: MSG.TX_ROLLBACK,
  conflictReport: MSG.CONFLICT_REPORT,
  documentGet: MSG.DOCUMENT_GET,
  documentGetResult: MSG.DOCUMENT_GET_RESULT,
  upsert: MSG.UPSERT,
  upsertResult: MSG.UPSERT_RESULT,
  lockAcquire: MSG.LOCK_ACQUIRE,
  lockRenew: MSG.LOCK_RENEW,
  lockRelease: MSG.LOCK_RELEASE,
  lockResult: MSG.LOCK_RESULT,
};

/** Client mode strings (`ClientMode.String()`, go/kdb/wire/types.go:241-247). */
export const CLIENT_MODE_SQL = "SQL_CLIENT";

export const READ_COMMITTED = "READ_COMMITTED";

// --- payload shapes -------------------------------------------------------------------------

export interface HandshakeDto {
  nodeId: string;
  namespaces: string[];
  localHeads: Record<string, string>;
  supportsZstd: boolean;
  supportsIndexHints: boolean;
  supportsDirectDeltaIngest: boolean;
  maxFrameBytes: number;
  preferredEncodings: string[];
  clientMode: string;
  protocolVersion: number;
  user?: string;
  password?: string;
  token?: string;
}

export interface HandshakeAckDto {
  accepted: boolean;
  negotiatedEncoding: string;
  protocolVersion: number;
  remoteHeads: Record<string, string>;
  rejectionReason?: string | null;
}

export interface SessionBeginDto {
  namespace: string;
  sessionId: string | null;
  readConsistency: string;
  baseVersionHex: string | null;
}

export interface SessionBeginAckDto {
  namespace: string;
  sessionId: string;
  headHex: string;
  readConsistency: string;
  error?: string | null;
}

export interface SqlExecDto {
  namespace: string;
  sessionId: string;
  sql: string;
  parametersJson: string | null;
}

export interface SqlResultDto {
  namespace: string;
  sessionId: string;
  columns: string[];
  rows: string[][];
  rowsAffected: number;
  resolvedCommitHex: string;
  readOnly: boolean;
  error?: string | null;
  generatedIds: string[];
  errorCode?: string | null;
  retryAfterMs?: number | null;
}

export interface TxCommitDto {
  namespace: string;
  sessionId: string;
  /** Encoded per bytes.ts - an array of numbers, not base64 and not a Uint8Array. */
  transactionBytes: number[];
}

export interface TxRollbackDto {
  namespace: string;
  sessionId: string;
}

export interface ConflictReportDto {
  namespace: string;
  reportBytes: number[];
  errorCode?: string | null;
  retryAfterMs?: number | null;
}

export interface DocumentGetDto {
  namespace: string;
  docId: string;
}

export interface DocumentGetResultDto {
  namespace: string;
  docId: string;
  /** Absent or null when no document exists at commitHex. */
  json?: string | null;
  commitHex: string;
  error?: string | null;
  errorCode?: string | null;
  retryAfterMs?: number | null;
}

export interface UpsertDto {
  namespace: string;
  docId: string;
  json: string;
  /**
   * Load-bearing despite upsert needing no session of its own: it is how the server tells a
   * lease holder's own upsert from a stranger's, and an empty value is treated as "not the
   * holder" (go/kdb/wire/document_ops.go:41-47). Always send it.
   */
  sessionId: string;
}

export interface UpsertResultDto {
  namespace: string;
  commitHex: string;
  error?: string | null;
  errorCode?: string | null;
  retryAfterMs?: number | null;
}

export interface LockAcquireDto {
  namespace: string;
  sessionId: string;
  docId: string;
  ttlMillis: number;
}

export type LockRenewDto = LockAcquireDto;

export interface LockReleaseDto {
  namespace: string;
  sessionId: string;
  docId: string;
}

export interface LockResultDto {
  namespace: string;
  sessionId: string;
  docId: string;
  granted: boolean;
  /**
   * A uint64 on the wire, so it arrives as a JSON number and loses precision past 2^53. Fences
   * are small monotonic counters in practice and the client only ever compares them for
   * equality across a renew, so a number is the honest representation here rather than a
   * string that would imply arbitrary precision the wire does not actually preserve.
   */
  fence: number;
  expiresAtMillis: number;
  holderSessionId?: string | null;
  error?: string | null;
  errorCode?: string | null;
}

/** The shape inside transactionBytes (go/kdb/wire/transaction_codec.go). */
export interface TransactionDto {
  id: string;
  baseVersionHex: string;
  timestampMicros: number;
  authorNodeId: string;
  operations: OpDto[];
  /**
   * Omitted entirely when empty - never `[]`. The Go encoder tags it omitempty so a
   * transaction with no preconditions encodes exactly as it did before the field existed, and
   * an older peer decoding a newer producer's bytes sees what it always saw
   * (transaction_codec.go:19-25). Emitting an empty array breaks that guarantee.
   */
  preconditions?: PreconditionDto[];
}

export interface OpDto {
  kind: "write" | "delete" | "fileWrite" | "schemaMigration";
  docId?: string;
  /** For a write: the whole document JSON, as a string containing JSON. */
  patch?: string;
  path?: string;
  blobHashHex?: string;
  migrationId?: string;
  migrationPayload?: string;
}

/** Kinds travel as enum constant names, matching kotlinx.serialization's convention. */
export type PreconditionKind =
  | "EXPECT_ANY"
  | "EXPECT_ABSENT"
  | "EXPECT_PRESENT"
  | "EXPECT_CONTENT_HASH";

export interface PreconditionDto {
  opIndex: number;
  kind: PreconditionKind;
  contentHashHex?: string;
}

/** The JSON inside a conflictReport's reportBytes. */
export interface ConflictReportBody {
  transactionId: string;
  baseHash: string;
  targetHash: string;
  conflicts: Array<{
    documentId: string;
    operationType: string;
    actualContentHash: string;
  }>;
}
