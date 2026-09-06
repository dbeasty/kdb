/**
 * The operation seam (Component 63 §11.3).
 *
 * `KdbOperations` is deliberately separate from `Client`: every method here makes sense against
 * a local engine as well as a remote one, so an embedded runtime (@kdb/embed, Phase 3) can
 * implement the same contract and application code can move between local-first and
 * server-backed without a rewrite. This costs nothing now and is awkward to retrofit later.
 *
 * What does NOT generalize stays on Client: connect, close, the leases (which are a
 * multi-client coordination primitive and meaningless in-process), and connection state.
 */

import type { PreconditionKind } from "./wire/messages.ts";

export type CommitHash = string;

/** A SQL row. Values arrive string-typed - see the note on `query`. */
export type Row = Record<string, string>;

export interface DocWrite {
  docId: string;
  body: unknown;
}

export type Precondition =
  | { opIndex: number; kind: Exclude<PreconditionKind, "EXPECT_CONTENT_HASH"> }
  | { opIndex: number; kind: "EXPECT_CONTENT_HASH"; contentHashHex: string };

export interface Transaction {
  namespace: string;
  baseVersion: CommitHash;
  writes: DocWrite[];
  preconditions?: Precondition[];
}

export interface CallOptions {
  signal?: AbortSignal;
  timeoutMs?: number;
}

export interface CompareAndSwapOptions extends CallOptions {
  /** Default 5, matching the Go client. */
  attempts?: number;
}

export interface KdbOperations {
  /** Writes a document as a new commit anchored on the namespace's current head. */
  putJSON(ns: string, docId: string, body: unknown, options?: CallOptions): Promise<CommitHash>;

  /** Reads a document and the commit it was read at. Throws KdbNotFoundError if absent. */
  get(
    ns: string,
    docId: string,
    options?: CallOptions,
  ): Promise<{ body: unknown; commit: CommitHash }>;

  /**
   * Reads a document plus the content hash of what was read - the token replaceIf compares
   * against. Computed locally from the body, never carried on the wire (see wire/hash.ts).
   */
  getWithHash(
    ns: string,
    docId: string,
    options?: CallOptions,
  ): Promise<{ body: unknown; contentHash: string }>;

  /**
   * Writes unconditionally: create if absent, replace if present. No baseVersion, cannot
   * conflict. The analogue of Mongo's ReplaceOne(filter, doc, upsert=true), for namespaces
   * whose conflict policy is LAST_WRITE.
   */
  upsert(ns: string, docId: string, body: unknown, options?: CallOptions): Promise<CommitHash>;

  /**
   * Appends one entry to an APPEND_ONLY namespace. Same transport as upsert, different intent:
   * every write is a new independent record rather than a wholesale replacement.
   */
  appendEvent(ns: string, docId: string, body: unknown, options?: CallOptions): Promise<void>;

  putIfAbsent(ns: string, docId: string, body: unknown, options?: CallOptions): Promise<CommitHash>;

  replaceIf(
    ns: string,
    docId: string,
    body: unknown,
    expectedContentHash: string,
    options?: CallOptions,
  ): Promise<CommitHash>;

  replaceIfPresent(
    ns: string,
    docId: string,
    body: unknown,
    options?: CallOptions,
  ): Promise<CommitHash>;

  /**
   * Reads, transforms and writes back under a compare-and-set, retrying when another writer
   * wins the race.
   *
   * `mutate` receives null when the document does not exist, so a caller can seed it.
   * Returning null means "leave it alone" and throws KdbAbortedError without writing. Every
   * attempt re-reads: recomputing from a stale value is exactly the lost update this exists to
   * prevent, so the read cannot be hoisted out of the loop.
   */
  compareAndSwap(
    ns: string,
    docId: string,
    mutate: (current: unknown | null) => unknown | null | Promise<unknown | null>,
    options?: CompareAndSwapOptions,
  ): Promise<CommitHash>;

  /** Submits a multi-document transaction. All-or-nothing. */
  commit(tx: Transaction, options?: CallOptions): Promise<CommitHash>;

  /**
   * Runs one SELECT and maps each row to an object keyed by column name.
   *
   * Values are strings: `sqlResult.rows` is `string[][]` on the wire, and this layer does not
   * guess at types. Coercion belongs in @kdb/codegen (Phase 2), where the schema is known -
   * a driver that silently parsed "0123" as 123 would corrupt data.
   */
  query<T = Row>(ns: string, sql: string, args?: unknown[], options?: CallOptions): Promise<T[]>;

  queryRaw(
    ns: string,
    sql: string,
    args?: unknown[],
    options?: CallOptions,
  ): Promise<{ columns: string[]; rows: string[][] }>;

  /** Runs one non-SELECT statement, auto-committing it if the server reports it as a write. */
  exec(ns: string, sql: string, args?: unknown[], options?: CallOptions): Promise<void>;
}
