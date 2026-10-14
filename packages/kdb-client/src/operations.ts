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
  /**
   * Make `body` the whole document: keys it omits are removed. The default is a merge patch,
   * which can add and overwrite keys but never drop one.
   */
  replace?: boolean;
}

export type Precondition =
  | { opIndex: number; kind: Exclude<PreconditionKind, "EXPECT_CONTENT_HASH"> }
  | { opIndex: number; kind: "EXPECT_CONTENT_HASH"; contentHashHex: string };

export interface Transaction {
  namespace: string;
  baseVersion: CommitHash;
  writes: DocWrite[];
  /**
   * Documents to delete in the same commit. Their operations follow the writes, so a
   * precondition's `opIndex` for a write is unchanged by adding deletes. Deleting an absent
   * document is not an error; the commit just records nothing for it.
   */
  deletes?: string[];
  /**
   * Attachment operations in the same commit, after the writes and deletes. Build a put with
   * Client.prepareAttachment (which uploads the bytes and resolves the attachment's ID), or by
   * hand from an uploaded blob.
   */
  attachments?: AttachmentOp[];
  preconditions?: Precondition[];
}

/**
 * Creates or replaces one attachment of a document - by `id`, under `name` - with an uploaded blob
 * (`hash`, `size`). The same ID with a new name renames it.
 */
export interface AttachmentPutOp {
  kind: "put";
  docId: string;
  id: string;
  name: string;
  hash: string;
  size: number;
  mime?: string;
}

/** Removes one attachment of a document, by ID. */
export interface AttachmentDeleteOp {
  kind: "delete";
  docId: string;
  id: string;
}

export type AttachmentOp = AttachmentPutOp | AttachmentDeleteOp;

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

  /**
   * Deletes a document as a new commit, by key. Unlike `DELETE ... WHERE` through SQL it does not
   * scan, so its cost does not grow with the namespace.
   */
  delete(ns: string, docId: string, options?: CallOptions): Promise<CommitHash>;

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
   * Writes unconditionally: create if absent, otherwise merge `body` into the stored document
   * (keys it omits survive - use `replace` to drop them). No baseVersion, cannot conflict.
   * For namespaces whose conflict policy is LAST_WRITE.
   */
  upsert(ns: string, docId: string, body: unknown, options?: CallOptions): Promise<CommitHash>;

  /**
   * Appends one entry to an APPEND_ONLY namespace. Same transport as upsert, different intent:
   * every write is a new independent record rather than a wholesale replacement.
   */
  appendEvent(ns: string, docId: string, body: unknown, options?: CallOptions): Promise<void>;

  /**
   * Makes `body` the whole document at `docId`, creating it if absent. Unlike `putJSON` and
   * `upsert`, which merge, a key `body` omits is removed - in ONE operation and one commit, so a
   * crash cannot leave the document missing the way delete-then-write could.
   *
   * Anchored on this client's last-seen head, like `putIfAbsent`: it throws KdbConflictError if
   * the document changed since. Use `replaceIf` / `compareAndSwap` to guard on content.
   */
  replace(ns: string, docId: string, body: unknown, options?: CallOptions): Promise<CommitHash>;

  putIfAbsent(ns: string, docId: string, body: unknown, options?: CallOptions): Promise<CommitHash>;

  /**
   * Replaces the document wholesale only if its content hash is still `expectedContentHash`
   * (what `getWithHash` returned): a server-arbitrated compare-and-set. When several clients
   * race on one hash, the server lets exactly one through and the rest get
   * KdbPreconditionError - it holds across processes, not just within this one.
   */
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

  /**
   * Streams a SELECT's result page by page, so a whole-collection read is not capped by the
   * server's `--max-result-rows` (only one page has to fit in a reply). Every page reads the
   * snapshot the first one did, so writes made meanwhile are not seen. The server re-runs the
   * query per page: an ORDER BY pays its sort on each, and a deep page of an unordered query
   * re-reads the rows before it. `pageSize` 0 means the largest page the server allows.
   */
  queryRawPages(
    ns: string,
    sql: string,
    args?: unknown[],
    pageSize?: number,
    options?: CallOptions,
  ): AsyncGenerator<{ columns: string[]; rows: string[][] }, void, undefined>;

  /** `queryRawPages`, collected: every row of the result, mapped like `query`. */
  queryAll<T = Row>(
    ns: string,
    sql: string,
    args?: unknown[],
    pageSize?: number,
    options?: CallOptions,
  ): Promise<T[]>;

  /**
   * Runs one non-SELECT statement, auto-committing it if the server reports it as a write.
   * Resolves to the number of rows the statement affected, so a DELETE that matched nothing is
   * distinguishable from one that removed a row. Concurrent calls on a namespace are safe.
   */
  exec(ns: string, sql: string, args?: unknown[], options?: CallOptions): Promise<number>;
}
