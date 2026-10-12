/**
 * The typed handle: `@kdb/client`'s `unknown` bodies narrowed to the generated document type,
 * and the SQL builder bound to one namespace.
 *
 * Commits and `baseVersion` stay visible rather than hidden behind an ORM's illusion of mutable
 * objects (Component 63 §10.4): `get` returns the commit a body was read at, and `commit` takes
 * that as its `baseVersion`, so a read-then-write that lost a race is a conflict the caller sees.
 * The wire client exposes no branch operations, so neither does this - it would be inventing them.
 */

import type {
  CallOptions,
  CommitHash,
  CompareAndSwapOptions,
  KdbOperations,
  Precondition,
} from "@kdb/client";
import {
  CountQuery,
  DeleteQuery,
  InsertQuery,
  SelectQuery,
  UpdateQuery,
  type TableMeta,
} from "./builder.ts";
import type { Columns, InsertValues, SqlColumnKey, UpdateValues, SelectableKey } from "./types.ts";

export interface TypedTransaction<Doc> {
  /** The commit this transaction was computed against - typically the `commit` a `get` returned. */
  baseVersion: CommitHash;
  writes: Array<{ docId: string; body: Doc }>;
  deletes?: string[];
  preconditions?: Precondition[];
}

export class Table<Doc, Cols extends Columns> {
  readonly meta: TableMeta<Cols>;
  readonly #ops: KdbOperations;

  constructor(ops: KdbOperations, meta: TableMeta<Cols>) {
    this.#ops = ops;
    this.meta = meta;
  }

  get namespace(): string {
    return this.meta.namespace;
  }

  /** Installs the schema on the namespace (`CREATE TABLE`). Run once, before the first write. */
  create(options?: CallOptions): Promise<void> {
    return this.#ops.exec(this.meta.namespace, this.meta.createSql, [], options);
  }

  /**
   * `create()` that is safe to run on every start: installs the schema if the namespace has none,
   * and does nothing if it already has one. Returns whether it installed it.
   *
   * Needed because a SQL-defined schema is not restored when a store is reopened - the engine opens
   * every namespace with no schema - so a persistent database must re-apply it each time. It does
   * not check that an existing schema equals this one.
   */
  async ensure(options?: CallOptions): Promise<boolean> {
    try {
      await this.create(options);
      return true;
    } catch (error) {
      if (error instanceof Error && /schema already exists/i.test(error.message)) return false;
      throw error;
    }
  }

  // --- documents, typed ----------------------------------------------------------------------

  put(docId: string, body: Doc, options?: CallOptions): Promise<CommitHash> {
    return this.#ops.putJSON(this.meta.namespace, docId, body, options);
  }

  upsert(docId: string, body: Doc, options?: CallOptions): Promise<CommitHash> {
    return this.#ops.upsert(this.meta.namespace, docId, body, options);
  }

  putIfAbsent(docId: string, body: Doc, options?: CallOptions): Promise<CommitHash> {
    return this.#ops.putIfAbsent(this.meta.namespace, docId, body, options);
  }

  replaceIf(docId: string, body: Doc, expectedContentHash: string, options?: CallOptions): Promise<CommitHash> {
    return this.#ops.replaceIf(this.meta.namespace, docId, body, expectedContentHash, options);
  }

  replaceIfPresent(docId: string, body: Doc, options?: CallOptions): Promise<CommitHash> {
    return this.#ops.replaceIfPresent(this.meta.namespace, docId, body, options);
  }

  delete(docId: string, options?: CallOptions): Promise<CommitHash> {
    return this.#ops.delete(this.meta.namespace, docId, options);
  }

  async get(docId: string, options?: CallOptions): Promise<{ body: Doc; commit: CommitHash }> {
    const { body, commit } = await this.#ops.get(this.meta.namespace, docId, options);
    return { body: body as Doc, commit };
  }

  async getWithHash(docId: string, options?: CallOptions): Promise<{ body: Doc; contentHash: string }> {
    const { body, contentHash } = await this.#ops.getWithHash(this.meta.namespace, docId, options);
    return { body: body as Doc, contentHash };
  }

  compareAndSwap(
    docId: string,
    mutate: (current: Doc | null) => Doc | null | Promise<Doc | null>,
    options?: CompareAndSwapOptions,
  ): Promise<CommitHash> {
    return this.#ops.compareAndSwap(
      this.meta.namespace,
      docId,
      (current) => mutate(current as Doc | null),
      options,
    );
  }

  commit(tx: TypedTransaction<Doc>, options?: CallOptions): Promise<CommitHash> {
    return this.#ops.commit({ namespace: this.meta.namespace, ...tx }, options);
  }

  // --- SQL, typed ------------------------------------------------------------------------------

  /** `select("name", "age")` - the row type is exactly those columns, coerced per the schema. */
  select<K extends SelectableKey<Cols>>(...columns: K[]): SelectQuery<Cols, K> {
    if (columns.length === 0) throw new Error("kdb-codegen: select needs at least one column");
    return new SelectQuery(this.#ops, this.meta, columns);
  }

  /** Every SQL-addressable column (not `kdb_id`, not object/array fields). */
  selectAll(): SelectQuery<Cols, SqlColumnKey<Cols>> {
    const columns = Object.entries(this.meta.columns)
      .filter(([, def]) => def.kind !== "object" && def.kind !== "array")
      .map(([name]) => name) as SqlColumnKey<Cols>[];
    return new SelectQuery(this.#ops, this.meta, columns);
  }

  count(): CountQuery<Cols> {
    return new CountQuery(this.#ops, this.meta);
  }

  insert(values: InsertValues<Cols>): InsertQuery<Cols> {
    return new InsertQuery(this.#ops, this.meta, values);
  }

  update(values: UpdateValues<Cols>): UpdateQuery<Cols> {
    return new UpdateQuery(this.#ops, this.meta, values);
  }

  deleteFrom(): DeleteQuery<Cols> {
    return new DeleteQuery(this.#ops, this.meta);
  }
}

/**
 * Called by generated code: `defineTable<User>()(meta)` fixes the document type once and lets the
 * column types be inferred from the `as const` metadata beside it.
 */
export function defineTable<Doc>(): <const Cols extends Columns>(
  meta: TableMeta<Cols>,
) => TableDefinition<Doc, Cols> {
  return (meta) => ({
    meta,
    bind: (ops: KdbOperations) => new Table<Doc, typeof meta.columns>(ops, meta),
  });
}

export interface TableDefinition<Doc, Cols extends Columns> {
  readonly meta: TableMeta<Cols>;
  bind(ops: KdbOperations): Table<Doc, Cols>;
}
