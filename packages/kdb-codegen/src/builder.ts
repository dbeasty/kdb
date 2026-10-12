/**
 * The typed SQL builder: produces SQL text plus an argument array for `query`/`exec`, and maps
 * result strings back to real types using the schema (Component 63 §10.3).
 *
 * Everything a caller supplies reaches the server as a bound `?` parameter, never as text - with
 * one exception, a bigint, which JSON cannot carry. It is inlined as decimal digits, which is safe
 * because it is a `bigint` by type (and checked at runtime), so it can contain nothing else.
 * Identifiers come from the generated schema, validated by the generator, never from the caller.
 */

import type { CallOptions, KdbOperations } from "@kdb/client";
import { coerceValue } from "./coerce.ts";
import type {
  AllColumns,
  ColumnDef,
  Columns,
  Comparison,
  InsertValues,
  ParamValue,
  RowOf,
  SelectableKey,
  UpdateValues,
} from "./types.ts";

export interface CompiledSql {
  sql: string;
  args: unknown[];
}

/** A boolean condition tree. Built with `ExpressionBuilder`, compiled by `compileCondition`. */
export type Condition =
  | { op: "cmp"; column: string; comparison: Comparison; value: unknown }
  | { op: "in"; column: string; values: unknown[] }
  | { op: "null"; column: string; negated: boolean }
  | { op: "and" | "or"; terms: Condition[] };

export interface ExpressionBuilder<Cols extends Columns> {
  cmp<K extends SelectableKey<Cols>>(
    column: K,
    comparison: Comparison,
    value: ParamValue<AllColumns<Cols>[K]>,
  ): Condition;
  in<K extends SelectableKey<Cols>>(column: K, values: ParamValue<AllColumns<Cols>[K]>[]): Condition;
  isNull(column: SelectableKey<Cols>): Condition;
  isNotNull(column: SelectableKey<Cols>): Condition;
  and(...terms: Condition[]): Condition;
  or(...terms: Condition[]): Condition;
}

export class BuilderError extends Error {
  constructor(message: string) {
    super(`kdb-codegen: ${message}`);
    this.name = "BuilderError";
  }
}

const COMPARISONS: ReadonlySet<string> = new Set(["=", "<>", "<", "<=", ">", ">=", "like"]);

class Compiler {
  readonly args: unknown[] = [];

  private readonly columns: Readonly<Record<string, ColumnDef>>;

  constructor(columns: Readonly<Record<string, ColumnDef>>) {
    this.columns = columns;
  }

  column(name: string): ColumnDef {
    const def = this.columns[name];
    if (def === undefined) throw new BuilderError(`unknown column ${JSON.stringify(name)}`);
    if (def.kind === "object" || def.kind === "array") {
      throw new BuilderError(`column ${JSON.stringify(name)} is ${def.kind}-typed and has no SQL column`);
    }
    return def;
  }

  /** A value as SQL: a bound `?`, or digits for a bigint. */
  value(def: ColumnDef, name: string, value: unknown): string {
    if (typeof value === "bigint") return value.toString();
    if (value === undefined) throw new BuilderError(`column ${JSON.stringify(name)}: undefined is not a value (use null)`);
    if (value !== null && typeof value === "object") {
      throw new BuilderError(`column ${JSON.stringify(name)}: ${def.kind} columns take scalar values`);
    }
    this.args.push(value);
    return "?";
  }

  condition(c: Condition, nested = false): string {
    switch (c.op) {
      case "cmp": {
        const def = this.column(c.column);
        if (!COMPARISONS.has(c.comparison)) throw new BuilderError(`unsupported comparison ${JSON.stringify(c.comparison)}`);
        if (c.comparison === "like" && !(def.kind === "string" || def.kind === "enum" || def.kind === "uuid" || def.kind === "timestamp")) {
          throw new BuilderError(`LIKE needs a text column; ${JSON.stringify(c.column)} is ${def.kind}`);
        }
        if (c.value === null) {
          throw new BuilderError(`${c.column} ${c.comparison} null never matches in SQL - use isNull / isNotNull`);
        }
        return `${c.column} ${c.comparison === "like" ? "LIKE" : c.comparison} ${this.value(def, c.column, c.value)}`;
      }
      case "in": {
        const def = this.column(c.column);
        if (c.values.length === 0) throw new BuilderError(`IN on ${JSON.stringify(c.column)} needs at least one value`);
        return `${c.column} IN (${c.values.map((v) => this.value(def, c.column, v)).join(", ")})`;
      }
      case "null":
        this.column(c.column);
        return `${c.column} IS ${c.negated ? "NOT " : ""}NULL`;
      case "and":
      case "or": {
        if (c.terms.length === 0) throw new BuilderError(`${c.op.toUpperCase()} needs at least one term`);
        const parts = c.terms.map((t) => this.condition(t, true));
        if (parts.length === 1) return parts[0] as string;
        const joined = parts.join(` ${c.op.toUpperCase()} `);
        return nested ? `(${joined})` : joined;
      }
    }
  }
}

function expressionBuilder<Cols extends Columns>(): ExpressionBuilder<Cols> {
  return {
    cmp: (column, comparison, value) => ({ op: "cmp", column, comparison, value }),
    in: (column, values) => ({ op: "in", column, values }),
    isNull: (column) => ({ op: "null", column, negated: false }),
    isNotNull: (column) => ({ op: "null", column, negated: true }),
    and: (...terms) => ({ op: "and", terms }),
    or: (...terms) => ({ op: "or", terms }),
  };
}

/** The table's runtime description, as emitted by the generator. */
export interface TableMeta<Cols extends Columns> {
  readonly namespace: string;
  readonly table: string;
  readonly columns: Cols;
  readonly createSql: string;
}

function allColumns(meta: TableMeta<Columns>): Record<string, ColumnDef> {
  return { ...meta.columns, kdb_id: { kind: "uuid", required: true } };
}

abstract class Filtered<Cols extends Columns, Self> {
  protected readonly conditions: Condition[] = [];
  protected readonly ops: KdbOperations;
  protected readonly meta: TableMeta<Cols>;

  protected constructor(ops: KdbOperations, meta: TableMeta<Cols>) {
    this.ops = ops;
    this.meta = meta;
  }

  protected abstract self(): Self;

  /** Adds `column <comparison> value`, ANDed with what is already there. */
  where<K extends SelectableKey<Cols>>(
    column: K,
    comparison: Comparison,
    value: ParamValue<AllColumns<Cols>[K]>,
  ): Self;
  /** Adds an arbitrary condition built with the expression builder, e.g. `eb.or(eb.cmp(...), eb.isNull(...))`. */
  where(build: (eb: ExpressionBuilder<Cols>) => Condition): Self;
  where(first: unknown, comparison?: Comparison, value?: unknown): Self {
    if (typeof first === "function") {
      this.conditions.push((first as (eb: ExpressionBuilder<Cols>) => Condition)(expressionBuilder<Cols>()));
    } else {
      this.conditions.push({ op: "cmp", column: first as string, comparison: comparison as Comparison, value });
    }
    return this.self();
  }

  protected whereSql(compiler: Compiler): string {
    if (this.conditions.length === 0) return "";
    const root: Condition = this.conditions.length === 1 ? (this.conditions[0] as Condition) : { op: "and", terms: this.conditions };
    return ` WHERE ${compiler.condition(root)}`;
  }
}

export class SelectQuery<Cols extends Columns, K extends SelectableKey<Cols>> extends Filtered<Cols, SelectQuery<Cols, K>> {
  #order: Array<{ column: string; desc: boolean }> = [];
  #limit: number | undefined;
  #offset: number | undefined;

  readonly #selected: readonly K[];

  constructor(ops: KdbOperations, meta: TableMeta<Cols>, selected: readonly K[]) {
    super(ops, meta);
    this.#selected = selected;
  }

  protected self(): this {
    return this;
  }

  orderBy(column: SelectableKey<Cols>, direction: "asc" | "desc" = "asc"): this {
    this.#order.push({ column, desc: direction === "desc" });
    return this;
  }

  limit(n: number): this {
    if (!Number.isInteger(n) || n < 0) throw new BuilderError("limit must be a non-negative integer");
    this.#limit = n;
    return this;
  }

  offset(n: number): this {
    if (!Number.isInteger(n) || n < 0) throw new BuilderError("offset must be a non-negative integer");
    this.#offset = n;
    return this;
  }

  toSQL(): CompiledSql {
    const compiler = new Compiler(allColumns(this.meta));
    for (const c of this.#selected) compiler.column(c);
    let sql = `SELECT ${this.#selected.join(", ")} FROM ${this.meta.table}`;
    sql += this.whereSql(compiler);
    if (this.#order.length > 0) {
      sql += ` ORDER BY ${this.#order.map((o) => { compiler.column(o.column); return o.column + (o.desc ? " DESC" : ""); }).join(", ")}`;
    }
    if (this.#limit !== undefined) sql += ` LIMIT ${this.#limit}`;
    if (this.#offset !== undefined) sql += ` OFFSET ${this.#offset}`;
    return { sql, args: compiler.args };
  }

  async execute(options?: CallOptions): Promise<Array<RowOf<Cols, K>>> {
    const { sql, args } = this.toSQL();
    const defs = allColumns(this.meta);
    const result = await this.ops.queryRaw(this.meta.namespace, sql, args, options);
    return result.rows.map((row) => {
      const out: Record<string, unknown> = {};
      result.columns.forEach((name, i) => {
        const def = defs[name];
        if (def === undefined) throw new BuilderError(`server returned unexpected column ${JSON.stringify(name)}`);
        out[name] = coerceValue(name, def, row[i] ?? "");
      });
      return out as RowOf<Cols, K>;
    });
  }

  /** The first matching row, or undefined. Adds LIMIT 1. */
  async first(options?: CallOptions): Promise<RowOf<Cols, K> | undefined> {
    const rows = await this.limit(1).execute(options);
    return rows[0];
  }
}

export class CountQuery<Cols extends Columns> extends Filtered<Cols, CountQuery<Cols>> {
  constructor(ops: KdbOperations, meta: TableMeta<Cols>) {
    super(ops, meta);
  }

  protected self(): this {
    return this;
  }

  toSQL(): CompiledSql {
    const compiler = new Compiler(allColumns(this.meta));
    const sql = `SELECT COUNT(*) AS n FROM ${this.meta.table}${this.whereSql(compiler)}`;
    return { sql, args: compiler.args };
  }

  async execute(options?: CallOptions): Promise<number> {
    const { sql, args } = this.toSQL();
    const result = await this.ops.queryRaw(this.meta.namespace, sql, args, options);
    const raw = result.rows[0]?.[0];
    const n = Number(raw);
    if (raw === undefined || !Number.isInteger(n)) throw new BuilderError(`COUNT returned ${JSON.stringify(raw)}`);
    return n;
  }
}

export class InsertQuery<Cols extends Columns> {
  readonly #ops: KdbOperations;
  readonly #meta: TableMeta<Cols>;
  readonly #values: InsertValues<Cols>;

  constructor(ops: KdbOperations, meta: TableMeta<Cols>, values: InsertValues<Cols>) {
    this.#ops = ops;
    this.#meta = meta;
    this.#values = values;
  }

  toSQL(): CompiledSql {
    const compiler = new Compiler(allColumns(this.#meta));
    const names = Object.keys(this.#values);
    if (names.length === 0) throw new BuilderError("insert needs at least one value");
    for (const [name, def] of Object.entries(this.#meta.columns)) {
      if (def.required && (this.#values as Record<string, unknown>)[name] == null && def.kind !== "object" && def.kind !== "array") {
        throw new BuilderError(`insert is missing required column ${JSON.stringify(name)}`);
      }
    }
    const placeholders = names.map((n) => compiler.value(compiler.column(n), n, (this.#values as Record<string, unknown>)[n]));
    return { sql: `INSERT INTO ${this.#meta.table} (${names.join(", ")}) VALUES (${placeholders.join(", ")})`, args: compiler.args };
  }

  async execute(options?: CallOptions): Promise<void> {
    const { sql, args } = this.toSQL();
    await this.#ops.exec(this.#meta.namespace, sql, args, options);
  }
}

export class UpdateQuery<Cols extends Columns> extends Filtered<Cols, UpdateQuery<Cols>> {
  readonly #values: UpdateValues<Cols>;

  constructor(ops: KdbOperations, meta: TableMeta<Cols>, values: UpdateValues<Cols>) {
    super(ops, meta);
    this.#values = values;
  }

  protected self(): this {
    return this;
  }

  toSQL(): CompiledSql {
    const compiler = new Compiler(allColumns(this.meta));
    const names = Object.keys(this.#values);
    if (names.length === 0) throw new BuilderError("update needs at least one value");
    const sets = names.map((n) => `${n} = ${compiler.value(compiler.column(n), n, (this.#values as Record<string, unknown>)[n])}`);
    return { sql: `UPDATE ${this.meta.table} SET ${sets.join(", ")}${this.whereSql(compiler)}`, args: compiler.args };
  }

  /** Refuses to run without a WHERE: an unfiltered UPDATE is almost never what was meant. Use `.all()` if it is. */
  async execute(options?: CallOptions): Promise<void> {
    if (this.conditions.length === 0 && !this.#all) throw new BuilderError("update without a WHERE would change every row; call .all() to confirm");
    const { sql, args } = this.toSQL();
    await this.ops.exec(this.meta.namespace, sql, args, options);
  }

  #all = false;
  all(): this {
    this.#all = true;
    return this;
  }
}

export class DeleteQuery<Cols extends Columns> extends Filtered<Cols, DeleteQuery<Cols>> {
  #all = false;

  constructor(ops: KdbOperations, meta: TableMeta<Cols>) {
    super(ops, meta);
  }

  protected self(): this {
    return this;
  }

  all(): this {
    this.#all = true;
    return this;
  }

  toSQL(): CompiledSql {
    const compiler = new Compiler(allColumns(this.meta));
    return { sql: `DELETE FROM ${this.meta.table}${this.whereSql(compiler)}`, args: compiler.args };
  }

  async execute(options?: CallOptions): Promise<void> {
    if (this.conditions.length === 0 && !this.#all) throw new BuilderError("delete without a WHERE would remove every row; call .all() to confirm");
    const { sql, args } = this.toSQL();
    await this.ops.exec(this.meta.namespace, sql, args, options);
  }
}
