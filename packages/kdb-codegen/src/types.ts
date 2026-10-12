/** Type-level mapping from a column definition to the TypeScript type it reads and writes as. */

export type ColumnKind =
  | "string"
  | "int32"
  | "int64"
  | "float64"
  | "bool"
  | "timestamp"
  | "uuid"
  | "object"
  | "array"
  | "enum";

export interface ColumnDef {
  readonly kind: ColumnKind;
  readonly required: boolean;
  readonly values?: readonly string[];
}

export type Columns = Readonly<Record<string, ColumnDef>>;

/** Columns the engine adds to every row. `kdb_id` is the document id and can be selected and filtered on. */
export interface SystemColumns {
  readonly kdb_id: { readonly kind: "uuid"; readonly required: true };
}

/**
 * What a value of this column is, once present.
 *
 * int64 is `bigint` in rows: a BIGINT above 2^53 survives the SQL result string exactly, and
 * coercing it to `number` would silently corrupt it - the thing the client refuses to do.
 * (Document bodies are plain JSON, so an int64 field there is a `number`; JSON has no bigint.)
 */
export type PresentValue<C extends ColumnDef> = C["kind"] extends "string"
  ? string
  : C["kind"] extends "int32" | "float64"
    ? number
    : C["kind"] extends "int64"
      ? bigint
      : C["kind"] extends "bool"
        ? boolean
        : C["kind"] extends "timestamp" | "uuid"
          ? string
          : C["kind"] extends "enum"
            ? C["values"] extends readonly string[]
              ? C["values"][number]
              : string
            : never;

/**
 * What a row column reads as. An absent value is `null` - except for `string`: SQL returns NULL
 * and the empty string identically (both arrive as ""), so an optional string reads as "" rather
 * than pretending to tell them apart.
 */
export type RowValue<C extends ColumnDef> = C["kind"] extends "string"
  ? string
  : C["required"] extends true
    ? PresentValue<C>
    : PresentValue<C> | null;

/** What may be bound as a parameter or written to this column. */
export type ParamValue<C extends ColumnDef> = C["required"] extends true
  ? PresentValue<C>
  : PresentValue<C> | null;

/** Columns reachable from SQL: everything except object/array, which have no SQL column type. */
export type SqlColumnKey<Cols extends Columns> = {
  [K in keyof Cols & string]: Cols[K]["kind"] extends "object" | "array" ? never : K;
}[keyof Cols & string];

export type AllColumns<Cols extends Columns> = Cols & SystemColumns;

export type SelectableKey<Cols extends Columns> = SqlColumnKey<Cols> | "kdb_id";

export type RowOf<Cols extends Columns, K extends SelectableKey<Cols>> = {
  [P in K]: RowValue<AllColumns<Cols>[P]>;
};

/** Fields an INSERT must provide vs. may omit, from the required flag. */
export type InsertValues<Cols extends Columns> = {
  [K in SqlColumnKey<Cols> as Cols[K]["required"] extends true ? K : never]: ParamValue<Cols[K]>;
} & {
  [K in SqlColumnKey<Cols> as Cols[K]["required"] extends true ? never : K]?: ParamValue<Cols[K]>;
};

export type UpdateValues<Cols extends Columns> = {
  [K in SqlColumnKey<Cols>]?: ParamValue<Cols[K]>;
};

export type Comparison = "=" | "<>" | "<" | "<=" | ">" | ">=" | "like";
