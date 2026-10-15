/**
 * @limidus/kdb-codegen - types generated from a KDB schema, plus a typed SQL builder over them.
 *
 * Generated files import `defineTable` from here; applications import the generated file.
 */

export { defineTable, Table } from "./table.ts";
export type { TableDefinition, TypedTransaction } from "./table.ts";
export {
  BuilderError,
  CountQuery,
  DeleteQuery,
  InsertQuery,
  SelectQuery,
  UpdateQuery,
} from "./builder.ts";
export type { CompiledSql, Condition, ExpressionBuilder, TableMeta } from "./builder.ts";
export { coerceValue, KdbCoercionError } from "./coerce.ts";
export type {
  ColumnDef,
  ColumnKind,
  Columns,
  Comparison,
  InsertValues,
  ParamValue,
  PresentValue,
  RowOf,
  RowValue,
  SelectableKey,
  UpdateValues,
} from "./types.ts";
export { createTableSql, parseSchemaFile, SchemaError } from "./schema.ts";
export type { FieldDef, FieldKind, SchemaFile, TableSchema } from "./schema.ts";
