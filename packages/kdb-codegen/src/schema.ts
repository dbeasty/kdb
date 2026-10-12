/**
 * The schema description codegen consumes, and its validation.
 *
 * The input is the JSON form of `kdb-schema` that `EmbedSchemaDto` (kdb-embed) already defines -
 * `{ "fields": [{ "name", "type", "required", "indexed", "unique" }] }` - grouped by namespace.
 * Codegen deliberately defines no schema language of its own (Component 63 §10.2): the schema
 * authority is server-side, and a second hand-written description is exactly the drift this
 * package exists to prevent.
 */

export type FieldKind =
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

export interface FieldDef {
  name: string;
  kind: FieldKind;
  required: boolean;
  indexed: boolean;
  unique: boolean;
  /** Only for kind "enum". */
  values?: string[];
}

export interface TableSchema {
  namespace: string;
  /** SQL table name. The server ignores it (one namespace is one table) but queries read better with it. */
  table: string;
  /** Name of the generated document interface. */
  typeName: string;
  fields: FieldDef[];
}

export interface SchemaFile {
  tables: TableSchema[];
}

export class SchemaError extends Error {
  constructor(message: string) {
    super(`kdb-codegen: ${message}`);
    this.name = "SchemaError";
  }
}

/** Mirrors EmbedSchema.kt's parseFieldType, so a file valid for KdbBrowser is valid here. */
const TYPE_ALIASES: Record<string, FieldKind> = {
  string: "string",
  text: "string",
  int32: "int32",
  integer: "int32",
  int64: "int64",
  bigint: "int64",
  float64: "float64",
  real: "float64",
  double: "float64",
  bool: "bool",
  boolean: "bool",
  timestamp: "timestamp",
  uuid: "uuid",
  object: "object",
  json: "object",
  array: "array",
};

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;
/** Words the KDB SQL parser would read as syntax rather than as a column name. */
const SQL_RESERVED = new Set([
  "select", "from", "where", "and", "or", "not", "in", "like", "is", "null", "order", "by",
  "group", "limit", "offset", "insert", "into", "values", "update", "set", "delete", "create",
  "table", "drop", "as", "asc", "desc", "true", "false", "count", "on", "join", "having",
  "distinct", "primary", "key", "index", "unique", "between", "union",
]);

export function sqlIdentifier(name: string, what: string): string {
  if (!IDENTIFIER.test(name)) {
    throw new SchemaError(`${what} ${JSON.stringify(name)} is not a plain SQL identifier ([A-Za-z_][A-Za-z0-9_]*)`);
  }
  if (SQL_RESERVED.has(name.toLowerCase())) {
    throw new SchemaError(`${what} ${JSON.stringify(name)} is a SQL keyword and cannot be addressed as a column`);
  }
  return name;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseField(raw: unknown, where: string): FieldDef {
  if (!isRecord(raw)) throw new SchemaError(`${where}: field must be an object`);
  const name = raw["name"];
  if (typeof name !== "string") throw new SchemaError(`${where}: field.name must be a string`);
  const at = `${where}.${name}`;
  sqlIdentifier(name, `${where}: field name`);

  const typeRaw = raw["type"];
  let kind: FieldKind;
  let values: string[] | undefined;
  if (typeof typeRaw === "string") {
    const lowered = typeRaw.toLowerCase();
    if (lowered === "enum") {
      throw new SchemaError(`${at}: an enum needs its values - use { "type": { "enum": ["a", "b"] } }`);
    }
    const mapped = TYPE_ALIASES[lowered];
    if (mapped === undefined) throw new SchemaError(`${at}: unknown schema field type ${JSON.stringify(typeRaw)}`);
    kind = mapped;
  } else if (isRecord(typeRaw) && Array.isArray(typeRaw["enum"])) {
    const list = typeRaw["enum"];
    if (list.length === 0 || !list.every((v) => typeof v === "string" && v !== "")) {
      throw new SchemaError(`${at}: enum values must be a non-empty list of non-empty strings`);
    }
    if (new Set(list).size !== list.length) throw new SchemaError(`${at}: enum values must be distinct`);
    kind = "enum";
    values = [...(list as string[])];
  } else {
    throw new SchemaError(`${at}: field.type must be a type name or { "enum": [...] }`);
  }

  const flag = (key: string): boolean => {
    const value = raw[key];
    if (value === undefined) return false;
    if (typeof value !== "boolean") throw new SchemaError(`${at}: ${key} must be a boolean`);
    return value;
  };
  const field: FieldDef = {
    name,
    kind,
    required: flag("required"),
    indexed: flag("indexed"),
    unique: flag("unique"),
  };
  if (values !== undefined) field.values = values;
  return field;
}

function pascalCase(input: string): string {
  const parts = input.split(/[^A-Za-z0-9]+/).filter((p) => p !== "");
  const joined = parts.map((p) => p.charAt(0).toUpperCase() + p.slice(1)).join("");
  return /^[0-9]/.test(joined) ? `_${joined}` : joined;
}

function tableFrom(namespace: string, raw: unknown): TableSchema {
  const where = `namespace ${JSON.stringify(namespace)}`;
  if (!isRecord(raw)) throw new SchemaError(`${where}: expected an object with "fields"`);
  const fieldsRaw = raw["fields"];
  if (!Array.isArray(fieldsRaw) || fieldsRaw.length === 0) {
    throw new SchemaError(`${where}: "fields" must be a non-empty array`);
  }
  const fields = fieldsRaw.map((f) => parseField(f, where));
  const seen = new Set<string>();
  for (const f of fields) {
    if (seen.has(f.name)) throw new SchemaError(`${where}: duplicate field ${JSON.stringify(f.name)}`);
    seen.add(f.name);
  }
  if (seen.has("kdb_id") || seen.has("_doc")) {
    throw new SchemaError(`${where}: "kdb_id" and "_doc" are columns the engine adds itself`);
  }

  const segments = namespace.split("/").filter((s) => s !== "");
  const defaultTable = (segments[segments.length - 1] ?? "").replace(/[^A-Za-z0-9_]/g, "_");
  const tableRaw = raw["table"];
  if (tableRaw !== undefined && typeof tableRaw !== "string") throw new SchemaError(`${where}: table must be a string`);
  const table = sqlIdentifier(tableRaw ?? defaultTable, `${where}: table name`);

  const typeRaw = raw["typeName"];
  if (typeRaw !== undefined && typeof typeRaw !== "string") throw new SchemaError(`${where}: typeName must be a string`);
  const typeName = typeRaw ?? pascalCase(table);
  if (!IDENTIFIER.test(typeName)) throw new SchemaError(`${where}: typeName ${JSON.stringify(typeName)} is not a valid TypeScript identifier`);

  return { namespace, table, typeName, fields };
}

/**
 * Accepts either `{ "namespaces": { "<ns>": { "fields": [...] } } }` or a bare EmbedSchemaDto
 * `{ "fields": [...] }`, which needs `defaultNamespace` to say which namespace it describes.
 */
export function parseSchemaFile(input: unknown, defaultNamespace?: string): SchemaFile {
  if (!isRecord(input)) throw new SchemaError("schema file must be a JSON object");
  let tables: TableSchema[];
  if (isRecord(input["namespaces"])) {
    const entries = Object.entries(input["namespaces"]);
    if (entries.length === 0) throw new SchemaError(`"namespaces" is empty`);
    tables = entries.map(([ns, raw]) => tableFrom(ns, raw));
  } else if (Array.isArray(input["fields"])) {
    if (defaultNamespace === undefined || defaultNamespace === "") {
      throw new SchemaError(`a bare { "fields": [...] } schema needs --namespace to say which namespace it describes`);
    }
    tables = [tableFrom(defaultNamespace, input)];
  } else {
    throw new SchemaError(`expected { "namespaces": {...} } or { "fields": [...] }`);
  }

  const names = new Set<string>();
  const tableNames = new Set<string>();
  for (const t of tables) {
    if (tableNames.has(t.table)) throw new SchemaError(`two namespaces use the table name ${t.table}; set "table" on one`);
    tableNames.add(t.table);
    if (names.has(t.typeName)) throw new SchemaError(`two namespaces generate the type name ${t.typeName}; set "typeName" on one`);
    names.add(t.typeName);
  }
  return { tables };
}

/** The DDL that installs this schema on a namespace. Object/array fields are omitted: the SQL DDL has no JSON column type. */
export function createTableSql(table: TableSchema): string {
  const columns: string[] = [];
  for (const f of table.fields) {
    const sql = SQL_TYPE[f.kind];
    if (sql === null) continue;
    columns.push(`${f.name} ${sql}${f.required ? " NOT NULL" : ""}`);
  }
  return `CREATE TABLE ${table.table} (${columns.join(", ")})`;
}

const SQL_TYPE: Record<FieldKind, string | null> = {
  string: "TEXT",
  int32: "INTEGER",
  int64: "BIGINT",
  float64: "REAL",
  bool: "BOOLEAN",
  timestamp: "TIMESTAMP",
  uuid: "TEXT",
  enum: "TEXT",
  object: null,
  array: null,
};
