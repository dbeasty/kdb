/**
 * Per-column coercion of SQL result strings - the part `@limidus/kdb-client` refuses to guess at.
 *
 * Every function here fails loudly on a value that cannot be what the schema says it is. That is
 * deliberate: an empty value in a *required* column, or "abc" in an INTEGER column, means the
 * generated types and the namespace's real schema have drifted apart, and surfacing that as an
 * error is the point of generating from the schema in the first place.
 */

import type { ColumnDef } from "./types.ts";

export class KdbCoercionError extends Error {
  readonly column: string;
  readonly raw: string;
  constructor(column: string, raw: string, expected: string) {
    super(`kdb-codegen: column ${JSON.stringify(column)} returned ${JSON.stringify(raw)}, expected ${expected} - the generated types have drifted from the server's schema`);
    this.name = "KdbCoercionError";
    this.column = column;
    this.raw = raw;
  }
}

export function coerceValue(column: string, def: ColumnDef, raw: string): unknown {
  if (def.kind === "string") return raw;
  if (raw === "") {
    if (def.required) throw new KdbCoercionError(column, raw, `a ${def.kind} (column is required)`);
    return null;
  }
  switch (def.kind) {
    case "int32": {
      const n = Number(raw);
      if (!Number.isInteger(n)) throw new KdbCoercionError(column, raw, "an integer");
      return n;
    }
    case "float64": {
      const n = Number(raw);
      if (Number.isNaN(n)) throw new KdbCoercionError(column, raw, "a number");
      return n;
    }
    case "int64":
      try {
        return BigInt(raw);
      } catch {
        throw new KdbCoercionError(column, raw, "a 64-bit integer");
      }
    case "bool": {
      if (raw === "1" || raw === "true") return true;
      if (raw === "0" || raw === "false") return false;
      throw new KdbCoercionError(column, raw, "a boolean (1/0/true/false)");
    }
    case "enum":
      if (def.values !== undefined && !def.values.includes(raw)) {
        throw new KdbCoercionError(column, raw, `one of ${def.values.join(", ")}`);
      }
      return raw;
    case "timestamp":
    case "uuid":
      return raw;
    default:
      throw new KdbCoercionError(column, raw, "a SQL-addressable column");
  }
}
