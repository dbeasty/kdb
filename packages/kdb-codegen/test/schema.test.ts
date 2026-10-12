import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { createTableSql, parseSchemaFile, SchemaError } from "../src/schema.ts";
import { generate } from "../src/emit.ts";

const ok = (fields: unknown[], extra: object = {}) =>
  parseSchemaFile({ namespaces: { "app/users": { fields, ...extra } } });

describe("schema parsing", () => {
  it("accepts every type spelling EmbedSchemaDto accepts", () => {
    const names = ["string", "text", "int32", "integer", "int64", "bigint", "float64", "real", "double", "bool", "boolean", "timestamp", "uuid", "object", "json", "array", "TEXT", "Integer"];
    const file = ok(names.map((type, i) => ({ name: `f${i}`, type })));
    assert.deepEqual(
      file.tables[0]!.fields.map((f) => f.kind),
      ["string", "string", "int32", "int32", "int64", "int64", "float64", "float64", "float64", "bool", "bool", "timestamp", "uuid", "object", "object", "array", "string", "int32"],
    );
  });

  it("derives table and type names from the namespace, and honours overrides", () => {
    const t = ok([{ name: "a", type: "string" }]).tables[0]!;
    assert.equal(t.table, "users");
    assert.equal(t.typeName, "Users");
    const o = ok([{ name: "a", type: "string" }], { table: "people", typeName: "Person" }).tables[0]!;
    assert.equal(o.table, "people");
    assert.equal(o.typeName, "Person");
  });

  it("accepts a bare EmbedSchemaDto only with a namespace", () => {
    const bare = { fields: [{ name: "a", type: "string", required: true }] };
    assert.throws(() => parseSchemaFile(bare), /needs --namespace/);
    assert.equal(parseSchemaFile(bare, "team/notes").tables[0]!.table, "notes");
  });

  for (const [label, input, pattern] of [
    ["unknown type", { name: "a", type: "decimal" }, /unknown schema field type "decimal"/],
    ["bare enum", { name: "a", type: "enum" }, /needs its values/],
    ["empty enum", { name: "a", type: { enum: [] } }, /non-empty list/],
    ["duplicate enum value", { name: "a", type: { enum: ["x", "x"] } }, /distinct/],
    ["non-identifier name", { name: "first name", type: "string" }, /not a plain SQL identifier/],
    ["SQL keyword name", { name: "order", type: "string" }, /SQL keyword/],
    ["reserved system column", { name: "kdb_id", type: "uuid" }, /engine adds itself/],
    ["non-boolean flag", { name: "a", type: "string", required: "yes" }, /required must be a boolean/],
  ] as const) {
    it(`rejects ${label}`, () => {
      assert.throws(() => ok([input]), (e: unknown) => e instanceof SchemaError && pattern.test(e.message));
    });
  }

  it("rejects duplicate fields, empty field lists, and clashing type names", () => {
    assert.throws(() => ok([{ name: "a", type: "string" }, { name: "a", type: "int32" }]), /duplicate field/);
    assert.throws(() => ok([]), /non-empty array/);
    assert.throws(
      () =>
        parseSchemaFile({
          namespaces: {
            "a/users": { fields: [{ name: "x", type: "string" }] },
            "b/users": { fields: [{ name: "x", type: "string" }] },
          },
        }),
      /table name users/,
    );
  });
});

describe("DDL", () => {
  it("maps types, NOT NULL, and omits object/array fields (no JSON column type in the SQL DDL)", () => {
    const t = ok([
      { name: "name", type: "string", required: true },
      { name: "n", type: "int32" },
      { name: "big", type: "int64" },
      { name: "f", type: "float64" },
      { name: "b", type: "bool" },
      { name: "ts", type: "timestamp" },
      { name: "id", type: "uuid" },
      { name: "e", type: { enum: ["x"] } },
      { name: "o", type: "object" },
      { name: "a", type: "array" },
    ]).tables[0]!;
    assert.equal(
      createTableSql(t),
      "CREATE TABLE users (name TEXT NOT NULL, n INTEGER, big BIGINT, f REAL, b BOOLEAN, ts TIMESTAMP, id TEXT, e TEXT)",
    );
  });
});

describe("generate", () => {
  it("is deterministic and changes when the schema changes", () => {
    const a = generate(ok([{ name: "a", type: "string" }]));
    assert.equal(a, generate(ok([{ name: "a", type: "string" }])));
    assert.notEqual(a, generate(ok([{ name: "a", type: "string", required: true }])));
  });

  it("is injection-safe for hostile enum values and namespaces", () => {
    const out = generate(ok([{ name: "e", type: { enum: ['a"; process.exit(1); "', "b"] } }]));
    assert.ok(out.includes(JSON.stringify('a"; process.exit(1); "')));
  });
});
