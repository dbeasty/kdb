# `@kdb/codegen`

Types generated from a KDB schema, plus a typed SQL builder over them. Implements
[Component 63](../../docs/kdb-spec-layer12-component63-ts-client-sdk.md) Phase 2 (§10).

The schema authority is the server's. This package never asks you to hand-declare a second one: it
reads the JSON form of `kdb-schema` (the same `EmbedSchemaDto` shape `KdbBrowser.openWithSchema`
consumes) and emits TypeScript, so a generated type cannot drift from the schema without
`kdb-codegen --check` failing in CI.

```jsonc
// kdb.schema.json
{
  "namespaces": {
    "app/users": {
      "table": "users", "typeName": "User",
      "fields": [
        { "name": "name",    "type": "string", "required": true },
        { "name": "age",     "type": "int32" },
        { "name": "balance", "type": "int64" },
        { "name": "tier",    "type": { "enum": ["free", "pro"] } },
        { "name": "profile", "type": "object" }
      ]
    }
  }
}
```

```sh
npx kdb-codegen --schema kdb.schema.json --out src/kdb.ts
npx kdb-codegen --schema kdb.schema.json --out src/kdb.ts --check   # CI: exit 1 if stale
```

A bare `{ "fields": [...] }` (an `EmbedSchemaDto`) also works with `--namespace app/users`.
Types are `string|text`, `int32|integer`, `int64|bigint`, `float64|real|double`, `bool|boolean`,
`timestamp`, `uuid`, `object|json`, `array`, or `{ "enum": [...] }`.

## Using it

`bind` takes anything that implements `KdbOperations` — a `@kdb/client` connection or an
`@kdb/embed` database — so the same application code runs against either.

```ts
import { connect } from "@kdb/client";
import { bind } from "./kdb.ts";

const db = bind(await connect("ws://localhost:9090/kdb"));
await db.users.create();                       // installs the schema (CREATE TABLE)

await db.users.insert({ name: "ann", age: 30, tier: "pro" }).execute();

const rows = await db.users
  .select("name", "age", "balance", "tier")    // row type is exactly these columns
  .where("age", ">", 26)                       // value checked against the column type
  .where((eb) => eb.or(eb.cmp("tier", "=", "pro"), eb.isNull("age")))
  .orderBy("age", "desc")
  .limit(10)
  .execute();
// rows[0].age: number | null   rows[0].balance: bigint | null   rows[0].tier: "free" | "pro" | null

await db.users.update({ age: 31 }).where("name", "=", "ann").execute();
await db.users.count().where("tier", "=", "pro").execute();      // number
const { sql, args } = db.users.select("name").where("age", ">", 3).toSQL();  // inspect without running
```

Documents are typed too, and commits stay visible rather than hidden behind an ORM:

```ts
const { body, commit } = await db.users.get(id);              // body: User
await db.users.commit({ baseVersion: commit, writes: [{ docId: id, body: { ...body, age: 32 } }] });
await db.users.compareAndSwap(id, (cur) => (cur ? { ...cur, age: (cur.age ?? 0) + 1 } : null));
```

## What the builder does about types

`@kdb/client` returns every SQL value as a string and refuses to guess. With the schema in hand this
layer can, and is strict about it:

| Column | Row type | Notes |
|---|---|---|
| `string` | `string` | An absent optional string reads as `""`: SQL returns NULL and `""` identically, so claiming `null` would be a lie. |
| `int32`, `float64` | `number` / `number \| null` | |
| `int64` | `bigint` / `bigint \| null` | Exact above 2^53. Filters take a `bigint`; it is inlined as digits since JSON cannot carry one. |
| `bool` | `boolean` / `boolean \| null` | The engine reports `1`/`0`; `false` is not mistaken for absent. |
| `timestamp`, `uuid` | `string` | ISO-8601 text as stored, not a lossy `Date`. |
| `enum` | union of the literals | |
| `object`, `array` | — | No SQL column type exists; they are typed on documents only and not selectable. |

A value that cannot be what the schema says — `""` in a required column, `"abc"` in an `INTEGER`,
an enum value outside the set — throws `KdbCoercionError`. That is the drift detector: it means the
generated file is stale against the server.

Every caller-supplied value is a bound `?` parameter. Identifiers come from the generated schema,
never from the caller. `update()`/`deleteFrom()` without a `WHERE` throw unless you say `.all()`.

## Limits

- The server has no message that exports a namespace's schema, so the schema file is an input you
  maintain (or generate from the same source that creates the namespace). Reading it live from a
  server is not implemented.
- Field names must be plain identifiers and not SQL keywords (they have to be addressable as
  columns); the generator rejects the rest with a clear error rather than emitting broken SQL.
- `CREATE TABLE` has no JSON column type, so `object`/`array` fields are left out of the DDL and are
  therefore not validated by the server's schema.
- One namespace is one table. The table name is cosmetic — the server ignores it.
- No joins, aggregates beyond `COUNT(*)`, or `GROUP BY` yet; use `queryRaw` for those.
