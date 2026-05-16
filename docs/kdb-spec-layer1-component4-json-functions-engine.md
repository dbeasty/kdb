# KDB Component Spec — Layer 1, Component 4

# JSON Functions Engine

# Package: `dev.kdb.json`

# Spec version: aligned with master spec v0.5

-----

## 1. Purpose

Implements the `kdb_json_*` SQL function set and the equivalent Kotlin API for JSONPath-based document access and mutation described in master spec §3.4. This module is the runtime that evaluates JSONPath expressions against JSON strings, returning extracted values or producing new JSON strings with mutations applied. It is the foundation of the hybrid query engine (Layer 5) and is used directly by the transaction engine for non-schema field writes (the `SET _doc = kdb_json_set(...)` pattern).

-----

## 2. Dependencies

|Module         |Interfaces Used                                                                                                                                                     |
|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|`dev.kdb.codec`|`BsonValue`, `BsonDocument`, `BsonArray`, `BsonString`, `BsonInt32`, `BsonInt64`, `BsonDouble`, `BsonBoolean`, `BsonNull`, `BsonDateTime`, `KdbUuid`, `KdbTimestamp`|
|`dev.kdb.error`|`KdbException`, `KdbResult`, `KdbErrorCode`, `JsonPathException`, `kdbRunCatching`                                                                                  |

No dependency on Component 3 (Document + Commit Model). This module operates on raw JSON strings and `BsonValue` trees — it does not know about `KdbDocument` or commits. Higher layers compose the two.

-----

## 3. Public Interface

```kotlin
package dev.kdb.json

import dev.kdb.codec.*
import dev.kdb.error.*

// ── JSONPath evaluation context ────────────────────────────────────────────────

/**
 * Compiled JSONPath expression. Compile once, evaluate many times.
 * Path syntax: $.field, $.nested.field, $.array[0], $.array[*]
 */
class JsonPath private constructor(val expression: String) {
    companion object {
        /**
         * Compile a JSONPath string.
         * @throws JsonPathException if the expression is syntactically invalid.
         */
        fun compile(expression: String): JsonPath

        /**
         * Compile without throwing — returns null on invalid expression.
         */
        fun compileOrNull(expression: String): JsonPath?
    }

    override fun toString(): String
    override fun equals(other: Any?): Boolean
    override fun hashCode(): Int
}

// ── Core functions (operate on JSON strings) ──────────────────────────────────

/**
 * Extract the value at [path] from [json].
 * Returns null if the path does not exist in the document.
 * @throws JsonPathException if [path] is structurally invalid for this document.
 */
fun kdbJsonGet(json: String, path: JsonPath): JsonValue?

/** Convenience overload — compiles path inline. */
fun kdbJsonGet(json: String, path: String): JsonValue?

/**
 * Return a new JSON string with the value at [path] set to [value].
 * Creates intermediate objects as needed.
 * @throws JsonPathException if the path traverses a non-object/non-array node.
 */
fun kdbJsonSet(json: String, path: JsonPath, value: JsonValue): String

/** Convenience overload. */
fun kdbJsonSet(json: String, path: String, value: JsonValue): String

/**
 * Return a new JSON string with the node at [path] removed.
 * No-op (returns [json] unchanged) if the path does not exist.
 * @throws JsonPathException if traversal encounters a type mismatch.
 */
fun kdbJsonDelete(json: String, path: JsonPath): String

/** Convenience overload. */
fun kdbJsonDelete(json: String, path: String): String

/**
 * Return a new JSON string with [patchJson] merged into [json] at the root level.
 * Root-level keys from [patchJson] overwrite corresponding keys in [json].
 * Equivalent to: kdbJsonSet for each key in patchJson at root.
 * @throws JsonPathException if either argument is not a valid JSON object.
 */
fun kdbJsonMerge(json: String, patchJson: String): String

/**
 * Return true if the array at [path] in [json] contains [value].
 * Returns false if the path does not exist or is not an array.
 * @throws JsonPathException if the path resolves to a non-array, non-null node.
 */
fun kdbJsonContains(json: String, path: JsonPath, value: JsonValue): Boolean

/** Convenience overload. */
fun kdbJsonContains(json: String, path: String, value: JsonValue): Boolean

/**
 * Return the set of keys at [path] in [json].
 * Returns null if the path does not exist.
 * @throws JsonPathException if the path resolves to a non-object node.
 */
fun kdbJsonKeys(json: String, path: JsonPath): List<String>?

/** Convenience overload. */
fun kdbJsonKeys(json: String, path: String): List<String>?

/**
 * Return the JSON type name of the value at [path] in [json].
 * Returns null if the path does not exist.
 * Possible return values: "string", "number", "boolean", "null", "object", "array"
 */
fun kdbJsonType(json: String, path: JsonPath): String?

/** Convenience overload. */
fun kdbJsonType(json: String, path: String): String?

/**
 * Return the length of the array at [path] in [json].
 * Returns null if the path does not exist or resolves to null.
 * @throws JsonPathException if the path resolves to a non-array node.
 */
fun kdbJsonArrayLength(json: String, path: JsonPath): Int?

/** Convenience overload. */
fun kdbJsonArrayLength(json: String, path: String): Int?

// ── JsonValue — typed result of path evaluation ───────────────────────────────

/**
 * Typed value returned from JSONPath evaluation.
 * Bridges between JSON types and KDB codec types.
 */
sealed class JsonValue {
    data class JString(val value: String) : JsonValue()
    data class JNumber(val value: Double) : JsonValue()    // JSON has one number type
    data class JInt(val value: Long) : JsonValue()         // integer numbers preserved as Long
    data class JBool(val value: Boolean) : JsonValue()
    object JNull : JsonValue()
    data class JObject(val fields: Map<String, JsonValue>) : JsonValue()
    data class JArray(val elements: List<JsonValue>) : JsonValue()

    /** Encode this value to a JSON string fragment. */
    fun toJsonString(): String

    /** Convert to a BsonValue for index or codec interop. */
    fun toBsonValue(): BsonValue
}

/** Parse a JSON string fragment into a [JsonValue]. */
fun JsonValue.Companion.fromJsonString(json: String): JsonValue

/** Convert a [BsonValue] to a [JsonValue]. */
fun BsonValue.toJsonValue(): JsonValue

// ── Wildcard / multi-match ────────────────────────────────────────────────────

/**
 * Evaluate a path that may contain wildcards ($.array[*], $.*.field).
 * Returns a list of all matching values (empty list if nothing matches).
 * @throws JsonPathException on structural type mismatch during traversal.
 */
fun kdbJsonGetAll(json: String, path: JsonPath): List<JsonValue>

/** Convenience overload. */
fun kdbJsonGetAll(json: String, path: String): List<JsonValue>

// ── Precompiled function registry (for SQL engine integration) ────────────────

/**
 * Registry of all KDB JSON functions, keyed by their SQL name.
 * The hybrid query engine (Layer 5) looks up functions here by name at query plan time.
 */
object KdbJsonFunctionRegistry {
    /** All registered function descriptors. */
    val all: List<KdbJsonFunctionDescriptor>

    /** Look up by SQL function name (case-insensitive). */
    fun get(sqlName: String): KdbJsonFunctionDescriptor?
}

/**
 * Descriptor for a single KDB JSON function, used by the SQL engine.
 */
data class KdbJsonFunctionDescriptor(
    val sqlName: String,              // e.g. "kdb_json_get"
    val minArgs: Int,
    val maxArgs: Int,
    val returnType: JsonFunctionReturnType,
    val evaluate: (args: List<JsonValue?>) -> JsonValue?,
)

enum class JsonFunctionReturnType {
    JSON_STRING,    // returns a JSON string (new document)
    SCALAR,         // returns a scalar JsonValue
    BOOLEAN,        // returns JBool
    INTEGER,        // returns JInt
    STRING_LIST,    // returns JArray of JStrings
}

// ── Exceptions ────────────────────────────────────────────────────────────────

// JsonPathException is defined in dev.kdb.error (Layer 0) and re-exported for convenience.
// No new exception types are introduced by this module.
// All errors surface as JsonPathException with the failing [path] string set.
```

-----

## 4. Data Structures

### JsonValue

`JsonValue` is the bridge type between raw JSON parsing and the rest of the engine. Key decisions:

- JSON has a single `number` type; this module preserves the distinction between integer-valued numbers (`JInt`, backed by `Long`) and floating-point (`JNumber`, backed by `Double`). A JSON value `42` parses to `JInt(42)`, while `42.5` parses to `JNumber(42.5)`. This distinction is important for codec interop (BSON `Int64` vs `Double`).
- `JObject` uses a `Map<String, JsonValue>`. For deterministic serialisation, always use `LinkedHashMap` internally to preserve insertion order.
- `JNull` is a singleton object, not a data class.

### JsonPath syntax supported

|Syntax          |Meaning                                                           |
|----------------|------------------------------------------------------------------|
|`$`             |Root element                                                      |
|`$.field`       |Named field on object                                             |
|`$.nested.field`|Nested field (dot-chained)                                        |
|`$.array[0]`    |Array element by index                                            |
|`$.array[-1]`   |Last array element                                                |
|`$.array[*]`    |All array elements (wildcard — only valid in `kdbJsonGetAll`)     |
|`$.*`           |All fields of an object (wildcard — only valid in `kdbJsonGetAll`)|

Wildcards (`[*]`, `.*`) are not permitted in mutating functions (`kdbJsonSet`, `kdbJsonDelete`). Attempting them returns `JsonPathException`.

### KdbJsonFunctionDescriptor

Used by the SQL query planner (Layer 4) for type checking and evaluation delegation. The `evaluate` lambda receives arguments as `JsonValue?` (null for missing/null SQL column values) and returns a `JsonValue?` (null maps to SQL NULL).

-----

## 5. Contracts

### `JsonPath.compile(expression)`

- **Pre:** `expression` starts with `$`.
- **Post:** Returns a compiled `JsonPath` on valid syntax.
- **Throws:** `JsonPathException` with `path = expression` on invalid syntax.
- **Guarantee:** `compile(p).toString() == p` for any valid expression.

### `kdbJsonGet(json, path)`

- **Pre:** `json` is a valid JSON string (any type at root).
- **Post:** Returns the `JsonValue` at the resolved path, or `null` if the path does not exist. Does not return `null` for a JSON `null` value — that returns `JsonValue.JNull`.
- **Throws:** `JsonPathException` if traversal encounters a type mismatch (e.g. path indexes into a string).
- **Guarantee:** Does not modify `json`.

### `kdbJsonSet(json, path, value)`

- **Pre:** `json` is a valid JSON object string. `path` does not contain wildcards.
- **Post:** Returns a new JSON string identical to `json` except the node at `path` is replaced with `value`. Intermediate objects are created if missing.
- **Guarantee:** `kdbJsonGet(result, path) == value`.
- **Guarantee:** All other paths in `json` are unchanged.

### `kdbJsonDelete(json, path)`

- **Pre:** `json` is a valid JSON string. `path` does not contain wildcards.
- **Post:** Returns `json` unchanged if path absent. Returns new JSON string with the node at `path` removed if present.
- **Guarantee:** `kdbJsonGet(result, path) == null`.

### `kdbJsonMerge(json, patchJson)`

- **Pre:** Both arguments are valid JSON object strings.
- **Post:** Returns new JSON string. Root-level keys from `patchJson` overwrite those in `json`. Keys in `json` not present in `patchJson` are preserved. Nested values are not recursively merged.
- **Throws:** `JsonPathException` if either argument is not a JSON object.

### `kdbJsonContains(json, path, value)`

- **Pre:** `json` is valid JSON.
- **Post:** Returns `true` iff the node at `path` is a JSON array containing an element equal to `value` (deep equality via `JsonValue` equality).
- Returns `false` if path absent, resolves to `null`, or resolves to an empty array.
- **Throws:** `JsonPathException` if path resolves to a non-array, non-null node.

### `kdbJsonKeys(json, path)`

- **Pre:** `json` is valid JSON.
- **Post:** Returns the list of field names at `path` in insertion order, or `null` if path absent.
- **Throws:** `JsonPathException` if path resolves to a non-object node.

### `kdbJsonGetAll(json, path)`

- **Pre:** `json` is valid JSON. `path` may contain `[*]` or `.*` wildcards.
- **Post:** Returns all values matching the path. Empty list if nothing matches.
- **Guarantee:** Never throws for absent paths — returns empty list.

### `JsonValue` round-trip

- **Guarantee:** `JsonValue.fromJsonString(v.toJsonString()) == v` for all `JsonValue` instances (excluding floating-point edge cases `NaN`, `Infinity` — these are not valid JSON and must throw `JsonPathException`).

-----

## 6. Error Cases

|Exception                                  |When Thrown                                                                     |
|-------------------------------------------|--------------------------------------------------------------------------------|
|`JsonPathException(path = expression)`     |`JsonPath.compile` called with syntactically invalid expression                 |
|`JsonPathException(path = path.expression)`|Traversal reaches a node of unexpected type (e.g. indexing a string as an array)|
|`JsonPathException(path = "$")`            |`kdbJsonMerge` called with non-object JSON on either side                       |
|`JsonPathException(path = path.expression)`|Wildcard used in a mutating function (`kdbJsonSet`, `kdbJsonDelete`)            |
|`JsonPathException(path = path.expression)`|`kdbJsonContains` called and path resolves to non-array, non-null node          |
|`JsonPathException(path = path.expression)`|`kdbJsonKeys` called and path resolves to non-object node                       |

`kdbJsonGet` and `kdbJsonGetAll` never throw for absent paths — they return `null` / empty list. They throw only on type mismatches during traversal.

-----

## 7. Test Cases

|# |Name                           |Input                                                         |Expected                               |
|--|-------------------------------|--------------------------------------------------------------|---------------------------------------|
|1 |`get_topLevelField`            |`json={"a":1}`, `path=$.a`                                    |`JInt(1)`                              |
|2 |`get_nestedField`              |`json={"a":{"b":"hello"}}`, `path=$.a.b`                      |`JString("hello")`                     |
|3 |`get_arrayElement`             |`json={"tags":["x","y"]}`, `path=$.tags[0]`                   |`JString("x")`                         |
|4 |`get_missingPath_returnsNull`  |`json={"a":1}`, `path=$.z`                                    |`null`                                 |
|5 |`get_jsonNull`                 |`json={"a":null}`, `path=$.a`                                 |`JNull`                                |
|6 |`set_newField`                 |`json={"a":1}`, `path=$.b`, `value=JString("v")`              |`{"a":1,"b":"v"}`                      |
|7 |`set_overwriteField`           |`json={"a":1}`, `path=$.a`, `value=JInt(99)`                  |`{"a":99}`                             |
|8 |`set_createsIntermediateObject`|`json={}`, `path=$.a.b.c`, `value=JBool(true)`                |`{"a":{"b":{"c":true}}}`               |
|9 |`set_arrayElement`             |`json={"t":["a","b"]}`, `path=$.t[1]`, `value=JString("z")`   |`{"t":["a","z"]}`                      |
|10|`delete_existingField`         |`json={"a":1,"b":2}`, `path=$.a`                              |`{"b":2}`                              |
|11|`delete_missingPath_noOp`      |`json={"a":1}`, `path=$.z`                                    |`{"a":1}` unchanged                    |
|12|`merge_rootKeys`               |`json={"a":1,"b":2}`, `patch={"b":99,"c":3}`                  |`{"a":1,"b":99,"c":3}`                 |
|13|`contains_true`                |`json={"tags":["a","b"]}`, `path=$.tags`, `value=JString("a")`|`true`                                 |
|14|`contains_false`               |same doc, `value=JString("z")`                                |`false`                                |
|15|`contains_emptyArray`          |`json={"t":[]}`, `path=$.t`, `value=JString("x")`             |`false`                                |
|16|`keys_returnsFieldNames`       |`json={"a":1,"b":2}` at `$`                                   |`["a","b"]`                            |
|17|`type_string`                  |`json={"x":"hi"}` at `$.x`                                    |`"string"`                             |
|18|`type_number`                  |`json={"x":3.14}` at `$.x`                                    |`"number"`                             |
|19|`arrayLength_returns`          |`json={"t":[1,2,3]}` at `$.t`                                 |`3`                                    |
|20|`arrayLength_missingPath_null` |`json={"t":[]}` at `$.z`                                      |`null`                                 |
|21|`getAll_wildcard`              |`json={"a":1,"b":2}`, `path=$.*`                              |`[JInt(1), JInt(2)]`                   |
|22|`invalidPath_throws`           |`path="not-a-path"`                                           |`JsonPathException`                    |
|23|`set_wildcardPath_throws`      |`path=$.arr[*]`, value=anything                               |`JsonPathException`                    |
|24|`merge_nonObjectLeft_throws`   |`json="[1,2]"`, any patch                                     |`JsonPathException`                    |
|25|`jsonValue_roundTrip`          |Any `JsonValue` constructed manually                          |`fromJsonString(v.toJsonString()) == v`|

-----

## 8. Non-Goals

- **No JSONPath filter expressions** — predicates like `$.store.book[?(@.price < 10)]` are not supported. The supported subset is navigation-only (fields, indices, wildcards).
- **No recursive descent** (`$..field`) — this operator is not part of the supported path syntax.
- **No document identity** — this module operates on JSON strings only. It does not know about `KdbUuid`, `KdbDocument`, or `KdbCommit`.
- **No schema awareness** — validation of values against field type declarations belongs to the Schema Engine (Component 5, Layer 2).
- **No SQL parsing** — the SQL engine (Component 13, Layer 4) is responsible for parsing `kdb_json_get(...)` function calls in SQL text. This module provides the runtime evaluation only.
- **No index updates** — writing to a schema-declared field via `kdbJsonSet` does not update any index. Index updates are the responsibility of the Transaction Engine (Component 7, Layer 3).
- **No streaming / partial parse** — all inputs are parsed fully in memory. Large document streaming is not in scope.
- **No JSON schema validation** — JSON Schema (draft-07 etc.) is not part of this module.

-----

## 9. Implementation Notes

### JSON parser strategy

Do not use `kotlinx.serialization` for the internal parse tree — it is not designed for structural mutation and re-serialisation. Instead, build a lightweight recursive descent JSON parser that produces `JsonValue` trees directly. This gives full control over number type preservation (`JInt` vs `JNumber`), insertion-order preservation, and round-trip fidelity.

Alternatively, parse into `BsonDocument` via the existing codec and then convert via `BsonValue.toJsonValue()`. This is acceptable for a first implementation and avoids a second parser, but adds a BSON round-trip overhead for pure-JSON paths.

### Path compilation

`JsonPath.compile` should:

1. Validate that expression starts with `$`.
1. Tokenise into a `List<PathSegment>` where `PathSegment` is one of: `Root`, `Field(name)`, `Index(n)`, `Wildcard`.
1. Cache compiled paths if the same string is compiled multiple times (use a `LinkedHashMap` LRU cache capped at 256 entries).

### Mutating functions and structural sharing

`kdbJsonSet` and `kdbJsonDelete` must return a new JSON string — they never mutate the input. Internally, reconstruct the relevant sub-tree top-down (path of nodes from root to the mutation point). Unchanged sibling nodes are copied by value (not structurally shared, since we serialise to strings anyway).

### Number precision

When deserialising JSON numbers:

- If the number string contains no `.` or `e`/`E` and fits in a `Long`, emit `JInt`.
- Otherwise, emit `JNumber(Double)`.
- When serialising `JInt` back to JSON, emit the integer without decimal point.
- When serialising `JNumber`, emit with `toString()` — do not strip trailing zeros.

### Kotlin Multiplatform constraints

- All code in `commonMain`. No platform dependencies.
- Do not use `java.util.regex` — write the path tokeniser as a simple index-walking loop.
- `LinkedHashMap` is available in `commonMain`.
- The `KdbJsonFunctionRegistry` is an `object` in `commonMain` — populated at class-load time via a fixed `init` block enumerating all functions. No reflection or annotation processing.

### SQL function name mapping

|Kotlin function     |SQL name               |
|--------------------|-----------------------|
|`kdbJsonGet`        |`kdb_json_get`         |
|`kdbJsonSet`        |`kdb_json_set`         |
|`kdbJsonDelete`     |`kdb_json_delete`      |
|`kdbJsonMerge`      |`kdb_json_merge`       |
|`kdbJsonContains`   |`kdb_json_contains`    |
|`kdbJsonKeys`       |`kdb_json_keys`        |
|`kdbJsonType`       |`kdb_json_type`        |
|`kdbJsonArrayLength`|`kdb_json_array_length`|

-----

## 10. Estimated Lines

|Sub-component                                                           |NBNC lines|
|------------------------------------------------------------------------|----------|
|`JsonPath` — tokeniser + compile + validation                           |300       |
|JSON recursive descent parser → `JsonValue`                             |350       |
|`JsonValue` → JSON string serialiser                                    |150       |
|`kdbJsonGet` / `kdbJsonGetAll`                                          |200       |
|`kdbJsonSet`                                                            |200       |
|`kdbJsonDelete`                                                         |120       |
|`kdbJsonMerge`                                                          |80        |
|`kdbJsonContains` / `kdbJsonKeys` / `kdbJsonType` / `kdbJsonArrayLength`|180       |
|`BsonValue.toJsonValue()` / `JsonValue.toBsonValue()` interop           |120       |
|`KdbJsonFunctionRegistry` + descriptors                                 |150       |
|Unit tests                                                              |400       |
|**Total**                                                               |**~2,250**|

-----

## Session Instructions (for next spec or implementation session)

> **Note added per project convention:** All component specs must be saved as files for download. When generating specs, produce one `.md` file per component and present them for download before the session ends.

When implementing this component, paste the master spec (kdb-spec-v0_5.md or later) plus this file and say:

```
You are implementing KDB, a portable embedded database engine in Kotlin Multiplatform.
This document is the master architecture spec. The attached component spec is your implementation contract.
Implement Component 4: JSON Functions Engine in Kotlin Multiplatform (commonMain).
All dependencies are in Section 17 — treat those interfaces as already existing.
Produce production-quality Kotlin. No placeholders.
```

After implementation, extract the public interface (Section 3 of this spec) and paste it into Section 17 → Layer 1 of the master spec, then mark `[x] 4. JSON Functions Engine` in the checklist.