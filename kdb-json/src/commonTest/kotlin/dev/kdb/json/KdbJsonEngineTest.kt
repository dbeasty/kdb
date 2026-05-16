package dev.kdb.json

import dev.kdb.error.JsonPathException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class KdbJsonEngineTest {

    @Test
    fun get_topLevelField() {
        assertEquals(JsonValue.JInt(1), kdbJsonGet("""{"a":1}""", "$.a"))
    }

    @Test
    fun get_nestedField() {
        assertEquals(JsonValue.JString("hello"), kdbJsonGet("""{"a":{"b":"hello"}}""", "$.a.b"))
    }

    @Test
    fun get_arrayElement() {
        assertEquals(JsonValue.JString("x"), kdbJsonGet("""{"tags":["x","y"]}""", "$.tags[0]"))
    }

    @Test
    fun get_missingPath_returnsNull() {
        assertNull(kdbJsonGet("""{"a":1}""", "$.z"))
    }

    @Test
    fun get_jsonNull() {
        assertEquals(JsonValue.JNull, kdbJsonGet("""{"a":null}""", "$.a"))
    }

    @Test
    fun set_newField() {
        val out = kdbJsonSet("""{"a":1}""", "$.b", JsonValue.JString("v"))
        assertEquals("""{"a":1,"b":"v"}""", out)
    }

    @Test
    fun set_overwriteField() {
        val out = kdbJsonSet("""{"a":1}""", "$.a", JsonValue.JInt(99))
        assertEquals("""{"a":99}""", out)
    }

    @Test
    fun set_createsIntermediateObject() {
        val out = kdbJsonSet("""{}""", "$.a.b.c", JsonValue.JBool(true))
        assertEquals("""{"a":{"b":{"c":true}}}""", out)
    }

    @Test
    fun set_arrayElement() {
        val out = kdbJsonSet("""{"t":["a","b"]}""", "$.t[1]", JsonValue.JString("z"))
        assertEquals("""{"t":["a","z"]}""", out)
    }

    @Test
    fun delete_existingField() {
        val out = kdbJsonDelete("""{"a":1,"b":2}""", "$.a")
        assertEquals("""{"b":2}""", out)
    }

    @Test
    fun delete_missingPath_noOp() {
        val doc = """{"a":1}"""
        assertEquals(doc, kdbJsonDelete(doc, "$.z"))
    }

    @Test
    fun merge_rootKeys() {
        val out = kdbJsonMerge("""{"a":1,"b":2}""", """{"b":99,"c":3}""")
        assertEquals("""{"a":1,"b":99,"c":3}""", out)
    }

    @Test
    fun contains_true() {
        assertTrue(
            kdbJsonContains(
                """{"tags":["a","b"]}""",
                "$.tags",
                JsonValue.JString("a"),
            ),
        )
    }

    @Test
    fun contains_false() {
        assertTrue(
            !kdbJsonContains(
                """{"tags":["a","b"]}""",
                "$.tags",
                JsonValue.JString("z"),
            ),
        )
    }

    @Test
    fun contains_emptyArray() {
        assertTrue(
            !kdbJsonContains(
                """{"t":[]}""",
                "$.t",
                JsonValue.JString("x"),
            ),
        )
    }

    @Test
    fun keys_returnsFieldNames() {
        assertEquals(
            listOf("a", "b"),
            kdbJsonKeys("""{"a":1,"b":2}""", "$"),
        )
    }

    @Test
    fun type_string() {
        assertEquals("string", kdbJsonType("""{"x":"hi"}""", "$.x"))
    }

    @Test
    fun type_number() {
        assertEquals("number", kdbJsonType("""{"x":3.14}""", "$.x"))
    }

    @Test
    fun arrayLength_returns() {
        assertEquals(3, kdbJsonArrayLength("""{"t":[1,2,3]}""", "$.t"))
    }

    @Test
    fun arrayLength_missingPath_null() {
        assertNull(kdbJsonArrayLength("""{"t":[]}""", "$.z"))
    }

    @Test
    fun getAll_wildcard() {
        val all = kdbJsonGetAll("""{"a":1,"b":2}""", "$.*")
        assertEquals(listOf(JsonValue.JInt(1), JsonValue.JInt(2)), all)
    }

    @Test
    fun invalidPath_throws() {
        assertFailsWith<JsonPathException> {
            JsonPath.compile("not-a-path")
        }
    }

    @Test
    fun set_wildcardPath_throws() {
        assertFailsWith<JsonPathException> {
            kdbJsonSet("""{"arr":[1]}""", "$.arr[*]", JsonValue.JInt(1))
        }
    }

    @Test
    fun merge_nonObjectLeft_throws() {
        assertFailsWith<JsonPathException> {
            kdbJsonMerge("""[1,2]""", """{}""")
        }
    }

    @Test
    fun jsonValue_roundTrip() {
        val v =
            JsonValue.JObject(
                linkedMapOf(
                    "a" to JsonValue.JArray(listOf(JsonValue.JInt(1), JsonValue.JString("z"))),
                    "b" to JsonValue.JNull,
                ),
            )
        val back = JsonValue.fromJsonString(v.toJsonString())
        assertEquals(v, back)
    }

    @Test
    fun jsonValue_kdb_roundTrip() {
        val v =
            JsonValue.JObject(
                linkedMapOf("n" to JsonValue.JInt(42), "f" to JsonValue.JNumber(1.5)),
            )
        val back = JsonValue.fromKdbValue(v.toKdbValue())
        assertEquals(v, back)
    }

    @Test
    fun registry_lookup_case_insensitive() {
        assertEquals("kdb_json_get", KdbJsonFunctionRegistry.get("KDB_JSON_GET")?.sqlName)
    }
}
