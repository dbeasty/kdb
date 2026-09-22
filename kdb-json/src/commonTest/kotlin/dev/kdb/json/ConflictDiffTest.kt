package dev.kdb.json

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The same cases as Go's `kdb/json/diff_test.go`, so the two implementations are held to one
 * description of what a conflict's fields are.
 */
class ConflictDiffTest {
    private fun summary(
        base: String?,
        local: String?,
        remote: String?,
    ): String =
        diffConflict(
            base?.let { JsonValue.fromJsonString(it) },
            local?.let { JsonValue.fromJsonString(it) },
            remote?.let { JsonValue.fromJsonString(it) },
        ).joinToString(" ") { "${it.path}:${it.kind}" }

    @Test
    fun classifiesEachSide() {
        assertEquals(
            "/a:local /b:remote /c:same /d:both",
            summary("""{"a":1,"b":2,"c":3,"d":4}""", """{"a":9,"b":2,"c":8,"d":7}""", """{"a":1,"b":5,"c":8,"d":6}"""),
        )
    }

    @Test
    fun omitsUnchangedFields() {
        assertEquals("/b:local", summary("""{"a":1,"b":2}""", """{"a":1,"b":3}""", """{"a":1,"b":2}"""))
    }

    @Test
    fun recursesIntoObjectsOnly() {
        assertEquals(
            "/arr:local /o/x:local /o/y:remote",
            summary(
                """{"o":{"x":1,"y":1},"arr":[1,2,3]}""",
                """{"o":{"x":2,"y":1},"arr":[1,2,3,4]}""",
                """{"o":{"x":1,"y":3},"arr":[1,2,3]}""",
            ),
        )
    }

    @Test
    fun stopsRecursingWhenASideIsNotAnObject() {
        assertEquals("/o:both", summary("""{"o":{"x":1}}""", """{"o":{"x":2}}""", """{"o":"scalar"}"""))
    }

    @Test
    fun separatesAbsentFromNull() {
        val changes =
            diffConflict(
                JsonValue.fromJsonString("""{"a":1}"""),
                JsonValue.fromJsonString("""{"a":null}"""),
                JsonValue.fromJsonString("{}"),
            )
        assertEquals(1, changes.size)
        assertEquals(ChangeKind.BOTH, changes[0].kind)
        assertTrue(changes[0].localPresent, "local set the field to null, so it is present")
        assertEquals(JsonValue.JNull, changes[0].local)
        assertFalse(changes[0].remotePresent, "remote deleted the field, so it is absent")
    }

    @Test
    fun wholeDocumentDeleteIsOneChangeAtTheRoot() {
        val changes = diffConflict(JsonValue.fromJsonString("""{"a":1}"""), null, JsonValue.fromJsonString("""{"a":2}"""))
        assertEquals(1, changes.size)
        assertEquals("", changes[0].path)
        assertEquals(ChangeKind.BOTH, changes[0].kind)
        assertFalse(changes[0].localPresent)
    }

    @Test
    fun fieldOrderIsNotAChange() {
        assertEquals("", summary("""{"a":1,"b":2}""", """{"b":2,"a":1}""", """{"a":1,"b":2}"""))
    }

    @Test
    fun pathsAreEscaped() {
        assertEquals(
            "/a~1b:local /c~0d:local",
            summary("""{"a/b":1,"c~d":1}""", """{"a/b":2,"c~d":2}""", """{"a/b":1,"c~d":1}"""),
        )
    }

    @Test
    fun orderIsByUtf8Bytes() {
        assertEquals(
            "/a:local /z:local /é:local",
            summary("""{"z":1,"é":1,"a":1}""", """{"z":2,"é":2,"a":2}""", """{"z":1,"é":1,"a":1}"""),
        )
        // An astral character sorts after every BMP one by UTF-8 bytes (it starts 0xF0) and
        // before some of them by UTF-16 code unit (its lead surrogate is 0xD83D). This is the
        // case Kotlin's own String.compareTo gets differently from Go's, so it is the one worth
        // naming: the two runtimes would otherwise hand a procedure its fields in different
        // orders.
        assertTrue(compareUtf8("\uD83D\uDE00", "\uFFFD") > 0)
        assertTrue("\uD83D\uDE00" < "\uFFFD", "Kotlin's own comparison disagrees, which is why it is not used")
    }

    @Test
    fun canonicalTextSortsKeys() {
        val a = canonicalText(JsonValue.fromJsonString("""{"b":{"d":1,"c":2},"a":3}"""))
        assertEquals(canonicalText(JsonValue.fromJsonString("""{"a":3,"b":{"c":2,"d":1}}""")), a)
        assertEquals("""{"a":3,"b":{"c":2,"d":1}}""", a)
    }
}
