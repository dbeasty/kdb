package dev.kdb.index.vector

import dev.kdb.json.JsonValue
import java.io.File
import java.nio.file.Paths
import kotlin.io.path.isDirectory

/**
 * Locates the shared Layer 16 search fixtures under `go/testdata/golden/search`, walking up from
 * the working directory the way `ExportGoldenTest` does. The Go tree authors these files; the
 * Kotlin tests only read them.
 */
public object GoldenFixtures {
    public val directory: File? by lazy {
        val start = Paths.get(System.getProperty("user.dir"))
        generateSequence(start) { it.parent }
            .firstOrNull { it.resolve("go/testdata/golden/search").isDirectory() }
            ?.resolve("go/testdata/golden/search")
            ?.toFile()
    }

    /** The fixture file, or null when it has not been authored yet. */
    public fun file(name: String): File? = directory?.resolve(name)?.takeIf { it.isFile }

    public fun json(name: String): JsonValue? = file(name)?.let { JsonValue.fromJsonString(it.readText()) }

    public fun text(name: String): String? = file(name)?.readText()

    /** Message for a test that has to skip because the fixture is absent. */
    public fun missing(name: String): String =
        "fixture $name not found under ${directory?.path ?: "go/testdata/golden/search"} — skipping parity assertions"
}

// ------------------------------------------------------------------ JSON helpers

public fun JsonValue.arr(): List<JsonValue> = (this as JsonValue.JArray).elements

public fun JsonValue.obj(): Map<String, JsonValue> = (this as JsonValue.JObject).fields

public fun JsonValue.field(name: String): JsonValue? = obj()[name]?.takeIf { it !== JsonValue.JNull }

public fun JsonValue.str(): String = (this as JsonValue.JString).value

public fun JsonValue.num(): Double =
    when (this) {
        is JsonValue.JNumber -> value
        is JsonValue.JInt -> value.toDouble()
        else -> error("expected a number, got $this")
    }

public fun JsonValue.int(): Int = num().toInt()
