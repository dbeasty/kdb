package dev.kdb.replica

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.nio.file.Files
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/** The repository root: the nearest ancestor of the working directory holding `go/`. */
internal val repoRoot: File by lazy {
    val start = File("").absoluteFile
    generateSequence(start) { it.parentFile }.firstOrNull { File(it, "go").isDirectory && File(it, "packages").isDirectory }
        ?: error("cannot find the repository root above $start")
}

/**
 * The fixtures `go run ./cmd/kdb-tsfixtures` writes for the TS replica
 * (packages/kdb-replica/test/golden). They come from the Go code a node runs, so agreeing with
 * them is agreeing with a live host; both replicas read the same files.
 */
internal fun golden(name: String): JsonElement =
    wireJson.parseToJsonElement(File(repoRoot, "packages/kdb-replica/test/golden/$name").readText())

internal val JsonElement.obj: JsonObject get() = jsonObject
internal val JsonElement.arr: JsonArray get() = jsonArray

internal fun JsonObject.s(k: String): String = this[k]?.jsonPrimitive?.contentOrNull.orEmpty()

internal fun goAvailable(): Boolean =
    try {
        ProcessBuilder("go", "version").redirectErrorStream(true).start().let {
            it.inputStream.readAllBytes()
            it.waitFor(30, TimeUnit.SECONDS) && it.exitValue() == 0
        }
    } catch (_: Exception) {
        false
    }

/**
 * A real Go node for the live interop tests: go/cmd/kdb-replicahost, built once per JVM and spawned
 * per test class (as packages/kdb-replica/test/host.ts does). It serves peer sync v2 over WebSocket
 * with token auth (rw-token, ro-token), and answers JSON commands on stdin, one line each, so a
 * test can write to the host and read its state back.
 */
internal class GoHost private constructor(private val proc: Process, private val dir: File, val url: String, private val lines: LinkedBlockingQueue<String>) {
    companion object {
        private val binary: File by lazy {
            val out = File(Files.createTempDirectory("kdb-replicahost-").toFile(), "kdb-replicahost")
            val p =
                ProcessBuilder("go", "build", "-o", out.absolutePath, "./cmd/kdb-replicahost")
                    .directory(File(repoRoot, "go"))
                    .redirectErrorStream(true)
                    .start()
            val log = p.inputStream.readAllBytes().decodeToString()
            check(p.waitFor(600, TimeUnit.SECONDS) && p.exitValue() == 0) { "go build kdb-replicahost failed:\n$log" }
            out.deleteOnExit()
            out
        }

        /**
         * Starts a host on a fresh data directory: file-backed like a deployed node, which matters -
         * the generation numbers squash's backlog comes from are a file-graph feature.
         */
        fun start(): GoHost {
            val dir = Files.createTempDirectory("kdb-replicahost-data-").toFile()
            val proc =
                ProcessBuilder(binary.absolutePath, "-dir", dir.absolutePath)
                    .redirectError(ProcessBuilder.Redirect.INHERIT)
                    .start()
            val lines = LinkedBlockingQueue<String>()
            Thread {
                BufferedReader(InputStreamReader(proc.inputStream)).useLines { seq -> seq.forEach { lines.put(it) } }
            }.apply {
                isDaemon = true
                start()
            }
            val ready = lines.poll(60, TimeUnit.SECONDS) ?: error("kdb-replicahost: no ready line in 60s")
            val m = Regex("^ready (ws://\\S+)$").find(ready) ?: error("kdb-replicahost: unexpected first line $ready")
            return GoHost(proc, dir, m.groupValues[1], lines)
        }
    }

    @Synchronized
    fun cmd(c: JsonObject): JsonObject {
        proc.outputStream.write((wireJson.encodeToString(JsonObject.serializer(), c) + "\n").toByteArray())
        proc.outputStream.flush()
        val line = lines.poll(120, TimeUnit.SECONDS) ?: error("kdb-replicahost: no answer to ${c.s("op")} in 120s")
        val res = wireJson.parseToJsonElement(line).obj
        res["error"]?.let { if (it is JsonPrimitive && it.isString) error("kdb-replicahost ${c.s("op")}: ${it.content}") }
        return res
    }

    fun cmd(op: String, vararg fields: Pair<String, Any>): JsonObject =
        cmd(
            buildJsonObject {
                put("op", JsonPrimitive(op))
                for ((k, v) in fields) {
                    put(
                        k,
                        when (v) {
                            is JsonElement -> v
                            is Number -> JsonPrimitive(v)
                            is Boolean -> JsonPrimitive(v)
                            else -> JsonPrimitive(v.toString())
                        },
                    )
                }
            },
        )

    fun put(ns: String, id: String, json: String): String = cmd("put", "ns" to ns, "id" to id, "json" to json).s("head")

    fun head(ns: String): Pair<String, String> = cmd("head", "ns" to ns).let { it.s("head") to it.s("tree") }

    fun docs(ns: String): Map<String, String> =
        cmd("docs", "ns" to ns)["docs"]!!.obj.mapValues { it.value.jsonPrimitive.content }

    /** Puts (or replaces) the attachment [name] on a document at the host; returns the new head. */
    fun attach(ns: String, id: String, name: String, text: String, mime: String = "text/plain"): String =
        cmd("attach", "ns" to ns, "id" to id, "name" to name, "mime" to mime, "text" to text).s("head")

    fun detach(ns: String, id: String, name: String): String = cmd("detach", "ns" to ns, "id" to id, "name" to name).s("head")

    /** The document's attachments at the host's head, as (name, content hash, size). */
    fun attachments(ns: String, id: String): List<Triple<String, String, Long>> =
        cmd("attachments", "ns" to ns, "id" to id)["attachments"]!!.arr.map {
            val o = it.obj
            Triple(o.s("name"), o.s("hash"), o.s("size").toLong())
        }

    fun stop() {
        try {
            proc.outputStream.close()
        } catch (_: Exception) {
        }
        if (!proc.waitFor(5, TimeUnit.SECONDS)) proc.destroyForcibly().waitFor()
        dir.deleteRecursively()
    }
}

internal suspend fun docsOf(r: Replica, ns: String): Map<String, String> = r.scan(ns).associate { it.id to it.json }

/** Polls [ok] every 50 ms for up to [timeoutMs]. */
internal suspend fun until(what: String, timeoutMs: Long = 10_000, ok: suspend () -> Boolean) {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (System.currentTimeMillis() < deadline) {
        if (ok()) return
        kotlinx.coroutines.delay(50)
    }
    throw AssertionError("timed out waiting for $what")
}
