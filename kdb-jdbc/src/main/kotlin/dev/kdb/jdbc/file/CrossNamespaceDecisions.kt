package dev.kdb.jdbc.file

import dev.kdb.document.KdbCommit
import java.io.File
import java.nio.ByteBuffer
import java.util.zip.CRC32C

/**
 * Reads the cross-namespace transaction state a Go writer keeps under `<dataRoot>/txn/`, so a
 * Kotlin replay of a data root the Go runtime wrote decides the same way Go's does - see
 * docs/kdb-cross-namespace-transactions-plan.md §4 and go/kdb/embed/txn_coordinator.go.
 *
 * The Kotlin reference does not *write* cross-namespace transactions: it has no multi-namespace
 * host to run them on. But its delta replay reads the same logs, and a log can hold the parts of a
 * group whose Go writer crashed before deciding it. Read as ordinary commits, those parts would
 * come back - in one namespace, and not in the other it was supposed to commit with. This is the
 * read side of the protocol, byte-compatible with Go's (pinned by TxnDecisionsGoldenTest).
 *
 * Kotlin takes the part of a writer that has just opened the root: an epoch whose decision file is
 * still live (`.log`) or dead (`.dead`) and does not record a group means that group did not
 * commit. It never renames or writes anything under `txn/`.
 */
public class CrossNamespaceDecisions(private val dataRoot: String) {
    /** What replay does with one commit. */
    public enum class Decision { NOT_A_PART, COMMITTED, ABORTED }

    private val host: String by lazy {
        File(txnDir(), HOST_FILE).takeIf { it.isFile }?.readText()?.trim().orEmpty()
    }
    private val epochs = HashMap<Long, Set<String>?>()

    public fun resolve(commit: KdbCommit): Decision {
        val marker = parseMarker(commit.message) ?: return Decision.NOT_A_PART
        // A part written under another data root's log - peer sync, a copied log - is decided by
        // that root, not this one.
        if (marker.host != host) return Decision.COMMITTED
        val committed =
            epochs.getOrPut(marker.epoch) {
                val live = File(txnDir(), decisionFileName(marker.epoch, LIVE_SUFFIX))
                val dead = File(txnDir(), decisionFileName(marker.epoch, DEAD_SUFFIX))
                when {
                    live.isFile -> readDecisionFile(live.readBytes())
                    dead.isFile -> readDecisionFile(dead.readBytes())
                    else -> null // sealed: every group of the epoch committed
                }
            } ?: return Decision.COMMITTED
        return if (marker.group in committed) Decision.COMMITTED else Decision.ABORTED
    }

    private fun txnDir(): File = File(dataRoot, TXN_DIR)

    /** The group a part commit belongs to, as its commit message records it. */
    public data class Marker(val host: String, val epoch: Long, val group: String, val parts: List<String>)

    public companion object {
        public const val TXN_DIR: String = "txn"
        public const val HOST_FILE: String = "HOST"
        public const val LIVE_SUFFIX: String = ".log"
        public const val DEAD_SUFFIX: String = ".dead"
        private const val MARKER_PREFIX = "kdb:xns/1 "
        private const val HEADER_MAGIC = "KDBXNSD1"
        private const val HEADER_SIZE = 16
        private const val RECORD_SIZE = 20

        public fun decisionFileName(epoch: Long, suffix: String): String = "decisions-" + epoch.toString().padStart(16, '0') + suffix

        /** Parses Go's GroupMarker message; null for every ordinary commit. */
        public fun parseMarker(message: String): Marker? {
            if (!message.startsWith(MARKER_PREFIX)) return null
            var host = ""
            var epoch: Long? = null
            var group: String? = null
            var parts = emptyList<String>()
            for (field in message.removePrefix(MARKER_PREFIX).split(' ').filter { it.isNotEmpty() }) {
                val eq = field.indexOf('=')
                if (eq < 0) continue
                val value = field.substring(eq + 1)
                when (field.substring(0, eq)) {
                    "host" -> host = value
                    "epoch" -> epoch = value.toLongOrNull() ?: return null
                    "group" -> group = value.lowercase()
                    "parts" -> parts = if (value.isEmpty()) emptyList() else value.split(',')
                }
            }
            return Marker(host, epoch ?: return null, group ?: return null, parts)
        }

        /**
         * The group ids a decision file records. A short or corrupt record ends the read: it can
         * only be the torn tail of a write that was never acknowledged.
         */
        public fun readDecisionFile(bytes: ByteArray): Set<String> {
            if (bytes.size < HEADER_SIZE) return emptySet()
            require(String(bytes, 0, 8, Charsets.US_ASCII) == HEADER_MAGIC) { "not a cross-namespace decision log" }
            val out = HashSet<String>()
            var off = HEADER_SIZE
            while (off + RECORD_SIZE <= bytes.size) {
                val crc = CRC32C().apply { update(bytes, off, 16) }.value.toInt()
                val stored = ByteBuffer.wrap(bytes, off + 16, 4).int
                if (crc != stored) break
                val buf = ByteBuffer.wrap(bytes, off, 16)
                out += java.util.UUID(buf.long, buf.long).toString()
                off += RECORD_SIZE
            }
            return out
        }
    }
}
