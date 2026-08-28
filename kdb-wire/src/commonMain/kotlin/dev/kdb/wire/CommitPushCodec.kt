package dev.kdb.wire

import dev.kdb.document.KdbCommit

/** Length-prefixed commit list for [WireMessage.CommitPush] payloads. */
public object CommitPushCodec {
    public fun encodeCommits(commits: List<KdbCommit>): ByteArray {
        var size = 4
        val payloads = commits.map { it.toPayloadBytes() }
        for (p in payloads) {
            size += 4 + p.size
        }
        val out = ByteArray(size)
        var o = 0
        writeIntLe(out, o, commits.size)
        o += 4
        for (p in payloads) {
            writeIntLe(out, o, p.size)
            o += 4
            p.copyInto(out, o)
            o += p.size
        }
        return out
    }

    public fun decodeCommits(bytes: ByteArray): List<KdbCommit> {
        if (bytes.size < 4) return emptyList()
        var o = 0
        val count = readIntLe(bytes, o)
        o += 4
        // The count comes from a peer, so it cannot size the allocation on its own: every commit
        // costs at least its own 4-byte length prefix, so a payload of bytes.size can back at
        // most (bytes.size - 4) / 4 of them. Without this bound a four-byte CommitPush declaring
        // Int.MAX_VALUE commits asked ArrayList for a two-billion-element backing array before
        // discovering, one iteration later, that there were no commit bodies at all - an
        // OutOfMemoryError rather than a decode error, reachable by any peer that can send a
        // commitPush frame. A negative count (a length prefix with the high bit set) took out
        // ArrayList's own argument check instead. Go's DecodeCommits enforces the same bound.
        val maxPossible = (bytes.size - 4) / 4
        require(count in 0..maxPossible) {
            "commit push declares $count commits, payload can hold at most $maxPossible"
        }
        val result = ArrayList<KdbCommit>(count)
        repeat(count) {
            require(o + 4 <= bytes.size) { "truncated commit push payload" }
            val len = readIntLe(bytes, o)
            o += 4
            require(o + len <= bytes.size) { "truncated commit bytes" }
            result += KdbCommit.fromPayloadBytes(bytes.copyOfRange(o, o + len))
            o += len
        }
        return result
    }

    private fun writeIntLe(
        buf: ByteArray,
        offset: Int,
        value: Int,
    ) {
        buf[offset] = (value and 0xff).toByte()
        buf[offset + 1] = ((value shr 8) and 0xff).toByte()
        buf[offset + 2] = ((value shr 16) and 0xff).toByte()
        buf[offset + 3] = ((value shr 24) and 0xff).toByte()
    }

    private fun readIntLe(
        buf: ByteArray,
        offset: Int,
    ): Int =
        (buf[offset].toInt() and 0xff) or
            ((buf[offset + 1].toInt() and 0xff) shl 8) or
            ((buf[offset + 2].toInt() and 0xff) shl 16) or
            ((buf[offset + 3].toInt() and 0xff) shl 24)
}
