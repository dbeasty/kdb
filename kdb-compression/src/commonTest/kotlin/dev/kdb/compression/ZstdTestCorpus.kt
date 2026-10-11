package dev.kdb.compression

/**
 * Deterministic payloads for the zstd golden corpus. Go generates the same bytes
 * (zstdCorpusJSON in go/kdb/interop/delta_physical_golden_test.go); keep the two in lockstep.
 */
object ZstdTestCorpus {
    /**
     * JSON-like records drawn from a 32-bit LCG, truncated to exactly [size] bytes: compressible
     * enough for Huffman literals, FSE tables and repeat offsets, varied enough to need them.
     */
    fun json(size: Int): ByteArray {
        var x = 0x12345678
        fun next(): Int {
            x = x * 1664525 + 1013904223
            return x ushr 16
        }
        val sb = StringBuilder(size + 200)
        var i = 0
        while (sb.length < size) {
            sb.append("{\"id\":").append(i++)
            sb.append(",\"name\":\"user-").append(next() % 1000)
            sb.append("\",\"tags\":[\"t").append(next() % 17).append("\",\"t").append(next() % 17)
            sb.append("\"],\"score\":").append(next() % 100000)
            sb.append(",\"active\":").append(next() % 2 == 0).append("}\n")
        }
        return sb.substring(0, size).encodeToByteArray()
    }
}
