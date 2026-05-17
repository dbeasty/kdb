package dev.kdb.file

public enum class FileEncoding {
    RAW,
    ZIP,
    ;

    public fun wireName(): String =
        when (this) {
            RAW -> "raw"
            ZIP -> "zip"
        }

    public companion object {
        public fun fromWire(name: String): FileEncoding =
            when (name.lowercase()) {
                "raw" -> RAW
                "zip" -> ZIP
                else -> throw IllegalArgumentException("unknown file encoding: $name")
            }
    }
}
