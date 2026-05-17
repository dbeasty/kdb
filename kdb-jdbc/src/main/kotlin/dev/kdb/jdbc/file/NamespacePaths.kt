package dev.kdb.jdbc.file

import java.nio.file.Files
import java.nio.file.Path

public object NamespacePaths {
    public fun nsDir(dataRoot: String, namespaceId: String): Path =
        Path.of(dataRoot, "ns", namespaceId)

    public fun metaFile(dataRoot: String, namespaceId: String): Path =
        nsDir(dataRoot, namespaceId).resolve("meta.json")

    public fun ensureDirs(dataRoot: String, namespaceId: String): Path {
        val dir = nsDir(dataRoot, namespaceId)
        Files.createDirectories(dir)
        return dir
    }

    public fun catalogFromNamespace(namespaceId: String): String =
        namespaceId.substringBefore('/').ifEmpty { "default" }
}
