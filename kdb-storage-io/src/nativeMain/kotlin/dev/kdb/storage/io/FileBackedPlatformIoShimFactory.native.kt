@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package dev.kdb.storage.io

import dev.kdb.codec.KdbUuid
import dev.kdb.storage.PlatformIoShim
import kotlinx.cinterop.toKString
import kotlinx.io.files.Path
import kotlinx.io.files.SystemFileSystem
import platform.posix.getenv

public actual object FileBackedPlatformIoShimFactory {
    // Mirrors the JVM factory's fallback (java.io.tmpdir + a unique suffix) when no
    // rootDirectory is configured - without this, NativeFileBackedPlatformIoShim's constructor
    // (requireNotNull(config.rootDirectory)) throws immediately, so callers on this platform
    // could never rely on the same "just open one, I don't care where" convenience the JVM and
    // browser factories already offer.
    public actual fun open(config: PlatformIoConfig): PlatformIoShim {
        val root = config.rootDirectory ?: defaultNativeTempRoot()
        return NativeFileBackedPlatformIoShim(config.copy(rootDirectory = root))
    }

    private fun defaultNativeTempRoot(): String {
        val base = getenv("TMPDIR")?.toKString()?.trimEnd('/')?.takeIf { it.isNotBlank() } ?: "/tmp"
        return "$base/kdb-data-${KdbUuid.random()}"
    }
}

public actual class JvmFileBackedPlatformIoShim actual constructor(config: PlatformIoConfig) :
    NativeFileBackedPlatformIoShim(config),
    PlatformIoShim

public actual open class NativeFileBackedPlatformIoShim actual constructor(config: PlatformIoConfig) :
    FileBackedPlatformIoShimBase(
        config,
        NativeSegmentByteStore(
            Path(requireNotNull(config.rootDirectory) { "rootDirectory required" }),
        ),
    ),
    PlatformIoShim

public actual class BrowserFileBackedPlatformIoShim actual constructor(config: PlatformIoConfig) :
    NativeFileBackedPlatformIoShim(config),
    PlatformIoShim
