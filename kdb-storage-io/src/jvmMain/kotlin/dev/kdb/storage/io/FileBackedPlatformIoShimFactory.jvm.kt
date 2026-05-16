package dev.kdb.storage.io

import dev.kdb.storage.PlatformIoShim
import java.io.File

public actual object FileBackedPlatformIoShimFactory {
    public actual fun open(config: PlatformIoConfig): PlatformIoShim {
        val root =
            config.rootDirectory?.let { File(it) }
                ?: File(System.getProperty("java.io.tmpdir"), "kdb-data-${System.nanoTime()}")
        root.mkdirs()
        return JvmFileBackedPlatformIoShim(config.copy(rootDirectory = root.absolutePath))
    }
}

public actual class JvmFileBackedPlatformIoShim actual constructor(config: PlatformIoConfig) :
    FileBackedPlatformIoShimBase(
        config,
        JvmSegmentByteStore(File(requireNotNull(config.rootDirectory) { "rootDirectory required" })),
    ),
    PlatformIoShim

public actual class NativeFileBackedPlatformIoShim actual constructor(config: PlatformIoConfig) :
    FileBackedPlatformIoShimBase(
        config,
        JvmSegmentByteStore(File(requireNotNull(config.rootDirectory) { "rootDirectory required" })),
    ),
    PlatformIoShim

public actual class BrowserFileBackedPlatformIoShim actual constructor(config: PlatformIoConfig) :
    FileBackedPlatformIoShimBase(
        config,
        JvmSegmentByteStore(File(requireNotNull(config.rootDirectory) { "rootDirectory required" })),
    ),
    PlatformIoShim
