package dev.kdb.storage.io

import dev.kdb.storage.PlatformIoShim
import kotlinx.io.files.Path
import kotlinx.io.files.SystemFileSystem

public actual object FileBackedPlatformIoShimFactory {
    public actual fun open(config: PlatformIoConfig): PlatformIoShim =
        NativeFileBackedPlatformIoShim(config)
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
