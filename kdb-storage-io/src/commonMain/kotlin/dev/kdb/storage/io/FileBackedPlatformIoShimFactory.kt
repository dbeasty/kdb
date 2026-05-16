package dev.kdb.storage.io

import dev.kdb.storage.PlatformIoShim

public expect object FileBackedPlatformIoShimFactory {
    public fun open(config: PlatformIoConfig = PlatformIoConfig()): PlatformIoShim
}

public expect class JvmFileBackedPlatformIoShim(config: PlatformIoConfig) : PlatformIoShim

public expect class NativeFileBackedPlatformIoShim(config: PlatformIoConfig) : PlatformIoShim

public expect class BrowserFileBackedPlatformIoShim(config: PlatformIoConfig) : PlatformIoShim
