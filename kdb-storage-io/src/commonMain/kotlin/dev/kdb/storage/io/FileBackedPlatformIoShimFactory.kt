package dev.kdb.storage.io

import dev.kdb.storage.PlatformIoShim

public expect object FileBackedPlatformIoShimFactory {
    public fun open(config: PlatformIoConfig = PlatformIoConfig()): PlatformIoShim
}

// Every actual extends FileBackedPlatformIoShimBase, and the expect declarations must say so: the
// common-metadata compilation checks an expect class on its own, so one that names only
// PlatformIoShim reads there as a concrete class implementing none of its members.
public expect class JvmFileBackedPlatformIoShim(config: PlatformIoConfig) :
    FileBackedPlatformIoShimBase, PlatformIoShim

public expect class NativeFileBackedPlatformIoShim(config: PlatformIoConfig) :
    FileBackedPlatformIoShimBase, PlatformIoShim

public expect class BrowserFileBackedPlatformIoShim(config: PlatformIoConfig) :
    FileBackedPlatformIoShimBase, PlatformIoShim
