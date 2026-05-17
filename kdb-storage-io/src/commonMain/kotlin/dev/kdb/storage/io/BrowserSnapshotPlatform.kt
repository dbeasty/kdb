package dev.kdb.storage.io

internal expect fun localStorageGet(key: String): String?

internal expect fun localStorageSet(key: String, value: String)

internal expect fun localStorageRemove(key: String)

internal expect fun sessionStorageGet(key: String): String?

internal expect fun sessionStorageSet(key: String, value: String)

internal expect fun sessionStorageRemove(key: String)

internal expect fun encodeBase64(data: ByteArray): String

internal expect fun decodeBase64(b64: String): ByteArray
