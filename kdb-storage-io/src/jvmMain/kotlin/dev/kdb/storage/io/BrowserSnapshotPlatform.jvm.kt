package dev.kdb.storage.io

import java.util.Base64

internal actual fun localStorageGet(key: String): String? = null

internal actual fun localStorageSet(key: String, value: String) {}

internal actual fun localStorageRemove(key: String) {}

internal actual fun sessionStorageGet(key: String): String? = null

internal actual fun sessionStorageSet(key: String, value: String) {}

internal actual fun sessionStorageRemove(key: String) {}

internal actual fun encodeBase64(data: ByteArray): String = Base64.getEncoder().encodeToString(data)

internal actual fun decodeBase64(b64: String): ByteArray = Base64.getDecoder().decode(b64)
