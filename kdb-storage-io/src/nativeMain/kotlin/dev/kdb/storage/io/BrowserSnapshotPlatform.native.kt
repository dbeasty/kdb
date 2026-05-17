package dev.kdb.storage.io

import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

internal actual fun localStorageGet(key: String): String? = null

internal actual fun localStorageSet(key: String, value: String) {}

internal actual fun localStorageRemove(key: String) {}

internal actual fun sessionStorageGet(key: String): String? = null

internal actual fun sessionStorageSet(key: String, value: String) {}

internal actual fun sessionStorageRemove(key: String) {}

@OptIn(ExperimentalEncodingApi::class)
internal actual fun encodeBase64(data: ByteArray): String = Base64.encode(data)

@OptIn(ExperimentalEncodingApi::class)
internal actual fun decodeBase64(b64: String): ByteArray = Base64.decode(b64)
