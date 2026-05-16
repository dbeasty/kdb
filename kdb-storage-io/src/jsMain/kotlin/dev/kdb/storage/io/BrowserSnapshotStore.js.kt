package dev.kdb.storage.io

internal actual fun localStorageGet(key: String): String? =
    js("typeof localStorage !== 'undefined' ? localStorage.getItem(key) : null") as String?

internal actual fun localStorageSet(key: String, value: String) {
    js("if (typeof localStorage !== 'undefined') localStorage.setItem(key, value)")
}

internal actual fun localStorageRemove(key: String) {
    js("if (typeof localStorage !== 'undefined') localStorage.removeItem(key)")
}

internal actual fun sessionStorageGet(key: String): String? =
    js("typeof sessionStorage !== 'undefined' ? sessionStorage.getItem(key) : null") as String?

internal actual fun sessionStorageSet(key: String, value: String) {
    js("if (typeof sessionStorage !== 'undefined') sessionStorage.setItem(key, value)")
}

internal actual fun sessionStorageRemove(key: String) {
    js("if (typeof sessionStorage !== 'undefined') sessionStorage.removeItem(key)")
}

@Suppress("UNUSED_PARAMETER")
internal actual fun encodeBase64(data: ByteArray): String {
    var binary = ""
    for (b in data) binary += Char(b.toUShort()).toString()
    return js("btoa(binary)") as String
}

@Suppress("UNUSED_PARAMETER")
internal actual fun decodeBase64(b64: String): ByteArray {
    val binary = js("atob(b64)") as String
    return ByteArray(binary.length) { i -> binary[i].code.toByte() }
}
