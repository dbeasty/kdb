package dev.kdb.storage.io

internal object BrowserSnapshotStore {
    private val memory = mutableMapOf<String, ByteArray>()

    fun read(key: String): ByteArray? = memory[key] ?: readWeb(key)

    fun write(key: String, data: ByteArray) {
        memory[key] = data.copyOf()
        if (!writeWeb(key, data)) {
            // best-effort per Layer 3
        }
    }

    fun delete(key: String) {
        memory.remove(key)
        deleteWeb(key)
    }

    private fun readWeb(key: String): ByteArray? {
        val b64 = webGetItem("localStorage", key) ?: webGetItem("sessionStorage", key) ?: return null
        return decodeBase64(b64)
    }

    private fun writeWeb(key: String, data: ByteArray): Boolean {
        val b64 = encodeBase64(data)
        return webSetItem("localStorage", key, b64) || webSetItem("sessionStorage", key, b64)
    }

    private fun deleteWeb(key: String) {
        webRemoveItem("localStorage", key)
        webRemoveItem("sessionStorage", key)
    }

    private fun webGetItem(storage: String, key: String): String? =
        runCatching {
            when (storage) {
                "localStorage" -> localStorageGet(key)
                else -> sessionStorageGet(key)
            }
        }.getOrNull()

    private fun webSetItem(storage: String, key: String, value: String): Boolean =
        runCatching {
            when (storage) {
                "localStorage" -> localStorageSet(key, value)
                else -> sessionStorageSet(key, value)
            }
            true
        }.getOrDefault(false)

    private fun webRemoveItem(storage: String, key: String) {
        runCatching {
            when (storage) {
                "localStorage" -> localStorageRemove(key)
                else -> sessionStorageRemove(key)
            }
        }
    }
}
