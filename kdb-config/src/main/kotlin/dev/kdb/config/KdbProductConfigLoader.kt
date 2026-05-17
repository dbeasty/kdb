package dev.kdb.config

import kotlinx.serialization.json.Json
import java.nio.file.Files
import java.nio.file.Path

private val json = Json { ignoreUnknownKeys = true }

public fun loadKdbProductConfig(path: Path): KdbProductConfig =
    json.decodeFromString(KdbProductConfig.serializer(), Files.readString(path))

public fun loadKdbProductConfigOrNull(path: Path): KdbProductConfig? =
    if (Files.isRegularFile(path)) {
        loadKdbProductConfig(path)
    } else {
        null
    }

public fun resolveKdbProductConfig(
    configFile: Path?,
    dataDirConfig: Path?,
    peerSyncEnabledOverride: Boolean? = null,
    sqlWireEnabledOverride: Boolean? = null,
    listenWsOverride: String? = null,
    listenSqlWsOverride: String? = null,
    authConfigPathOverride: String? = null,
    tlsEnabledOverride: Boolean? = null,
    tlsKeyStorePathOverride: String? = null,
    tlsTrustStorePathOverride: String? = null,
    tlsRequireClientAuthOverride: Boolean? = null,
): KdbProductConfig {
    val base =
        when {
            configFile != null -> loadKdbProductConfig(configFile)
            dataDirConfig != null -> loadKdbProductConfigOrNull(dataDirConfig) ?: KdbProductConfig.DEFAULT
            else -> KdbProductConfig.DEFAULT
        }
    val peer =
        base.features.peerSync.let { peer ->
            peer.copy(
                enabled = peerSyncEnabledOverride ?: peer.enabled,
                listenUri = listenWsOverride ?: peer.listenUri,
            )
        }
    val sql =
        base.features.sqlWire.let { sql ->
            sql.copy(
                enabled = sqlWireEnabledOverride ?: sql.enabled,
                listenUri = listenSqlWsOverride ?: sql.listenUri,
            )
        }
    val tls =
        mergeTlsConfig(
            base = base.tls,
            enabled = tlsEnabledOverride,
            keyStorePath = tlsKeyStorePathOverride,
            trustStorePath = tlsTrustStorePathOverride,
            requireClientAuth = tlsRequireClientAuthOverride,
        )
    return base.copy(
        features = base.features.copy(peerSync = peer, sqlWire = sql),
        authConfigPath = authConfigPathOverride ?: base.authConfigPath,
        tls = tls,
    )
}

private fun mergeTlsConfig(
    base: KdbTlsConfig?,
    enabled: Boolean?,
    keyStorePath: String?,
    trustStorePath: String?,
    requireClientAuth: Boolean?,
): KdbTlsConfig? {
    if (base == null && enabled == null && keyStorePath == null && trustStorePath == null && requireClientAuth == null) {
        return null
    }
    val current = base ?: KdbTlsConfig()
    return current.copy(
        enabled = enabled ?: current.enabled,
        keyStorePath = keyStorePath ?: current.keyStorePath,
        trustStorePath = trustStorePath ?: current.trustStorePath,
        requireClientAuth = requireClientAuth ?: current.requireClientAuth,
    )
}

public fun defaultDataDirConfigPath(dataDir: String): Path = Path.of(dataDir, "config.json")
