package dev.kdb.service

import dev.kdb.config.KdbTlsConfig
import dev.kdb.transport.core.TransportConnectOptions
import dev.kdb.transport.core.TransportTlsSettings

internal fun transportOptionsForProduct(tls: KdbTlsConfig?): TransportConnectOptions =
    TransportConnectOptions(tls = tls?.toTransportTlsSettings())

internal fun KdbTlsConfig.toTransportTlsSettings(): TransportTlsSettings? {
    if (!enabled) return null
    return TransportTlsSettings(
        enabled = true,
        keyStorePath = keyStorePath,
        keyStorePassword = keyStorePassword ?: System.getenv(ENV_KEYSTORE_PASSWORD),
        keyStoreType = keyStoreType,
        trustStorePath = trustStorePath,
        trustStorePassword = trustStorePassword ?: System.getenv(ENV_TRUSTSTORE_PASSWORD),
        trustStoreType = trustStoreType,
        requireClientAuth = requireClientAuth,
    )
}

private const val ENV_KEYSTORE_PASSWORD = "KDB_TLS_KEYSTORE_PASSWORD"
private const val ENV_TRUSTSTORE_PASSWORD = "KDB_TLS_TRUSTSTORE_PASSWORD"
