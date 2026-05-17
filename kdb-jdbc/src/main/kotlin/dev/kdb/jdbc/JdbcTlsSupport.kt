package dev.kdb.jdbc

import dev.kdb.transport.core.TransportConnectOptions
import dev.kdb.transport.core.TransportTlsSettings
import java.util.Properties

fun KdbJdbcUrl.transportConnectOptions(): TransportConnectOptions {
    if (!sslEnabled) {
        return TransportConnectOptions()
    }
    return TransportConnectOptions(
        tls =
            TransportTlsSettings(
                enabled = true,
                keyStorePath = sslKeyStore,
                keyStorePassword = sslKeyStorePassword ?: System.getenv(ENV_KEYSTORE_PASSWORD),
                trustStorePath = sslTrustStore,
                trustStorePassword = sslTrustStorePassword ?: System.getenv(ENV_TRUSTSTORE_PASSWORD),
                trustAll = sslTrustAll,
            ),
    )
}

internal fun sslSettingsFromProperties(info: Properties?): JdbcSslSettings {
    if (info == null) return JdbcSslSettings()
    val ssl =
        info.getProperty("ssl")?.toBooleanStrictOrNull()
            ?: info.getProperty("sslEnabled")?.toBooleanStrictOrNull()
            ?: false
    val sslMode = info.getProperty("sslmode")?.lowercase()
    val enabled = ssl || sslMode == "require" || sslMode == "verify-ca" || sslMode == "verify-full"
    return JdbcSslSettings(
        enabled = enabled,
        trustStore = info.getProperty("sslTrustStore"),
        trustStorePassword = info.getProperty("sslTrustStorePassword"),
        keyStore = info.getProperty("sslKeyStore"),
        keyStorePassword = info.getProperty("sslKeyStorePassword"),
        trustAll = info.getProperty("sslTrustAll")?.toBooleanStrictOrNull() ?: false,
    )
}

internal data class JdbcSslSettings(
    val enabled: Boolean = false,
    val trustStore: String? = null,
    val trustStorePassword: String? = null,
    val keyStore: String? = null,
    val keyStorePassword: String? = null,
    val trustAll: Boolean = false,
)

private const val ENV_KEYSTORE_PASSWORD = "KDB_TLS_KEYSTORE_PASSWORD"
private const val ENV_TRUSTSTORE_PASSWORD = "KDB_TLS_TRUSTSTORE_PASSWORD"
