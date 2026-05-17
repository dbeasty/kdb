package dev.kdb.transport.core

public data class TransportTlsSettings(
    val enabled: Boolean = true,
    val keyStorePath: String? = null,
    val keyStorePassword: String? = null,
    val keyStoreType: String = "PKCS12",
    val trustStorePath: String? = null,
    val trustStorePassword: String? = null,
    val trustStoreType: String = "PKCS12",
    /** When true, the server requires a valid client certificate (mTLS). */
    val requireClientAuth: Boolean = false,
    /** Test-only: accept any server/client certificate. Never use in production. */
    val trustAll: Boolean = false,
)
