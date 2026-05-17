package dev.kdb.config

import kotlinx.serialization.Serializable

@Serializable
public data class KdbTlsConfig(
    val enabled: Boolean = false,
    val keyStorePath: String? = null,
    val keyStorePassword: String? = null,
    val keyStoreType: String = "PKCS12",
    val trustStorePath: String? = null,
    val trustStorePassword: String? = null,
    val trustStoreType: String = "PKCS12",
    val requireClientAuth: Boolean = false,
)
