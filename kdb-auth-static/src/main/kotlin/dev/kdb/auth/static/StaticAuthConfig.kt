package dev.kdb.auth.static

import kotlinx.serialization.Serializable

@Serializable
public data class StaticAuthConfig(
    val users: Map<String, StaticUserConfig> = emptyMap(),
    val roles: Map<String, List<String>> = emptyMap(),
)

@Serializable
public data class StaticUserConfig(
    val secret: String,
    val roles: List<String> = emptyList(),
)
