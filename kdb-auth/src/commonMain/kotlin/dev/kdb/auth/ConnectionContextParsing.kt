package dev.kdb.auth

import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

@OptIn(ExperimentalEncodingApi::class)
public fun connectionContextFromHeaders(headers: Map<String, String>): ConnectionContext {
    val normalized = headers.mapKeys { (k, _) -> k.lowercase() }
    val authHeader = normalized["authorization"]
    val apiKey = normalized["x-kdb-api-key"] ?: normalized["x-api-key"]
    if (authHeader != null) {
        when {
            authHeader.startsWith("Basic ", ignoreCase = true) -> {
                val decoded =
                    runCatching {
                        Base64.decode(authHeader.substring(6).trim()).decodeToString()
                    }.getOrNull()
                if (decoded != null) {
                    val colon = decoded.indexOf(':')
                    if (colon >= 0) {
                        return ConnectionContext(
                            user = decoded.substring(0, colon),
                            password = decoded.substring(colon + 1),
                            headers = headers,
                        )
                    }
                }
            }
            authHeader.startsWith("Bearer ", ignoreCase = true) ->
                return ConnectionContext(
                    token = authHeader.substring(7).trim(),
                    headers = headers,
                )
        }
    }
    if (apiKey != null) {
        return ConnectionContext(token = apiKey, headers = headers)
    }
    return ConnectionContext(headers = headers)
}

@OptIn(ExperimentalEncodingApi::class)
public fun ConnectionContext.toHttpHeaders(): Map<String, String> {
    val out = mutableMapOf<String, String>()
    when {
        user != null && password != null ->
            out["Authorization"] =
                "Basic " + Base64.encode("$user:$password".encodeToByteArray())
        token != null && token.contains(':') ->
            out["Authorization"] = "Basic " + Base64.encode(token.encodeToByteArray())
        token != null ->
            out["Authorization"] = "Bearer $token"
    }
    return out
}
