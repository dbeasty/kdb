package dev.kdb.config

import kotlinx.serialization.Serializable

@Serializable
public data class FeatureToggle(
    val enabled: Boolean = false,
    val listenUri: String? = null,
)

@Serializable
public data class KdbFeatures(
    val peerSync: FeatureToggle = FeatureToggle(),
    val sqlWire: FeatureToggle =
        FeatureToggle(
            enabled = true,
            listenUri = DEFAULT_SQL_LISTEN_URI,
        ),
) {
    public companion object {
        public const val DEFAULT_PEER_LISTEN_URI: String = "kdb-ws://0.0.0.0:7443/kdb?bind=true"
        public const val DEFAULT_SQL_LISTEN_URI: String = "kdb-ws://0.0.0.0:7444/kdb?bind=true"

        public val DEFAULT: KdbFeatures = KdbFeatures()

        public fun peerListenUri(features: KdbFeatures): String? =
            if (features.peerSync.enabled) {
                features.peerSync.listenUri ?: DEFAULT_PEER_LISTEN_URI
            } else {
                null
            }

        public fun sqlListenUri(features: KdbFeatures): String? =
            if (features.sqlWire.enabled) {
                features.sqlWire.listenUri ?: DEFAULT_SQL_LISTEN_URI
            } else {
                null
            }

        /** Stream coordinator URI derived from peer listen URI when peer sync is enabled. */
        public fun streamListenUri(features: KdbFeatures): String? =
            peerListenUri(features)?.let { peer ->
                val pathEnd = peer.indexOf('?')
                val base = if (pathEnd >= 0) peer.substring(0, pathEnd) else peer
                val query = if (pathEnd >= 0) peer.substring(pathEnd) else ""
                val streamBase =
                    when {
                        base.endsWith("/kdb") -> base + "/stream"
                        base.endsWith("/") -> base + "kdb/stream"
                        else -> base.trimEnd('/') + "/stream"
                    }
                streamBase + query
            }
    }
}

@Serializable
public data class KdbProductConfig(
    val features: KdbFeatures = KdbFeatures.DEFAULT,
    val authConfigPath: String? = null,
    val tls: KdbTlsConfig? = null,
) {
    public companion object {
        public val DEFAULT: KdbProductConfig = KdbProductConfig()
    }
}

public fun isNetworkPeerUri(peerUri: String): Boolean {
    val uri = peerUri.trim()
    if (uri.startsWith("memory://")) return false
    if (uri.startsWith("inproc-ws://")) return false
    return true
}

public fun requireNetworkPeerSyncEnabled(
    features: KdbFeatures,
    peerUri: String,
) {
    if (!isNetworkPeerUri(peerUri)) return
    require(features.peerSync.enabled) {
        "network peer sync is disabled (set features.peerSync.enabled=true in config or use --peer-sync)"
    }
}
