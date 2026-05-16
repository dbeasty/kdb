package dev.kdb.wire

import dev.kdb.error.EncodingNegotiationFailureException
import dev.kdb.error.UnsupportedProtocolVersionException

internal class DefaultHandshakeNegotiator : HandshakeNegotiator {
    override fun negotiate(local: HandshakePayload, remote: HandshakePayload): HandshakeAckPayload {
        if (remote.protocolVersion > KDB_WIRE_PROTOCOL_VERSION ||
            remote.protocolVersion < MIN_SUPPORTED_WIRE_PROTOCOL_VERSION
        ) {
            throw UnsupportedProtocolVersionException(
                "peer protocol ${remote.protocolVersion} not supported",
                remote.protocolVersion,
                KDB_WIRE_PROTOCOL_VERSION,
            )
        }
        val encoding = intersectEncodings(local.preferredEncodings, remote.preferredEncodings)
            ?: throw EncodingNegotiationFailureException(
                "no common encoding",
                local.preferredEncodings.map { it.name },
                remote.preferredEncodings.map { it.name },
            )
        return HandshakeAckPayload(
            accepted = true,
            negotiatedEncoding = encoding,
            protocolVersion = minOf(KDB_WIRE_PROTOCOL_VERSION, remote.protocolVersion),
            remoteHeads = remote.localHeads,
        )
    }

    private fun intersectEncodings(
        a: List<PayloadEncoding>,
        b: List<PayloadEncoding>,
    ): PayloadEncoding? {
        for (enc in a) {
            if (enc in b) return enc
        }
        return null
    }
}
