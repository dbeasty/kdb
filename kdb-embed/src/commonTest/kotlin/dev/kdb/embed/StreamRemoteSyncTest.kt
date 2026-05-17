package dev.kdb.embed

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class StreamRemoteSyncTest {
    @Test
    fun reconnectBackoffCapsAtMax() {
        assertEquals(500L, StreamReconnectPolicy.backoffMs(0))
        assertTrue(StreamReconnectPolicy.backoffMs(10) <= StreamReconnectPolicy.MAX_BACKOFF_MS)
        assertTrue(StreamReconnectPolicy.shouldRetry(0))
        assertTrue(!StreamReconnectPolicy.shouldRetry(StreamReconnectPolicy.MAX_ATTEMPTS))
    }

    @Test
    fun recoveryJsonHelpers() {
        assertTrue(streamRecoveryStartedJson("lost").contains("SyncFallback"))
        assertTrue(
            streamRecoveryCompletedJson(
                StreamRecoveryResult(2, 0, dev.kdb.codec.KdbHash.fromHex("aa".repeat(32))),
            ).contains("SyncRecovered"),
        )
        assertTrue(streamRecoveryFailedJson(IllegalStateException("x")).contains("SyncFallbackFailed"))
        assertTrue(streamReconnectingJson(1, 500).contains("Reconnecting"))
    }
}
