package dev.kdb.service

import dev.kdb.policy.DocumentExpiryPolicy
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

/** Layer 16 §9.5: the service's --expire-* flags, which are the only way an operator turns expiry on. */
class ExpiryFlagsConfigTest {

    /** Guards: no --expire-field means no expiry policy at all (the feature is off by default). */
    @Test
    fun expiryIsOffUnlessTheFieldIsGiven() {
        assertNull(ServiceConfig.parse(emptyArray()).expiry)
        // grace/interval alone do nothing without a field.
        assertNull(ServiceConfig.parse(arrayOf("--expire-grace", "30s")).expiry)
    }

    /** Guards: --expire-field alone uses the spec's defaults (grace 0, sweep every 60s). */
    @Test
    fun fieldAloneUsesSpecDefaults() {
        assertEquals(
            DocumentExpiryPolicy("expiresAt", 0, 60_000),
            ServiceConfig.parse(arrayOf("--expire-field", "expiresAt")).expiry,
        )
    }

    /** Guards: durations accept Go-style, ISO-8601 and bare milliseconds. */
    @Test
    fun graceAndIntervalAcceptDurationsAndMillis() {
        assertEquals(
            DocumentExpiryPolicy("ttl", 30_000, 300_000),
            ServiceConfig.parse(
                arrayOf("--expire-field", "ttl", "--expire-grace", "30s", "--expire-interval", "5m"),
            ).expiry,
        )
        assertEquals(
            DocumentExpiryPolicy("ttl", 1_500, 250),
            ServiceConfig.parse(
                arrayOf("--expire-field", "ttl", "--expire-grace", "1500", "--expire-interval", "250"),
            ).expiry,
        )
        assertEquals(
            45_000,
            ServiceConfig.parse(arrayOf("--expire-field", "ttl", "--expire-grace", "PT45S")).expiry?.graceMillis,
        )
    }

    /** Guards: an unparsable duration or a non-positive interval is rejected at parse time rather
     * than producing a sweeper that never runs. */
    @Test
    fun invalidDurationsAreRejected() {
        assertFailsWith<IllegalStateException> {
            ServiceConfig.parse(arrayOf("--expire-field", "ttl", "--expire-grace", "soon"))
        }
        assertFailsWith<IllegalArgumentException> {
            ServiceConfig.parse(arrayOf("--expire-field", "ttl", "--expire-interval", "0"))
        }
        assertFailsWith<IllegalStateException> {
            ServiceConfig.parse(arrayOf("--expire-field"))
        }
    }
}
