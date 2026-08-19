package dev.kdb.storage

/**
 * Browsers/Node don't expose total system memory to JS in a portable
 * way; callers using percentOfAvailable on this target fall back to
 * DEFAULT_HOT_TIER_BYTES.
 */
public actual fun totalSystemMemoryBytes(): Long? = null
