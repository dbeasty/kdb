package storage

// Durability selects how strongly a namespace's writes are synced to
// disk before WriteBlob returns. See docs/benchmarks/phase0-baseline.md
// Phase 4 for the throughput/durability tradeoff this exists to make
// explicit and per-namespace rather than a single global default.
type Durability int

const (
	// DurabilitySync fsyncs (via group commit) before every write
	// acknowledgement. Default; matches pre-Phase-4 behavior.
	DurabilitySync Durability = iota
	// DurabilityAsync acknowledges writes once appended to the WAL in
	// memory, syncing on a background timer instead of per-write. A
	// crash can lose up to one sync interval of acknowledged writes.
	DurabilityAsync
	// DurabilityMemoryOnly never syncs the WAL to disk; durability is
	// whatever periodic checkpointing the caller layers on top. Intended
	// for namespaces that treat data as reconstructable/ephemeral.
	DurabilityMemoryOnly
)

// StorageEngineConfig bundles runtime limits with a platform I/O shim.
type StorageEngineConfig struct {
	PageTargetSizeBytes int64
	PageMaxSizeBytes    int64
	// GlobalMemoryBudgetBytes, if > 0, is used directly as the hot-tier
	// byte budget (block cache + memtable sizing). If <= 0, it is
	// resolved from HotTierMemory instead (see ResolveHotTierBytes) -
	// small by default, configurable via an absolute value or a
	// percentage of total system memory. Set this field directly only
	// when you want to bypass that resolution entirely.
	GlobalMemoryBudgetBytes int64
	// HotTierMemory configures the hot-tier budget when
	// GlobalMemoryBudgetBytes is left at zero. See Phase 5 of
	// docs/benchmarks/phase0-baseline.md.
	HotTierMemory         HotTierMemoryConfig
	CompressionCodec      CompressionCodec
	DefaultIndexRetention IndexRetention
	IOShim                PlatformIOShim
	// Durability controls WAL sync behavior for this namespace. Zero
	// value is DurabilitySync, preserving prior behavior.
	Durability Durability
	// AsyncSyncIntervalMillis is the background fsync period used when
	// Durability is DurabilityAsync. Zero uses a built-in default.
	AsyncSyncIntervalMillis int64
}

// ResolvedGlobalMemoryBudgetBytes returns GlobalMemoryBudgetBytes if set,
// otherwise resolves HotTierMemory (falling back to DefaultHotTierBytes).
func (c StorageEngineConfig) ResolvedGlobalMemoryBudgetBytes() int64 {
	if c.GlobalMemoryBudgetBytes > 0 {
		return c.GlobalMemoryBudgetBytes
	}
	return ResolveHotTierBytes(c.HotTierMemory)
}
