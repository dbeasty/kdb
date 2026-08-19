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
	PageTargetSizeBytes     int64
	PageMaxSizeBytes        int64
	GlobalMemoryBudgetBytes int64
	CompressionCodec        CompressionCodec
	DefaultIndexRetention   IndexRetention
	IOShim                  PlatformIOShim
	// Durability controls WAL sync behavior for this namespace. Zero
	// value is DurabilitySync, preserving prior behavior.
	Durability Durability
	// AsyncSyncIntervalMillis is the background fsync period used when
	// Durability is DurabilityAsync. Zero uses a built-in default.
	AsyncSyncIntervalMillis int64
}
