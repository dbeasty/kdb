package storage

// StorageEngineConfig bundles runtime limits with a platform I/O shim.
type StorageEngineConfig struct {
	PageTargetSizeBytes     int64
	PageMaxSizeBytes        int64
	GlobalMemoryBudgetBytes int64
	CompressionCodec        CompressionCodec
	DefaultIndexRetention   IndexRetention
	IOShim                  PlatformIOShim
}
