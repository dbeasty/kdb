package storage

// CapabilitySet declares what a storage engine can do.
type CapabilitySet struct {
	PersistsDeltaLog          bool
	PersistsAcrossReload      bool
	SupportsGpuBulkRead       bool
	SupportsDirectDeltaIngest bool
	MaxEnlistments            *int
	IndexRetentionDefault     IndexRetention
}

// MemoryCapabilities is the reference profile for in-memory adapters.
var MemoryCapabilities = CapabilitySet{
	PersistsDeltaLog:          false,
	PersistsAcrossReload:      false,
	SupportsGpuBulkRead:       false,
	SupportsDirectDeltaIngest: false,
	MaxEnlistments:            nil,
	IndexRetentionDefault:     IndexRetentionEvictable,
}
