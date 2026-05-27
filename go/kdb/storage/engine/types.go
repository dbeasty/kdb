package engine

import (
	"github.com/limidus/kdb/go/kdb/storage"
)

// Target selects which storage engine profile to open.
type Target int

const (
	TargetServer Target = iota
	TargetBrowser
	TargetInMemory
	TargetGPU
)

// Factory opens namespace storage engines.
type Factory interface {
	Target() Target
	Open(namespaceID string, config storage.StorageEngineConfig) (Handle, error)
}

// Handle bundles adapter and optional delta I/O.
type Handle interface {
	NamespaceID() string
	Adapter() storage.EvictableAdapter
	DeltaWriter() storage.DeltaSegmentWriter
	DeltaReader() storage.DeltaSegmentReader
	Close() error
}

// Engine is a namespace-scoped evictable storage adapter.
type Engine interface {
	storage.EvictableAdapter
	NamespaceID() string
}
