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
	// TargetReadOnly opens a namespace for reading only: no write-ahead log is created or
	// recovered, and no delta segment writer is opened, so the process touches nothing another
	// process may be writing. The delta *reader* is still opened - that is how a read-only
	// replica sees the writer's committed history at all. Appended last: these are iota-based
	// and TargetServer's zero value is load-bearing (an unset Target must stay "server").
	TargetReadOnly
)

// IsReadOnly reports whether this target may write to the data directory.
func (t Target) IsReadOnly() bool { return t == TargetReadOnly }

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
