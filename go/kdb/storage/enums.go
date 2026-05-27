package storage

import "github.com/limidus/kdb/go/kdb/codec"

// IndexRetention controls index store eviction participation.
type IndexRetention int

const (
	IndexRetentionPinned IndexRetention = iota
	IndexRetentionEvictable
)

// CompressionCodec names a delta segment compression codec.
type CompressionCodec int

const (
	CompressionNone CompressionCodec = iota
	CompressionZSTD
)

// EnlistmentEvictionState is the eviction state of a realized store per enlistment.
type EnlistmentEvictionState int

const (
	EnlistmentEvictionFull EnlistmentEvictionState = iota
	EnlistmentEvictionDocEvicted
	EnlistmentEvictionEvicted
	EnlistmentEvictionReleased
)

// RebuildBlockingPolicy controls realized-store rebuild blocking.
type RebuildBlockingPolicy int

const (
	RebuildBlockingWait RebuildBlockingPolicy = iota
	RebuildBlockingPartialOK
)

// EvictableAdapter extends Adapter with enlistment eviction hooks.
type EvictableAdapter interface {
	Adapter
	EvictDocuments(enlistmentID codec.UUID) error
	EvictIndex(enlistmentID codec.UUID) error
	RebuildDocuments(enlistmentID codec.UUID, from DeltaSegmentReader) error
	RebuildIndex(enlistmentID codec.UUID, from Adapter) error
	EvictionState(enlistmentID codec.UUID) EnlistmentEvictionState
}

// RealizedStoreHandle is a reference-counted handle to a realized store.
type RealizedStoreHandle interface {
	NamespaceID() string
	CommitHash() codec.Hash
	EnlistmentID() codec.UUID
	IsReady() bool
	AwaitReady(blockingPolicy RebuildBlockingPolicy) error
	Storage() Adapter
	Close()
}
