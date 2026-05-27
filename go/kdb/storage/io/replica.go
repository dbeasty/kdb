package io

// ReplicaSink receives sealed segments and snapshots from a primary store.
// Implementations must not be used as the live append target for the LSM engine.
type ReplicaSink interface {
	PutSegment(segmentName string, data []byte) error
	DeleteSegment(segmentName string) error
	WriteSnapshot(key string, data []byte) error
	DeleteSnapshot(key string) error
}
