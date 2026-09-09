package io

// ReplicaSink receives sealed segments and snapshots from a primary store.
// Implementations must not be used as the live append target for the LSM engine.
type ReplicaSink interface {
	PutSegment(segmentName string, data []byte) error
	DeleteSegment(segmentName string) error
	WriteSnapshot(key string, data []byte) error
	DeleteSnapshot(key string) error
}

// SegmentFetcher is a sink that can also hand a segment back.
//
// Optional, and separate from ReplicaSink, because writing a copy somewhere and being able to
// read it again are different capabilities: a sink may be a write-only destination (a pipe, a
// backup process) and still be a perfectly good replica. Only an archive needs to be readable,
// and only then because restoring is the point of one - see PrimaryWithReplicas.RestoreSegment,
// which refuses an archive that cannot produce what it was given.
type SegmentFetcher interface {
	GetSegment(segmentName string) ([]byte, error)
}
