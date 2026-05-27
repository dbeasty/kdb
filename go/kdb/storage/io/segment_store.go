package io

// SegmentByteStore is the platform-specific byte store for segments.
type SegmentByteStore interface {
	Append(segmentName string, bytes []byte) (int64, error)
	Read(segmentName string, offset int64, length int) ([]byte, error)
	Flush(segmentName string, fsync bool) error
	MarkSealed(segmentName string) error
	List(prefix string) ([]string, error)
	Delete(segmentName string) error
	AvailableBytes() (int64, error)
	ReadSnapshot(key string) ([]byte, error)
	WriteSnapshot(key string, data []byte) error
	DeleteSnapshot(key string) error
}
