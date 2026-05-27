package storage

// PlatformIOShim is the platform I/O boundary for segment and snapshot storage.
type PlatformIOShim interface {
	AppendToSegment(segmentName string, bytes []byte) (int64, error)
	ReadFromSegment(segmentName string, offset int64, length int) ([]byte, error)
	FlushSegment(segmentName string) error
	SealSegment(segmentName string) error
	ListSegments(namespaceID string) ([]string, error)
	DeleteSegment(segmentName string) error
	AvailableBytes() (int64, error)
	ReadSnapshot(key string) ([]byte, error)
	WriteSnapshot(key string, data []byte) error
	DeleteSnapshot(key string) error
}
