package io

import (
	"fmt"
	"strings"
)

// SegmentKind names a segment category under a namespace.
type SegmentKind int

const (
	SegmentKindDelta SegmentKind = iota
	SegmentKindWAL
	SegmentKindSSTable
)

func (k SegmentKind) String() string {
	switch k {
	case SegmentKindDelta:
		return "delta"
	case SegmentKindWAL:
		return "wal"
	case SegmentKindSSTable:
		return "sstable"
	default:
		return "unknown"
	}
}

// SegmentNameBuilder constructs canonical segment paths.
var SegmentNameBuilder segmentNameBuilder

type segmentNameBuilder struct{}

func (segmentNameBuilder) Delta(namespaceID, segmentID string) string {
	return path(namespaceID, SegmentKindDelta, segmentID)
}

func (segmentNameBuilder) WAL(namespaceID, walID string) string {
	return path(namespaceID, SegmentKindWAL, walID)
}

func (segmentNameBuilder) SSTable(namespaceID string, level int, fileID string) string {
	return fmt.Sprintf("ns/%s/%s/L%d/%s", namespaceID, SegmentKindSSTable.String(), level, fileID)
}

func (segmentNameBuilder) NamespacePrefix(namespaceID string) string {
	return "ns/" + namespaceID + "/"
}

func path(namespaceID string, kind SegmentKind, segmentID string) string {
	return fmt.Sprintf("ns/%s/%s/%s", namespaceID, kind.String(), segmentID)
}

// SnapshotKeyBuilder builds snapshot keys.
var SnapshotKeyBuilder snapshotKeyBuilder

type snapshotKeyBuilder struct{}

func (snapshotKeyBuilder) Enlistment(enlistmentID string) string {
	return "kdb:snap:" + enlistmentID
}

// ValidateSegmentName checks segment path safety.
func ValidateSegmentName(segmentName string) error {
	if segmentName == "" {
		return &PlatformIOError{Message: "segment name must not be empty"}
	}
	if strings.Contains(segmentName, "..") {
		return &PlatformIOError{Message: "segment name must not contain '..'", SegmentName: segmentName}
	}
	if !strings.HasPrefix(segmentName, "ns/") {
		return &PlatformIOError{Message: "segment name must start with ns/", SegmentName: segmentName}
	}
	return nil
}
