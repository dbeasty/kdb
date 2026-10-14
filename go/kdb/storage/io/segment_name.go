package io

import (
	"fmt"
	"strconv"
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

// DeltaSequencedFileName returns just the file-name component (no
// namespace/kind prefix) a sequenced delta segment uses: a 20-digit
// zero-padded decimal sequence number plus ".seg". Zero-padded so that
// lexicographic sort (what ListSegments and every SegmentByteStore
// implementation give you for free) equals commit order - see
// DeltaSequenced.
func DeltaSequencedFileName(seq int64) string {
	return fmt.Sprintf("%020d.seg", seq)
}

// ParseDeltaSequencedFileName parses a file-name component produced by
// DeltaSequencedFileName back into its sequence number. ok is false for
// anything else, including the pre-Layer-13 random-UUID segment names
// (see kdb-spec-layer13 Component 47 §4.1) - callers use that to detect a
// legacy data directory and refuse to guess at its order.
func ParseDeltaSequencedFileName(fileName string) (seq int64, ok bool) {
	const suffix = ".seg"
	if !strings.HasSuffix(fileName, suffix) {
		return 0, false
	}
	digits := strings.TrimSuffix(fileName, suffix)
	if len(digits) != 20 {
		return 0, false
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// DeltaSequenced builds the full segment path for a sequenced delta
// segment - the naming scheme Component 47 replaces random-UUID delta
// segment names with, so that segment file order is commit order by
// construction (see kdb-spec-layer13-resource-governance.md §4.1).
func (segmentNameBuilder) DeltaSequenced(namespaceID string, seq int64) string {
	return path(namespaceID, SegmentKindDelta, DeltaSequencedFileName(seq))
}

func (segmentNameBuilder) WAL(namespaceID, walID string) string {
	return path(namespaceID, SegmentKindWAL, walID)
}

func (segmentNameBuilder) SSTable(namespaceID string, level int, fileID string) string {
	return fmt.Sprintf("ns/%s/%s/L%d/%s", namespaceID, SegmentKindSSTable.String(), level, fileID)
}

// SSTablePartial names an SSTable while it is still being written. It sits beside the sstable/
// directory rather than in it, so nothing that registers tables - this build's DiscoverTables or
// an older one's, which take every name under sstable/ - can mistake a half-written table for a
// finished one. The writer renames it to its SSTable name once the footer is down.
func (b segmentNameBuilder) SSTablePartial(namespaceID string, fileID string) string {
	return b.SSTablePartialPrefix(namespaceID) + fileID
}

// SSTablePartialPrefix is the prefix every SSTablePartial name in namespaceID starts with.
func (segmentNameBuilder) SSTablePartialPrefix(namespaceID string) string {
	return fmt.Sprintf("ns/%s/%s-partial/", namespaceID, SegmentKindSSTable.String())
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
