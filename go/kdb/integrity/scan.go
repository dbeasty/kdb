package integrity

import (
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// ListSequencedSegments returns namespaceID's delta segment sequence numbers in ascending
// (commit) order - the exported face of listSequencedSegments, for tooling (backup) that needs
// the same legacy-name refusal semantics as verify/restore.
func ListSequencedSegments(shim storage.PlatformIOShim, namespaceID string) ([]int64, error) {
	return listSequencedSegments(shim, namespaceID)
}

// VerifiedSegmentPrefix reads segment seq and returns only its L1 CRC-verified prefix: every
// byte up to the end of the last frame that scanned clean, never the tail past it (which may be
// a torn in-progress write - kdb-spec-layer15 Component 60 §6.5 test 4). Also returns how many
// commits that prefix holds.
func VerifiedSegmentPrefix(shim storage.PlatformIOShim, namespaceID string, seq int64) ([]byte, int, error) {
	ss, err := readAndScanSegment(shim, namespaceID, seq)
	if err != nil {
		return nil, 0, err
	}
	return ss.raw[:ss.consumedBytes], len(ss.commits), nil
}

// ScanVerifiedCommits returns every commit whose frame passed L1 CRC
// verification, across every segment of namespaceID, keyed by hex hash.
// Commits at or after the first corrupt or short frame in any given
// segment are excluded from that segment's contribution - restore (see
// package recovery) uses this so a source with any unrepaired corruption
// still contributes everything it safely can, and nothing it can't.
func ScanVerifiedCommits(shim storage.PlatformIOShim, namespaceID string) (map[string]document.Commit, error) {
	seqs, err := listSequencedSegments(shim, namespaceID)
	if err != nil {
		return nil, err
	}
	out := make(map[string]document.Commit)
	for _, seq := range seqs {
		ss, err := readAndScanSegment(shim, namespaceID, seq)
		if err != nil {
			return nil, err
		}
		for _, c := range ss.commits {
			out[c.CommitHash.Hex()] = c.Commit
		}
	}
	return out, nil
}
