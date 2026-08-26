package integrity

import (
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// ScanVerifiedCommits returns every commit whose frame passed L1 CRC
// verification, across every segment of namespaceID, keyed by hex hash.
// Commits at or after the first corrupt or short frame in any given
// segment are excluded from that segment's contribution - restore (see
// package recovery) uses this so a source with any unrepaired corruption
// still contributes everything it safely can, and nothing it can't.
func ScanVerifiedCommits(shim storage.PlatformIOShim, namespaceID string, comp storage.CompressionCodec) (map[string]document.Commit, error) {
	seqs, err := listSequencedSegments(shim, namespaceID)
	if err != nil {
		return nil, err
	}
	out := make(map[string]document.Commit)
	for _, seq := range seqs {
		ss, err := readAndScanSegment(shim, namespaceID, seq, comp)
		if err != nil {
			return nil, err
		}
		for _, c := range ss.commits {
			out[c.CommitHash.Hex()] = c.Commit
		}
	}
	return out, nil
}
