package integrity

import (
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
)

// ListSequencedSegments returns namespaceID's delta segment sequence numbers in ascending
// (commit) order - the exported face of listSequencedSegments, for tooling (backup) that needs
// the same legacy-name refusal semantics as verify/restore.
func ListSequencedSegments(shim storage.PlatformIOShim, namespaceID string) ([]int64, error) {
	return listSequencedSegments(shim, namespaceID)
}

// VerifiedSegmentPrefix reads segment seq and returns its L1 CRC-verified prefix - every byte up
// to the end of the last intact frame - and the commits in it, classifying what follows with the
// rule replay and Verify apply (storage.TailKind):
//
//   - clean (nothing, or a preallocated segment's zero padding): the prefix is every frame;
//   - torn (bytes no intact frame follows - an append a crash cut short, never acknowledged): the
//     prefix leaves them out (kdb-spec-layer15 Component 60 §6.5 test 4);
//   - damaged (an intact frame follows the bad bytes, or a frame passes its CRC and will not
//     decode): a *delta.DamagedSegmentError. The commits after the damage were acknowledged, so
//     the prefix alone would silently lack them - the loss N5 fixed for startup.
func VerifiedSegmentPrefix(shim storage.PlatformIOShim, namespaceID string, seq int64) ([]byte, []delta.ScannedCommit, error) {
	ss, err := readAndScanSegment(shim, namespaceID, seq)
	if err != nil {
		return nil, nil, err
	}
	if ss.tail.Kind == storage.TailDamaged {
		return nil, nil, &delta.DamagedSegmentError{
			NamespaceID:     namespaceID,
			Sequence:        seq,
			Offset:          ss.tail.Offset,
			Reason:          ss.tail.Reason,
			NextIntactFrame: ss.tail.NextIntactFrame,
		}
	}
	return ss.raw[:ss.consumedBytes], ss.commits, nil
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
