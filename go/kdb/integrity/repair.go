package integrity

import (
	"fmt"
	"time"

	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// Action names what Repair did (or refused to do) for one Finding.
type Action string

const (
	ActionTruncatedTornTail    Action = "truncated_torn_tail"
	ActionRewroteSegmentPrefix Action = "rewrote_segment_prefix"
	ActionRefused              Action = "refused"
	ActionNoOp                 Action = "no_op"
)

// RepairStep is what Repair did for one finding, and why.
type RepairStep struct {
	Finding        Finding
	Action         Action
	QuarantineName string   // set when bytes were preserved before mutating anything
	MissingHashes  []string // set only when Action == ActionRefused
	Detail         string
}

// RepairResult is the outcome of one Repair run.
type RepairResult struct {
	Namespace string
	Steps     []RepairStep
}

// AnyRefused reports whether any step could not be safely repaired.
func (r *RepairResult) AnyRefused() bool {
	for _, s := range r.Steps {
		if s.Action == ActionRefused {
			return true
		}
	}
	return false
}

// Repair acts on a verification Report - see kdb-spec-layer15 Component 59.
// It never invents its own opinion of what is wrong: every step corresponds
// to exactly one L1 Finding already produced by Verify. Legacy (pre-Layer-13
// random-UUID) segment migration is not yet implemented by this package -
// see kdb-spec-layer15 §10 and the execution plan's Phase 7 follow-up.
//
// Repair is idempotent (kdb-spec-layer15 P3): re-running it against a
// report from an already-repaired directory finds nothing to do, since a
// re-Verify of that directory would no longer surface the fixed findings.
func Repair(shim storage.PlatformIOShim, report *Report, opts Options) (*RepairResult, error) {
	result := &RepairResult{Namespace: report.Namespace}
	for _, f := range report.Findings {
		if f.Level != L1 {
			continue // L2 findings (missing_parent, sequence_gap) name a gap repair cannot fabricate data to fill; see Component 61 (restore).
		}
		switch f.Classification {
		case TornTail:
			step, err := repairTornTail(shim, report.Namespace, f)
			if err != nil {
				return nil, err
			}
			result.Steps = append(result.Steps, step)
		case MidLogCorruption:
			step, err := repairMidLogCorruption(shim, report.Namespace, f, opts)
			if err != nil {
				return nil, err
			}
			result.Steps = append(result.Steps, step)
		}
	}
	return result, nil
}

func repairTornTail(shim storage.PlatformIOShim, namespaceID string, f Finding) (RepairStep, error) {
	name := storio.SegmentNameBuilder.DeltaSequenced(namespaceID, f.Segment)
	raw, err := shim.ReadFromSegment(name, 0, 1<<30)
	if err != nil {
		return RepairStep{}, fmt.Errorf("reading segment %d for torn-tail repair: %w", f.Segment, err)
	}
	if f.Offset < 0 || f.Offset > len(raw) {
		return RepairStep{}, fmt.Errorf("finding offset %d out of range for segment %d (%d bytes)", f.Offset, f.Segment, len(raw))
	}
	good := raw[:f.Offset]
	torn := raw[f.Offset:]
	quarantineName := quarantineSegmentName(namespaceID, f.Segment)
	if _, err := shim.AppendToSegment(quarantineName, torn); err != nil {
		return RepairStep{}, fmt.Errorf("quarantining torn tail of segment %d: %w", f.Segment, err)
	}
	if err := shim.DeleteSegment(name); err != nil {
		return RepairStep{}, fmt.Errorf("removing segment %d before rewrite: %w", f.Segment, err)
	}
	if len(good) > 0 {
		if _, err := shim.AppendToSegment(name, good); err != nil {
			return RepairStep{}, fmt.Errorf("rewriting segment %d without its torn tail: %w", f.Segment, err)
		}
	}
	return RepairStep{
		Finding: f, Action: ActionTruncatedTornTail, QuarantineName: quarantineName,
		Detail: fmt.Sprintf("truncated segment %d at byte %d; %d torn byte(s) preserved in %s", f.Segment, f.Offset, len(torn), quarantineName),
	}, nil
}

// repairMidLogCorruption quarantines the full original segment and, only
// if the namespace's parent closure holds using just that segment's good
// prefix (the frames before the corrupt one - the scanner cannot resync
// past a corrupt frame to recover anything after it), rewrites the
// segment to contain that prefix alone. If closure would break, it
// touches nothing and reports exactly which commit hashes would go
// missing (kdb-spec-layer15 §5.2, P3: never guess, never destroy
// evidence unless the repair it would enable is actually safe).
func repairMidLogCorruption(shim storage.PlatformIOShim, namespaceID string, f Finding, opts Options) (RepairStep, error) {
	seqs, err := listSequencedSegments(shim, namespaceID)
	if err != nil {
		return RepairStep{}, err
	}
	goodPrefixLen := -1
	allCommits := make(map[string]struct{})
	prefixCommits := make(map[string]struct{})
	for _, seq := range seqs {
		ss, err := readAndScanSegment(shim, namespaceID, seq)
		if err != nil {
			return RepairStep{}, err
		}
		if seq == f.Segment {
			goodPrefixLen = f.Offset
			for _, c := range ss.commits {
				if c.FrameOffset < f.Offset {
					allCommits[c.CommitHash.Hex()] = struct{}{}
					prefixCommits[c.CommitHash.Hex()] = struct{}{}
				}
			}
			continue
		}
		for _, c := range ss.commits {
			allCommits[c.CommitHash.Hex()] = struct{}{}
		}
	}
	if goodPrefixLen < 0 {
		return RepairStep{}, fmt.Errorf("segment %d named by finding not found among namespace %s's segments", f.Segment, namespaceID)
	}

	genesis, err := GenesisCommitHash(namespaceID)
	if err != nil {
		return RepairStep{}, err
	}
	var missing []string
	for _, seq := range seqs {
		if seq <= f.Segment {
			continue
		}
		ss, err := readAndScanSegment(shim, namespaceID, seq)
		if err != nil {
			return RepairStep{}, err
		}
		for _, c := range ss.commits {
			for _, p := range c.Commit.ParentHashes {
				if p == genesis {
					continue // never persisted, by design - see GenesisCommitHash
				}
				if _, ok := allCommits[p.Hex()]; !ok {
					missing = append(missing, p.Hex())
				}
			}
		}
	}
	if len(missing) > 0 {
		return RepairStep{
			Finding: f, Action: ActionRefused, MissingHashes: missing,
			Detail: fmt.Sprintf("removing the corrupt frame in segment %d would drop %d commit(s) still referenced as parents by later segments - run kdb restore instead", f.Segment, len(missing)),
		}, nil
	}

	name := storio.SegmentNameBuilder.DeltaSequenced(namespaceID, f.Segment)
	raw, err := shim.ReadFromSegment(name, 0, 1<<30)
	if err != nil {
		return RepairStep{}, err
	}
	quarantineName := quarantineSegmentName(namespaceID, f.Segment)
	if _, err := shim.AppendToSegment(quarantineName, raw); err != nil {
		return RepairStep{}, fmt.Errorf("quarantining corrupt segment %d: %w", f.Segment, err)
	}
	if err := shim.DeleteSegment(name); err != nil {
		return RepairStep{}, fmt.Errorf("removing corrupt segment %d before rewrite: %w", f.Segment, err)
	}
	good := raw[:goodPrefixLen]
	if len(good) > 0 {
		if _, err := shim.AppendToSegment(name, good); err != nil {
			return RepairStep{}, fmt.Errorf("rewriting segment %d prefix: %w", f.Segment, err)
		}
	}
	return RepairStep{
		Finding: f, Action: ActionRewroteSegmentPrefix, QuarantineName: quarantineName,
		Detail: fmt.Sprintf("kept %d of %d commit(s) from segment %d (frames before the corrupt one); full original preserved in %s", len(prefixCommits), len(prefixCommits)+1, f.Segment, quarantineName),
	}, nil
}

// quarantineSegmentName builds a path for preserved bytes deliberately
// outside ns/<namespaceID>/delta/ - alongside the live .seg files, a
// quarantine file's name would not parse as a sequenced segment and
// delta.DefaultReader.ListSegments / delta.Factory.OpenWriter would treat
// it exactly like a pre-Layer-13 legacy segment (refusing to open the
// namespace at all). A dedicated quarantine/ subdirectory keeps evidence
// preserved (kdb-spec-layer15 P3) without the production delta package
// ever needing to know this convention exists.
func quarantineSegmentName(namespaceID string, seq int64) string {
	return fmt.Sprintf("ns/%s/quarantine/%020d.quarantine-%d", namespaceID, seq, time.Now().UnixNano())
}
