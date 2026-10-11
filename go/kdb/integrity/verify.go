// Package integrity implements kdb-spec-layer15 Component 58 (integrity
// verification) and Component 59 (repair, quarantine): read-only detection
// of delta-log corruption, and the repair actions that are provably safe
// to take on what verification finds.
//
// Verification reads raw segment bytes itself rather than going through
// delta.DefaultReader, whose listing records only where a segment's intact
// frames stop. It classifies what follows with the same rule replay applies
// (delta.ScanSegment, storage.TailKind), so verify and open cannot disagree
// about whether a segment lost data.
package integrity

import (
	"fmt"
	"sort"
	"strings"

	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// quarantineSuffix marks a sidecar file Repair wrote to preserve bytes it
// removed from a live segment (see repair.go's quarantineSegmentName).
// listSequencedSegments must never treat these as segments in their own
// right - legacy or otherwise - since they are evidence, not log content.
const quarantineSuffix = ".quarantine-"

// Classification names the kind of problem a Finding reports - see
// kdb-spec-layer15 §4.2.
type Classification string

const (
	// TornTail is a corrupt or short final frame in the highest-sequence
	// segment, with nothing intact after it - by kdb-spec-layer13 §4.3's
	// rule, the expected shape of an unclean shutdown (a commit that was
	// never acknowledged), not real corruption.
	TornTail Classification = "torn_tail"
	// MidLogCorruption is a corrupt or short frame anywhere else: any
	// fault that intact frames follow, in any segment (N5), and a faulty
	// last frame in a segment that is no longer the newest - real
	// corruption, never silently tolerated.
	MidLogCorruption Classification = "mid_log_corruption"
	// MissingParent is a commit whose parent hash is not present anywhere
	// in the namespace's scanned log.
	MissingParent Classification = "missing_parent"
	// SequenceGap is a gap in segment sequence numbers.
	SequenceGap Classification = "sequence_gap"
)

// Level names a verification depth - see kdb-spec-layer15 §4.1. L3
// (semantic: document tree / blob re-materialization) is specified but
// not implemented by this package yet.
type Level string

const (
	L1 Level = "L1" // physical: frame CRC / framing
	L2 Level = "L2" // logical: commit hash + parent closure
)

// Finding is one verification result. Segment/Offset/CommitHash are
// zero-valued when not applicable to the finding's classification.
type Finding struct {
	Namespace      string
	Level          Level
	Segment        int64
	Offset         int
	Classification Classification
	Detail         string
	CommitHash     string
}

// SegmentSummary describes one segment's scan, independent of whether it
// produced any findings.
type SegmentSummary struct {
	Sequence   int64
	SizeBytes  int64
	FrameCount int
}

// Report is the output of Verify - the sole input contract Repair (see
// repair.go) acts on.
type Report struct {
	Namespace string
	Segments  []SegmentSummary
	Findings  []Finding
}

// Clean reports whether verification found nothing wrong.
func (r *Report) Clean() bool { return len(r.Findings) == 0 }

// scannedSegment is one segment's independently-scanned bytes and result.
type scannedSegment struct {
	sequence int64
	raw      []byte
	commits  []delta.ScannedCommit
	// tail is how the segment's intact frames end - the rule replay applies too (see
	// storage.TailKind): nothing or zero padding after them, a torn tail, or damage that
	// intact frames follow.
	tail storage.SegmentTail
	// consumedBytes is where the intact frames end (tail.Offset). Anything past it that is not
	// zero padding is a torn tail or damage, as tail says.
	consumedBytes int
}

// listSequencedSegments returns namespaceID's delta segment sequence
// numbers in ascending (commit) order, refusing to guess at order for any
// pre-Layer-13 legacy (random-UUID) segment name - the same refusal
// delta.DefaultReader.ListSegments and delta.Factory.OpenWriter apply.
func listSequencedSegments(shim storage.PlatformIOShim, namespaceID string) ([]int64, error) {
	prefix := storio.SegmentNameBuilder.NamespacePrefix(namespaceID) + "delta/"
	names, err := shim.ListSegments(namespaceID)
	if err != nil {
		return nil, err
	}
	var seqs []int64
	var legacy []string
	for _, name := range names {
		if len(name) < len(prefix) || name[:len(prefix)] != prefix {
			continue
		}
		fileName := name[len(prefix):]
		if strings.Contains(fileName, quarantineSuffix) {
			continue
		}
		seq, ok := storio.ParseDeltaSequencedFileName(fileName)
		if !ok {
			legacy = append(legacy, name)
			continue
		}
		seqs = append(seqs, seq)
	}
	if len(legacy) > 0 {
		return nil, &delta.LegacySegmentFormatError{NamespaceID: namespaceID, Names: legacy}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs, nil
}

func readAndScanSegment(shim storage.PlatformIOShim, namespaceID string, seq int64) (scannedSegment, error) {
	name := storio.SegmentNameBuilder.DeltaSequenced(namespaceID, seq)
	raw, err := shim.ReadFromSegment(name, 0, 1<<30)
	if err != nil {
		return scannedSegment{}, err
	}
	commits, tail := delta.ScanSegment(raw)
	return scannedSegment{sequence: seq, raw: raw, commits: commits, tail: tail, consumedBytes: int(tail.Offset)}, nil
}

// Options configures a verification run.
//
// There is deliberately no compression setting: since the v2 KDBP frame
// (delta.PageCodec) records its codec per frame, verification reads whatever a
// segment was actually written with. Previously the caller had to supply it,
// and a wrong answer was indistinguishable from real corruption.
type Options struct {
	Level Level
}

// Verify walks namespaceID's delta log at the requested level and reports
// exactly what it finds - see kdb-spec-layer15 Component 58.
func Verify(shim storage.PlatformIOShim, namespaceID string, opts Options) (*Report, error) {
	seqs, err := listSequencedSegments(shim, namespaceID)
	if err != nil {
		return nil, err
	}
	report := &Report{Namespace: namespaceID}
	allCommits := make(map[string]document.Commit)
	commitSegment := make(map[string]int64)

	for i, seq := range seqs {
		if i > 0 && seqs[i-1] != seq-1 {
			report.Findings = append(report.Findings, Finding{
				Namespace: namespaceID, Level: L1, Segment: seq,
				Classification: SequenceGap,
				Detail:         fmt.Sprintf("sequence jumps from %d to %d - a segment is missing", seqs[i-1], seq),
			})
		}
		ss, err := readAndScanSegment(shim, namespaceID, seq)
		if err != nil {
			return nil, fmt.Errorf("reading segment %d: %w", seq, err)
		}
		report.Segments = append(report.Segments, SegmentSummary{
			Sequence: seq, SizeBytes: int64(len(ss.raw)), FrameCount: len(ss.commits),
		})
		isLastSegment := i == len(seqs)-1

		// The same rule replay applies (storage.TailKind). Zero padding after the last frame -
		// a preallocated segment's unused space - is not a finding. Damage that intact frames
		// follow is mid-log corruption wherever it is, the newest segment included: calling it a
		// torn tail there let repair truncate away the intact, acknowledged frames after it.
		switch ss.tail.Kind {
		case storage.TailDamaged:
			detail := fmt.Sprintf("%s - damaged in place, not a torn tail", ss.tail.Reason)
			if ss.tail.NextIntactFrame >= 0 {
				detail = fmt.Sprintf("%s - intact frames follow it (the next at offset %d), so this is damage in place, not a torn tail",
					ss.tail.Reason, ss.tail.NextIntactFrame)
			}
			report.Findings = append(report.Findings, Finding{
				Namespace: namespaceID, Level: L1, Segment: seq, Offset: ss.consumedBytes,
				Classification: MidLogCorruption, Detail: detail,
			})
		case storage.TailTorn:
			// Nothing intact follows. In the newest segment that is a torn tail. In an older one
			// it is either a torn tail from an earlier unclean shutdown (a restart then opened a
			// newer segment) or a damaged last frame - indistinguishable here, so it stays
			// mid-log corruption: repair then removes it only if no later commit builds on it.
			class := MidLogCorruption
			detail := fmt.Sprintf("%s, %d byte(s) before the end of the segment, with no intact frame after it - in a segment that is no longer the newest, so either a torn tail from an earlier unclean shutdown or a damaged last frame",
				ss.tail.Reason, len(ss.raw)-ss.consumedBytes)
			if isLastSegment {
				class = TornTail
				detail = fmt.Sprintf("%s, with no intact frame after it - an incomplete write never fsynced before shutdown", ss.tail.Reason)
			}
			report.Findings = append(report.Findings, Finding{
				Namespace: namespaceID, Level: L1, Segment: seq, Offset: ss.consumedBytes,
				Classification: class, Detail: detail,
			})
		}

		for _, sc := range ss.commits {
			allCommits[sc.CommitHash.Hex()] = sc.Commit
			commitSegment[sc.CommitHash.Hex()] = seq
		}
	}

	if opts.Level == L2 {
		genesis, err := GenesisCommitHash(namespaceID)
		if err != nil {
			return nil, err
		}
		for hex, c := range allCommits {
			for _, parent := range c.ParentHashes {
				if parent == genesis {
					// The genesis commit is a fixed, deterministically
					// reconstructed root (see dag.NewInMemoryCommitDag) -
					// it is never written to the delta log, by design, so
					// its absence here is not evidence of anything missing.
					continue
				}
				if _, ok := allCommits[parent.Hex()]; !ok {
					report.Findings = append(report.Findings, Finding{
						Namespace: namespaceID, Level: L2, Segment: commitSegment[hex],
						Classification: MissingParent,
						Detail:         fmt.Sprintf("commit %s references parent %s, which is not present anywhere in the scanned log", hex, parent.Hex()),
						CommitHash:     parent.Hex(),
					})
				}
			}
		}
	}

	sort.Slice(report.Findings, func(i, j int) bool {
		if report.Findings[i].Segment != report.Findings[j].Segment {
			return report.Findings[i].Segment < report.Findings[j].Segment
		}
		return report.Findings[i].Offset < report.Findings[j].Offset
	})
	return report, nil
}
