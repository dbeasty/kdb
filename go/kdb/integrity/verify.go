// Package integrity implements kdb-spec-layer15 Component 58 (integrity
// verification) and Component 59 (repair, quarantine): read-only detection
// of delta-log corruption, and the repair actions that are provably safe
// to take on what verification finds.
//
// Verification is deliberately independent of delta.DefaultReader: that
// reader's ListSegments/ReadAll are replay-oriented and silently discard
// a *delta.CorruptFrameError on any segment that isn't the caller's
// concern at replay time (see DefaultReader.ListSegments' scanSegmentRef,
// which drops scanErr whenever it's a CorruptFrameError at all, not just
// on the last segment). A verification tool exists specifically to not
// silently discard that information, so it scans raw segment bytes itself.
package integrity

import (
	"errors"
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
	// segment - by kdb-spec-layer13 §4.3's rule, the expected shape of an
	// unclean shutdown (a commit that was never acknowledged), not real
	// corruption.
	TornTail Classification = "torn_tail"
	// MidLogCorruption is a corrupt or short frame anywhere other than
	// the tail of the last segment - real corruption, never silently
	// tolerated.
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
	sequence   int64
	raw        []byte
	commits    []delta.ScannedCommit
	corruptErr *delta.CorruptFrameError
	// consumedBytes is how much of raw the scan actually accounted for -
	// either up to corruptErr's offset, or (on a clean scan) up to the end
	// of the last frame. Anything past it and before len(raw) is either
	// the expected shape of a torn tail (see ScanSegmentBytes' doc
	// comment on the frameEnd > len(bytes) case) or, if this isn't the
	// last segment, evidence of truncation that the scanner's silent
	// short-tail tolerance was never meant to hide.
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

func readAndScanSegment(shim storage.PlatformIOShim, namespaceID string, seq int64, comp storage.CompressionCodec) (scannedSegment, error) {
	name := storio.SegmentNameBuilder.DeltaSequenced(namespaceID, seq)
	raw, err := shim.ReadFromSegment(name, 0, 1<<30)
	if err != nil {
		return scannedSegment{}, err
	}
	commits, scanErr := delta.ScanSegmentBytes(raw, comp)
	ss := scannedSegment{sequence: seq, raw: raw, commits: commits}
	var corrupt *delta.CorruptFrameError
	if scanErr != nil {
		if !errors.As(scanErr, &corrupt) {
			return scannedSegment{}, scanErr
		}
		ss.corruptErr = corrupt
		ss.consumedBytes = corrupt.Offset
		return ss, nil
	}
	if len(commits) == 0 {
		ss.consumedBytes = 0
		return ss, nil
	}
	last := commits[len(commits)-1]
	ss.consumedBytes = last.FrameOffset + frameLen(raw, last.FrameOffset)
	return ss, nil
}

const frameHeaderSize = 16

// frameLen reads the 16-byte frame header's compressed-body-length field
// (the same layout delta.ScanSegmentBytes parses) to compute the full
// on-disk size of the frame starting at offset, so verify can tell
// whether a clean scan actually consumed every byte of the segment.
func frameLen(raw []byte, offset int) int {
	b := raw[offset+4 : offset+8]
	return frameHeaderSize + (int(b[0])<<24 | int(b[1])<<16 | int(b[2])<<8 | int(b[3]))
}

// Options configures a verification run. Compression must name the codec
// the namespace was written with - it is never recorded in a frame (see
// kdb-spec-layer15 §2 background), so a mismatch here cannot be detected
// from the bytes alone and Verify will not guess.
type Options struct {
	Level       Level
	Compression storage.CompressionCodec
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
		ss, err := readAndScanSegment(shim, namespaceID, seq, opts.Compression)
		if err != nil {
			return nil, fmt.Errorf("reading segment %d: %w", seq, err)
		}
		report.Segments = append(report.Segments, SegmentSummary{
			Sequence: seq, SizeBytes: int64(len(ss.raw)), FrameCount: len(ss.commits),
		})
		isLastSegment := i == len(seqs)-1

		if ss.corruptErr != nil {
			class := MidLogCorruption
			if isLastSegment {
				class = TornTail
			}
			report.Findings = append(report.Findings, Finding{
				Namespace: namespaceID, Level: L1, Segment: seq, Offset: ss.corruptErr.Offset,
				Classification: class, Detail: ss.corruptErr.Reason,
			})
		} else if ss.consumedBytes < len(ss.raw) {
			class := MidLogCorruption
			detail := fmt.Sprintf("%d trailing byte(s) after the last valid frame were never consumed by a frame - truncated data in a sealed segment", len(ss.raw)-ss.consumedBytes)
			if isLastSegment {
				class = TornTail
				detail = fmt.Sprintf("%d trailing byte(s) after the last valid frame - an incomplete write never fsynced before shutdown", len(ss.raw)-ss.consumedBytes)
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
