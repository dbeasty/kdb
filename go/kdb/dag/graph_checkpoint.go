package dag

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// Checkpoints that reference the graph instead of carrying it.
//
// A checkpoint used to carry every commit in the namespace, so opening one decoded and admitted
// all of history, and writing one encoded all of it - both proportional to the commit count even
// once the graph lived on disk. With graph segments attached, a checkpoint instead carries only
// the resident tier and names the segments by sequence and checksum; open maps those segments
// and restores the small remainder.
//
// The segments do not become an authority. The checkpoint vouches for exactly the segments it
// names, and a segment it names is never deleted while it is the checkpoint on disk - nor while it
// is being written: the capture pins what it names (graphPinned), since a background merge may
// supersede a segment between the capture and the write.
// If a named segment is missing or different, the checkpoint is unusable and open replays the
// delta log - the same fallback as any other unusable checkpoint. That is only sound where the
// log is the whole record, so the caller uses this form only under history=full (keep == nil).

// GraphSegmentRef names one graph segment a checkpoint relies on.
type GraphSegmentRef struct {
	Seq     uint64
	CRC     uint32
	Records int
}

// CheckpointSnapshotWithGraph is CheckpointSnapshotLogged, except that when graph segments are
// attached and keep is nil it captures only the resident tier and returns references to the
// segments that hold the rest, and the segment records that no longer count. With no segments,
// or a retention filter, refs is nil and the state is the full one. Refuses with ErrHeadNotLogged
// exactly as CheckpointSnapshotLogged does.
func (d *InMemoryCommitDag) CheckpointSnapshotWithGraph(keep func(document.Commit) bool) (CheckpointState, []GraphSegmentRef, []codec.Hash, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if keep != nil || len(d.nodes.segs) == 0 {
		state, mainUnlogged := d.checkpointSnapshotLocked(keep, false, true)
		if mainUnlogged {
			return CheckpointState{}, nil, nil, ErrHeadNotLogged
		}
		return state, nil, nil, nil
	}
	state, mainUnlogged := d.checkpointSnapshotLocked(nil, true, true)
	if mainUnlogged {
		return CheckpointState{}, nil, nil, ErrHeadNotLogged
	}
	refs := make([]GraphSegmentRef, 0, len(d.nodes.segs))
	d.graphFiles.Lock()
	if d.graphPinned == nil {
		d.graphPinned = make(map[uint64]bool, len(d.nodes.segs))
	}
	for _, b := range d.nodes.segs {
		refs = append(refs, GraphSegmentRef{Seq: b.seq, CRC: b.crc, Records: b.count})
		// Until SetGraphCheckpointRefs replaces the pins with exactly these - or for good, if the
		// write fails: a superset only defers deletes.
		d.graphPinned[b.seq] = true
	}
	d.graphFiles.Unlock()
	hidden := make([]codec.Hash, 0, len(d.nodes.hidden))
	for h := range d.nodes.hidden {
		hidden = append(hidden, h)
	}
	// A commit the checkpoint leaves out for want of a log frame - unlogged, or built on one - may
	// already be in a segment it names: flushed before it was marked again after a failed log
	// write, or before history arrived beneath it unlogged. Hidden, the next open does not have
	// it, exactly as after a crash; the next sync fetches it again.
	for h := range d.unloggedClosureLocked() {
		if _, hid := d.nodes.hidden[h]; !hid && !d.nodes.isResident(h) && d.nodes.inAnySegment(h) {
			hidden = append(hidden, h)
		}
	}
	return state, refs, hidden, nil
}

// AttachCheckpointGraph maps the segments a checkpoint names, before RestoreCheckpoint puts back
// the commits it carries. Every named segment must be present with the named checksum and record
// count, or nothing is attached and the error says which - the caller then treats the checkpoint
// as unusable. Segments on disk the checkpoint does not name were written after it and are
// removed (unless read-only): their commits are either in the checkpoint's resident tier or after
// it in the log, and replay brings those back.
//
// A named segment may still be under its .pending name: a merge output the checkpoint named,
// with the process gone before the inputs it replaced were deleted and it was renamed. It is used
// from there, and - once every file the checkpoint does not name is gone, so nothing on disk shares
// a record with it - renamed (unless read-only).
func (d *InMemoryCommitDag) AttachCheckpointGraph(refs []GraphSegmentRef, hidden []codec.Hash) error {
	d.mu.RLock()
	dir, ro, enabled := d.graphDir, d.graphReadOnly, d.graph.FileEnabled
	d.mu.RUnlock()
	if dir == "" || !enabled {
		return fmt.Errorf("dag: checkpoint references graph segments but the graph file is not enabled")
	}
	var segs []*graphBase // newest first, as the store keeps them
	named := make(map[uint64]bool, len(refs))
	fail := func(err error) error {
		for _, b := range segs {
			b.close()
		}
		return err
	}
	for i := len(refs) - 1; i >= 0; i-- {
		ref := refs[i]
		named[ref.Seq] = true
		final := filepath.Join(dir, graphSegmentName(ref.Seq))
		at := final
		if _, err := os.Stat(final); errors.Is(err, os.ErrNotExist) {
			if _, perr := os.Stat(final + graphPendingSuffix); perr == nil {
				at = final + graphPendingSuffix
			}
		}
		b, err := openGraphBase(at, d.NamespaceID)
		if err != nil {
			return fail(err)
		}
		b.path, b.diskPath = final, at
		if b.seq != ref.Seq || b.crc != ref.CRC || b.count != ref.Records {
			b.close()
			return fail(fmt.Errorf("%w: segment %d is not the one the checkpoint names (crc %08x/%08x, records %d/%d)",
				errGraphFile, ref.Seq, b.crc, ref.CRC, b.count, ref.Records))
		}
		segs = append(segs, b)
	}
	// refs arrive newest first (CheckpointSnapshotWithGraph); reverse iteration above built
	// oldest first, so flip back.
	for i, j := 0, len(segs)-1; i < j; i, j = i+1, j-1 {
		segs[i], segs[j] = segs[j], segs[i]
	}
	onDisk, _ := listGraphSegments(dir)
	maxSeq := maxGraphFileSeq(dir, onDisk)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nodes.attachTrusted(segs, hidden)
	d.graphFiles.Lock()
	d.graphPinned = named
	d.graphDeferred = nil
	d.graphFiles.Unlock()
	if len(onDisk) > 0 || maxSeq > 0 {
		d.graphNextSeq = max(d.graphNextSeq, maxSeq+1)
	}
	if ro {
		return nil
	}
	for _, seq := range onDisk {
		if !named[seq] {
			os.Remove(filepath.Join(dir, graphSegmentName(seq)))
		}
	}
	usingPending := make(map[uint64]bool)
	for _, b := range segs {
		if b.diskPath != b.path {
			usingPending[b.seq] = true
		}
	}
	removeGraphLeftovers(dir, usingPending)
	renamed := false
	for _, b := range segs {
		if b.diskPath != b.path && os.Rename(b.diskPath, b.path) == nil {
			b.diskPath = b.path
			renamed = true
		}
	}
	if renamed {
		syncGraphDir(dir)
	}
	return nil
}

// SetGraphCheckpointRefs records which segments the checkpoint now on disk references, and
// deletes superseded segments the previous checkpoint was holding back.
//
// Then every merge output that was waiting for those segments to go takes its final name - the
// one this checkpoint names it by.
func (d *InMemoryCommitDag) SetGraphCheckpointRefs(refs []GraphSegmentRef) {
	d.mu.Lock()
	pinned := make(map[uint64]bool, len(refs))
	for _, r := range refs {
		pinned[r.Seq] = true
	}
	d.graphFiles.Lock()
	d.graphPinned = pinned
	kept := d.graphDeferred[:0]
	removed := false
	for _, ds := range d.graphDeferred {
		if pinned[ds.seq] {
			kept = append(kept, ds)
			continue
		}
		os.Remove(ds.diskPath)
		removed = true
	}
	d.graphDeferred = kept
	if removed {
		d.publishPendingLocked(nil)
	}
	d.graphFiles.Unlock()
	dir := d.graphDir
	d.mu.Unlock()
	if removed && dir != "" {
		syncGraphDir(dir)
	}
}

// DetachGraph drops the segment tier, for an open that attached a checkpoint's segments and then
// found the checkpoint unusable: the replay that follows rebuilds every commit resident.
func (d *InMemoryCommitDag) DetachGraph() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nodes.detach()
	d.graphFiles.Lock()
	d.graphPinned = nil
	d.graphFiles.Unlock()
}
