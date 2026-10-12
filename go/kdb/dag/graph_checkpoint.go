package dag

import (
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
// names, and a segment it names is never deleted while it is the checkpoint on disk (graphPinned).
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
		state, mainUnlogged := d.checkpointSnapshotLocked(keep, false)
		if mainUnlogged {
			return CheckpointState{}, nil, nil, ErrHeadNotLogged
		}
		return state, nil, nil, nil
	}
	state, mainUnlogged := d.checkpointSnapshotLocked(nil, true)
	if mainUnlogged {
		return CheckpointState{}, nil, nil, ErrHeadNotLogged
	}
	refs := make([]GraphSegmentRef, 0, len(d.nodes.segs))
	for _, b := range d.nodes.segs {
		refs = append(refs, GraphSegmentRef{Seq: b.seq, CRC: b.crc, Records: b.count})
	}
	hidden := make([]codec.Hash, 0, len(d.nodes.hidden))
	for h := range d.nodes.hidden {
		hidden = append(hidden, h)
	}
	return state, refs, hidden, nil
}

// AttachCheckpointGraph maps the segments a checkpoint names, before RestoreCheckpoint puts back
// the commits it carries. Every named segment must be present with the named checksum and record
// count, or nothing is attached and the error says which - the caller then treats the checkpoint
// as unusable. Segments on disk the checkpoint does not name were written after it and are
// removed (unless read-only): their commits are either in the checkpoint's resident tier or after
// it in the log, and replay brings those back.
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
		b, err := openGraphBase(filepath.Join(dir, graphSegmentName(ref.Seq)), d.NamespaceID)
		if err != nil {
			return fail(err)
		}
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
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nodes.attachTrusted(segs, hidden)
	d.graphPinned = named
	d.graphDeferred = nil
	if n := len(onDisk); n > 0 {
		d.graphNextSeq = onDisk[n-1] + 1
	}
	for _, seq := range onDisk {
		if !named[seq] && !ro {
			os.Remove(filepath.Join(dir, graphSegmentName(seq)))
		}
	}
	return nil
}

// SetGraphCheckpointRefs records which segments the checkpoint now on disk references, and
// deletes superseded segments the previous checkpoint was holding back.
func (d *InMemoryCommitDag) SetGraphCheckpointRefs(refs []GraphSegmentRef) {
	d.mu.Lock()
	defer d.mu.Unlock()
	pinned := make(map[uint64]bool, len(refs))
	for _, r := range refs {
		pinned[r.Seq] = true
	}
	d.graphPinned = pinned
	kept := d.graphDeferred[:0]
	for _, ds := range d.graphDeferred {
		if pinned[ds.seq] {
			kept = append(kept, ds)
			continue
		}
		os.Remove(ds.path)
	}
	d.graphDeferred = kept
}

// DetachGraph drops the segment tier, for an open that attached a checkpoint's segments and then
// found the checkpoint unusable: the replay that follows rebuilds every commit resident.
func (d *InMemoryCommitDag) DetachGraph() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nodes.detach()
	d.graphPinned = nil
}
