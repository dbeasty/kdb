package dag

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/limidus/kdb/go/kdb/codec"
)

// The DAG's half of the on-disk commit graph: when to write segments, which kind, and attaching
// them. The embedding runtime decides where they live (ConfigureGraphFile) and when it is worth
// trying (open, maintenance); what goes in them is decided here, under the same lock that guards
// the commits themselves.

// GraphTombstoneRatio is the share of segment records that may be hidden (deleted from the DAG
// since they were written) before a full rewrite drops them - Cassandra's tombstone compaction.
const GraphTombstoneRatio = 0.2

// ConfigureGraphFile names the directory this DAG's graph segments live in. Without one no
// segment is ever written or read, whatever GraphSettings.FileEnabled says. A read-only DAG maps
// segments but never writes or deletes one: it shares the directory with a writer.
func (d *InMemoryCommitDag) ConfigureGraphFile(dir string, readOnly bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.graphDir = dir
	d.graphReadOnly = readOnly
}

// OpenGraphFile attaches the segments in the configured directory. Missing segments are not an
// error. If any segment is unusable, none is used: the DAG keeps every commit resident, exactly
// where it was before, the segments are removed (unless read-only), and the next write is a full
// rewrite. All-or-nothing because segments are only disjoint as a set; keeping some of a set that
// a damaged full segment had superseded could show a commit twice.
func (d *InMemoryCommitDag) OpenGraphFile() error {
	d.mu.RLock()
	dir, ro, enabled := d.graphDir, d.graphReadOnly, d.graph.FileEnabled
	d.mu.RUnlock()
	if dir == "" || !enabled {
		return nil
	}
	seqs, err := listGraphSegments(dir)
	if err != nil {
		return err
	}
	var opened []*graphBase // ascending seq
	var firstErr error
	for _, seq := range seqs {
		b, err := openGraphBase(filepath.Join(dir, graphSegmentName(seq)), d.NamespaceID)
		if err == nil && b.seq != seq {
			b.close()
			err = fmt.Errorf("%w: segment %d names itself %d", errGraphFile, seq, b.seq)
		}
		if err != nil {
			firstErr = err
			break
		}
		opened = append(opened, b)
	}
	var maxSeq uint64
	if len(seqs) > 0 {
		maxSeq = seqs[len(seqs)-1]
	}
	if firstErr != nil {
		for _, b := range opened {
			b.close()
		}
		if !ro {
			for _, seq := range seqs {
				os.Remove(filepath.Join(dir, graphSegmentName(seq)))
			}
		}
		d.mu.Lock()
		d.graphNextSeq = maxSeq + 1
		d.mu.Unlock()
		return firstErr
	}
	// A full segment supersedes everything below it: what a crash between a full rewrite's rename
	// and its deletes leaves behind.
	start := 0
	for i, b := range opened {
		if b.full {
			start = i
		}
	}
	for _, b := range opened[:start] {
		b.close()
		if !ro {
			os.Remove(b.path)
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, b := range opened[start:] {
		d.attachLocked(b, nil)
	}
	if len(seqs) > 0 {
		d.graphNextSeq = maxSeq + 1
	}
	return nil
}

// graphWrite is which kind of segment write a rebuild makes.
type graphWrite int

const (
	graphWriteNone  graphWrite = iota
	graphWriteFlush            // the resident tier as a new segment
	graphWriteFull             // everything, as one segment superseding the rest
)

// planGraphWriteLocked decides what, if anything, to write now. Must hold mu.
func (d *InMemoryCommitDag) planGraphWriteLocked(forceFull bool) graphWrite {
	if d.graphDir == "" || !d.graph.FileEnabled || d.graphReadOnly {
		return graphWriteNone
	}
	if forceFull {
		return graphWriteFull
	}
	n := &d.nodes
	resident, visible := n.residentLen(), n.segVisible()
	// Tombstones accumulate in the overlay until a rewrite drops them; past the ratio, rewrite
	// whether or not anything new arrived.
	if len(n.segs) > 0 && float64(len(n.hidden)) > GraphTombstoneRatio*float64(n.segRecords) {
		return graphWriteFull
	}
	if resident < d.graph.RebuildCommits {
		return graphWriteNone
	}
	switch {
	case len(n.segs) == 0:
		return graphWriteFull // the first segment is the whole graph
	case len(n.segs) >= d.graph.MaxSegments:
		return graphWriteFull // too many segments for a lookup to search
	case resident*2 >= visible:
		return graphWriteFull // rewriting everything costs little more than a flush would
	default:
		return graphWriteFlush
	}
}

// MaybeRebuildGraphFile writes whatever the policy calls for now - nothing, a flush of the
// resident tier as a new segment, or a full rewrite - and reports whether it wrote. Safe to call
// concurrently with reads and writes: segments are written without the DAG's lock, from a
// snapshot.
func (d *InMemoryCommitDag) MaybeRebuildGraphFile() (bool, error) {
	return d.writeGraph(false)
}

// CompactGraphFile rewrites every segment and the resident tier as one segment, dropping the
// records of deleted commits - the operator's major compaction. A no-op where no graph directory
// is configured or the DAG is read-only.
func (d *InMemoryCommitDag) CompactGraphFile() error {
	_, err := d.writeGraph(true)
	return err
}

// RebuildGraphFile is CompactGraphFile.
func (d *InMemoryCommitDag) RebuildGraphFile() error { return d.CompactGraphFile() }

func (d *InMemoryCommitDag) writeGraph(forceFull bool) (bool, error) {
	d.graphRebuild.Lock()
	defer d.graphRebuild.Unlock()

	d.mu.Lock()
	kind := d.planGraphWriteLocked(forceFull)
	if kind == graphWriteNone {
		d.mu.Unlock()
		return false, nil
	}
	full := kind == graphWriteFull
	// Generations go into the segment, so they must be sound before they are copied.
	d.ensureGenerationsLocked()
	// Commits a checkpoint must leave out - unlogged, and everything descending from one - must
	// not reach a segment either, or a checkpoint that references the segment would bring them
	// back. See unloggedClosureLocked.
	excluded := d.unloggedClosureLocked()
	snap := d.nodes.snapshotForSegment(full,
		func(h codec.Hash, r []byte) bool {
			if d.nodes.namespaces[recNsIdx(r)] != d.NamespaceID {
				return false
			}
			if _, ok := excluded[h]; ok {
				return false
			}
			// Commits that may yet be taken back stay resident: a group part not yet decided,
			// and one received from a peer that is not in the log.
			if _, ok := d.provisional[h]; ok {
				return false
			}
			if _, ok := d.unlogged[h]; ok {
				return false
			}
			return true
		},
		func(h codec.Hash, tx codec.UUID) bool {
			got, ok := d.txIndex[tx]
			return ok && got == h
		},
		func(h codec.Hash) bool {
			return len(d.nodes.ops[h]) > 0 || d.opsEvictedFor(h)
		},
	)
	dir, seq := d.graphDir, d.graphNextSeq
	d.graphNextSeq++
	d.mu.Unlock()
	defer snap.release()

	if !full && len(snap.resident) == 0 {
		return false, nil // everything resident is held back; nothing to flush
	}
	path, err := writeGraphSegment(dir, d.NamespaceID, seq, full, snap.sources)
	if err != nil {
		return false, err
	}
	nb, err := openGraphBase(path, d.NamespaceID)
	if err != nil {
		os.Remove(path)
		return false, err
	}
	d.mu.Lock()
	var replaced []*graphBase
	if full {
		// Everything attached now is what this one supersedes. Segment writes are serialized,
		// so nothing was added since the snapshot.
		replaced = append(replaced, d.nodes.segs...)
	}
	d.attachLocked(nb, replaced)
	d.mu.Unlock()
	// After the attach, so a crash before these deletes leaves a full segment that the next open
	// recognises as superseding them. Unlinking a mapped file is safe: the mapping keeps the
	// inode until the snapshot's reference is released.
	for _, b := range replaced {
		d.retireSegmentFile(b.seq, b.path)
	}
	if full {
		syncGraphDir(dir)
	}
	return true, nil
}

// retireSegmentFile deletes a superseded segment's file - unless the checkpoint on disk still
// references it, in which case it waits for the next checkpoint (SetGraphCheckpointRefs). The
// checkpoint is what an open restores from, so a segment it names must outlive its replacement.
func (d *InMemoryCommitDag) retireSegmentFile(seq uint64, path string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.graphPinned[seq] {
		d.graphDeferred = append(d.graphDeferred, deferredSegment{seq: seq, path: path})
		return
	}
	os.Remove(path)
}

type deferredSegment struct {
	seq  uint64
	path string
}

// attachLocked makes nb the newest segment and drops the transaction-index entries the segments
// now answer. Must hold mu exclusively.
func (d *InMemoryCommitDag) attachLocked(nb *graphBase, replaced []*graphBase) {
	d.nodes.attach(nb, replaced)
	for tx, h := range d.txIndex {
		if d.nodes.isResident(h) {
			continue
		}
		if got, ok := d.segmentTxLocked(tx); ok && got == h {
			delete(d.txIndex, tx)
		}
	}
	d.txIndex = shrunk(d.txIndex) // see nodeStore.attach
	// Segment records carry whether a commit's operations must be loaded, so the eviction set
	// keeps entries for the resident tier only.
	for h := range d.opsEvicted {
		if !d.nodes.isResident(h) && d.nodes.has(h) {
			delete(d.opsEvicted, h)
		}
	}
	d.opsEvicted = shrunk(d.opsEvicted)
	// The segments answer for the same graph, so ancestry memos stay valid; the head snapshot
	// holds a copy and is unaffected. Nothing to republish.
}

// segmentTxLocked resolves a transaction id through the segments, newest first.
func (d *InMemoryCommitDag) segmentTxLocked(tx codec.UUID) (codec.Hash, bool) {
	for _, b := range d.nodes.segs {
		if h, ok := b.findTx(tx); ok {
			return h, true
		}
	}
	return codec.Hash{}, false
}

// CloseGraphFile unmaps every segment. The DAG must not be used afterwards: commits that lived
// only in segments are no longer reachable.
func (d *InMemoryCommitDag) CloseGraphFile() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nodes.detach()
}

// GraphFileStatus describes the on-disk graph as this DAG sees it.
type GraphFileStatus struct {
	Segments        int   // mapped segment files
	FileCommits     int   // commits the segments answer for
	HiddenRecords   int   // segment records for commits since deleted; dropped by a full rewrite
	ResidentCommits int   // commits held in memory
	FileBytes       int64 // total size of the segment files
}

// GraphFileStats reports the on-disk graph's shape.
func (d *InMemoryCommitDag) GraphFileStats() GraphFileStatus {
	d.mu.RLock()
	defer d.mu.RUnlock()
	st := GraphFileStatus{
		Segments:        len(d.nodes.segs),
		FileCommits:     d.nodes.segVisible(),
		HiddenRecords:   len(d.nodes.hidden),
		ResidentCommits: d.nodes.residentLen(),
	}
	for _, b := range d.nodes.segs {
		st.FileBytes += b.fileBytes
	}
	return st
}
