package dag

import (
	"errors"
	"os"

	"github.com/limidus/kdb/go/kdb/codec"
)

// The DAG's half of the on-disk commit graph: when to write the file, and attaching it. The
// embedding runtime decides where it lives (ConfigureGraphFile) and when it is worth trying
// (open, checkpoint, close); everything about what goes in it is decided here, under the same
// lock that guards the commits themselves.

// ConfigureGraphFile names where this DAG's graph file lives. Without a path no file is ever
// written or read, whatever GraphSettings.FileEnabled says.
func (d *InMemoryCommitDag) ConfigureGraphFile(path string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.graphPath = path
}

// OpenGraphFile attaches the graph file if one exists and is usable. A missing file is not an
// error. An unusable one is removed and reported; the DAG keeps every commit resident, which is
// exactly where it was before.
func (d *InMemoryCommitDag) OpenGraphFile() error {
	d.mu.RLock()
	path, enabled := d.graphPath, d.graph.FileEnabled
	d.mu.RUnlock()
	if path == "" || !enabled {
		return nil
	}
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	nb, err := openGraphBase(path, d.NamespaceID)
	if err != nil {
		os.Remove(path)
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.attachLocked(nb)
	return nil
}

// MaybeRebuildGraphFile writes a new graph file and attaches it when the resident tier has grown
// past GraphSettings.RebuildCommits, or when there is no file yet and the DAG holds that many.
// Reports whether it rebuilt. Safe to call concurrently with reads and writes: the file is
// written without the DAG's lock, from a snapshot.
func (d *InMemoryCommitDag) MaybeRebuildGraphFile() (bool, error) {
	return d.rebuildGraphFile(false)
}

// RebuildGraphFile writes and attaches a graph file regardless of the threshold.
func (d *InMemoryCommitDag) RebuildGraphFile() error {
	_, err := d.rebuildGraphFile(true)
	return err
}

func (d *InMemoryCommitDag) rebuildGraphFile(force bool) (bool, error) {
	d.graphRebuild.Lock()
	defer d.graphRebuild.Unlock()

	d.mu.Lock()
	if d.graphPath == "" || !d.graph.FileEnabled {
		d.mu.Unlock()
		return false, nil
	}
	if !force && d.nodes.residentLen() < d.graph.RebuildCommits {
		d.mu.Unlock()
		return false, nil
	}
	// Generations go into the file, so they must be sound before they are copied.
	d.ensureGenerationsLocked()
	snap := d.nodes.snapshotForFile(
		func(h codec.Hash, sl *slot) bool {
			if d.nodes.namespaces[sl.ns] != d.NamespaceID {
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
		func(h codec.Hash, tx codec.UUID) bool { return d.txIndex[tx] == h },
	)
	path := d.graphPath
	d.mu.Unlock()
	defer snap.release()

	next, nextTx := snap.sources()
	if err := writeGraphFile(path, d.NamespaceID, next, nextTx); err != nil {
		return false, err
	}
	nb, err := openGraphBase(path, d.NamespaceID)
	if err != nil {
		os.Remove(path)
		return false, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.attachLocked(nb)
	return true, nil
}

// attachLocked makes nb the base and drops the transaction-index entries it now answers.
func (d *InMemoryCommitDag) attachLocked(nb *graphBase) {
	d.nodes.attach(nb)
	for tx, h := range d.txIndex {
		if _, resident := d.nodes.index[h]; resident {
			continue
		}
		if bh, ok := nb.findTx(tx); ok && bh == h {
			delete(d.txIndex, tx)
		}
	}
	// What the base answers is now the same graph, so ancestry memos stay valid; the head
	// snapshot holds a copy and is unaffected. Nothing to republish.
}

// CloseGraphFile unmaps the graph file. The DAG must not be used afterwards: commits that lived
// only in the file are no longer reachable.
func (d *InMemoryCommitDag) CloseGraphFile() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nodes.detach()
}

// GraphFileStats reports how many commits the attached file holds, how many the resident tier
// holds, and the file's size in bytes.
func (d *InMemoryCommitDag) GraphFileStats() (fileCommits, residentCommits int, fileBytes int64) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if b := d.nodes.base; b != nil {
		fileCommits = b.count - len(d.nodes.hidden)
		fileBytes = b.fileBytes
	}
	return fileCommits, d.nodes.residentLen(), fileBytes
}
