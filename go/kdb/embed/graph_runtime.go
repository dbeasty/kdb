package embed

import "github.com/limidus/kdb/go/kdb/dag"

// CompactCommitGraph rewrites this runtime's commit-graph segments, and the commits still held
// in memory, as one segment - a full compaction, dropping the records of commits deleted since
// they were written. Maintenance does this on its own when segments or deleted records pile up
// (see dag.MaybeRebuildGraphFile); this is for an operator who wants it now. A no-op when
// graph.file is off or the runtime is read-only.
func (rt *EmbeddedKdbRuntime) CompactCommitGraph() error {
	d := dagOf(rt)
	if d == nil {
		return nil
	}
	return d.CompactGraphFile()
}

// CommitGraphStatus reports the shape of this runtime's on-disk commit graph.
func (rt *EmbeddedKdbRuntime) CommitGraphStatus() dag.GraphFileStatus {
	d := dagOf(rt)
	if d == nil {
		return dag.GraphFileStatus{}
	}
	return d.GraphFileStats()
}
