package embed

import (
	"errors"
	"log"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// EmbeddedKdbRuntime composes DAG + storage for embedded use.
type EmbeddedKdbRuntime struct {
	Catalog          string
	DAG              dag.CommitDAG
	Storage          storage.Adapter
	Schema           schema.KdbSchema
	DefaultNamespace string
	WriteBaseVersion *codec.Hash
	// DataRoot is set for file-backed runtimes (empty for pure memory).
	DataRoot string
	// ReadOnly marks a runtime opened under a shared directory lock alongside a writer in
	// another process. Every write path checks it; see AssertWritable.
	ReadOnly bool
	// refresh re-reads the writer's committed history onto this runtime's DAG. Non-nil only for
	// read-only runtimes, which are the only ones whose view can fall behind reality.
	refresh func() error
	release func()
	// storageClose flushes and seals the active delta segment and closes
	// the underlying storage engine handle (WAL final sync included) -
	// nil for a pure in-memory runtime. Deliberately best-effort and
	// never load-bearing for correctness (see kdb-spec-layer13 Component
	// 47 §4.4/§4.5): every commit is already durable at ack time, so a
	// failure here (or skipping it entirely via kill -9) never risks data
	// loss on the next open - it only means the next open pays for an
	// extra topological-replay pass instead of a fast sequential one, and
	// leaves one extra small segment on disk.
	storageClose func() error
}

// ErrReadOnly is returned by every write path on a runtime opened with ReadOnly.
var ErrReadOnly = errors.New("kdb: runtime is open read-only")

// AssertWritable returns ErrReadOnly if this runtime may not be written to. Write paths call it
// at their entry point rather than relying on the storage engine to fail: a read-only runtime
// has no WAL and no delta writer, so a write that got past this would fail somewhere deep with
// an error that describes a missing component instead of the actual reason.
func (r *EmbeddedKdbRuntime) AssertWritable() error {
	if r != nil && r.ReadOnly {
		return ErrReadOnly
	}
	return nil
}

// Refresh re-reads the writer's committed history, advancing a read-only runtime's view to
// whatever has been made durable since it last looked. A no-op on a writable runtime, whose own
// commits keep it current by construction.
//
// A reader's view is always a snapshot of some past moment - there is no way to be continuously
// current with another process - so callers that need a freshness bound should Refresh on their
// own cadence and treat "how stale may this be" as an explicit part of their contract rather
// than an accident of timing.
func (r *EmbeddedKdbRuntime) Refresh() error {
	if r == nil || r.refresh == nil {
		return nil
	}
	return r.refresh()
}

// Close performs an orderly shutdown: flush and seal the active delta
// segment and close the storage engine handle (storageClose - a no-op
// for a pure in-memory runtime), then free file-runtime resources (the
// directory lock), in that order. See kdb-spec-layer13 Component 47 §4.5:
// this whole sequence is an optimization, never a correctness dependency
// - every acked commit is already durable, so skipping this entirely
// (kill -9, or a panic before Close runs) is always safe to replay from,
// just slower to open next time.
//
// Named Close rather than Release: gomobile's iOS binding generates an Objective-C class per
// exported type, and Release collides with Objective-C ARC's reserved -release selector, which
// breaks the iOS bind entirely (see docs/kdb-spec-layer12-execution-plan.md's Phase 0 spike).
func (r *EmbeddedKdbRuntime) Close() {
	if r == nil {
		return
	}
	if r.storageClose != nil {
		if err := r.storageClose(); err != nil {
			log.Printf("kdb: close: flushing/sealing storage for namespace %s: %v (safe to ignore - "+
				"nothing here is required for correctness, see EmbeddedKdbRuntime.storageClose)",
				r.DefaultNamespace, err)
		}
		r.storageClose = nil
	}
	if r.release != nil {
		r.release()
		r.release = nil
	}
}
