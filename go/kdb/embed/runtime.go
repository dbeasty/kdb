package embed

import (
	"errors"
	"log"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
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
	// deltaReader is this runtime's view of the durable commit log. Held so
	// maintenance paths (see MigrateHistoryStrategy) can walk the log
	// without reopening the storage handle behind the runtime's back.
	deltaReader storage.DeltaSegmentReader
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
	// maintain writes a checkpoint and, under storage.HistoryModeNone,
	// reclaims the delta segments that are past both it and the retention
	// window. Nil for a runtime with nothing to reclaim - a read-only or
	// pure in-memory one. See Maintain.
	maintain func() (TruncationResult, error)
}

// Maintain writes a checkpoint and reclaims what the retention window
// allows, and reports what it reclaimed.
//
// Safe and cheap to call at any time, on either history mode: under
// storage.HistoryModeFull it writes a checkpoint and deletes nothing,
// which is exactly what a checkpoint has always been. Under
// storage.HistoryModeNone it is how a long-running process keeps its
// footprint bounded rather than waiting for a clean shutdown that may
// never come - a server should call it on a timer.
//
// Errors are real here, unlike the best-effort checkpoint on close: a
// caller that asked for maintenance explicitly wants to know it did not
// happen.
func (rt *EmbeddedKdbRuntime) Maintain() (TruncationResult, error) {
	if err := rt.AssertWritable(); err != nil {
		return TruncationResult{}, err
	}
	if rt.maintain == nil {
		return TruncationResult{}, nil
	}
	return rt.maintain()
}

// CompactHistory reclaims now, whatever this namespace's reclaim mode says,
// and reports what it freed.
//
// This is the explicit ask that storage.ReclaimManual exists to wait for,
// and it is the destructive operation in the whole history-mode story - the
// one-way door. Switching a namespace between history modes moves a marker
// and destroys nothing; *this* deletes segments, and under history=none
// what it deletes is gone. A caller offering it to a human should say so
// here rather than on the mode switch, which is the harmless half.
//
// A no-op under history=full, which reclaims no segments by definition; it
// still writes a checkpoint and compacts the blob store, which is what a
// full-history namespace's maintenance has always done.
func (rt *EmbeddedKdbRuntime) CompactHistory() (TruncationResult, error) {
	if err := rt.AssertWritable(); err != nil {
		return TruncationResult{}, err
	}
	if rt.maintain == nil {
		return TruncationResult{}, nil
	}
	// Force a reclaiming pass by lifting the mode for the duration of it.
	// Restored afterwards, including on the error path: a compaction that
	// failed halfway must not leave the namespace quietly reclaiming from
	// then on, which would be the opposite of what manual was chosen for.
	eng, ok := rt.Storage.(*engine.ServerEngine)
	if !ok {
		return rt.maintain()
	}
	previous := eng.ReclaimMode()
	eng.SetReclaimMode(storage.ReclaimImmediate)
	defer eng.SetReclaimMode(previous)
	return rt.maintain()
}

// ReclaimMode reports how eagerly this namespace reclaims.
func (rt *EmbeddedKdbRuntime) ReclaimMode() storage.ReclaimMode {
	eng, ok := rt.Storage.(*engine.ServerEngine)
	if !ok {
		return storage.DefaultReclaimMode
	}
	return eng.ReclaimMode()
}

// SetReclaimMode changes how eagerly this namespace reclaims, on a running
// runtime. In force from the next maintenance pass.
//
// Moving to storage.ReclaimManual stops future reclamation; it does not and
// cannot undo reclamation that has already happened.
func (rt *EmbeddedKdbRuntime) SetReclaimMode(r storage.ReclaimMode) error {
	if err := rt.AssertWritable(); err != nil {
		return err
	}
	eng, ok := rt.Storage.(*engine.ServerEngine)
	if !ok {
		return errors.New("kdb: this runtime has no engine that reclaims")
	}
	eng.SetReclaimMode(r)
	return nil
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
