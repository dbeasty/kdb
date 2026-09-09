package io

import (
	"fmt"
)

// ReplicationPolicy controls replica error handling.
type ReplicationPolicy struct {
	// FailOnReplicaError when true returns the first replica error to the caller.
	// Default (false) is fail-open: primary success wins, replica errors are ignored.
	FailOnReplicaError bool
}

// A replica and an archive are different jobs, and conflating them is why
// reclaiming used to destroy the only other copy of what it reclaimed.
//
// A *replica* mirrors the primary, deletions included: its purpose is to be
// what the primary is, somewhere else, so a segment the primary no longer has
// is a segment it should not have either.
//
// An *archive* deliberately does not follow deletions. Its purpose is to hold
// what the primary has let go, which is precisely the case retention creates:
// under history=none a compaction deletes segments locally, and if that
// deletion fans out to every sink then nothing can restore them and
// "compaction" is destruction. With an archive, the same operation is
// eviction from local disk - bounded local footprint, and the commits
// themselves still there.
//
// The distinction is a role rather than a flag on the sink, because it is a
// statement about what the copy is *for*. A flag reading
// "replica.honoursDeletes=false" would leave a replica that silently diverges
// from its primary and calls itself a replica.

// SinkRole says whether a sink is a replica or an archive.
type SinkRole int

const (
	// SinkReplica mirrors the primary exactly, deletions included.
	SinkReplica SinkRole = iota
	// SinkArchive mirrors writes but never deletions, so it retains what
	// the primary reclaims. See RestoreSegment for reading it back.
	SinkArchive
)

// RoledSink pairs a sink with what it is for.
type RoledSink struct {
	Sink ReplicaSink
	Role SinkRole
}

// PrimaryWithReplicas delegates live I/O to primary and mirrors sealed segments and snapshots to replicas.
type PrimaryWithReplicas struct {
	primary SegmentByteStore
	sinks   []RoledSink
	policy  ReplicationPolicy
}

// NewPrimaryWithReplicas wraps a primary store with zero or more replica sinks, all in the
// SinkReplica role - the behaviour that predates archives, so existing callers are unchanged.
func NewPrimaryWithReplicas(primary SegmentByteStore, replicas []ReplicaSink, policy ReplicationPolicy) *PrimaryWithReplicas {
	roled := make([]RoledSink, 0, len(replicas))
	for _, r := range replicas {
		roled = append(roled, RoledSink{Sink: r, Role: SinkReplica})
	}
	return &PrimaryWithReplicas{primary: primary, sinks: roled, policy: policy}
}

// NewPrimaryWithSinks is NewPrimaryWithReplicas with each sink's role named, which is what a
// deployment wanting a restorable archive uses.
func NewPrimaryWithSinks(primary SegmentByteStore, sinks []RoledSink, policy ReplicationPolicy) *PrimaryWithReplicas {
	if sinks == nil {
		sinks = []RoledSink{}
	}
	return &PrimaryWithReplicas{primary: primary, sinks: sinks, policy: policy}
}

// HasArchive reports whether any sink retains what the primary deletes, which is what decides
// whether a compaction is eviction or destruction. Callers about to reclaim should say which one
// it is.
func (s *PrimaryWithReplicas) HasArchive() bool {
	for _, sink := range s.sinks {
		if sink.Role == SinkArchive {
			return true
		}
	}
	return false
}

// RestoreSegment fetches a segment back from an archive and writes it to the primary, for a
// segment local disk no longer has.
//
// Tries each archive in turn and takes the first that has it. Replicas are not consulted: a
// replica follows deletions, so it does not have what was deleted, and asking it would only
// produce a misleading "not found" from a sink that was never going to hold it.
func (s *PrimaryWithReplicas) RestoreSegment(segmentName string) error {
	var lastErr error
	for _, sink := range s.sinks {
		if sink.Role != SinkArchive {
			continue
		}
		fetcher, ok := sink.Sink.(SegmentFetcher)
		if !ok {
			// A write-only archive is a contradiction rather than a
			// configuration: it retains what the primary deleted and cannot
			// give any of it back. Named plainly, because the moment this is
			// discovered is the moment somebody needs the data.
			lastErr = fmt.Errorf(
				"an archive sink (%T) cannot read segments back, so nothing it holds can be restored", sink.Sink)
			continue
		}
		data, err := fetcher.GetSegment(segmentName)
		if err != nil {
			lastErr = err
			continue
		}
		if len(data) == 0 {
			continue
		}
		if _, err := s.primary.Append(segmentName, data); err != nil {
			return fmt.Errorf("writing restored segment %s to the primary: %w", segmentName, err)
		}
		return nil
	}
	if lastErr != nil {
		return fmt.Errorf("no archive could produce segment %s: %w", segmentName, lastErr)
	}
	return fmt.Errorf("no archive holds segment %s", segmentName)
}

func (s *PrimaryWithReplicas) Append(segmentName string, bytes []byte) (int64, error) {
	return s.primary.Append(segmentName, bytes)
}

func (s *PrimaryWithReplicas) Read(segmentName string, offset int64, length int) ([]byte, error) {
	return s.primary.Read(segmentName, offset, length)
}

func (s *PrimaryWithReplicas) Flush(segmentName string, fsync bool) error {
	return s.primary.Flush(segmentName, fsync)
}

func (s *PrimaryWithReplicas) MarkSealed(segmentName string) error {
	if err := s.primary.MarkSealed(segmentName); err != nil {
		return err
	}
	data, err := readFullSegment(s.primary, segmentName)
	if err != nil {
		return err
	}
	return s.replicate(func(r ReplicaSink) error {
		return r.PutSegment(segmentName, data)
	})
}

func (s *PrimaryWithReplicas) List(prefix string) ([]string, error) {
	return s.primary.List(prefix)
}

// Delete removes a segment from the primary and from every *replica*, leaving archives holding
// it - see SinkRole. That asymmetry is what lets a history=none compaction be eviction from local
// disk rather than destruction of the commits themselves.
func (s *PrimaryWithReplicas) Delete(segmentName string) error {
	if err := s.primary.Delete(segmentName); err != nil {
		return err
	}
	return s.replicateDeletion(func(r ReplicaSink) error {
		return r.DeleteSegment(segmentName)
	})
}

func (s *PrimaryWithReplicas) AvailableBytes() (int64, error) {
	return s.primary.AvailableBytes()
}

func (s *PrimaryWithReplicas) ReadSnapshot(key string) ([]byte, error) {
	return s.primary.ReadSnapshot(key)
}

func (s *PrimaryWithReplicas) WriteSnapshot(key string, data []byte) error {
	if err := s.primary.WriteSnapshot(key, data); err != nil {
		return err
	}
	copy := append([]byte(nil), data...)
	return s.replicate(func(r ReplicaSink) error {
		return r.WriteSnapshot(key, copy)
	})
}

func (s *PrimaryWithReplicas) DeleteSnapshot(key string) error {
	if err := s.primary.DeleteSnapshot(key); err != nil {
		return err
	}
	// Snapshots follow the same rule as segments: a replica drops it, an archive keeps it. A
	// checkpoint an archive still holds is what makes a restored segment range openable.
	return s.replicateDeletion(func(r ReplicaSink) error {
		return r.DeleteSnapshot(key)
	})
}

// replicate runs fn against every sink, whatever its role. For writes, which both roles take.
func (s *PrimaryWithReplicas) replicate(fn func(ReplicaSink) error) error {
	return s.fanOut(fn, func(RoledSink) bool { return true })
}

// replicateDeletion runs fn against the sinks that follow deletions - replicas only.
//
// This is the whole archive story in one predicate. Before it, Delete fanned out to every sink,
// so reclaiming a segment under history=none destroyed the copy that could have restored it and
// the operation was destruction rather than eviction.
func (s *PrimaryWithReplicas) replicateDeletion(fn func(ReplicaSink) error) error {
	return s.fanOut(fn, func(sink RoledSink) bool { return sink.Role == SinkReplica })
}

func (s *PrimaryWithReplicas) fanOut(fn func(ReplicaSink) error, include func(RoledSink) bool) error {
	var first error
	for _, sink := range s.sinks {
		if !include(sink) {
			continue
		}
		if err := fn(sink.Sink); err != nil {
			if s.policy.FailOnReplicaError {
				return err
			}
			if first == nil {
				first = err
			}
		}
	}
	return nil
}

func readFullSegment(store SegmentByteStore, segmentName string) ([]byte, error) {
	const chunk = 1024 * 1024
	var out []byte
	off := int64(0)
	for {
		b, err := store.Read(segmentName, off, chunk)
		if err != nil {
			return nil, fmt.Errorf("read segment %s: %w", segmentName, err)
		}
		if len(b) == 0 {
			break
		}
		out = append(out, b...)
		off += int64(len(b))
		if len(b) < chunk {
			break
		}
	}
	return out, nil
}

var _ SegmentByteStore = (*PrimaryWithReplicas)(nil)
