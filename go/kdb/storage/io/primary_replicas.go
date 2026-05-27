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

// PrimaryWithReplicas delegates live I/O to primary and mirrors sealed segments and snapshots to replicas.
type PrimaryWithReplicas struct {
	primary  SegmentByteStore
	replicas []ReplicaSink
	policy   ReplicationPolicy
}

// NewPrimaryWithReplicas wraps a primary store with zero or more replica sinks.
func NewPrimaryWithReplicas(primary SegmentByteStore, replicas []ReplicaSink, policy ReplicationPolicy) *PrimaryWithReplicas {
	if replicas == nil {
		replicas = []ReplicaSink{}
	}
	return &PrimaryWithReplicas{primary: primary, replicas: replicas, policy: policy}
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

func (s *PrimaryWithReplicas) Delete(segmentName string) error {
	if err := s.primary.Delete(segmentName); err != nil {
		return err
	}
	return s.replicate(func(r ReplicaSink) error {
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
	return s.replicate(func(r ReplicaSink) error {
		return r.DeleteSnapshot(key)
	})
}

func (s *PrimaryWithReplicas) replicate(fn func(ReplicaSink) error) error {
	var first error
	for _, r := range s.replicas {
		if err := fn(r); err != nil {
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
