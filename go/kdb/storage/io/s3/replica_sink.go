package s3

import (
	"context"
	"strings"
	"sync"
)

// ReplicaSink mirrors sealed segments and snapshots to S3.
type ReplicaSink struct {
	blobs  BlobStore
	prefix string
}

// NewReplicaSink returns an S3 replica sink backed by blobs.
func NewReplicaSink(blobs BlobStore, cfg Config) *ReplicaSink {
	p := strings.Trim(cfg.Prefix, "/")
	if p != "" {
		p += "/"
	}
	return &ReplicaSink{blobs: blobs, prefix: p}
}

// OpenReplicaSink creates an AWS-backed replica sink from config.
func OpenReplicaSink(ctx context.Context, cfg Config) (*ReplicaSink, error) {
	blobs, err := NewAWSBlobStore(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return NewReplicaSink(blobs, cfg), nil
}

func (r *ReplicaSink) segmentKey(segmentName string) string {
	return r.prefix + segmentName
}

func (r *ReplicaSink) snapshotKey(key string) string {
	return r.prefix + "snapshots/" + key
}

func (r *ReplicaSink) PutSegment(segmentName string, data []byte) error {
	return r.blobs.Put(context.Background(), r.segmentKey(segmentName), data)
}

func (r *ReplicaSink) DeleteSegment(segmentName string) error {
	return r.blobs.Delete(context.Background(), r.segmentKey(segmentName))
}

func (r *ReplicaSink) WriteSnapshot(key string, data []byte) error {
	return r.blobs.Put(context.Background(), r.snapshotKey(key), data)
}

func (r *ReplicaSink) DeleteSnapshot(key string) error {
	return r.blobs.Delete(context.Background(), r.snapshotKey(key))
}

// GetSegment reads a previously-mirrored segment back from S3 - the read-back half that makes
// the sink a restorable backup target rather than a write-only mirror (kdb-finish-up-plan
// Phase 2.11).
func (r *ReplicaSink) GetSegment(segmentName string) ([]byte, error) {
	return r.blobs.Get(context.Background(), r.segmentKey(segmentName))
}

// ListSegments lists mirrored segment keys under namePrefix (relative to the sink's own
// prefix), returning them with the sink prefix stripped so they match the names PutSegment took.
func (r *ReplicaSink) ListSegments(namePrefix string) ([]string, error) {
	keys, err := r.blobs.List(context.Background(), r.prefix+namePrefix)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, strings.TrimPrefix(k, r.prefix))
	}
	return out, nil
}

// ReadSnapshot reads a previously-written snapshot back.
func (r *ReplicaSink) ReadSnapshot(key string) ([]byte, error) {
	return r.blobs.Get(context.Background(), r.snapshotKey(key))
}

// Objects exposes the sink's bucket+prefix as a plain object store (Put/Get/Delete/List with
// the sink prefix applied) - the shape kdb/backup's ObjectStore expects, so `kdb-inspect
// backup` can target the same bucket the replica sink mirrors into.
func (r *ReplicaSink) Objects() *PrefixedStore {
	return &PrefixedStore{blobs: r.blobs, prefix: r.prefix}
}

// PrefixedStore is a BlobStore view with a fixed key prefix.
type PrefixedStore struct {
	blobs  BlobStore
	prefix string
}

func (p *PrefixedStore) Put(ctx context.Context, key string, data []byte) error {
	return p.blobs.Put(ctx, p.prefix+key, data)
}

func (p *PrefixedStore) Get(ctx context.Context, key string) ([]byte, error) {
	return p.blobs.Get(ctx, p.prefix+key)
}

func (p *PrefixedStore) Delete(ctx context.Context, key string) error {
	return p.blobs.Delete(ctx, p.prefix+key)
}

func (p *PrefixedStore) List(ctx context.Context, prefix string) ([]string, error) {
	keys, err := p.blobs.List(ctx, p.prefix+prefix)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, strings.TrimPrefix(k, p.prefix))
	}
	return out, nil
}

// MemoryBlobStore is an in-memory BlobStore for tests.
type MemoryBlobStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

// NewMemoryBlobStore returns an empty in-memory blob store.
func NewMemoryBlobStore() *MemoryBlobStore {
	return &MemoryBlobStore{data: make(map[string][]byte)}
}

func (m *MemoryBlobStore) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = append([]byte(nil), data...)
	return nil
}

func (m *MemoryBlobStore) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.data[key]
	if b == nil {
		return nil, nil
	}
	return append([]byte(nil), b...), nil
}

func (m *MemoryBlobStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *MemoryBlobStore) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for k := range m.data {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}
