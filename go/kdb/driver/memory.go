package driver

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

type memoryKey struct {
	catalog     string
	namespaceID string
	isolate     string
}

type memoryEntry struct {
	runtime  *embed.EmbeddedKdbRuntime
	refCount int
}

type memoryPool struct {
	mu      sync.Mutex
	entries map[memoryKey]*memoryEntry
}

func newMemoryPool() *memoryPool {
	return &memoryPool{entries: make(map[memoryKey]*memoryEntry)}
}

func (r *memoryPool) clearAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = make(map[memoryKey]*memoryEntry)
}

func (r *memoryPool) acquire(parsed ParsedURL) (*embed.EmbeddedKdbRuntime, func(), error) {
	key := memoryKey{
		catalog:     parsed.Catalog,
		namespaceID: parsed.NamespaceID,
		isolate:     isolateKey(parsed.MemoryParams),
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	ent, ok := r.entries[key]
	if !ok {
		rt, err := embed.OpenMemoryRuntime(parsed.Catalog, parsed.NamespaceID, schema.None())
		if err != nil {
			return nil, nil, err
		}
		ent = &memoryEntry{runtime: rt}
		r.entries[key] = ent
	}
	ent.refCount++
	release := func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		ent.refCount--
		if ent.refCount <= 0 && dropOnClose(parsed.MemoryParams) {
			delete(r.entries, key)
		}
	}
	return ent.runtime, release, nil
}

func isolateKey(params map[string]string) string {
	if params["unique"] == "true" {
		var b [16]byte
		_, _ = rand.Read(b[:])
		return hex.EncodeToString(b[:])
	}
	return params["isolate"]
}

func dropOnClose(params map[string]string) bool {
	return params["dropOnClose"] == "true"
}

var sharedMemory = newMemoryPool()

func openRuntime(parsed ParsedURL) (*embed.EmbeddedKdbRuntime, func(), error) {
	switch parsed.Mode {
	case ModeMemory:
		return sharedMemory.acquire(parsed)
	case ModeFile:
		if parsed.DataRoot == "" {
			return nil, nil, fmt.Errorf("file URL missing data root")
		}
		rt, err := embed.OpenFileRuntime(parsed.DataRoot, parsed.Catalog, parsed.NamespaceID, schema.None())
		if err != nil {
			return nil, nil, err
		}
		return rt, func() {
			if rt != nil {
				rt.Close()
			}
		}, nil
	default:
		return nil, nil, fmt.Errorf("unsupported mode")
	}
}
