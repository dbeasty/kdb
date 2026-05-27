package server

import (
	"sync"
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
)

// KdbServerRuntime wraps an embedded runtime with write coordination (skeleton).
type KdbServerRuntime struct {
	Runtime *embed.EmbeddedKdbRuntime

	refCount   atomic.Int32
	closeMu    sync.Mutex
}

// NewKdbServerRuntime creates a server runtime with ref-count 1.
func NewKdbServerRuntime(rt *embed.EmbeddedKdbRuntime) *KdbServerRuntime {
	s := &KdbServerRuntime{Runtime: rt}
	s.refCount.Store(1)
	return s
}

// Retain increments the reference count.
func (s *KdbServerRuntime) Retain() {
	s.refCount.Add(1)
}

// Release decrements the reference count; v1 does not tear down storage.
func (s *KdbServerRuntime) Release() {
	if s.refCount.Add(-1) > 0 {
		return
	}
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.refCount.Load() > 0 {
		return
	}
}

// Commit is a placeholder for commit-via-engine (not yet ported).
func (s *KdbServerRuntime) Commit(namespaceID string, tx document.Transaction, sessionID string) (document.Commit, error) {
	_ = namespaceID
	_ = tx
	_ = sessionID
	return document.Commit{}, errNotImplemented("commit")
}

// ServerRuntimeRegistry holds shared server runtimes by key.
type ServerRuntimeRegistry struct {
	mu       sync.Mutex
	runtimes map[string]*KdbServerRuntime
}

// NewServerRuntimeRegistry returns an empty registry.
func NewServerRuntimeRegistry() *ServerRuntimeRegistry {
	return &ServerRuntimeRegistry{runtimes: make(map[string]*KdbServerRuntime)}
}

// GetOrOpen returns an existing runtime or opens a new one.
func (r *ServerRuntimeRegistry) GetOrOpen(key string, open func() (*KdbServerRuntime, error)) (*KdbServerRuntime, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rt, ok := r.runtimes[key]; ok {
		rt.Retain()
		return rt, nil
	}
	rt, err := open()
	if err != nil {
		return nil, err
	}
	rt.Retain()
	r.runtimes[key] = rt
	rt.Retain()
	return rt, nil
}

// Release releases a registry entry reference.
func (r *ServerRuntimeRegistry) Release(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rt, ok := r.runtimes[key]; ok {
		rt.Release()
	}
}

func errNotImplemented(op string) error {
	return &notImplementedError{op: op}
}

type notImplementedError struct{ op string }

func (e *notImplementedError) Error() string {
	return "kdb server: " + e.op + " not yet implemented in Go port"
}
