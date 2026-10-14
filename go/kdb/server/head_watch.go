package server

import "sync"

// headWatchers are the callbacks waiting for this namespace's head to move: peer sync sessions
// that asked to be told (REFS_WATCH, docs/kdb-client-replica-plan.md §6). Each is called after a
// commit lands, from the committing goroutine, so it must only take note and return.
type headWatchers struct {
	mu   sync.Mutex
	next int
	fns  map[int]func()
}

// WatchHead calls fn after every commit that moves this namespace's head - local writes, a peer's
// push, a cross-namespace part - until cancel is called. fn must not block.
func (s *KdbServerRuntime) WatchHead(fn func()) (cancel func()) {
	w := &s.headWatch
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fns == nil {
		w.fns = map[int]func(){}
	}
	id := w.next
	w.next++
	w.fns[id] = fn
	return func() {
		w.mu.Lock()
		delete(w.fns, id)
		w.mu.Unlock()
	}
}

// notifyHeadWatchers runs at the same points as CommitListener.
func (s *KdbServerRuntime) notifyHeadWatchers() {
	w := &s.headWatch
	w.mu.Lock()
	fns := make([]func(), 0, len(w.fns))
	for _, fn := range w.fns {
		fns = append(fns, fn)
	}
	w.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
}
