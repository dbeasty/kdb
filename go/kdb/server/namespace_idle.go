package server

import (
	"log/slog"
	"sort"
	"sync"
	"time"
)

// Idle close (Phase 16.6, G6). A process holding a namespace per user and per match cannot keep
// them all open: each open namespace holds tens of kilobytes of heap however little it stores
// (docs/kdb-distributed-implementation-plan.md, Phase 16.6 measurements). The set closes the
// namespaces nobody has used for a while - and the least recently used when more than a cap are
// open - and reopens one through its opener the next time anything resolves it. A closed
// namespace stays known, so peers still see it and a sync reopens it.

// IdlePolicy configures idle close. Close is required; everything else has a default.
type IdlePolicy struct {
	// MaxOpen caps how many namespaces stay open; beyond it the least recently used idle ones
	// close first. 0 means no cap.
	MaxOpen int
	// IdleAfter closes a namespace unused for this long. 0 closes only to honour MaxOpen.
	IdleAfter time.Duration
	// MinIdle is how long a namespace must have been unused before it may close at all, even to
	// honour MaxOpen: a namespace in use is never closed under a caller. 0 means 30s.
	MinIdle time.Duration
	// Interval is how often to sweep; 0 means a quarter of IdleAfter, at most 30s.
	Interval time.Duration
	// Close releases a namespace's storage once the set has let go of it - an embed.Host's
	// CloseNamespace, and whatever else the application opened alongside it.
	Close func(rt *KdbServerRuntime) error
	// Pinned names namespaces never to close: a process's primary, say.
	Pinned func(ns string) bool
}

// touch records that rt was just used. Cheap enough for every Get: one atomic load, and a store
// only when the recorded second has changed.
func (rt *KdbServerRuntime) touch(now time.Time) {
	sec := now.Unix()
	if rt.lastUsed.Load() != sec {
		rt.lastUsed.Store(sec)
	}
}

// LastUsed is when the set last handed rt out.
func (rt *KdbServerRuntime) LastUsed() time.Time { return time.Unix(rt.lastUsed.Load(), 0) }

// busy reports whether rt has work in flight that closing would cut short.
func (rt *KdbServerRuntime) busy() bool {
	return rt.WriteQueueDepth() > 0 || rt.groupPublishing.Load() > 0
}

// AddKnown records namespaces that exist though they are not open - found on disk at startup,
// say - so peers see them and a sync opens them on demand.
func (s *NamespaceSet) AddKnown(namespaces ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.known == nil {
		s.known = map[string]struct{}{}
	}
	for _, ns := range namespaces {
		s.known[ns] = struct{}{}
	}
}

// KnownNamespaces lists every namespace the set holds or knows exists, sorted: what it serves.
func (s *NamespaceSet) KnownNamespaces() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seen := make(map[string]struct{}, len(s.runtimes)+len(s.known))
	out := make([]string, 0, len(s.runtimes)+len(s.known))
	for ns := range s.runtimes {
		seen[ns] = struct{}{}
		out = append(out, ns)
	}
	for ns := range s.known {
		if _, dup := seen[ns]; !dup {
			out = append(out, ns)
		}
	}
	sort.Strings(out)
	return out
}

// StartIdleClose sweeps the set by p until stop is called; stop waits for a sweep in flight.
func (s *NamespaceSet) StartIdleClose(p IdlePolicy) (stop func()) {
	interval := p.Interval
	if interval <= 0 {
		interval = 30 * time.Second
		if p.IdleAfter > 0 && p.IdleAfter/4 < interval {
			interval = p.IdleAfter / 4
		}
	}
	done, quit := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-quit:
				return
			case now := <-t.C:
				s.CloseIdle(p, now)
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(quit) })
		<-done
	}
}

// CloseIdle runs one sweep at now and returns the namespaces it closed.
func (s *NamespaceSet) CloseIdle(p IdlePolicy, now time.Time) []string {
	if p.Close == nil {
		return nil
	}
	minIdle := p.MinIdle
	if minIdle <= 0 {
		minIdle = 30 * time.Second
	}
	type cand struct {
		ns   string
		rt   *KdbServerRuntime
		last time.Time
	}
	s.mu.RLock()
	open := len(s.runtimes)
	var cands []cand
	for ns, rt := range s.runtimes {
		if p.Pinned != nil && p.Pinned(ns) {
			continue
		}
		if last := rt.LastUsed(); now.Sub(last) >= minIdle {
			cands = append(cands, cand{ns, rt, last})
		}
	}
	s.mu.RUnlock()
	sort.Slice(cands, func(i, j int) bool { return cands[i].last.Before(cands[j].last) })
	var closed []string
	for _, c := range cands {
		overCap := p.MaxOpen > 0 && open-len(closed) > p.MaxOpen
		stale := p.IdleAfter > 0 && now.Sub(c.last) >= p.IdleAfter
		if !overCap && !stale {
			continue
		}
		if s.closeOne(c.ns, c.rt, p) {
			closed = append(closed, c.ns)
		}
	}
	return closed
}

// closeOne lets go of one idle namespace: out of the set first, so nothing new reaches it, then
// drained, forgotten by the definitions store (a reopened runtime is a new one and must have its
// definitions applied afresh), then closed. A namespace that turned busy in between is left open.
func (s *NamespaceSet) closeOne(ns string, rt *KdbServerRuntime, p IdlePolicy) bool {
	s.openMu.Lock() // no Resolve reopening it while it is being closed
	defer s.openMu.Unlock()
	s.mu.Lock()
	if cur, ok := s.runtimes[ns]; !ok || cur != rt || rt.busy() {
		s.mu.Unlock()
		return false
	}
	delete(s.runtimes, ns)
	if s.known == nil {
		s.known = map[string]struct{}{}
	}
	s.known[ns] = struct{}{}
	s.mu.Unlock()
	rt.BeginDraining()
	rt.WaitForWritesToDrain(5 * time.Second)
	if rt.Meta != nil {
		rt.Meta.Forget(ns)
	}
	if err := p.Close(rt); err != nil {
		slog.Warn("closing an idle namespace failed", "namespace", ns, "error", err)
	}
	return true
}
