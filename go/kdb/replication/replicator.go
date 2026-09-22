package replication

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Config configures a Replicator.
type Config struct {
	// NodeID is this node's identity, sent to every peer.
	NodeID string
	// Local is this node's namespaces.
	Local peersync.NamespaceProvider
	// Peers are the configured peers.
	Peers []PeerConfig
	// State stores per-peer progress.
	State *StateStore
	// Transport returns the transport to reach a peer with; nil uses TCP with TLS (the scheme
	// decides - tcps:// requires it).
	Transport func(PeerConfig) stream.Transport
	// TLS is the client TLS configuration for tcps:// peers.
	TLS *core.TransportTlsSettings
	// Debounce is how long a commit trigger waits for more commits before syncing; 0 means 50ms.
	Debounce time.Duration
	// MaxBackoff caps the retry delay after consecutive failures; 0 means 10 minutes.
	MaxBackoff time.Duration
	// Timeout bounds each request in a sync; 0 means peersync's default.
	Timeout time.Duration
	// Projections opens (creating if need be) the local namespace that keeps the projection of
	// a source namespace through a filter. Required only for filtered peers.
	Projections func(source, filter string, writeBack bool) (peersync.ProjectionTarget, error)
}

// Replicator runs one sync loop per configured peer.
type Replicator struct {
	cfg   Config
	loops map[string]*peerLoop
	order []string
	wg    sync.WaitGroup
	stop  chan struct{}
	once  sync.Once
}

// New returns a replicator for cfg's peers; Start runs it.
func New(cfg Config) (*Replicator, error) {
	if cfg.Local == nil {
		return nil, errors.New("replication: Local is required")
	}
	if cfg.State == nil {
		st, _ := NewStateStore("")
		cfg.State = st
	}
	if cfg.Debounce <= 0 {
		cfg.Debounce = 50 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 10 * time.Minute
	}
	r := &Replicator{cfg: cfg, loops: map[string]*peerLoop{}, stop: make(chan struct{})}
	for _, p := range cfg.Peers {
		if _, dup := r.loops[p.Name]; dup {
			return nil, fmt.Errorf("replication: peer %q configured twice", p.Name)
		}
		if p.Interval <= 0 {
			p.Interval = DefaultInterval
		}
		r.loops[p.Name] = &peerLoop{r: r, peer: p, trigger: make(chan struct{}, 1), done: make(chan struct{})}
		r.order = append(r.order, p.Name)
	}
	sort.Strings(r.order)
	return r, nil
}

// Start runs every peer's loop. Each syncs once immediately.
func (r *Replicator) Start() {
	for _, name := range r.order {
		l := r.loops[name]
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			l.run()
		}()
		l.kick()
	}
}

// Stop ends every loop, waiting for a sync in flight to finish rather than cutting it short.
func (r *Replicator) Stop() {
	r.once.Do(func() { close(r.stop) })
	r.wg.Wait()
}

// OnLocalCommit tells the replicator ns changed: every peer syncing ns syncs soon.
func (r *Replicator) OnLocalCommit(ns string) {
	for _, name := range r.order {
		l := r.loops[name]
		if len(peersync.SelectNamespaces(l.namespaces(), []string{ns})) == 1 {
			l.kick()
		}
	}
}

// SetPeerNamespaces replaces the namespace patterns synced with the named peer, while the
// replicator runs: the peer's next cycle - started now - syncs the new set. Progress recorded for
// namespaces that stay in the set is kept, so nothing is re-sent from scratch. How an application
// whose working set changes at runtime (a user signing in, joining a match) follows it without
// restarting replication and losing every peer's watermarks and backoff.
func (r *Replicator) SetPeerNamespaces(name string, patterns []string) error {
	l, ok := r.loops[name]
	if !ok {
		return fmt.Errorf("replication: no peer named %q", name)
	}
	if len(patterns) == 0 {
		return fmt.Errorf("replication: peer %q needs at least one namespace pattern", name)
	}
	if l.peer.Filter != "" {
		return fmt.Errorf("replication: peer %q is filtered; its source namespace is fixed", name)
	}
	l.mu.Lock()
	l.peer.Namespaces = append([]string(nil), patterns...)
	l.mu.Unlock()
	l.kick()
	return nil
}

// AddNamespaces adds namespaces (or patterns) to those synced with the named peer, skipping any
// the peer's patterns already select, and syncs soon.
func (r *Replicator) AddNamespaces(name string, namespaces ...string) error {
	l, ok := r.loops[name]
	if !ok {
		return fmt.Errorf("replication: no peer named %q", name)
	}
	if l.peer.Filter != "" {
		return fmt.Errorf("replication: peer %q is filtered; its source namespace is fixed", name)
	}
	l.mu.Lock()
	added := false
	for _, ns := range namespaces {
		if len(peersync.SelectNamespaces(l.peer.Namespaces, []string{ns})) == 1 || containsString(l.peer.Namespaces, ns) {
			continue
		}
		l.peer.Namespaces = append(append([]string(nil), l.peer.Namespaces...), ns)
		added = true
	}
	l.mu.Unlock()
	if added {
		l.kick()
	}
	return nil
}

// RemoveNamespaces stops syncing namespaces (exact patterns, as added) with the named peer. What
// was synced stays local; only replication of it stops.
func (r *Replicator) RemoveNamespaces(name string, namespaces ...string) error {
	l, ok := r.loops[name]
	if !ok {
		return fmt.Errorf("replication: no peer named %q", name)
	}
	drop := map[string]bool{}
	for _, ns := range namespaces {
		drop[ns] = true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var kept []string
	for _, p := range l.peer.Namespaces {
		if !drop[p] {
			kept = append(kept, p)
		}
	}
	if len(kept) == 0 {
		return fmt.Errorf("replication: peer %q would be left with no namespaces", name)
	}
	l.peer.Namespaces = kept
	return nil
}

func containsString(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// SyncNow runs one sync with the named peer and waits for it.
func (r *Replicator) SyncNow(name string) (peersync.V2Result, error) {
	l, ok := r.loops[name]
	if !ok {
		return peersync.V2Result{}, fmt.Errorf("replication: no peer named %q", name)
	}
	return l.cycle()
}

// PeerStatus is one peer's status for operators.
type PeerStatus struct {
	Name       string
	Addr       string
	Namespaces []string
	Mode       string
	State      PeerState
	// Failing is true while the peer's last attempt failed.
	Failing bool
	// NextAttempt is when the loop will next sync if nothing triggers it sooner.
	NextAttempt time.Time
}

// Status reports every peer, by name.
func (r *Replicator) Status() []PeerStatus {
	out := make([]PeerStatus, 0, len(r.order))
	for _, name := range r.order {
		l := r.loops[name]
		st, _ := r.cfg.State.Load(name)
		l.mu.Lock()
		next := l.nextAttempt
		l.mu.Unlock()
		out = append(out, PeerStatus{
			Name: name, Addr: l.peer.Addr, Namespaces: l.namespaces(), Mode: modeName(l.peer.Mode),
			State: st, Failing: st.ConsecutiveFailures > 0, NextAttempt: next,
		})
	}
	return out
}

func modeName(m peersync.SyncMode) string {
	switch m {
	case peersync.SyncPull:
		return "pull"
	case peersync.SyncPush:
		return "push"
	default:
		return "both"
	}
}

type peerLoop struct {
	r       *Replicator
	peer    PeerConfig
	trigger chan struct{}
	done    chan struct{}

	cycleMu     sync.Mutex // one sync at a time per peer
	mu          sync.Mutex
	nextAttempt time.Time
}

// namespaces is the peer's namespace patterns as of now: SetPeerNamespaces and AddNamespaces
// change them while the loop runs, and each cycle reads them afresh.
func (l *peerLoop) namespaces() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.peer.Namespaces
}

func (l *peerLoop) kick() {
	select {
	case l.trigger <- struct{}{}:
	default:
	}
}

func (l *peerLoop) run() {
	timer := time.NewTimer(l.peer.Interval)
	defer timer.Stop()
	for {
		select {
		case <-l.r.stop:
			return
		case <-l.trigger:
			// Let a burst of commits settle into one sync.
			select {
			case <-time.After(l.r.cfg.Debounce):
			case <-l.r.stop:
				return
			}
			l.drainTrigger()
		case <-timer.C:
		}
		_, err := l.cycle()
		delay := l.peer.Interval
		if err != nil {
			delay = l.backoff()
		}
		l.mu.Lock()
		l.nextAttempt = time.Now().Add(delay)
		l.mu.Unlock()
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(delay)
	}
}

func (l *peerLoop) drainTrigger() {
	select {
	case <-l.trigger:
	default:
	}
}

// backoff is the delay before retrying after the failures recorded so far: the interval doubled
// per consecutive failure, capped, with ±20% jitter so peers that failed together do not retry
// together.
func (l *peerLoop) backoff() time.Duration {
	st, _ := l.r.cfg.State.Load(l.peer.Name)
	d := l.peer.Interval
	for i := 1; i < st.ConsecutiveFailures && d < l.r.cfg.MaxBackoff; i++ {
		d *= 2
	}
	if d > l.r.cfg.MaxBackoff {
		d = l.r.cfg.MaxBackoff
	}
	jitter := time.Duration(rand.Int63n(int64(d)/5+1)) - d/10
	return d + jitter
}

// cycle runs one sync with the peer and records the outcome.
func (l *peerLoop) cycle() (peersync.V2Result, error) {
	l.cycleMu.Lock()
	defer l.cycleMu.Unlock()
	st, err := l.r.cfg.State.Load(l.peer.Name)
	if err != nil {
		return peersync.V2Result{}, err
	}
	extra := map[string][]codec.Hash{}
	for ns, progress := range st.Namespaces {
		for _, hex := range progress.PendingTips {
			if h, err := codec.HashFromHex(hex); err == nil {
				extra[ns] = append(extra[ns], h)
			}
		}
		// The peer's main as of the last clean sync is a commit both sides hold, and usually the
		// newest one: as a have it pins the merge base almost exactly. Without it the exponentially
		// spaced haves over-send up to the local divergence - the whole shared history once the
		// local side has diverged further than that history is long (Phase 15, M1).
		if h, err := codec.HashFromHex(progress.RemoteMain); err == nil {
			extra[ns] = append(extra[ns], h)
		}
	}
	transport := l.transport()
	if l.peer.Filter != "" {
		return peersync.V2Result{}, l.projectionCycle(st, transport)
	}
	// A namespace that syncs cleanly holds what the peer had when the session began, not when it
	// ended: that is the retention floor to record.
	started := time.Now().UTC()
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), transport, peersync.V2ClientConfig{
		NodeID: l.r.cfg.NodeID, PeerURI: l.peer.Addr, TLS: l.r.cfg.TLS,
		ConnectionContext: auth.ConnectionContext{User: l.peer.User, Password: l.peer.Password},
		Namespaces:        l.namespaces(), Mode: l.peer.Mode, Local: l.r.cfg.Local,
		CreateLocal: l.peer.CreateLocal, ExtraHaves: extra, Timeout: l.r.cfg.Timeout,
		PreferSnapshot: l.peer.PreferSnapshot,
	})
	now := time.Now().UTC()
	st.LastAttempt = now
	if err == nil {
		st.PeerNodeID = res.RemoteNodeID
		for _, ns := range res.Namespaces {
			cur := st.Namespaces[ns.Namespace]
			cur.Pulled += int64(ns.Pulled)
			cur.Pushed += int64(ns.Pushed)
			cur.OpenConflicts = len(ns.Conflicts)
			if ns.Err != nil {
				cur.LastError = ns.Err.Error()
				// Keep what an interrupted pull stored so the next attempt resumes it.
				for _, h := range ns.ReceivedTips {
					cur.PendingTips = append(cur.PendingTips, h.Hex())
				}
				if err == nil {
					err = fmt.Errorf("namespace %s: %w", ns.Namespace, ns.Err)
				}
			} else {
				cur.LastError, cur.PendingTips = "", nil
				cur.LocalMain, cur.RemoteMain, cur.LastSync = ns.LocalMain, ns.RemoteMain, started
			}
			st.Namespaces[ns.Namespace] = cur
			if ns.Snapshot != "" {
				slog.Info("replication: bootstrapped a namespace from a peer's snapshot", "peer", l.peer.Name,
					"namespace", ns.Namespace, "root", ns.Snapshot)
			}
			if len(ns.Grafted) > 0 {
				slog.Info("replication: grafted an unrelated history", "peer", l.peer.Name, "namespace", ns.Namespace, "roots", ns.Grafted)
			}
		}
		if l.peer.Deepen {
			l.deepen(res.Namespaces, transport)
		}
	}
	if err != nil {
		st.ConsecutiveFailures++
		st.LastError = err.Error()
		slog.Warn("replication: sync failed", "peer", l.peer.Name, "addr", l.peer.Addr,
			"consecutiveFailures", st.ConsecutiveFailures, "error", err)
	} else {
		st.ConsecutiveFailures, st.LastError, st.LastSuccess = 0, "", now
	}
	if serr := l.r.cfg.State.Save(st); serr != nil && err == nil {
		err = serr
	}
	return res, err
}

func (l *peerLoop) transport() stream.Transport {
	if l.r.cfg.Transport != nil {
		return l.r.cfg.Transport(l.peer)
	}
	return defaultTransport(l.r.cfg.TLS)
}

// WriteMetrics appends the replicator's Prometheus families to b.
func (r *Replicator) WriteMetrics(b *strings.Builder) {
	status := r.Status()
	b.WriteString("# HELP kdb_replication_last_success_seconds Unix time of the last successful sync with a peer.\n")
	b.WriteString("# TYPE kdb_replication_last_success_seconds gauge\n")
	for _, p := range status {
		var ts float64
		if !p.State.LastSuccess.IsZero() {
			ts = float64(p.State.LastSuccess.UnixMilli()) / 1000
		}
		fmt.Fprintf(b, "kdb_replication_last_success_seconds{peer=%q} %g\n", p.Name, ts)
	}
	b.WriteString("# HELP kdb_replication_consecutive_failures Syncs with a peer that have failed in a row.\n")
	b.WriteString("# TYPE kdb_replication_consecutive_failures gauge\n")
	for _, p := range status {
		fmt.Fprintf(b, "kdb_replication_consecutive_failures{peer=%q} %d\n", p.Name, p.State.ConsecutiveFailures)
	}
	b.WriteString("# HELP kdb_replication_commits_total Commits new to the receiving side, per peer, namespace and direction.\n")
	b.WriteString("# TYPE kdb_replication_commits_total counter\n")
	for _, p := range status {
		names := make([]string, 0, len(p.State.Namespaces))
		for ns := range p.State.Namespaces {
			names = append(names, ns)
		}
		sort.Strings(names)
		for _, ns := range names {
			st := p.State.Namespaces[ns]
			fmt.Fprintf(b, "kdb_replication_commits_total{peer=%q,namespace=%q,direction=\"pull\"} %d\n", p.Name, ns, st.Pulled)
			fmt.Fprintf(b, "kdb_replication_commits_total{peer=%q,namespace=%q,direction=\"push\"} %d\n", p.Name, ns, st.Pushed)
		}
	}
}

// PeerFloor is the oldest moment every active peer this node pushes ns to is known to have
// caught up to: for each peer that syncs ns in a mode that pushes, and has succeeded within
// grace, the time of its last successful sync of ns. Retention must keep everything newer, or a
// peer that is only behind would find the commits it needs deleted. A peer silent for longer than
// grace stops holding retention back - otherwise one abandoned laptop pins history forever - and
// catches up by snapshot if it ever returns.
func (r *Replicator) PeerFloor(ns string, grace time.Duration, now time.Time) (time.Time, bool) {
	var floor time.Time
	found := false
	for _, name := range r.order {
		l := r.loops[name]
		if l.peer.Mode&peersync.SyncPush == 0 {
			continue
		}
		if len(peersync.SelectNamespaces(l.namespaces(), []string{ns})) == 0 {
			continue
		}
		st, err := r.cfg.State.Load(name)
		if err != nil || st.LastSuccess.IsZero() || now.Sub(st.LastSuccess) > grace {
			continue
		}
		synced := st.Namespaces[ns].LastSync
		if synced.IsZero() {
			continue
		}
		if !found || synced.Before(floor) {
			floor, found = synced, true
		}
	}
	return floor, found
}

// projectionCycle runs one sync of a filtered peer and records it like any other.
func (l *peerLoop) projectionCycle(st PeerState, transport stream.Transport) error {
	source := l.namespaces()[0]
	var err error
	var res peersync.ProjectionResult
	var started time.Time
	if l.r.cfg.Projections == nil {
		err = errors.New("replication: a filtered peer needs a projection target, and none is configured")
	} else {
		started = time.Now().UTC()
		var target peersync.ProjectionTarget
		if target, err = l.r.cfg.Projections(source, l.peer.Filter, l.peer.WriteBack); err == nil {
			res, err = peersync.SyncProjection(wire.NewCodec(wire.EncodingJSON), transport, peersync.ProjectionConfig{
				NodeID: l.r.cfg.NodeID, PeerURI: l.peer.Addr, TLS: l.r.cfg.TLS,
				ConnectionContext: auth.ConnectionContext{User: l.peer.User, Password: l.peer.Password},
				Namespace:         source, Filter: l.peer.Filter, Timeout: l.r.cfg.Timeout, Target: target,
				WriteBack: l.peer.WriteBack,
			})
		}
	}
	now := time.Now().UTC()
	st.LastAttempt = now
	ns := peersync.ProjectionNamespace(source, l.peer.Filter)
	cur := st.Namespaces[ns]
	if err != nil {
		st.ConsecutiveFailures++
		st.LastError, cur.LastError = err.Error(), err.Error()
		slog.Warn("replication: projection sync failed", "peer", l.peer.Name, "error", err)
	} else {
		st.ConsecutiveFailures, st.LastError, st.LastSuccess = 0, "", now
		cur.LastError, cur.RemoteMain, cur.LastSync = "", res.Source, started
		cur.Pulled += int64(res.Writes + res.Deletes)
		cur.Pushed += int64(res.Written)
	}
	st.Namespaces[ns] = cur
	if serr := l.r.cfg.State.Save(st); serr != nil && err == nil {
		err = serr
	}
	return err
}

// FetchBodies asks the configured peers, in name order, for the bodies of wanted (document id to
// the content hash it must have) in namespace ns - what a scrub needs to repair documents it can
// no longer read. Each peer is asked only for what the peers before it did not supply, and every
// body returned has been verified against its content hash (peersync.RepairSession.Fetch), so a
// peer need not be trusted. Filtered peers, which hold only part of a namespace, are skipped, as
// are peers whose patterns do not cover ns. A peer that cannot be reached is skipped too; the
// error returned is the last such failure, and only when nothing at all could be fetched.
func (r *Replicator) FetchBodies(ns string, wanted map[codec.UUID]codec.Hash, treeHex string) (map[codec.UUID]string, error) {
	out := map[codec.UUID]string{}
	var lastErr error
	for _, name := range r.healthiestFirst() {
		if len(out) == len(wanted) {
			break
		}
		l := r.loops[name]
		if l.peer.Filter != "" || len(peersync.SelectNamespaces(l.namespaces(), []string{ns})) == 0 {
			continue
		}
		rest := map[codec.UUID]codec.Hash{}
		for id, h := range wanted {
			if _, got := out[id]; !got {
				rest[id] = h
			}
		}
		s, err := peersync.OpenRepairSession(wire.NewCodec(wire.EncodingJSON), l.transport(), peersync.V2ClientConfig{
			NodeID: r.cfg.NodeID, PeerURI: l.peer.Addr, TLS: r.cfg.TLS,
			ConnectionContext: auth.ConnectionContext{User: l.peer.User, Password: l.peer.Password},
			Namespaces:        []string{ns}, Timeout: r.cfg.Timeout,
		})
		if err != nil {
			lastErr = fmt.Errorf("replication: peer %s: %w", name, err)
			continue
		}
		got, err := s.Fetch(ns, rest, treeHex)
		s.Close()
		for id, body := range got {
			out[id] = body
		}
		if err != nil {
			lastErr = fmt.Errorf("replication: peer %s: %w", name, err)
		}
	}
	if len(out) == 0 && lastErr != nil {
		return out, lastErr
	}
	return out, nil
}

// Compare finds the documents this node's local tree holds differently from peer name's head for
// namespace ns, by Merkle diff (peersync.RepairSession.Diff). It returns the peer's tree compared
// against.
func (r *Replicator) Compare(name, ns string, local document.DocumentTree) (string, []document.TreeDifference, error) {
	l, ok := r.loops[name]
	if !ok {
		return "", nil, fmt.Errorf("replication: no peer %q", name)
	}
	s, err := peersync.OpenRepairSession(wire.NewCodec(wire.EncodingJSON), l.transport(), peersync.V2ClientConfig{
		NodeID: r.cfg.NodeID, PeerURI: l.peer.Addr, TLS: r.cfg.TLS,
		ConnectionContext: auth.ConnectionContext{User: l.peer.User, Password: l.peer.Password},
		Namespaces:        []string{ns}, Timeout: r.cfg.Timeout,
	})
	if err != nil {
		return "", nil, err
	}
	defer s.Close()
	return s.Diff(ns, local, "")
}

// OpenDocSession connects to the filtered peer whose projection lives in projectionNS, for reading
// source documents the projection does not hold (DOC_FETCH).
func (r *Replicator) OpenDocSession(projectionNS string) (*peersync.RepairSession, string, error) {
	for _, name := range r.order {
		l := r.loops[name]
		if l.peer.Filter == "" || peersync.ProjectionNamespace(l.namespaces()[0], l.peer.Filter) != projectionNS {
			continue
		}
		s, err := peersync.OpenDocSession(wire.NewCodec(wire.EncodingJSON), l.transport(), peersync.V2ClientConfig{
			NodeID: r.cfg.NodeID, PeerURI: l.peer.Addr, TLS: r.cfg.TLS,
			ConnectionContext: auth.ConnectionContext{User: l.peer.User, Password: l.peer.Password},
			Timeout:           r.cfg.Timeout,
		})
		return s, l.namespaces()[0], err
	}
	return nil, "", fmt.Errorf("replication: no filtered peer keeps its projection in %s", projectionNS)
}

// deepen fetches, from this peer, the history below the shallow roots of every namespace the sync
// just brought up to date (PeerConfig.Deepen). A failure is logged, not the sync's: the namespace
// is as synced as it was, and the next cycle tries again.
func (l *peerLoop) deepen(synced []peersync.NamespaceSyncResult, transport stream.Transport) {
	for _, ns := range synced {
		if ns.Err != nil {
			continue
		}
		env, err := l.r.cfg.Local.Env(ns.Namespace, false)
		if err != nil || len(env.DAG.ShallowRoots()) == 0 {
			continue
		}
		s, err := peersync.OpenRepairSession(wire.NewCodec(wire.EncodingJSON), transport, peersync.V2ClientConfig{
			NodeID: l.r.cfg.NodeID, PeerURI: l.peer.Addr, TLS: l.r.cfg.TLS,
			ConnectionContext: auth.ConnectionContext{User: l.peer.User, Password: l.peer.Password},
			Namespaces:        []string{ns.Namespace}, Timeout: l.r.cfg.Timeout,
		})
		if err != nil {
			slog.Warn("replication: deepen could not connect", "peer", l.peer.Name, "namespace", ns.Namespace, "error", err)
			continue
		}
		env.Peer = s.Peer
		results, err := peersync.DeepenAll(env, s)
		s.Close()
		fetched := 0
		for _, r := range results {
			fetched += r.Fetched
		}
		if err != nil {
			slog.Warn("replication: deepen failed", "peer", l.peer.Name, "namespace", ns.Namespace, "error", err)
		} else if fetched > 0 {
			slog.Info("replication: fetched history below a snapshot", "peer", l.peer.Name, "namespace", ns.Namespace,
				"commits", fetched, "remainingShallowRoots", len(env.DAG.ShallowRoots()))
		}
	}
}

// OpenRepairSessionTo connects to peer name for namespace ns - for deepen and other repairs asked
// for by hand.
func (r *Replicator) OpenRepairSessionTo(name, ns string) (*peersync.RepairSession, error) {
	l, ok := r.loops[name]
	if !ok {
		return nil, fmt.Errorf("replication: no peer %q", name)
	}
	return peersync.OpenRepairSession(wire.NewCodec(wire.EncodingJSON), l.transport(), peersync.V2ClientConfig{
		NodeID: r.cfg.NodeID, PeerURI: l.peer.Addr, TLS: r.cfg.TLS,
		ConnectionContext: auth.ConnectionContext{User: l.peer.User, Password: l.peer.Password},
		Namespaces:        []string{ns}, Timeout: r.cfg.Timeout,
	})
}

// healthiestFirst orders the peers for a repair request: fewest consecutive failures first, then
// the most recent success. A peer that is down is asked last rather than first, so a repair does
// not wait out its timeout before trying one that answers. (The Phase 15 stand-in for a phi-accrual
// detector: sync outcomes, not heartbeats, are what this replicator observes about a peer.)
func (r *Replicator) healthiestFirst() []string {
	type ranked struct {
		name     string
		failures int
		success  time.Time
	}
	peers := make([]ranked, 0, len(r.order))
	for _, name := range r.order {
		st, err := r.cfg.State.Load(name)
		if err != nil {
			peers = append(peers, ranked{name: name, failures: 1 << 30})
			continue
		}
		peers = append(peers, ranked{name: name, failures: st.ConsecutiveFailures, success: st.LastSuccess})
	}
	sort.SliceStable(peers, func(i, j int) bool {
		if peers[i].failures != peers[j].failures {
			return peers[i].failures < peers[j].failures
		}
		return peers[i].success.After(peers[j].success)
	})
	out := make([]string, len(peers))
	for i, p := range peers {
		out[i] = p.name
	}
	return out
}
