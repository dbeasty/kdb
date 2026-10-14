package peersync

import (
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/wire"
)

// REFS_WATCH (docs/kdb-client-replica-plan.md §6): a session asks to be told when the refs of
// some namespaces move, and the host sends REFS_CHANGED on the session's connection, under the
// watch's correlation id, whenever one does - so a client replica syncs on change instead of on a
// poll interval. Changes are coalesced: a burst of commits is one REFS_CHANGED, sent at most
// every watchCoalesce, carrying each changed namespace's refs as they are when it is sent.

const watchCoalesce = 25 * time.Millisecond

type refsWatch struct {
	correlation int
	cancels     []func()

	mu      sync.Mutex
	pending map[string]bool
	sending bool
	stopped bool
}

func (h *V2Host) startWatch(m wire.RefsWatchMessage) (wire.Message, error) {
	if h.cfg.WatchHead == nil || h.cfg.Send == nil {
		return nil, &UnsupportedFrameError{Type: wire.MsgRefsWatch}
	}
	// Authorized like a refs request, namespace by namespace, now and again on every change.
	refs, err := h.refs(m.Namespaces)
	if err != nil {
		return nil, err
	}
	h.stopWatch()
	w := &refsWatch{correlation: m.H.CorrelationID, pending: map[string]bool{}}
	for _, ns := range m.Namespaces {
		ns := ns
		cancel, err := h.cfg.WatchHead(ns, func() { h.changed(w, ns) })
		if err != nil {
			w.cancelAll()
			return nil, err
		}
		w.cancels = append(w.cancels, cancel)
	}
	h.mu.Lock()
	h.watch = w
	h.mu.Unlock()
	return wire.RefsChangedMessage{H: header(wire.MsgRefsChanged, m.H.CorrelationID), Refs: refs}, nil
}

func (w *refsWatch) cancelAll() {
	for _, c := range w.cancels {
		c()
	}
	w.mu.Lock()
	w.stopped = true
	w.mu.Unlock()
}

func (h *V2Host) stopWatch() {
	h.mu.Lock()
	w := h.watch
	h.watch = nil
	h.mu.Unlock()
	if w != nil {
		w.cancelAll()
	}
}

// changed notes that ns moved; it runs on the committing goroutine, so it only records and wakes
// the sender.
func (h *V2Host) changed(w *refsWatch, ns string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return
	}
	w.pending[ns] = true
	if !w.sending {
		w.sending = true
		go h.sendChanges(w)
	}
}

func (h *V2Host) sendChanges(w *refsWatch) {
	for {
		time.Sleep(watchCoalesce)
		w.mu.Lock()
		if w.stopped || len(w.pending) == 0 {
			w.sending = false
			w.mu.Unlock()
			return
		}
		nss := make([]string, 0, len(w.pending))
		for ns := range w.pending {
			nss = append(nss, ns)
		}
		w.pending = map[string]bool{}
		w.mu.Unlock()
		// One namespace at a time, so one whose grant was revoked meanwhile drops out alone.
		var refs []wire.NamespaceRefs
		for _, ns := range nss {
			if r, err := h.refs([]string{ns}); err == nil {
				refs = append(refs, r...)
			}
		}
		if len(refs) == 0 {
			continue
		}
		frame, err := h.wire.Encode(wire.RefsChangedMessage{H: header(wire.MsgRefsChanged, w.correlation), Refs: refs})
		if err == nil {
			err = h.cfg.Send(frame)
		}
		if err != nil {
			// The connection is gone: the session's Close cancels the watch.
			w.mu.Lock()
			w.sending = false
			w.mu.Unlock()
			return
		}
	}
}
