package control

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/limidus/kdb/go/kdb/auth"
)

// commitEvent is one committed write, as an SSE subscriber sees it.
type commitEvent struct {
	Namespace string `json:"namespace"`
	Hash      string `json:"hash"`
	ShortHash string `json:"shortHash"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
	OpCount   int    `json:"opCount"`
}

// eventHub fans commits out to connected subscribers.
//
// Every send is non-blocking against a small per-subscriber buffer, and a subscriber that cannot
// keep up loses events rather than applying backpressure. That is deliberate and it is the whole
// reason this is safe to call from CommitListener: that listener runs synchronously inside the
// transaction's success path, so a slow browser must never be able to slow down a commit. The UI
// treats an event as "something changed, refresh", not as a log it must receive in full, so a
// dropped event costs nothing but a slightly later refresh.
type eventHub struct {
	mu     sync.Mutex
	next   int
	subs   map[int]chan commitEvent
	closed bool
}

func newEventHub() *eventHub {
	return &eventHub{subs: make(map[int]chan commitEvent)}
}

func (h *eventHub) subscribe() (int, <-chan commitEvent, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0, nil, false
	}
	id := h.next
	h.next++
	ch := make(chan commitEvent, 16)
	h.subs[id] = ch
	return id, ch, true
}

func (h *eventHub) unsubscribe(id int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if ch, ok := h.subs[id]; ok {
		delete(h.subs, id)
		close(ch)
	}
}

func (h *eventHub) publish(ev commitEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range h.subs {
		select {
		case ch <- ev:
		default:
			// Full: this subscriber is behind. Drop, for the reason in the type comment.
		}
	}
}

func (h *eventHub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for id, ch := range h.subs {
		delete(h.subs, id)
		close(ch)
	}
}

func (h *eventHub) subscriberCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

// handleEvents streams commits as they happen (text/event-stream).
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, _ *serverRuntime) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "no_flush",
			"this connection cannot stream; SSE requires a flushable response writer")
		return
	}
	id, ch, ok := s.events.subscribe()
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "shutting_down", "the control plane is closing")
		return
	}
	defer s.events.unsubscribe(id)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Connection", "keep-alive")
	// Without this, a reverse proxy that buffers will hold every event until the response ends,
	// which for a stream is never.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	// An immediate comment frame so the client's EventSource fires onopen even when the namespace
	// is idle, rather than sitting in a connecting state that looks like a hang.
	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case ev, open := <-ch:
			if !open {
				return
			}
			if ev.Namespace != "" && ns != "" && ev.Namespace != ns {
				continue
			}
			payload, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: commit\ndata: %s\n\n", payload)
			flusher.Flush()
		}
	}
}
