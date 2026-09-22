package server

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/peersync"
)

// ConflictWebhook tells a resolver authority about the conflicts its namespaces hand it: every
// queued entry flagged for the authority, not yet delivered, and addressed to this node (its
// chain names this node, or no node). Each is POSTed as JSON; a 2xx marks it delivered, anything
// else is retried on the next pass. Delivery is at least once - an entry re-recorded with a
// changed report is sent again - so a receiver deduplicates on the X-KDB-Delivery header.
//
// The body is signed with HMAC-SHA256 over the raw bytes, keyed by Secret, in X-KDB-Signature as
// "sha256=<hex>", so the receiver can refuse anything this node did not send.
type ConflictWebhook struct {
	URL    string
	Secret string
	// Interval is how often undelivered entries are retried; 0 means 10 seconds. A new entry
	// is sent at once, not on the next interval.
	Interval time.Duration
	Client   *http.Client

	set  *NamespaceSet
	node string
	kick chan struct{}
	stop chan struct{}
	done chan struct{}
	once sync.Once

	// passMu serializes delivery passes; hooked records the queues whose OnRecord already kicks
	// this webhook.
	passMu sync.Mutex
	hooked map[*peersync.ConflictQueue]bool
}

// ConflictNotification is the webhook's JSON body.
type ConflictNotification struct {
	Type      string                 `json:"type"` // always "kdb.conflict"
	Node      string                 `json:"node"`
	Namespace string                 `json:"namespace"`
	Conflict  peersync.ConflictEntry `json:"conflict"`
}

// StartConflictWebhook starts delivering set's authority conflicts to w.URL from node.
func StartConflictWebhook(set *NamespaceSet, node string, w *ConflictWebhook) *ConflictWebhook {
	w.set, w.node = set, node
	if w.Interval <= 0 {
		w.Interval = 10 * time.Second
	}
	if w.Client == nil {
		w.Client = &http.Client{Timeout: 10 * time.Second}
	}
	w.kick = make(chan struct{}, 1)
	w.stop = make(chan struct{})
	w.done = make(chan struct{})
	w.hooked = map[*peersync.ConflictQueue]bool{}
	go w.loop()
	w.Kick()
	return w
}

// Kick asks for a delivery pass now.
func (w *ConflictWebhook) Kick() {
	select {
	case w.kick <- struct{}{}:
	default:
	}
}

// Close stops delivering, waiting for a pass in flight.
func (w *ConflictWebhook) Close() {
	w.once.Do(func() { close(w.stop) })
	<-w.done
}

func (w *ConflictWebhook) loop() {
	defer close(w.done)
	t := time.NewTicker(w.Interval)
	defer t.Stop()
	for {
		select {
		case <-w.stop:
			return
		case <-w.kick:
		case <-t.C:
		}
		w.Pass()
	}
}

// Pass delivers every undelivered authority conflict once, oldest first per namespace, and
// reports how many were acknowledged. Namespaces opened since the last pass are picked up here.
func (w *ConflictWebhook) Pass() int {
	w.passMu.Lock()
	defer w.passMu.Unlock()
	runtimes := w.set.Runtimes()
	names := make([]string, 0, len(runtimes))
	for ns := range runtimes {
		names = append(names, ns)
	}
	sort.Strings(names)
	delivered := 0
	for _, ns := range names {
		rt := runtimes[ns]
		if rt.Conflicts == nil {
			continue
		}
		if !w.hooked[rt.Conflicts] {
			w.hooked[rt.Conflicts] = true
			rt.Conflicts.OnRecord(func(e peersync.ConflictEntry) {
				if e.Authority {
					w.Kick()
				}
			})
		}
		for _, e := range rt.Conflicts.List() {
			if !e.Authority || e.Delivered || (e.AuthorityNode != "" && e.AuthorityNode != w.node) {
				continue
			}
			select {
			case <-w.stop:
				return delivered
			default:
			}
			if err := w.deliver(ns, e); err != nil {
				continue // retried on the next pass
			}
			if err := rt.Conflicts.MarkDelivered(e); err == nil {
				delivered++
			}
		}
	}
	return delivered
}

func (w *ConflictWebhook) deliver(ns string, e peersync.ConflictEntry) error {
	body, err := json.Marshal(ConflictNotification{Type: "kdb.conflict", Node: w.node, Namespace: ns, Conflict: e})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, w.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-KDB-Event", "conflict")
	req.Header.Set("X-KDB-Delivery", e.ID)
	if w.Secret != "" {
		req.Header.Set("X-KDB-Signature", SignConflictNotification(w.Secret, body))
	}
	resp, err := w.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("conflict webhook: %s answered %s", w.URL, resp.Status)
	}
	return nil
}

// SignConflictNotification is the X-KDB-Signature value for body under secret. A receiver
// recomputes it over the raw request body and compares in constant time (hmac.Equal).
func SignConflictNotification(secret string, body []byte) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(body)
	return "sha256=" + hex.EncodeToString(m.Sum(nil))
}
