package embed

import (
	"fmt"
	"time"

	"github.com/limidus/kdb/go/kdb/schema"
)

// Reopening a namespace with different storage options, without restarting
// the process.
//
// A good many settings are read once, when a namespace is opened, and then
// held: cache sizes, whether checkpoints are written, how the commit graph
// is built. config.MutabilityNamespaceReopen is the class for exactly those,
// and until now it was a class with no implementation - the control plane's
// refusal said so outright. The primitives were already here
// (Host.CloseNamespace, Host.NamespaceWithOptions); what was missing was the
// safety around them.
//
// The dangerous part is not the close and the open, it is the gap between
// them. Host.CloseNamespace unroutes the namespace before tearing it down,
// so a request arriving mid-reopen fails with ErrUnknownNamespace rather
// than reaching an engine being closed underneath it. That is correct and it
// is also an outage, however brief - so a reopen has to be bounded, has to
// be reported, and must never be something a caller does by accident.

// ReopenResult reports a reopen: what it changed, and what it cost.
type ReopenResult struct {
	NamespaceID string
	// Unavailable is how long the namespace was unroutable. Reported rather
	// than hidden: this is the price of the operation, and an operator
	// deciding whether to do it again needs the number.
	Unavailable time.Duration
	// Runtime is the newly-opened runtime. The old one is closed and must
	// not be used again - anything holding it needs to be pointed here.
	Runtime *EmbeddedKdbRuntime
}

// ReopenNamespace closes a namespace and opens it again under new storage
// options, returning the new runtime.
//
// The namespace is unroutable for the duration, which the result reports.
// Callers that serve traffic should quiesce writes first - the server
// runtime's BeginDraining and WaitForWritesToDrain do exactly that - because
// this does not wait for in-flight work on its own: the Host has no view of
// who is mid-commit, and blocking here on something it cannot see would
// deadlock rather than protect.
//
// Everything already durable survives, because this is an ordinary close
// followed by an ordinary open: the close flushes and seals, the open
// replays whatever the checkpoint does not cover. What does not survive is
// in-memory state that was never part of the durable record - caches, and
// any session-scoped assumption about the runtime that existed before.
func (h *Host) ReopenNamespace(catalog, namespaceID string, sch schema.KdbSchema, sopts StorageOptions) (ReopenResult, error) {
	if h == nil {
		return ReopenResult{}, fmt.Errorf("kdb: no host to reopen %q on", namespaceID)
	}
	h.mu.Lock()
	_, known := h.nss[namespaceID]
	closed := h.closed
	h.mu.Unlock()
	if closed {
		return ReopenResult{}, fmt.Errorf("kdb: host for %s is closed", h.dataRoot)
	}
	if !known {
		return ReopenResult{}, fmt.Errorf("kdb: namespace %q is not open on this host", namespaceID)
	}

	started := time.Now()
	if err := h.CloseNamespace(namespaceID); err != nil {
		// The namespace is already unrouted at this point, so there is
		// nothing to roll back to - reopening is the only way forward, and
		// the caller needs to know the close was unclean either way.
		rt, openErr := h.NamespaceWithOptions(catalog, namespaceID, sch, sopts)
		if openErr != nil {
			return ReopenResult{NamespaceID: namespaceID, Unavailable: time.Since(started)}, fmt.Errorf(
				"kdb: namespace %q failed to close (%v) and could not be reopened: %w", namespaceID, err, openErr)
		}
		return ReopenResult{NamespaceID: namespaceID, Unavailable: time.Since(started), Runtime: rt}, fmt.Errorf(
			"kdb: namespace %q was reopened, but its close reported: %w", namespaceID, err)
	}
	rt, err := h.NamespaceWithOptions(catalog, namespaceID, sch, sopts)
	if err != nil {
		// The worst outcome: closed and not reopened, so the namespace is
		// gone from this process until something opens it again. Said
		// plainly rather than wrapped in the option that caused it.
		return ReopenResult{NamespaceID: namespaceID, Unavailable: time.Since(started)}, fmt.Errorf(
			"kdb: namespace %q was closed for a reopen and could not be opened again; "+
				"it is not being served by this process until it is: %w", namespaceID, err)
	}
	return ReopenResult{
		NamespaceID: namespaceID,
		Unavailable: time.Since(started),
		Runtime:     rt,
	}, nil
}

// StorageOptionsOf reports the storage options a namespace is currently open
// with, as the starting point for a reopen that changes one of them.
//
// Reopening with options assembled from anywhere else would silently revert
// every setting the caller did not happen to name, which is the trap this
// exists to close.
func (h *Host) StorageOptionsOf(namespaceID string) (StorageOptions, bool) {
	if h == nil {
		return StorageOptions{}, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	entry, ok := h.nss[namespaceID]
	if !ok || entry == nil || entry.rt == nil {
		return StorageOptions{}, false
	}
	return entry.storage, true
}
