package service

import (
	"fmt"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// The service's side of changing a setting that is only read when a namespace
// opens: close it, open it again under the new options, and point everything
// that was serving it at the result.
//
// Three pieces have to move together, which is why this lives here rather
// than in any one of them. The Host owns namespace lifecycle. The server
// runtime owns the write gate and everything derived from the namespace. The
// control plane owns the decision. Putting the sequence in the process that
// holds all three is what keeps each of them from reaching into the others.

// namespaceReopener implements control.NamespaceReopener over this process's host and the server
// runtimes serving its namespaces.
type namespaceReopener struct {
	host *embed.Host
	// drainTimeout bounds how long a reopen waits for in-flight writes before giving up. Giving
	// up is the safe outcome: the namespace keeps serving what it was.
	drainTimeout time.Duration

	mu sync.Mutex
	// runtimes are the server runtimes to re-point, by namespace. Held rather than looked up so
	// a reopen can swap the same instance every listener is already holding - replacing the
	// instance would leave every connection talking to the namespace that was just closed.
	runtimes map[string]*server.KdbServerRuntime
}

func newNamespaceReopener(host *embed.Host, drainTimeout time.Duration) *namespaceReopener {
	return &namespaceReopener{
		host:         host,
		drainTimeout: drainTimeout,
		runtimes:     map[string]*server.KdbServerRuntime{},
	}
}

// setHost hands over the host once the process has opened one. Separate from construction
// because the control listener is given this reopener before the host exists, in the same way
// and for the same reason as the maintenance registry.
func (n *namespaceReopener) setHost(h *embed.Host) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.host = h
}

func (n *namespaceReopener) add(namespaceID string, srv *server.KdbServerRuntime) {
	if srv == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.runtimes[namespaceID] = srv
}

// ReopenableNamespaces is what this process can reopen: the namespaces it has a host for and a
// server runtime pointed at.
func (n *namespaceReopener) ReopenableNamespaces() []string {
	if n == nil {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.host == nil {
		return nil
	}
	out := make([]string, 0, len(n.runtimes))
	for ns := range n.runtimes {
		out = append(out, ns)
	}
	return out
}

// ReopenNamespace applies edit to the namespace's current options and reopens it.
//
// The order is: read the options actually in force, let the caller edit those, drain, close,
// open, swap. Reading first is what stops a change to one setting reverting the others; draining
// before the close is what stops a commit being interrupted by it.
func (n *namespaceReopener) ReopenNamespace(namespaceID string, edit func(*embed.StorageOptions)) (time.Duration, error) {
	if n == nil {
		return 0, fmt.Errorf("this process has no host to reopen %q on", namespaceID)
	}
	n.mu.Lock()
	host := n.host
	srv, known := n.runtimes[namespaceID]
	n.mu.Unlock()
	if host == nil {
		return 0, fmt.Errorf("this process has no host to reopen %q on", namespaceID)
	}
	if !known {
		return 0, fmt.Errorf("this process is not serving namespace %q", namespaceID)
	}

	current, ok := host.StorageOptionsOf(namespaceID)
	if !ok {
		return 0, fmt.Errorf("namespace %q is not open on this host", namespaceID)
	}
	edit(&current)

	// Drain before the close, and give up rather than force it: a namespace that keeps serving
	// its old settings is a much better outcome than one whose storage was swapped out from
	// under a running commit.
	srv.BeginDraining()
	drained := srv.WaitForWritesToDrain(n.drainTimeout)
	if !drained {
		srv.ResumeAfterDrain()
		return 0, fmt.Errorf(
			"writes on %q did not drain within %s, so it was not reopened and is still serving "+
				"its current settings", namespaceID, n.drainTimeout)
	}

	res, err := host.ReopenNamespace(embed.CatalogFromNamespace(namespaceID), namespaceID, schema.None(), current)
	if err != nil {
		srv.ResumeAfterDrain()
		return res.Unavailable, err
	}
	// Already drained, so the swap's own drain returns immediately.
	if err := srv.ReopenWith(res.Runtime, n.drainTimeout); err != nil {
		srv.ResumeAfterDrain()
		return res.Unavailable, fmt.Errorf(
			"namespace %q was reopened but this process could not be pointed at it: %w", namespaceID, err)
	}
	srv.ResumeAfterDrain()
	return res.Unavailable, nil
}
