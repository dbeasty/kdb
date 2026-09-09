package control

import (
	"sort"
	"sync"

	"github.com/limidus/kdb/go/kdb/server"
)

// NamespaceSource is where the control plane finds the runtimes it serves.
//
// One KdbServerRuntime serves exactly one namespace, so "which namespace" and "which runtime" are
// the same question. This interface is what lets the answer be more than one: the control plane
// resolves the namespace out of the request path and asks for its runtime, rather than holding a
// single one and assuming every request is about it.
type NamespaceSource interface {
	// Namespaces lists what this control plane can see, in a stable order.
	Namespaces() []string
	// Runtime returns the runtime serving namespaceID, or false if there is none.
	Runtime(namespaceID string) (*server.KdbServerRuntime, bool)
}

// SingleNamespace is a NamespaceSource over one runtime - what an embedded caller, a test, or a
// service started with a single --namespace has.
func SingleNamespace(namespaceID string, rt *server.KdbServerRuntime) NamespaceSource {
	return &staticSource{order: []string{namespaceID}, byID: map[string]*server.KdbServerRuntime{namespaceID: rt}}
}

// StaticNamespaces is a NamespaceSource over a fixed set, for a host that opened its namespaces up
// front. A namespace opened later needs a source that can see it - see the service's own.
func StaticNamespaces(runtimes map[string]*server.KdbServerRuntime) NamespaceSource {
	order := make([]string, 0, len(runtimes))
	byID := make(map[string]*server.KdbServerRuntime, len(runtimes))
	for id, rt := range runtimes {
		order = append(order, id)
		byID[id] = rt
	}
	sort.Strings(order)
	return &staticSource{order: order, byID: byID}
}

type staticSource struct {
	mu    sync.RWMutex
	order []string
	byID  map[string]*server.KdbServerRuntime
}

func (s *staticSource) Namespaces() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, len(s.order))
	copy(out, s.order)
	return out
}

func (s *staticSource) Runtime(namespaceID string) (*server.KdbServerRuntime, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rt, ok := s.byID[namespaceID]
	return rt, ok
}

// namespaces is the source this server resolves against: the configured one, plus any staged
// restores attached for inspection.
//
// The overlay is not part of NamespaceSource on purpose. A staged copy is this control plane's
// business and nobody else's - the service that built the source has no reason to know a restore is
// being looked at - so it is composed in here rather than pushed back into the service's wiring.
func (s *Server) namespaces() NamespaceSource {
	base := s.opts.Namespaces
	if base == nil {
		base = SingleNamespace(s.opts.Namespace, s.opts.Runtime)
	}
	attached := s.attachedNamespaces()
	if len(attached) == 0 {
		return base
	}
	return &overlaySource{base: base, overlay: attached}
}

// overlaySource is a NamespaceSource with extra namespaces layered on top.
type overlaySource struct {
	base    NamespaceSource
	overlay map[string]*server.KdbServerRuntime
}

func (o *overlaySource) Namespaces() []string {
	out := append([]string(nil), o.base.Namespaces()...)
	for id := range o.overlay {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func (o *overlaySource) Runtime(namespaceID string) (*server.KdbServerRuntime, bool) {
	// The overlay wins. A staged alias cannot collide with a live namespace id - it is prefixed -
	// so this only ever resolves what the base does not have.
	if rt, ok := o.overlay[namespaceID]; ok {
		return rt, true
	}
	return o.base.Runtime(namespaceID)
}

// defaultNamespace is the namespace a request that names none is about. It exists for the
// namespace-less routes, which still have to authorize against something.
func (s *Server) defaultNamespace() string {
	if s.opts.Namespace != "" {
		return s.opts.Namespace
	}
	if all := s.namespaces().Namespaces(); len(all) > 0 {
		return all[0]
	}
	return ""
}
