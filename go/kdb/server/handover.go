package server

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Application-driven handover (docs/kdb-distributed-implementation-plan.md Phase 16.7, G8).
// Moving a single-home namespace used to be an operator's control-plane call. An application
// that resumes a match on another device needs the move itself: the device that wants to be home
// asks the current home over the sync connection (HOME_REQUEST), the current home's application
// decides (HandoverPolicy - "is the match idle, and is the requester a seated player?"), and on
// yes the home assigns the namespace to the requester, raising the fence. When the home is
// unreachable, a node the namespace's resolution chain names as its authority may force the move;
// the fence makes the old home's later writes unadoptable wherever they arrive.

// HandoverRequest is what a HandoverPolicy decides.
type HandoverRequest struct {
	Namespace string
	// Node asks to become home, reachable for clients at Addr; Principal is who it authenticated
	// as.
	Node, Addr string
	Principal  auth.Principal
	Reason     string
	// Force is a reassignment without the current home, asked of the authority.
	Force bool
	// Current is the assignment being replaced; HasHome is false for a multi-leader namespace.
	Current Home
	HasHome bool
}

// HandoverPolicy approves a handover (nil) or refuses it with the reason the requester is told.
type HandoverPolicy func(HandoverRequest) error

// HandoverRefusedError is a handover the home (or the authority) did not grant.
type HandoverRefusedError struct {
	Namespace string
	Reason    string
	// Home is the current assignment, when known: where to ask instead.
	Home Home
}

func (e *HandoverRefusedError) Error() string {
	return fmt.Sprintf("handover of %s refused: %s", e.Namespace, e.Reason)
}

// Handover moves ns's home from this node to node (reachable for clients at addr): the in-process
// call an application makes on the current home. A multi-leader namespace becomes single-home.
func (s *KdbServerRuntime) Handover(ns, node, addr string) (Home, error) {
	rt, err := s.namespaceSet().Resolve(ns, false)
	if err != nil {
		return Home{}, err
	}
	if h, ok := rt.HomeOf(); ok && h.Node != s.NodeID.String() && h.Node != node {
		return Home{}, &NotHomeError{Namespace: ns, Home: h}
	}
	return rt.Meta.AssignHome(ns, node, addr)
}

// ForceHandover reassigns ns's home without the current home - for when it is unreachable. Only a
// node the namespace's resolution chain names as its authority may.
func (s *KdbServerRuntime) ForceHandover(ns, node, addr string) (Home, error) {
	rt, err := s.namespaceSet().Resolve(ns, false)
	if err != nil {
		return Home{}, err
	}
	if a := rt.ResolutionChainOf().Authority(); a == nil || a.Node != s.NodeID.String() {
		return Home{}, &HandoverRefusedError{Namespace: ns, Reason: "only the node the namespace's resolution chain names as its authority may force a handover"}
	}
	return rt.Meta.AssignHome(ns, node, addr)
}

// answerHomeRequest decides a peer's HOME_REQUEST on this node.
func (s *KdbServerRuntime) answerHomeRequest(principal auth.Principal, m wire.HomeRequestMessage) (wire.HomeRequestResultMessage, error) {
	res := wire.HomeRequestResultMessage{Namespace: m.Namespace}
	refuse := func(reason string, h Home) (wire.HomeRequestResultMessage, error) {
		res.Reason, res.HomeNode, res.HomeAddr, res.Fence, res.Since = reason, h.Node, h.Addr, h.Fence, h.Since
		return res, nil
	}
	rt, err := s.namespaceSet().Resolve(m.Namespace, false)
	if err != nil {
		return refuse(err.Error(), Home{})
	}
	if rt.Meta == nil {
		return refuse("this node keeps no definitions, so it cannot assign homes", Home{})
	}
	current, hasHome := rt.HomeOf()
	if hasHome && current.Node == m.Node {
		res.Granted = true
		res.HomeNode, res.HomeAddr, res.Fence, res.Since = current.Node, current.Addr, current.Fence, current.Since
		return res, nil
	}
	self := s.NodeID.String()
	if m.Force {
		if a := rt.ResolutionChainOf().Authority(); a == nil || a.Node != self {
			return refuse("only the node the namespace's resolution chain names as its authority may force a handover", current)
		}
	} else if hasHome && current.Node != self {
		return refuse("this node is not the namespace's home; ask the home", current)
	}
	if s.HandoverPolicy == nil {
		return refuse("this node has no handover policy, so it hands nothing over on request", current)
	}
	if err := s.HandoverPolicy(HandoverRequest{
		Namespace: m.Namespace, Node: m.Node, Addr: m.Addr, Principal: principal, Reason: m.Reason,
		Force: m.Force, Current: current, HasHome: hasHome,
	}); err != nil {
		return refuse(err.Error(), current)
	}
	h, err := rt.Meta.AssignHome(m.Namespace, m.Node, m.Addr)
	if err != nil {
		return wire.HomeRequestResultMessage{}, err
	}
	res.Granted = true
	res.HomeNode, res.HomeAddr, res.Fence, res.Since = h.Node, h.Addr, h.Fence, h.Since
	return res, nil
}
