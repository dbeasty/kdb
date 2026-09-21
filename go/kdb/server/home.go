package server

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/peersync"
)

// Single-home ownership (docs/kdb-distributed-implementation-plan.md Phase 9): a namespace can
// be assigned one home node that alone accepts its writes. Every other node still holds and
// serves it - replication is unchanged - but refuses writes with NotHomeError naming the home, so
// a client (or the SDK's redirect) writes there. With one writer, a unique constraint and a
// compare-and-set mean the same thing on every node, which multi-leader replication cannot offer.
//
// The assignment is a replicated definition in the metadata namespace, and moving it (handover)
// raises a fence. The home stamps every commit it makes with its fence; a node ingesting a commit
// stamped with an older fence than the namespace's current one refuses it - that is the write a
// former home made before it learned it was no longer home.

// Home is a namespace's single-home assignment.
type Home struct {
	Node  string `json:"node"`
	Addr  string `json:"addr,omitempty"`
	Fence int64  `json:"fence"`
	// Since is the handover point: the assigning node's head when the assignment was made,
	// captured after in-flight writes finished. Every write made before the assignment is in its
	// history; the new home accepts no writes until it holds it.
	Since string `json:"since,omitempty"`
}

// SetHome records ns's assignment on this runtime. Called by the metadata store as the definition
// arrives or changes; nil clears it (multi-leader).
func (s *KdbServerRuntime) SetHome(h *Home) { s.home.Store(h) }

// HomeOf returns this namespace's assignment, if it has one.
func (s *KdbServerRuntime) HomeOf() (Home, bool) {
	if h := s.home.Load(); h != nil {
		return *h, true
	}
	return Home{}, false
}

// NotHomeError refuses a write on a node that is not the namespace's home.
type NotHomeError struct {
	Namespace string
	Home      Home
}

func (e *NotHomeError) Error() string {
	return fmt.Sprintf("namespace %s is single-home: only node %s accepts its writes (home=%s)", e.Namespace, e.Home.Node, e.Home.Addr)
}

// StaleFenceError refuses a replicated commit a former home made after the namespace moved.
type StaleFenceError struct {
	Namespace  string
	CommitHash codec.Hash
	Fence      int64
	Current    int64
}

func (e *StaleFenceError) Error() string {
	return fmt.Sprintf("namespace %s: commit %s was made by a home at fence %d, but the namespace's home is now at fence %d",
		e.Namespace, e.CommitHash.Hex(), e.Fence, e.Current)
}

const homeStampPrefix = "kdb:home/1 "

// homeStamp is the message the home gives every commit it makes.
func homeStamp(h Home) string {
	return fmt.Sprintf("%snode=%s fence=%d", homeStampPrefix, h.Node, h.Fence)
}

func parseHomeStamp(msg string) (node string, fence int64, ok bool) {
	if !strings.HasPrefix(msg, homeStampPrefix) {
		return "", 0, false
	}
	for _, f := range strings.Fields(strings.TrimPrefix(msg, homeStampPrefix)) {
		k, v, _ := strings.Cut(f, "=")
		switch k {
		case "node":
			node = v
		case "fence":
			fence, _ = strconv.ParseInt(v, 10, 64)
		}
	}
	return node, fence, node != ""
}

// admitHome refuses a client write on a node that is not this namespace's home - and, on the new
// home, until it holds the handover point, since writing before then would build on a history
// missing the previous home's last writes.
func (s *KdbServerRuntime) admitHome() error {
	h, ok := s.HomeOf()
	if !ok {
		return nil
	}
	if h.Node != s.NodeID.String() {
		return &NotHomeError{Namespace: s.Runtime.DefaultNamespace, Home: h}
	}
	if since, err := codec.HashFromHex(h.Since); err == nil && !s.dag.HasCommit(since) {
		return &UnavailableError{Reason: "this node is becoming the namespace's home and has not yet received the previous home's last writes (" + h.Since + ")"}
	}
	return nil
}

// commitMessage is the message a client write commits with: the home's stamp on a single-home
// namespace, nothing otherwise.
func (s *KdbServerRuntime) commitMessage() string {
	if h, ok := s.HomeOf(); ok && h.Node == s.NodeID.String() {
		return homeStamp(h)
	}
	return ""
}

// checkReplicatedCommit refuses a commit written by anyone but the current home after the
// handover point. It passes:
//
//   - the current home's own writes (authored by it, or stamped with the current fence);
//   - history up to the handover point - including a former home's writes made before the
//     handover and acknowledged, which must replicate even if they arrive late;
//   - merge commits, which write nothing of their own (anything stale they bring is checked as
//     the commit it is).
//
// Everything else - notably a former home's write made after it was replaced, cross-namespace
// parts included - is refused. Until the handover point itself arrives there is no telling the
// two apart, so the check passes and the new home, which refuses writes until then, decides.
func (s *KdbServerRuntime) checkReplicatedCommit(c document.Commit) error {
	h, ok := s.HomeOf()
	if !ok {
		return nil
	}
	if c.AuthorNodeID == peersync.MergeAuthorNodeID || c.AuthorNodeID.String() == h.Node {
		return nil
	}
	if _, fence, stamped := parseHomeStamp(c.Message); stamped && fence == h.Fence {
		return nil
	}
	since, err := codec.HashFromHex(h.Since)
	if err != nil || !s.dag.HasCommit(since) {
		return nil
	}
	if c.Hash == since || s.dag.IsAncestor(c.Hash, since) {
		return nil
	}
	fence := int64(0)
	if _, f, stamped := parseHomeStamp(c.Message); stamped {
		fence = f
	}
	return &StaleFenceError{Namespace: s.Runtime.DefaultNamespace, CommitHash: c.Hash, Fence: fence, Current: h.Fence}
}
