package server

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
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

// admitHome refuses a client write on a node that is not this namespace's home.
func (s *KdbServerRuntime) admitHome() error {
	h, ok := s.HomeOf()
	if !ok || h.Node == s.NodeID.String() {
		return nil
	}
	return &NotHomeError{Namespace: s.Runtime.DefaultNamespace, Home: h}
}

// commitMessage is the message a client write commits with: the home's stamp on a single-home
// namespace, nothing otherwise.
func (s *KdbServerRuntime) commitMessage() string {
	if h, ok := s.HomeOf(); ok && h.Node == s.NodeID.String() {
		return homeStamp(h)
	}
	return ""
}

// checkReplicatedCommit refuses a commit stamped by a home at a fence older than this
// namespace's current one. Unstamped commits are history from before the namespace was
// single-home, or merges, and pass.
func (s *KdbServerRuntime) checkReplicatedCommit(c document.Commit) error {
	h, ok := s.HomeOf()
	if !ok {
		return nil
	}
	if _, fence, stamped := parseHomeStamp(c.Message); stamped && fence < h.Fence {
		return &StaleFenceError{Namespace: s.Runtime.DefaultNamespace, CommitHash: c.Hash, Fence: fence, Current: h.Fence}
	}
	return nil
}
