package peersync

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// ConflictKind classifies a queued conflict.
type ConflictKind string

const (
	// ConflictDivergence: a peer's history for a ref diverged from this node's and the policy
	// would not merge it - a same-document conflict under strict, a side branch that forked, a
	// tag that names different commits.
	ConflictDivergence ConflictKind = "divergence"
	// ConflictUniqueDuplicate: replicated documents claim a value a unique constraint says only
	// one document may hold.
	ConflictUniqueDuplicate ConflictKind = "unique-duplicate"
	// ConflictForeignGroupPart: a replicated commit is one part of a cross-namespace group decided
	// on another host, and this node cannot check the rest of the group arrived with it.
	ConflictForeignGroupPart ConflictKind = "foreign-group-part"
)

// ConflictEntry is one conflict awaiting an operator or application.
type ConflictEntry struct {
	ID        string       `json:"id"`
	Kind      ConflictKind `json:"kind"`
	Namespace string       `json:"namespace"`
	// Ref is "branch:<name>" or "tag:<name>" for a divergence; empty otherwise.
	Ref string `json:"ref,omitempty"`
	// Peer is the node the conflicting history came from, when known.
	Peer string `json:"peer,omitempty"`
	// LocalHex / IncomingHex are the two sides of a divergence as last seen.
	LocalHex    string `json:"localHex,omitempty"`
	IncomingHex string `json:"incomingHex,omitempty"`
	// TrackingRef is the branch this node keeps pointing at the incoming side, so retention
	// cannot drop it and resolution can find it.
	TrackingRef string                `json:"trackingRef,omitempty"`
	Items       []kdberr.ConflictItem `json:"items,omitempty"`
	Detail      string                `json:"detail,omitempty"`
	FirstSeen   time.Time             `json:"firstSeen"`
	LastSeen    time.Time             `json:"lastSeen"`
	// Seen counts how many syncs have reported this conflict.
	Seen int `json:"seen"`
}

// ConflictQueue holds a node's open replication conflicts, durably when it has a directory.
//
// It is local state, never replicated: each node reports what it saw. An entry is keyed by what
// it is about - one per (namespace, ref, peer) for a divergence, so a conflict that persists
// across many syncs is one entry whose LastSeen moves, not a new entry every time.
type ConflictQueue struct {
	dir     string
	mu      sync.Mutex
	entries map[string]ConflictEntry
}

// NewConflictQueue opens the queue kept in dir, loading what is already there. An empty dir keeps
// the queue in memory only.
func NewConflictQueue(dir string) (*ConflictQueue, error) {
	q := &ConflictQueue{dir: dir, entries: map[string]ConflictEntry{}}
	if dir == "" {
		return q, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		var e ConflictEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("conflict queue: %s: %w", f, err)
		}
		q.entries[e.ID] = e
	}
	return q, nil
}

// ConflictQueueDir is where a data root keeps namespace ns's conflict queue - outside the
// namespace's own directory, which nests namespaces and so could collide with a namespace named
// ".../conflicts".
func ConflictQueueDir(dataRoot, ns string) string {
	return filepath.Join(dataRoot, "replication", "conflicts", url.PathEscape(ns))
}

// ConflictID is the key an entry about these things is stored under.
func ConflictID(kind ConflictKind, parts ...string) string {
	sum := sha256.Sum256([]byte(string(kind) + "\x00" + strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:8])
}

// Record adds e, or updates the entry with its ID: the first sighting's time is kept, the rest
// is replaced with what was just seen.
func (q *ConflictQueue) Record(e ConflictEntry) (ConflictEntry, error) {
	if q == nil {
		return e, nil
	}
	now := time.Now().UTC()
	q.mu.Lock()
	defer q.mu.Unlock()
	if prev, ok := q.entries[e.ID]; ok {
		e.FirstSeen, e.Seen = prev.FirstSeen, prev.Seen
	} else {
		e.FirstSeen = now
	}
	e.LastSeen = now
	e.Seen++
	if err := q.writeLocked(e); err != nil {
		return e, err
	}
	q.entries[e.ID] = e
	return e, nil
}

// Get returns the entry with id.
func (q *ConflictQueue) Get(id string) (ConflictEntry, bool) {
	if q == nil {
		return ConflictEntry{}, false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	e, ok := q.entries[id]
	return e, ok
}

// List returns every open entry, oldest first.
func (q *ConflictQueue) List() []ConflictEntry {
	if q == nil {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]ConflictEntry, 0, len(q.entries))
	for _, e := range q.entries {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].FirstSeen.Equal(out[j].FirstSeen) {
			return out[i].FirstSeen.Before(out[j].FirstSeen)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// Len counts open entries.
func (q *ConflictQueue) Len() int {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries)
}

// Remove deletes the entry with id; removing an absent entry is not an error.
func (q *ConflictQueue) Remove(id string) error {
	if q == nil {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.entries[id]; !ok {
		return nil
	}
	if q.dir != "" {
		if err := os.Remove(filepath.Join(q.dir, id+".json")); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	delete(q.entries, id)
	return nil
}

func (q *ConflictQueue) writeLocked(e ConflictEntry) error {
	if q.dir == "" {
		return nil
	}
	raw, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(q.dir, e.ID+".json.tmp")
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(q.dir, e.ID+".json"))
}

// trackingBranchPrefix names the branches that keep a conflicting peer's side reachable. They
// are local bookkeeping: never advertised to peers, never replicated.
const trackingBranchPrefix = "peers/"

// TrackingBranch is the branch that tracks peer's side of ref.
func TrackingBranch(peer, ref string) string {
	return trackingBranchPrefix + peer + "/" + strings.ReplaceAll(ref, ":", "/")
}

// IsTrackingBranch reports whether name is a tracking branch.
func IsTrackingBranch(name string) bool { return strings.HasPrefix(name, trackingBranchPrefix) }

// Choice is how one conflicting document is resolved.
type Choice struct {
	// Take is "local" or "remote": keep that side's final state of the document, deletion
	// included. Ignored when Body is set.
	Take string `json:"take,omitempty"`
	// Body replaces the document with this JSON.
	Body *string `json:"body,omitempty"`
}

// ErrConflictNotFound is resolving an entry the queue does not hold.
var ErrConflictNotFound = errors.New("peer sync: no such conflict")

// ResolveConflict settles a queued divergence of main: it merges the incoming side the entry
// tracks into the current head, taking each conflicting document as choices say, and clears the
// entry and its tracking branch. The merge is an ordinary two-parent merge commit - a node that
// receives it fast-forwards - so the resolution replicates like any other write.
//
// Every document the two sides changed differently needs a choice; a missing one leaves the
// conflict open and is an error naming the documents still to decide. The head may have moved
// since the conflict was recorded - the merge is against the head as it is now, so new local
// writes are kept, and a document they touched may need a choice it did not need before.
func ResolveConflict(env IngestEnv, id string, choices map[codec.UUID]Choice) (IngestResult, error) {
	e, ok := env.Conflicts.Get(id)
	if !ok {
		return IngestResult{}, ErrConflictNotFound
	}
	if e.Kind != ConflictDivergence || e.Ref != "branch:"+mainBranch {
		return IngestResult{}, fmt.Errorf("peer sync: conflict %s (%s on %s) is not a divergence of main; only those resolve by merging", id, e.Kind, e.Ref)
	}
	incoming, err := codec.HashFromHex(e.IncomingHex)
	if err != nil {
		return IngestResult{}, err
	}
	env.Resolution.Choose = func(docID codec.UUID, local, remote document.Op) (document.Op, bool) {
		c, ok := choices[docID]
		switch {
		case !ok:
			return nil, false
		case c.Body != nil:
			return document.WriteOp{DocID: docID, Patch: *c.Body}, true
		case c.Take == "local":
			return local, true
		case c.Take == "remote":
			return remote, true
		}
		return nil, false
	}
	env.Peer = e.Peer
	res, err := Adopt(env, incoming)
	if err != nil {
		return res, err
	}
	if res.Outcome.Kind == OutcomeConflict {
		var undecided []string
		for _, item := range res.Outcome.Report.Conflicts {
			undecided = append(undecided, item.DocumentID)
		}
		return res, fmt.Errorf("peer sync: conflict %s still has documents without a choice: %s", id, strings.Join(undecided, ", "))
	}
	// Adopt clears the entry when it merges; an entry recorded under another peer name, or a
	// divergence that stopped being one because the head moved past it, is cleared here.
	return res, env.Conflicts.Remove(id)
}
