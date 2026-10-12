package server

import (
	"errors"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/peersync"
)

// The "stranded unlogged commit" family, the two cases #113 left open: a Deepen and a merge (or
// join, or fast-forward) adopted from a peer whose log write fails after the commits are already
// in the DAG. Delta replay applies every logged commit, so a checkpoint must not describe a commit
// the log never received: on the next open the DAG holds it with no operations and no frame to
// load them from, and anything that reaches it fails with "has no frame in the delta log".
//
// Each test fails the log write the way the v0.6.2 rollback tests do - by wrapping the runtime's
// PersistAsync - closes the node cleanly (which writes a checkpoint), reopens it from the same
// directory and requires a node that opens, whose head is a logged commit, and whose next sync
// converges.

var errInjectedLog = errors.New("injected: log device is full")

// persistFailure is where an injected log failure strikes: refusing the queue (under the
// serialization, so persistAll never returns a wait), failing the fsync waited for after it, or
// failing only the wait of a commit this node made itself (a merge or a join) while the peer's
// commits before it reach the log - the two landing in different log batches.
type persistFailure int

const (
	failQueue persistFailure = iota
	failWait
	failMadeWait
)

func (f persistFailure) String() string {
	return [...]string{"queue", "wait", "made-wait"}[f]
}

// failingPersist is env with log writes failing at f. A failing write is never handed to the real
// log, so nothing reaches disk for it, which is the state the failure leaves either way. Under
// failMadeWait the commits in peers are written for real and every other one fails.
func failingPersist(env peersync.IngestEnv, f persistFailure, peers ...document.Commit) peersync.IngestEnv {
	real := env.PersistAsync
	fromPeer := map[codec.Hash]bool{}
	for _, c := range peers {
		fromPeer[c.Hash] = true
	}
	env.PersistAsync = func(c document.Commit) (func() error, error) {
		switch {
		case f == failQueue:
			return nil, errInjectedLog
		case f == failMadeWait && fromPeer[c.Hash]:
			return real(c)
		}
		return func() error { return errInjectedLog }, nil
	}
	return env
}

// requireReadable fails for any of hs the reopened node holds without being able to read its
// operations - the broken state: resident, and no frame in the log.
func requireReadable(t *testing.T, label string, rt *KdbServerRuntime, hs ...codec.Hash) {
	t.Helper()
	for _, h := range hs {
		if !rt.dag.HasCommit(h) {
			continue
		}
		if _, err := rt.dag.CommitOperations(h); err != nil {
			t.Fatalf("%s: the reopened node holds %s but cannot read its operations: %v", label, h.Hex(), err)
		}
	}
}

// requireLoggedHistory walks everything main reaches on a reopened node and requires each commit's
// operations to be readable - main's head first, which is what "the head is a logged commit" means
// on a node that has just been rebuilt from its checkpoint and log.
func requireLoggedHistory(t *testing.T, label string, rt *KdbServerRuntime) {
	t.Helper()
	head := mustHeadOf(t, rt)
	requireReadable(t, label+" (head)", rt, head)
	for _, e := range rt.dag.Walk(head, nil, 10_000) {
		if full, ok := e.(dag.FullEntry); ok {
			requireReadable(t, label, rt, full.Commit.Hash)
		}
	}
}

// TestFailedDeepenIsNotCheckpointed: the history a Deepen stored is resident when its log write
// fails. Deepen is re-runnable within the process (59dae9e), but a clean close before the retry
// checkpointed that history without operations. The reopened node then held it, so a retry found
// every parent "already held", fetched nothing, and failed reading the operations back to log them
// - the namespace could never be deepened again, and history reads below the root failed.
func TestFailedDeepenIsNotCheckpointed(t *testing.T) {
	for _, f := range []persistFailure{failQueue, failWait} {
		t.Run(f.String(), func(t *testing.T) {
			dir := t.TempDir()
			rt, a := openFileNode(t, dir)
			b := newMetaNode(t).data
			history := writeN(t, b, "b", 5)
			root := bootstrapFrom(t, a, b)
			writeN(t, a, "a", 1) // a logged write of its own, so the close has a segment to checkpoint

			session, err := openerFor(t, b)()
			if err != nil {
				t.Fatal(err)
			}
			_, err = peersync.Deepen(failingPersist(a.PeerIngestEnv(), f), session, root)
			session.Close()
			if err == nil || !strings.Contains(err.Error(), errInjectedLog.Error()) {
				t.Fatalf("the deepen should have failed on the log, got %v", err)
			}
			if !a.dag.HasCommit(history[0]) {
				t.Fatal("precondition: the failed deepen is expected to leave the fetched history resident")
			}
			rt.Close()

			rt, a = openFileNode(t, dir)
			requireLoggedHistory(t, "after the failed deepen", a)
			requireReadable(t, "after the failed deepen", a, history...)

			// The deepen, retried after the restart, goes through: it either fetches the history
			// afresh or finds it already logged, and in both cases leaves it readable.
			results, err := a.Deepen(openerFor(t, b))
			if err != nil {
				t.Fatalf("deepen after restart: %v", err)
			}
			if len(results) != 1 || !results[0].Unshallowed {
				t.Fatalf("deepen after restart: %+v", results)
			}
			rt.Close()

			rt, a = openFileNode(t, dir)
			defer rt.Close()
			if roots := a.ShallowRoots(); len(roots) != 0 {
				t.Fatalf("shallow roots after a completed deepen and a restart: %v", roots)
			}
			requireLoggedHistory(t, "after the retried deepen", a)
			if err := trySync(t, a, b); err != nil {
				t.Fatalf("sync after the retried deepen: %v", err)
			}
			if err := trySync(t, b, a); err != nil {
				t.Fatalf("sync back after the retried deepen: %v", err)
			}
			if mustHeadOf(t, a) != mustHeadOf(t, b) {
				t.Fatal("the nodes did not converge")
			}
		})
	}
}

// adoptCase is one way main adopts a peer's head: a fast-forward, a merge of disjoint writes, or
// the canonical join of two heads holding the same documents (#114).
type adoptCase struct {
	name string
	// diverge writes to a (file-backed) and b after they share history, returning the documents
	// each should hold once they converge.
	diverge func(t *testing.T, a, b *KdbServerRuntime) map[codec.UUID]string
	// newCommit says the adoption creates a commit of its own (a merge or a join).
	newCommit bool
}

func adoptCases() []adoptCase {
	upsert := func(t *testing.T, rt *KdbServerRuntime, id codec.UUID, body string) {
		t.Helper()
		if _, err := rt.Upsert("app/data", id, body, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	return []adoptCase{
		{name: "fast-forward", diverge: func(t *testing.T, a, b *KdbServerRuntime) map[codec.UUID]string {
			id := mustRandomUUID(t)
			upsert(t, b, id, `{"on":"b"}`)
			return map[codec.UUID]string{id: `{"on":"b"}`}
		}},
		{name: "merge", newCommit: true, diverge: func(t *testing.T, a, b *KdbServerRuntime) map[codec.UUID]string {
			ia, ib := mustRandomUUID(t), mustRandomUUID(t)
			upsert(t, a, ia, `{"on":"a"}`)
			upsert(t, b, ib, `{"on":"b"}`)
			return map[codec.UUID]string{ia: `{"on":"a"}`, ib: `{"on":"b"}`}
		}},
		{name: "join", newCommit: true, diverge: func(t *testing.T, a, b *KdbServerRuntime) map[codec.UUID]string {
			id := mustRandomUUID(t)
			upsert(t, a, id, `{"same":true}`)
			upsert(t, b, id, `{"same":true}`)
			return map[codec.UUID]string{id: `{"same":true}`}
		}},
	}
}

// TestFailedAdoptionIsNotCheckpointed: main adopting a peer's head - by fast-forward, by a merge
// commit made here, or by a canonical join - moved main and stored the new commit before logging
// anything. When the log write failed, main was left naming a commit the log never received, the
// peer's commits behind it were (correctly) left out of the checkpoint, and main and a locally
// made merge or join were not: the close checkpointed a head whose history was missing.
func TestFailedAdoptionIsNotCheckpointed(t *testing.T) {
	for _, tc := range adoptCases() {
		modes := []persistFailure{failQueue, failWait}
		if tc.newCommit {
			modes = append(modes, failMadeWait)
		}
		for _, f := range modes {
			t.Run(tc.name+"/"+f.String(), func(t *testing.T) {
				ns := "app/data"
				dir := t.TempDir()
				rt, a := openFileNode(t, dir)
				b := newTestRuntime(t)
				b.NodeID = mustRandomUUID(t)
				base := mustRandomUUID(t)
				if _, err := a.Upsert(ns, base, `{"base":true}`, auth.Principal{}); err != nil {
					t.Fatal(err)
				}
				commits, head := peerPush(t, a, b)
				if _, err := peersync.Ingest(b.PeerIngestEnv(), commits, head); err != nil {
					t.Fatalf("seeding b: %v", err)
				}
				want := tc.diverge(t, a, b)
				want[base] = `{"base":true}`
				before := mustHeadOf(t, a)

				commits, head = peerPush(t, b, a)
				res, err := peersync.Ingest(failingPersist(a.PeerIngestEnv(), f, commits...), commits, head)
				if err == nil || !strings.Contains(err.Error(), errInjectedLog.Error()) {
					t.Fatalf("the adoption should have failed on the log, got %v (%+v)", err, res)
				}
				if tc.newCommit && res.Outcome.MergeCommit == nil {
					t.Fatalf("precondition: the adoption is expected to make a commit of its own, got %+v", res.Outcome)
				}
				if f == failQueue {
					// Refused under the serialization, the adoption is taken back there: main and
					// storage are where they were, so a local write that follows builds on a
					// logged head and survives the restart. Left on the unlogged head, it was
					// logged on top of a commit replay never stores.
					if h := mustHeadOf(t, a); h != before {
						t.Fatalf("head after the refused adoption %s, want it back on %s", h.Hex(), before.Hex())
					}
					after := mustRandomUUID(t)
					if _, err := a.Upsert(ns, after, `{"after":"failure"}`, auth.Principal{}); err != nil {
						t.Fatalf("local write after the refused adoption: %v", err)
					}
					want[after] = `{"after":"failure"}`
					before = mustHeadOf(t, a)
				}
				rt.Close()

				rt, a = openFileNode(t, dir)
				requireLoggedHistory(t, "after the failed adoption", a)
				requireReadable(t, "after the failed adoption", a, head)
				if res.Outcome.MergeCommit != nil {
					requireReadable(t, "after the failed adoption", a, res.Outcome.MergeCommit.Hash)
				}
				if h := mustHeadOf(t, a); h != before && h != head && (res.Outcome.MergeCommit == nil || h != res.Outcome.MergeCommit.Hash) {
					t.Fatalf("head after reopening is %s, neither the head before the sync nor what it adopted", h.Hex())
				}

				// A local write moves main off whatever the reopen restored, and the restart after
				// it checkpoints that history as ordinary history: operations only for the new
				// head, the rest read back from the log. A commit the checkpoint restored as main
				// (with its operations) but the log never received breaks here.
				local := mustRandomUUID(t)
				if _, err := a.Upsert(ns, local, `{"after":"reopen"}`, auth.Principal{}); err != nil {
					t.Fatalf("local write after reopening: %v", err)
				}
				want[local] = `{"after":"reopen"}`
				rt.Close()
				rt, a = openFileNode(t, dir)
				requireLoggedHistory(t, "after a local write and another restart", a)

				// The next sync converges, and what it adopts survives another restart.
				commits, head = peerPush(t, b, a)
				if _, err := peersync.Ingest(a.PeerIngestEnv(), commits, head); err != nil {
					t.Fatalf("sync after restart: %v", err)
				}
				commits, head = peerPush(t, a, b)
				if _, err := peersync.Ingest(b.PeerIngestEnv(), commits, head); err != nil {
					t.Fatalf("sync back after restart: %v", err)
				}
				if mustHeadOf(t, a) != mustHeadOf(t, b) {
					t.Fatal("the nodes did not converge")
				}
				converged := mustHeadOf(t, a)
				rt.Close()

				rt, a = openFileNode(t, dir)
				defer rt.Close()
				if h := mustHeadOf(t, a); h != converged {
					t.Fatalf("head after the last restart %s, want %s", h.Hex(), converged.Hex())
				}
				requireLoggedHistory(t, "after the converged sync", a)
				for id, body := range want {
					for name, rt := range map[string]*KdbServerRuntime{"a": a, "b": b} {
						if got, _, found, err := rt.GetDocument(ns, id); err != nil || !found || got != body {
							t.Fatalf("%s: document %s = %q found=%v err=%v, want %s", name, id, got, found, err, body)
						}
					}
				}
			})
		}
	}
}
