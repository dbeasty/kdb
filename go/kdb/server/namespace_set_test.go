package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

// --- fixtures -------------------------------------------------------------------------------

// fileSet opens a host over root with one server runtime per namespace, all in one NamespaceSet.
func fileSet(t *testing.T, root string, namespaces ...string) (*embed.Host, *NamespaceSet) {
	t.Helper()
	host, err := embed.OpenFileHost(root, embed.FileRuntimeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	set := NewNamespaceSet(host.Transactions())
	for _, ns := range namespaces {
		rt, err := host.Namespace(embed.CatalogFromNamespace(ns), ns, schema.None())
		if err != nil {
			t.Fatal(err)
		}
		if err := set.Add(NewKdbServerRuntime(rt)); err != nil {
			t.Fatal(err)
		}
	}
	return host, set
}

func memorySet(t *testing.T, namespaces ...string) *NamespaceSet {
	t.Helper()
	set := NewNamespaceSet(nil)
	for _, ns := range namespaces {
		rt, err := embed.OpenMemoryRuntime(embed.CatalogFromNamespace(ns), ns, schema.None())
		if err != nil {
			t.Fatal(err)
		}
		if err := set.Add(NewKdbServerRuntime(rt)); err != nil {
			t.Fatal(err)
		}
	}
	return set
}

func mustUUID(t testing.TB) codec.UUID {
	t.Helper()
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func writeTx(base codec.Hash, docID codec.UUID, body string) document.Transaction {
	return document.Transaction{
		BaseVersion: base,
		Operations:  []document.Op{document.WriteOp{DocID: docID, Patch: body}},
		Timestamp:   codec.TimestampNow(),
	}
}

func headOf(t testing.TB, set *NamespaceSet, ns string) codec.Hash {
	t.Helper()
	rt, ok := set.Get(ns)
	if !ok {
		t.Fatalf("namespace %s not in set", ns)
	}
	h, err := rt.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func docAtHead(t testing.TB, set *NamespaceSet, ns string, id codec.UUID) (string, bool) {
	t.Helper()
	rt, _ := set.Get(ns)
	body, _, found, err := rt.GetDocument(ns, id)
	if err != nil {
		t.Fatal(err)
	}
	return body, found
}

func docAtRuntime(t testing.TB, rt *embed.EmbeddedKdbRuntime, id codec.UUID) (string, bool) {
	t.Helper()
	_, head, ok, err := rt.DAG.HeadCommit()
	if err != nil || !ok {
		t.Fatalf("head: ok=%v err=%v", ok, err)
	}
	doc, err := rt.Storage.GetDocument(rt.DefaultNamespace, id, head.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	if doc == nil {
		return "", false
	}
	return doc.JSON, true
}

func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(target)
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

// --- the transaction lands everywhere or nowhere ---------------------------------------------

func TestCrossNamespaceCommitLandsInEveryNamespace(t *testing.T) {
	for _, mode := range []string{"memory", "file"} {
		t.Run(mode, func(t *testing.T) {
			var set *NamespaceSet
			if mode == "memory" {
				set = memorySet(t, "bank/accounts", "bank/ledger")
			} else {
				host, s := fileSet(t, t.TempDir(), "bank/accounts", "bank/ledger")
				defer host.Close()
				set = s
			}
			acct, entry := mustUUID(t), mustUUID(t)
			res, err := set.CommitAcross([]NamespaceTransaction{
				{Namespace: "bank/ledger", Tx: writeTx(codec.Hash{}, entry, `{"amount":10}`)},
				{Namespace: "bank/accounts", Tx: writeTx(codec.Hash{}, acct, `{"balance":90}`)},
			}, auth.Principal{})
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Commits) != 2 || res.Commits[0].Namespace != "bank/accounts" {
				t.Fatalf("commits not returned in namespace order: %+v", res.Commits)
			}
			for _, c := range res.Commits {
				if c.Commit.TransactionID != res.Group {
					t.Errorf("%s: transaction id %s, want the group id %s", c.Namespace, c.Commit.TransactionID, res.Group)
				}
				m, ok := embed.ParseGroupMarker(c.Commit.Message)
				if !ok || m.Group != res.Group || strings.Join(m.Parts, ",") != "bank/accounts,bank/ledger" {
					t.Errorf("%s: message %q does not describe the group", c.Namespace, c.Commit.Message)
				}
				if h := headOf(t, set, c.Namespace); h != c.Commit.Hash {
					t.Errorf("%s: head %s is not the group's commit %s", c.Namespace, h.Hex(), c.Commit.Hash.Hex())
				}
			}
			if body, ok := docAtHead(t, set, "bank/accounts", acct); !ok || !strings.Contains(body, `"balance":90`) {
				t.Errorf("account: %q %v", body, ok)
			}
			if body, ok := docAtHead(t, set, "bank/ledger", entry); !ok || !strings.Contains(body, `"amount":10`) {
				t.Errorf("ledger entry: %q %v", body, ok)
			}
		})
	}
}

// Every way one participant can refuse must leave every namespace exactly as it was.
func TestCrossNamespaceRejectionWritesNothingAnywhere(t *testing.T) {
	uniqueEmail := func(t *testing.T) schema.KdbSchema {
		sch, err := schema.Build([]schema.Field{
			schema.MustField("email", schema.StringType{}, false, true, true),
		}, 1, codec.Timestamp{}, "unique email")
		if err != nil {
			t.Fatal(err)
		}
		return sch
	}

	cases := []struct {
		name  string
		setup func(t *testing.T, set *NamespaceSet) NamespaceTransaction
		check func(t *testing.T, err error)
	}{
		{
			name: "conflict",
			setup: func(t *testing.T, set *NamespaceSet) NamespaceTransaction {
				id := mustUUID(t)
				base := headOf(t, set, "b")
				rt, _ := set.Get("b")
				if _, err := rt.Commit("b", writeTx(base, id, `{"v":1}`), "", auth.Principal{}); err != nil {
					t.Fatal(err)
				}
				stale := base
				if _, err := rt.Commit("b", writeTx(headOf(t, set, "b"), id, `{"v":2}`), "", auth.Principal{}); err != nil {
					t.Fatal(err)
				}
				// Anchored before both writes: the document changed under it.
				return NamespaceTransaction{Namespace: "b", Tx: writeTx(stale, id, `{"v":3}`)}
			},
			check: func(t *testing.T, err error) {
				var conflict *ConflictError
				if !errors.As(err, &conflict) {
					t.Fatalf("want a *ConflictError, got %v", err)
				}
			},
		},
		{
			name: "unique",
			setup: func(t *testing.T, set *NamespaceSet) NamespaceTransaction {
				rt, _ := set.Get("b")
				rt.SetSchema(uniqueEmail(t))
				if _, err := rt.Commit("b", writeTx(headOf(t, set, "b"), mustUUID(t), `{"email":"a@x"}`), "", auth.Principal{}); err != nil {
					t.Fatal(err)
				}
				return NamespaceTransaction{Namespace: "b", Tx: writeTx(codec.Hash{}, mustUUID(t), `{"email":"a@x"}`)}
			},
			check: func(t *testing.T, err error) {
				var se *SchemaError
				if !errors.As(err, &se) || !se.HasUniqueViolation() {
					t.Fatalf("want a unique violation, got %v", err)
				}
			},
		},
		{
			name: "precondition",
			setup: func(t *testing.T, set *NamespaceSet) NamespaceTransaction {
				id := mustUUID(t)
				rt, _ := set.Get("b")
				if _, err := rt.Commit("b", writeTx(headOf(t, set, "b"), id, `{"v":1}`), "", auth.Principal{}); err != nil {
					t.Fatal(err)
				}
				tx := writeTx(codec.Hash{}, id, `{"v":2}`)
				tx.Preconditions = []document.Precondition{{OpIndex: 0, Kind: document.ExpectAbsent}}
				return NamespaceTransaction{Namespace: "b", Tx: tx}
			},
			check: func(t *testing.T, err error) {
				var conflict *ConflictError
				if !errors.As(err, &conflict) {
					t.Fatalf("want a *ConflictError for the failed precondition, got %v", err)
				}
			},
		},
		{
			name: "authorization",
			setup: func(t *testing.T, set *NamespaceSet) NamespaceTransaction {
				rt, _ := set.Get("b")
				rt.AuthEngine = denyWrites{}
				return NamespaceTransaction{Namespace: "b", Tx: writeTx(codec.Hash{}, mustUUID(t), `{"v":1}`)}
			},
			check: func(t *testing.T, err error) {
				var ae *AuthorizationError
				if !errors.As(err, &ae) {
					t.Fatalf("want an *AuthorizationError, got %v", err)
				}
			},
		},
		{
			name: "leased document",
			setup: func(t *testing.T, set *NamespaceSet) NamespaceTransaction {
				id := mustUUID(t)
				rt, _ := set.Get("b")
				if _, err := rt.DocumentLocks.TryAcquireLease("b", id, "someone-else", time.Minute); err != nil {
					t.Fatal(err)
				}
				return NamespaceTransaction{Namespace: "b", Tx: writeTx(codec.Hash{}, id, `{"v":1}`)}
			},
			check: func(t *testing.T, err error) {
				if err == nil {
					t.Fatal("a document leased to another session was written")
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			host, set := fileSet(t, t.TempDir(), "a", "b")
			defer host.Close()
			bPart := tc.setup(t, set)
			aBefore, bBefore := headOf(t, set, "a"), headOf(t, set, "b")
			aDoc := mustUUID(t)

			_, err := set.CommitAcross([]NamespaceTransaction{
				{Namespace: "a", Tx: writeTx(codec.Hash{}, aDoc, `{"x":1}`)},
				bPart,
			}, auth.Principal{})
			if err == nil {
				t.Fatal("the transaction committed")
			}
			var cne *CrossNamespaceError
			if !errors.As(err, &cne) || cne.Namespace != "b" {
				t.Fatalf("want a CrossNamespaceError naming b, got %v", err)
			}
			tc.check(t, err)
			if h := headOf(t, set, "a"); h != aBefore {
				t.Error("namespace a moved although b refused")
			}
			if h := headOf(t, set, "b"); h != bBefore {
				t.Error("namespace b moved although it refused")
			}
			if _, found := docAtHead(t, set, "a", aDoc); found {
				t.Error("a's document is visible")
			}
			// Nothing was left staged: the next ordinary commit to a writes only its own document.
			rtA, _ := set.Get("a")
			other := mustUUID(t)
			if _, err := rtA.Commit("a", writeTx(headOf(t, set, "a"), other, `{"y":1}`), "", auth.Principal{}); err != nil {
				t.Fatal(err)
			}
			if _, found := docAtHead(t, set, "a", aDoc); found {
				t.Error("the refused transaction's staged write leaked into a later commit")
			}
		})
	}
}

type denyWrites struct{}

func (denyWrites) Authenticator() auth.Authenticator { return auth.AllowAll.Authenticator() }
func (denyWrites) Authorizer() auth.Authorizer       { return denyWrites{} }
func (denyWrites) Authorize(_ context.Context, _ auth.Principal, action auth.Action) error {
	switch action.(type) {
	case auth.DocumentWriteAction, auth.DocumentDeleteAction:
		return errors.New("writes denied")
	}
	return nil
}

func TestCrossNamespaceRefusesDuplicateAndEmpty(t *testing.T) {
	set := memorySet(t, "a", "b")
	if _, err := set.CommitAcross(nil, auth.Principal{}); err == nil {
		t.Error("an empty transaction was accepted")
	}
	_, err := set.CommitAcross([]NamespaceTransaction{
		{Namespace: "a", Tx: writeTx(codec.Hash{}, mustUUID(t), `{}`)},
		{Namespace: "a", Tx: writeTx(codec.Hash{}, mustUUID(t), `{}`)},
	}, auth.Principal{})
	if err == nil {
		t.Error("two transactions into one namespace were accepted")
	}
	_, err = set.CommitAcross([]NamespaceTransaction{
		{Namespace: "missing", Tx: writeTx(codec.Hash{}, mustUUID(t), `{}`)},
	}, auth.Principal{})
	if !errors.Is(err, ErrUnknownNamespace) {
		t.Errorf("want ErrUnknownNamespace, got %v", err)
	}
}

// --- durability and recovery -----------------------------------------------------------------

func TestCrossNamespaceCommitSurvivesReopenAndSealsItsEpoch(t *testing.T) {
	root := t.TempDir()
	host, set := fileSet(t, root, "a", "b")
	aDoc, bDoc := mustUUID(t), mustUUID(t)
	for i := 0; i < 5; i++ {
		if _, err := set.CommitAcross([]NamespaceTransaction{
			{Namespace: "a", Tx: writeTx(codec.Hash{}, aDoc, fmt.Sprintf(`{"n":%d}`, i))},
			{Namespace: "b", Tx: writeTx(codec.Hash{}, bDoc, fmt.Sprintf(`{"n":%d}`, i))},
		}, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	// A clean close with nothing in flight seals the epoch: no decision file is left behind.
	entries, _ := os.ReadDir(filepath.Join(root, "txn"))
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "decisions-") {
			t.Errorf("clean close left %s behind", e.Name())
		}
	}

	host2, set2 := fileSet(t, root, "a", "b")
	defer host2.Close()
	for _, ns := range []string{"a", "b"} {
		id := aDoc
		if ns == "b" {
			id = bDoc
		}
		if body, ok := docAtHead(t, set2, ns, id); !ok || !strings.Contains(body, `"n":4`) {
			t.Errorf("%s after reopen: %q %v", ns, body, ok)
		}
	}
}

// The crash that matters: every part is on disk, the decision is not. Recovery has to roll the
// group back in every namespace - and roll back anything built on it, which was never acknowledged.
func TestCrashBeforeDecisionRollsBackEveryPartAndWhatWasBuiltOnIt(t *testing.T) {
	root := t.TempDir()
	host, set := fileSet(t, root, "a", "b")
	defer host.Close()
	rtA, _ := set.Get("a")

	settled := mustUUID(t)
	if _, err := rtA.Commit("a", writeTx(headOf(t, set, "a"), settled, `{"before":true}`), "", auth.Principal{}); err != nil {
		t.Fatal(err)
	}

	crashed := filepath.Join(t.TempDir(), "crashed")
	release := make(chan struct{})
	reached := make(chan struct{})
	var chainedDone atomic.Bool
	chainedErr := make(chan error, 1)
	after := mustUUID(t)
	set.Coordinator().SetBeforeDecisionHookForTest(func(codec.UUID) {
		// Parts are durable. Queue an ordinary commit on top of a's part: it must not be
		// acknowledged while the group is undecided.
		go func() {
			_, err := rtA.Commit("a", writeTx(headOf(t, set, "a"), after, `{"after":true}`), "", auth.Principal{})
			chainedDone.Store(true)
			chainedErr <- err
		}()
		// Wait until that commit is on disk too, then capture the directory: this is the state a
		// kill -9 at this instant leaves.
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if _, ok := docAtHead(t, set, "a", after); ok {
				break
			}
			time.Sleep(time.Millisecond)
		}
		time.Sleep(50 * time.Millisecond) // its log write, which does not wait for the decision
		if chainedDone.Load() {
			t.Error("a commit built on an undecided group was acknowledged before the decision")
		}
		copyDir(t, root, crashed)
		close(reached)
		<-release
	})

	aDoc, bDoc := mustUUID(t), mustUUID(t)
	groupErr := make(chan error, 1)
	go func() {
		_, err := set.CommitAcross([]NamespaceTransaction{
			{Namespace: "a", Tx: writeTx(codec.Hash{}, aDoc, `{"group":true}`)},
			{Namespace: "b", Tx: writeTx(codec.Hash{}, bDoc, `{"group":true}`)},
		}, auth.Principal{})
		groupErr <- err
	}()
	<-reached
	close(release)
	if err := <-groupErr; err != nil {
		t.Fatal(err)
	}
	if err := <-chainedErr; err != nil {
		t.Fatalf("the chained commit failed once the group committed: %v", err)
	}

	// The live process committed all of it.
	for _, want := range []struct {
		ns string
		id codec.UUID
	}{{"a", aDoc}, {"b", bDoc}, {"a", after}, {"a", settled}} {
		if _, ok := docAtHead(t, set, want.ns, want.id); !ok {
			t.Errorf("live: %s/%s missing", want.ns, want.id)
		}
	}

	// The crashed copy committed none of the group and nothing built on it.
	reopenCrashed := func() (*embed.Host, *embed.EmbeddedKdbRuntime, *embed.EmbeddedKdbRuntime) {
		h, err := embed.OpenFileHost(crashed, embed.FileRuntimeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		a, err := h.Namespace("a", "a", schema.None())
		if err != nil {
			t.Fatal(err)
		}
		b, err := h.Namespace("b", "b", schema.None())
		if err != nil {
			t.Fatal(err)
		}
		return h, a, b
	}
	h2, a2, b2 := reopenCrashed()
	if _, ok := docAtRuntime(t, a2, aDoc); ok {
		t.Error("crashed: a's part survived without a decision")
	}
	if _, ok := docAtRuntime(t, b2, bDoc); ok {
		t.Error("crashed: b's part survived without a decision")
	}
	if _, ok := docAtRuntime(t, a2, after); ok {
		t.Error("crashed: a commit built on the rolled-back part survived")
	}
	if _, ok := docAtRuntime(t, a2, settled); !ok {
		t.Error("crashed: a commit from before the group was lost")
	}

	// Recovery's answer is stable: new work after it survives the next open, even though it lands
	// in the log after the rolled-back part.
	srvA := NewKdbServerRuntime(a2)
	later := mustUUID(t)
	head, _ := a2.DAG.Head()
	if _, err := srvA.Commit("a", writeTx(head, later, `{"later":true}`), "", auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	if err := h2.Close(); err != nil {
		t.Fatal(err)
	}
	h3, a3, b3 := reopenCrashed()
	defer h3.Close()
	if _, ok := docAtRuntime(t, a3, later); !ok {
		t.Error("a commit made after recovery was lost on the next open")
	}
	if _, ok := docAtRuntime(t, a3, aDoc); ok {
		t.Error("the rolled-back part came back on the next open")
	}
	if _, ok := docAtRuntime(t, b3, bDoc); ok {
		t.Error("the rolled-back part came back in b on the next open")
	}
}

// A failure after publication - here, the decision log refusing the write - must fence every
// participant, and recovery must roll the group back.
func TestFailureAfterPublicationFencesAndRollsBack(t *testing.T) {
	root := t.TempDir()
	host, set := fileSet(t, root, "a", "b")
	rtA, _ := set.Get("a")
	set.Coordinator().SetDecisionFailureForTest(errors.New("disk on fire"))

	aDoc := mustUUID(t)
	_, err := set.CommitAcross([]NamespaceTransaction{
		{Namespace: "a", Tx: writeTx(codec.Hash{}, aDoc, `{"x":1}`)},
		{Namespace: "b", Tx: writeTx(codec.Hash{}, mustUUID(t), `{"x":1}`)},
	}, auth.Principal{})
	var failed *embed.GroupFailedError
	if !errors.As(err, &failed) {
		t.Fatalf("want *embed.GroupFailedError, got %v", err)
	}
	if _, err := rtA.Commit("a", writeTx(headOf(t, set, "a"), mustUUID(t), `{}`), "", auth.Principal{}); !errors.As(err, &failed) {
		t.Fatalf("a fenced namespace accepted a write: %v", err)
	}
	if err := host.Close(); err != nil {
		t.Logf("close: %v", err)
	}

	host2, set2 := fileSet(t, root, "a", "b")
	defer host2.Close()
	if _, ok := docAtHead(t, set2, "a", aDoc); ok {
		t.Error("the failed group's part survived a reopen")
	}
	rt2, _ := set2.Get("a")
	if _, err := rt2.Commit("a", writeTx(headOf(t, set2, "a"), mustUUID(t), `{}`), "", auth.Principal{}); err != nil {
		t.Errorf("a reopened namespace is still fenced: %v", err)
	}
}

// A read-only follower must not see a group until the writer has decided it.
func TestFollowerHoldsAnUndecidedGroup(t *testing.T) {
	root := t.TempDir()
	host, set := fileSet(t, root, "a", "b")
	defer host.Close()
	// A decided group first, so the follower has an epoch to look at.
	if _, err := set.CommitAcross([]NamespaceTransaction{
		{Namespace: "a", Tx: writeTx(codec.Hash{}, mustUUID(t), `{}`)},
		{Namespace: "b", Tx: writeTx(codec.Hash{}, mustUUID(t), `{}`)},
	}, auth.Principal{}); err != nil {
		t.Fatal(err)
	}

	aDoc := mustUUID(t)
	var follower *embed.EmbeddedKdbRuntime
	release := make(chan struct{})
	set.Coordinator().SetBeforeDecisionHookForTest(func(codec.UUID) {
		f, err := embed.OpenReadOnlyFileRuntime(root, "a", "a", schema.None())
		if err != nil {
			t.Error(err)
		} else {
			follower = f
			if _, ok := docAtRuntime(t, f, aDoc); ok {
				t.Error("a follower saw a group before it was decided")
			}
		}
		<-release
	})
	done := make(chan error, 1)
	go func() {
		_, err := set.CommitAcross([]NamespaceTransaction{
			{Namespace: "a", Tx: writeTx(codec.Hash{}, aDoc, `{"y":1}`)},
			{Namespace: "b", Tx: writeTx(codec.Hash{}, mustUUID(t), `{}`)},
		}, auth.Principal{})
		done <- err
	}()
	time.Sleep(200 * time.Millisecond)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if follower == nil {
		t.Fatal("follower never opened")
	}
	defer follower.Close()
	if err := follower.Refresh(); err != nil {
		t.Fatal(err)
	}
	if _, ok := docAtRuntime(t, follower, aDoc); !ok {
		t.Error("the follower did not pick the group up once it was decided")
	}
}

func TestEpochRollsOnceItsFileIsLarge(t *testing.T) {
	root := t.TempDir()
	host, set := fileSet(t, root, "a", "b")
	set.Coordinator().SetRollThresholdForTest(16 + 3*20)
	var epochs []uint64
	for i := 0; i < 10; i++ {
		if _, err := set.CommitAcross([]NamespaceTransaction{
			{Namespace: "a", Tx: writeTx(codec.Hash{}, mustUUID(t), `{}`)},
			{Namespace: "b", Tx: writeTx(codec.Hash{}, mustUUID(t), `{}`)},
		}, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
		epochs = append(epochs, set.Coordinator().Epoch())
	}
	if epochs[len(epochs)-1] < 3 {
		t.Errorf("ten groups at three per epoch should have rolled at least twice, epochs seen: %v", epochs)
	}
	entries, _ := os.ReadDir(filepath.Join(root, "txn"))
	live := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			live++
		}
	}
	if live > 1 {
		t.Errorf("%d live decision files; rolled epochs should have been sealed", live)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	// Every group of every rolled epoch still reads back as committed.
	host2, set2 := fileSet(t, root, "a", "b")
	defer host2.Close()
	rt, _ := set2.Get("a")
	if n := countCommits(t, rt); n < 10 {
		t.Errorf("after reopen namespace a has %d commits, want at least the 10 group parts", n)
	}
}

// --- concurrency ------------------------------------------------------------------------------

// Money moves between accounts in three namespaces, concurrently, with conflicts retried. At every
// consistent snapshot, and at the end, the total is what it started as. Run under -race.
func TestConcurrentTransfersPreserveTheTotal(t *testing.T) {
	for _, mode := range []string{"memory", "file"} {
		t.Run(mode, func(t *testing.T) {
			namespaces := []string{"n1", "n2", "n3"}
			var set *NamespaceSet
			if mode == "memory" {
				set = memorySet(t, namespaces...)
			} else {
				host, s := fileSet(t, t.TempDir(), namespaces...)
				defer host.Close()
				set = s
			}
			const perNS = 3
			const initial = 1000
			type account struct {
				ns string
				id codec.UUID
			}
			var accounts []account
			for _, ns := range namespaces {
				rt, _ := set.Get(ns)
				for i := 0; i < perNS; i++ {
					a := account{ns, mustUUID(t)}
					accounts = append(accounts, a)
					if _, err := rt.Commit(ns, writeTx(headOf(t, set, ns), a.id, fmt.Sprintf(`{"balance":%d}`, initial)), "", auth.Principal{}); err != nil {
						t.Fatal(err)
					}
				}
			}
			want := initial * len(accounts)

			balanceAt := func(ns string, id codec.UUID, head codec.Hash) int {
				rt, _ := set.Get(ns)
				c, ok := rt.Runtime.DAG.GetCommit(head)
				if !ok {
					t.Fatalf("snapshot head %s missing", head.Hex())
				}
				doc, err := rt.Runtime.Storage.GetDocument(ns, id, c.DocumentTreeHash)
				if err != nil || doc == nil {
					t.Fatalf("account %s/%s unreadable: %v", ns, id, err)
				}
				var v struct{ Balance int }
				if err := json.Unmarshal([]byte(doc.JSON), &v); err != nil {
					t.Fatal(err)
				}
				return v.Balance
			}
			total := func() int {
				heads, err := set.Snapshot(namespaces...)
				if err != nil {
					t.Fatal(err)
				}
				sum := 0
				for _, a := range accounts {
					sum += balanceAt(a.ns, a.id, heads[a.ns])
				}
				return sum
			}

			stop := make(chan struct{})
			var readerWG sync.WaitGroup
			var snapshots, torn atomic.Int64
			for r := 0; r < 2; r++ {
				readerWG.Add(1)
				go func() {
					defer readerWG.Done()
					for {
						select {
						case <-stop:
							return
						default:
						}
						if got := total(); got != want {
							torn.Add(1)
							t.Errorf("a snapshot saw a total of %d, want %d", got, want)
							return
						}
						snapshots.Add(1)
					}
				}()
			}

			const workers, transfers = 8, 40
			var wg sync.WaitGroup
			var committed, conflicts atomic.Int64
			deadline := time.After(60 * time.Second)
			finished := make(chan struct{})
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(seed uint64) {
					defer wg.Done()
					rng := rand.New(rand.NewPCG(seed, seed*7+1))
					for n := 0; n < transfers; {
						from := accounts[rng.IntN(len(accounts))]
						to := accounts[rng.IntN(len(accounts))]
						if from.ns == to.ns {
							continue
						}
						heads, err := set.Snapshot(from.ns, to.ns)
						if err != nil {
							t.Error(err)
							return
						}
						amount := 1 + rng.IntN(20)
						fb := balanceAt(from.ns, from.id, heads[from.ns])
						tb := balanceAt(to.ns, to.id, heads[to.ns])
						_, err = set.CommitAcross([]NamespaceTransaction{
							{Namespace: from.ns, Tx: writeTx(heads[from.ns], from.id, fmt.Sprintf(`{"balance":%d}`, fb-amount))},
							{Namespace: to.ns, Tx: writeTx(heads[to.ns], to.id, fmt.Sprintf(`{"balance":%d}`, tb+amount))},
						}, auth.Principal{})
						var conflict *ConflictError
						switch {
						case err == nil:
							committed.Add(1)
							n++
						case errors.As(err, &conflict):
							conflicts.Add(1)
						default:
							t.Errorf("transfer: %v", err)
							return
						}
					}
				}(uint64(w + 1))
			}
			go func() { wg.Wait(); close(finished) }()
			select {
			case <-finished:
			case <-deadline:
				t.Fatal("transfers did not finish - deadlock?")
			}
			close(stop)
			readerWG.Wait()
			if got := total(); got != want {
				t.Fatalf("final total %d, want %d", got, want)
			}
			t.Logf("%d transfers committed, %d conflicts retried, %d consistent snapshots", committed.Load(), conflicts.Load(), snapshots.Load())
		})
	}
}

// Groups over overlapping namespace sets, in every order, alongside single-namespace writers: the
// ordered gate acquisition must never deadlock.
func TestOverlappingGroupsNeverDeadlock(t *testing.T) {
	namespaces := []string{"p", "q", "r", "s"}
	host, set := fileSet(t, t.TempDir(), namespaces...)
	defer host.Close()
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for w := 0; w < 12; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, seed))
			for i := 0; i < 25; i++ {
				perm := rng.Perm(len(namespaces))
				k := 1 + rng.IntN(len(namespaces))
				if k == 1 && rng.IntN(2) == 0 {
					rt, _ := set.Get(namespaces[perm[0]])
					if _, err := rt.Upsert(namespaces[perm[0]], mustUUID(t), `{}`, auth.Principal{}); err != nil {
						errs <- err
						return
					}
					continue
				}
				var parts []NamespaceTransaction
				for _, idx := range perm[:k] {
					parts = append(parts, NamespaceTransaction{Namespace: namespaces[idx], Tx: writeTx(codec.Hash{}, mustUUID(t), `{}`)})
				}
				if _, err := set.CommitAcross(parts, auth.Principal{}); err != nil {
					errs <- err
					return
				}
			}
		}(uint64(w + 1))
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("overlapping groups deadlocked")
	}
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func countCommits(t *testing.T, rt *KdbServerRuntime) int {
	t.Helper()
	nav, ok := rt.Runtime.DAG.(dag.HistoryNavigator)
	if !ok {
		t.Fatalf("%T cannot list commits", rt.Runtime.DAG)
	}
	head, err := rt.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	commits, err := nav.ListCommits(head, 0, 10000)
	if err != nil {
		t.Fatal(err)
	}
	return len(commits)
}
