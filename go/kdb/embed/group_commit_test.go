package embed

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
)

// fakeParticipant records what CommitGroup does to it.
type fakeParticipant struct {
	ns       string
	rt       *EmbeddedKdbRuntime
	log      *[]string
	mu       *sync.Mutex
	lockErr  error
	prepErr  error
	applyErr error
	fenced   error
	locked   bool
}

func (f *fakeParticipant) record(s string) {
	f.mu.Lock()
	*f.log = append(*f.log, s)
	f.mu.Unlock()
}

func (f *fakeParticipant) Namespace() string            { return f.ns }
func (f *fakeParticipant) Runtime() *EmbeddedKdbRuntime { return f.rt }
func (f *fakeParticipant) Fence(cause error)            { f.fenced = cause; f.record("fence " + f.ns) }
func (f *fakeParticipant) PublishStarted()              { f.record("publish-start " + f.ns) }
func (f *fakeParticipant) PublishFinished()             { f.record("publish-end " + f.ns) }

func (f *fakeParticipant) Lock() (func(), error) {
	if f.lockErr != nil {
		return nil, f.lockErr
	}
	f.locked = true
	f.record("lock " + f.ns)
	return func() { f.locked = false; f.record("unlock " + f.ns) }, nil
}

func (f *fakeParticipant) Prepare(tx document.Transaction) (PreparedPart, error) {
	f.record("prepare " + f.ns)
	if f.prepErr != nil {
		return nil, f.prepErr
	}
	return &fakePrepared{f: f, tx: tx}, nil
}

type fakePrepared struct {
	f  *fakeParticipant
	tx document.Transaction
}

func (p *fakePrepared) Discard() { p.f.record("discard " + p.f.ns) }

func (p *fakePrepared) Apply(message string) (document.Commit, error) {
	p.f.record("apply " + p.f.ns)
	if p.f.applyErr != nil {
		return document.Commit{}, p.f.applyErr
	}
	if !strings.HasPrefix(message, groupMarkerPrefix) {
		p.f.record("bad message")
	}
	return document.Commit{NamespaceID: p.f.ns, TransactionID: p.tx.ID, Message: message}, nil
}

func fakes(t *testing.T, names ...string) ([]*fakeParticipant, *[]string) {
	t.Helper()
	log := &[]string{}
	mu := &sync.Mutex{}
	out := make([]*fakeParticipant, len(names))
	for i, n := range names {
		rt, err := OpenMemoryRuntime(CatalogFromNamespace(n), n, schema.None())
		if err != nil {
			t.Fatal(err)
		}
		out[i] = &fakeParticipant{ns: n, rt: rt, log: log, mu: mu}
	}
	return out, log
}

func partsOf(fs []*fakeParticipant) []GroupPart {
	out := make([]GroupPart, len(fs))
	for i, f := range fs {
		out[i] = GroupPart{Participant: f, Tx: document.Transaction{}}
	}
	return out
}

func TestCommitGroupRunsTheProtocolInNamespaceOrder(t *testing.T) {
	fs, log := fakes(t, "z/b", "a/a")
	res, wait, err := CommitGroup(NewMemoryTxnCoordinator(), partsOf(fs))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"lock a/a", "lock z/b",
		"prepare a/a", "prepare z/b",
		"publish-start a/a", "publish-start z/b",
		"apply a/a", "apply z/b",
		"publish-end a/a", "publish-end z/b",
		"unlock z/b", "unlock a/a",
	}
	if strings.Join(*log, "|") != strings.Join(want, "|") {
		t.Errorf("protocol order:\n got %v\nwant %v", *log, want)
	}
	if err := wait(); err != nil {
		t.Fatal(err)
	}
	if len(res.Commits) != 2 || res.Commits[0].Namespace != "a/a" {
		t.Fatalf("commits: %+v", res.Commits)
	}
	for _, c := range res.Commits {
		if c.Commit.TransactionID != res.Group || c.Commit.TransactionID == (codec.UUID{}) {
			t.Errorf("%s carries transaction id %s, want the group %s", c.Namespace, c.Commit.TransactionID, res.Group)
		}
	}
}

func TestCommitGroupRefusalWritesNothing(t *testing.T) {
	fs, log := fakes(t, "a/a", "b/b", "c/c")
	fs[1].prepErr = errors.New("nope")
	_, _, err := CommitGroup(NewMemoryTxnCoordinator(), partsOf(fs))
	var pe *GroupPartError
	if !errors.As(err, &pe) || pe.Namespace != "b/b" {
		t.Fatalf("want a GroupPartError naming b/b, got %v", err)
	}
	for _, entry := range *log {
		if strings.HasPrefix(entry, "apply") || strings.HasPrefix(entry, "fence") {
			t.Errorf("a refused group reached %q", entry)
		}
	}
	for _, f := range fs {
		if f.locked {
			t.Errorf("%s left locked", f.ns)
		}
	}
	if !contains(*log, "discard a/a") {
		t.Error("the already-prepared participant was not discarded")
	}
}

func TestCommitGroupLockFailureReleasesWhatItTook(t *testing.T) {
	fs, log := fakes(t, "a/a", "b/b")
	fs[1].lockErr = errors.New("busy")
	_, _, err := CommitGroup(NewMemoryTxnCoordinator(), partsOf(fs))
	var pe *GroupPartError
	if !errors.As(err, &pe) || pe.Namespace != "b/b" {
		t.Fatalf("want a GroupPartError naming b/b, got %v", err)
	}
	if fs[0].locked || !contains(*log, "unlock a/a") {
		t.Error("the first lock was not released")
	}
}

func TestCommitGroupFailureAfterPublicationFencesEveryone(t *testing.T) {
	fs, _ := fakes(t, "a/a", "b/b", "c/c")
	fs[1].applyErr = errors.New("disk")
	_, _, err := CommitGroup(NewMemoryTxnCoordinator(), partsOf(fs))
	var gf *GroupFailedError
	if !errors.As(err, &gf) {
		t.Fatalf("want a GroupFailedError, got %v", err)
	}
	for _, f := range fs {
		if f.fenced == nil {
			t.Errorf("%s was not fenced", f.ns)
		}
		if f.locked {
			t.Errorf("%s left locked", f.ns)
		}
	}
	// Nothing applied at all: abandoned, nobody fenced.
	fs2, _ := fakes(t, "a/a", "b/b")
	fs2[0].applyErr = errors.New("disk")
	if _, _, err := CommitGroup(NewMemoryTxnCoordinator(), partsOf(fs2)); err == nil || errors.As(err, &gf) {
		t.Fatalf("a failure before any publication should refuse, not fail the group: %v", err)
	}
	for _, f := range fs2 {
		if f.fenced != nil {
			t.Errorf("%s fenced although nothing was published", f.ns)
		}
	}
}

func TestCommitGroupRefusesDuplicatesAndEmpty(t *testing.T) {
	fs, _ := fakes(t, "a/a")
	if _, _, err := CommitGroup(NewMemoryTxnCoordinator(), nil); err == nil {
		t.Error("empty group accepted")
	}
	if _, _, err := CommitGroup(NewMemoryTxnCoordinator(), append(partsOf(fs), partsOf(fs)...)); err == nil {
		t.Error("duplicate namespace accepted")
	}
}

func contains(log []string, s string) bool {
	for _, l := range log {
		if l == s {
			return true
		}
	}
	return false
}
