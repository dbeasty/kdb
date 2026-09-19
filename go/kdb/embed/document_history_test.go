package embed_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Reading one document's past.
//
// The property these pin is the one an embedded caller could not have before:
// that a document id is enough to find the commits that wrote it, and that
// each of those commits still yields the bytes as they were at the time —
// not as they are now.

func historyRuntime(t *testing.T) (*embed.EmbeddedKdbRuntime, string) {
	t.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 1 << 20
	opts.Storage.HistoryMode = storage.HistoryModeFull
	rt, err := embed.OpenFileRuntimeWithOptions(t.TempDir(), "test", "test/docs", schema.None(), opts)
	if err != nil {
		t.Fatalf("opening a runtime: %v", err)
	}
	t.Cleanup(rt.Close)
	return rt, rt.DefaultNamespace
}

func TestDocumentVersionsFindsEveryWriteOfOneDocument(t *testing.T) {
	rt, ns := historyRuntime(t)

	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	other, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}

	// Three versions of the document we care about, with an unrelated
	// document written in between — the noise this has to see past.
	var wrote []codec.Hash
	for i := 1; i <= 3; i++ {
		res, err := embed.PutJSONDocument(rt, ns,
			fmt.Sprintf(`{"id":%q,"move":%d}`, id.String(), i))
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		wrote = append(wrote, res.Commit)
		if _, err := embed.PutJSONDocument(rt, ns,
			fmt.Sprintf(`{"id":%q,"unrelated":%d}`, other.String(), i)); err != nil {
			t.Fatalf("noise %d: %v", i, err)
		}
	}

	versions, err := embed.DocumentVersions(rt, id)
	if err != nil {
		t.Fatalf("DocumentVersions: %v", err)
	}
	if len(versions) != len(wrote) {
		t.Fatalf("got %d versions, want %d — the unrelated writes leaked in or ours were missed",
			len(versions), len(wrote))
	}
	for i, v := range versions {
		if v.Commit != wrote[i] {
			t.Errorf("version %d is commit %s, want %s (oldest first)", i, v.Commit.Hex(), wrote[i].Hex())
		}
		if v.Deleted {
			t.Errorf("version %d reported a delete", i)
		}
	}
}

func TestDocumentAtReadsTheBytesAsTheyWere(t *testing.T) {
	rt, ns := historyRuntime(t)

	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	var commits []codec.Hash
	for i := 1; i <= 3; i++ {
		res, err := embed.PutJSONDocument(rt, ns, fmt.Sprintf(`{"id":%q,"move":%d}`, id.String(), i))
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		commits = append(commits, res.Commit)
	}

	// Each commit still yields the document as it stood then, not as it
	// stands now — which is the whole point of asking.
	for i, at := range commits {
		body, ok, err := embed.DocumentAt(rt, ns, id, at)
		if err != nil {
			t.Fatalf("DocumentAt %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("DocumentAt %d: the document was not there", i)
		}
		if want := fmt.Sprintf(`"move":%d`, i+1); !strings.Contains(body, want) {
			t.Errorf("at commit %d the document reads %s, want it to contain %s", i, body, want)
		}
	}
}

func TestDocumentAtBeforeADocumentExisted(t *testing.T) {
	rt, ns := historyRuntime(t)

	first, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	early, err := embed.PutJSONDocument(rt, ns, fmt.Sprintf(`{"id":%q,"n":1}`, first.String()))
	if err != nil {
		t.Fatal(err)
	}

	later, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := embed.PutJSONDocument(rt, ns, fmt.Sprintf(`{"id":%q,"n":1}`, later.String())); err != nil {
		t.Fatal(err)
	}

	// Asking for the second document at a commit from before it was written
	// is a legitimate question with a plain answer, not an error.
	if _, ok, err := embed.DocumentAt(rt, ns, later, early.Commit); err != nil {
		t.Fatalf("DocumentAt: %v", err)
	} else if ok {
		t.Errorf("a document reported as present at a commit written before it existed")
	}
}

func TestDocumentVersionsOfSomethingNeverWritten(t *testing.T) {
	rt, _ := historyRuntime(t)
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	versions, err := embed.DocumentVersions(rt, id)
	if err != nil {
		t.Fatalf("DocumentVersions: %v", err)
	}
	if len(versions) != 0 {
		t.Errorf("a document nobody wrote has %d versions", len(versions))
	}
}
