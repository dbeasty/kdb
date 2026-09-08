package embed_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// The conformance matrix: every history operation, run under both modes,
// with the outcome each mode owes.
//
// This is the artifact that keeps the two modes from drifting apart. The
// interesting column is not "none fails" - it is how much of the matrix
// *matches*, because inside its retention window a none namespace is
// supposed to behave exactly like a full one. A change that makes one of
// the "same" rows differ is a regression even if every test that names a
// mode still passes.

type modeCase struct {
	name string
	// run performs the operation and reports whether it succeeded plus
	// something comparable about what it returned.
	run func(t *testing.T, rt *embed.EmbeddedKdbRuntime) (ok bool, detail string)
	// sameUnderBothModes says this operation must behave identically on a
	// none namespace whose window still covers the history being asked
	// for. Where it is false, wantNoneOK says what none does instead.
	sameUnderBothModes bool
	wantNoneOK         bool
}

func conformanceRuntime(t *testing.T, root string, mode storage.HistoryMode) *embed.EmbeddedKdbRuntime {
	t.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.HistoryMode = mode
	// A window wide enough to cover everything these cases write, so the
	// comparison is about the mode rather than about the clock.
	opts.Storage.Retain = storage.RetentionWindow{Duration: storage.DefaultRetentionDuration}
	rt, err := embed.OpenFileRuntimeWithOptions(root, "app", "app/docs", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func seedHistory(t *testing.T, rt *embed.EmbeddedKdbRuntime) {
	t.Helper()
	for i := 0; i < 4; i++ {
		if _, err := embed.PutJSONDocument(rt, "app/docs", fmt.Sprintf(`{"id":"a","n":%d}`, i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := embed.PutJSONDocument(rt, "app/docs", `{"id":"b","n":0}`); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryOperationsConformAcrossModes(t *testing.T) {
	cases := []modeCase{
		{
			name:               "read at head",
			sameUnderBothModes: true,
			run: func(t *testing.T, rt *embed.EmbeddedKdbRuntime) (bool, string) {
				body, ok := readDoc(t, rt, "a")
				return ok, body
			},
		},
		{
			name:               "read at an earlier commit",
			sameUnderBothModes: true,
			run: func(t *testing.T, rt *embed.EmbeddedKdbRuntime) (bool, string) {
				nav := rt.DAG.(dag.HistoryNavigator)
				h, err := nav.ResolveRevision("head~2")
				if err != nil {
					return false, err.Error()
				}
				c, err := rt.DAG.GetCommitOrThrow(h)
				if err != nil {
					return false, err.Error()
				}
				docID, _, err := documentIDOf(`{"id":"a"}`)
				if err != nil {
					return false, err.Error()
				}
				doc, err := rt.Storage.GetDocument("app/docs", docID, c.DocumentTreeHash)
				if err != nil || doc == nil {
					return false, fmt.Sprint(err)
				}
				return true, doc.JSON
			},
		},
		{
			name:               "list commits",
			sameUnderBothModes: true,
			run: func(t *testing.T, rt *embed.EmbeddedKdbRuntime) (bool, string) {
				nav := rt.DAG.(dag.HistoryNavigator)
				head, _ := rt.DAG.Head()
				entries, err := nav.ListCommits(head, 0, 10)
				if err != nil {
					return false, err.Error()
				}
				return true, fmt.Sprintf("%d commits", len(entries))
			},
		},
		{
			name:               "resolve a relative revision",
			sameUnderBothModes: true,
			run: func(t *testing.T, rt *embed.EmbeddedKdbRuntime) (bool, string) {
				nav := rt.DAG.(dag.HistoryNavigator)
				if _, err := nav.ResolveRevision("head~3"); err != nil {
					return false, err.Error()
				}
				return true, "resolved"
			},
		},
		{
			name:               "diff two revisions",
			sameUnderBothModes: true,
			run: func(t *testing.T, rt *embed.EmbeddedKdbRuntime) (bool, string) {
				d, err := embed.DiffCommits(rt, "head~2", "head")
				if err != nil {
					return false, err.Error()
				}
				return true, fmt.Sprintf("%d changed", len(d.Entries))
			},
		},
		{
			name:               "revert to an earlier commit",
			sameUnderBothModes: true,
			run: func(t *testing.T, rt *embed.EmbeddedKdbRuntime) (bool, string) {
				res, err := embed.RevertTo(rt, "app/docs", "head~2")
				if err != nil {
					return false, err.Error()
				}
				return true, fmt.Sprintf("%d restored, %d removed", res.Restored, res.Removed)
			},
		},
		{
			name:               "resolve a revision past the root",
			sameUnderBothModes: true,
			run: func(t *testing.T, rt *embed.EmbeddedKdbRuntime) (bool, string) {
				nav := rt.DAG.(dag.HistoryNavigator)
				_, err := nav.ResolveRevision("head~500")
				return err == nil, "should fail under both"
			},
		},
		{
			// The one row that legitimately differs, and the reason is in
			// HistoryFeatureUnavailableError: a tag is a retention root,
			// and none reclaims on a window without consulting one.
			name:               "tag a commit",
			sameUnderBothModes: false,
			wantNoneOK:         false,
			run: func(t *testing.T, rt *embed.EmbeddedKdbRuntime) (bool, string) {
				if err := rt.AssertRetainsHistory("app/docs", "tagging a commit"); err != nil {
					return false, err.Error()
				}
				nav := rt.DAG.(dag.HistoryNavigator)
				head, _ := rt.DAG.Head()
				if _, err := nav.CreateTag("v1", head, ""); err != nil {
					return false, err.Error()
				}
				return true, "tagged"
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fullRoot, noneRoot := t.TempDir(), t.TempDir()

			rtFull := conformanceRuntime(t, fullRoot, storage.HistoryModeFull)
			seedHistory(t, rtFull)
			fullOK, fullDetail := tc.run(t, rtFull)
			rtFull.Close()

			rtNone := conformanceRuntime(t, noneRoot, storage.HistoryModeNone)
			seedHistory(t, rtNone)
			noneOK, noneDetail := tc.run(t, rtNone)
			rtNone.Close()

			if tc.sameUnderBothModes {
				if fullOK != noneOK {
					t.Fatalf("modes disagree: full ok=%v (%s), none ok=%v (%s)",
						fullOK, fullDetail, noneOK, noneDetail)
				}
				if fullOK && fullDetail != noneDetail {
					t.Fatalf("modes returned different results: full %q, none %q", fullDetail, noneDetail)
				}
				return
			}
			if !fullOK {
				t.Fatalf("history=full should support this: %s", fullDetail)
			}
			if noneOK != tc.wantNoneOK {
				t.Fatalf("history=none ok=%v, want %v (%s)", noneOK, tc.wantNoneOK, noneDetail)
			}
			if !noneOK && !strings.Contains(noneDetail, "history=none") {
				t.Fatalf("the refusal should name the mode so the reason is not a mystery: %q", noneDetail)
			}
		})
	}
}

// Outside the window, a none namespace refuses with an error that says why
// - and says something a longer window would fix, rather than a bare
// "not found".
func TestNoneModeExplainsReclaimedHistory(t *testing.T) {
	root := t.TempDir()
	window := storage.RetentionWindow{Duration: storage.RetainNothing}
	writeSessions(t, root, 4, storage.RetentionWindow{})

	rt := noneRuntime(t, root, window)
	nav := rt.DAG.(dag.HistoryNavigator)
	head, _ := rt.DAG.Head()
	old, err := nav.NthAncestor(head, 6)
	if err != nil {
		t.Skipf("history is shorter than expected: %v", err)
	}
	if _, err := rt.Maintain(); err != nil {
		t.Fatal(err)
	}
	rt.Close()

	rt = noneRuntime(t, root, window)
	defer rt.Close()
	_, err = embed.RevertTo(rt, "app/docs", old.Hex())
	if err == nil {
		t.Skip("the target survived truncation, so there is nothing to explain")
	}
	var notRetained *embed.HistoryNotRetainedError
	if !errors.As(err, &notRetained) {
		t.Fatalf("want HistoryNotRetainedError, got %v", err)
	}
	if !strings.Contains(err.Error(), "retention window") {
		t.Fatalf("the error should point at the window: %v", err)
	}
}
