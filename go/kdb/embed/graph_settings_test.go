package embed

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/schema"
)

func TestGraphSettingsFromEnv(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want dag.GraphSettings
	}{
		{
			// An operator who has not heard of any of this gets the mapped commit graph - the
			// default since it passed its gates - and nothing else.
			name: "nothing set maps the graph and turns nothing else on",
			env:  nil,
			want: dag.GraphSettings{FileEnabled: true},
		},
		{
			name: "the graph file can still be turned off",
			env:  map[string]string{"KDB_GRAPH_FILE": "off"},
			want: dag.GraphSettings{},
		},
		{
			name: "an unparseable graph file value is the default",
			env:  map[string]string{"KDB_GRAPH_FILE": "perhaps"},
			want: dag.GraphSettings{FileEnabled: true},
		},
		{
			name: "pruning on",
			env:  map[string]string{"KDB_ANCESTRY_PRUNING": "on"},
			want: dag.GraphSettings{AncestryPruning: true, FileEnabled: true},
		},
		{
			name: "the other spellings of on",
			env:  map[string]string{"KDB_ANCESTRY_PRUNING": " TRUE "},
			want: dag.GraphSettings{AncestryPruning: true, FileEnabled: true},
		},
		{
			name: "off is off",
			env:  map[string]string{"KDB_ANCESTRY_PRUNING": "off"},
			want: dag.GraphSettings{FileEnabled: true},
		},
		{
			// Same reading as envBytes and KDB_HISTORY_STRATEGY: this
			// function reports no error, so a value nobody can parse has to
			// mean "leave it alone" rather than "guess".
			name: "an unparseable value leaves it off",
			env:  map[string]string{"KDB_ANCESTRY_PRUNING": "maybe"},
			want: dag.GraphSettings{FileEnabled: true},
		},
		{
			name: "the file flag, with its counts",
			env: map[string]string{
				"KDB_GRAPH_FILE":              "1",
				"KDB_GRAPH_REBUILD_COMMITS":   "1000",
				"KDB_HISTORY_ANCHOR_INTERVAL": "250",
			},
			want: dag.GraphSettings{FileEnabled: true, RebuildCommits: 1000, AnchorInterval: 250},
		},
		{
			name: "a negative count is no count",
			env:  map[string]string{"KDB_HISTORY_ANCHOR_INTERVAL": "-5"},
			want: dag.GraphSettings{FileEnabled: true},
		},
		{
			name: "go memory",
			env:  map[string]string{"KDB_GRAPH_MEMORY": "Go"},
			want: dag.GraphSettings{Memory: dag.GraphMemoryGo, FileEnabled: true},
		},
		{
			name: "an unknown memory mode is the default",
			env:  map[string]string{"KDB_GRAPH_MEMORY": "jvm"},
			want: dag.GraphSettings{FileEnabled: true},
		},
		{
			name: "a non-numeric count is no count",
			env:  map[string]string{"KDB_GRAPH_REBUILD_COMMITS": "lots"},
			want: dag.GraphSettings{FileEnabled: true},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear every variable first: a case names only what it sets, and anything the
			// surrounding environment exports (KDB_GRAPH_FILE=on for a whole-suite run, say)
			// would otherwise leak into a case that asserts it is off.
			for _, k := range []string{"KDB_ANCESTRY_PRUNING", "KDB_GRAPH_FILE", "KDB_GRAPH_REBUILD_COMMITS", "KDB_GRAPH_MAX_SEGMENTS", "KDB_GRAPH_MEMORY", "KDB_HISTORY_ANCHOR_INTERVAL"} {
				t.Setenv(k, "")
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			if got := graphSettingsFromEnv(); got != tc.want {
				t.Errorf("graphSettingsFromEnv() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestGraphSettingsReachTheDag is the plumbing test: a setting a caller
// puts on StorageOptions has to arrive at the DAG that actually reads it,
// normalized, and it has to arrive before the namespace is restored - see
// the comment at the SetGraphSettings call in file.go.
func TestGraphSettingsReachTheDag(t *testing.T) {
	root := t.TempDir()
	opts := FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 1 << 20
	opts.Storage.Graph = dag.GraphSettings{FileEnabled: true}

	rt, err := OpenFileRuntimeWithOptions(root, "test", "test/graph", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()

	p, ok := rt.DAG.(*PersistingCommitDAG)
	if !ok {
		t.Fatalf("expected a persisting DAG, got %T", rt.DAG)
	}
	got := p.delegate.GraphSettings()
	want := dag.GraphSettings{
		FileEnabled:     true,
		AncestryPruning: true,
		RebuildCommits:  dag.DefaultGraphRebuildCommits,
		MaxSegments:     dag.DefaultGraphMaxSegments,
		Memory:          dag.GraphMemoryKDB,
	}
	if got != want {
		t.Errorf("DAG settings = %+v, want %+v", got, want)
	}
	if p.delegate.GraphFileActive() {
		t.Error("a fresh namespace below the rebuild threshold should not have a graph file yet")
	}
}

// TestDefaultOpenTurnsNothingOn is the other half: a caller that says
// nothing gets the behaviour that predates all of this.
func TestDefaultOpenTurnsNothingOn(t *testing.T) {
	root := t.TempDir()
	opts := FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 1 << 20

	rt, err := OpenFileRuntimeWithOptions(root, "test", "test/graph", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()

	p, ok := rt.DAG.(*PersistingCommitDAG)
	if !ok {
		t.Fatalf("expected a persisting DAG, got %T", rt.DAG)
	}
	// Nothing optional is on; the only value filled in is which memory representation the
	// DAG uses, and that is the default one.
	if got := p.delegate.GraphSettings(); got != (dag.GraphSettings{Memory: dag.GraphMemoryKDB}) {
		t.Errorf("DAG settings = %+v, want nothing on and the default memory mode", got)
	}
}
