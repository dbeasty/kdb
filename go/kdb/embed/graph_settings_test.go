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
			// The property that matters most: an operator who has not heard
			// of any of this gets exactly the behaviour they had.
			name: "nothing set turns nothing on",
			env:  nil,
			want: dag.GraphSettings{},
		},
		{
			name: "pruning on",
			env:  map[string]string{"KDB_ANCESTRY_PRUNING": "on"},
			want: dag.GraphSettings{AncestryPruning: true},
		},
		{
			name: "the other spellings of on",
			env:  map[string]string{"KDB_ANCESTRY_PRUNING": " TRUE "},
			want: dag.GraphSettings{AncestryPruning: true},
		},
		{
			name: "off is off",
			env:  map[string]string{"KDB_ANCESTRY_PRUNING": "off"},
			want: dag.GraphSettings{},
		},
		{
			// Same reading as envBytes and KDB_HISTORY_STRATEGY: this
			// function reports no error, so a value nobody can parse has to
			// mean "leave it alone" rather than "guess".
			name: "an unparseable value leaves it off",
			env:  map[string]string{"KDB_ANCESTRY_PRUNING": "maybe"},
			want: dag.GraphSettings{},
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
			want: dag.GraphSettings{},
		},
		{
			name: "a non-numeric count is no count",
			env:  map[string]string{"KDB_GRAPH_REBUILD_COMMITS": "lots"},
			want: dag.GraphSettings{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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
	}
	if got != want {
		t.Errorf("DAG settings = %+v, want %+v", got, want)
	}
	if p.delegate.GraphFileActive() {
		t.Error("a graph file is reported active, but none is implemented")
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
	if got := p.delegate.GraphSettings(); got != (dag.GraphSettings{}) {
		t.Errorf("DAG settings = %+v, want the zero value", got)
	}
}
