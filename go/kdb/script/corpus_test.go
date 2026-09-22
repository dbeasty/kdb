package script

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	kdbjson "github.com/limidus/kdb/go/kdb/json"
)

// corpusCase is one file of testdata/conflict_corpus. Kotlin's kdb-script runs the same files;
// see that directory's README.
type corpusCase struct {
	Name    string `json:"name"`
	Comment string `json:"comment"`
	Source  string `json:"source"`
	Input   struct {
		DocID   string          `json:"docId"`
		Base    json.RawMessage `json:"base"`
		Local   json.RawMessage `json:"local"`
		Remote  json.RawMessage `json:"remote"`
		Origins struct {
			Local  Origin `json:"local"`
			Remote Origin `json:"remote"`
		} `json:"origins"`
	} `json:"input"`
	Expected *struct {
		Kind string          `json:"kind"`
		Side int             `json:"side"`
		Doc  json.RawMessage `json:"doc"`
	} `json:"expected"`
	ErrorContains string `json:"errorContains"`
}

func body(raw json.RawMessage) *string {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	s := string(raw)
	return &s
}

func TestConflictCorpus(t *testing.T) {
	files, err := filepath.Glob("testdata/conflict_corpus/*.json")
	if err != nil || len(files) == 0 {
		t.Fatalf("no corpus files: %v", err)
	}
	r := New(Limits{})
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var c corpusCase
		if err := json.Unmarshal(raw, &c); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		t.Run(c.Name, func(t *testing.T) {
			in := ConflictInput{
				DocID:        c.Input.DocID,
				Base:         body(c.Input.Base),
				Local:        body(c.Input.Local),
				Remote:       body(c.Input.Remote),
				LocalOrigin:  c.Input.Origins.Local,
				RemoteOrigin: c.Input.Origins.Remote,
			}
			got, err := r.ResolveConflict(context.Background(), c.Source, ModePure, nil, in)
			if c.ErrorContains != "" {
				if err == nil {
					t.Fatalf("expected a failure containing %q, got %+v", c.ErrorContains, got)
				}
				if !strings.Contains(err.Error(), c.ErrorContains) {
					t.Fatalf("error %v does not mention %q", err, c.ErrorContains)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := Decision{Kind: kindByName(t, c.Expected.Kind), Side: c.Expected.Side}
			if len(c.Expected.Doc) > 0 {
				v, err := kdbjson.ParseValue(string(c.Expected.Doc))
				if err != nil {
					t.Fatal(err)
				}
				want.Doc = kdbjson.ToJSONString(v)
			}
			if got.Kind != want.Kind || got.Side != want.Side {
				t.Fatalf("got %+v want %+v", got, want)
			}
			if want.Doc != "" && kdbjson.CanonicalText(mustValue(t, got.Doc)) != kdbjson.CanonicalText(mustValue(t, want.Doc)) {
				t.Fatalf("got doc %s want %s", got.Doc, want.Doc)
			}
		})
	}
}

func mustValue(t *testing.T, s string) kdbjson.Value {
	t.Helper()
	v, err := kdbjson.ParseValue(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return v
}

func kindByName(t *testing.T, name string) DecisionKind {
	t.Helper()
	switch name {
	case "take":
		return DecideTake
	case "doc":
		return DecideDoc
	case "fork":
		return DecideFork
	case "delete":
		return DecideDelete
	case "defer":
		return DecideDefer
	default:
		t.Fatalf("unknown expected kind %q", name)
		return DecideDefer
	}
}
