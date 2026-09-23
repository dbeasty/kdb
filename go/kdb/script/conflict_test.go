package script

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func ptr(s string) *string { return &s }

func TestParseDecisionAcceptsEveryForm(t *testing.T) {
	cases := []struct {
		text string
		want Decision
	}{
		{`{"take":"local"}`, Decision{Kind: DecideTake, Side: 0}},
		{`{"take":"remote"}`, Decision{Kind: DecideTake, Side: 1}},
		{`{"take":"side0"}`, Decision{Kind: DecideTake, Side: 0}},
		{`{"take":"side1"}`, Decision{Kind: DecideTake, Side: 1}},
		{`{"fork":{"keep":"remote"}}`, Decision{Kind: DecideFork, Side: 1}},
		{`{"delete":true}`, Decision{Kind: DecideDelete}},
		{`{"defer":true}`, Decision{Kind: DecideDefer}},
		{`{"doc":{"b":2,"a":1}}`, Decision{Kind: DecideDoc, Doc: `{"b":2,"a":1}`}},
	}
	for _, c := range cases {
		got, err := ParseDecision(c.text)
		if err != nil {
			t.Fatalf("%s: %v", c.text, err)
		}
		if got != c.want {
			t.Fatalf("%s: got %+v want %+v", c.text, got, c.want)
		}
	}
}

func TestParseDecisionRejectsAmbiguity(t *testing.T) {
	// Anything a caller would have to guess at is refused: two nodes must not be able to read one
	// result two ways.
	for _, text := range []string{
		`{}`,
		`{"take":"local","delete":true}`,
		`{"take":"either"}`,
		`{"doc":[1,2]}`,
		`{"doc":"a string"}`,
		`{"delete":false}`,
		`{"defer":false}`,
		`{"fork":{"keep":"both"}}`,
		`{"keep":"local"}`,
		`"local"`,
		`null`,
	} {
		if _, err := ParseDecision(text); !errors.Is(err, ErrOutput) {
			t.Fatalf("%s was accepted: %v", text, err)
		}
	}
}

func TestConflictArgsShape(t *testing.T) {
	args, err := ConflictArgs(ConflictInput{
		DocID:        "doc-1",
		Base:         ptr(`{"a":1,"b":1}`),
		Local:        ptr(`{"a":2,"b":1}`),
		Remote:       ptr(`{"a":1,"b":3}`),
		LocalOrigin:  Origin{NodeID: "n1", Commit: "c1", TimestampMicros: 10},
		RemoteOrigin: Origin{NodeID: "n2", Commit: "c2", TimestampMicros: 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(args), &got); err != nil {
		t.Fatal(err)
	}
	fields, _ := got["fields"].([]any)
	if len(fields) != 2 {
		t.Fatalf("expected two changed fields, got %s", args)
	}
	// side0/side1 are aliases, so a procedure need never ask which node it is running on.
	if string(mustJSON(t, got["local"])) != string(mustJSON(t, got["side0"])) {
		t.Fatalf("side0 should alias local: %s", args)
	}
	if !strings.Contains(args, `"timestampMicros":20`) {
		t.Fatalf("origins are missing: %s", args)
	}
}

func TestConflictArgsMarksADeletedSideAbsent(t *testing.T) {
	args, err := ConflictArgs(ConflictInput{DocID: "d", Base: ptr(`{"a":1}`), Local: nil, Remote: ptr(`{"a":2}`)})
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Local   json.RawMessage `json:"local"`
		Present map[string]bool `json:"present"`
	}
	if err := json.Unmarshal([]byte(args), &got); err != nil {
		t.Fatal(err)
	}
	if got.Present["local"] {
		t.Fatalf("a deleted side is absent: %s", args)
	}
	if string(got.Local) != "null" {
		t.Fatalf("an absent side reads as null: %s", args)
	}
}

func TestConflictArgsRejectsUnparseableBody(t *testing.T) {
	_, err := ConflictArgs(ConflictInput{DocID: "d", Local: ptr(`{not json`), Remote: ptr(`{}`)})
	if !errors.Is(err, ErrOutput) {
		t.Fatalf("got %v", err)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// fakeHost is a Host that serves a fixed document set.
type fakeHost struct {
	docs  map[string]string
	calls int
}

func (h *fakeHost) Get(id string) (string, error) {
	h.calls++
	return h.docs[id], nil
}

func (h *fakeHost) Query(string, []any) (string, error) {
	h.calls++
	return `[{"n":1}]`, nil
}

func TestReadModeCanReadTheDatabase(t *testing.T) {
	r := New(Limits{})
	host := &fakeHost{docs: map[string]string{"policy": `{"prefer":"remote"}`}}
	src := `function main(c) { var p = kdb.get("policy"); return { take: p.prefer }; }`
	got, err := r.ResolveConflict(context.Background(), src, ModeRead, host,
		ConflictInput{DocID: "d", Base: ptr(`{"a":1}`), Local: ptr(`{"a":2}`), Remote: ptr(`{"a":3}`)})
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != DecideTake || got.Side != 1 {
		t.Fatalf("got %+v", got)
	}
}

func TestReadModeCannotWrite(t *testing.T) {
	r := New(Limits{})
	src := `function main() { kdb.put("x", {}); return { defer: true }; }`
	_, err := r.Call(context.Background(), src, ModeRead, &fakeHost{}, `{}`)
	if err == nil || !strings.Contains(err.Error(), "kdb.put is not available") {
		t.Fatalf("got %v", err)
	}
}

func TestReadModeHostCallsAreCapped(t *testing.T) {
	r := New(Limits{MaxHostCalls: 3})
	src := `function main() { for (var i = 0; i < 10; i++) { kdb.get("x"); } return { defer: true }; }`
	_, err := r.Call(context.Background(), src, ModeRead, &fakeHost{docs: map[string]string{}}, `{}`)
	if err == nil || !strings.Contains(err.Error(), "host-call limit") {
		t.Fatalf("got %v", err)
	}
}

func TestReadModeLogsAreCollectedAndCapped(t *testing.T) {
	r := New(Limits{MaxLogBytes: 10})
	src := `function main() { kdb.log("hello"); kdb.log("world again"); return { defer: true }; }`
	got, err := r.Call(context.Background(), src, ModeRead, &fakeHost{}, `{}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Logs) != 2 || got.Logs[0] != "hello" || got.Logs[1] != "world" {
		t.Fatalf("got %q", got.Logs)
	}
}

func TestPureModeHasNoHostAtAll(t *testing.T) {
	r := New(Limits{})
	_, err := r.Call(context.Background(), `function main() { kdb.get("x"); return {}; }`, ModePure, nil, `{}`)
	if err == nil || !strings.Contains(err.Error(), "kdb is not defined") {
		t.Fatalf("got %v", err)
	}
}

func TestReadModeNeedsAHost(t *testing.T) {
	r := New(Limits{})
	if _, err := r.Call(context.Background(), `function main() { return {}; }`, ModeRead, nil, `{}`); !errors.Is(err, ErrDenied) {
		t.Fatalf("got %v", err)
	}
}

func TestSourceHashChangesWithSource(t *testing.T) {
	if SourceHash("function main() {}") == SourceHash("function main() { }") {
		t.Fatal("whitespace changes the source, so it changes the hash")
	}
}
