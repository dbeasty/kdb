package json

import "testing"

const candidateDoc = `{
  "title": "alpha",
  "tags": ["x", "y"],
  "collaborators": [{"userId": "u1", "role": "owner"}, {"userId": "u2"}],
  "steps": [{"text": "plan"}, {"text": "deploy"}, {"other": 1}],
  "matrix": [[1, 2], [3]],
  "meta": {"reviewed": true, "count": null},
  "n": 7
}`

func jsonStrings(vals []Value) []string {
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		out = append(out, ToJSONString(v))
	}
	return out
}

func assertCandidates(t *testing.T, dotted string, expand bool, want ...string) {
	t.Helper()
	var got []Value
	var err error
	if expand {
		got, err = Candidates(candidateDoc, dotted)
	} else {
		got, err = PathValues(candidateDoc, dotted)
	}
	if err != nil {
		t.Fatalf("%s: %v", dotted, err)
	}
	gs := jsonStrings(got)
	if len(gs) != len(want) {
		t.Fatalf("%s (expand=%v): got %v, want %v", dotted, expand, gs, want)
	}
	for i := range want {
		if gs[i] != want[i] {
			t.Fatalf("%s (expand=%v): got %v, want %v", dotted, expand, gs, want)
		}
	}
}

func TestCandidatesScalarPath(t *testing.T) {
	assertCandidates(t, "title", true, `"alpha"`)
	assertCandidates(t, "n", true, `7`)
	assertCandidates(t, "meta.reviewed", true, `true`)
	assertCandidates(t, "meta.count", true, `null`)
}

func TestCandidatesTerminalArrayExpands(t *testing.T) {
	assertCandidates(t, "tags", true, `"x"`, `"y"`)
	// PathValues keeps the array whole.
	assertCandidates(t, "tags", false, `["x","y"]`)
}

func TestCandidatesImplicitTraversalThroughArrays(t *testing.T) {
	assertCandidates(t, "collaborators.userId", true, `"u1"`, `"u2"`)
	assertCandidates(t, "collaborators.role", true, `"owner"`)
	assertCandidates(t, "steps.text", true, `"plan"`, `"deploy"`)
	// Nested arrays flatten in document order.
	assertCandidates(t, "matrix", true, `[1,2]`, `[3]`)
}

func TestCandidatesAbsentPathIsEmpty(t *testing.T) {
	assertCandidates(t, "nope", true)
	assertCandidates(t, "title.deeper", true)
	assertCandidates(t, "meta.missing", true)
	assertCandidates(t, "steps.text.x", true)
	assertCandidates(t, "", true, candidateRootJSON())
}

func candidateRootJSON() string {
	v, _ := ParseValue(candidateDoc)
	return ToJSONString(v)
}

func TestCandidatesNeverPanicOnTypeMismatch(t *testing.T) {
	for _, doc := range []string{`[1,2,3]`, `"str"`, `42`, `null`, `true`, `{}`, `{"a":[]}`, `{"a":[[]]}`} {
		for _, path := range []string{"a", "a.b", "a.b.c", "", ".", "a..b"} {
			if _, err := Candidates(doc, path); err != nil {
				t.Fatalf("%s / %q: unexpected error %v", doc, path, err)
			}
		}
	}
}

func TestCandidatesMalformedJSONIsAnError(t *testing.T) {
	if _, err := Candidates(`{"a":`, "a"); err == nil {
		t.Fatal("expected a parse error")
	}
}

func TestDeepEqualNumericAndStructural(t *testing.T) {
	if !DeepEqual(IntValue{V: 1}, NumberValue{V: 1.0}) {
		t.Fatal("1 must equal 1.0")
	}
	if DeepEqual(StringValue{V: "1"}, IntValue{V: 1}) {
		t.Fatal("\"1\" must not equal 1")
	}
	a, _ := ParseValue(`{"x":[1,{"y":2}]}`)
	b, _ := ParseValue(`{"x":[1.0,{"y":2}]}`)
	if !DeepEqual(a, b) {
		t.Fatal("structurally equal objects must be DeepEqual")
	}
	if DeepEqual(nil, a) || !DeepEqual(nil, nil) {
		t.Fatal("nil handling")
	}
}
