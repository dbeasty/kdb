package json

import (
	"strings"
	"testing"
)

func mustParse(t *testing.T, s string) Value {
	t.Helper()
	if s == "" {
		return nil
	}
	v, err := ParseValue(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return v
}

// summary renders a diff as "path:kind" pairs, in order.
func summary(t *testing.T, base, local, remote string) string {
	t.Helper()
	changes := DiffConflict(mustParse(t, base), mustParse(t, local), mustParse(t, remote))
	parts := make([]string, len(changes))
	for i, c := range changes {
		parts[i] = c.Path + ":" + c.Kind
	}
	return strings.Join(parts, " ")
}

func TestDiffClassifiesEachSide(t *testing.T) {
	got := summary(t,
		`{"a":1,"b":2,"c":3,"d":4}`,
		`{"a":9,"b":2,"c":8,"d":7}`,
		`{"a":1,"b":5,"c":8,"d":6}`)
	want := "/a:local /b:remote /c:same /d:both"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestDiffOmitsUnchangedFields(t *testing.T) {
	if got := summary(t, `{"a":1,"b":2}`, `{"a":1,"b":3}`, `{"a":1,"b":2}`); got != "/b:local" {
		t.Fatalf("got %q", got)
	}
}

func TestDiffRecursesIntoObjectsOnly(t *testing.T) {
	// Nested objects are walked; arrays are one value however they differ.
	got := summary(t,
		`{"o":{"x":1,"y":1},"arr":[1,2,3]}`,
		`{"o":{"x":2,"y":1},"arr":[1,2,3,4]}`,
		`{"o":{"x":1,"y":3},"arr":[1,2,3]}`)
	want := "/arr:local /o/x:local /o/y:remote"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestDiffStopsRecursingWhenASideIsNotAnObject(t *testing.T) {
	if got := summary(t, `{"o":{"x":1}}`, `{"o":{"x":2}}`, `{"o":"scalar"}`); got != "/o:both" {
		t.Fatalf("got %q", got)
	}
}

func TestDiffSeparatesAbsentFromNull(t *testing.T) {
	changes := DiffConflict(mustParse(t, `{"a":1}`), mustParse(t, `{"a":null}`), mustParse(t, `{}`))
	if len(changes) != 1 || changes[0].Kind != ChangeBoth {
		t.Fatalf("got %+v", changes)
	}
	c := changes[0]
	if !c.LocalPresent {
		t.Fatal("local set the field to null, so it is present")
	}
	if _, ok := c.Local.(NullValue); !ok {
		t.Fatalf("local should be null, got %T", c.Local)
	}
	if c.RemotePresent {
		t.Fatal("remote deleted the field, so it is absent")
	}
}

func TestDiffWholeDocumentDelete(t *testing.T) {
	changes := DiffConflict(mustParse(t, `{"a":1}`), nil, mustParse(t, `{"a":2}`))
	if len(changes) != 1 {
		t.Fatalf("a deleted side is one change at the root, got %+v", changes)
	}
	if changes[0].Path != "" || changes[0].Kind != ChangeBoth || changes[0].LocalPresent {
		t.Fatalf("got %+v", changes[0])
	}
}

func TestDiffIgnoresFieldOrder(t *testing.T) {
	if got := summary(t, `{"a":1,"b":2}`, `{"b":2,"a":1}`, `{"a":1,"b":2}`); got != "" {
		t.Fatalf("field order is not a change, got %q", got)
	}
}

func TestDiffPathEscaping(t *testing.T) {
	if got := summary(t, `{"a/b":1,"c~d":1}`, `{"a/b":2,"c~d":2}`, `{"a/b":1,"c~d":1}`); got != "/a~1b:local /c~0d:local" {
		t.Fatalf("got %q", got)
	}
}

func TestDiffOrderIsByPathBytes(t *testing.T) {
	// "é" (U+00E9, 0xC3 0xA9) sorts after "z" by UTF-8 bytes. A UTF-16 comparison agrees here;
	// the point is that the order does not depend on the input's field order.
	got := summary(t, `{"z":1,"é":1,"a":1}`, `{"z":2,"é":2,"a":2}`, `{"z":1,"é":1,"a":1}`)
	if got != "/a:local /z:local /é:local" {
		t.Fatalf("got %q", got)
	}
}

func TestCanonicalTextSortsKeys(t *testing.T) {
	a := CanonicalText(mustParse(t, `{"b":{"d":1,"c":2},"a":3}`))
	b := CanonicalText(mustParse(t, `{"a":3,"b":{"c":2,"d":1}}`))
	if a != b {
		t.Fatalf("%q != %q", a, b)
	}
	if a != `{"a":3,"b":{"c":2,"d":1}}` {
		t.Fatalf("got %q", a)
	}
}
