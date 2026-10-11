package json

import (
	"errors"
	"testing"

	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// TestGettersReturnPathErrorsInsteadOfPanicking pins the contract that every exported accessor
// reports a path that does not fit the document (a field step into a non-object, an index step
// into a non-array, an index below -1) as a returned *JsonPathError. These used to panic from
// inside navigateGet/collectAll/normalizeIndex although every signature returns error, which
// left the first caller without a recover() to take the process down.
func TestGettersReturnPathErrorsInsteadOfPanicking(t *testing.T) {
	const doc = `{"s":"str","n":1,"o":{"k":true},"a":[1,[2,3],{"x":4}]}`

	type call struct {
		name string
		run  func(p *Path) error
	}
	calls := []call{
		{"Get", func(p *Path) error { _, err := Get(doc, p); return err }},
		{"GetAll", func(p *Path) error { _, err := GetAll(doc, p); return err }},
		{"Contains", func(p *Path) error { _, err := Contains(doc, p, IntValue{V: 1}); return err }},
		{"Keys", func(p *Path) error { _, err := Keys(doc, p); return err }},
		{"TypeName", func(p *Path) error { _, err := TypeName(doc, p); return err }},
		{"ArrayLength", func(p *Path) error { _, err := ArrayLength(doc, p); return err }},
		{"Delete", func(p *Path) error { _, err := Delete(doc, p); return err }},
		{"Set", func(p *Path) error { _, err := Set(doc, p, IntValue{V: 9}); return err }},
	}
	paths := []struct {
		expr string
		why  string
	}{
		{"$.s.x", "field step into a string"},
		{"$.n.x", "field step into a number"},
		{"$.a.x", "field step into an array"},
		{"$.o[0]", "index step into an object"},
		{"$.s[0]", "index step into a string"},
		{"$.a[1].x", "field step into a nested array"},
		{"$.a[2][0]", "index step into a nested object"},
		{"$.a[-2]", "index below -1"},
		{"$.o.k.deeper", "field step into a boolean"},
	}
	for _, pc := range paths {
		p, err := CompilePath(pc.expr)
		if err != nil {
			t.Fatalf("compile %s: %v", pc.expr, err)
		}
		for _, c := range calls {
			t.Run(c.name+"/"+pc.expr, func(t *testing.T) {
				var got error
				func() {
					defer func() {
						if r := recover(); r != nil {
							t.Fatalf("%s(%s) [%s] panicked: %v", c.name, pc.expr, pc.why, r)
						}
					}()
					got = c.run(p)
				}()
				if got == nil {
					t.Fatalf("%s(%s) [%s]: want error, got nil", c.name, pc.expr, pc.why)
				}
				var pe *kdberr.JsonPathError
				if !errors.As(got, &pe) {
					t.Fatalf("%s(%s): want *JsonPathError, got %T: %v", c.name, pc.expr, got, got)
				}
			})
		}
	}
}

// TestGetAllWildcardMismatchReturnsError covers the wildcard branch of GetAll (collectAll),
// which navigated separately from navigateGet and panicked the same way.
func TestGetAllWildcardMismatchReturnsError(t *testing.T) {
	const doc = `{"a":[{"b":1},2],"o":{"x":"s"}}`
	for _, expr := range []string{"$.a[*].b", "$.o.*[0]", "$.o[*]", "$.a.*", "$.a[*][-3]"} {
		p, err := CompilePath(expr)
		if err != nil {
			t.Fatalf("compile %s: %v", expr, err)
		}
		t.Run(expr, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("GetAll(%s) panicked: %v", expr, r)
				}
			}()
			if _, err := GetAll(doc, p); err == nil {
				t.Fatalf("GetAll(%s): want error, got nil", expr)
			}
		})
	}
}

// TestSetLastIndexOnEmptyArrayReturnsError: [-1] on an empty array resolves to no element, and
// Set used to index list[-1] - a runtime panic rather than even a JsonPathError.
func TestSetLastIndexOnEmptyArrayReturnsError(t *testing.T) {
	p, err := CompilePath("$.a[-1]")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Set panicked: %v", r)
		}
	}()
	if _, err := Set(`{"a":[]}`, p, IntValue{V: 1}); err == nil {
		t.Fatal("want error, got nil")
	}
	// The non-empty case still means "the last element".
	out, err := Set(`{"a":[1,2]}`, p, IntValue{V: 7})
	if err != nil {
		t.Fatal(err)
	}
	if out != `{"a":[1,7]}` {
		t.Fatalf("got %s", out)
	}
}

// TestGettersStillReturnNilForMissing guards the other side: an absent field or out-of-range
// index is not a type mismatch and must stay a nil result with no error.
func TestGettersStillReturnNilForMissing(t *testing.T) {
	const doc = `{"o":{"k":1},"a":[1]}`
	for _, expr := range []string{"$.missing", "$.o.missing", "$.a[5]", "$.missing.deeper", "$.a[5].x"} {
		v, err := GetString(doc, expr)
		if err != nil || v != nil {
			t.Fatalf("GetString(%s) = %v, %v; want nil, nil", expr, v, err)
		}
	}
}
