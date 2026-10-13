package schema_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/schema"
)

// fieldViaRaw decodes what FieldRaw found, to compare with FieldValue's decoded answer.
func fieldViaRaw(t *testing.T, doc, path string) any {
	raw, ok := schema.FieldRaw(doc, path)
	if !ok {
		return nil
	}
	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		t.Fatalf("FieldRaw(%q, %q) returned undecodable %q: %v", doc, path, raw, err)
	}
	return v
}

func TestFieldRawAgreesWithFieldValue(t *testing.T) {
	docs := []string{
		`{"a":1,"b":"two","c":{"d":[1,{"e":2}],"f":null},"g":true}`,
		`  {"a" : { "b" :{"c":"deep"}} , "a2":[] }`,
		`{"k":1,"k":{"x":"last wins"}}`,
		`{"q\"uote":1,"tricky":"}\"{[","after":3}`,
		`{"a":"escaped key","ü":"unicode"}`,
		`{}`, `[]`, `"s"`, `12`,
	}
	paths := []string{"a", "b", "c", "c.d", "c.f", "c.d.e", "g", "a.b", "a.b.c", "a2", "k", "k.x", `q"uote`, "tricky", "after", "ü", "missing", ""}
	for _, d := range docs {
		for _, p := range paths {
			want, err := schema.FieldValue(d, p)
			if err != nil {
				want = nil
			}
			if got := fieldViaRaw(t, d, p); !reflect.DeepEqual(got, want) {
				t.Errorf("%s @ %q: FieldRaw gave %#v, FieldValue %#v", d, p, got, want)
			}
		}
	}
}

func FuzzFieldRawAgreesWithFieldValue(f *testing.F) {
	f.Add(`{"a":{"b":[1,"x"]},"c":"\"}"}`, "a.b")
	f.Add(`{"k":1,"k":2}`, "k")
	f.Fuzz(func(t *testing.T, doc, path string) {
		if !json.Valid([]byte(doc)) || !strings.HasPrefix(strings.TrimSpace(doc), "{") {
			return
		}
		want, err := schema.FieldValue(doc, path)
		if err != nil {
			// encoding/json refuses the whole document (a number out of float64 range, say);
			// FieldRaw only reads the path, so there is nothing to compare.
			return
		}
		if got := fieldViaRaw(t, doc, path); !reflect.DeepEqual(got, want) {
			t.Fatalf("%q @ %q: FieldRaw gave %#v, FieldValue %#v", doc, path, got, want)
		}
	})
}
