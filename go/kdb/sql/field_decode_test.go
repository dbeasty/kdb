package sql

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdbjson "github.com/limidus/kdb/go/kdb/json"
	"github.com/limidus/kdb/go/kdb/schema"
)

// treeCandidates is columnValues as it was before field-at-a-time decoding: decode the whole
// document, then walk the path. The oracle for TestFieldDecodingMatchesWholeDocument.
func treeCandidates(doc string, name string, expand bool) []kdbjson.Value {
	d := newEvalDoc(document.Document{JSON: doc})
	root := d.tree()
	if root == nil {
		return nil
	}
	return kdbjson.CandidatesOf(root, kdbjson.SplitDotted(name), expand)
}

func renderValues(vs []kdbjson.Value) string {
	out := ""
	for _, v := range vs {
		out += fmt.Sprintf("%#v|", jsonValueToCell(v, nil))
	}
	return out
}

func TestFieldDecodingMatchesWholeDocument(t *testing.T) {
	docs := []string{
		`{"a":1,"b":{"c":[1,2,{"d":"x"}]},"e":[{"f":1},{"f":[2,3]}],"emb":[0.1,-0.2,3e-5]}`,
		`{"a":1,"a":2}`,
		`{"kéy":"v","k\"q":true,"esc":"a\"b\\"}`,
		` { "sp" : [ 1 , 2 ] , "n" : null } `,
		`{}`,
		`[{"a":1}]`,
		`{"a":}`,
		`{"a":1`,
		`not json`,
		`{"nested":{"a":{"b":{"c":"deep"}}},"t":true,"f":false,"big":12345678901234567890}`,
	}
	paths := []string{"a", "b", "b.c", "b.c.d", "e.f", "emb", "missing", "kéy", `k"q`, "esc", "sp", "n",
		"nested.a.b.c", "nested.a", "t", "f", "big", "a.b"}
	id, _ := codec.RandomUUID()
	env := newEvalEnv(schema.None(), nil, TableRef{})
	for _, doc := range docs {
		for _, p := range paths {
			for _, expand := range []bool{false, true} {
				d := newEvalDoc(document.Document{ID: id, JSON: doc})
				got := renderValues(env.columnValues(p, d, expand))
				want := renderValues(treeCandidates(doc, p, expand))
				if got != want {
					t.Errorf("doc %s path %s expand %v: field decoding %s, whole document %s", doc, p, expand, got, want)
				}
			}
		}
	}
}
