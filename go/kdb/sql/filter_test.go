package sql

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
)

func TestParseFilterEvaluatesAgainstDocuments(t *testing.T) {
	expr, err := ParseFilter(`region = 'EU' AND qty > 2`)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := codec.RandomUUID()
	for body, want := range map[string]bool{
		`{"region":"EU","qty":3}`: true,
		`{"region":"EU","qty":1}`: false,
		`{"region":"US","qty":9}`: false,
		`{"qty":9}`:               false,
	} {
		if got := EvalPredicate(expr, document.Document{ID: id, JSON: body}, schema.None(), nil); got != want {
			t.Errorf("%s: got %v want %v", body, got, want)
		}
	}
	if _, err := ParseFilter(`region = `); err == nil {
		t.Error("an incomplete expression parsed")
	}
}
