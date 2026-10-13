package server

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/schema"
)

// legacyExpiryTimestamp is the decode-everything implementation expiryTimestamp replaced: the
// oracle the field-scanning version must agree with on every well-formed document.
func legacyExpiryTimestamp(jsonBody, fieldPath string) (time.Time, bool) {
	raw, err := schema.FieldValue(jsonBody, fieldPath)
	if err != nil || raw == nil {
		return time.Time{}, false
	}
	switch v := raw.(type) {
	case string:
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			t, err = time.Parse(time.RFC3339, v)
		}
		if err != nil {
			return time.Time{}, false
		}
		return t, true
	case float64:
		return time.UnixMilli(int64(v)), true
	default:
		return time.Time{}, false
	}
}

var expiryCases = []struct{ body, path string }{
	{`{"expiresAt":"2026-01-02T03:04:05Z"}`, "expiresAt"},
	{`{"expiresAt":"2026-01-02T03:04:05.123456789+02:00","x":1}`, "expiresAt"},
	{`{"expiresAt":1767225600000}`, "expiresAt"},
	{`{"expiresAt":-5.5e3}`, "expiresAt"},
	{`{"expiresAt":1e400}`, "expiresAt"},
	{`{"expiresAt":null}`, "expiresAt"},
	{`{"expiresAt":true}`, "expiresAt"},
	{`{"expiresAt":{"at":1}}`, "expiresAt"},
	{`{"expiresAt":[1]}`, "expiresAt"},
	{`{"expiresAt":"not a time"}`, "expiresAt"},
	{`{"other":"2026-01-02T03:04:05Z"}`, "expiresAt"},
	{`{}`, "expiresAt"},
	{`[1,2]`, "expiresAt"},
	{`"str"`, "expiresAt"},
	{` { "a" : { "b" : { "c" : 1700000000000 } } } `, "a.b.c"},
	{`{"a":{"b":1},"a":{"c":2}}`, "a.b"},
	{`{"a":{"b":1},"a":{"c":2}}`, "a.c"},
	{`{"t":1,"t":"2026-01-02T03:04:05Z"}`, "t"},
	{`{"t":"2026-01-02T03:04:05Z","t":1}`, "t"},
	{`{"skip":{"x":"}{][\"","y":[{"t":2}]},"t":"2026-05-06T07:08:09Z"}`, "t"},
	{`{"escaped":3}`, "escaped"},
	{`{"ünï":4}`, "ünï"},
	{`{"t":"2026-01-02T03:04:05Z"}`, "t"},
	{`{"t":"2026-01-02T03:04:05Z\n"}`, "t"},
	{`{"a":1}`, "a.b"},
	{`{"a":{"b":{}}}`, "a.b"},
	{`{"":{"x":5}}`, ".x"},
}

func TestExpiryTimestampMatchesTheDecodingImplementation(t *testing.T) {
	for _, c := range expiryCases {
		gotT, gotOK := expiryTimestamp(c.body, c.path)
		wantT, wantOK := legacyExpiryTimestamp(c.body, c.path)
		if gotOK != wantOK || !gotT.Equal(wantT) {
			t.Errorf("%s @ %s: got (%v, %v), want (%v, %v)", c.body, c.path, gotT, gotOK, wantT, wantOK)
		}
	}
}

func FuzzExpiryTimestampMatchesTheDecodingImplementation(f *testing.F) {
	for _, c := range expiryCases {
		f.Add(c.body, c.path)
	}
	f.Fuzz(func(t *testing.T, body, path string) {
		if !json.Valid([]byte(body)) || !strings.HasPrefix(strings.TrimSpace(body), "{") {
			return // stored bodies are validated JSON objects
		}
		var any_ any
		if json.Unmarshal([]byte(body), &any_) != nil {
			return // see TestExpiryNoLongerDependsOnUnrelatedFields
		}
		gotT, gotOK := expiryTimestamp(body, path)
		wantT, wantOK := legacyExpiryTimestamp(body, path)
		if gotOK != wantOK || !gotT.Equal(wantT) {
			t.Fatalf("%q @ %q: got (%v, %v), want (%v, %v)", body, path, gotT, gotOK, wantT, wantOK)
		}
	})
}

// One deliberate difference: the old implementation decoded the whole body, so a number anywhere
// in it that does not fit a float64 made encoding/json reject the document and the expiry field
// was never seen - the document never expired. Reading only the field fixes that.
func TestExpiryNoLongerDependsOnUnrelatedFields(t *testing.T) {
	body := `{"huge":1e400,"expiresAt":"2026-01-02T03:04:05Z"}`
	if _, ok := legacyExpiryTimestamp(body, "expiresAt"); ok {
		t.Fatal("oracle changed: the legacy path used to miss this field")
	}
	if at, ok := expiryTimestamp(body, "expiresAt"); !ok || at.Year() != 2026 {
		t.Fatalf("got (%v, %v)", at, ok)
	}
}

func TestExpiryTimestampDoesNotAllocate(t *testing.T) {
	body := `{"name":"x","tags":["a","b"],"nested":{"k":1},"expiresAt":"2026-01-02T03:04:05Z","more":{"deep":[1,2,3]}}`
	if n := testing.AllocsPerRun(100, func() { expiryTimestamp(body, "expiresAt") }); n > 0 {
		t.Fatalf("expiryTimestamp allocated %v times per call", n)
	}
}

func BenchmarkExpiryTimestamp(b *testing.B) {
	body := `{"name":"` + strings.Repeat("n", 200) + `","tags":["a","b","c"],"nested":{"k":1,"v":[1,2,3]},"expiresAt":"2026-01-02T03:04:05Z","pad":"` + strings.Repeat("p", 1500) + `"}`
	b.Run("fieldscan", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			expiryTimestamp(body, "expiresAt")
		}
	})
	b.Run("decode-legacy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			legacyExpiryTimestamp(body, "expiresAt")
		}
	})
}
