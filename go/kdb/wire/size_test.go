package wire

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestEncodedStringLenMatchesEncodingJSON(t *testing.T) {
	cases := []string{
		"", "plain", `{"a":1,"b":"x"}`, "quote\"back\\slash", "nl\ncr\rtab\t",
		"ctrl\x01\x1f", "<html>&amp;", "é漢字🙂", "bad\xffutf8", "ls ps ",
		strings.Repeat(`{"k":"v"}`, 1000),
	}
	for _, s := range cases {
		b, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		if got := EncodedStringLen(s); got != len(b) {
			t.Errorf("EncodedStringLen(%q) = %d, encoding/json wrote %d", s, got, len(b))
		}
	}
	// \b and \f are the one documented over-estimate.
	if got, b := EncodedStringLen("\b\f"), mustMarshal(t, "\b\f"); got < len(b) {
		t.Errorf("EncodedStringLen must not under-estimate \\b\\f: %d < %d", got, len(b))
	}
}

func FuzzEncodedStringLenIsAnUpperBound(f *testing.F) {
	f.Add("x")
	f.Add("\x00\b\f<>&\"\\ \xff")
	f.Fuzz(func(t *testing.T, s string) {
		b := mustMarshal(t, s)
		got := EncodedStringLen(s)
		if got < len(b) || got > len(b)+8*strings.Count(s, "\b")+8*strings.Count(s, "\f") {
			t.Fatalf("EncodedStringLen(%q) = %d, encoding/json wrote %d", s, got, len(b))
		}
	})
}

func mustMarshal(t *testing.T, s string) []byte {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
