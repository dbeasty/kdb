package json_test

import (
	"fmt"
	"strings"
	"testing"

	kjson "github.com/limidus/kdb/go/kdb/json"
)

// esc builds JSON \uXXXX escape text from raw UTF-16 code units. Written this way rather than
// as literals so the test source contains no non-ASCII of its own - what is under test is
// precisely how the parser turns escape text into characters, and a literal character in the
// source would test nothing.
func esc(codeUnits ...int) string {
	var b strings.Builder
	for _, u := range codeUnits {
		fmt.Fprintf(&b, "%c%c%04X", '\\', 'u', u)
	}
	return b.String()
}

// surrogatePair splits a code point above the BMP into the two UTF-16 code units JSON must use
// to express it.
func surrogatePair(r rune) (high, low int) {
	v := r - 0x10000
	return int(0xD800 + (v >> 10)), int(0xDC00 + (v & 0x3FF))
}

func escapeFor(r rune) string {
	if r <= 0xFFFF {
		return esc(int(r))
	}
	h, l := surrogatePair(r)
	return esc(h, l)
}

func getString(t *testing.T, doc, path string) string {
	t.Helper()
	v, err := kjson.GetString(doc, path)
	if err != nil {
		t.Fatalf("GetString(%s) on %s: %v", path, doc, err)
	}
	s, ok := v.(kjson.StringValue)
	if !ok {
		t.Fatalf("value at %s is %T, want StringValue", path, v)
	}
	return s.V
}

// A \u escape carries one UTF-16 code unit, so any character outside the BMP - every emoji, and
// a great deal of CJK - can only be written as a surrogate pair. Encoding each half separately
// hands a lone surrogate to the UTF-8 encoder, which substitutes U+FFFD: the character used to
// arrive as two replacement characters. Kotlin's parser never had this problem, because a
// Kotlin String is UTF-16 and the two halves land in it unchanged - so the two implementations
// disagreed on the same input.
func TestParseCombinesSurrogatePairs(t *testing.T) {
	for _, tc := range []struct {
		name string
		want rune
	}{
		{"emoji", 0x1F600},           // GRINNING FACE
		{"musical symbol", 0x1D11E},  // G CLEF
		{"cjk extension b", 0x2000B}, // CJK ext B ideograph
		{"lowest supplementary", 0x10000},
		{"highest code point", 0x10FFFF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := `{"v":"` + escapeFor(tc.want) + `"}`
			got := getString(t, doc, "$.v")
			if got != string(tc.want) {
				t.Fatalf("%s parsed to %q (%d runes), want %q",
					doc, got, len([]rune(got)), string(tc.want))
			}
			if n := len([]rune(got)); n != 1 {
				t.Fatalf("a surrogate pair produced %d runes, want 1", n)
			}
		})
	}
}

// A surrogate pair has to survive a full round trip, not just the parse: the writer emits raw
// UTF-8, so re-reading its output must give the same character back.
func TestSurrogatePairRoundTripsThroughSetAndGet(t *testing.T) {
	const want = rune(0x1F600)
	doc := `{"v":"` + escapeFor(want) + `"}`
	path, err := kjson.CompilePath("$.v")
	if err != nil {
		t.Fatal(err)
	}
	rewritten, err := kjson.Set(`{}`, path, kjson.StringValue{V: getString(t, doc, "$.v")})
	if err != nil {
		t.Fatal(err)
	}
	if got := getString(t, rewritten, "$.v"); got != string(want) {
		t.Fatalf("round trip gave %q, want %q", got, string(want))
	}
}

// An unpaired surrogate is not a valid character on its own. It must not be combined with
// whatever happens to follow it into a character nobody wrote.
func TestParseLeavesUnpairedSurrogatesAlone(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"high surrogate then a plain character", esc(0xD83D) + "x"},
		{"high surrogate then another high surrogate", esc(0xD83D, 0xD83D)},
		{"low surrogate alone", esc(0xDE00)},
		{"low surrogate then high surrogate (wrong order)", esc(0xDE00, 0xD83D)},
		{"high surrogate at end of string", esc(0xD83D)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := getString(t, `{"v":"`+tc.body+`"}`, "$.v")
			for _, r := range got {
				if r > 0xFFFF {
					t.Fatalf("an unpaired surrogate produced a supplementary character %U in %q", r, got)
				}
			}
		})
	}
}

// The escape reader must not consume input it decided not to use: a high surrogate followed by
// something that is not a low surrogate has to leave that something to be parsed normally.
func TestParseDoesNotConsumeAFollowingEscapeItDidNotUse(t *testing.T) {
	got := getString(t, `{"v":"`+esc(0xD83D)+`A"}`, "$.v")
	if !strings.HasSuffix(got, "A") {
		t.Fatalf("got %q - the character after an unpaired surrogate was swallowed", got)
	}
	// A following BMP escape must still be decoded rather than eaten as a would-be low half.
	got = getString(t, `{"v":"`+esc(0xD83D, 0x0041)+`"}`, "$.v")
	if !strings.HasSuffix(got, "A") {
		t.Fatalf("got %q - the escape after an unpaired surrogate was swallowed", got)
	}
}

func TestParseHandlesTheSimpleEscapes(t *testing.T) {
	body := `a` + "\\" + `"b` + "\\" + "\\" + `c` + "\\" + `/d` + "\\" + `be` + "\\" + `ff` + "\\" + `ng` + "\\" + `rh` + "\\" + `ti`
	got := getString(t, `{"v":"`+body+`"}`, "$.v")
	want := "a\"b" + "\\" + "c/d\be\ff\ng\rh\ti"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestParseBmpUnicodeEscapes(t *testing.T) {
	for _, tc := range []struct {
		name string
		unit int
		want rune
	}{
		{"ascii", 0x0041, 'A'},
		{"latin-1", 0x00E9, 0x00E9},
		{"cjk", 0x4E2D, 0x4E2D},
		{"nul", 0x0000, 0x0000},
		{"last bmp", 0xFFFF, 0xFFFF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := getString(t, `{"v":"`+esc(tc.unit)+`"}`, "$.v")
			if got != string(tc.want) {
				t.Fatalf("got %q, want %q", got, string(tc.want))
			}
		})
	}
	// Hex digits are accepted in either case.
	lower := getString(t, `{"v":"`+"\\"+`u00e9"}`, "$.v")
	upper := getString(t, `{"v":"`+"\\"+`u00E9"}`, "$.v")
	if lower != upper {
		t.Fatalf("case of the hex digits changed the result: %q vs %q", lower, upper)
	}
}

func TestParseRejectsMalformedEscapes(t *testing.T) {
	for _, tc := range []struct {
		name string
		doc  string
	}{
		{"unknown escape", `{"v":"` + "\\" + `q"}`},
		{"backslash at end", `{"v":"` + "\\"},
		{"short u escape", `{"v":"` + "\\" + `u12"}`},
		{"non-hex in u escape", `{"v":"` + "\\" + `u12zz"}`},
		{"unclosed string", `{"v":"abc}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := kjson.GetString(tc.doc, "$.v"); err == nil {
				t.Fatalf("accepted malformed input %q", tc.doc)
			}
		})
	}
}

// Raw (unescaped) UTF-8 in the source must pass through untouched - the escape path is not the
// only way a non-ASCII character arrives.
func TestParseAcceptsRawUtf8(t *testing.T) {
	for _, want := range []string{string(rune(0x00E9)), string(rune(0x4E2D)), string(rune(0x1F600))} {
		doc := `{"v":"` + want + `"}`
		if got := getString(t, doc, "$.v"); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}
