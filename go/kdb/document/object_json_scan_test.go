package document

import (
	"encoding/json"
	"strings"
	"testing"
)

// unmarshalAcceptsObject is what validateObjectJSON accepted before the scan: Unmarshal into an
// any succeeds and yields a map.
func unmarshalAcceptsObject(text string) bool {
	var v any
	if err := json.Unmarshal([]byte(text), &v); err != nil {
		return false
	}
	_, ok := v.(map[string]any)
	return ok
}

// oldValidateObjectJSON is validateObjectJSON as it was, for comparing errors.
func oldValidateObjectJSON(jsonText string) error {
	var v any
	if err := json.Unmarshal([]byte(jsonText), &v); err != nil {
		return NewDecodeError("invalid json", nil, err)
	}
	if _, ok := v.(map[string]any); !ok {
		return NewDecodeError("root must be a JSON object", nil, nil)
	}
	return nil
}

var objectJSONCases = []string{
	``, ` `, `{`, `}`, `{}`, ` {} `, "\t{\r\n}\n", "\v{}", "\f{}", "\u00a0{}", "\ufeff{}",
	`[]`, `"x"`, `1`, `null`, `true`, `{}{}`, `{} x`, `{},`,
	`{"a":1}`, `{"a":1,}`, `{,"a":1}`, `{"a" 1}`, `{"a":}`, `{a:1}`, `{'a':1}`, `{"a":1 "b":2}`,
	`{"a":1,"a":2}`, `{"":0}`,
	`{"a":0}`, `{"a":-0}`, `{"a":01}`, `{"a":-01}`, `{"a":1.}`, `{"a":.5}`, `{"a":1.5}`, `{"a":-}`,
	`{"a":+1}`, `{"a":1e5}`, `{"a":1E+5}`, `{"a":1e-5}`, `{"a":1e}`, `{"a":1e+}`, `{"a":0x10}`,
	`{"a":1e308}`, `{"a":1.8e308}`, `{"a":1e309}`, `{"a":-1e400}`, `{"a":1e400}`, `{"a":1e-400}`,
	`{"a":4.9e-324}`, `{"a":2.5e-324}`, `{"a":123456789012345678901234567890}`,
	`{"a":` + strings.Repeat("9", 308) + `}`, `{"a":` + strings.Repeat("9", 309) + `}`,
	`{"a":` + strings.Repeat("9", 400) + `}`, `{"a":0.` + strings.Repeat("0", 400) + `1}`,
	`{"a":1` + strings.Repeat("0", 299) + `.5}`, `{"a":NaN}`, `{"a":Infinity}`,
	`{"a":true}`, `{"a":false}`, `{"a":null}`, `{"a":tru}`, `{"a":truex}`, `{"a":nul}`, `{"a":True}`,
	`{"a":"\"\\\/\b\f\n\r\t"}`, `{"a":"Aé😀"}`, `{"a":"\uD800"}`, `{"a":"\u12"}`,
	`{"a":"\u12G4"}`, `{"a":"\x"}`, `{"a":"\'"}`, `{"a":"` + "\x01" + `"}`, `{"a":"` + "\x7f" + `"}`,
	`{"a":"` + "\xff\xfe" + `"}`, `{"a":"` + "\xc3" + `"}`, `{"` + "\xff" + `":1}`, `{"a":"unterminated}`,
	`{"a":"\`, `{"a":"\u00`, "{\"a\":\"\t\"}", "{\"a\":\"\n\"}", `{"a":"é ü 中文"}`, "{\"a\":1}\x00",
	`{"a":[]}`, `{"a":[1,2,3]}`, `{"a":[1,]}`, `{"a":[,1]}`, `{"a":[1 2]}`, `{"a":[}`, `{"a":]}`,
	`{"a":{"b":{"c":[{"d":null}]}}}`, `{"a":{"b":}}`, `{"a":{}`, `{"a":[{}]]}`,
	`{"field0":"` + strings.Repeat("x", 100) + `","field1":"` + strings.Repeat("y", 100) + `"}`,
	`{"a":1 }`, `{ "a" : 1 , "b" : [ 1 , 2 ] }`,
	strings.Repeat(`{"a":`, maxScanDepth) + `1` + strings.Repeat(`}`, maxScanDepth),
	strings.Repeat(`{"a":`, maxScanDepth+1) + `1` + strings.Repeat(`}`, maxScanDepth+1),
	`{"a":` + strings.Repeat(`[`, 10001) + strings.Repeat(`]`, 10001) + `}`,
	`{"a":` + strings.Repeat(`[`, 9998) + strings.Repeat(`]`, 9998) + `}`,
}

// The scan accepts exactly what Unmarshal accepts as an object, wherever it does not give up on
// depth; where it gives up, validateObjectJSON's fallback decides.
func TestIsObjectJSONMatchesUnmarshal(t *testing.T) {
	for _, text := range objectJSONCases {
		want := unmarshalAcceptsObject(text)
		got := isObjectJSON(text)
		if got && !want {
			t.Errorf("isObjectJSON(%.80q) = true, Unmarshal rejects it", text)
		}
		if !got && want && len(text) <= 2*maxScanDepth {
			t.Errorf("isObjectJSON(%.80q) = false, Unmarshal accepts it", text)
		}
	}
}

// validateObjectJSON accepts and rejects exactly as it did when it was Unmarshal alone, with the
// same error text.
func TestValidateObjectJSONUnchanged(t *testing.T) {
	for _, text := range objectJSONCases {
		got, want := validateObjectJSON(text), oldValidateObjectJSON(text)
		if (got == nil) != (want == nil) || (got != nil && got.Error() != want.Error()) {
			t.Errorf("validateObjectJSON(%.80q) = %v, was %v", text, got, want)
		}
	}
}

func FuzzIsObjectJSONMatchesUnmarshal(f *testing.F) {
	for _, text := range objectJSONCases {
		f.Add(text)
	}
	f.Fuzz(func(t *testing.T, text string) {
		want := unmarshalAcceptsObject(text)
		got := isObjectJSON(text)
		if got && !want {
			t.Fatalf("isObjectJSON(%q) = true, Unmarshal rejects it", text)
		}
		if !got && want && len(text) <= maxScanDepth {
			t.Fatalf("isObjectJSON(%q) = false, Unmarshal accepts it", text)
		}
		if g, w := validateObjectJSON(text), oldValidateObjectJSON(text); (g == nil) != (w == nil) || (g != nil && g.Error() != w.Error()) {
			t.Fatalf("validateObjectJSON(%q) = %v, was %v", text, g, w)
		}
	})
}

var benchDoc = func() string {
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i < 10; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`"field`)
		b.WriteByte(byte('0' + i))
		b.WriteString(`":"`)
		b.WriteString(strings.Repeat("abcdefghij", 10))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}()

func BenchmarkValidateObjectJSON(b *testing.B) {
	b.Run("scan", func(b *testing.B) {
		b.SetBytes(int64(len(benchDoc)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := validateObjectJSON(benchDoc); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("unmarshal", func(b *testing.B) {
		b.SetBytes(int64(len(benchDoc)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := oldValidateObjectJSON(benchDoc); err != nil {
				b.Fatal(err)
			}
		}
	})
}
