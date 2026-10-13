package wire

import "unicode/utf8"

// EncodedStringLen is an upper bound on the bytes encoding/json writes for s as a JSON string,
// quotes included, under the escaping Encode uses (HTML-safe). It is exact except for '\b' and
// '\f', which it counts as six-byte \u escapes whatever the Go version writes.
//
// It exists so a reply can be bounded in bytes before it is encoded: a row or a document body
// travels as a JSON string inside the envelope, and its encoded size - not its raw size - is what
// has to fit the frame.
func EncodedStringLen(s string) int {
	n := 2
	for i := 0; i < len(s); {
		b := s[i]
		if b < utf8.RuneSelf {
			switch {
			case b == '"' || b == '\\' || b == '\n' || b == '\r' || b == '\t':
				n += 2
			case b < 0x20 || b == '<' || b == '>' || b == '&':
				n += 6
			default:
				n++
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			n += 6 // invalid UTF-8 is written as �
		} else if r == ' ' || r == ' ' {
			n += 6
		} else {
			n += size
		}
		i += size
	}
	return n
}
