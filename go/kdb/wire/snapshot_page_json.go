package wire

import (
	"encoding/base64"
	"strconv"
	"unicode/utf8"

	"github.com/limidus/kdb/go/kdb/document"
)

// appendSnapshotPageJSON appends m's JSON envelope to dst - exactly the bytes encoding/json writes
// for messageToEnvelope(m) - sized up front.
//
// A snapshot page is the one message that is routinely megabytes of document bodies, and
// encoding/json builds its output in a buffer that starts small and doubles, then copies it into
// the frame: about three times the page in garbage per page, which on a bootstrap was the host's
// largest allocation after the reads themselves. Here the frame is sized once from the bodies'
// lengths and written into directly.
func appendSnapshotPageJSON(dst []byte, m SnapshotPageMessage) ([]byte, error) {
	payload, err := EncodeCommits([]document.Commit{m.Commit})
	if err != nil {
		return nil, err
	}
	size := 128 + len(m.Namespace) + len(m.Next) + base64.StdEncoding.EncodedLen(len(payload)) + base64.StdEncoding.EncodedLen(len(m.AttachmentTables))
	for _, d := range m.Docs {
		// Bodies are JSON, so most of their quotes are escaped: an eighth more is a fair guess,
		// and a wrong one only costs a regrowth.
		size += len(d.DocID) + len(d.Body) + len(d.Body)/8 + 20
	}
	if cap(dst)-len(dst) < size {
		grown := make([]byte, len(dst), len(dst)+size)
		copy(grown, dst)
		dst = grown
	}
	dst = append(dst, `{"kind":"snapshotPage","snapshotPage":{"namespace":`...)
	dst = appendJSONString(dst, m.Namespace)
	dst = append(dst, `,"commitPayload":`...)
	dst = appendJSONBytes(dst, payload)
	dst = append(dst, `,"docs":[`...) // never null: encodeSnapshotV2Message always makes the slice
	for i, d := range m.Docs {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, `{"id":`...)
		dst = appendJSONString(dst, d.DocID)
		dst = append(dst, `,"body":`...)
		dst = appendJSONString(dst, d.Body)
		dst = append(dst, '}')
	}
	dst = append(dst, ']')
	if m.Next != "" {
		dst = append(dst, `,"next":`...)
		dst = appendJSONString(dst, m.Next)
	}
	dst = append(dst, `,"done":`...)
	dst = strconv.AppendBool(dst, m.Done)
	dst = append(dst, `,"total":`...)
	dst = strconv.AppendInt(dst, int64(m.Total), 10)
	if len(m.AttachmentTables) > 0 {
		dst = append(dst, `,"attachmentTables":`...)
		dst = appendJSONBytes(dst, m.AttachmentTables)
	}
	return append(dst, "}}"...), nil
}

// appendJSONBytes is encoding/json's encoding of a []byte: base64, or null for nil.
func appendJSONBytes(dst, b []byte) []byte {
	if b == nil {
		return append(dst, "null"...)
	}
	dst = append(dst, '"')
	dst = base64.StdEncoding.AppendEncode(dst, b)
	return append(dst, '"')
}

const jsonHex = "0123456789abcdef"

// appendJSONString is encoding/json's encoding of a string with HTML escaping on (its default,
// and json.Encoder's): <, > and & escaped, invalid UTF-8 replaced, U+2028 and U+2029 escaped.
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if b >= 0x20 && b != '"' && b != '\\' && b != '<' && b != '>' && b != '&' {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			switch b {
			case '\\', '"':
				dst = append(dst, '\\', b)
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', jsonHex[b>>4], jsonHex[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(s[i:])
		if c == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', 'f', 'f', 'f', 'd')
			i += size
			start = i
			continue
		}
		if c == ' ' || c == ' ' {
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', '2', '0', '2', jsonHex[c&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}
