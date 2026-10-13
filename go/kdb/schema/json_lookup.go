package schema

import (
	"encoding/json"
	"strings"
)

// FieldRaw returns the raw JSON text of the value at fieldPath - a dotted path of object keys, as
// FieldValue takes - without decoding the document. found is false when the path is absent, runs
// through something that is not an object, or the document is not well-formed along the way.
//
// It exists for hot paths that need one field of every document they touch: the expiry check
// runs on every point read and every scanned row when an expiry policy is set, and FieldValue's
// json.Unmarshal of the whole body into a map (after copying it to a []byte) cost an allocation
// per key and value for one timestamp. FieldRaw walks the text once, skipping every value it is
// not looking for, and allocates nothing unless a key on the path is escaped or non-ASCII.
//
// Semantics match FieldValue on well-formed JSON: when a key repeats, the last occurrence wins,
// as it does when encoding/json decodes into a map.
func FieldRaw(docJSON, fieldPath string) (raw string, found bool) {
	s := docJSON
	i := skipWS(s, 0)
	rest := fieldPath
	for {
		seg := rest
		dot := strings.IndexByte(rest, '.')
		if dot >= 0 {
			seg, rest = rest[:dot], rest[dot+1:]
		}
		start, end, ok := findMember(s, i, seg)
		if !ok {
			return "", false
		}
		if dot < 0 {
			return s[start:end], true
		}
		i = start
	}
}

// findMember scans the object starting at s[i] for key and returns the bounds of its (last)
// value.
func findMember(s string, i int, key string) (start, end int, ok bool) {
	if i >= len(s) || s[i] != '{' {
		return 0, 0, false
	}
	i = skipWS(s, i+1)
	if i < len(s) && s[i] == '}' {
		return 0, 0, false
	}
	found := false
	for {
		if i >= len(s) || s[i] != '"' {
			return 0, 0, false
		}
		kEnd, ok := skipString(s, i)
		if !ok {
			return 0, 0, false
		}
		match := keyEquals(s[i:kEnd], key)
		i = skipWS(s, kEnd)
		if i >= len(s) || s[i] != ':' {
			return 0, 0, false
		}
		i = skipWS(s, i+1)
		vEnd, ok := skipValue(s, i)
		if !ok {
			return 0, 0, false
		}
		if match {
			start, end, found = i, vEnd, true
		}
		i = skipWS(s, vEnd)
		if i >= len(s) {
			return 0, 0, false
		}
		switch s[i] {
		case ',':
			i = skipWS(s, i+1)
		case '}':
			return start, end, found
		default:
			return 0, 0, false
		}
	}
}

// keyEquals compares a quoted JSON key with key. Plain ASCII keys compare in place; anything
// escaped or non-ASCII is decoded first, exactly as encoding/json would.
func keyEquals(quoted, key string) bool {
	inner := quoted[1 : len(quoted)-1]
	for j := 0; j < len(inner); j++ {
		if c := inner[j]; c == '\\' || c >= 0x80 {
			var decoded string
			if err := json.Unmarshal([]byte(quoted), &decoded); err != nil {
				return false
			}
			return decoded == key
		}
	}
	return inner == key
}

func skipWS(s string, i int) int {
	for i < len(s) {
		switch s[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// skipString returns the index just past the string starting at s[i] (a '"').
func skipString(s string, i int) (int, bool) {
	j := i + 1
	for {
		q := strings.IndexByte(s[j:], '"')
		if q < 0 {
			return 0, false
		}
		j += q
		// The quote ends the string unless an odd run of backslashes escapes it.
		bs := 0
		for k := j - 1; k > i && s[k] == '\\'; k-- {
			bs++
		}
		if bs%2 == 0 {
			return j + 1, true
		}
		j++
	}
}

// skipValue returns the index just past the value starting at s[i].
func skipValue(s string, i int) (int, bool) {
	if i >= len(s) {
		return 0, false
	}
	switch s[i] {
	case '"':
		return skipString(s, i)
	case '{', '[':
		depth := 0
		for j := i; j < len(s); j++ {
			switch s[j] {
			case '"':
				end, ok := skipString(s, j)
				if !ok {
					return 0, false
				}
				j = end - 1
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return j + 1, true
				}
			}
		}
		return 0, false
	default:
		j := i
		for j < len(s) {
			switch s[j] {
			case ',', '}', ']', ' ', '\t', '\n', '\r':
				if j == i {
					return 0, false
				}
				return j, true
			}
			j++
		}
		if j == i {
			return 0, false
		}
		return j, true
	}
}
