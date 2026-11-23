package document

import "strconv"

// isObjectJSON reports whether text is a JSON object that encoding/json's Unmarshal into an any
// accepts - without building the map that Unmarshal would. It is the fast path of
// validateObjectJSON, which every inserted document and every replayed write goes through: the
// map[string]any of a 1 KB document costs dozens of allocations, and the caller threw it away.
//
// It may answer false for text Unmarshal would accept (it gives up past maxScanDepth, for
// instance); validateObjectJSON then runs Unmarshal, which decides, and reports Unmarshal's own
// error. So a false is never an answer on its own - only a true is, and a true must mean
// Unmarshal succeeds and yields a map. That is the grammar of encoding/json's scanner (RFC 8259:
// whitespace is space, tab, CR and LF; no leading zeros; control characters must be escaped;
// bytes of 0x80 and above, valid UTF-8 or not, are accepted inside strings) plus the one way
// decoding into any fails after the scanner accepted the text: a number out of float64's range,
// such as 1e400, which strconv.ParseFloat rejects. FuzzIsObjectJSONMatchesUnmarshal holds the two
// to the same answers.
func isObjectJSON(text string) bool {
	s := objectScanner{s: text}
	s.skipSpace()
	if s.i >= len(s.s) || s.s[s.i] != '{' {
		return false
	}
	if !s.value(0) {
		return false
	}
	s.skipSpace()
	return s.i == len(s.s)
}

// maxScanDepth is how deep isObjectJSON follows nested containers before giving up and leaving
// the answer to encoding/json, whose own limit (10000) it then applies.
const maxScanDepth = 512

type objectScanner struct {
	s string
	i int
}

func (s *objectScanner) skipSpace() {
	for s.i < len(s.s) {
		switch s.s[s.i] {
		case ' ', '\t', '\n', '\r':
			s.i++
		default:
			return
		}
	}
}

// value scans one JSON value starting at s.i (no leading whitespace).
func (s *objectScanner) value(depth int) bool {
	if s.i >= len(s.s) {
		return false
	}
	switch c := s.s[s.i]; {
	case c == '{':
		return s.object(depth + 1)
	case c == '[':
		return s.array(depth + 1)
	case c == '"':
		return s.str()
	case c == '-' || ('0' <= c && c <= '9'):
		return s.number()
	case c == 't':
		return s.literal("true")
	case c == 'f':
		return s.literal("false")
	case c == 'n':
		return s.literal("null")
	}
	return false
}

func (s *objectScanner) object(depth int) bool {
	if depth > maxScanDepth {
		return false
	}
	s.i++ // '{'
	s.skipSpace()
	if s.i < len(s.s) && s.s[s.i] == '}' {
		s.i++
		return true
	}
	for {
		if s.i >= len(s.s) || s.s[s.i] != '"' || !s.str() {
			return false
		}
		s.skipSpace()
		if s.i >= len(s.s) || s.s[s.i] != ':' {
			return false
		}
		s.i++
		s.skipSpace()
		if !s.value(depth) {
			return false
		}
		s.skipSpace()
		if s.i >= len(s.s) {
			return false
		}
		switch s.s[s.i] {
		case ',':
			s.i++
			s.skipSpace()
		case '}':
			s.i++
			return true
		default:
			return false
		}
	}
}

func (s *objectScanner) array(depth int) bool {
	if depth > maxScanDepth {
		return false
	}
	s.i++ // '['
	s.skipSpace()
	if s.i < len(s.s) && s.s[s.i] == ']' {
		s.i++
		return true
	}
	for {
		if !s.value(depth) {
			return false
		}
		s.skipSpace()
		if s.i >= len(s.s) {
			return false
		}
		switch s.s[s.i] {
		case ',':
			s.i++
			s.skipSpace()
		case ']':
			s.i++
			return true
		default:
			return false
		}
	}
}

// str scans a string literal starting at its opening quote.
func (s *objectScanner) str() bool {
	i := s.i + 1
	for i < len(s.s) {
		c := s.s[i]
		switch {
		case c == '"':
			s.i = i + 1
			return true
		case c == '\\':
			if i+1 >= len(s.s) {
				return false
			}
			switch s.s[i+1] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				i += 2
			case 'u':
				if i+6 > len(s.s) {
					return false
				}
				if !isHex(s.s[i+2]) || !isHex(s.s[i+3]) || !isHex(s.s[i+4]) || !isHex(s.s[i+5]) {
					return false
				}
				i += 6
			default:
				return false
			}
		case c < 0x20:
			return false
		default:
			i++
		}
	}
	return false
}

func isHex(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F'
}

// number scans a number: -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?, which must also be in
// float64's range, as decoding it into an any requires.
func (s *objectScanner) number() bool {
	start := s.i
	i := s.i
	if s.s[i] == '-' {
		i++
	}
	if i >= len(s.s) {
		return false
	}
	switch c := s.s[i]; {
	case c == '0':
		i++
	case '1' <= c && c <= '9':
		for i < len(s.s) && '0' <= s.s[i] && s.s[i] <= '9' {
			i++
		}
	default:
		return false
	}
	if i < len(s.s) && s.s[i] == '.' {
		i++
		digits := i
		for i < len(s.s) && '0' <= s.s[i] && s.s[i] <= '9' {
			i++
		}
		if i == digits {
			return false
		}
	}
	exponent := false
	if i < len(s.s) && (s.s[i] == 'e' || s.s[i] == 'E') {
		exponent = true
		i++
		if i < len(s.s) && (s.s[i] == '+' || s.s[i] == '-') {
			i++
		}
		digits := i
		for i < len(s.s) && '0' <= s.s[i] && s.s[i] <= '9' {
			i++
		}
		if i == digits {
			return false
		}
	}
	s.i = i
	// Without an exponent, a number of at most 300 characters is below 1e300 and cannot overflow
	// (underflow is not an error to ParseFloat). Anything else is checked exactly.
	if exponent || i-start > 300 {
		if _, err := strconv.ParseFloat(s.s[start:i], 64); err != nil {
			return false
		}
	}
	return true
}

func (s *objectScanner) literal(word string) bool {
	if len(s.s)-s.i < len(word) || s.s[s.i:s.i+len(word)] != word {
		return false
	}
	s.i += len(word)
	return true
}
