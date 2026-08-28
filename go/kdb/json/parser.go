package json

import (
	"math"
	"strconv"
	"unicode/utf8"

	kdberr "github.com/limidus/kdb/go/kdb/error"
)

type parser struct {
	s string
	i int
}

func newParser(s string) *parser { return &parser{s: s} }

func (p *parser) parseValue() (Value, error) {
	p.skipWS()
	v, err := p.parseValueFragment()
	if err != nil {
		return nil, err
	}
	p.skipWS()
	if p.i < len(p.s) {
		return nil, kdberr.NewJsonPathError("trailing data", "$", nil)
	}
	return v, nil
}

func (p *parser) parseValueFragment() (Value, error) {
	p.skipWS()
	if p.i >= len(p.s) {
		return nil, kdberr.NewJsonPathError("invalid JSON at EOF", "$", nil)
	}
	switch p.peek() {
	case '{':
		return p.parseObject()
	case '[':
		return p.parseArray()
	case '"':
		s, err := p.parseStringLit()
		if err != nil {
			return nil, err
		}
		return StringValue{V: s}, nil
	case 't':
		if err := p.expectLit("true"); err != nil {
			return nil, err
		}
		return BoolValue{V: true}, nil
	case 'f':
		if err := p.expectLit("false"); err != nil {
			return nil, err
		}
		return BoolValue{V: false}, nil
	case 'n':
		if err := p.expectLit("null"); err != nil {
			return nil, err
		}
		return NullValue{}, nil
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return p.parseNumber()
	default:
		return nil, kdberr.NewJsonPathError("invalid JSON at "+strconv.Itoa(p.i), "$", nil)
	}
}

func (p *parser) parseObject() (Value, error) {
	if err := p.expectCh('{'); err != nil {
		return nil, err
	}
	m := make(map[string]Value)
	var keys []string
	p.skipWS()
	if p.peek() == '}' {
		p.i++
		return newObject(m, keys), nil
	}
	for {
		p.skipWS()
		if p.peek() != '"' {
			return nil, kdberr.NewJsonPathError("expected string key", "$", nil)
		}
		key, err := p.parseStringLit()
		if err != nil {
			return nil, err
		}
		p.skipWS()
		if err := p.expectCh(':'); err != nil {
			return nil, err
		}
		p.skipWS()
		val, err := p.parseValueFragment()
		if err != nil {
			return nil, err
		}
		m[key] = val
		keys = append(keys, key)
		p.skipWS()
		switch p.peek() {
		case ',':
			p.i++
		case '}':
			p.i++
			return newObject(m, keys), nil
		default:
			return nil, kdberr.NewJsonPathError("expected , or }", "$", nil)
		}
	}
}

func (p *parser) parseArray() (Value, error) {
	if err := p.expectCh('['); err != nil {
		return nil, err
	}
	p.skipWS()
	var list []Value
	if p.peek() == ']' {
		p.i++
		return ArrayValue{Elements: list}, nil
	}
	for {
		p.skipWS()
		el, err := p.parseValueFragment()
		if err != nil {
			return nil, err
		}
		list = append(list, el)
		p.skipWS()
		switch p.peek() {
		case ',':
			p.i++
		case ']':
			p.i++
			return ArrayValue{Elements: list}, nil
		default:
			return nil, kdberr.NewJsonPathError("expected , or ]", "$", nil)
		}
	}
}

func (p *parser) parseNumber() (Value, error) {
	start := p.i
	if p.peek() == '-' {
		p.i++
	}
	if p.peek() == '0' {
		p.i++
	} else {
		if !p.requireDigit() {
			return nil, kdberr.NewJsonPathError("digit needed", "$", nil)
		}
		for p.peek() >= '0' && p.peek() <= '9' {
			p.i++
		}
	}
	isFloat := false
	if p.peek() == '.' {
		isFloat = true
		p.i++
		if !p.requireDigit() {
			return nil, kdberr.NewJsonPathError("digit needed", "$", nil)
		}
		for p.peek() >= '0' && p.peek() <= '9' {
			p.i++
		}
	}
	if p.peek() == 'e' || p.peek() == 'E' {
		isFloat = true
		p.i++
		if p.peek() == '+' || p.peek() == '-' {
			p.i++
		}
		if !p.requireDigit() {
			return nil, kdberr.NewJsonPathError("digit needed", "$", nil)
		}
		for p.peek() >= '0' && p.peek() <= '9' {
			p.i++
		}
	}
	txt := p.s[start:p.i]
	if isFloat || containsAny(txt, "eE.") {
		d, err := strconv.ParseFloat(txt, 64)
		if err != nil {
			return nil, kdberr.NewJsonPathError("bad number", "$", nil)
		}
		if math.IsInf(d, 0) || math.IsNaN(d) {
			return nil, kdberr.NewJsonPathError("non-finite number", "$", nil)
		}
		return NumberValue{V: d}, nil
	}
	iv, err := strconv.ParseInt(txt, 10, 64)
	if err != nil {
		d, err2 := strconv.ParseFloat(txt, 64)
		if err2 != nil {
			return nil, kdberr.NewJsonPathError("bad number", "$", nil)
		}
		return NumberValue{V: d}, nil
	}
	return IntValue{V: iv}, nil
}

func containsAny(s, chars string) bool {
	for _, c := range s {
		for _, ch := range chars {
			if c == ch {
				return true
			}
		}
	}
	return false
}

func (p *parser) parseStringLit() (string, error) {
	if err := p.expectCh('"'); err != nil {
		return "", err
	}
	var sb []byte
	for p.i < len(p.s) {
		c := p.s[p.i]
		p.i++
		if c == '"' {
			return string(sb), nil
		}
		if c != '\\' {
			sb = append(sb, c)
			continue
		}
		if p.i >= len(p.s) {
			return "", kdberr.NewJsonPathError("bad escape", "$", nil)
		}
		e := p.s[p.i]
		p.i++
		switch e {
		case '"':
			sb = append(sb, '"')
		case '\\':
			sb = append(sb, '\\')
		case '/':
			sb = append(sb, '/')
		case 'b':
			sb = append(sb, '\b')
		case 'f':
			sb = append(sb, '\f')
		case 'n':
			sb = append(sb, '\n')
		case 'r':
			sb = append(sb, '\r')
		case 't':
			sb = append(sb, '\t')
		case 'u':
			ch, err := p.parseHex4()
			if err != nil {
				return "", err
			}
			// A \u escape carries one UTF-16 code unit, so a character outside the BMP - every
			// emoji, and a great deal of CJK - can only be written as a surrogate pair. Encoding
			// each half on its own hands utf8.EncodeRune a lone surrogate, which is not a valid
			// rune: it substitutes U+FFFD, so "😀" used to arrive as two replacement
			// characters instead of one emoji. That is silent corruption of any document from a
			// client that escapes non-ASCII, which many JSON writers do by default.
			if isHighSurrogate(ch) {
				if low, ok := p.peekUnicodeEscape(); ok && isLowSurrogate(low) {
					p.i += 6 // consume the \uXXXX we only peeked at
					ch = combineSurrogates(ch, low)
				}
			}
			var buf [utf8.UTFMax]byte
			// An unpaired surrogate is still invalid on its own; EncodeRune substitutes U+FFFD
			// for it, which is what encoding/json does with the same input.
			n := utf8.EncodeRune(buf[:], ch)
			sb = append(sb, buf[:n]...)
		default:
			return "", kdberr.NewJsonPathError("bad escape", "$", nil)
		}
	}
	return "", kdberr.NewJsonPathError("unclosed string", "$", nil)
}

// UTF-16 surrogate ranges, as used by JSON's \u escapes for characters outside the BMP.
const (
	highSurrogateMin = 0xD800
	highSurrogateMax = 0xDBFF
	lowSurrogateMin  = 0xDC00
	lowSurrogateMax  = 0xDFFF
)

func isHighSurrogate(r rune) bool { return r >= highSurrogateMin && r <= highSurrogateMax }
func isLowSurrogate(r rune) bool  { return r >= lowSurrogateMin && r <= lowSurrogateMax }

// combineSurrogates rebuilds the code point a surrogate pair encodes.
func combineSurrogates(high, low rune) rune {
	return 0x10000 + (high-highSurrogateMin)<<10 + (low - lowSurrogateMin)
}

// peekUnicodeEscape reads a following \uXXXX without consuming it, so a high surrogate that
// turns out not to be followed by a low one is left exactly as it was for the caller to handle.
func (p *parser) peekUnicodeEscape() (rune, bool) {
	if p.i+6 > len(p.s) || p.s[p.i] != '\\' || p.s[p.i+1] != 'u' {
		return 0, false
	}
	saved := p.i
	p.i += 2
	r, err := p.parseHex4()
	p.i = saved
	if err != nil {
		return 0, false
	}
	return r, true
}

func (p *parser) parseHex4() (rune, error) {
	var v int
	for k := 0; k < 4; k++ {
		if p.i >= len(p.s) {
			return 0, kdberr.NewJsonPathError("bad \\u", "$", nil)
		}
		ch := p.s[p.i]
		p.i++
		var d int
		switch {
		case ch >= '0' && ch <= '9':
			d = int(ch - '0')
		case ch >= 'a' && ch <= 'f':
			d = 10 + int(ch-'a')
		case ch >= 'A' && ch <= 'F':
			d = 10 + int(ch-'A')
		default:
			return 0, kdberr.NewJsonPathError("bad \\u", "$", nil)
		}
		v = v*16 + d
	}
	return rune(v), nil
}

func (p *parser) skipWS() {
	for p.i < len(p.s) {
		switch p.s[p.i] {
		case ' ', '\n', '\r', '\t':
			p.i++
		default:
			return
		}
	}
}

func (p *parser) peek() byte {
	if p.i < len(p.s) {
		return p.s[p.i]
	}
	return 0
}

func (p *parser) expectCh(ch byte) error {
	if p.i >= len(p.s) || p.s[p.i] != ch {
		return kdberr.NewJsonPathError("expected "+string(ch), "$", nil)
	}
	p.i++
	return nil
}

func (p *parser) expectLit(lit string) error {
	if !hasPrefix(p.s, p.i, lit) {
		return kdberr.NewJsonPathError("expected "+lit, "$", nil)
	}
	p.i += len(lit)
	return nil
}

func (p *parser) requireDigit() bool {
	if p.peek() >= '0' && p.peek() <= '9' {
		return true
	}
	return false
}

func hasPrefix(s string, i int, lit string) bool {
	return i+len(lit) <= len(s) && s[i:i+len(lit)] == lit
}
