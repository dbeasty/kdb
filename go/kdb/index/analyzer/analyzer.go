// Package analyzer is the Layer 16 (§6.1) text analyzer shared by the full-text index and its
// query parser: Unicode letter/digit tokenization, simple lowercasing, a 64-code-point length
// cap, the fixed English stopword list, and the original Porter stemmer. Positions are
// assigned after stopword removal so phrase matching ignores stopwords.
package analyzer

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// MaxTokenLength is the longest token (in code points) the analyzer keeps.
const MaxTokenLength = 64

// Token is one analyzed term with its 0-based position after stopword removal.
type Token struct {
	Term     string
	Position int
}

// Analyze tokenizes, lowercases, filters and stems text.
func Analyze(text string) []Token {
	var out []Token
	pos := 0
	emit := func(raw string) {
		tok := strings.ToLower(raw)
		if utf8.RuneCountInString(tok) > MaxTokenLength {
			return
		}
		if IsStopword(tok) {
			return
		}
		out = append(out, Token{Term: Stem(tok), Position: pos})
		pos++
	}
	start := -1
	for i, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 {
			emit(text[start:i])
			start = -1
		}
	}
	if start >= 0 {
		emit(text[start:])
	}
	return out
}

// Terms returns the analyzed terms of text in order (positions dropped).
func Terms(text string) []string {
	toks := Analyze(text)
	out := make([]string, len(toks))
	for i, t := range toks {
		out[i] = t.Term
	}
	return out
}
