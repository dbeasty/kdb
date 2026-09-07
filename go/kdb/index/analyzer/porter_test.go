package analyzer_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/index/analyzer"
)

// TestStemClassicPorterCases guards the stemmer against every word/stem pair the Layer 16
// spec (§6.1) lists - the classic examples from Porter's paper, run through the whole
// algorithm rather than the single step each illustrates (so "agreed" is "agre", not "agree").
func TestStemClassicPorterCases(t *testing.T) {
	cases := map[string]string{
		"caresses": "caress", "ponies": "poni", "ties": "ti", "caress": "caress", "cats": "cat",
		"feed": "feed", "agreed": "agre", "plastered": "plaster", "motoring": "motor", "sing": "sing",
		"conflated": "conflat", "troubled": "troubl", "sized": "size", "hopping": "hop", "tanned": "tan",
		"falling": "fall", "hissing": "hiss", "fizzed": "fizz", "failing": "fail", "filing": "file",
		"happy": "happi", "sky": "sky", "relational": "relat", "conditional": "condit", "rational": "ration",
		"valenci": "valenc", "hesitanci": "hesit", "digitizer": "digit", "conformabli": "conform",
		"radicalli": "radic", "differentli": "differ", "vileli": "vile", "analogousli": "analog",
		"vietnamization": "vietnam", "predication": "predic", "operator": "oper", "feudalism": "feudal",
		"decisiveness": "decis", "hopefulness": "hope", "callousness": "callous", "formaliti": "formal",
		"sensitiviti": "sensit", "sensibiliti": "sensibl", "triplicate": "triplic", "formative": "form",
		"formalize": "formal", "electriciti": "electr", "electrical": "electr", "hopeful": "hope",
		"goodness": "good", "revival": "reviv", "allowance": "allow", "inference": "infer",
		"airliner": "airlin", "gyroscopic": "gyroscop", "adjustable": "adjust", "defensible": "defens",
		"irritant": "irrit", "replacement": "replac", "adjustment": "adjust", "dependent": "depend",
		"adoption": "adopt", "homologou": "homolog", "communism": "commun", "activate": "activ",
		"angulariti": "angular", "homologous": "homolog", "effective": "effect", "bowdlerize": "bowdler",
		"probate": "probat", "rate": "rate", "cease": "ceas", "controll": "control", "roll": "roll",
	}
	for word, want := range cases {
		if got := analyzer.Stem(word); got != want {
			t.Errorf("Stem(%q) = %q, want %q", word, got, want)
		}
	}
}

// TestStemLeavesShortAndNonASCIITokensAlone: the algorithm is defined over ASCII letters, and
// the reference returns words of two letters or fewer untouched.
func TestStemLeavesShortAndNonASCIITokensAlone(t *testing.T) {
	for _, w := range []string{"", "a", "is", "über", "naïveté", "日本語"} {
		if got := analyzer.Stem(w); got != w {
			t.Errorf("Stem(%q) = %q, want unchanged", w, got)
		}
	}
}
