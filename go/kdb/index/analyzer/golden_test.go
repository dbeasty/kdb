package analyzer_test

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/index/analyzer"
)

const fixtureDir = "../../../testdata/golden/search"

// TestAnalyzerGoldenVectors pins the whole analyzer pipeline (tokenize, lowercase, length cap,
// stopwords, stem) to the fixture the Kotlin tree asserts against, so the two implementations
// cannot drift apart on tokenization.
func TestAnalyzerGoldenVectors(t *testing.T) {
	raw, err := os.ReadFile(fixtureDir + "/analyzer_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors []struct {
		Text   string   `json:"text"`
		Tokens []string `json:"tokens"`
	}
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	if len(vectors) < 15 {
		t.Fatalf("fixture has only %d vectors", len(vectors))
	}
	for _, v := range vectors {
		got := analyzer.Terms(v.Text)
		if strings.Join(got, "\x00") != strings.Join(v.Tokens, "\x00") {
			t.Errorf("Analyze(%q) = %v, want %v", v.Text, got, v.Tokens)
		}
	}
}

// TestPorterGoldenVectors pins the stemmer to the shared word/stem fixture (≥ 200 pairs,
// including every case the spec lists).
func TestPorterGoldenVectors(t *testing.T) {
	f, err := os.Open(fixtureDir + "/porter_vectors.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 2 {
			t.Fatalf("malformed fixture line: %q", line)
		}
		if got := analyzer.Stem(parts[0]); got != parts[1] {
			t.Errorf("Stem(%q) = %q, want %q", parts[0], got, parts[1])
		}
		n++
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if n < 200 {
		t.Fatalf("fixture has %d pairs, spec requires at least 200", n)
	}
}

// TestAnalyzePositionsSkipStopwords: positions are assigned after stopword removal (§6.1),
// which is what lets a phrase query match across a dropped stopword.
func TestAnalyzePositionsSkipStopwords(t *testing.T) {
	toks := analyzer.Analyze("the deploy of the staging")
	if len(toks) != 2 {
		t.Fatalf("got %v, want two tokens", toks)
	}
	if toks[0].Term != "deploi" || toks[0].Position != 0 {
		t.Errorf("first token = %+v, want {deploi 0}", toks[0])
	}
	if toks[1].Term != "stage" || toks[1].Position != 1 {
		t.Errorf("second token = %+v, want {stage 1} (the dropped stopwords must not advance positions)", toks[1])
	}
}

// TestAnalyzeDropsOverlongTokens: a token longer than 64 code points is dropped entirely and
// does not occupy a position.
func TestAnalyzeDropsOverlongTokens(t *testing.T) {
	long := strings.Repeat("a", 65)
	toks := analyzer.Analyze(long + " short")
	if len(toks) != 1 || toks[0].Term != "short" || toks[0].Position != 0 {
		t.Fatalf("got %+v, want only {short 0}", toks)
	}
	if got := analyzer.Terms(strings.Repeat("b", 64)); len(got) != 1 {
		t.Errorf("a 64-code-point token is kept: got %v", got)
	}
}

// TestStopwordListIsExactlyTheSpecSet guards the 33-word list itself: adding or losing one
// silently changes every score in the corpus.
func TestStopwordListIsExactlyTheSpecSet(t *testing.T) {
	want := strings.Fields(`a an and are as at be but by for if in into is it no not of on or such
		that the their then there these they this to was will with`)
	if len(analyzer.Stopwords) != 33 {
		t.Fatalf("stopword list has %d words, spec fixes 33", len(analyzer.Stopwords))
	}
	if strings.Join(analyzer.Stopwords, " ") != strings.Join(want, " ") {
		t.Errorf("stopword list = %v, want %v", analyzer.Stopwords, want)
	}
}
