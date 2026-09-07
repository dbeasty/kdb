package analyzer

// Stopwords is the classic Lucene/Snowball English set (Layer 16 §6.2), matched after
// lowercasing. It is exactly these 33 words.
var Stopwords = []string{
	"a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it",
	"no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these", "they",
	"this", "to", "was", "will", "with",
}

var stopwordSet = func() map[string]struct{} {
	m := make(map[string]struct{}, len(Stopwords))
	for _, w := range Stopwords {
		m[w] = struct{}{}
	}
	return m
}()

// IsStopword reports whether a lowercased token is dropped by the analyzer.
func IsStopword(token string) bool {
	_, ok := stopwordSet[token]
	return ok
}
