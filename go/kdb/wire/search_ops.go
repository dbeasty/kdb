package wire

// Search wire messages (SEARCH 0x1D / SEARCH_RESULT 0x1E) - kdb-spec-layer16 §11, Component 69.
//
// Layer 16 — implemented in both trees: unlike 0x14-0x1C these have a Kotlin counterpart and
// identical JSON bodies, so a Go client can search a Kotlin server and vice versa. Sessionless
// like DOCUMENT_GET; authorized as DocumentRead on the namespace.

// SearchTextArm is the lexical (full-text) arm of a SEARCH.
type SearchTextArm struct {
	// Index is a FULLTEXT index name, or the first field of one.
	Index string
	Query string
	// Depth caps candidates fetched from this arm before fusion; 0 means the server default.
	Depth    int
	MinScore *float64
	// Weight is this arm's weight under "weighted" fusion; nil means 1.
	Weight *float64
}

// SearchVectorArm is the vector-similarity arm of a SEARCH.
type SearchVectorArm struct {
	// Index is a VECTOR index name, or the field it indexes.
	Index    string
	Vector   []float64
	Depth    int
	MinScore *float64
	Weight   *float64
}

// SearchMessage asks for the top Limit documents by one arm's ranking, or by the fused ranking
// when both arms are present (Fusion "rrf" or "weighted", §8).
type SearchMessage struct {
	H         Header
	Namespace string
	// SessionID is optional: a SNAPSHOT session may name itself so the search runs at its pin.
	SessionID string
	Text      *SearchTextArm
	Vector    *SearchVectorArm
	Fusion    string
	Limit     int
	// IncludeJSON asks for each hit's document body alongside its id and score.
	IncludeJSON bool
	// AtCommitHex pins the search to a historical commit; empty means head.
	AtCommitHex string
}

func (m SearchMessage) Header() Header { return m.H }

// SearchHit is one ranked document.
type SearchHit struct {
	DocID string
	Score float64
	// JSON is set only when the request asked for IncludeJSON.
	JSON *string
}

// SearchResultMessage is SEARCH's reply. Error/ErrorCode/RetryAfterMs follow the same additive
// convention as every other result frame (Component 51 §8.1).
type SearchResultMessage struct {
	H                 Header
	Namespace         string
	Hits              []SearchHit
	ResolvedCommitHex string
	Error             *string
	ErrorCode         *ErrorCode
	RetryAfterMs      *int
}

func (m SearchResultMessage) Header() Header { return m.H }

type searchTextArmDto struct {
	Index    string   `json:"index"`
	Query    string   `json:"query"`
	Depth    int      `json:"depth,omitempty"`
	MinScore *float64 `json:"minScore,omitempty"`
	Weight   *float64 `json:"weight,omitempty"`
}

type searchVectorArmDto struct {
	Index    string    `json:"index"`
	Vector   []float64 `json:"vector"`
	Depth    int       `json:"depth,omitempty"`
	MinScore *float64  `json:"minScore,omitempty"`
	Weight   *float64  `json:"weight,omitempty"`
}

type searchDto struct {
	Namespace   string              `json:"namespace"`
	SessionID   string              `json:"sessionId,omitempty"`
	Text        *searchTextArmDto   `json:"text,omitempty"`
	Vector      *searchVectorArmDto `json:"vector,omitempty"`
	Fusion      string              `json:"fusion,omitempty"`
	Limit       int                 `json:"limit"`
	IncludeJSON bool                `json:"includeJson"`
	AtCommitHex string              `json:"atCommitHex,omitempty"`
}

type searchHitDto struct {
	DocID string  `json:"docId"`
	Score float64 `json:"score"`
	JSON  *string `json:"json,omitempty"`
}

type searchResultDto struct {
	Namespace         string         `json:"namespace"`
	Hits              []searchHitDto `json:"hits"`
	ResolvedCommitHex string         `json:"resolvedCommitHex"`
	Error             *string        `json:"error,omitempty"`
	ErrorCode         *ErrorCode     `json:"errorCode,omitempty"`
	RetryAfterMs      *int           `json:"retryAfterMs,omitempty"`
}

func encodeSearchMessage(msg Message) (payloadEnvelope, bool, error) {
	switch m := msg.(type) {
	case SearchMessage:
		d := &searchDto{
			Namespace: m.Namespace, SessionID: m.SessionID, Fusion: m.Fusion, Limit: m.Limit,
			IncludeJSON: m.IncludeJSON, AtCommitHex: m.AtCommitHex,
		}
		if m.Text != nil {
			d.Text = &searchTextArmDto{
				Index: m.Text.Index, Query: m.Text.Query, Depth: m.Text.Depth,
				MinScore: m.Text.MinScore, Weight: m.Text.Weight,
			}
		}
		if m.Vector != nil {
			vec := m.Vector.Vector
			if vec == nil {
				vec = []float64{}
			}
			d.Vector = &searchVectorArmDto{
				Index: m.Vector.Index, Vector: vec, Depth: m.Vector.Depth,
				MinScore: m.Vector.MinScore, Weight: m.Vector.Weight,
			}
		}
		return payloadEnvelope{Kind: "search", Search: d}, true, nil
	case SearchResultMessage:
		hits := make([]searchHitDto, len(m.Hits))
		for i, h := range m.Hits {
			hits[i] = searchHitDto{DocID: h.DocID, Score: h.Score, JSON: h.JSON}
		}
		return payloadEnvelope{Kind: "searchResult", SearchResult: &searchResultDto{
			Namespace: m.Namespace, Hits: hits, ResolvedCommitHex: m.ResolvedCommitHex,
			Error: m.Error, ErrorCode: m.ErrorCode, RetryAfterMs: m.RetryAfterMs,
		}}, true, nil
	default:
		return payloadEnvelope{}, false, nil
	}
}

func decodeSearchMessage(header Header, env payloadEnvelope) (Message, bool, error) {
	switch env.Kind {
	case "search":
		d := env.Search
		if d == nil {
			return nil, true, newDecodeError("missing search body")
		}
		m := SearchMessage{
			H: header, Namespace: d.Namespace, SessionID: d.SessionID, Fusion: d.Fusion, Limit: d.Limit,
			IncludeJSON: d.IncludeJSON, AtCommitHex: d.AtCommitHex,
		}
		if d.Text != nil {
			m.Text = &SearchTextArm{
				Index: d.Text.Index, Query: d.Text.Query, Depth: d.Text.Depth,
				MinScore: d.Text.MinScore, Weight: d.Text.Weight,
			}
		}
		if d.Vector != nil {
			m.Vector = &SearchVectorArm{
				Index: d.Vector.Index, Vector: d.Vector.Vector, Depth: d.Vector.Depth,
				MinScore: d.Vector.MinScore, Weight: d.Vector.Weight,
			}
		}
		return m, true, nil
	case "searchResult":
		d := env.SearchResult
		if d == nil {
			return nil, true, newDecodeError("missing searchResult body")
		}
		hits := make([]SearchHit, len(d.Hits))
		for i, h := range d.Hits {
			hits[i] = SearchHit{DocID: h.DocID, Score: h.Score, JSON: h.JSON}
		}
		return SearchResultMessage{
			H: header, Namespace: d.Namespace, Hits: hits, ResolvedCommitHex: d.ResolvedCommitHex,
			Error: d.Error, ErrorCode: d.ErrorCode, RetryAfterMs: d.RetryAfterMs,
		}, true, nil
	default:
		return nil, false, nil
	}
}
