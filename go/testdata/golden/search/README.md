# Layer 16 search fixtures

Shared golden fixtures for the hybrid-search components (Layer 16, components 63–65). Both
trees read these same files: Go from `go/kdb/index/**`, Kotlin from its index modules. They are
the parity gate — Go and Kotlin must agree on every value here, and neither tree may edit a
fixture to make its own tests pass.

Document ids are fixed, human-readable UUIDs (`00000000-0000-4000-8000-0000000000NN`) so the
files stay stable and diffs stay readable.

Regenerate with:

    cd go && go run ./kdb/index/internal/fixturegen -out testdata/golden/search

The generator is deterministic. Expected values are produced by the Go implementation, but the
BM25 `database` query and the `rrf_two_arms_overlap` fusion case were derived by hand first,
and those derivations are written out in the test comments that assert them
(`go/kdb/index/fulltext/store_test.go` `TestBM25ScoreMatchesHandComputation`,
`go/kdb/index/fusion/golden_test.go` `TestRRFScoreMatchesHandComputation`). Re-verify them by
hand after any deliberate scoring change; do not regenerate a fixture to paper over a
regression.

`derived_id_vectors.json` belongs to component 72 (§9.3) and is documented by that work, not
here.

## analyzer_vectors.json

Analyzer pipeline (spec §6.1): tokenize on non-letter/digit code points, lowercase, drop
tokens over 64 code points, drop stopwords, Porter-stem.

```json
[ { "text": "Deploy staging environment", "tokens": ["deploi", "stage", "environ"] } ]
```

`tokens` is the analyzed term list in order; positions are not in this file (they are implicit
in the order, since positions are assigned after stopword removal). Covers Unicode, digits,
punctuation, the length cap, the empty string, and a stopword-only text (empty `tokens`).

## porter_vectors.txt

One `word stem` pair per line, space-separated, 633 lines. Includes every pair the spec lists
(the classic cases from Porter's 1980 paper) plus wider English coverage. The stemmer is the
original 1980 algorithm over ASCII letters; a token of two letters or fewer, or one containing
a non-ASCII letter, is returned unchanged.

## bm25_corpus.json

Corpus, index definition and expected rankings for BM25F-lite (§6.3, §6.4).

```json
{
  "index":     { "fields": [ { "path": "title", "weight": 3 }, … ] },
  "documents": [ { "id": "<uuid>", "json": "<the document's JSON text>" } ],
  "queries":   [ { "query": "deploy staging", "expected": [ ["<uuid>", 9.196583], … ] } ]
}
```

12 documents over three fields (`title` weight 3, `description` weight 1, `tags` weight 2 —
`tags` is an array field, so every string element is indexed). 12 queries including a quoted
phrase, a stopword-only query (expected empty), a query matching nothing, a mixed
phrase-plus-term query, and a case/punctuation variant.

`expected` is the full ranking: score descending, ties by document id ascending, compared
within a relative tolerance of 1e-4. Scoring is
`idf(t) = ln(1 + (N − n_t + 0.5)/(n_t + 0.5))` and
`tfnorm = tf·(k1+1)/(tf + k1·(1 − b + b·len/avglen))` with k1 = 1.2, b = 0.75, summed over
query terms and fields with the field weight. Documents are hits under OR semantics; a quoted
phrase additionally requires the analyzed terms to be contiguous within one field.

## vector_corpus.json

Vectors and expected exact (brute-force) rankings per metric (§7).

```json
{
  "dimensions": 8,
  "documents":  [ { "id": "<uuid>", "vector": [0.42, -0.17, …] } ],
  "queries":    [ { "metric": "cosine", "vector": [...], "k": 5,
                    "expected": [ ["<uuid>", 0.8712], … ] } ]
}
```

20 documents of 8 dimensions, with three deliberate edge cases: the last document is the zero
vector (cosine against it is 0, never NaN), and two documents share an identical vector so
every metric ties them and the ordering falls to document id. Nine queries — three per metric
(`cosine`, `l2`, `inner_product`), one of which retrieves the whole corpus (k = 20) and one of
which is an exact copy of the tied vector.

`expected` is the exact-search result, compared within 1e-5. Metrics (higher is always better):
cosine = dot/(‖a‖‖b‖) with 0 when either norm is 0; l2 = 1/(1+‖a−b‖); inner_product = dot.
HNSW is not pinned by fixture — it is tested for recall ≥ 0.95 at k = 10 against this exact
oracle in each tree.

## fusion_cases.json

Rank-fusion cases (§8).

```json
{ "cases": [ {
    "name": "rrf_two_arms_overlap",
    "mode": "rrf",                                  // or "weighted"
    "limit": 0,                                     // 0 = no limit
    "arms": [ { "weight": 1, "depth": 0,            // depth 0 = keep all
                "minScore": null,                   // null = no floor
                "results": [ ["<uuid>", 1.0], … ] } ],
    "expected": [ ["<uuid>", 0.032522473], … ]
} ] }
```

16 cases covering both modes, weights, `depth`, `minScore`, disjoint and overlapping arms,
empty arms, single arms, negative scores, and exact ties resolved by document id. Arm
`results` are already sorted score descending / id ascending, as the spec requires of an arm.

Per arm: drop results below `minScore`, then truncate to `depth`. RRF sums
`weight / (60 + rank)` with 1-based ranks; weighted sum min-max normalises each arm's
filtered, truncated list to [0, 1] (all-equal → 1.0) and sums `weight · normalised`. Absent
arms contribute 0. Output is score descending, ties by document id ascending, truncated to
`limit`. Scores are compared to 1e-9, which both trees achieve by accumulating in float64 and
rounding to float32 once at the end.
