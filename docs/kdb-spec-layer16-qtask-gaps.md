# KDB Component Spec — Layer 16
## Hybrid Search, Query Semantics, Document Predicates, Mutating SQL, Lifecycle, Throughput
### Components 63–73

**Depends on:** Layer 3 (transaction engine, index layer core), Layer 5 (SQL), Layer 12 (Go-native
server, document ops 0x14–0x17), Layer 13 (resource governance / admission).

**Origin:** the "KDB Gaps for qtask" analysis (2026-09-05): what KDB needs before an application
built on MongoDB (qtask) can move onto it. Eleven components; three are hybrid search, three are
silent-wrong-answer bugs, the rest are ordinary document-store features. Every component lands in
**both** trees (Go and Kotlin) with identical observable semantics, pinned by shared golden
fixtures under `go/testdata/golden/search/` that both test suites read.

-----

## 1. Decisions taken

The analysis asked four questions. The answers below are fixed for this layer.

| Question | Decision |
|---|---|
| One server or keep the Go/Kotlin split? | Both trees implement every component. The Go server remains the deployment target; Kotlin stays the reference implementation and must agree with Go on every fixture. |
| How closely does lexical ranking track Mongo `$text`? | KDB's analyzer stems (Porter) and drops English stopwords, like `$text`. Scores are BM25, which `$text` is not, so ranks can differ from Mongo; parity testing is KDB-vs-KDB (Go vs Kotlin), never KDB-vs-Mongo. |
| Does the port wait for Phase 2? | Not this layer's concern; the engine ships every phase. |
| Reserved `id`: fix or document? | **Fix.** Documents round-trip byte-exact: nothing is injected and keys are never reordered. A supplied top-level `id` becomes the document identity (§9.3) and any non-empty string is accepted. |

-----

## 2. Column resolution (Components 69, 70 — both trees)

A *column reference* is a dotted identifier path `a.b.c`. If the first segment names the FROM
table or its alias it is stripped. The remaining path is the JSON path `$.a.b.c` into the
document. Two reserved names resolve outside the document: `kdb_id` (the document id as a
string) and `_doc` (the whole document as JSON).

**Rule 1 — declared schema.** When the namespace has a schema (not `NONE`), the *root* segment
of every column reference must be a schema field or a reserved name. Anything else is a planning
error (`PlanningError` in Go, `SqlPlanningException` in Kotlin) with the message
`unknown column: <name>` — in the projection, `WHERE`, `ORDER BY`, `GROUP BY`, and function
arguments alike. Nothing is ever silently matched, sorted, or grouped by an unresolvable name.

**Rule 2 — schemaless namespace.** When the schema is `NONE` the namespace is a document store:
every column reference resolves dynamically by JSON path and an absent path yields SQL `NULL`.
This is what makes `SELECT kdb_id, _doc FROM t WHERE title = 'alpha'` work against documents
written with `kdb put`, which the analysis showed returning zero rows.

**Path evaluation with implicit array traversal (Mongo semantics).** Evaluating a path yields a
*candidate list*. Walking `$.a.b`: if the value at a segment is an array, the rest of the path is
applied to each element and the results are concatenated in document order. If the final value is
an array, its elements are the candidates (so `tags = 'x'` is membership). A comparison predicate
over a path is true when **any** candidate satisfies it. `ORDER BY` and projection use the
**first** candidate; a path with no candidates is `NULL`.

**Comparison rules (no panics, no exceptions).** Same-type values compare naturally; integers and
doubles compare numerically; booleans compare `false < true`. Mismatched types are *incomparable*:
`=` is false, `<>` is true, every ordering operator is false. `NULL` compared with anything is
unknown → the predicate is false (including `NULL = NULL`); use `IS NULL`. For sorting, the
comparator is total: `NULL` sorts before every value (so first in `ASC`, last in `DESC`), and
`NULL` vs `NULL` is 0.

-----

## 3. Component 69 — Query Semantics Hardening (Phase 0)

1. Unknown column in `WHERE` → planning error (Rule 1). Was: matches nothing (`=`) or everything (`<>`).
2. Unknown column in `ORDER BY` → planning error. Was: sort silently ignored (Go) / inconsistent comparator (Kotlin).
3. `DISTINCT` is applied by the executor. Pipeline order for a non-aggregate `SELECT`:
   resolve ids → materialize → **sort** → **project** → **distinct** (first occurrence wins) →
   **offset/limit**. `LIMIT` therefore bounds distinct rows, not pre-dedup rows.
4. A parser that panics on malformed input is a bug. `SELECT FROM t` and `WHERE stringcol = 5`
   must return a parse/planning error or an empty result, never crash the connection goroutine.
5. **Conformance suite** — a test per clause the parser accepts, asserting it either takes
   effect or errors: projection, `WHERE`, `ORDER BY` (both directions, NULL placement), `DISTINCT`,
   `LIMIT`/`OFFSET`, `GROUP BY`, each aggregate, `LIKE`/`IN`/`BETWEEN`, `MATCH`, `SIMILARITY`,
   `FUSE`. Written against an in-memory runtime in both trees with the same corpus.

-----

## 4. Component 70 — Predicate Coverage over Documents

Syntax (both parsers):

```
expr    := or
or      := and { OR and }
and     := not { AND not }
not     := [NOT] cmp
cmp     := operand ( ( = | <> | != | < | <= | > | >= ) operand
                   | [NOT] LIKE string | [NOT] ILIKE string
                   | [NOT] IN '(' operand {',' operand} ')'
                   | [NOT] BETWEEN operand AND operand
                   | IS [NOT] NULL )?
        | func '(' args ')' | '(' expr ')'
operand := literal | ? | column-path | func '(' args ')'
```

- `LIKE` is case-sensitive; `%` matches any run, `_` one character; every other character is
  literal (regex metacharacters are escaped). `ILIKE` is the case-insensitive form.
- `IN (…)` takes literals or parameters. `BETWEEN a AND b` is inclusive.
- Boolean functions usable in `WHERE`:
  - `ARRAY_CONTAINS(path, v1[, v2, …])` — true iff the value at `path` is an array containing
    **every** listed value (deep JSON equality; numbers compare numerically). One argument is
    membership; several is the superset test qtask uses for `tags`.
  - `ARRAY_CONTAINS_ANY(path, v1[, v2, …])` — true iff it contains at least one.
  - `ARRAY_LENGTH(path)` — integer, or `NULL` when the value is not an array (usable as an operand).
- A bare column reference as a predicate (`WHERE flag`) is true iff a candidate is boolean `true`.
- Nested and array paths are ordinary column references (§2): `collaborators.userId = 'u1'`,
  `steps.text LIKE '%deploy%'`, `projectIds = 'p1'`.

Kotlin already parses `LIKE`/`IN`/`BETWEEN`; it gains `NOT` forms, `ILIKE`, regex escaping, and
case-sensitive `LIKE` (previously ignore-case). Go gains all of it.

-----

## 5. Component 71 — Mutating SQL and Aggregates

```
UPDATE t [alias] SET path = expr {, path = expr} [WHERE expr]
DELETE FROM t [alias] [WHERE expr]
```

- `SET _doc = '<json>'` supplies a whole-document body; any other target is a JSON path set
  (`SET status = 'done'`, `SET meta.reviewed = true`). `expr` is a literal, parameter, or column
  reference evaluated against the pre-update document. After assignment the document is
  validated against the schema; a violation fails the statement.
- **Known limitation, both trees, deliberate:** a `WriteOp` body is applied by the transaction
  engine as a shallow root-level *merge* against the existing document, so neither `SET _doc` nor
  wire `UPSERT` can remove a top-level key — keys absent from the new body survive. This is
  pre-existing engine behaviour, not new here; true replacement needs a replace-capable document
  op, which is a wire-format change deferred to its own layer. Both trees must merge identically
  and both carry a test pinning it.
- **Three-valued logic is not implemented.** `NOT` is two-valued over the §2 comparison rules, so
  `NOT BETWEEN` / `NOT IN` / `NOT LIKE` over a NULL or absent path return the row (the inner
  comparison is false, `NOT` negates it). Standard SQL would exclude it. Both trees behave this
  way and both carry a test pinning it.
- **Score widening.** A float32 score becomes a double through the shortest 32-bit round-trip
  (`0.9f` → `0.9`, never `0.8999999761581421`), so Go and Kotlin print identical score cells.
- Both lower to `WriteOp`/`DeleteOp` and follow the same commit path as `INSERT` (Go: appended to
  the session's pending transaction and committed by `TxCommit`, or committed immediately when the
  session is in autocommit; Kotlin: `HybridQueryEngine` DML path). `rowsAffected` is the number of
  documents written or deleted.
- Aggregates: `COUNT(*)`, `COUNT(col)`, `SUM`, `AVG`, `MIN`, `MAX`, with `GROUP BY col {, col}`.
  Group-key columns are projectable (Kotlin previously returned `NULL` for them — bug). Without
  `ORDER BY`, groups are emitted in ascending group-key order (total comparator from §2), so both
  trees produce the same row order. `SUM`/`AVG` over integers-only yield integers for `SUM` and
  doubles for `AVG`; any double input makes `SUM` a double. `MIN`/`MAX` ignore `NULL`. An
  aggregate over zero rows is `NULL` (except `COUNT`, which is `0`).

-----

## 6. Component 63 — Scored Full-Text Index

### 6.1 Analyzer (identical in both trees)

1. Split on every code point that is not a Unicode letter or digit.
2. Lowercase (`String.lowercase()` / `strings.ToLower` — simple case mapping).
3. Drop tokens longer than 64 code points.
4. Drop stopwords (§6.2).
5. Porter-stem each remaining token (original 1980 algorithm, ASCII letters only; a token with
   any non-ASCII letter is left unstemmed).
6. Record 0-based positions **after** stopword removal (so phrase matching ignores stopwords).

Fixtures: `go/testdata/golden/search/analyzer_vectors.json` (`[{"text": …, "tokens": […]}]`)
and `go/testdata/golden/search/porter_vectors.txt` (`word stem` per line, ≥ 200 entries
including the classic Porter test cases: caresses→caress, ponies→poni, ties→ti, caress→caress,
cats→cat, feed→feed, agreed→agre, plastered→plaster, motoring→motor, sing→sing, conflated→conflat,
troubled→troubl, sized→size, hopping→hop, tanned→tan, falling→fall, hissing→hiss, fizzed→fizz,
failing→fail, filing→file, happy→happi, sky→sky, relational→relat, conditional→condit,
rational→ration, valenci→valenc, hesitanci→hesit, digitizer→digit, conformabli→conform,
radicalli→radic, differentli→differ, vileli→vile, analogousli→analog, vietnamization→vietnam,
predication→predic, operator→oper, feudalism→feudal, decisiveness→decis, hopefulness→hope,
callousness→callous, formaliti→formal, sensitiviti→sensit, sensibiliti→sensibl, triplicate→triplic,
formative→form, formalize→formal, electriciti→electr, electrical→electr, hopeful→hope,
goodness→good, revival→reviv, allowance→allow, inference→infer, airliner→airlin, gyroscopic→gyroscop,
adjustable→adjust, defensible→defens, irritant→irrit, replacement→replac, adjustment→adjust,
dependent→depend, adoption→adopt, homologou→homolog, communism→commun, activate→activ,
angulariti→angular, homologous→homolog, effective→effect, bowdlerize→bowdler, probate→probat,
rate→rate, cease→ceas, controll→control, roll→roll).

### 6.2 Stopwords

Exactly this list (the classic Lucene/Snowball English set, 33 words), matched after lowercasing:
`a an and are as at be but by for if in into is it no not of on or such that the their then
there these they this to was will with`.

### 6.3 Index shape

A full-text index covers **one or more fields**, each a JSON path with a weight:

```
CREATE INDEX tasks_text ON tasks (title WEIGHT 3, description, tags WEIGHT 2, steps.text) USING FULLTEXT
```

`WEIGHT` defaults to 1. A field whose value is an array (or an array reached by implicit
traversal) contributes every string element, concatenated with a position gap of 1. Non-string,
non-array values contribute nothing. The descriptor's `fieldName` is the first field (for the
registry key); `fields` carries the whole list; weights live in the descriptor options.

Per field `f` the index keeps: postings `term → [(docId, tf, positions)]`, document length
`len_{f,d}` (token count), and `avglen_f`. Globally: `N` (documents with at least one indexed
token in any field) and `n_t` (documents containing term `t` in any field).

### 6.4 Scoring — BM25F-lite

For query terms `q` (analyzed like documents; duplicates collapsed):

```
idf(t)        = ln( 1 + (N − n_t + 0.5) / (n_t + 0.5) )
tfnorm(t,f,d) = tf · (k1 + 1) / ( tf + k1 · (1 − b + b · len_{f,d} / avglen_f) )
score(d)      = Σ_{t∈q} Σ_f  w_f · idf(t) · tfnorm(t,f,d)         k1 = 1.2, b = 0.75
```

A document is a hit if it contains at least one query term (OR semantics). A quoted phrase
`"deploy staging"` restricts hits to documents where the analyzed phrase occurs contiguously in
some field; phrase terms also score. Results are sorted by score descending, ties by document id
ascending (compare the canonical 36-character lowercase UUID string). Scores are `float32` on the
API (`RankedResult.score`). The old Kotlin fuzzy (Levenshtein) matching stays available only as an
explicit opt-in and is **off** by default; Go does not implement it.

Fixture: `go/testdata/golden/search/bm25_corpus.json` — a corpus of ~12 documents with fixed
UUIDs and 2–3 fields, an index definition with weights, and ≥ 8 queries (incl. a phrase and a
stopword-only query) with expected `[docId, score]` lists. Both trees assert the exact rank order
and scores within a relative tolerance of 1e-4. One query's expected score must be derived by
hand in a test comment, not copied from the implementation.

### 6.5 Persistence and rebuild

Both trees persist a full-text index as a versioned snapshot written atomically (temp file +
rename in Go under `<dataRoot>/<catalog>/index/<indexId>/`; via the storage adapter's blob store
in Kotlin), containing a manifest (`formatVersion`, descriptor, `headCommitHex`, `N`, per-field
`avglen`) plus the postings, lengths, and per-document tombstones. It is flushed on close and
after every `flushEvery` commits (default 64). On open, a missing or stale snapshot
(`headCommitHex` ≠ DAG head) triggers **rebuild-from-scan** at head. Memory runtimes never persist.

-----

## 7. Component 64 — Vector Index

- Metrics: `cosine` (score = dot / (‖a‖‖b‖), 0 when either norm is 0), `l2` (score =
  1 / (1 + ‖a − b‖)), `inner_product` (score = dot). Higher is always better.
- **Exact search** (brute force) is the oracle: results sorted by score desc, ties by docId asc.
  Both trees must return identical exact results on the fixture.
- **HNSW** (`m`, `ef_construction`, `ef_search`; defaults 16 / 200 / 64) is used when the index
  holds more than `exactThreshold` (default 1 000) live vectors, otherwise exact search is used.
  Node level = `floor(−ln(u) · 1/ln(m))` where `u ∈ (0,1]` is derived from the first 8 bytes of
  `sha256(docId)`, so levels do not depend on insertion order. HNSW is tested for recall ≥ 0.95
  at k = 10 against the exact oracle on 2 000 random 32-d vectors, in both trees.
- A vector is read from the indexed JSON path as an array of numbers; wrong length →
  `VectorDimensionMismatch` (schema-violation class) and the **commit is rejected** (§10).
- Deletes are tombstones `(docId, commitHash)`: a read `atCommit` earlier than the tombstone still
  sees the vector; head reads do not.
- Persistence/rebuild as §6.5. The graph is not persisted; it is rebuilt from the stored vectors
  on open.

```
CREATE INDEX tasks_vec ON tasks (embedding) USING VECTOR WITH (dimensions = 768, metric = 'cosine', m = 16, ef_construction = 200, ef_search = 64)
```

Fixture: `go/testdata/golden/search/vector_corpus.json` — 20 documents with 8-d vectors, queries
per metric, expected exact `[docId, score]` (tolerance 1e-5).

-----

## 8. Component 65 — Rank Fusion

Pure functions over per-arm ranked lists (`docId`, `score`), each list already sorted by score
desc / docId asc.

```
FusionArm  { results, weight = 1.0, depth = 0 (0 = all), minScore = -inf }
FusionMode { RRF (default, k = 60), WEIGHTED_SUM }
```

1. Per arm: drop results with `score < minScore`, then truncate to `depth`.
2. RRF: `score(d) = Σ_arms weight_a / (k + rank_a(d))`, `rank` 1-based, absent arms contribute 0.
3. Weighted sum: per arm min-max normalise scores over the (filtered, truncated) list to [0, 1]
   (all equal → 1.0), then `score(d) = Σ weight_a · norm_a(d)`, absent arms contribute 0.
4. Output sorted by fused score desc, ties by docId asc, truncated to `limit`.

Fixture: `go/testdata/golden/search/fusion_cases.json` — ≥ 10 cases covering both modes, depth,
minScore, weights, disjoint/overlapping arms, and exact ties. Expected scores to 1e-9.

-----

## 9. SQL surface, planner, DDL (Component 66) and lifecycle (Component 72)

### 9.1 Search in SQL

```
SELECT kdb_id, _doc, MATCH(tasks_text, 'deploy staging') AS score
  FROM tasks WHERE MATCH(tasks_text, 'deploy staging') ORDER BY score DESC LIMIT 20

SELECT kdb_id, SIMILARITY(embedding, ?) AS score FROM tasks ORDER BY score DESC LIMIT 10

SELECT kdb_id, FUSE(MATCH(tasks_text, ?), SIMILARITY(embedding, ?), 'rrf') AS score
  FROM tasks ORDER BY score DESC LIMIT 20
```

- `MATCH(index_or_field, query)` — first argument is a FULLTEXT index name or an indexed field
  (the index whose first field it is). As a predicate it is true for hits; as a projection it is
  the BM25 score (`0` for non-hits). Requires a FULLTEXT index: none → planning error
  `no FULLTEXT index for <name>`. The old Kotlin substring fallback is removed.
- `SIMILARITY(field, vector)` — vector is a parameter of the new type **vector** (JSON array of
  numbers on the wire; `ParamVector` / `SqlParameter.VectorParam`) or a literal `[0.1, 0.2, …]`.
  Requires a VECTOR index on the field. Projection value is the metric score.
- `FUSE(arm1, arm2[, 'rrf' | 'weighted'])` — arms are `MATCH`/`SIMILARITY` calls. Score per §8.
- A query whose `ORDER BY` is a score expression with `LIMIT n [OFFSET m]` fetches `depth =
  min(1000, max(50, 4·(n+m)))` candidates per arm; without `LIMIT`, every hit.
- Score columns are `DOUBLE` cells (Go `CellDouble`, Kotlin `SqlCell.DoubleVal`) and cross the
  wire as `strconv.FormatFloat(v, 'g', -1, 64)` / Kotlin `Double.toString()`-equivalent shortest
  round-trip formatting.

### 9.2 DDL

```
CREATE [UNIQUE] INDEX name ON t (field) [USING HASH | BTREE]
CREATE INDEX name ON t (f1 [WEIGHT n] {, f2 [WEIGHT n]}) USING FULLTEXT
CREATE INDEX name ON t (f) USING VECTOR WITH (dimensions = n [, metric = '…', m = n, ef_construction = n, ef_search = n])
DROP INDEX name ON t
CREATE TABLE t (col type [NOT NULL] [UNIQUE] {, …} {, UNIQUE (a, b {, c})})
```

FULLTEXT and VECTOR indexes are allowed on schemaless namespaces (fields are JSON paths).
HASH/BTREE require declared fields. Index descriptors are persisted in the runtime's index
catalog (Go: `<dataRoot>/<catalog>/index/catalog.json`; Kotlin: storage blob) so they survive
restart; hash/btree indexes are rebuilt from the schema on open, fulltext/vector from their
snapshots or by scan.

### 9.3 Planner index selection

For the top-level `AND` conjuncts of `WHERE`, in order: `path = v` with a HASH or BTREE index →
exact lookup; `path < | <= | > | >= v`, `BETWEEN` with a BTREE index → range with **correct
strictness** (Kotlin previously collapsed `>` into `>=`); `path IN (…)` → per-value lookups
unioned; `MATCH` → full-text scan. Every remaining conjunct is the residual filter. `EXPLAIN`
(Kotlin) and `QueryResult.Plan` (Go, new field) name the chosen access path so tests can assert
an index was used.

### 9.4 Document identity and round-trip (Component 72)

- Documents are stored and returned **byte-exact**. No key is injected, no key is reordered, on
  any write path (`kdb put`, embed `PutJSONDocument`/`putJson`, wire `UPSERT`, SQL `INSERT`).
- A top-level `id` in the body is honoured as identity: a UUID string parses directly; any other
  non-empty string `s` maps to the derived id
  `uuid8(sha256(KDB_DOC_ID_NAMESPACE ‖ utf8(s)))` — the first 16 bytes of the digest with the
  version nibble set to 8 and the variant bits to `10`, where
  `KDB_DOC_ID_NAMESPACE = 6f5b9a1c-2d3e-4f70-8a9b-1c2d3e4f5a6b` (its 16 raw bytes). An `id`
  that is not a string, or an empty string, is rejected. Fixture:
  `go/testdata/golden/search/derived_id_vectors.json` (`[{"id": …, "uuid": …}]`).
- When no `id` is supplied the engine mints a random UUID and reports it (`PutResult.DocID`,
  `kdb_id`); the body is still stored untouched.
- README's "exactly as provided" promise becomes true; the README documents `id`.

### 9.5 Document expiry (Component 72)

```
NamespacePolicy.DocumentExpiry / documentExpiry:
  { fieldPath: string, graceMillis: int64 = 0, sweepIntervalMillis: int64 = 60_000 }
```

A document is **expired** when the value at `$.<fieldPath>` is a timestamp `≤ now − grace`.
Accepted forms: an RFC 3339 string, or a number of epoch milliseconds. Any other value means
"never expires".

- Reads honour expiry between sweeps: `GetDocument`, SQL scans, index lookups, and search skip
  expired documents at **head**. Historical reads (`atCommit`) do not apply expiry.
- The sweeper runs every `sweepIntervalMillis` on the server runtime, scans head, and commits
  `DeleteOp`s in batches of at most 500 per commit with the runtime's system principal under the
  LAST_WRITE engine, message `expiry sweep`. It is a goroutine/coroutine owned by the runtime,
  stopped on close, and never runs on a read-only runtime.
- `kdb-service` flags (both): `--expire-field <path>`, `--expire-grace <duration>`,
  `--expire-interval <duration>`.

### 9.6 Compound unique (Component 72)

`UNIQUE (a, b)` table constraints (schema `UniqueConstraints` / `uniqueConstraints`, already on
the schema body as wire field 5). Enforcement at commit, in the transaction engine, through a
`UniqueKeyRegistry` keyed by `(namespace, fields tuple, canonical value tuple)`; the single-field
`UNIQUE` flag is the 1-tuple case of the same mechanism. A document in which **any** part is
absent or JSON `null` claims nothing. Values are canonical JSON (so `1` and `1.0` collide; strings
are byte-compared). Violation → `UNIQUE_VIOLATION` error class naming the constraint fields and
both document ids. Kotlin gains the registry (it previously enforced only inside index stores,
post-commit); Go generalises its existing one.

-----

## 10. Component 67 — Index Maintenance on the Commit Path

- Before the DAG append, the engine derives **index hints** from the transaction's ops against
  every registered index: `WriteOp` → `PUT` with the extracted key / analyzed text / vector;
  `DeleteOp` → `DELETE`. Extraction errors that are the document's fault (wrong vector dimension)
  reject the commit as a schema error; nothing is half-applied.
- After the append (still under the write gate) hints are applied to the stores, tagged with the
  new commit hash so `atCommit` ancestry filtering keeps working, and attached to the commit's
  `IndexHints` for replication (`DeltaCommitPayload.indexHints`, already on the wire).
- Rebuild-from-scan on open when a store's snapshot is missing or stale (§6.5).

-----

## 11. Component 68 — Search over the Wire

New message pair, both trees, JSON body like every other frame:

| Code | Name | Body |
|---|---|---|
| `0x1D` | `SEARCH` | `namespace`, `sessionId?`, `text?: {index, query, depth?, minScore?, weight?}`, `vector?: {index, vector: [number], depth?, minScore?, weight?}`, `fusion?: "rrf" \| "weighted"`, `limit`, `includeJson: bool`, `atCommitHex?` |
| `0x1E` | `SEARCH_RESULT` | `namespace`, `hits: [{docId, score, json?}]`, `resolvedCommitHex`, `error?`, `errorCode?`, `retryAfterMs?` |

`index` is an index name or the first field of an index. With one arm present the result is
that arm's ranking; with both, the fused ranking (§8). Authorization is `DocumentRead` on the
namespace, sessionless like `DOCUMENT_GET`. The Go client SDK gains `Search(ctx, SearchRequest)`.

-----

## 12. Component 73 — Write and Connection Throughput

- **Per-namespace write gates.** Kotlin's `WriteCoordinator` becomes one coordinator per
  namespace inside a runtime; Go's gate is already per runtime (= per namespace) and gets a test
  proving two namespaces in a `ServerRuntimeRegistry` commit concurrently.
- **Concurrent frame handling per connection (Go).** Each decoded frame is handled on its own
  goroutine; replies are serialised through a send mutex; frames that name a session are ordered
  per session by a per-session mutex (`SqlExec`, `TxCommit`, `TxRollback`, lock ops); sessionless
  frames (`DocumentGet`, `Upsert`, `Search`) run freely. No frame is dispatched before the
  handshake reply is sent. In-flight frames per connection are bounded (default 64); beyond that
  the reader blocks (backpressure). On disconnect the handler waits for in-flight work before
  releasing sessions and leases. Kotlin already dispatches concurrently; both trees get a test
  that a slow scan on one session does not delay a `DocumentGet` on the same connection.

-----

## 13. Sequencing

Phase 0: 69. Phase 1: 63, 64, 65 (exact vector search first, HNSW behind the oracle).
Phase 2: 70, 71, 72. Phase 3: 66, 67, 73. Phase 4: 68. Phase 5: HNSW and snapshot persistence
measured against the Phase-1 oracle. All of it lands in this layer, in both trees, with the shared
fixtures as the parity gate.
