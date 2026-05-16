# KDB Component Spec — Layer 5
## Component 13: Index — Full-text
### `dev.kdb.index.fulltext`

**File:** `kdb-spec-layer5-component13-index-fulltext.md`  
**Layer:** 5 — Index Implementations  
**Status:** Implementation-ready  
**Gradle module:** `:kdb-index-fulltext`  
**Depends on:** Layer 0–3, Component 12 (shared `IndexKey` encoding optional), Layer 4a storage blobs

-----

## 1. Purpose

Implements `IndexType.FULLTEXT` — tokenised keyword search with prefix matching and Levenshtein-based fuzzy tolerance over schema `StringType` fields (and optional dedicated full-text indexes created via SQL `CREATE INDEX … FULLTEXT`). Powers SQL `MATCH(column, 'query')` predicates planned by Component 15.

The index maintains an inverted posting list (token → docIds) with per-commit versioning, supports historical reads at `atCommit`, and integrates with Component 8 `IndexStore.search`.

-----

## 2. Dependencies

| Module | Interfaces used |
|---|---|
| `dev.kdb.codec` | `KdbHash`, `KdbUuid`, `KdbValue` |
| `dev.kdb.error` | `KdbException`, `IndexCorruptionException` |
| `dev.kdb.dag` | `CommitDag` |
| `dev.kdb.index` | `IndexStore`, `IndexDescriptor`, `IndexEntry`, `IndexKey`, `IndexType`, `IndexStoreFactory` |
| `dev.kdb.storage` | `StorageAdapter` |

-----

## 3. Public Interface

```kotlin
package dev.kdb.index.fulltext

import dev.kdb.dag.CommitDag
import dev.kdb.index.*
import dev.kdb.storage.StorageAdapter

fun interface FullTextIndexStoreFactory {
    fun create(descriptor: IndexDescriptor, dag: CommitDag, storage: StorageAdapter): IndexStore
}

fun fullTextIndexStoreFactory(dag: CommitDag, storage: StorageAdapter): IndexStoreFactory =
    IndexStoreFactory { descriptor ->
        require(descriptor.type == IndexType.FULLTEXT) {
            "FullTextIndexStoreFactory expected FULLTEXT, got ${descriptor.type}"
        }
        DefaultFullTextIndexStore(descriptor, dag, storage)
    }

class DefaultFullTextIndexStore(
    override val descriptor: IndexDescriptor,
    private val dag: CommitDag,
    private val storage: StorageAdapter,
    private val tokenizer: TextTokenizer = UnicodeTokenizer(),
    private val fuzzyConfig: FuzzyMatchConfig = FuzzyMatchConfig.DEFAULT,
) : IndexStore

/** Pluggable tokenisation (v1: Unicode word boundaries + lowercase normalisation). */
interface TextTokenizer {
    fun tokenize(text: String): List<Token>
}

data class Token(val term: String, val startOffset: Int, val endOffset: Int)

data class FuzzyMatchConfig(
    val maxEditDistance: Int = 2,
    val prefixLength: Int = 3,          // prefix must match exactly
    val maxCandidatesPerTerm: Int = 32,
) {
    companion object { val DEFAULT = FuzzyMatchConfig() }
}

/** Query parse result for [IndexStore.search]. */
data class FullTextQuery(
    val terms: List<String>,            // required terms (AND)
    val phrase: String? = null,       // optional exact phrase in quotes
    val fuzzy: Boolean = true,
)

fun parseFullTextQuery(raw: String): FullTextQuery
```

`DefaultFullTextIndexStore` implements all `IndexStore` methods: `lookup`/`range` throw `IndexTypeMismatchException`; `search` is the primary read path.

-----

## 4. Data Structures

### Inverted index (logical)
`Map<term, PostingList>` where `PostingList` holds `(docId, commitHash, fieldPayloadHash)` sorted by `docId`.

### `PostingList` (on-disk segment)
Append-only list of postings; periodic merge compacts duplicates per `docId` keeping newest ancestral write at HEAD.

### `FullTextIndexManifest`
```kotlin
data class FullTextIndexManifest(
    val indexId: KdbUuid,
    val generation: Long,
    val dictionarySegmentHash: KdbHash,   // term → posting list offset
    val statsSegmentHash: KdbHash?,      // doc count, token count
)
```

### `FullTextQuery`
Parsed from user string: whitespace-separated terms; double-quoted substring → `phrase`; trailing `~` on term disables fuzzy for that term (optional v1).

-----

## 5. Contracts

### `put` / `delete`
On `put`, tokenise the string field value from `IndexEntry.key` (`StringKey` only — other key types throw `IndexTypeMismatchException`). Insert one posting per distinct token. On `delete`, remove all postings for `docId` visible at HEAD (tombstone postings with `commitHash`).

### `search(query, atCommit, limit)`
1. Parse `query` via `parseFullTextQuery`.
2. For each required term: resolve dictionary to posting lists; if fuzzy enabled and term missing, run bounded Levenshtein scan over terms sharing `prefixLength` prefix.
3. Intersect posting lists (AND across terms).
4. If `phrase` set, verify token sequence in original field text (re-fetch from storage or store positions in posting — v1 may re-tokenise stored value hash).
5. Filter by `dag.isAncestor(posting.commitHash, atCommit ?: HEAD)`.
6. Return up to `limit` distinct `docId`s ordered by static score (v1: insertion order / docId tie-break; BM25 optional stretch).

### `bulkLoad` / `rebuild`
Same as Component 12 — full replace.

### `snapshot` / `restoreSnapshot`
Serialise dictionary + generation for browser enlistment eviction (Layer 4b).

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `IndexTypeMismatchException` | `lookup`/`range`/`nearestNeighbours` called. |
| `FullTextQueryException` | Unbalanced quotes or empty query. |
| `IndexCorruptionException` | Dictionary segment cannot be decoded. |

```kotlin
class FullTextQueryException(message: String, val query: String) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.INDEX_CORRUPTION
}
```

-----

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `tokenize_basic` | `"Hello, world!"` | `["hello", "world"]` |
| 2 | `search_singleTerm` | Docs with "alice" and "bob"; query `alice`. | Only alice doc. |
| 3 | `search_andTerms` | Doc1: "foo bar", Doc2: "foo". Query `foo bar`. | Doc1 only. |
| 4 | `search_fuzzyTypo` | Index `example.com`; query `exampl` edit distance 2. | Match doc. |
| 5 | `search_prefixRequired` | Fuzzy term with wrong prefix beyond `prefixLength`. | No match. |
| 6 | `delete_removesFromSearch` | Put then delete doc. `search` same term. | Empty. |
| 7 | `historical_atCommit` | Write at H1, delete at H2. `search(atCommit=H1)`. | Doc still found. |
| 8 | `phrase_match` | Text `"fast embedded storage"`; query `"embedded storage"`. | Match; `"storage embedded"` no match. |
| 9 | `limit_respected` | 100 matching docs; `limit=10`. | ≤10 ids. |
| 10 | `parse_quotedPhrase` | `'"foo bar" baz'`. | `phrase="foo bar"`, terms include baz. |
| 11 | `nonStringKey_putThrows` | `put` with `Int32Key`. | `IndexTypeMismatchException`. |
| 12 | `snapshot_roundTrip` | Index 30 docs; snapshot/restore. | Same search results. |

-----

## 8. Non-Goals

- **Stemming / stop words / language-specific analysers** — v1 uses simple Unicode tokenizer; plugins later.
- **BM25 ranking** — optional improvement; v1 uses intersection + docId order.
- **Highlighting / snippet generation** — Layer 6 / JDBC.
- **Embeddings or semantic search** — Component 14.
- **Cross-field search in one index** — one `IndexDescriptor` per field.

-----

## 9. Implementation Notes

### Tokeniser
`UnicodeTokenizer`: split on `Character.isLetterOrDigit` boundaries, lowercase with default locale, max token length 64 chars.

### Fuzzy search
For each query term not in dictionary: enumerate dictionary terms with same first `prefixLength` chars; compute Levenshtein distance ≤ `maxEditDistance`; cap at `maxCandidatesPerTerm`.

### Storage
Reuse index blob prefix pattern from Component 12. Dictionary may be a sorted SSTable (term → posting list pointer) for scalability.

### Registration
`CompositeIndexStoreFactory` (Component 12) accepts `fullTextIndexStoreFactory` delegate.

### Index creation
`inferIndexType` does not create FULLTEXT indexes automatically — created when schema marks field `fullText = true` (future schema flag) or via `CREATE FULLTEXT INDEX` in Component 15. Until schema flag exists, SQL DDL creates `IndexDescriptor(type=FULLTEXT)`.

### KMP
Pure `commonMain`.

-----

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| `UnicodeTokenizer` + `parseFullTextQuery` | 200 |
| Inverted index + postings | 900 |
| `DefaultFullTextIndexStore` | 700 |
| Fuzzy matcher | 250 |
| Manifest + persistence | 400 |
| Tests | 1,050 |
| **Total** | **~3,500** |
