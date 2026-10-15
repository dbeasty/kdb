# Unreleased

Notes collect here until the next VERSION bump renames this file to that version's notes.

## Performance

**An OR in a WHERE clause is served from indexes instead of a full scan** (Go and Kotlin
servers). Index selection used to consider only top-level AND conjuncts, and an OR was never one
an index could serve, so a filter like

```sql
SELECT kdb_id, _doc FROM docs WHERE _c = ? AND (userId = ? OR collaborators.userId = ?)
```

read every document in the namespace even with an index on each field - the shape of a typical
"owner or collaborator" access-control filter. Now, when every disjunct of an OR can be served
(by `=`, a range, `BETWEEN`, `IN`, a `kdb_id` key, an AND with at least one servable conjunct, or
a nested OR that is itself servable), the OR is served by the union of those lookups. The whole
WHERE is still re-checked on every candidate, so results are unchanged; an OR with any disjunct
that has no index still scans, as before.

- The Go plan label is `index:or(...)` with one entry per disjunct, e.g.
  `index:or(eq(userId)|eq(collaborators.userId))`; Kotlin's EXPLAIN access path is
  `UnionScan(...)`.
- Go costs an OR as the sum of its disjuncts' estimated sizes, so a cheaper single conjunct (such
  as `taskId = ?`) is still preferred, and a small OR is preferred over a large conjunct such as a
  shared collection tag. `IN` lists are now costed too (the sum of their values' counts).
- The union is deduplicated and in document id order. Paged SELECTs (including keyset pages),
  `COUNT(*)`, `UPDATE` and `DELETE` with such a WHERE use it too.
- Measured in the Go test `TestOrUnionAvoidsTheScanAtScale` (in-memory storage, 20,000
  documents): `_c = 'tasks' AND (userId = ? OR collaborators.userId = ?)` went from 26.6 ms
  (20,000 rows examined) to 4.5 ms (1,928 examined); the 3-way form with
  `OR projectId IN (?, ?)` from 29.0 ms to 12.5 ms (5,213 examined). Rows examined now follow the size
  of the union, not of the namespace.
- Kotlin chooses the first servable conjunct rather than the cheapest (it has no costs), so there
  an indexed AND conjunct ahead of the OR is still used instead of the union; and it only
  plans indexes on fields its schema declares indexed.
