# Kotlin DocumentTree.build: 87 s → 0.8 s at 1M documents

`DocumentTree.build` runs when Kotlin opens a namespace from a Go checkpoint (`GoCheckpoint`),
installs or serves a snapshot (`EmbedSnapshot`, `EmbedPeerBlobs.headTree`), squashes, and when
the file replica stages a tree. At 1M documents it took 87 s. Three things made it slow, and
fixing them also makes every commit's `with`/`without` about 10x cheaper.

## Where the time went

At 100k documents a build cost ~87 µs per document. Nearly all of that was SHA-256.

1. **The digest was pure Kotlin on every platform.** `internal/Sha256.kt` is portable code. It
   also rebuilt its 64-entry round-constant table and copied a padded message on every call. On
   the JVM, `MessageDigest` runs on the CPU's SHA instructions: a 513-byte trie-node preimage takes
   2,040 ns in the portable code and 200 ns in `MessageDigest` (M3 Max, JDK 21). A 49-byte leaf
   preimage takes 245 ns against 36 ns.
2. **The build inserted entries one at a time.** Each insert rehashed every internal node on the
   entry's path. Each split refolded the displaced compressed leaf's whole spine, about 27 hashes
   of 513 bytes. Go has built in bulk for a while (`trieBuild`: sort once, hash each node exactly
   once, the 16 top-level subtrees on separate goroutines).
3. **Spine hashes started from scratch.** A compressed leaf's hash folds up through single-child
   nodes. Each preimage is a constant prefix, the child, then zeros. Go resumes from a
   precomputed state after the prefix's whole blocks (#280); Kotlin rehashed the whole preimage.

## Change

- `sha256Digest` is now `expect`/`actual`. The JVM uses `MessageDigest` with one instance per
  thread. JS and native keep the portable digest, which no longer rebuilds its constants per
  call. This covers every `kdbSha256` caller (content hashes, commit hashes, DAG checks), not
  just the trie.
- `trieBuild` mirrors Go's bulk build. It sorts the ids by unsigned bytes and builds each node
  once: one entry is a compressed leaf at that depth, and several are an internal node over runs
  that share the next nibble. From 4096 entries up, the 16 top-level subtrees go through
  `runEach`, which uses a parallel stream on the JVM and runs sequentially elsewhere. Building
  from a tree's own `entries` view reuses its trie.
- `sha256OnlyChild` is the single-child spine hash. The JVM clones a digest primed with each
  nibble's constant prefix and hashes only the rest. The other platforms hash the whole preimage.

The tree is the same in every case. `DocumentTreeBulkBuildTest` checks that a bulk-built trie
matches an insert-built one node for node (hashes, leaf positions, children) at sizes on both
sides of the parallel threshold, and that later `with`/`without` edits agree. `Sha256PlatformTest`
checks the JVM digests against the portable ones at every length from 0 to 300 bytes plus longer
inputs, for all 16 nibbles of the spine hash, and across 8 threads. The cross-language
`DocumentTreeTrieParityTest` vectors pass unchanged.

## Numbers

Apple M3 Max (16 cores), JDK 21, load ~3.5. Random UUIDs. Best of the timed runs after a warm-up
build. Both columns come from the same benchmark files, run on `main` and on this branch one
after the other. The tree hashes match: `40fb1ef9…` at 100k and `f193571d…` at 1M.

| | main | this change | |
|---|---|---|---|
| `build`, 100k docs | 8,770 ms | 74 ms | ~118x |
| `build`, 1M docs | 87,049 ms | 810 ms | ~107x |
| `with`, per insert, 1M-doc tree | 91.8 µs | 8.7 µs | ~10.5x |
| `without`, per delete, 1M-doc tree | 37.1 µs | 4.6 µs | ~8x |

How the build gain splits, measured at 300k documents:

| | build |
|---|---|
| main | 25.9 s |
| JVM digest, still inserting one at a time | 2.9 s |
| + bulk build, single thread | 1.9 s |
| + parallel top level and midstate spine hashes | 0.22 s |

Midstate alone takes the insert loop from 2.9 s to 2.2 s, which is where most of the `with` gain
beyond the digest comes from.

JS and native get the bulk build and the hoisted constants but keep the portable digest, so
their gain is smaller. They were not measured here.

```
KDB_BENCH=1 ./gradlew :kdb-document:jvmTest --tests '*DocumentTreeBuildBenchmark*' --tests '*DocumentTreeInsertBenchmark*' --rerun
```
