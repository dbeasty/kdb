# Distributed KDB: self-healing, merging and partial replication — research and gap analysis

Status: research and proposal. Nothing in this document is built.
Basis: `main` at d30efd4, which includes the distributed work (PR #78, Phases 0–10 plus 7.4 and 7.5). Every `file:line` below refers to `go/kdb/` at that commit.
Companion docs: [kdb-distributed-plan.md](kdb-distributed-plan.md) (design, D1–D12) and [kdb-distributed-implementation-plan.md](kdb-distributed-implementation-plan.md) (phases and progress log).

## 0. Summary

This survey covers the research on replicated, self-healing and partially replicated databases. It looks for mechanisms that fit a database whose history is a **content-addressed commit DAG** and whose state is a **Merkle trie**. It then checks the distributed plan against that research.

**What the plan already does as the research recommends:**
- git-style have/want negotiation
- shallow bootstrap
- retention that respects peers
- write-back that uses the projection's own DAG as an outbox

**Where the research points somewhere the plan has not gone:**

1. **Merging a database with no shared history.** It is refused today (`peersync/merge_resolution.go:188-194`). This blocks "merge the state of another database".
2. **Conflict resolution by the application.** Today the resolver can't see where a value came from, and `kdb-service` can't plug one in. This needs to be deterministic resolution chains plus an application *resolver authority*.
3. **Merkle tree-diff anti-entropy.** The trie is already a Merkle tree, but nothing exposes its interior hashes.
4. **Scrub and repair from peers.** A corrupt body is treated as a miss and never refetched.
5. **Lazy fill for edge replicas, using Merkle inclusion proofs.** This reframes the rejected Phase 8: the edge fetches what it lacks and proves it, without holding the full trie.

These become proposed Phases 10.5 and 11–15 (§5).

## 1. KDB today: the facts the analysis depends on

| Fact | Where |
|---|---|
| The tree is a 16-ary Merkle trie over the UUID's 32 hex nibbles. Leaf = `sha256(uuid‖contentHash)`, interior = hash of 16 children with a zero sentinel for absent children. It is canonical: the same set of documents gives the same hash, whatever the history. | `document/document_tree_trie.go:17-63` |
| The public tree API has no interior-node access and no proofs (Size/Contains/HashFor/Walk/With/Without). No wire frame carries subtree hashes. | `document/document_tree.go:42-121` |
| Negotiation is single-shot have/want. The haves are the head plus first-parent ancestors at distances 1, 2, 4, …, at most 32, plus local branch heads. This can overshoot the merge base by up to the divergence length. | `peersync/client.go:319-342` |
| A merge with no common ancestor is refused ("a commit references a parent this node never received"). | `peersync/merge_resolution.go:188-194` |
| Merges resolve by value and origin. Each value is traced to the write that created it; a merge that took the value from a parent passes the trace to that parent. | `peersync/merge_resolution.go:129-170` |
| Resolution options are `Choose`, `LastWrite`, a `Custom` resolver, or report a conflict. | `peersync/merge_resolution.go:296-367` |
| The resolver's input has doc id, op type and existing/incoming/base docs. It has no origin node and no timestamp, and the merge never fills `BaseDoc`. | `transaction/types.go:25-31` |
| `kdb-service` wires only a global policy, never a resolver. | `server/peersync_ingest.go:44` |
| A projection fetches a net-effect delta when its position is an ancestor, otherwise a full `Reset` snapshot. Nothing reads through for documents outside the filter. | `peersync/projection.go:94-106`, `:301-320` |
| A body whose content hash doesn't match is treated as a **miss**, not repaired. | `storage/engine/document_bodies.go:60-77` |
| Repair refuses to drop a frame that would orphan commits and says "run kdb restore instead". It never fetches from a peer. | `integrity/repair.go:59`, `:174` |
| Conflict keys are deterministic: `sha256(kind, parts)`. | `peersync/conflicts.go:113` |
| Peers are static configuration. Failure handling is capped exponential backoff with ±20% jitter. | `replication/replicator.go:39`, `:227` |

## 2. Research survey

Citations are numbered as in §7. Entries marked † were checked against a primary source during this research. The others are well-established references whose DOIs should be spot-checked before anyone relies on them.

### 2.1 Anti-entropy and Merkle trees

- **Merkle trees** [1]. A parent hash commits to its children. Two parties find their differences by descending only into subtrees whose hashes differ: O(d log n) for d differences. KDB's trie is exactly this.
- **Epidemic algorithms** [2] define three ways to spread updates:
  - *rumor mongering*: fast, but may miss nodes
  - *anti-entropy*: slow, but eventually complete
  - *death certificates*: tombstones with a lifetime

  Lesson: push new heads fast, and keep a periodic full comparison as the correctness backstop.
- **Dynamo** [3] combines Merkle-tree anti-entropy per key range, sloppy quorums, *hinted handoff* (a stand-in holds writes for a down node) and *read repair* (fix stale replicas found during a read).
- **Cassandra repair** [4]: tombstone GC grace must be longer than the repair interval, or deleted data comes back. KDB's peer-aware retention floor (plan §3.8) is this same rule.

### 2.2 Set reconciliation

- **Characteristic polynomials** (Minsky, Trachtenberg & Zippel) [5]†: near-optimal communication, O(d). Decoding is compute-heavy and d must be bounded in advance.
- **Invertible Bloom Lookup Tables** (Eppstein et al.) [6]†: one round trip recovers the symmetric difference, with no shared context. A strata estimator sizes the table first.
- **Rateless IBLT** (Yang, Gilad & Alizadeh) [7]†: an unbounded stream of coded symbols, so d need not be estimated. The paper reports 3–4× less communication than a fixed IBLT. A Go implementation exists.
- **Range-based set reconciliation** (Meyer) [8]†: fingerprints over sorted key ranges, splitting ranges that differ. Deployed as Negentropy [9]†. It fits a keyspace ordered by docID.
- **Byzantine eventual consistency** (Kleppmann & Howard) [10]†, [11]†: hash-DAG sync by exchanging heads and a Bloom filter of recently added hashes, then sending what the filter lacks plus its descendants. Hash linking makes it safe against forged history. **This is the closest published design to KDB's commit sync.**

### 2.3 Git

- **Pack negotiation and protocol v2** [12]: want/have, then a pack of everything reachable from the wants and not from the common set. The skipping negotiator spaces the haves exponentially, as KDB does.
- **Partial clone and promisor remotes** [12]†: filters (`blob:none`, `blob:limit`, `sparse:oid`) leave objects out. A missing object from a promisor is *fetched on demand, not treated as corruption*, with several promisors tried in order. This is the model for "fill in the parts we need".
- **Shallow clone** [12]: history cut at graft points. KDB already has this as shallow roots.
- **Commit-graph with changed-path Bloom filters** [12]: lets a reader skip commits that didn't touch a path without loading their trees.

### 2.4 CRDTs, local-first and content-addressed stores

- **CRDTs** [13]: state-based merges that are associative, commutative and idempotent converge with no coordination. KDB's merge commits must meet the same bar, and deterministic merge commits (plan §3.3) do.
- **Local-first software** [14], **JSON CRDT** [15], **Automerge** [16]†: Automerge's changes form a hash DAG, and its sync protocol keeps per-peer sync state and exchanges heads plus Bloom filters. It is a precedent for per-peer reconciliation state.
- **Prolly trees** (Noms/Dolt) [17]† and **Merkle Search Trees** (Auvolat & Taïani) [18]†: history-independent Merkle structures, so diff and merge between any two versions skip identical subtrees. KDB's trie is also history-independent (§1), so it has the same property for free.
- **IPFS Bitswap and Graphsync** [19]: want-lists of content ids, answered by any peer, verified by hash. Graphsync fetches a whole selected subgraph in one request.
- **Hypercore/Dat** [20]: per-peer bitfields of held blocks, sparse download, and each block verified against a signed Merkle root.

### 2.5 Partial replication

- **Bayou** [21]: tentative and committed writes, application merge procedures, *session guarantees* (read-your-writes, monotonic reads, …) and log truncation with a full-state fallback.
- **PRACTI** (Belaramani et al.) [22]†: gives partial replication, arbitrary consistency and topology independence together. It separates *invalidations* (metadata, sent everywhere) from *bodies* (sent only to interested nodes). Causality stays exact without the full data.
- **Cimbiosys** (Ramasubramanian et al.) [23]†: filters are content queries, with *eventual filter consistency*. Knowledge is compact and scoped to the filter it was learned under. Items leaving a filter trigger explicit move-out, and devices relay items they don't store.
- **Coda** [24]: hoarding (prefetch by priority), disconnected operation, and log reintegration on reconnect.
- **CouchDB replication** [25]: per-peer checkpoints that resume, filtered replication, and `_conflicts`, which lets clients see every competing revision and write the winner.
- **ElectricSQL shapes** [26]†, **PowerSync** and **Replicache** [26]: a WHERE-clause partial replica delivered as a log that resumes, with a local queue of pending writes. This is KDB's projection + write-back pattern.
- **Dotted version vectors** [27]†: per-key causality with no false conflicts. Only needed if KDB tracks causality per document as well as through the DAG.

### 2.6 Self-healing and failure detection

- **SWIM** [28]: probe a random peer, probe indirectly on failure, mark it *suspect* before *dead*, and piggyback membership changes. Each node's load stays constant.
- **φ accrual detector** [29]: a continuous suspicion level computed from heartbeat arrival times. It adapts to WAN jitter.
- **Scrubbing** [30] (ZFS scrub, HDFS block scanner, Ceph deep-scrub): re-read everything, check it against its checksum, and repair from a redundant copy. With content addressing, the repair source *need not be trusted*, because the hash is the proof.

### 2.7 Causality

Version vectors [33], interval tree clocks [31]† and hybrid logical clocks [32]†. KDB's DAG heads already are an exact, verifiable version vector, and commits carry causal timestamps (plan §3.2). No change is needed.

## 3. The plan against the research

| # | Mechanism (source) | Plan status | Verdict |
|---|---|---|---|
| 1 | have/want, protocol v2, shallow [12] | §3.4, §3.9, landed | ✓ matches |
| 2 | Tombstone/GC grace ≥ repair interval [2][4] | §3.8 peer-aware retention, landed | ✓ matches |
| 3 | Hinted handoff / outbox [3][24][26] | 7.4 write-back uses the projection DAG as outbox, landed | ✓ matches. Missing: relaying for a third node |
| 4 | Deterministic, coordination-free merge [13][18] | §3.3 deterministic merge commits, landed | ✓ matches, **but only for histories that share a base** |
| 5 | Merge without a shared base (git `--allow-unrelated-histories`; state-based merge [13][18]) | Refused | **Gap. Phase 11** |
| 6 | Application merge procedures [21], client-visible conflicts [25] | `Custom` resolver + queue. No origin, not wired in the service | **Gap. Phase 10.5** |
| 7 | Merkle tree-diff anti-entropy [1][3][17][18] | Not in the plan. The trie supports it | **Gap. Phase 12, the foundation for 8–10** |
| 8 | Scrub and repair by content address [30] | Absent. Mismatch = miss | **Gap. Phase 13** |
| 9 | Promisor lazy fill [12][19] | Phase 8 rejected *holding the full trie*. Lazy fill itself was never built | **Reframe. Phase 14** (§4) |
| 10 | Filter move-in/move-out, delta on filter change [22][23] | Move-out = delete ✓. Widening = full `Reset` | **Partial gap. Phase 14** |
| 11 | Bloom/IBLT DAG reconciliation [6][7][8][10] | Single-shot have/want | Build only if measured (Phase 15) |
| 12 | SWIM + φ accrual [28][29] | §1.10 defers gossip | Optional (Phase 15) |
| 13 | Changed-docID Bloom per commit [12] | Not in the plan | Build only if measured (Phase 15) |
| 14 | Session guarantees [21] | §1.9 names none | Gap, low cost (Phase 15) |
| 15 | Version vectors / ITC / HLC [31][32][33] | Causal timestamps; the DAG is the vector | ✓ no change |

## 4. Phase 8 reframed: prove what you fetch, instead of holding everything

PRACTI's split is metadata everywhere, bodies selectively. Phase 8 priced that split for KDB:
- about 165 B per trie entry plus about 519 B per commit;
- about 138 MB of overhead at 50k documents and 250k commits, whatever the body size.

It never beat a projection (`docs/benchmarks/2026-09-21-partial-clone-model.md`). That conclusion stands.

The research offers a different split that the gate didn't evaluate:
- The edge holds **only its projection**, as today.
- Anything else it needs, it fetches from the source with a **Merkle inclusion proof**: the leaf plus the sibling hashes on the path to the root. It checks the proof against the source's tree hash at a known commit.
- A 16-ary trie with single-entry compression is about ⌈log₁₆ N⌉ levels deep, 5 at 1M documents. A proof is therefore about 5 × 15 × 32 B ≈ **2.4 KB per document**, and the edge holds no trie for data it doesn't store.

What this does and does not buy:
- It gives integrity against corruption and against a peer serving the wrong version, relative to a tree hash the edge accepted.
- It is *not* full Byzantine verification of the source's history. Commit hashes cover full op bodies, so a commit can't be checked without its bodies — the reason commit format v2 was proposed.

That is the same trust level projections have today, plus verified reads beyond the filter.

## 5. Proposed phases

### Phase 10.5 — Resolution policies and application authority (before Phase 11)

Merging an unrelated database (Phase 11) turns every document that differs into a conflict, so this has to come first.

**The rule that shapes it:**
- **Resolution *during* a merge must be deterministic on every node.** The merge commit's hash depends on its result, so nodes that resolve differently never converge.
  - Merge-time rules must be pure functions of (both bodies, base, origin metadata).
  - They must be defined identically everywhere: stored as replicated definitions in `_kdb/meta` (Phase 5), not as per-server flags.
- **Resolution *after* the merge, by an application or a person, is free of that constraint.** The resolution is an ordinary new commit that replicates like any write. So arbitrary business logic belongs there.

**Work items:**

1. **Enrich `DocumentConflict`** (`transaction/types.go:25`):
   - fill `BaseDoc` (empty for unrelated histories)
   - add `ExistingOrigin` / `IncomingOrigin` = {node id, node labels, commit timestamp, commit hash}

   The origin comes from the existing trace (`merge_resolution.go:129-170`), which already follows merges to the node that actually wrote the value, so a merge commit's fixed author never shows up as an origin.
2. **Per-namespace/collection resolution chain**, a replicated definition. Rules are tried in order, and the first that decides wins:
   - `source-priority`: a ranked list of node ids or labels, e.g. `aws-prod > region/* > edge/*`
   - `validity`: prefer the side that passes the collection's schema and validators. Optional deterministic tie-breakers: fewer missing fields, then later
   - `field-merge`: a three-way JSON merge against the base when the two sides changed different fields. It falls through when both changed the same field
   - `last-write` (exists)
   - a terminal rule, one of:
     - `queue`: leave it for the application or an operator
     - `authority` (item 3)

   The chain's hash goes into the sync hello. Peers with different chains refuse to auto-merge rather than diverge.
3. **Resolver authority:** an application service with business logic owns the decision.
   - **Permission:** a new RBAC permission, `ConflictResolve`, scoped per namespace/collection.
   - **Delivery:** pull (a conflict stream, or `GET /v1/ns/{ns}/conflicts`) or push (a signed webhook with retries). Each conflict carries both bodies, the base, both origins and the collection, and is identified by its deterministic conflict key (`peersync/conflicts.go:113`).
   - **One decision-maker:** only the resolver node raises conflicts to the authority: the namespace's home (Phase 9), or a configured node. Other nodes keep the conflict queued. The resolution commit is tagged `kdb:resolve/1 key=<conflictKey>`, and any node that adopts it closes its local entry. A duplicate resolution is a no-op.
   - **While a conflict is pending,** per scope:
     - `hold`: serve the local value, flagged as conflicted, like CouchDB `_conflicts` [25]
     - `provisional`: apply a deterministic fallback rule now and let the authority override it

     An optional timeout makes the fallback final.
   - **Client-side resolution** is the same path. A read with `includeConflicts` returns every candidate. A write with `resolves: <conflictKey>` from a principal holding `ConflictResolve` commits the winner.
   - **Write-back conflicts (Phase 7.4)** go through the same path. Today a rejected write-back puts the document back to the source's state and queues a `write-back` entry. With an authority, the entry is routed to it, and its decision is committed at the source like any other write.
   - **Embedded Go** gets an async `ConflictHandler` callback. The in-merge `Resolver` stays for code that is deterministic, and its contract is documented as such.
4. **Operator bulk actions** (CLI and the deferred admin UI Conflicts tab):
   - take theirs or ours for a peer, namespace or collection, like git `-X theirs`
   - a dry-run preview of what a chain would do to the queued conflicts
5. **Tests:**
   - The same chain on two nodes gives the same merge hash in either sync order.
   - Mismatched chains are refused.
   - An authority resolution closes the entry on a third node that never raised it.
   - A duplicate resolution is a no-op.
   - A principal without `ConflictResolve` is refused.
   - Under `provisional`, the fallback is replaced by the authority's decision.

### Phase 11 — Merge unrelated histories (import another database)

- Add opt-in `AllowUnrelated` on `ResolutionOptions`. When `CommonAncestor` is nil, `resolveDivergedLocked` uses the empty tree as the base, instead of refusing.
- A document present on one side only is adopted. A document present on both sides with different values goes through the Phase 10.5 chain.
- The merge commit stays deterministic under the existing §3.3 rules.
- Surface it as `kdb sync --allow-unrelated` and a control-plane namespace import. It must stay opt-in, because today's refusal also catches a genuinely missing parent.
- Tests: two separately seeded namespaces converge to the same merge hash in either order, and a third node that syncs with both converges too.

### Phase 12 — Tree-diff frames and object fetch

The foundation for Phases 13 and 14.

- **Trie:** add `NodeHash(prefix)`, `Children(prefix)` and `Proof(docID)`, plus a proof verifier.
- **Wire:**
  - `TREE_NODES(treeHash, prefixes[])` returns the 16 child hashes per prefix.
  - `OBJECT_FETCH(contentHashes[] | docIDs + treeHash)` returns bodies, with inclusion proofs when asked by doc id.
  - Both are Go-only frames, recorded as such, like 0x1F–0x22.
- **Anti-entropy:** comparing root hashes at a shared commit is one message. On a mismatch, descend only into the differing prefixes.

### Phase 13 — Scrub and self-repair

- **Scrub pass** in the existing `embed.MaintenanceScheduler`: re-hash stored bodies and materialised tree nodes at a bounded rate.
- **On a mismatch:** quarantine the object, `OBJECT_FETCH` it by content hash from the configured peers, verify it, rewrite it. The peer need not be trusted.
- **Missing commits on the repair path:** `integrity.Repair` fetches them from a peer before falling back to "run kdb restore".
- **Live-tree check:** periodically compare the live tree's root with the declared tree hash of the head. On a mismatch, walk `TREE_NODES` against a peer to find the damaged documents and repair only those.

### Phase 14 — Edge lazy fill

- **Read-through:** a projection's `GetDocument` for an id outside its data does `OBJECT_FETCH` from the source with a proof (§4). The result optionally goes into a bounded LRU *hoard* [24] kept apart from the projection's history, so the projection's DAG stays exactly its filter.
- **Filter widening** becomes a tree-diff against the source instead of a `Reset` [23].
- **Deepen:** a request that extends a shallow root further back.

### Phase 15 — Build only on measurement

- **Bloom-filter negotiation** [10], or RIBLT [7], when a peer has more than N heads or measured overshoot is high. Needs the 100k-commit v1-vs-v2 benchmark first.
- **φ-accrual suspicion** [29] for peer choice and backoff in the replicator. SWIM [28] only if meshes need dynamic membership.
- **A changed-docID Bloom filter per commit** [12], to speed up projection deltas. It fits the on-disk commit-graph plan.
- **Session head tokens** [21]: a client carries the heads it has seen, and a replica that is behind waits or redirects.

## 6. Non-goals

- **Consensus in the data path.** The plan's decision stands: multi-leader by default, with single-home ownership as the strong opt-in.
- **Field-level CRDTs as the storage model.** `field-merge` is a resolution rule over whole-document values, not a new data type.
- **Byzantine verification of a source's history.** It needs commit format v2 (§4).

## 7. References

† = checked against a primary source during this research.

1. R. C. Merkle, "A Digital Signature Based on a Conventional Encryption Function," CRYPTO '87, LNCS 293, 1988. https://doi.org/10.1007/3-540-48184-2_32
2. A. Demers et al., "Epidemic Algorithms for Replicated Database Maintenance," PODC 1987. https://doi.org/10.1145/41840.41841
3. G. DeCandia et al., "Dynamo: Amazon's Highly Available Key-value Store," SOSP 2007. https://doi.org/10.1145/1294261.1294281
4. Apache Cassandra documentation, "Repair." https://cassandra.apache.org/doc/latest/cassandra/managing/operating/repair.html
5. † Y. Minsky, A. Trachtenberg, R. Zippel, "Set Reconciliation with Nearly Optimal Communication Complexity," IEEE Trans. Inf. Theory 49(9):2213–2218, 2003. https://doi.org/10.1109/TIT.2003.815784
6. † D. Eppstein, M. T. Goodrich, F. Uyeda, G. Varghese, "What's the Difference? Efficient Set Reconciliation without Prior Context," SIGCOMM 2011. https://doi.org/10.1145/2018436.2018462
7. † L. Yang, Y. Gilad, M. Alizadeh, "Practical Rateless Set Reconciliation," SIGCOMM 2024. https://doi.org/10.1145/3651890.3672219. Code: https://github.com/yangl1996/riblt
8. † A. Meyer, "Range-Based Set Reconciliation," SRDS 2023. https://arxiv.org/abs/2212.13567
9. † D. Hoyte, Negentropy. https://github.com/hoytech/negentropy. Nostr NIP-77: https://nips.nostr.com/77
10. † M. Kleppmann, H. Howard, "Byzantine Eventual Consistency and the Fundamental Limits of Peer-to-Peer Databases," arXiv:2012.00472, 2020. https://arxiv.org/abs/2012.00472
11. † M. Kleppmann, "Using Bloom filters to efficiently synchronise hash graphs," 2020. https://martin.kleppmann.com/2020/12/02/bloom-filter-hash-graph-sync.html
12. Git documentation: pack protocol https://git-scm.com/docs/gitprotocol-pack; protocol v2 https://git-scm.com/docs/protocol-v2; † partial clone https://git-scm.com/docs/partial-clone; commit-graph https://git-scm.com/docs/git-commit-graph
13. M. Shapiro, N. Preguiça, C. Baquero, M. Zawirski, "Conflict-free Replicated Data Types," SSS 2011, LNCS 6976. https://doi.org/10.1007/978-3-642-24550-3_29
14. M. Kleppmann, A. Wiggins, P. van Hardenberg, M. McGranaghan, "Local-first software: You own your data, in spite of the cloud," Onward! 2019. https://doi.org/10.1145/3359591.3359737
15. M. Kleppmann, A. R. Beresford, "A Conflict-Free Replicated JSON Datatype," IEEE TPDS 28(10), 2017. https://doi.org/10.1109/TPDS.2017.2697382
16. † Automerge sync protocol, `generateSyncMessage`. https://automerge.org/automerge/api-docs/js/functions/generateSyncMessage.html
17. † DoltHub, "Prolly Trees." https://docs.dolthub.com/architecture/storage-engine/prolly-tree
18. † A. Auvolat, F. Taïani, "Merkle Search Trees: Efficient State-Based CRDTs in Open Networks," SRDS 2019. https://doi.org/10.1109/SRDS.2019.00032
19. IPFS Bitswap specification. https://specs.ipfs.tech/bitswap-protocol/
20. M. Ogden, K. McKelvey, M. B. Madsen, "Dat — Distributed Dataset Synchronization and Versioning," 2017. https://github.com/datprotocol/whitepaper
21. D. Terry et al., "Managing Update Conflicts in Bayou, a Weakly Connected Replicated Storage System," SOSP 1995. https://doi.org/10.1145/224056.224070. D. Terry et al., "Session Guarantees for Weakly Consistent Replicated Data," PDIS 1994. K. Petersen et al., "Flexible Update Propagation for Weakly Consistent Replication," SOSP 1997. https://doi.org/10.1145/268998.266711
22. † N. Belaramani et al., "PRACTI Replication," NSDI 2006. https://www.usenix.org/legacy/event/nsdi06/tech/full_papers/belaramani/belaramani.pdf
23. † V. Ramasubramanian et al., "Cimbiosys: A Platform for Content-based Partial Replication," NSDI 2009. https://www.usenix.org/conference/nsdi-09/cimbiosys-platform-content-based-partial-replication
24. J. J. Kistler, M. Satyanarayanan, "Disconnected Operation in the Coda File System," ACM TOCS 10(1), 1992. https://doi.org/10.1145/146941.146942
25. Apache CouchDB, "Replication Protocol" and "Replication and conflict model." https://docs.couchdb.org/en/stable/replication/protocol.html
26. † ElectricSQL, "Shapes." https://electric-sql.com/docs/guides/shapes. PowerSync: https://docs.powersync.com. Replicache: https://doc.replicache.dev
27. † P. S. Almeida, C. Baquero, R. Gonçalves, N. Preguiça, V. Fonte, "Scalable and Accurate Causality Tracking for Eventually Consistent Stores," DAIS 2014. https://doi.org/10.1007/978-3-662-43352-2_6
28. A. Das, I. Gupta, A. Motivala, "SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol," DSN 2002. https://doi.org/10.1109/DSN.2002.1028914
29. N. Hayashibara, X. Défago, R. Yared, T. Katayama, "The φ Accrual Failure Detector," SRDS 2004. https://doi.org/10.1109/RELDIS.2004.1353004
30. OpenZFS `zpool-scrub(8)`: https://openzfs.github.io/openzfs-docs/man/master/8/zpool-scrub.8.html. Ceph scrubbing: https://docs.ceph.com/en/latest/rados/configuration/osd-config-ref/#scrubbing. HDFS architecture: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html
31. † P. S. Almeida, C. Baquero, V. Fonte, "Interval Tree Clocks," OPODIS 2008, LNCS 5401, pp. 259–274. https://doi.org/10.1007/978-3-540-92221-6_18
32. † S. S. Kulkarni, M. Demirbas, D. Madappa, B. Avva, M. Leone, "Logical Physical Clocks," OPODIS 2014. https://doi.org/10.1007/978-3-319-14472-6_2
33. D. S. Parker et al., "Detection of Mutual Inconsistency in Distributed Systems," IEEE TSE SE-9(3), 1983. https://doi.org/10.1109/TSE.1983.236733
