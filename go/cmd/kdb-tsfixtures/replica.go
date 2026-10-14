package main

// Fixtures for @kdb/replica (docs/kdb-client-replica-plan.md): commit payloads and hashes, document
// tree roots, and a commit chain with a merge, all from the Go code that produces them on a node.
// The replica verifies every commit hash and every tree hash it is sent, so a TS encoder or trie
// that is 99% right would refuse real data: these vectors are what prove the last 1%.

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

type replicaOpFixture struct {
	Kind        string `json:"kind"`
	DocID       string `json:"docId,omitempty"`
	JSON        string `json:"json,omitempty"`
	Path        string `json:"path,omitempty"`
	BlobHash    string `json:"blobHash,omitempty"`
	MigrationID string `json:"migrationId,omitempty"`
	Payload     string `json:"payload,omitempty"`
}

type replicaCommitFixture struct {
	Name            string             `json:"name"`
	PayloadHex      string             `json:"payloadHex"`
	Hash            string             `json:"hash"`
	Parents         []string           `json:"parents"`
	Namespace       string             `json:"namespace"`
	TransactionID   string             `json:"transactionId"`
	TimestampMicros string             `json:"timestampMicros"`
	AuthorNodeID    string             `json:"authorNodeId"`
	Ops             []replicaOpFixture `json:"ops"`
	TreeHash        string             `json:"treeHash"`
	SchemaHash      string             `json:"schemaHash,omitempty"`
	Message         string             `json:"message"`
}

type treeEntryFixture struct {
	ID          string `json:"id"`
	ContentHash string `json:"contentHash"` // "" deletes, in an edit sequence
}

type treeFixture struct {
	Name    string             `json:"name"`
	Entries []treeEntryFixture `json:"entries"`
	Root    string             `json:"root"`
}

type treeEditFixture struct {
	Name  string `json:"name"`
	Steps []struct {
		Edits []treeEntryFixture `json:"edits"`
		Root  string             `json:"root"`
	} `json:"steps"`
}

type chainStepFixture struct {
	From    string   `json:"from"` // "" = empty namespace
	To      string   `json:"to"`
	Commits []string `json:"commits"` // the range from..to, in no particular order
	// Docs is the document set at To: what a replica fast-forwarded there must hold.
	Docs map[string]string `json:"docs"`
}

type chainFixture struct {
	Namespace string                 `json:"namespace"`
	Commits   []replicaCommitFixture `json:"commits"`
	Steps     []chainStepFixture     `json:"steps"`
}

func writeReplicaFixtures(dir string) {
	writeJSON(filepath.Join(dir, "commits.json"), buildReplicaCommits())
	writeJSON(filepath.Join(dir, "trees.json"), buildTrees())
	writeJSON(filepath.Join(dir, "tree-edits.json"), buildTreeEdits())
	writeJSON(filepath.Join(dir, "chain.json"), buildChain())
}

func fixedHash(seed string) codec.Hash {
	return codec.Hash{Bytes: [32]byte(document.SHA256Digest([]byte(seed)))}
}

func contentOf(id codec.UUID, json string) codec.Hash {
	h, err := document.ComputeContentHash(document.Document{ID: id, JSON: json})
	if err != nil {
		fail(err)
	}
	return h
}

func commitFixture(name string, c document.Commit) replicaCommitFixture {
	payload, err := c.ToPayloadBytes()
	if err != nil {
		fail(fmt.Errorf("%s: %w", name, err))
	}
	back, err := document.FromPayloadBytes(payload)
	if err != nil {
		fail(fmt.Errorf("%s: %w", name, err))
	}
	f := replicaCommitFixture{
		Name: name, PayloadHex: hex.EncodeToString(payload), Hash: back.Hash.Hex(),
		Parents: []string{}, Namespace: c.NamespaceID, TransactionID: c.TransactionID.String(),
		TimestampMicros: fmt.Sprint(c.Timestamp.EpochMicros()), AuthorNodeID: c.AuthorNodeID.String(),
		Ops: []replicaOpFixture{}, TreeHash: c.DocumentTreeHash.Hex(), Message: c.Message,
	}
	if back.Hash != c.Hash {
		fail(fmt.Errorf("%s: BuildCommit hash %s, payload hash %s", name, c.Hash.Hex(), back.Hash.Hex()))
	}
	for _, p := range c.ParentHashes {
		f.Parents = append(f.Parents, p.Hex())
	}
	if c.SchemaHash != nil {
		f.SchemaHash = c.SchemaHash.Hex()
	}
	for _, op := range c.Operations {
		switch o := op.(type) {
		case document.WriteOp:
			f.Ops = append(f.Ops, replicaOpFixture{Kind: "write", DocID: o.DocID.String(), JSON: o.Patch})
		case document.DeleteOp:
			f.Ops = append(f.Ops, replicaOpFixture{Kind: "delete", DocID: o.DocID.String()})
		case document.FileWriteOp:
			f.Ops = append(f.Ops, replicaOpFixture{Kind: "fileWrite", Path: o.Path, BlobHash: o.BlobHash.Hex()})
		case document.SchemaMigrationOp:
			f.Ops = append(f.Ops, replicaOpFixture{Kind: "schemaMigration", MigrationID: o.MigrationID.String(), Payload: o.MigrationPayload})
		}
	}
	return f
}

func build(parents []codec.Hash, ns, tx string, micros int64, author string, ops []document.Op, tree codec.Hash, schemaHash *codec.Hash, msg string) document.Commit {
	c, err := document.BuildCommit(parents, ns, mustUUID(tx), codec.TimestampFromEpochMicros(micros), mustUUID(author), ops, tree, schemaHash, msg)
	if err != nil {
		fail(err)
	}
	return c
}

func buildReplicaCommits() []replicaCommitFixture {
	a := mustUUID("6f9619ff-8b86-d011-b42d-00cf4fc964ff")
	b := mustUUID("00000000-0000-0000-0000-000000000001")
	author := "11111111-2222-3333-4444-555555555555"
	tx := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	schema := fixedHash("schema")
	var out []replicaCommitFixture
	out = append(out, commitFixture("genesis", build(nil, "app/u/42", tx, 0, author, nil, codec.Hash{}, nil, "")))
	out = append(out, commitFixture("writeAndDelete", build([]codec.Hash{fixedHash("p1")}, "orders", tx,
		1_760_000_000_123_456, author,
		[]document.Op{document.WriteOp{DocID: a, Patch: `{"name":"Ada Lovelace ✨","n":1}`}, document.DeleteOp{DocID: b}},
		fixedHash("tree"), nil, "hello")))
	out = append(out, commitFixture("mergeWithSchema", build([]codec.Hash{fixedHash("p1"), fixedHash("p2")}, "orders", tx,
		-5, author, []document.Op{document.WriteOp{DocID: b, Patch: `{}`}}, fixedHash("tree2"), &schema,
		"kdb:projection/1 source=abc complete=true")))
	out = append(out, commitFixture("fileAndMigration", build([]codec.Hash{fixedHash("p3")}, "files", tx, 42, author,
		[]document.Op{
			document.FileWriteOp{Path: "/a/b.txt", BlobHash: fixedHash("blob")},
			document.SchemaMigrationOp{MigrationID: b, MigrationPayload: `{"v":2}`},
		}, fixedHash("tree3"), nil, "")))
	long := make([]document.Op, 0, 200)
	for i := 0; i < 200; i++ {
		id := codec.DerivedUUID(fmt.Sprint("doc-", i))
		long = append(long, document.WriteOp{DocID: id, Patch: fmt.Sprintf(`{"i":%d,"pad":"%040d"}`, i, i)})
	}
	out = append(out, commitFixture("manyOps", build([]codec.Hash{fixedHash("p4")}, "big", tx, 7, author, long, fixedHash("tree4"), nil, "many")))
	return out
}

func entriesFixture(entries map[codec.UUID]codec.Hash) []treeEntryFixture {
	out := make([]treeEntryFixture, 0, len(entries))
	for id, h := range entries {
		out = append(out, treeEntryFixture{ID: id.String(), ContentHash: h.Hex()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func treeRoot(entries map[codec.UUID]codec.Hash) string {
	t, err := document.BuildDocumentTree(entries)
	if err != nil {
		fail(err)
	}
	return t.TreeHash.Hex()
}

func buildTrees() []treeFixture {
	rng := rand.New(rand.NewSource(7))
	randomID := func() codec.UUID { return codec.UUID{MSB: rng.Int63() - rng.Int63(), LSB: rng.Int63() - rng.Int63()} }
	cases := map[string]map[codec.UUID]codec.Hash{
		"empty": {},
		"one":   {mustUUID("6f9619ff-8b86-d011-b42d-00cf4fc964ff"): fixedHash("c1")},
		"sharedPrefix": {
			mustUUID("6f9619ff-8b86-d011-b42d-00cf4fc964f0"): fixedHash("c1"),
			mustUUID("6f9619ff-8b86-d011-b42d-00cf4fc964f1"): fixedHash("c2"),
			mustUUID("0f9619ff-8b86-d011-b42d-00cf4fc964f1"): fixedHash("c3"),
		},
		"extremes": {
			mustUUID("00000000-0000-0000-0000-000000000000"): fixedHash("lo"),
			mustUUID("ffffffff-ffff-ffff-ffff-ffffffffffff"): fixedHash("hi"),
		},
	}
	for _, n := range []int{17, 300, 1500} {
		m := map[codec.UUID]codec.Hash{}
		for i := 0; i < n; i++ {
			m[randomID()] = fixedHash(fmt.Sprint("c", n, "-", i))
		}
		cases[fmt.Sprint("random", n)] = m
	}
	names := make([]string, 0, len(cases))
	for name := range cases {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]treeFixture, 0, len(cases))
	for _, name := range names {
		out = append(out, treeFixture{Name: name, Entries: entriesFixture(cases[name]), Root: treeRoot(cases[name])})
	}
	return out
}

// buildTreeEdits records the root after each step of puts and deletes, built incrementally with
// With/Without the way a node builds its trees - including deletes that collapse a subtree back
// to a single leaf.
func buildTreeEdits() []treeEditFixture {
	rng := rand.New(rand.NewSource(11))
	var fixtures []treeEditFixture
	for _, tc := range []struct {
		name         string
		steps, batch int
		pool         int
	}{{"small", 40, 1, 8}, {"churn", 30, 25, 200}, {"collapse", 60, 3, 12}} {
		pool := make([]codec.UUID, tc.pool)
		for i := range pool {
			// a shared first byte keeps the trie deep enough for collapses to cascade
			pool[i] = codec.UUID{MSB: int64(0x6f00000000000000 | uint64(rng.Int63n(1<<12))), LSB: rng.Int63()}
		}
		tree := document.EmptyDocumentTree()
		f := treeEditFixture{Name: tc.name}
		for s := 0; s < tc.steps; s++ {
			var edits []treeEntryFixture
			for e := 0; e < tc.batch; e++ {
				id := pool[rng.Intn(len(pool))]
				var err error
				if rng.Intn(3) == 0 {
					tree, err = tree.Without(id)
					edits = append(edits, treeEntryFixture{ID: id.String()})
				} else {
					h := fixedHash(fmt.Sprint(tc.name, s, e))
					tree, err = tree.With(id, h)
					edits = append(edits, treeEntryFixture{ID: id.String(), ContentHash: h.Hex()})
				}
				if err != nil {
					fail(err)
				}
			}
			f.Steps = append(f.Steps, struct {
				Edits []treeEntryFixture `json:"edits"`
				Root  string             `json:"root"`
			}{edits, tree.TreeHash.Hex()})
		}
		fixtures = append(fixtures, f)
	}
	return fixtures
}

// buildChain makes a small history the way a node would hold one: a linear run, two branches and
// a merge commit carrying every document its parents disagree on, with real tree hashes. A replica
// fast-forwarding any step must reach exactly the target's tree.
func buildChain() chainFixture {
	const ns = "app/u/42"
	author := "11111111-2222-3333-4444-555555555555"
	ids := make([]codec.UUID, 8)
	for i := range ids {
		ids[i] = codec.DerivedUUID(fmt.Sprint("chain-doc-", i))
	}
	type state map[codec.UUID]string
	treeOf := func(s state) codec.Hash {
		m := map[codec.UUID]codec.Hash{}
		for id, j := range s {
			m[id] = contentOf(id, j)
		}
		t, err := document.BuildDocumentTree(m)
		if err != nil {
			fail(err)
		}
		return t.TreeHash
	}
	clone := func(s state) state {
		out := state{}
		for k, v := range s {
			out[k] = v
		}
		return out
	}
	var all []document.Commit
	states := map[codec.Hash]state{}
	seq := 0
	commit := func(parents []codec.Hash, base state, ops []document.Op) document.Commit {
		s := clone(base)
		for _, op := range ops {
			switch o := op.(type) {
			case document.WriteOp:
				s[o.DocID] = o.Patch
			case document.DeleteOp:
				delete(s, o.DocID)
			}
		}
		seq++
		tx := codec.DerivedUUID(fmt.Sprint("chain-tx-", seq)).String()
		c := build(parents, ns, tx, int64(1_760_000_000_000_000+seq), author, ops, treeOf(s), nil, "")
		all = append(all, c)
		states[c.Hash] = s
		return c
	}
	w := func(i int, v string) document.Op { return document.WriteOp{DocID: ids[i], Patch: v} }
	d := func(i int) document.Op { return document.DeleteOp{DocID: ids[i]} }

	c1 := commit(nil, state{}, []document.Op{w(0, `{"v":1}`), w(1, `{"v":1}`), w(2, `{"v":1}`)})
	c2 := commit([]codec.Hash{c1.Hash}, states[c1.Hash], []document.Op{w(0, `{"v":2}`), d(1)})
	c3 := commit([]codec.Hash{c2.Hash}, states[c2.Hash], []document.Op{w(3, `{"v":3}`), w(0, `{"v":3}`)})
	a1 := commit([]codec.Hash{c3.Hash}, states[c3.Hash], []document.Op{w(4, `{"a":1}`), w(2, `{"a":1}`)})
	a2 := commit([]codec.Hash{a1.Hash}, states[a1.Hash], []document.Op{d(3), w(5, `{"a":2}`)})
	b1 := commit([]codec.Hash{c3.Hash}, states[c3.Hash], []document.Op{w(2, `{"b":1}`), w(6, `{"b":1}`), d(0)})
	// The merge: a resolution of every document its parents disagree on (b wins doc 2).
	resolved := clone(states[a2.Hash])
	for id, v := range states[b1.Hash] {
		resolved[id] = v
	}
	delete(resolved, ids[0])
	var mergeOps []document.Op
	for _, id := range ids {
		va, ina := states[a2.Hash][id]
		vb, inb := states[b1.Hash][id]
		if ina == inb && va == vb {
			continue
		}
		if v, ok := resolved[id]; ok {
			mergeOps = append(mergeOps, document.WriteOp{DocID: id, Patch: v})
		} else {
			mergeOps = append(mergeOps, document.DeleteOp{DocID: id})
		}
	}
	m := commit([]codec.Hash{a2.Hash, b1.Hash}, resolved, mergeOps)
	m2 := commit([]codec.Hash{m.Hash}, states[m.Hash], []document.Op{w(7, `{"after":"merge"}`), w(4, `{"a":9}`)})

	f := chainFixture{Namespace: ns}
	for i, c := range all {
		f.Commits = append(f.Commits, commitFixture(fmt.Sprint("chain", i), c))
	}
	step := func(from *document.Commit, to document.Commit, rng ...document.Commit) {
		s := chainStepFixture{To: to.Hash.Hex(), Docs: map[string]string{}}
		if from != nil {
			s.From = from.Hash.Hex()
		}
		for _, c := range rng {
			s.Commits = append(s.Commits, c.Hash.Hex())
		}
		for id, v := range states[to.Hash] {
			s.Docs[id.String()] = v
		}
		f.Steps = append(f.Steps, s)
	}
	step(nil, c2, c2, c1) // out of order on purpose: the replica must topo-sort
	step(&c2, c3, c3)
	step(&c3, m, b1, m, a1, a2)
	step(&m, m2, m2)
	return f
}
