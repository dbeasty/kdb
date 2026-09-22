package peersync

import (
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// Resolver authority scenarios: a namespace whose chain ends in RuleAuthority hands undecided
// conflicts to an application or person, either holding them unmerged or merging provisionally.

// authNode is one node in these scenarios: its DAG and storage, its conflict queue, its identity.
type authNode struct {
	side
	id    codec.UUID
	queue *ConflictQueue
}

func newAuthNodes(t *testing.T, ns string, n int) []authNode {
	t.Helper()
	out := make([]authNode, n)
	for i := range out {
		q, err := NewConflictQueue("")
		if err != nil {
			t.Fatal(err)
		}
		out[i] = authNode{side: newSide(t, ns), id: newUUID(t), queue: q}
	}
	return out
}

func (a authNode) env(ns string, chain *ResolutionChain, peer authNode) IngestEnv {
	return IngestEnv{
		DAG: a.dag, Storage: a.storage, NamespaceID: ns, ApplyToStorage: true,
		Resolution: ResolutionOptions{Chain: chain}, Conflicts: a.queue,
		Peer: peer.id.String(), Self: a.id.String(),
	}
}

// pullFrom has dst fetch everything src's main has and decide its own main, as a sync would.
func (a authNode) pullFrom(t *testing.T, ns string, src authNode, chain *ResolutionChain) IngestResult {
	t.Helper()
	return a.pullAs(t, ns, src, chain, src)
}

// pullAs is pullFrom with the history attributed to peer - as when it arrives relayed.
func (a authNode) pullAs(t *testing.T, ns string, src authNode, chain *ResolutionChain, peer authNode) IngestResult {
	t.Helper()
	c, s, h := snapshotFor(t, a.side, src.side)
	env := a.env(ns, chain, peer)
	if _, err := StoreCommits(env, c, s); err != nil {
		t.Fatal(err)
	}
	r, err := Adopt(env, h)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// write commits body to doc on a's main as a's own write.
func (a authNode) write(t *testing.T, ns string, doc codec.UUID, body string) document.Commit {
	t.Helper()
	return a.commit(t, ns, doc, body, "test write")
}

func (a authNode) commit(t *testing.T, ns string, doc codec.UUID, body, message string) document.Commit {
	t.Helper()
	head, _ := a.dag.Head()
	if err := a.storage.PutDocument(ns, document.Document{ID: doc, JSON: body}); err != nil {
		t.Fatal(err)
	}
	parent, err := a.dag.GetCommitOrThrow(head)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := a.storage.CommitTree(ns, parent.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	c, err := a.dag.AppendCommitDetached(document.Transaction{
		ID: newUUID(t), BaseVersion: head, Timestamp: codec.TimestampNow(), AuthorNodeID: a.id,
		Operations: []document.Op{document.WriteOp{DocID: doc, Patch: body}},
	}, head, tree, nil, message)
	if err != nil {
		t.Fatal(err)
	}
	setHead(t, a.side, c.Hash)
	return c
}

func (a authNode) body(t *testing.T, ns string, doc codec.UUID) string {
	t.Helper()
	_, h, _, err := a.dag.HeadCommit()
	if err != nil {
		t.Fatal(err)
	}
	d, err := a.storage.GetDocument(ns, doc, h.DocumentTreeHash)
	if err != nil || d == nil {
		t.Fatalf("document %s: %+v %v", doc, d, err)
	}
	return d.JSON
}

// divergeOn gives a and b a shared document, then has each change it, b last.
func divergeOn(t *testing.T, ns string, a, b authNode, chain *ResolutionChain) codec.UUID {
	t.Helper()
	doc := newUUID(t)
	a.write(t, ns, doc, `{"v":"base"}`)
	b.pullFrom(t, ns, a, chain)
	a.write(t, ns, doc, `{"v":"a"}`)
	b.write(t, ns, doc, `{"v":"b"}`)
	return doc
}

func authorityChain(pending, node string) *ResolutionChain {
	return &ResolutionChain{Rules: []ResolutionRule{{Kind: RuleFieldMerge}, {Kind: RuleAuthority, Pending: pending, Node: node}}}
}

func TestAuthorityHoldQueuesTheConflictWithBaseAndOrigins(t *testing.T) {
	ns := "app/authority-hold"
	n := newAuthNodes(t, ns, 2)
	a, b := n[0], n[1]
	chain := authorityChain(PendingHold, b.id.String())
	doc := divergeOn(t, ns, a, b, chain)
	headA, _ := a.dag.Head()

	r := a.pullFrom(t, ns, b, chain)
	if r.Outcome.Kind != OutcomeConflict {
		t.Fatalf("expected the conflict held, got %v", r.Outcome.Kind)
	}
	if h, _ := a.dag.Head(); h != headA {
		t.Fatal("hold must leave main where it was")
	}
	entries := a.queue.List()
	if len(entries) != 1 {
		t.Fatalf("expected one entry, got %+v", entries)
	}
	e := entries[0]
	if e.Kind != ConflictDivergence || !e.Authority || e.AuthorityNode != b.id.String() {
		t.Fatalf("entry not handed to the authority: %+v", e)
	}
	if len(e.Details) != 1 || e.Details[0].DocumentID != doc.String() {
		t.Fatalf("expected a detail for the document: %+v", e.Details)
	}
	dt := e.Details[0]
	if dt.Base == nil || *dt.Base != `{"v":"base"}` {
		t.Fatalf("expected the base value, got %v", dt.Base)
	}
	if dt.LocalOrigin.NodeID != a.id || dt.IncomingOrigin.NodeID != b.id {
		t.Fatalf("origins should name the writers: local %s incoming %s", dt.LocalOrigin.NodeID, dt.IncomingOrigin.NodeID)
	}
	if _, ok := a.dag.GetBranch(e.TrackingRef); !ok {
		t.Fatal("the incoming side must stay reachable on its tracking branch")
	}
}

// A chain whose earlier rules settle a document never reaches the authority: only what they
// leave is held.
func TestAuthorityOnlyReceivesWhatEarlierRulesLeave(t *testing.T) {
	ns := "app/authority-partial"
	n := newAuthNodes(t, ns, 2)
	a, b := n[0], n[1]
	chain := authorityChain(PendingHold, "")
	merged, clash := newUUID(t), newUUID(t)
	a.write(t, ns, merged, `{"x":1,"y":1}`)
	a.write(t, ns, clash, `{"v":0}`)
	b.pullFrom(t, ns, a, chain)
	a.write(t, ns, merged, `{"x":2,"y":1}`)
	a.write(t, ns, clash, `{"v":"a"}`)
	b.write(t, ns, merged, `{"x":1,"y":2}`)
	b.write(t, ns, clash, `{"v":"b"}`)

	r := a.pullFrom(t, ns, b, chain)
	if r.Outcome.Kind != OutcomeConflict || len(r.Outcome.Report.Conflicts) != 1 || r.Outcome.Report.Conflicts[0].DocumentID != clash.String() {
		t.Fatalf("expected only the clashing document held, got %v %+v", r.Outcome.Kind, r.Outcome.Report)
	}
}

func TestAuthorityProvisionalMergesByLastWriteAndMarksIt(t *testing.T) {
	ns := "app/authority-provisional"
	n := newAuthNodes(t, ns, 3)
	a, b, c := n[0], n[1], n[2]
	chain := authorityChain(PendingProvisional, "")
	doc := divergeOn(t, ns, a, b, chain)

	r := a.pullFrom(t, ns, b, chain)
	if r.Outcome.Kind != OutcomeMerged {
		t.Fatalf("expected a provisional merge, got %v", r.Outcome.Kind)
	}
	m := *r.Outcome.MergeCommit
	if ids := ProvisionalDocuments(m); len(ids) != 1 || ids[0] != doc {
		t.Fatalf("the merge should name the provisional document, message %q", m.Message)
	}
	if got := a.body(t, ns, doc); got != `{"v":"b"}` {
		t.Fatalf("provisional value should be the later write, got %s", got)
	}
	id := ConflictID(ConflictProvisional, ns, m.Hash.Hex())
	check := func(who string, q *ConflictQueue) {
		t.Helper()
		e, ok := q.Get(id)
		if !ok {
			t.Fatalf("%s did not record the provisional decision: %+v", who, q.List())
		}
		if e.Kind != ConflictProvisional || !e.Authority || e.MergeHex != m.Hash.Hex() || len(e.Items) != 1 {
			t.Fatalf("%s: bad entry %+v", who, e)
		}
		it, dt := e.Items[0], e.Details[0]
		if *it.LocalDoc != `{"v":"b"}` || *it.IncomingDoc != `{"v":"a"}` {
			t.Fatalf("%s: local should be the kept value and incoming the displaced one: %+v", who, it)
		}
		if dt.LocalOrigin.NodeID != b.id || dt.IncomingOrigin.NodeID != a.id || dt.Base == nil || *dt.Base != `{"v":"base"}` {
			t.Fatalf("%s: bad detail %+v", who, dt)
		}
	}
	check("the merging node", a.queue)

	// B receives the merge as a fast-forward, and C - which saw neither side - fetches it cold:
	// both learn of the provisional decision from the merge itself.
	if r := b.pullFrom(t, ns, a, chain); r.Outcome.Kind != OutcomeFastForwarded {
		t.Fatalf("B should fast-forward to the merge, got %v", r.Outcome.Kind)
	}
	check("a node that fast-forwarded", b.queue)
	c.pullFrom(t, ns, a, chain)
	check("a node that fetched the merge cold", c.queue)
}

// The two nodes merging the same divergence provisionally build the same merge.
func TestAuthorityProvisionalMergeIsTheSameOnBothNodes(t *testing.T) {
	ns := "app/authority-provisional-both"
	n := newAuthNodes(t, ns, 2)
	a, b := n[0], n[1]
	chain := authorityChain(PendingProvisional, "")
	divergeOn(t, ns, a, b, chain)
	ra := a.pullFrom(t, ns, b, chain)
	// B merges A's side independently, from its own copy of the divergence.
	aHeadBeforeMerge := ra.Outcome.MergeCommit.ParentHashes
	var aSide codec.Hash
	hb, _ := b.dag.Head()
	for _, p := range aHeadBeforeMerge {
		if p != hb {
			aSide = p
		}
	}
	c, s, err := MissingCommits(a.dag, aSide, spreadAncestorsAll(b.side), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	env := b.env(ns, chain, a)
	if _, err := StoreCommits(env, c, s); err != nil {
		t.Fatal(err)
	}
	rb, err := Adopt(env, aSide)
	if err != nil {
		t.Fatal(err)
	}
	if rb.Outcome.Kind != OutcomeMerged || rb.Outcome.MergeCommit.Hash != ra.Outcome.MergeCommit.Hash {
		t.Fatalf("the nodes built different provisional merges: %v", rb.Outcome.Kind)
	}
}

// The authority settles a provisional decision with an ordinary commit naming the entry; every
// node that adopts it closes its own copy of the entry.
func TestResolutionCommitClosesTheEntryWhereverItArrives(t *testing.T) {
	ns := "app/authority-resolve"
	n := newAuthNodes(t, ns, 2)
	a, b := n[0], n[1]
	chain := authorityChain(PendingProvisional, "")
	doc := divergeOn(t, ns, a, b, chain)
	m := *a.pullFrom(t, ns, b, chain).Outcome.MergeCommit
	b.pullFrom(t, ns, a, chain)
	id := ConflictID(ConflictProvisional, ns, m.Hash.Hex())
	if _, ok := b.queue.Get(id); !ok {
		t.Fatal("B should hold the provisional entry")
	}

	a.commit(t, ns, doc, `{"v":"a"}`, ResolveMessage(id))
	if r := b.pullFrom(t, ns, a, chain); r.Outcome.Kind != OutcomeFastForwarded {
		t.Fatalf("B should fast-forward to the resolution, got %v", r.Outcome.Kind)
	}
	if _, ok := b.queue.Get(id); ok {
		t.Fatal("the resolution did not close B's entry")
	}
	if got := b.body(t, ns, doc); got != `{"v":"a"}` {
		t.Fatalf("B should hold the authority's value, got %s", got)
	}
}

// A held divergence is closed on a node once main contains its incoming side - even when the
// settlement came from a different peer than the one the divergence was recorded against, which
// the per-peer clearing cannot see.
func TestHeldDivergenceIsSweptWhenItsSettlementArrivesFromElsewhere(t *testing.T) {
	ns := "app/authority-sweep"
	n := newAuthNodes(t, ns, 3)
	a, b, relay := n[0], n[1], n[2]
	chain := authorityChain(PendingHold, "")
	doc := divergeOn(t, ns, a, b, chain)
	if r := b.pullFrom(t, ns, a, chain); r.Outcome.Kind != OutcomeConflict {
		t.Fatalf("B should hold the divergence, got %v", r.Outcome.Kind)
	}
	e := b.queue.List()[0]

	// A settles it (as its authority would, through ResolveConflict), taking A's value.
	a.pullFrom(t, ns, b, chain)
	var aEntry ConflictEntry
	for _, x := range a.queue.List() {
		aEntry = x
	}
	if _, err := ResolveConflict(a.env(ns, chain, b), aEntry.ID, map[codec.UUID]Choice{doc: {Take: "local"}}); err != nil {
		t.Fatal(err)
	}
	// The settlement reaches B attributed to another peer.
	if r := b.pullAs(t, ns, a, chain, relay); r.Outcome.Kind != OutcomeFastForwarded {
		t.Fatalf("B should fast-forward to the settlement, got %v", r.Outcome.Kind)
	}
	if _, ok := b.queue.Get(e.ID); ok {
		t.Fatal("B's held divergence should be swept once main contains its incoming side")
	}
	if _, ok := b.dag.GetBranch(e.TrackingRef); ok {
		t.Fatal("its tracking branch should go with it")
	}
	if got := b.body(t, ns, doc); got != `{"v":"a"}` {
		t.Fatalf("B should hold the settled value, got %s", got)
	}
}

func TestQueueDeliveryIsKeptUntilTheReportChanges(t *testing.T) {
	q, _ := NewConflictQueue(t.TempDir())
	var told []string
	q.OnRecord(func(e ConflictEntry) { told = append(told, e.ID) })
	body := func(s string) *string { return &s }
	e := ConflictEntry{ID: "x", Kind: ConflictDivergence, Authority: true, Items: []kdberr.ConflictItem{{DocumentID: "d", LocalDoc: body("1")}}}
	sent, _ := q.Record(e)
	if err := q.MarkDelivered(sent); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Record(e); err != nil { // seen again, unchanged
		t.Fatal(err)
	}
	if got, _ := q.Get("x"); !got.Delivered || got.Seen != 2 {
		t.Fatalf("an unchanged sighting must stay delivered: %+v", got)
	}
	if len(told) != 1 {
		t.Fatalf("a delivered entry must not be announced again, told %v", told)
	}
	e.Items = []kdberr.ConflictItem{{DocumentID: "d", LocalDoc: body("2")}}
	if _, err := q.Record(e); err != nil {
		t.Fatal(err)
	}
	if got, _ := q.Get("x"); got.Delivered {
		t.Fatal("a changed report must be delivered again")
	}
	if len(told) != 2 {
		t.Fatalf("a changed report must be announced, told %v", told)
	}
	// Acknowledging the older version does not mark the newer one.
	if err := q.MarkDelivered(sent); err != nil {
		t.Fatal(err)
	}
	if got, _ := q.Get("x"); got.Delivered {
		t.Fatal("a stale acknowledgement must not mark the current report delivered")
	}
	// Delivery survives a restart.
	cur, _ := q.Get("x")
	_ = q.MarkDelivered(cur)
	q2, err := NewConflictQueue(q.dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := q2.Get("x"); !got.Delivered {
		t.Fatal("delivery must be durable")
	}
}

func TestAuthorityRuleValidation(t *testing.T) {
	node := codec.DerivedUUID("n").String()
	for name, tc := range map[string]struct {
		rules []ResolutionRule
		ok    bool
	}{
		"hold":                 {[]ResolutionRule{{Kind: RuleAuthority}}, true},
		"provisional via node": {[]ResolutionRule{{Kind: RuleFieldMerge}, {Kind: RuleAuthority, Pending: PendingProvisional, Node: node}}, true},
		"bad pending":          {[]ResolutionRule{{Kind: RuleAuthority, Pending: "maybe"}}, false},
		"bad node":             {[]ResolutionRule{{Kind: RuleAuthority, Node: "aws"}}, false},
		"not last":             {[]ResolutionRule{{Kind: RuleAuthority}, {Kind: RuleLastWrite}}, false},
		"pending elsewhere":    {[]ResolutionRule{{Kind: RuleQueue, Pending: PendingHold}}, false},
		"timeout":              {[]ResolutionRule{{Kind: RuleAuthority, Timeout: "24h"}}, true},
		"bad timeout":          {[]ResolutionRule{{Kind: RuleAuthority, Timeout: "soon"}}, false},
		"negative timeout":     {[]ResolutionRule{{Kind: RuleAuthority, Timeout: "-1h"}}, false},
		"timeout elsewhere":    {[]ResolutionRule{{Kind: RuleQueue, Timeout: "1h"}}, false},
	} {
		c := ResolutionChain{Rules: tc.rules}
		if err := c.Validate(); (err == nil) != tc.ok {
			t.Errorf("%s: Validate() = %v", name, err)
		}
	}
	a := (&ResolutionChain{Rules: []ResolutionRule{{Kind: RuleAuthority, Node: node}}}).Authority()
	if a == nil || !a.Notifies(node) || a.Notifies(codec.DerivedUUID("other").String()) {
		t.Fatal("only the named node notifies")
	}
	if !(&ResolutionRule{Kind: RuleAuthority}).Notifies("anyone") {
		t.Fatal("with no node named, every node notifies")
	}
	if (&ResolutionChain{Rules: []ResolutionRule{{Kind: RuleQueue}}}).Authority() != nil {
		t.Fatal("a queue chain has no authority")
	}
}

// Over v1 - which carries no chain hash - a namespace with a chain never merges: the host queues.
func TestV1HostWithAChainDoesNotMerge(t *testing.T) {
	chain := authorityChain(PendingProvisional, "")
	h := &frameHandler{cfg: HostConfig{NamespaceID: "ns", ConflictPolicy: transaction.ConflictPolicyLastWrite, ChainOf: func() *ResolutionChain { return chain }}}
	if env := h.ingestEnv(); env.Resolution.Policy != transaction.ConflictPolicyStrict || env.Resolution.Chain != nil {
		t.Fatalf("expected strict with no chain over v1, got %+v", env.Resolution)
	}
	h.cfg.ChainOf = func() *ResolutionChain { return nil }
	if env := h.ingestEnv(); env.Resolution.Policy != transaction.ConflictPolicyLastWrite {
		t.Fatalf("without a chain the configured policy applies, got %+v", env.Resolution)
	}
}

func TestMergeMessageCarriesProvisionalDocuments(t *testing.T) {
	a, b := codec.DerivedUUID("a"), codec.DerivedUUID("b")
	if mergeMessageFor(nil) != mergeMessage {
		t.Fatal("a merge with nothing provisional keeps the plain message")
	}
	msg := mergeMessageFor([]codec.UUID{a, b})
	if !strings.HasPrefix(msg, mergeMessage+" ") {
		t.Fatalf("unexpected message %q", msg)
	}
	c := document.Commit{Message: msg, ParentHashes: []codec.Hash{{}, {}}}
	if got := ProvisionalDocuments(c); len(got) != 2 || got[0] != a || got[1] != b {
		t.Fatalf("round trip failed: %v", got)
	}
	c.ParentHashes = c.ParentHashes[:1]
	if ProvisionalDocuments(c) != nil {
		t.Fatal("only a merge commit carries provisional documents")
	}
	if id, ok := ResolvedEntry(document.Commit{Message: ResolveMessage("abc")}); !ok || id != "abc" {
		t.Fatal("resolve message round trip failed")
	}
}
