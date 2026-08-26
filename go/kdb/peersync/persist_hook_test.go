package peersync

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	mem "github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Regression test for kdb-spec-layer13 Component 47 §2.2 / Component 52 §9.2: a commit received
// from a peer (whether an ordinary pushed/pulled commit, or an auto-merge commit ResolveDivergence
// creates on the spot) must reach a caller-supplied Persist hook, since dag.PutCommit alone only
// ever mutates the in-memory DAG. Without this, a file-backed node's peer-received commits lived
// only in memory and vanished on restart.
func TestCommitPushCallsPersistForBothOrdinaryAndMergeCommits(t *testing.T) {
	ns := "app/persist-hook"
	hubName := "hub-persist-hook"

	localDag, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatalf("local dag: %v", err)
	}
	remoteDag, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatalf("remote dag: %v", err)
	}
	genesis, _ := localDag.Head()

	localStorage := mem.NewInMemoryStorageAdapter()
	remoteStorage := mem.NewInMemoryStorageAdapter()
	localSide := side{dag: localDag, storage: localStorage}
	remoteSide := side{dag: remoteDag, storage: remoteStorage}

	// Diverge with disjoint writes, same shape as TestPullMissingDoesNotBlindlyMoveHeadOnDivergence
	// - guarantees ResolveDivergence takes the auto-merge path and creates a brand new commit.
	localDoc := newUUID(t)
	remoteDoc := newUUID(t)
	localCommit := writeDoc(t, localSide, ns, genesis, localDoc, `{"v":"local"}`)
	remoteCommit := writeDoc(t, remoteSide, ns, genesis, remoteDoc, `{"v":"remote"}`)

	var hostPersisted []codec.Hash
	w := wire.NewCodec(wire.EncodingJSON)
	host := NewHost(w, remoteDag, remoteStorage, auth.AllowAll, auth.EmptyContext)
	if err := host.Start(HostConfig{
		NamespaceID:  ns,
		NodeID:       "host",
		TransportHub: hubName,
		Persist: func(c document.Commit) error {
			hostPersisted = append(hostPersisted, c.Hash)
			return nil
		},
	}); err != nil {
		t.Fatalf("host start: %v", err)
	}
	defer host.Stop()

	var clientPersisted []codec.Hash
	transport := stream.NewInMemoryTransport()
	client := NewClient(w, transport, localDag, localStorage)
	session, err := client.Connect(ClientConfig{
		NamespaceID: ns, NodeID: "client", PeerURI: "memory://" + hubName,
		Persist: func(c document.Commit) error {
			clientPersisted = append(clientPersisted, c.Hash)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	result, err := session.PullMissing()
	if err != nil {
		t.Fatalf("pullMissing: %v", err)
	}
	if result.Conflict != nil {
		t.Fatalf("expected auto-merge, got conflict: %+v", result.Conflict)
	}

	// The client pulled the remote's commit (ordinary) and then created a merge commit locally
	// (ResolveDivergence's OutcomeMerged path) - both must have reached Persist.
	if len(clientPersisted) != 2 {
		t.Fatalf("expected client Persist called for [remote commit, merge commit], got %d calls: %v",
			len(clientPersisted), clientPersisted)
	}
	if clientPersisted[0] != remoteCommit.Hash {
		t.Fatalf("expected first client Persist call for the pulled remote commit %s, got %s",
			remoteCommit.Hash.Hex(), clientPersisted[0].Hex())
	}
	head, _ := localDag.Head()
	if clientPersisted[1] != head {
		t.Fatalf("expected second client Persist call for the new local head (the merge commit) "+
			"%s, got %s", head.Hex(), clientPersisted[1].Hex())
	}
	// Now push a fresh commit at the (now-merged) head back to the host, driven directly through
	// HandleFrame rather than session.PushCommits - matching
	// TestHostCommitPushAutoMergesDisjointWritesWithSilentAck's own approach, since a clean
	// (non-conflicting) CommitPush deliberately gets a silent (nil) ack per that test, and
	// session.PushCommits waiting on a correlated response for that case is a separate,
	// pre-existing gap unrelated to this fix (worth its own follow-up, not chased down here).
	mergeCommit, err := localDag.GetCommitOrThrow(head)
	if err != nil {
		t.Fatalf("expected merge commit to exist locally: %v", err)
	}
	freshDoc := newUUID(t)
	freshCommit := writeDoc(t, localSide, ns, head, freshDoc, `{"v":"fresh"}`)
	// The host never saw the client's original commit or the client-side merge commit (only
	// ever pushed to, never pushed from, the host) - include both ancestors so freshCommit's
	// parent chain is fully resolvable on arrival.
	push := wire.CommitPushMessage{
		H:         wire.Header{MessageType: wire.MsgCommitPush, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 99},
		Namespace: ns,
		Commits:   []document.Commit{localCommit, mergeCommit, freshCommit},
	}
	frame, err := w.Encode(push)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := host.HandleFrame(frame); err != nil {
		t.Fatalf("handleFrame: %v", err)
	}
	found := false
	for _, h := range hostPersisted {
		if h == freshCommit.Hash {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected host Persist to include the pushed commit %s, got %v", freshCommit.Hash.Hex(), hostPersisted)
	}
}
