package peersync

import (
	"encoding/json"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	mem "github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

// End-to-end regression test for the flagged bug, driven through the real wire protocol (not
// ResolveDivergence called directly, the way conflict_detection_test.go does): a client whose
// local history has diverged from the peer it's pulling from must not blindly move "main" to
// whatever the peer sent - it must go through the same fast-forward/merge/conflict decision as
// the host side. This is the client half of Component 39's original bug
// (kdb-peer-sync/PeerSyncConflictDetection.kt), now fixed identically in Go.
func TestPullMissingDoesNotBlindlyMoveHeadOnDivergence(t *testing.T) {
	ns := "app/pull-diverge"
	hubName := "hub-pull-diverge"

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

	// Diverge: local and remote each write a different, disjoint document from the same genesis
	// - real-world "two nodes wrote while offline from each other" shape.
	localDoc := newUUID(t)
	remoteDoc := newUUID(t)
	localCommit := writeDoc(t, localSide, ns, genesis, localDoc, `{"v":"local"}`)
	remoteCommit := writeDoc(t, remoteSide, ns, genesis, remoteDoc, `{"v":"remote"}`)

	w := wire.NewCodec(wire.EncodingJSON)
	host := NewHost(w, remoteDag, remoteStorage, auth.AllowAll, auth.EmptyContext)
	if err := host.Start(HostConfig{NamespaceID: ns, NodeID: "host", TransportHub: hubName}); err != nil {
		t.Fatalf("host start: %v", err)
	}
	defer host.Stop()

	transport := stream.NewInMemoryTransport()
	client := NewClient(w, transport, localDag, localStorage)
	session, err := client.Connect(ClientConfig{NamespaceID: ns, NodeID: "client", PeerURI: "memory://" + hubName})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if session.RemoteHead() != remoteCommit.Hash {
		t.Fatalf("expected handshake to report remote head %s, got %s", remoteCommit.Hash.Hex(), session.RemoteHead().Hex())
	}

	result, err := session.PullMissing()
	if err != nil {
		t.Fatalf("pullMissing: %v", err)
	}
	if result.Conflict != nil {
		t.Fatalf("expected disjoint writes to auto-merge without conflict, got %+v", result.Conflict)
	}

	head, _ := localDag.Head()
	if head == remoteCommit.Hash {
		t.Fatal("main was blindly moved to the remote commit - exactly the bug this test guards against")
	}
	if head == localCommit.Hash {
		t.Fatal("main never moved at all - the remote's commit should have been merged in")
	}
	merged, err := localDag.GetCommitOrThrow(head)
	if err != nil {
		t.Fatalf("expected head to be a real commit in the local dag: %v", err)
	}
	if len(merged.ParentHashes) != 2 || merged.ParentHashes[0] != localCommit.Hash || merged.ParentHashes[1] != remoteCommit.Hash {
		t.Fatalf("expected a two-parent auto-merge commit [local, remote], got parents %v", merged.ParentHashes)
	}
	if !localDag.HasCommit(remoteCommit.Hash) {
		t.Fatal("remote's commit must still be stored even though main didn't move directly onto it")
	}
}

// The conflicting counterpart: pulling a peer whose divergent history touches the SAME
// document must report a conflict and leave main exactly where it was, not silently pick a
// winner (or, pre-fix, blindly overwrite local's version).
func TestPullMissingReportsConflictInsteadOfOverwritingLocalHead(t *testing.T) {
	ns := "app/pull-conflict"
	hubName := "hub-pull-conflict"

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

	sharedDoc := newUUID(t)
	localCommit := writeDoc(t, localSide, ns, genesis, sharedDoc, `{"v":"local"}`)
	remoteCommit := writeDoc(t, remoteSide, ns, genesis, sharedDoc, `{"v":"remote"}`)

	w := wire.NewCodec(wire.EncodingJSON)
	host := NewHost(w, remoteDag, remoteStorage, auth.AllowAll, auth.EmptyContext)
	if err := host.Start(HostConfig{NamespaceID: ns, NodeID: "host", TransportHub: hubName}); err != nil {
		t.Fatalf("host start: %v", err)
	}
	defer host.Stop()

	transport := stream.NewInMemoryTransport()
	client := NewClient(w, transport, localDag, localStorage)
	session, err := client.Connect(ClientConfig{NamespaceID: ns, NodeID: "client", PeerURI: "memory://" + hubName})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	result, err := session.PullMissing()
	if err != nil {
		t.Fatalf("pullMissing: %v", err)
	}
	if result.Conflict == nil {
		t.Fatal("expected a conflict report for a same-document divergence")
	}
	if len(result.Conflict.Conflicts) != 1 || result.Conflict.Conflicts[0].DocumentID != sharedDoc.String() {
		t.Fatalf("expected exactly one conflict on %s, got %+v", sharedDoc.String(), result.Conflict.Conflicts)
	}

	head, _ := localDag.Head()
	if head != localCommit.Hash {
		t.Fatalf("expected main to stay at the local commit on conflict, got %s (wanted %s, remote was %s)",
			head.Hex(), localCommit.Hash.Hex(), remoteCommit.Hash.Hex())
	}
}

// Host-side symmetry check (Component 39's §5 contract: one shared decision function serves
// both directions): a CommitPush that diverges from the host's own history on the same document
// must come back as an explicit ConflictReportMessage, not the ordinary silent ack the
// (pre-fix) unconditional-store-only handler always returned regardless of what "main" did.
func TestHostCommitPushReturnsConflictReportOnSameDocumentDivergence(t *testing.T) {
	ns := "app/host-push-conflict"

	hostDag, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatalf("host dag: %v", err)
	}
	incomingDag, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatalf("incoming dag: %v", err)
	}
	genesis, _ := hostDag.Head()

	hostStorage := mem.NewInMemoryStorageAdapter()
	incomingStorage := mem.NewInMemoryStorageAdapter()
	hostSide := side{dag: hostDag, storage: hostStorage}
	incomingSide := side{dag: incomingDag, storage: incomingStorage}

	sharedDoc := newUUID(t)
	hostCommit := writeDoc(t, hostSide, ns, genesis, sharedDoc, `{"v":"host"}`)
	incomingCommit := writeDoc(t, incomingSide, ns, genesis, sharedDoc, `{"v":"incoming"}`)

	w := wire.NewCodec(wire.EncodingJSON)
	connHost := NewConnectionHost(w, hostDag, hostStorage, HostConfig{NamespaceID: ns, NodeID: "host"}, auth.AllowAll, auth.EmptyContext)

	push := wire.CommitPushMessage{
		H:         wire.Header{MessageType: wire.MsgCommitPush, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 1},
		Namespace: ns,
		Commits:   []document.Commit{incomingCommit},
	}
	frame, err := w.Encode(push)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	respFrame, err := connHost.HandleFrame(frame)
	if err != nil {
		t.Fatalf("handleFrame: %v", err)
	}
	if respFrame == nil {
		t.Fatal("expected an explicit ConflictReportMessage, got a silent ack")
	}
	respMsg, err := w.Decode(respFrame)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	conflictMsg, ok := respMsg.(wire.ConflictReportMessage)
	if !ok {
		t.Fatalf("expected ConflictReportMessage, got %T", respMsg)
	}
	var report kdberr.ConflictReport
	if err := json.Unmarshal(conflictMsg.ReportBytes, &report); err != nil {
		t.Fatalf("unmarshal conflict report: %v", err)
	}
	if len(report.Conflicts) != 1 || report.Conflicts[0].DocumentID != sharedDoc.String() {
		t.Fatalf("expected exactly one conflict on %s, got %+v", sharedDoc.String(), report.Conflicts)
	}

	// main must be left exactly where it was - the incoming commit is still stored (history
	// never lost) but not adopted as the branch pointer.
	head, _ := hostDag.Head()
	if head != hostCommit.Hash {
		t.Fatalf("expected host main to stay at %s, got %s", hostCommit.Hash.Hex(), head.Hex())
	}
	if !hostDag.HasCommit(incomingCommit.Hash) {
		t.Fatal("incoming commit should still be stored even though main didn't move onto it")
	}
}

// The non-conflicting counterpart: a disjoint-document push must auto-merge silently (ordinary
// ack, nil response), landing main on a real two-parent merge commit - not the pushed commit's
// hash directly (that would be the bug: blindly adopting whatever was pushed).
func TestHostCommitPushAutoMergesDisjointWritesWithSilentAck(t *testing.T) {
	ns := "app/host-push-merge"

	hostDag, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatalf("host dag: %v", err)
	}
	incomingDag, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatalf("incoming dag: %v", err)
	}
	genesis, _ := hostDag.Head()

	hostStorage := mem.NewInMemoryStorageAdapter()
	incomingStorage := mem.NewInMemoryStorageAdapter()
	hostSide := side{dag: hostDag, storage: hostStorage}
	incomingSide := side{dag: incomingDag, storage: incomingStorage}

	hostDoc := newUUID(t)
	incomingDoc := newUUID(t)
	hostCommit := writeDoc(t, hostSide, ns, genesis, hostDoc, `{"v":"host"}`)
	incomingCommit := writeDoc(t, incomingSide, ns, genesis, incomingDoc, `{"v":"incoming"}`)

	w := wire.NewCodec(wire.EncodingJSON)
	connHost := NewConnectionHost(w, hostDag, hostStorage, HostConfig{NamespaceID: ns, NodeID: "host"}, auth.AllowAll, auth.EmptyContext)

	push := wire.CommitPushMessage{
		H:         wire.Header{MessageType: wire.MsgCommitPush, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 2},
		Namespace: ns,
		Commits:   []document.Commit{incomingCommit},
	}
	frame, err := w.Encode(push)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	respFrame, err := connHost.HandleFrame(frame)
	if err != nil {
		t.Fatalf("handleFrame: %v", err)
	}
	if respFrame != nil {
		t.Fatalf("expected a silent ack (nil) for a clean auto-merge, got a response frame")
	}

	head, _ := hostDag.Head()
	if head == incomingCommit.Hash {
		t.Fatal("main was blindly moved to the pushed commit - exactly the bug this test guards against")
	}
	merged, err := hostDag.GetCommitOrThrow(head)
	if err != nil {
		t.Fatalf("expected head to be a real merge commit: %v", err)
	}
	if len(merged.ParentHashes) != 2 || merged.ParentHashes[0] != hostCommit.Hash || merged.ParentHashes[1] != incomingCommit.Hash {
		t.Fatalf("expected a two-parent auto-merge [host, incoming], got %v", merged.ParentHashes)
	}
}
