package peersync

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
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

// The non-conflicting counterpart: a disjoint-document push must auto-merge and ack, landing
// main on a real two-parent merge commit - not the pushed commit's hash directly (that would be
// the bug: blindly adopting whatever was pushed). The ack reports that merge commit as the new
// head, not the hash the client pushed.
func TestHostCommitPushAutoMergesDisjointWritesAndAcks(t *testing.T) {
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
	if respFrame == nil {
		t.Fatal("expected a CommitPushAck for a clean auto-merge, got no response frame")
	}
	respMsg, err := w.Decode(respFrame)
	if err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	ack, ok := respMsg.(wire.CommitPushAckMessage)
	if !ok {
		t.Fatalf("expected CommitPushAckMessage, got %T", respMsg)
	}
	if ack.H.CorrelationID != 2 {
		t.Fatalf("ack must echo the push's correlation id 2, got %d", ack.H.CorrelationID)
	}
	if ack.AppliedCommits != 1 {
		t.Fatalf("expected 1 applied commit, got %d", ack.AppliedCommits)
	}

	head, _ := hostDag.Head()
	if ack.HeadHex != head.Hex() {
		t.Fatalf("ack head %s does not match the host's actual head %s", ack.HeadHex, head.Hex())
	}
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

// pushWithin runs PushCommits off the test goroutine so a host that never replies fails the test
// in seconds instead of blocking for the full correlation wait in defaultClient.request.
func pushWithin(t *testing.T, session Session, commits []document.Commit, within time.Duration) (int, error) {
	t.Helper()
	type outcome struct {
		applied int
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		applied, err := session.PushCommits(commits)
		done <- outcome{applied, err}
	}()
	select {
	case o := <-done:
		return o.applied, o.err
	case <-time.After(within):
		t.Fatalf("PushCommits did not return within %s - the host owes a clean push an ack and never sent one", within)
		return 0, nil
	}
}

// The push counterpart to the PullMissing tests above, and the regression test for the flagged
// bug: a clean (non-conflicting) push driven through the real client - not connHost.HandleFrame -
// must come back. CommitPush is a request/response pair (component 23 spec §5: "returns ack frame
// with applied count"), but the host used to answer every non-conflicting outcome with no frame
// at all, so PushCommits sat in its correlation wait for ~20s and then failed with "no response
// for correlation" - which also took SyncBidirectional down with it whenever there was anything
// to push. Every earlier push test dodged this by calling the frame handler directly.
func TestPushCommitsReturnsAckInsteadOfHangingOnCleanPush(t *testing.T) {
	ns := "app/push-ack"
	hubName := "hub-push-ack"

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

	// Only the client has written - the host is still at genesis, so this push fast-forwards.
	// The plainest possible success case, and the one that used to hang.
	localCommit := writeDoc(t, localSide, ns, genesis, newUUID(t), `{"v":"local"}`)

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

	applied, err := pushWithin(t, session, []document.Commit{localCommit}, 3*time.Second)
	if err != nil {
		t.Fatalf("pushCommits: %v", err)
	}
	if applied != 1 {
		t.Fatalf("expected 1 applied commit, got %d", applied)
	}
	remoteHead, _ := remoteDag.Head()
	if remoteHead != localCommit.Hash {
		t.Fatalf("expected host to fast-forward to %s, got %s", localCommit.Hash.Hex(), remoteHead.Hex())
	}

	// The ack carries an applied count, not an echo of what was sent (component 23 spec §5 calls
	// CommitPush idempotent): re-pushing history the host already has is a no-op worth zero.
	reapplied, err := pushWithin(t, session, []document.Commit{localCommit}, 3*time.Second)
	if err != nil {
		t.Fatalf("idempotent re-push: %v", err)
	}
	if reapplied != 0 {
		t.Fatalf("expected a re-push of known history to apply 0 commits, got %d", reapplied)
	}
}

// The conflicting counterpart: the host answers a same-document divergence with a
// ConflictReport, which the client used to send and never read - so a rejected push still
// reported every commit as pushed. The caller has to be able to tell the two apart.
func TestPushCommitsSurfacesConflictInsteadOfReportingSuccess(t *testing.T) {
	ns := "app/push-conflict"
	hubName := "hub-push-conflict"

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

	applied, err := pushWithin(t, session, []document.Commit{localCommit}, 3*time.Second)
	if err == nil {
		t.Fatalf("expected a conflicting push to fail, got %d commits reported pushed", applied)
	}
	if applied != 0 {
		t.Fatalf("expected 0 applied commits on a rejected push, got %d", applied)
	}
	var conflictErr *kdberr.ConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected a ConflictError carrying the peer's report, got %T: %v", err, err)
	}
	if len(conflictErr.Report.Conflicts) != 1 || conflictErr.Report.Conflicts[0].DocumentID != sharedDoc.String() {
		t.Fatalf("expected exactly one conflict on %s, got %+v", sharedDoc.String(), conflictErr.Report.Conflicts)
	}

	// The host keeps the pushed commit but must not adopt it as main - the client's own view of
	// that (a plain error) is only trustworthy if the host really did leave the head alone.
	remoteHead, _ := remoteDag.Head()
	if remoteHead != remoteCommit.Hash {
		t.Fatalf("expected host main to stay at %s, got %s", remoteCommit.Hash.Hex(), remoteHead.Hex())
	}
	if !remoteDag.HasCommit(localCommit.Hash) {
		t.Fatal("pushed commit should still be stored even though main didn't move onto it")
	}
}

// TestPullMissingMaterializesFetchedCommitIntoLocalStorage is the client-side mirror of the
// front door's own fix (go/kdb/server's ListenPeerSync wiring embed.MaterializeCommit into
// HostConfig): dag.PutCommit only updates DAG bookkeeping, so a commit fetched via PullMissing
// was previously reachable from the local DAG but invisible to anything that reads through
// storage.Adapter (e.g. a locally embedded runtime's SqlExec/Query/GetDocument) until this
// commit's ops were replayed into it. Uses the real production embed.MaterializeCommit as the
// callback, not a test-local reimplementation, so this proves the actual wiring works end to
// end, the same way TestListenPeerSyncPushIsVisibleToServerQueries does for the push direction.
func TestPullMissingMaterializesFetchedCommitIntoLocalStorage(t *testing.T) {
	ns := "app/pull-materialize"
	hubName := "hub-pull-materialize"

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
	remoteSide := side{dag: remoteDag, storage: remoteStorage}

	docID := newUUID(t)
	docJSON := `{"v":"remote"}`
	remoteCommit := writeDoc(t, remoteSide, ns, genesis, docID, docJSON)

	w := wire.NewCodec(wire.EncodingJSON)
	host := NewHost(w, remoteDag, remoteStorage, auth.AllowAll, auth.EmptyContext)
	if err := host.Start(HostConfig{NamespaceID: ns, NodeID: "host", TransportHub: hubName}); err != nil {
		t.Fatalf("host start: %v", err)
	}
	defer host.Stop()

	transport := stream.NewInMemoryTransport()
	client := NewClient(w, transport, localDag, localStorage)
	materializedCalls := 0
	session, err := client.Connect(ClientConfig{
		NamespaceID: ns,
		NodeID:      "client",
		PeerURI:     "memory://" + hubName,
		MaterializeCommit: func(commit document.Commit) error {
			materializedCalls++
			return embed.MaterializeCommit(localStorage, localDag, ns, commit)
		},
	})
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
		t.Fatalf("expected a clean fast-forward pull, got a conflict: %+v", result.Conflict)
	}
	if result.FinalHead != remoteCommit.Hash {
		t.Fatalf("expected local head to fast-forward to %s, got %s", remoteCommit.Hash.Hex(), result.FinalHead.Hex())
	}
	if materializedCalls != 1 {
		t.Fatalf("expected MaterializeCommit called exactly once, got %d", materializedCalls)
	}

	doc, err := localStorage.GetDocument(ns, docID, remoteCommit.DocumentTreeHash)
	if err != nil {
		t.Fatalf("GetDocument: %v", err)
	}
	if doc == nil {
		t.Fatal("pulled document not visible in local storage - MaterializeCommit was not wired into PullMissing")
	}
	if doc.JSON != docJSON {
		t.Fatalf("expected JSON %q, got %q", docJSON, doc.JSON)
	}
}
