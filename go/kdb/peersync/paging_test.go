package peersync

import (
	"errors"
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

// chain appends n single-document commits to s starting at parent, returning the last one.
func chain(t *testing.T, s side, ns string, parent codec.Hash, n int, tag string) document.Commit {
	t.Helper()
	var c document.Commit
	for i := 0; i < n; i++ {
		c = writeDoc(t, s, ns, parent, newUUID(t), fmt.Sprintf(`{"tag":%q,"i":%d}`, tag, i))
		parent = c.Hash
		if err := s.dag.SetHead(mainBranch, c.Hash); err != nil {
			t.Fatal(err)
		}
	}
	return c
}

// connectPair starts a host on remote and connects a client session from local to it.
func connectPair(t *testing.T, ns string, local, remote side, pageSize int) Session {
	t.Helper()
	hub := "hub-" + ns
	w := wire.NewCodec(wire.EncodingJSON)
	host := NewHost(w, remote.dag, remote.storage, auth.AllowAll, auth.EmptyContext)
	if err := host.Start(HostConfig{NamespaceID: ns, NodeID: "host", TransportHub: hub, ApplyToStorage: true}); err != nil {
		t.Fatalf("host start: %v", err)
	}
	t.Cleanup(func() { host.Stop() })
	client := NewClient(w, stream.NewInMemoryTransport(), local.dag, local.storage)
	session, err := client.Connect(ClientConfig{
		NamespaceID: ns, NodeID: "client", PeerURI: "memory://" + hub, ApplyToStorage: true, PageCommits: pageSize,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { client.Disconnect() })
	return session
}

// TestPullMoreThanOnePage is D1: a node more than one page behind used to be sent the newest
// page, whose oldest commit's parent it did not have, and could never catch up.
func TestPullMoreThanOnePage(t *testing.T) {
	ns := "app/pull-pages"
	local, remote := forkTwoSides(t, ns)
	genesis, _ := remote.dag.Head()
	last := chain(t, remote, ns, genesis, 1000, "remote")

	session := connectPair(t, ns, local, remote, 100)
	result, err := session.PullMissing()
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if result.AppliedCommits != 1000 {
		t.Fatalf("expected 1000 commits applied, got %d", result.AppliedCommits)
	}
	if head, _ := local.dag.Head(); head != last.Hash {
		t.Fatalf("expected local head %s, got %s", last.Hash.Hex(), head.Hex())
	}
}

func TestPushMoreThanOnePage(t *testing.T) {
	ns := "app/push-pages"
	local, remote := forkTwoSides(t, ns)
	genesis, _ := local.dag.Head()
	last := chain(t, local, ns, genesis, 750, "local")

	session := connectPair(t, ns, local, remote, 100)
	result, err := session.SyncBidirectional()
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if result.PushedCommits != 750 {
		t.Fatalf("expected 750 commits pushed, got %d", result.PushedCommits)
	}
	if head, _ := remote.dag.Head(); head != last.Hash {
		t.Fatalf("expected remote head %s, got %s", last.Hash.Hex(), head.Hex())
	}
}

// TestDivergedPushMergesOnceNotPerPage: deciding on each page of a divergent push would merge
// half of the branch and then merge again for every later page.
func TestDivergedPushMergesOnceNotPerPage(t *testing.T) {
	ns := "app/push-diverged-pages"
	local, remote := forkTwoSides(t, ns)
	genesis, _ := local.dag.Head()
	chain(t, local, ns, genesis, 250, "local")
	remoteTip := chain(t, remote, ns, genesis, 3, "remote")

	session := connectPair(t, ns, local, remote, 100)
	localHead, _ := local.dag.Head()
	pushed, err := session.(*defaultSession).pushMissing(localHead)
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if pushed != 250 {
		t.Fatalf("expected 250 pushed, got %d", pushed)
	}
	head, _ := remote.dag.Head()
	merge, _ := remote.dag.GetCommitOrThrow(head)
	if len(merge.ParentHashes) != 2 {
		t.Fatalf("expected the remote head to be one merge commit, got %d parents", len(merge.ParentHashes))
	}
	walked, _ := commitsBetween(remote.dag, head, remoteTip.Hash)
	merges := 0
	for _, c := range walked {
		if len(c.ParentHashes) == 2 {
			merges++
		}
	}
	if merges != 1 {
		t.Fatalf("expected exactly one merge commit, found %d", merges)
	}
}

// TestDivergedFetchDoesNotResendSharedHistory: a fetcher whose head the host has never seen
// used to be sent everything back to genesis. Spread ancestors let the host find the shared
// base and send only what diverged.
func TestDivergedFetchDoesNotResendSharedHistory(t *testing.T) {
	ns := "app/fetch-diverged"
	local, remote := forkTwoSides(t, ns)
	genesis, _ := local.dag.Head()
	base := chain(t, remote, ns, genesis, 500, "shared")
	for _, c := range mustMissing(t, remote, base.Hash, []codec.Hash{genesis}) {
		if err := local.dag.PutCommit(c, true); err != nil {
			t.Fatal(err)
		}
	}
	// Local writes are built against remote's storage only because that is where base's tree
	// lives; the commits land in local's DAG.
	localTip := chain(t, side{dag: local.dag, storage: remote.storage}, ns, base.Hash, 5, "local")
	remoteTip := chain(t, remote, ns, base.Hash, 5, "remote")

	// Haves sit at exponentially spaced distances, so the host can overshoot the true common
	// ancestor by at most the divergence length again - never by the 500 shared commits.
	missing := mustMissing(t, remote, remoteTip.Hash, spreadAncestors(local.dag, localTip.Hash))
	if len(missing) < 5 || len(missing) > 10 {
		t.Fatalf("expected the 5 divergent commits plus at most 5 shared ones, got %d", len(missing))
	}
	if missing[len(missing)-1].Hash != remoteTip.Hash {
		t.Fatal("the remote tip must be the last commit sent")
	}
}

func mustMissing(t *testing.T, s side, head codec.Hash, haves []codec.Hash) []document.Commit {
	t.Helper()
	commits, _, err := MissingCommits(s.dag, head, haves, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	return commits
}

// TestPeerFrameWrongNamespaceRefused is D5: the host served its one DAG whatever namespace a
// frame named, so a peer syncing namespace X could write into namespace Y.
func TestPeerFrameWrongNamespaceRefused(t *testing.T) {
	ns := "app/served"
	_, remote := forkTwoSides(t, ns)
	w := wire.NewCodec(wire.EncodingJSON)
	host := NewConnectionHost(w, remote.dag, remote.storage, HostConfig{NamespaceID: ns}, auth.AllowAll, auth.EmptyContext)
	frame, _ := w.Encode(wire.CommitFetchMessage{
		H:         wire.Header{MessageType: wire.MsgCommitFetch, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 5},
		Namespace: "app/other",
	})
	reply := decodeReply(t, w, host, frame)
	pe, ok := reply.(wire.PeerErrorMessage)
	if !ok || pe.Code != wire.ErrorCodeNamespaceMismatch || pe.H.CorrelationID != 5 {
		t.Fatalf("expected a correlated NAMESPACE_MISMATCH error, got %+v", reply)
	}
}

// TestPeerUnknownFrameGetsErrorReply is D7: an unknown frame got no reply at all, so the caller
// waited out its whole correlation timeout.
func TestPeerUnknownFrameGetsErrorReply(t *testing.T) {
	ns := "app/unknown-frame"
	_, remote := forkTwoSides(t, ns)
	w := wire.NewCodec(wire.EncodingJSON)
	host := NewConnectionHost(w, remote.dag, remote.storage, HostConfig{NamespaceID: ns}, auth.AllowAll, auth.EmptyContext)
	frame, _ := w.Encode(wire.SqlExecMessage{
		H: wire.Header{MessageType: wire.MsgSqlExec, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 9},
	})
	pe, ok := decodeReply(t, w, host, frame).(wire.PeerErrorMessage)
	if !ok || pe.Code != wire.ErrorCodeUnsupported || pe.H.CorrelationID != 9 {
		t.Fatalf("expected a correlated UNSUPPORTED error, got %+v", pe)
	}
}

// TestPeerErrorSurfacesAsRemoteError: the client turns the reply into a typed error instead of
// a timeout.
func TestPeerErrorSurfacesAsRemoteError(t *testing.T) {
	ns := "app/remote-error"
	local, remote := forkTwoSides(t, ns)
	session := connectPair(t, ns, local, remote, 0)
	sess := session.(*defaultSession)
	_, _, err := sess.client.fetchRemote(sess.conn, "app/elsewhere", nil, nil, 10)
	var re *RemoteError
	if !errors.As(err, &re) || re.Code != wire.ErrorCodeNamespaceMismatch {
		t.Fatalf("expected RemoteError NAMESPACE_MISMATCH, got %v", err)
	}
}

func decodeReply(t *testing.T, w wire.Codec, host *ConnectionHost, frame []byte) wire.Message {
	t.Helper()
	out, err := host.HandleFrame(frame)
	if err != nil {
		t.Fatalf("HandleFrame returned an error instead of a reply: %v", err)
	}
	msg, err := w.Decode(out)
	if err != nil {
		t.Fatal(err)
	}
	return msg
}

// TestFetchAcrossArchivedCommitSendsStub is D9's transfer half: a commit whose parent is
// archived used to be sent without anything naming that parent, so the receiver refused it with
// "missing parent". The stub now travels and the commits are stored. Adopting them still needs
// a shared ancestor the stub hides - that is the snapshot bootstrap of Phase 4, and until then
// the refusal names the real reason rather than a missing parent.
func TestFetchAcrossArchivedCommitSendsStub(t *testing.T) {
	ns := "app/archived"
	local, remote := forkTwoSides(t, ns)
	genesis, _ := remote.dag.Head()
	c1 := chain(t, remote, ns, genesis, 1, "a")
	last := chain(t, remote, ns, c1.Hash, 2, "b")
	if _, err := remote.dag.StubCommit(c1.Hash, "ice://c1"); err != nil {
		t.Fatalf("stub: %v", err)
	}
	session := connectPair(t, ns, local, remote, 0)
	_, err := session.PullMissing()
	if !local.dag.HasStub(c1.Hash) {
		t.Fatal("archived parent was not recorded as a stub")
	}
	if !local.dag.HasCommit(last.Hash) {
		t.Fatal("commits after the archived one were not stored")
	}
	var vnf *kdberr.VersionNotFoundError
	if !errors.As(err, &vnf) {
		t.Fatalf("expected the adopt step to report no common ancestor, got %v", err)
	}
}
