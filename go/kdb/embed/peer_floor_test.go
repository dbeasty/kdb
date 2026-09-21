package embed_test

import (
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/storage"
)

// TestTruncationWaitsForActivePeer: a replication peer that has not been seen to receive commits
// newer than an hour ago holds truncation back from deleting them, even under a window that
// keeps nothing; once the peer has caught up, the same pass reclaims.
func TestTruncationWaitsForActivePeer(t *testing.T) {
	root := t.TempDir()
	writeSessions(t, root, 5, storage.RetentionWindow{})
	before := deltaSegmentCount(t, root)

	rt := noneRuntime(t, root, storage.RetentionWindow{Duration: storage.RetainNothing})
	rt.SetPeerRetentionFloor(func() (time.Time, bool) { return time.Now().Add(-time.Hour), true })
	if _, err := rt.Maintain(); err != nil {
		t.Fatal(err)
	}
	if got := deltaSegmentCount(t, root); got < before {
		t.Fatalf("a peer an hour behind should have kept every segment: %d -> %d", before, got)
	}

	rt.SetPeerRetentionFloor(func() (time.Time, bool) { return time.Time{}, false })
	if _, err := rt.Maintain(); err != nil {
		t.Fatal(err)
	}
	rt.Close()
	if got := deltaSegmentCount(t, root); got >= before {
		t.Fatalf("with no peer holding it back, truncation should reclaim: %d -> %d", before, got)
	}
}
