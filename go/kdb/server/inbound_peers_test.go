package server

import (
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

func TestInboundPeerFloorPersists(t *testing.T) {
	dir := t.TempDir()
	rt, err := embed.OpenFileRuntime(dir, "app", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	srv := NewKdbServerRuntime(rt)
	now := time.Now()
	srv.NoteInboundPeer("edge-1", now.Add(-2*time.Hour))
	srv.NoteInboundPeer("edge-2", now.Add(-10*time.Minute))
	srv.NoteInboundPeer("gone", now.Add(-30*24*time.Hour))
	rt.Close()

	rt, err = embed.OpenFileRuntime(dir, "app", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	floor, ok := NewKdbServerRuntime(rt).InboundPeerFloor(7*24*time.Hour, now)
	if !ok || !floor.Equal(now.Add(-2*time.Hour).UTC()) {
		t.Fatalf("floor after reopen: %v %v", floor, ok)
	}
}
