package server

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

func TestSessionManagerBegin(t *testing.T) {
	rt, err := embed.OpenMemoryRuntime("demo", "demo/users", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	srv := NewKdbServerRuntime(rt)
	mgr := NewSessionManager(srv)
	sess, err := mgr.Begin("demo/users", Snapshot, "", "", auth.Principal{})
	if err != nil {
		t.Fatal(err)
	}
	if sess.ID.Value == "" {
		t.Fatal("missing session id")
	}
	got, ok := mgr.Get(sess.ID.Value)
	if !ok || got.NamespaceID != "demo/users" {
		t.Fatalf("session %+v", got)
	}
	mgr.End(sess.ID.Value)
	if _, ok := mgr.Get(sess.ID.Value); ok {
		t.Fatal("expected session removed")
	}
}
