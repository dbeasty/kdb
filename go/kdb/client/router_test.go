package client_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

func sqlServer(t *testing.T) (*server.KdbServerRuntime, string) {
	t.Helper()
	rt, err := embed.OpenMemoryRuntime("app", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	srv := server.NewKdbServerRuntime(rt)
	srv.NodeID, _ = codec.RandomUUID()
	ln, err := server.ListenSqlWire("tcp://127.0.0.1:0?bind=true", srv)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return srv, fmt.Sprintf("tcp://%s", ln.Addr().String())
}

// TestRouterFollowsNotHome: a write sent to a node that is not the namespace's home is refused
// with the home's address; the router retries there, and sends the namespace's later writes
// straight to it.
func TestRouterFollowsNotHome(t *testing.T) {
	home, homeAddr := sqlServer(t)
	other, otherAddr := sqlServer(t)
	h := &server.Home{Node: home.NodeID.String(), Addr: homeAddr, Fence: 1}
	home.SetHome(h)
	other.SetHome(h)

	ctx := context.Background()
	plain, err := client.Connect(ctx, otherAddr, "")
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	id, _ := codec.RandomUUID()
	_, err = plain.Upsert(ctx, "app/data", id.String(), []byte(`{"v":1}`))
	var nh *client.NotHomeError
	if !errors.As(err, &nh) || nh.Home != homeAddr {
		t.Fatalf("expected NotHomeError naming %s, got %v", homeAddr, err)
	}

	r, err := client.NewRouter(ctx, otherAddr, "", client.ConnectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for i := 0; i < 3; i++ {
		id, _ := codec.RandomUUID()
		if _, err := r.Upsert(ctx, "app/data", id.String(), []byte(fmt.Sprintf(`{"n":%d}`, i))); err != nil {
			t.Fatalf("write %d through the router: %v", i, err)
		}
		if body, _, found, _ := home.GetDocument("app/data", id); !found || body != fmt.Sprintf(`{"n":%d}`, i) {
			t.Fatalf("write %d did not land on the home", i)
		}
	}
}
