package kdbsync

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/syncnode"
)

// TestDeviceSyncsWithACloudOverWebSocket: the binding's whole surface, end to end - a device
// opens, adds the cloud as a wss-style peer (ws:// here), syncs its user namespace both ways,
// joins a match namespace at runtime, and reopens with its data.
func TestDeviceSyncsWithACloudOverWebSocket(t *testing.T) {
	cloudDir := t.TempDir()
	host, err := embed.OpenFileHost(cloudDir, embed.FileRuntimeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	rt, err := host.Namespace("app", "app/cloud", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	primary := server.NewKdbServerRuntime(rt)
	set := server.NewNamespaceSet(host.Transactions())
	if err := set.Add(primary); err != nil {
		t.Fatal(err)
	}
	cloud, err := syncnode.Open(host, set, primary, syncnode.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer cloud.Close()
	set.SetOpener(cloud.Opener(nil))
	if err := cloud.Start(); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/kdb/sync", cloud.Handler())
	srv := httptest.NewServer(mux)
	defer srv.Close()
	put := func(ns, id, body string) {
		t.Helper()
		r, err := set.Resolve(ns, true)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.PutJSON(ns, docID(id), body, nil, noPrincipal); err != nil {
			t.Fatal(err)
		}
	}
	put("app/u/42", "profile", `{"name":"ada"}`)
	put("app/m/7", "board", `{"moves":1}`)

	dir := t.TempDir()
	dev, err := Open(dir, "app/device")
	if err != nil {
		t.Fatal(err)
	}
	if err := dev.AddPeer("cloud", strings.Replace(srv.URL, "http://", "ws://", 1)+"/kdb/sync", "app/u/42", true); err != nil {
		t.Fatal(err)
	}
	dev.SetToken("cloud", "t0")
	if err := dev.Start(); err != nil {
		t.Fatal(err)
	}
	if err := dev.SyncNow("cloud"); err != nil {
		t.Fatal(err)
	}
	if got, _ := dev.Get("app/u/42", "profile"); got != `{"name":"ada"}` {
		t.Fatalf("device read %q", got)
	}
	if got, _ := dev.Get("app/m/7", "board"); got != "" {
		t.Fatal("the device synced a match it has not joined")
	}
	if err := dev.Put("app/u/42", "settings", `{"theme":"dark"}`); err != nil {
		t.Fatal(err)
	}
	if err := dev.AddNamespaces("cloud", "app/m/7"); err != nil {
		t.Fatal(err)
	}
	if err := dev.SyncNow("cloud"); err != nil {
		t.Fatal(err)
	}
	if got, _ := dev.Get("app/m/7", "board"); got != `{"moves":1}` {
		t.Fatalf("after joining the match: %q", got)
	}
	if r, _ := set.Get("app/u/42"); r == nil {
		t.Fatal("cloud lost its namespace")
	} else if body, _, found, _ := r.GetDocument("app/u/42", docID("settings")); !found || body != `{"theme":"dark"}` {
		t.Fatalf("the device's write did not reach the cloud: %q", body)
	}
	if err := dev.Close(); err != nil {
		t.Fatal(err)
	}

	again, err := Open(dir, "app/device")
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if err := again.Start(); err != nil {
		t.Fatal(err)
	}
	if got, _ := again.Get("app/u/42", "settings"); got != `{"theme":"dark"}` {
		t.Fatalf("after reopening: %q", got)
	}
}
