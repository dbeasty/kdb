package replication

import (
	"reflect"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/peersync"
)

func TestParsePeer(t *testing.T) {
	t.Setenv("PEER_PW", "s3cret")
	p, err := ParsePeer("name=cloud,addr=tcps://cloud:4242,namespaces=site/*|shared/*,mode=pull,interval=5s,user=edge,password-env=PEER_PW,create=true")
	if err != nil {
		t.Fatal(err)
	}
	if p.Name != "cloud" || p.Addr != "tcps://cloud:4242" || p.Mode != peersync.SyncPull || p.Interval != 5*time.Second ||
		!reflect.DeepEqual(p.Namespaces, []string{"site/*", "shared/*"}) || *p.User != "edge" || *p.Password != "s3cret" || !p.CreateLocal {
		t.Fatalf("parsed %+v", p)
	}
	d, err := ParsePeer("name=a,addr=tcp://x:1")
	if err != nil || d.Mode != peersync.SyncBoth || d.Interval != DefaultInterval || !reflect.DeepEqual(d.Namespaces, []string{"**"}) {
		t.Fatalf("defaults: %+v %v", d, err)
	}
	for _, bad := range []string{
		"addr=tcp://x:1",
		"name=a",
		"name=a,addr=tcp://x:1,colour=blue",
		"name=a,addr=tcp://x:1,mode=sideways",
		"name=a,addr=tcp://x:1,interval=-1s",
		"name=a,addr=tcp://x:1,password-env=DEFINITELY_NOT_SET_ANYWHERE",
		"name=a/b,addr=tcp://x:1",
	} {
		if _, err := ParsePeer(bad); err == nil {
			t.Errorf("ParsePeer(%q) accepted", bad)
		}
	}
	if _, err := ParsePeers("name=a,addr=tcp://x:1;name=a,addr=tcp://y:1"); err == nil {
		t.Error("a duplicated peer name was accepted")
	}
}

func TestParseFilteredPeer(t *testing.T) {
	p, err := ParsePeer("name=hq,addr=tcp://hq:1,namespaces=orders,filter=region IN ('EU', 'UK') AND qty > 2")
	if err != nil {
		t.Fatal(err)
	}
	if p.Filter != "region IN ('EU', 'UK') AND qty > 2" || p.Mode != peersync.SyncPull || p.Namespaces[0] != "orders" {
		t.Fatalf("parsed %+v", p)
	}
	for _, bad := range []string{
		"name=hq,addr=tcp://hq:1,namespaces=orders/*,filter=a = 1",
		"name=hq,addr=tcp://hq:1,filter=a = 1",
		"name=hq,addr=tcp://hq:1,namespaces=orders,filter=a = ",
	} {
		if _, err := ParsePeer(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestParseWriteBackPeer(t *testing.T) {
	p, err := ParsePeer("name=hq,addr=tcp://hq:1,namespaces=orders,writeback=true,filter=region = 'EU'")
	if err != nil {
		t.Fatal(err)
	}
	if !p.WriteBack || p.Filter != "region = 'EU'" {
		t.Fatalf("parsed %+v", p)
	}
	if _, err := ParsePeer("name=hq,addr=tcp://hq:1,namespaces=orders,writeback=true"); err == nil {
		t.Error("accepted writeback on an unfiltered peer")
	}
}
