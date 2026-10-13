package replication

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/peersync"
)

func TestParsePeerCatchUp(t *testing.T) {
	p, err := ParsePeer("name=hub,addr=tcp://h:4242,namespaces=app/*,catchup=auto,catchup-ns=app/audit:replay|app/feed/*:squash")
	if err != nil {
		t.Fatal(err)
	}
	if p.CatchUp != peersync.CatchUpAuto || len(p.CatchUpNamespaces) != 2 ||
		p.CatchUpNamespaces[0] != (peersync.CatchUpOverride{Pattern: "app/audit", Route: peersync.CatchUpReplay}) ||
		p.CatchUpNamespaces[1] != (peersync.CatchUpOverride{Pattern: "app/feed/*", Route: peersync.CatchUpSquash}) {
		t.Fatalf("parsed %+v", p)
	}
	if p, err := ParsePeer("name=hub,addr=tcp://h:4242,namespaces=app/*"); err != nil || p.CatchUp != peersync.CatchUpReplay {
		t.Fatalf("default: %+v %v", p, err)
	}
	for _, bad := range []string{"catchup=sometimes", "catchup-ns=app/x", "catchup-ns=app/x:never"} {
		if _, err := ParsePeer("name=hub,addr=tcp://h:4242,namespaces=app/*," + bad); err == nil {
			t.Fatalf("%s: accepted", bad)
		}
	}
}
