package server

import (
	"sort"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

// idleSet is a set whose opener opens memory runtimes and counts opens.
func idleSet(t *testing.T) (*NamespaceSet, *int) {
	t.Helper()
	set := NewNamespaceSet(nil)
	opens := 0
	set.SetOpener(func(ns string, create bool) (*KdbServerRuntime, error) {
		opens++
		rt, err := embed.OpenMemoryRuntime(embed.CatalogFromNamespace(ns), ns, schema.None())
		if err != nil {
			return nil, err
		}
		return NewKdbServerRuntime(rt), nil
	})
	return set, &opens
}

func TestCloseIdleClosesStaleAndLeastRecentlyUsedOverTheCap(t *testing.T) {
	set, opens := idleSet(t)
	for _, ns := range []string{"app/a", "app/b", "app/c", "app/d"} {
		if _, err := set.Resolve(ns, true); err != nil {
			t.Fatal(err)
		}
	}
	var closed []string
	policy := IdlePolicy{
		MaxOpen: 2, MinIdle: time.Second,
		Pinned: func(ns string) bool { return ns == "app/a" },
		Close:  func(rt *KdbServerRuntime) error { closed = append(closed, rt.Runtime.DefaultNamespace); return nil },
	}
	now := time.Now()
	// Everything was just used: under MinIdle nothing closes, even over the cap.
	if got := set.CloseIdle(policy, now); len(got) != 0 {
		t.Fatalf("closed namespaces in use: %v", got)
	}
	// b used long ago, c less long ago, d just now.
	b, _ := set.Get("app/b")
	c, _ := set.Get("app/c")
	b.lastUsed.Store(now.Add(-time.Hour).Unix())
	c.lastUsed.Store(now.Add(-time.Minute).Unix())
	got := set.CloseIdle(policy, now)
	sort.Strings(got)
	if len(got) != 2 || got[0] != "app/b" || got[1] != "app/c" {
		t.Fatalf("over a cap of 2 with a pinned a, the two idle ones should close: %v", got)
	}
	if names := set.Namespaces(); len(names) != 2 {
		t.Fatalf("open after sweep: %v", names)
	}
	if known := set.KnownNamespaces(); len(known) != 4 {
		t.Fatalf("closed namespaces must stay known: %v", known)
	}
	// Using a closed namespace reopens it through the opener.
	before := *opens
	if _, err := set.Resolve("app/b", false); err != nil || *opens != before+1 {
		t.Fatalf("resolve should reopen app/b: %v (opens %d -> %d)", err, before, *opens)
	}

	// IdleAfter closes by age alone.
	d, _ := set.Get("app/d")
	d.lastUsed.Store(now.Add(-10 * time.Minute).Unix())
	got = set.CloseIdle(IdlePolicy{IdleAfter: 5 * time.Minute, MinIdle: time.Second, Pinned: policy.Pinned, Close: policy.Close}, now)
	if len(got) != 1 || got[0] != "app/d" {
		t.Fatalf("idle-after should close only d: %v", got)
	}
}

func TestCloseIdleLeavesABusyNamespace(t *testing.T) {
	set, _ := idleSet(t)
	rt, err := set.Resolve("app/busy", true)
	if err != nil {
		t.Fatal(err)
	}
	rt.lastUsed.Store(time.Now().Add(-time.Hour).Unix())
	rt.groupPublishing.Add(1) // a cross-namespace group mid-publish
	policy := IdlePolicy{IdleAfter: time.Minute, Close: func(*KdbServerRuntime) error { t.Fatal("closed a busy namespace"); return nil }}
	if got := set.CloseIdle(policy, time.Now()); len(got) != 0 {
		t.Fatalf("closed: %v", got)
	}
	rt.groupPublishing.Add(-1)
	if _, err := rt.Upsert("app/busy", mustRandomUUID(t), `{"x":1}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
}
