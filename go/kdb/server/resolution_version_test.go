package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/peersync"
)

// TestSetResolutionNormalizesAndGatesTheVersion: "version": 1 is the chain with no version (the
// same hash older releases compute), 2 is a different chain, and a version this release does not
// run is refused when set here.
func TestSetResolutionNormalizesAndGatesTheVersion(t *testing.T) {
	a := newMetaNode(t)
	rules := []peersync.ResolutionRule{{Kind: peersync.RuleFieldMerge}, {Kind: peersync.RuleLastWrite}}
	legacy := peersync.ResolutionChain{Rules: rules}
	if err := a.store.SetResolution("app/data", peersync.ResolutionChain{Rules: rules, Version: 1}); err != nil {
		t.Fatal(err)
	}
	if got := a.data.ResolutionChainOf().Hash(); got != legacy.Hash() {
		t.Fatalf("version 1 should be stored as the unversioned chain: %s vs %s", got, legacy.Hash())
	}
	if err := a.store.SetResolution("app/data", peersync.ResolutionChain{Rules: rules, Version: 2}); err != nil {
		t.Fatal(err)
	}
	if c := a.data.ResolutionChainOf(); c.EffectiveVersion() != 2 || c.Hash() == legacy.Hash() {
		t.Fatalf("version 2 not applied: %+v", c)
	}
	if err := a.store.SetResolution("app/data", peersync.ResolutionChain{Rules: rules, Version: peersync.MaxChainVersion + 1}); err == nil {
		t.Fatal("a chain version this release does not run was accepted")
	}
}

// TestReplicatedChainOfAnUnknownVersionHoldsMerging: a chain newer than this release (written by a
// newer node) still replicates in and is installed - so this node does not carry on under the
// chain it held before - and it is reported in the conflict queue. The node advertises a hash no
// peer matches, so neither side merges the namespace.
func TestReplicatedChainOfAnUnknownVersionHoldsMerging(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	future := peersync.ResolutionChain{Rules: []peersync.ResolutionRule{{Kind: peersync.RuleLastWrite}}, Version: peersync.MaxChainVersion + 1}
	body, err := json.Marshal(metaDoc{Kind: "resolution", Namespace: "app/data", Resolution: &future})
	if err != nil {
		t.Fatal(err)
	}
	// Written as a newer release would write it: straight into the metadata namespace.
	if _, err := a.store.meta.systemUpsert(metaResolutionID("app/data"), string(body)); err != nil {
		t.Fatal(err)
	}
	if _, err := syncPatterns(t, a, b, MetaNamespace); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to install the chain", func() bool { return b.data.ResolutionChainOf().Hash() == future.Hash() })
	if h := b.data.AdvertisedResolutionHash(); !strings.HasPrefix(h, "unsupported:") {
		t.Fatalf("B advertises %q for a chain it cannot run", h)
	}
	reported := false
	for _, e := range b.data.Conflicts.List() {
		if e.Kind == "meta-apply" && strings.Contains(e.Detail, "newer than this node runs") {
			reported = true
		}
	}
	if !reported {
		t.Fatalf("expected the unknown version in B's conflict queue, got %+v", b.data.Conflicts.List())
	}
}
