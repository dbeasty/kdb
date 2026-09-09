package embed_test

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Switching history modes on a running namespace. The claim under test
// throughout is that the switch destroys nothing - that is what makes it
// reversible, and it is the whole reason reclamation is a separate act.

func switchableRuntime(t *testing.T, root string) *embed.EmbeddedKdbRuntime {
	t.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.DeltaMaxSegmentBytes = 4096
	opts.Storage.Retain = storage.RetentionWindow{Duration: storage.RetainNothing}
	rt, err := embed.OpenFileRuntimeWithOptions(root, "app", "app/docs", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

// full -> none writes a marker and nothing else. Every document is still
// there, and so is every segment.
func TestSwitchingToNoneReclaimsNothing(t *testing.T) {
	root := t.TempDir()
	rt := switchableRuntime(t, root)
	defer rt.Close()
	for i := 0; i < 40; i++ {
		writeRotationDoc(t, rt, fmt.Sprintf("doc-%d", i), i)
	}
	before := deltaSegmentCount(t, root)

	res, err := rt.SetHistoryMode(storage.HistoryModeNone, storage.ReclaimUnset)
	if err != nil {
		t.Fatal(err)
	}
	if res.To != storage.HistoryModeNone {
		t.Fatalf("the switch reports %v", res.To)
	}
	// Landing on manual is what keeps it reversible.
	if res.Reclaim != storage.ReclaimManual {
		t.Fatalf("a switch to none landed on %v; it must land on manual so it stays reversible", res.Reclaim)
	}
	if after := deltaSegmentCount(t, root); after != before {
		t.Fatalf("the switch changed the segment count from %d to %d; it must delete nothing", before, after)
	}
	for i := 0; i < 40; i++ {
		if _, ok := readDoc(t, rt, fmt.Sprintf("doc-%d", i)); !ok {
			t.Fatalf("doc-%d disappeared when the mode changed", i)
		}
	}
	// And it says what it is now holding, which is what makes manual an
	// informed state rather than a slow leak.
	if res.EligibleSegments == 0 {
		t.Error("the switch reported nothing eligible; an operator on none+manual needs that number")
	}
}

// The mode is durable: a restart honours it.
func TestSwitchedModeSurvivesAReopen(t *testing.T) {
	root := t.TempDir()
	rt := switchableRuntime(t, root)
	for i := 0; i < 10; i++ {
		writeRotationDoc(t, rt, fmt.Sprintf("doc-%d", i), i)
	}
	if _, err := rt.SetHistoryMode(storage.HistoryModeNone, storage.ReclaimUnset); err != nil {
		t.Fatal(err)
	}
	rt.Close()

	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.Retain = storage.RetentionWindow{Duration: storage.RetainNothing}
	reopened, err := embed.OpenFileRuntimeWithOptions(root, "app", "app/docs", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.HistoryMode(); got != storage.HistoryModeNone {
		t.Fatalf("after a reopen the namespace is %v, want none", got)
	}
}

// And the reversal: switch to none, switch back, lose nothing. This is the
// property the whole design is arranged around.
func TestSwitchingBackBeforeCompactingLosesNothing(t *testing.T) {
	root := t.TempDir()
	rt := switchableRuntime(t, root)
	defer rt.Close()
	for i := 0; i < 40; i++ {
		writeRotationDoc(t, rt, fmt.Sprintf("doc-%d", i), i)
	}
	before := deltaSegmentCount(t, root)

	if _, err := rt.SetHistoryMode(storage.HistoryModeNone, storage.ReclaimUnset); err != nil {
		t.Fatal(err)
	}
	back, err := rt.SetHistoryMode(storage.HistoryModeFull, storage.ReclaimUnset)
	if err != nil {
		t.Fatal(err)
	}
	if back.HistoryLost {
		t.Error("switching back before any compaction reported history lost; nothing was reclaimed")
	}
	if after := deltaSegmentCount(t, root); after != before {
		t.Fatalf("a round trip through none changed the segment count from %d to %d", before, after)
	}
	if got := rt.HistoryMode(); got != storage.HistoryModeFull {
		t.Fatalf("after switching back the namespace is %v", got)
	}
	for i := 0; i < 40; i++ {
		if _, ok := readDoc(t, rt, fmt.Sprintf("doc-%d", i)); !ok {
			t.Fatalf("doc-%d was lost by a round trip that should have been metadata only", i)
		}
	}
}

// Once a compaction has actually run, switching back cannot restore what it
// deleted - and has to say so rather than let "full" be read as "the past is
// back".
func TestSwitchingBackAfterCompactingReportsTheLoss(t *testing.T) {
	root := t.TempDir()
	rt := switchableRuntime(t, root)
	defer rt.Close()
	for i := 0; i < 60; i++ {
		writeRotationDoc(t, rt, fmt.Sprintf("doc-%d", i), i)
	}
	if _, err := rt.SetHistoryMode(storage.HistoryModeNone, storage.ReclaimUnset); err != nil {
		t.Fatal(err)
	}
	compacted, err := rt.CompactHistory()
	if err != nil {
		t.Fatal(err)
	}
	if compacted.Removed == 0 {
		t.Fatal("the explicit compaction reclaimed nothing, so this test proves nothing")
	}

	back, err := rt.SetHistoryMode(storage.HistoryModeFull, storage.ReclaimUnset)
	if err != nil {
		t.Fatal(err)
	}
	if !back.HistoryLost {
		t.Fatal("switching back after a compaction did not report the loss; \"full\" would read as " +
			"\"the history is back\", which it is not")
	}
	// The live dataset is still entirely intact - what was lost is the past,
	// not the present.
	for i := 0; i < 60; i++ {
		if _, ok := readDoc(t, rt, fmt.Sprintf("doc-%d", i)); !ok {
			t.Fatalf("doc-%d is missing; a compaction must not touch live data", i)
		}
	}
}

// A caller can opt into reclaiming as part of the switch, rather than
// landing on manual - but has to say so.
func TestSwitchCanOptIntoReclaiming(t *testing.T) {
	root := t.TempDir()
	rt := switchableRuntime(t, root)
	defer rt.Close()
	for i := 0; i < 20; i++ {
		writeRotationDoc(t, rt, fmt.Sprintf("doc-%d", i), i)
	}
	res, err := rt.SetHistoryMode(storage.HistoryModeNone, storage.ReclaimBalanced)
	if err != nil {
		t.Fatal(err)
	}
	if res.Reclaim != storage.ReclaimBalanced {
		t.Fatalf("an explicit reclaim mode was not honoured: %v", res.Reclaim)
	}
	if got := rt.ReclaimMode(); got != storage.ReclaimBalanced {
		t.Fatalf("the namespace is on %v", got)
	}
}

// Switching to none stops writing tree objects, because none reclaims the
// commits they exist to serve. The switch must be observed by the write path
// rather than only by the reporting accessor.
func TestSwitchingToNoneForcesReplayStrategy(t *testing.T) {
	root := t.TempDir()
	rt := switchableRuntime(t, root)
	defer rt.Close()
	writeRotationDoc(t, rt, "a", 1)
	if _, err := rt.SetHistoryMode(storage.HistoryModeNone, storage.ReclaimUnset); err != nil {
		t.Fatal(err)
	}
	type strategist interface {
		HistoryStrategy() storage.HistoryStrategy
	}
	eng, ok := rt.Storage.(strategist)
	if !ok {
		t.Skip("this runtime has no strategy to report")
	}
	if got := eng.HistoryStrategy(); got != storage.HistoryStrategyReplay {
		t.Fatalf("after switching to none the strategy is %v, want replay", got)
	}
	// Writes still work under the new strategy.
	writeRotationDoc(t, rt, "a", 2)
	if body, ok := readDoc(t, rt, "a"); !ok || body == "" {
		t.Fatal("writing after a mode switch did not read back")
	}
}
