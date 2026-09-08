package embed_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// The measurement the whole retention half exists to produce: on-disk
// footprint as a function of history length, under each mode.
//
// Skipped under -short. Run it alone - never alongside another heavy job -
// or contention makes the numbers meaningless:
//
//	go test ./kdb/embed/ -run TestFootprintByMode -v
//
// Measured 2026-09-08 at 25 documents of ~512 bytes, over 10 and 40
// sessions:
//
//	mode  sessions  total bytes  delta log
//	full        10       182603      60400
//	full        40       724074     242350
//	none        10        86201       6040
//	none        40       319909       6065
//
// The delta log is the part this work bounds, and it does: full grows x4.01
// for 4x the history, none holds at x1.00.
//
// The *total* still grows under none, and the assertions below deliberately
// do not claim otherwise. The reason is not retention: every memtable flush
// writes another SSTable holding the live dataset, and the Go tree has no
// SSTable compaction to merge them (kdb-storage-compaction exists only in
// the Kotlin tree). So history=none's honest claim today is "the delta log
// is a function of the retention window, not of history" - not yet "the
// whole footprint is a function of the dataset".
func TestFootprintByMode(t *testing.T) {
	if testing.Short() {
		t.Skip("footprint measurement is slow")
	}
	const (
		sessions     = 40
		writesEach   = 25
		bodyPadding  = 512
		shortHistory = 10
	)

	measure := func(mode storage.HistoryMode, window storage.RetentionWindow, n int) (total, delta int64) {
		root := t.TempDir()
		for s := 0; s < n; s++ {
			opts := embed.FileRuntimeOptions{}
			opts.Storage.MemoryBudgetBytes = 8 << 20
			opts.Storage.HistoryMode = mode
			opts.Storage.Retain = window
			rt, err := embed.OpenFileRuntimeWithOptions(root, "app", "app/docs", schema.None(), opts)
			if err != nil {
				t.Fatal(err)
			}
			for w := 0; w < writesEach; w++ {
				body := fmt.Sprintf(`{"id":"doc-%d","session":%d,"pad":%q}`,
					w, s, padding(bodyPadding))
				if _, err := embed.PutJSONDocument(rt, "app/docs", body); err != nil {
					t.Fatal(err)
				}
			}
			rt.Close()
		}
		return dirBytes(t, root), dirBytes(t, filepath.Join(root, "ns", "app", "docs", "delta"))
	}

	fullShort, fullShortDelta := measure(storage.HistoryModeFull, storage.RetentionWindow{}, shortHistory)
	fullLong, fullLongDelta := measure(storage.HistoryModeFull, storage.RetentionWindow{}, sessions)
	noneShort, noneShortDelta := measure(
		storage.HistoryModeNone, storage.RetentionWindow{Duration: storage.RetainNothing}, shortHistory)
	noneLong, noneLongDelta := measure(
		storage.HistoryModeNone, storage.RetentionWindow{Duration: storage.RetainNothing}, sessions)

	t.Logf("dataset: %d documents, ~%d bytes each", writesEach, bodyPadding)
	t.Logf("%-6s %9s %12s %12s", "mode", "sessions", "total bytes", "delta log")
	t.Logf("%-6s %9d %12d %12d", "full", shortHistory, fullShort, fullShortDelta)
	t.Logf("%-6s %9d %12d %12d", "full", sessions, fullLong, fullLongDelta)
	t.Logf("%-6s %9d %12d %12d", "none", shortHistory, noneShort, noneShortDelta)
	t.Logf("%-6s %9d %12d %12d", "none", sessions, noneLong, noneLongDelta)

	// The claim: under full, the delta log grows roughly with history;
	// under none with a zero window, it does not.
	fullGrowth := float64(fullLongDelta) / float64(fullShortDelta)
	noneGrowth := float64(noneLongDelta) / float64(noneShortDelta)
	t.Logf("delta log growth from %d to %d sessions: full x%.2f, none x%.2f",
		shortHistory, sessions, fullGrowth, noneGrowth)

	if fullGrowth < 2 {
		t.Fatalf("history=full should grow its log with history, grew x%.2f", fullGrowth)
	}
	if noneGrowth > 1.5 {
		t.Fatalf("history=none should hold its log roughly constant, grew x%.2f", noneGrowth)
	}
	if noneLongDelta >= fullLongDelta {
		t.Fatalf("history=none kept %d bytes of log against full's %d", noneLongDelta, fullLongDelta)
	}
}

func padding(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'x'
	}
	return string(b)
}

func dirBytes(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	err := filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return total
}
