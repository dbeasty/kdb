package embed_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// TestReplayOpenTimeWithoutCheckpoint measures the crash-recovery open found under issue #43: a
// namespace with many commits and no checkpoint (the process died before the maintenance pass
// wrote one) replays every commit on open, and every replayed commit probes the SSTable store
// for its tree object.
//
// A measurement, not a gate - wall time depends on the machine - so it only runs when asked:
//
//	KDB_REPLAY_COMMITS=14000 go test -run TestReplayOpenTimeWithoutCheckpoint -v ./kdb/embed/
//
// The issue #43 reproducer had 14,736 commits over 24 documents and took 28 s to open.
func TestReplayOpenTimeWithoutCheckpoint(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("KDB_REPLAY_COMMITS"))
	if n <= 0 {
		t.Skip("set KDB_REPLAY_COMMITS to run this measurement")
	}
	const docs = 24
	root := t.TempDir()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.Durability = storage.DurabilityAsync
	rt, err := embed.OpenFileRuntimeWithOptions(root, "bench", "bench/matches", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for i := 0; i < n; i++ {
		b, err := json.Marshal(map[string]any{
			"id":  fmt.Sprintf("00000000-0000-4000-8000-%012d", i%docs),
			"rev": i,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := embed.PutJSONDocument(rt, "bench/matches", string(b)); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("wrote %d commits in %s", n, time.Since(start).Round(time.Millisecond))
	rt.Close()
	removeCheckpoints(t, root)

	start = time.Now()
	rt, err = embed.OpenFileRuntimeWithOptions(root, "bench", "bench/matches", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	defer rt.Close()
	_, _, ok, err := rt.DAG.HeadCommit()
	if err != nil || !ok {
		t.Fatalf("no head after replay: %v", err)
	}
	t.Logf("open with replay of %d commits: %s (%.3f ms/commit)", n, elapsed.Round(time.Millisecond),
		float64(elapsed.Microseconds())/1000/float64(n))
	var tables int
	var tableBytes int64
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.Contains(filepath.ToSlash(path), "/sstable/") {
			tables++
			tableBytes += info.Size()
		}
		return nil
	})
	t.Logf("sstables on disk: %d, %.2f MB", tables, float64(tableBytes)/(1<<20))
}
