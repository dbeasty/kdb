package server

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

// TestWriteQueueDepthReflectsInFlightWrites covers the load signal the background maintenance
// loop schedules against (embed.MaintenanceOptions.Busy). A depth that stayed 0 while a commit
// held the gate would tell the scheduler the server was idle in exactly the moment it is not,
// which is the one reading that matters.
func TestWriteQueueDepthReflectsInFlightWrites(t *testing.T) {
	rt, err := embed.OpenMemoryRuntime(embed.CatalogFromNamespace("depth/ns"), "depth/ns", schema.None())
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	srv := NewKdbServerRuntime(rt)
	defer srv.Release()

	if got := srv.WriteQueueDepth(); got != 0 {
		t.Fatalf("an idle server reports write queue depth %d, want 0", got)
	}

	release, err := srv.AcquireWriteSlotForTest()
	if err != nil {
		t.Fatalf("acquire write slot: %v", err)
	}
	if got := srv.WriteQueueDepth(); got == 0 {
		t.Fatal("write queue depth is 0 while a write slot is held")
	}

	release()
	if got := srv.WriteQueueDepth(); got != 0 {
		t.Fatalf("after the write finished the depth is %d, want 0", got)
	}
}

// A nil runtime must answer rather than panic: the scheduler's Busy closure runs on a timer for
// the life of the process, and a shutdown that has already torn the runtime down should read as
// "not busy", not as a crash in a background goroutine.
func TestWriteQueueDepthOnNilRuntime(t *testing.T) {
	var srv *KdbServerRuntime
	if got := srv.WriteQueueDepth(); got != 0 {
		t.Fatalf("a nil runtime reports depth %d, want 0", got)
	}
}
