package server

import (
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

// TestWaitForWritesToDrain covers the graceful-shutdown wait (kdb-finish-up-plan Phase 2.4):
// an in-flight write holds the drain open until it finishes; an empty gate drains immediately.
func TestWaitForWritesToDrain(t *testing.T) {
	rt, err := embed.OpenMemoryRuntime(embed.CatalogFromNamespace("drain/ns"), "drain/ns", schema.None())
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	srv := NewKdbServerRuntime(rt)
	defer srv.Release()

	if !srv.WaitForWritesToDrain(time.Second) {
		t.Fatal("empty write gate should drain immediately")
	}

	release, err := srv.AcquireWriteSlotForTest()
	if err != nil {
		t.Fatalf("acquire write slot: %v", err)
	}
	srv.BeginDraining()

	if srv.WaitForWritesToDrain(150 * time.Millisecond) {
		t.Fatal("drain reported complete while a write slot was still held")
	}

	// Release the in-flight write from the background, as a finishing commit would.
	go func() {
		time.Sleep(50 * time.Millisecond)
		release()
	}()
	if !srv.WaitForWritesToDrain(5 * time.Second) {
		t.Fatal("drain did not complete after the in-flight write released")
	}
}
