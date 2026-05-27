package engine_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
	"github.com/limidus/kdb/go/kdb/storage/io"
)

func TestWriteBlob_roundTrip(t *testing.T) {
	shim := io.NewInMemoryPlatformIO()
	cfg := storage.StorageEngineConfig{GlobalMemoryBudgetBytes: 8_000_000, IOShim: shim}
	handle, err := engine.DefaultFactory{EngineTarget: engine.TargetInMemory}.Open("test", cfg)
	if err != nil {
		t.Fatal(err)
	}
	bytes := []byte{9, 8, 7}
	hash, err := handle.Adapter().WriteBlob(bytes)
	if err != nil {
		t.Fatal(err)
	}
	got, err := handle.Adapter().ReadBlob(hash)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(bytes) {
		t.Fatalf("len %d want %d", len(got), len(bytes))
	}
	for i := range bytes {
		if got[i] != bytes[i] {
			t.Fatalf("byte %d: %d want %d", i, got[i], bytes[i])
		}
	}
}
