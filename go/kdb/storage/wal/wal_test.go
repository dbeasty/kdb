package wal_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/io"
	"github.com/limidus/kdb/go/kdb/storage/wal"
)

func TestAppendAndRecover_roundTrip(t *testing.T) {
	shim := io.NewInMemoryPlatformIO()
	cfg := storage.StorageEngineConfig{GlobalMemoryBudgetBytes: 1_000_000, IOShim: shim}
	w, err := (&wal.DefaultFactory{}).OpenOrCreate("ns1", cfg, shim)
	if err != nil {
		t.Fatal(err)
	}
	sum := document.SHA256Digest([]byte{1, 2, 3})
	hash, err := codec.HashFromBytes(sum)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(wal.Record{
		Timestamp: codec.TimestampNow(),
		Kind:      wal.RecordKindPutBlob,
		Payload:   wal.EncodePutBlob(wal.PutBlob{ContentHash: hash, Bytes: []byte{1, 2, 3}}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	count := 0
	summary, err := w.Recover(func(wal.Record) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.RecordsReplayed != 1 {
		t.Fatalf("recordsReplayed=%d want 1", summary.RecordsReplayed)
	}
	if count != 1 {
		t.Fatalf("handler count=%d want 1", count)
	}
}
