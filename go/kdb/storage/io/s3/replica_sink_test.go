package s3

import (
	"context"
	"testing"
)

func TestReplicaSink_putAndDelete(t *testing.T) {
	blobs := NewMemoryBlobStore()
	sink := NewReplicaSink(blobs, Config{Prefix: "p"})
	seg := "ns/foo/delta/id"
	if err := sink.PutSegment(seg, []byte("data")); err != nil {
		t.Fatal(err)
	}
	got, err := blobs.Get(context.Background(), "p/"+seg)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "data" {
		t.Fatalf("got %q", got)
	}
	if err := sink.DeleteSegment(seg); err != nil {
		t.Fatal(err)
	}
	got, _ = blobs.Get(context.Background(), "p/"+seg)
	if got != nil {
		t.Fatalf("expected deleted, got %v", got)
	}
}

func TestConfigFromEnv_disabledWithoutBucket(t *testing.T) {
	t.Setenv("KDB_S3_BUCKET", "")
	if ConfigFromEnv() != nil {
		t.Fatal("expected nil without bucket")
	}
}

func TestConfigFromEnv_readsBucket(t *testing.T) {
	t.Setenv("KDB_S3_BUCKET", "my-bucket")
	t.Setenv("KDB_S3_ENDPOINT", "http://localhost:4566")
	cfg := ConfigFromEnv()
	if cfg == nil || cfg.Bucket != "my-bucket" {
		t.Fatalf("cfg: %+v", cfg)
	}
	if !cfg.PathStyle || !cfg.EnsureBucket {
		t.Fatalf("local endpoint flags: %+v", cfg)
	}
}
