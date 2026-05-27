package io

import (
	"path/filepath"
	"testing"

	s3io "github.com/limidus/kdb/go/kdb/storage/io/s3"
)

func TestPrimaryWithReplicas_sealedSegmentReplicated(t *testing.T) {
	root := t.TempDir()
	primary, err := NewOSByteStore(PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		t.Fatal(err)
	}
	blobs := s3io.NewMemoryBlobStore()
	replica := s3io.NewReplicaSink(blobs, s3io.Config{Prefix: "kdb"})
	store := NewPrimaryWithReplicas(primary, []ReplicaSink{replica}, ReplicationPolicy{})

	seg := "ns/testns/delta/00000000-0000-4000-8000-000000000001"
	payload := []byte("hello-delta-frame")
	if _, err := store.Append(seg, payload); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkSealed(seg); err != nil {
		t.Fatal(err)
	}

	got, err := primary.Read(seg, 0, len(payload))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("primary: got %q", got)
	}

	key := "kdb/" + seg
	replicaBytes, err := blobs.Get(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	if string(replicaBytes) != string(payload) {
		t.Fatalf("replica: got %q", replicaBytes)
	}
}

func TestPrimaryWithReplicas_snapshotReplicated(t *testing.T) {
	root := t.TempDir()
	primary, err := NewOSByteStore(PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		t.Fatal(err)
	}
	blobs := s3io.NewMemoryBlobStore()
	replica := s3io.NewReplicaSink(blobs, s3io.Config{})
	store := NewPrimaryWithReplicas(primary, []ReplicaSink{replica}, ReplicationPolicy{})

	data := []byte("snap-payload")
	if err := store.WriteSnapshot("kdb:snap:abc", data); err != nil {
		t.Fatal(err)
	}

	local, err := primary.ReadSnapshot("kdb:snap:abc")
	if err != nil {
		t.Fatal(err)
	}
	if string(local) != string(data) {
		t.Fatalf("primary snapshot: %q", local)
	}

	replicaBytes, err := blobs.Get(t.Context(), "snapshots/kdb:snap:abc")
	if err != nil {
		t.Fatal(err)
	}
	if string(replicaBytes) != string(data) {
		t.Fatalf("replica snapshot: %q", replicaBytes)
	}
	_ = filepath.Join(root, "snapshots") // ensure primary layout unchanged
}

func TestPrimaryWithReplicas_listFromPrimaryOnly(t *testing.T) {
	root := t.TempDir()
	primary, err := NewOSByteStore(PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		t.Fatal(err)
	}
	blobs := s3io.NewMemoryBlobStore()
	replica := s3io.NewReplicaSink(blobs, s3io.Config{})
	store := NewPrimaryWithReplicas(primary, []ReplicaSink{replica}, ReplicationPolicy{})

	seg := "ns/ns1/delta/seg1"
	if _, err := store.Append(seg, []byte("x")); err != nil {
		t.Fatal(err)
	}
	list, err := store.List("ns/ns1/")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0] != seg {
		t.Fatalf("list: %v", list)
	}
}
