package io

import (
	"testing"
)

// newTestPrimary is an OS-backed byte store under a temp root, matching what the other tests in
// this package use as a primary.
func newTestPrimary(t *testing.T) SegmentByteStore {
	t.Helper()
	root := t.TempDir()
	primary, err := NewOSByteStore(PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		t.Fatal(err)
	}
	return primary
}

// A replica and an archive differ in exactly one behaviour, and it is the one
// that decides whether reclaiming is eviction or destruction.

// recordingSink is a ReplicaSink that remembers what it holds, and can hand it back.
type recordingSink struct {
	segments  map[string][]byte
	snapshots map[string][]byte
}

func newRecordingSink() *recordingSink {
	return &recordingSink{segments: map[string][]byte{}, snapshots: map[string][]byte{}}
}

func (r *recordingSink) PutSegment(name string, data []byte) error {
	r.segments[name] = append([]byte(nil), data...)
	return nil
}
func (r *recordingSink) DeleteSegment(name string) error { delete(r.segments, name); return nil }
func (r *recordingSink) WriteSnapshot(k string, d []byte) error {
	r.snapshots[k] = append([]byte(nil), d...)
	return nil
}
func (r *recordingSink) DeleteSnapshot(k string) error { delete(r.snapshots, k); return nil }
func (r *recordingSink) GetSegment(name string) ([]byte, error) {
	return r.segments[name], nil
}

func seedSegment(t *testing.T, store *PrimaryWithReplicas, name string) {
	t.Helper()
	if _, err := store.Append(name, []byte("commit-bytes")); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkSealed(name); err != nil {
		t.Fatal(err)
	}
}

// The whole point: deleting a segment must clear it from a replica and leave it in an archive.
func TestDeletionSkipsArchivesButNotReplicas(t *testing.T) {
	replica, archive := newRecordingSink(), newRecordingSink()
	store := NewPrimaryWithSinks(newTestPrimary(t), []RoledSink{
		{Sink: replica, Role: SinkReplica},
		{Sink: archive, Role: SinkArchive},
	}, ReplicationPolicy{})

	seedSegment(t, store, "ns/app/delta/0001.seg")
	if len(replica.segments) != 1 || len(archive.segments) != 1 {
		t.Fatalf("both sinks should have the sealed segment: replica=%d archive=%d",
			len(replica.segments), len(archive.segments))
	}

	if err := store.Delete("ns/app/delta/0001.seg"); err != nil {
		t.Fatal(err)
	}
	if len(replica.segments) != 0 {
		t.Error("a replica kept a segment the primary deleted; it is meant to mirror the primary")
	}
	if len(archive.segments) != 1 {
		t.Fatal("an archive lost the segment the primary deleted, so the compaction destroyed the " +
			"only copy that could have restored it")
	}
}

// Snapshots follow the same rule - a checkpoint the archive still holds is what makes a restored
// range openable.
func TestSnapshotDeletionSkipsArchives(t *testing.T) {
	replica, archive := newRecordingSink(), newRecordingSink()
	store := NewPrimaryWithSinks(newTestPrimary(t), []RoledSink{
		{Sink: replica, Role: SinkReplica},
		{Sink: archive, Role: SinkArchive},
	}, ReplicationPolicy{})

	if err := store.WriteSnapshot("chk", []byte("checkpoint")); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteSnapshot("chk"); err != nil {
		t.Fatal(err)
	}
	if len(replica.snapshots) != 0 {
		t.Error("a replica kept a snapshot the primary deleted")
	}
	if len(archive.snapshots) != 1 {
		t.Error("an archive lost a snapshot, so a restored segment range would have no checkpoint")
	}
}

// And the segment comes back, which is what the archive is for.
func TestRestoreSegmentBringsItBack(t *testing.T) {
	archive := newRecordingSink()
	primary := newTestPrimary(t)
	store := NewPrimaryWithSinks(primary, []RoledSink{{Sink: archive, Role: SinkArchive}},
		ReplicationPolicy{})

	name := "ns/app/delta/0001.seg"
	seedSegment(t, store, name)
	if err := store.Delete(name); err != nil {
		t.Fatal(err)
	}
	if _, err := primary.Read(name, 0, 4); err == nil {
		t.Fatal("the segment is still on the primary, so this proves nothing")
	}

	if err := store.RestoreSegment(name); err != nil {
		t.Fatal(err)
	}
	got, err := primary.Read(name, 0, len("commit-bytes"))
	if err != nil {
		t.Fatalf("reading the restored segment: %v", err)
	}
	if string(got) != "commit-bytes" {
		t.Fatalf("the restored segment reads %q", got)
	}
}

// Restoring without an archive has to fail clearly rather than silently doing nothing - a
// replica does not hold what was deleted, so asking it would only produce a confusing miss.
func TestRestoreWithoutAnArchiveFails(t *testing.T) {
	replica := newRecordingSink()
	store := NewPrimaryWithSinks(newTestPrimary(t),
		[]RoledSink{{Sink: replica, Role: SinkReplica}}, ReplicationPolicy{})

	if err := store.RestoreSegment("ns/app/delta/0001.seg"); err == nil {
		t.Fatal("restoring succeeded with no archive configured")
	}
	if store.HasArchive() {
		t.Error("a store with only replicas reported that it has an archive, so a compaction " +
			"would describe itself as reversible when it is not")
	}
}

// A write-only archive is a contradiction, and the error has to say so at the moment somebody
// needs the data.
func TestWriteOnlyArchiveIsReportedNotSilent(t *testing.T) {
	store := NewPrimaryWithSinks(newTestPrimary(t),
		[]RoledSink{{Sink: struct{ ReplicaSink }{newRecordingSink()}, Role: SinkArchive}},
		ReplicationPolicy{})

	err := store.RestoreSegment("ns/app/delta/0001.seg")
	if err == nil {
		t.Fatal("restoring from a write-only archive reported success")
	}
}

// HasArchive is what a caller uses to say whether a compaction is reversible, so it has to be
// right in the affirmative case too.
func TestHasArchiveReportsAnArchive(t *testing.T) {
	store := NewPrimaryWithSinks(newTestPrimary(t),
		[]RoledSink{{Sink: newRecordingSink(), Role: SinkArchive}}, ReplicationPolicy{})
	if !store.HasArchive() {
		t.Fatal("a store with an archive says it has none")
	}
}
