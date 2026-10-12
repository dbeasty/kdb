package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/integrity"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
	s3io "github.com/limidus/kdb/go/kdb/storage/io/s3"
)

// A sealed segment's bytes can change without being damaged: kdb-inspect repair-segments
// quarantines frames and rewrites the segment, a kdb:repair/1 record is appended, history=none
// truncation rewrites it. Backups used to store every sealed segment under one shared key (its
// data-directory name), so the next backup re-uploaded the changed segment under the key a base
// backup's manifest names and silently replaced the base's bytes. Objects are now keyed by
// content, so a changed segment gets a new key and nothing a manifest names is ever replaced.

// storeKinds runs a test against both backup targets: a directory, and the S3 object view
// kdb-inspect backup --to s3 uses (sharing its bucket and prefix with a replica sink, as the
// shared KDB_S3_* configuration does).
func storeKinds(t *testing.T, fn func(t *testing.T, store ObjectStore, sink *s3io.ReplicaSink)) {
	t.Run("dir", func(t *testing.T) { fn(t, &DirStore{Root: t.TempDir()}, nil) })
	t.Run("s3", func(t *testing.T) {
		sink := s3io.NewReplicaSink(s3io.NewMemoryBlobStore(), s3io.Config{Bucket: "b", Prefix: "kdb/"})
		fn(t, sink.Objects(), sink)
	})
}

type putCounter struct {
	ObjectStore
	sealed []string
}

func (p *putCounter) Put(ctx context.Context, key string, data []byte) error {
	if !strings.HasSuffix(key, "manifest.json") && !strings.HasSuffix(key, ".prefix") {
		p.sealed = append(p.sealed, key)
	}
	return p.ObjectStore.Put(ctx, key, data)
}

func readSegment(t *testing.T, shim storage.PlatformIOShim, seq int64) []byte {
	t.Helper()
	raw, err := shim.ReadFromSegment(storio.SegmentNameBuilder.DeltaSequenced(ns, seq), 0, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func rewriteSegment(t *testing.T, shim storage.PlatformIOShim, seq int64, raw []byte) {
	t.Helper()
	name := storio.SegmentNameBuilder.DeltaSequenced(ns, seq)
	if err := shim.DeleteSegment(name); err != nil {
		t.Fatal(err)
	}
	appendRaw(t, shim, seq, raw)
}

func appendRaw(t *testing.T, shim storage.PlatformIOShim, seq int64, raw []byte) {
	t.Helper()
	name := storio.SegmentNameBuilder.DeltaSequenced(ns, seq)
	if _, err := shim.AppendToSegment(name, raw); err != nil {
		t.Fatal(err)
	}
	if err := shim.FlushSegment(name); err != nil {
		t.Fatal(err)
	}
}

// fetched returns the bytes a backup restores for segment seq, and the commits it restores.
func fetched(t *testing.T, store ObjectStore, id string, seq int64) ([]byte, int) {
	t.Helper()
	out := t.TempDir()
	if _, err := FetchToDir(store, ns, id, out); err != nil {
		t.Fatalf("restoring backup %s: %v", id, err)
	}
	raw, err := os.ReadFile(filepath.Join(out, filepath.FromSlash(storio.SegmentNameBuilder.DeltaSequenced(ns, seq))))
	if err != nil {
		t.Fatal(err)
	}
	outStore, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &out})
	if err != nil {
		t.Fatal(err)
	}
	commits, err := integrity.ScanVerifiedCommits(storio.NewFileBackedPlatformIO(storio.PlatformIOConfig{RootDirectory: &out}, outStore), ns)
	if err != nil {
		t.Fatal(err)
	}
	return raw, len(commits)
}

func mustVerify(t *testing.T, store ObjectStore, id, what string) {
	t.Helper()
	res, err := Verify(store, ns, id)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Clean() {
		t.Fatalf("%s backup no longer verifies: %v", what, res.Problems)
	}
}

// changedSegmentLog writes segment 0 = [c0, c1, leaf off c0], segment 1 = [c2], segment 2 =
// [c3] (the active one). The leaf has no children, so dropping it breaks nothing and
// repair-segments will remove it.
func changedSegmentLog(t *testing.T) (storage.PlatformIOShim, []document.Commit) {
	t.Helper()
	shim := newTestShim(t)
	commits := chain(t, 4)
	leaf := buildCommit(t, &commits[0].Hash)
	appendSegment(t, shim, 0, commits[0], commits[1], leaf)
	appendSegment(t, shim, 1, commits[2])
	appendSegment(t, shim, 2, commits[3])
	return shim, commits
}

// TestIncrementalOverAChangedSealedSegmentKeepsTheBase is the defect: base backup, the sealed
// segment's bytes change, incremental backup - both must still verify and restore to exactly
// what each recorded, and the unchanged sealed segment must still be shared, not re-uploaded.
func TestIncrementalOverAChangedSealedSegmentKeepsTheBase(t *testing.T) {
	changes := map[string]struct {
		change      func(t *testing.T, shim storage.PlatformIOShim)
		wantCommits int // restored by the incremental (the base restores 5)
	}{
		// kdb-inspect repair-segments: damage the leaf's frame, then let integrity.Repair
		// quarantine it and rewrite the segment without it.
		"repair-quarantines-a-frame": {func(t *testing.T, shim storage.PlatformIOShim) {
			raw := readSegment(t, shim, 0)
			offs := frameOffsetsOf(raw)
			raw[offs[len(offs)-1]+delta.PageFrameHeaderSize+1] ^= 0xff
			rewriteSegment(t, shim, 0, raw)
			report, err := integrity.Verify(shim, ns, integrity.Options{Level: integrity.L1})
			if err != nil {
				t.Fatal(err)
			}
			res, err := integrity.Repair(shim, report, integrity.Options{Level: integrity.L1})
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Steps) != 1 || res.AnyRefused() {
				t.Fatalf("repair did not rewrite segment 0: %+v", res.Steps)
			}
		}, 4},
		// A record appended to a sealed segment (a kdb:repair/1 append, say).
		"record-appended": {func(t *testing.T, shim storage.PlatformIOShim) {
			c0 := chain(t, 1)[0]
			appendRaw(t, shim, 0, rawFrame(t, c0))
		}, 6},
		// A truncation rewrite: same sequence, fewer frames (history=none, or anything else).
		"rewritten-shorter": {func(t *testing.T, shim storage.PlatformIOShim) {
			raw := readSegment(t, shim, 0)
			offs := frameOffsetsOf(raw)
			rewriteSegment(t, shim, 0, raw[:offs[len(offs)-1]])
		}, 4},
	}
	for name, tc := range changes {
		t.Run(name, func(t *testing.T) {
			storeKinds(t, func(t *testing.T, s ObjectStore, _ *s3io.ReplicaSink) {
				shim, _ := changedSegmentLog(t)
				store := &putCounter{ObjectStore: s}
				baseSeg0, baseSeg1 := readSegment(t, shim, 0), readSegment(t, shim, 1)
				base, err := Create(shim, ns, store, "")
				if err != nil {
					t.Fatal(err)
				}

				tc.change(t, shim)
				newSeg0 := readSegment(t, shim, 0)
				if bytes.Equal(newSeg0, baseSeg0) {
					t.Fatal("the change left segment 0's bytes as they were")
				}

				store.sealed = nil
				incr, err := Create(shim, ns, store, base.BackupID)
				if err != nil {
					t.Fatal(err)
				}
				if len(store.sealed) != 1 {
					t.Errorf("incremental uploaded sealed objects %v, want exactly the changed segment 0", store.sealed)
				}
				if incr.Segments[0].Key == base.Segments[0].Key {
					t.Errorf("the changed segment 0 went to the key the base names (%s)", base.Segments[0].Key)
				}
				if incr.Segments[1] != base.Segments[1] {
					t.Errorf("unchanged segment 1 is not shared: base %+v, incremental %+v", base.Segments[1], incr.Segments[1])
				}

				mustVerify(t, store, base.BackupID, "base")
				mustVerify(t, store, incr.BackupID, "incremental")
				for _, c := range []struct {
					what, id string
					seg0     []byte
					commits  int
				}{{"base", base.BackupID, baseSeg0, 5}, {"incremental", incr.BackupID, newSeg0, tc.wantCommits}} {
					got0, n := fetched(t, store, c.id, 0)
					if !bytes.Equal(got0, c.seg0) {
						t.Errorf("%s backup restores segment 0 as %d bytes, not the %d it recorded", c.what, len(got0), len(c.seg0))
					}
					if got1, _ := fetched(t, store, c.id, 1); !bytes.Equal(got1, baseSeg1) {
						t.Errorf("%s backup restores a different segment 1", c.what)
					}
					if n != c.commits {
						t.Errorf("%s backup restores %d commits, want %d", c.what, n, c.commits)
					}
				}
			})
		})
	}
}

func frameOffsetsOf(raw []byte) []int {
	var out []int
	for off := 0; off+delta.PageFrameHeaderSize <= len(raw) && string(raw[off:off+4]) == "KDBP"; {
		n := int(raw[off+8])<<24 | int(raw[off+9])<<16 | int(raw[off+10])<<8 | int(raw[off+11])
		out = append(out, off)
		off += delta.PageFrameHeaderSize + n
	}
	return out
}

// TestSealedSegmentObjectsAreContentAddressed: every sealed object's key carries the hash of
// its bytes, and a backup with no base whose segments are unchanged writes the same keys again
// (identical bytes, so nothing any manifest names changes).
func TestSealedSegmentObjectsAreContentAddressed(t *testing.T) {
	storeKinds(t, func(t *testing.T, store ObjectStore, _ *s3io.ReplicaSink) {
		shim, _ := changedSegmentLog(t)
		a, err := Create(shim, ns, store, "")
		if err != nil {
			t.Fatal(err)
		}
		b, err := Create(shim, ns, store, "")
		if err != nil {
			t.Fatal(err)
		}
		for i, e := range a.Segments {
			if e.VerifiedPrefix {
				continue
			}
			if !strings.Contains(e.Key, e.FileSha256) || e.isLegacySharedKey(ns) {
				t.Errorf("segment %d key %q is not content-addressed", e.Sequence, e.Key)
			}
			if b.Segments[i].Key != e.Key {
				t.Errorf("same bytes, different keys: %s vs %s", e.Key, b.Segments[i].Key)
			}
		}
		mustVerify(t, store, a.BackupID, "first")
		mustVerify(t, store, b.BackupID, "second")
	})
}

// TestReplicaMirrorDoesNotTouchBackupObjects: with S3, the replica sink mirrors live segments
// to <prefix><segment name> - which was exactly the legacy backup key. A sealed segment rewritten
// locally (and mirrored) must not change a backup.
func TestReplicaMirrorDoesNotTouchBackupObjects(t *testing.T) {
	shim, _ := changedSegmentLog(t)
	sink := s3io.NewReplicaSink(s3io.NewMemoryBlobStore(), s3io.Config{Bucket: "b", Prefix: "kdb/"})
	store := sink.Objects()
	m, err := Create(shim, ns, store, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.PutSegment(storio.SegmentNameBuilder.DeltaSequenced(ns, 0), []byte("mirrored, rewritten")); err != nil {
		t.Fatal(err)
	}
	if err := sink.DeleteSegment(storio.SegmentNameBuilder.DeltaSequenced(ns, 1)); err != nil {
		t.Fatal(err)
	}
	mustVerify(t, store, m.BackupID, "mirrored-over")
}

// legacyBackup turns a fresh backup into one in the format older kdb wrote: every sealed
// segment stored under its shared data-directory name, the manifest naming that key.
func legacyBackup(t *testing.T, store ObjectStore, m *Manifest) {
	t.Helper()
	ctx := context.Background()
	for i, e := range m.Segments {
		if e.VerifiedPrefix {
			continue
		}
		data, err := store.Get(ctx, e.Key)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Delete(ctx, e.Key); err != nil {
			t.Fatal(err)
		}
		m.Segments[i].Key = legacySealedSegmentKey(ns, e.Sequence)
		if err := store.Put(ctx, m.Segments[i].Key, data); err != nil {
			t.Fatal(err)
		}
	}
	body, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, ManifestKey(ns, m.BackupID), body); err != nil {
		t.Fatal(err)
	}
}

// TestLegacyManifestsStillVerifyRestoreAndServeAsBases: a manifest naming the old shared keys
// verifies and restores; an incremental over it shares its unchanged segments (no re-upload)
// and puts a changed one under a new key, leaving the legacy object alone.
func TestLegacyManifestsStillVerifyRestoreAndServeAsBases(t *testing.T) {
	storeKinds(t, func(t *testing.T, s ObjectStore, _ *s3io.ReplicaSink) {
		shim, _ := changedSegmentLog(t)
		store := &putCounter{ObjectStore: s}
		baseSeg0 := readSegment(t, shim, 0)
		old, err := Create(shim, ns, store, "")
		if err != nil {
			t.Fatal(err)
		}
		legacyBackup(t, store, old)
		mustVerify(t, store, old.BackupID, "legacy")
		if got, n := fetched(t, store, old.BackupID, 0); !bytes.Equal(got, baseSeg0) || n != 5 {
			t.Fatalf("legacy backup restores %d bytes / %d commits", len(got), n)
		}

		store.sealed = nil
		same, err := Create(shim, ns, store, old.BackupID)
		if err != nil {
			t.Fatal(err)
		}
		if len(store.sealed) != 0 || same.Segments[0] != old.Segments[0] || same.Segments[1] != old.Segments[1] {
			t.Fatalf("incremental over an unchanged legacy base re-uploaded %v / did not share its objects", store.sealed)
		}

		raw := readSegment(t, shim, 0)
		offs := frameOffsetsOf(raw)
		rewriteSegment(t, shim, 0, raw[:offs[len(offs)-1]])
		changed, err := Create(shim, ns, store, old.BackupID)
		if err != nil {
			t.Fatal(err)
		}
		if changed.Segments[0].isLegacySharedKey(ns) {
			t.Fatalf("changed segment uploaded to the legacy shared key %s", changed.Segments[0].Key)
		}
		mustVerify(t, store, old.BackupID, "legacy base")
		mustVerify(t, store, changed.BackupID, "incremental over legacy")
		if got, _ := fetched(t, store, old.BackupID, 0); !bytes.Equal(got, baseSeg0) {
			t.Fatal("legacy base no longer restores its own segment 0")
		}
	})
}

// TestVerifyNamesTheBackupThatOverwroteALegacyObject: the damage older kdb already did is
// detectable from the manifests alone - the base's recorded hash no longer matches, and another
// manifest names the same key with exactly the bytes now there. An incremental over such a broken
// base must not reference the broken object.
func TestVerifyNamesTheBackupThatOverwroteALegacyObject(t *testing.T) {
	storeKinds(t, func(t *testing.T, store ObjectStore, _ *s3io.ReplicaSink) {
		shim, _ := changedSegmentLog(t)
		base, err := Create(shim, ns, store, "")
		if err != nil {
			t.Fatal(err)
		}
		legacyBackup(t, store, base)

		// What older kdb did: the segment changed, the next backup re-uploaded it under the
		// same shared key and recorded the new hash.
		raw := readSegment(t, shim, 0)
		offs := frameOffsetsOf(raw)
		rewriteSegment(t, shim, 0, raw[:offs[len(offs)-1]])
		later, err := Create(shim, ns, store, "")
		if err != nil {
			t.Fatal(err)
		}
		legacyBackup(t, store, later)

		res, err := Verify(store, ns, base.BackupID)
		if err != nil {
			t.Fatal(err)
		}
		if res.Clean() || !strings.Contains(strings.Join(res.Problems, "\n"), "overwritten by backup "+later.BackupID) {
			t.Fatalf("verify of the overwritten base: %v", res.Problems)
		}
		mustVerify(t, store, later.BackupID, "overwriting")

		// Put the base's bytes back locally, so the segment's hash matches the base entry again,
		// and take an incremental over the broken base: the hash match alone must not make it
		// name the overwritten object - it must upload its own.
		rewriteSegment(t, shim, 0, raw)
		incr, err := Create(shim, ns, store, base.BackupID)
		if err != nil {
			t.Fatal(err)
		}
		if incr.Segments[0].isLegacySharedKey(ns) {
			t.Fatalf("incremental references the overwritten legacy object %s", incr.Segments[0].Key)
		}
		mustVerify(t, store, incr.BackupID, "incremental over a broken legacy base")
	})
}
