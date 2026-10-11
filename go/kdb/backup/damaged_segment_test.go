package backup_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/backup"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// A backup used to upload only the bytes of each segment before its first bad frame
// (integrity.VerifiedSegmentPrefix). For a torn tail that is right: those bytes were never
// acknowledged. For a segment damaged in place it is the silent loss N5 fixed for startup - the
// backup looked complete and lacked every acknowledged commit after the damage, and restoring it
// put that loss into the restored namespace with no error anywhere.
//
// The rule is now the one replay and verify apply (storage.TailKind): a damaged segment refuses
// the backup with a typed error and nothing is recorded; a torn tail backs up its intact prefix; a
// clean segment, zero padding included, is unchanged.
//
// These use real commits written by the embedded runtime to file-backed storage, uncompressed so
// a test can damage a known frame.

const bkNS = "app/bk"

type bkOptions struct{ preallocate bool }

func bkOpen(t *testing.T, root string, o bkOptions) (*embed.EmbeddedKdbRuntime, error) {
	t.Helper()
	opts := embed.FileRuntimeOptions{}
	none := storage.CompressionNone
	opts.Storage.Compression = &none
	if o.preallocate {
		opts.Storage.PreallocateSegments = true
		opts.Storage.DeltaMaxSegmentBytes = 1 << 20
	}
	return embed.OpenFileRuntimeWithOptions(root, "app", bkNS, schema.None(), opts)
}

// bkRun opens the namespace, writes n documents and closes it. Every open starts a fresh segment.
func bkRun(t *testing.T, root string, o bkOptions, run, n int) []codec.Hash {
	t.Helper()
	rt, err := bkOpen(t, root, o)
	if err != nil {
		t.Fatalf("run %d: open: %v", run, err)
	}
	defer rt.Close()
	var out []codec.Hash
	for i := 0; i < n; i++ {
		res, err := embed.PutJSONDocument(rt, bkNS, fmt.Sprintf(`{"id":"doc-%d-%d"}`, run, i))
		if err != nil {
			t.Fatalf("run %d: put %d: %v", run, i, err)
		}
		out = append(out, res.Commit)
	}
	return out
}

func bkShim(t *testing.T, root string) storage.PlatformIOShim {
	t.Helper()
	store, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		t.Fatal(err)
	}
	return storio.NewFileBackedPlatformIO(storio.PlatformIOConfig{RootDirectory: &root, FsyncOnFlush: true}, store)
}

// bkSegments returns the namespace's delta segment files holding at least one frame, in order.
func bkSegments(t *testing.T, root string) []string {
	t.Helper()
	dir := filepath.Join(root, "ns", "app", "bk", "delta")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".seg") {
			continue
		}
		p := filepath.Join(dir, e.Name())
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if len(frameOffsets(raw)) > 0 {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

func frameOffsets(raw []byte) []int {
	var out []int
	for off := 0; off+delta.PageFrameHeaderSize <= len(raw) && string(raw[off:off+4]) == "KDBP"; {
		end := frameEnd(raw, off)
		if end <= off+delta.PageFrameHeaderSize || end > len(raw) {
			break
		}
		out = append(out, off)
		off = end
	}
	return out
}

func frameEnd(raw []byte, off int) int {
	n := int(raw[off+8])<<24 | int(raw[off+9])<<16 | int(raw[off+10])<<8 | int(raw[off+11])
	return off + delta.PageFrameHeaderSize + n
}

var bkDamages = map[string]func(raw []byte, off int){
	// A garbled header.
	"magic": func(raw []byte, off int) { raw[off] ^= 0xFF },
	// A flipped bit in the body: the frame fails its CRC.
	"body": func(raw []byte, off int) { raw[(off+delta.PageFrameHeaderSize+frameEnd(raw, off))/2] ^= 0x01 },
	// The codec byte, outside the CRC: intact by CRC, will not decode.
	"codec": func(raw []byte, off int) { raw[off+5] = 0x7f },
}

// bkDamage damages frame (of the segment at path) in place and returns where it and the next
// frame start.
func bkDamage(t *testing.T, path string, frame int, kind string) (int, int) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	offs := frameOffsets(raw)
	off, next := offs[frame], offs[frame+1]
	bkDamages[kind](raw, off)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return off, next
}

func seqOf(t *testing.T, path string) int64 {
	t.Helper()
	seq, ok := storio.ParseDeltaSequencedFileName(filepath.Base(path))
	if !ok {
		t.Fatalf("not a sequenced segment name: %s", path)
	}
	return seq
}

// storeObjects lists every object in a DirStore.
func storeObjects(t *testing.T, store *backup.DirStore) []string {
	t.Helper()
	keys, err := store.List(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

// assertBackupRefused checks err is the typed refusal naming the damaged segment, and that the
// refused backup recorded nothing: no manifest, no object.
func assertBackupRefused(t *testing.T, err error, store *backup.DirStore, path string, offset, next int) {
	t.Helper()
	if err == nil {
		t.Fatalf("a backup of a segment damaged in place succeeded; it lacks the acknowledged commits after the damage")
	}
	var refused *backup.DamagedSegmentError
	if !errors.As(err, &refused) {
		t.Fatalf("backup failed, but not with *backup.DamagedSegmentError: %v", err)
	}
	if refused.NamespaceID != bkNS || refused.Sequence != seqOf(t, path) ||
		refused.Offset != int64(offset) || refused.NextIntactFrame != int64(next) {
		t.Fatalf("refusal names %q segment %d byte %d next intact %d; want %q segment %d byte %d next intact %d",
			refused.NamespaceID, refused.Sequence, refused.Offset, refused.NextIntactFrame,
			bkNS, seqOf(t, path), offset, next)
	}
	var damaged *delta.DamagedSegmentError
	if !errors.As(err, &damaged) {
		t.Fatalf("the scanner's *delta.DamagedSegmentError should stay reachable: %v", err)
	}
	msg := err.Error()
	for _, want := range []string{bkNS, fmt.Sprintf("segment %d", refused.Sequence), fmt.Sprintf("byte %d", offset),
		"kdb-inspect verify", "repair-segments", "restore"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("refusal does not mention %q: %v", want, msg)
		}
	}
	ids, lerr := backup.ListBackups(store, bkNS)
	if lerr != nil {
		t.Fatal(lerr)
	}
	if len(ids) != 0 {
		t.Fatalf("a refused backup recorded manifest(s) %v", ids)
	}
	if objs := storeObjects(t, store); len(objs) != 0 {
		t.Fatalf("a refused backup uploaded %v", objs)
	}
}

func TestBackupRefusesADamagedSegment(t *testing.T) {
	for _, where := range []string{"older", "newest"} {
		for _, kind := range []string{"magic", "body", "codec"} {
			t.Run(where+"/"+kind, func(t *testing.T) {
				root := t.TempDir()
				var o bkOptions
				bkRun(t, root, o, 0, 4)
				bkRun(t, root, o, 1, 4)
				segs := bkSegments(t, root)
				if len(segs) != 2 {
					t.Fatalf("want one segment per run, got %d", len(segs))
				}
				victim := segs[0]
				if where == "newest" {
					victim = segs[1]
				}
				off, next := bkDamage(t, victim, 1, kind) // the second of four frames

				store := &backup.DirStore{Root: t.TempDir()}
				_, err := backup.Create(bkShim(t, root), bkNS, store, "")
				assertBackupRefused(t, err, store, victim, off, next)
			})
		}
	}
}

// A database backup (every namespace under one lock) goes through Create per namespace, so it
// refuses too, and writes no database manifest.
func TestDatabaseBackupRefusesADamagedSegment(t *testing.T) {
	root := t.TempDir()
	var o bkOptions
	bkRun(t, root, o, 0, 4)
	segs := bkSegments(t, root)
	off, next := bkDamage(t, segs[0], 1, "body")

	store := &backup.DirStore{Root: t.TempDir()}
	_, err := backup.CreateDatabase(bkShim(t, root), []string{bkNS}, store, root, nil)
	assertBackupRefused(t, err, store, segs[0], off, next)
	if ids, _ := backup.ListDatabaseBackups(store); len(ids) != 0 {
		t.Fatalf("a refused database backup recorded %v", ids)
	}
}

// An incremental backup over a segment that was damaged after its base was taken. The old code
// uploaded the damaged segment's shorter prefix under the segment's shared key - the key the base
// backup's manifest names - so it also corrupted the base.
func TestIncrementalBackupRefusesASegmentDamagedSinceItsBase(t *testing.T) {
	root := t.TempDir()
	var o bkOptions
	bkRun(t, root, o, 0, 4)
	bkRun(t, root, o, 1, 4)
	bkRun(t, root, o, 2, 2)
	segs := bkSegments(t, root)

	store := &backup.DirStore{Root: t.TempDir()}
	first, err := backup.Create(bkShim(t, root), bkNS, store, "")
	if err != nil {
		t.Fatal(err)
	}
	before := storeObjects(t, store)

	off, next := bkDamage(t, segs[0], 1, "body")
	_, err = backup.Create(bkShim(t, root), bkNS, store, first.BackupID)
	var refused *backup.DamagedSegmentError
	if err == nil {
		t.Error("an incremental backup over a damaged segment succeeded")
	} else if !errors.As(err, &refused) || !refused.Incremental || refused.Sequence != seqOf(t, segs[0]) ||
		refused.Offset != int64(off) || refused.NextIntactFrame != int64(next) {
		t.Errorf("want an incremental *backup.DamagedSegmentError naming segment %d byte %d, got %v", seqOf(t, segs[0]), off, err)
	}
	if ids, _ := backup.ListBackups(store, bkNS); len(ids) != 1 || ids[0] != first.BackupID {
		t.Errorf("the incremental recorded a manifest: %v", ids)
	}
	if after := storeObjects(t, store); strings.Join(after, ",") != strings.Join(before, ",") {
		t.Errorf("the incremental changed the store:\nbefore %v\nafter  %v", before, after)
	}
	res, err := backup.Verify(store, bkNS, first.BackupID)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Clean() {
		t.Fatalf("the incremental damaged its base backup: %v", res.Problems)
	}
}

// bkRestoreAndOpen fetches a backup into a fresh data directory and opens it.
func bkRestoreAndOpen(t *testing.T, store *backup.DirStore, id string, o bkOptions) *embed.EmbeddedKdbRuntime {
	t.Helper()
	out := t.TempDir()
	if _, err := backup.FetchToDir(store, bkNS, id, out); err != nil {
		t.Fatal(err)
	}
	rt, err := bkOpen(t, out, o)
	if err != nil {
		t.Fatalf("opening the restored backup: %v", err)
	}
	t.Cleanup(rt.Close)
	return rt
}

func assertHistory(t *testing.T, rt *embed.EmbeddedKdbRuntime, head codec.Hash, have, lost []codec.Hash) {
	t.Helper()
	if got, _ := rt.DAG.Head(); got != head {
		t.Fatalf("head %s, want %s", got.Hex(), head.Hex())
	}
	for i, h := range have {
		if _, ok := rt.DAG.GetCommit(h); !ok {
			t.Fatalf("acknowledged commit %d (%s) is missing from the restored backup", i, h.Hex())
		}
	}
	for i, h := range lost {
		if _, ok := rt.DAG.GetCommit(h); ok {
			t.Fatalf("commit %d (%s) should have been dropped with the torn tail", i, h.Hex())
		}
	}
}

func cat(parts ...[]codec.Hash) []codec.Hash {
	var out []codec.Hash
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// tearLastFrame models an append that never completed (see embed's N5 tests).
func tearLastFrame(t *testing.T, path, shape string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	offs := frameOffsets(raw)
	off := offs[len(offs)-1]
	end := frameEnd(raw, off)
	switch shape {
	case "cut-short":
		raw = raw[:end-5]
	case "body-unwritten":
		for i := (off + delta.PageFrameHeaderSize + end) / 2; i < end; i++ {
			raw[i] = 0
		}
	case "header-only":
		for i := off + 4; i < end; i++ {
			raw[i] = 0
		}
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

// A torn tail is a crash: the bytes were never acknowledged, so backing up the intact prefix is
// correct, and restoring it gives every acknowledged commit - in the newest segment and, the kill
// -9 sequence, in an older one after a restart wrote more on top.
func TestBackupOfATornTailRestoresEveryAcknowledgedCommit(t *testing.T) {
	for _, shape := range []string{"cut-short", "body-unwritten", "header-only"} {
		t.Run("newest/"+shape, func(t *testing.T) {
			root := t.TempDir()
			var o bkOptions
			first := bkRun(t, root, o, 0, 3)
			second := bkRun(t, root, o, 1, 3)
			tearLastFrame(t, bkSegments(t, root)[1], shape)

			store := &backup.DirStore{Root: t.TempDir()}
			m, err := backup.Create(bkShim(t, root), bkNS, store, "")
			if err != nil {
				t.Fatalf("a torn tail must still back up: %v", err)
			}
			if m.CommitCount != 5 {
				t.Fatalf("backed up %d commits, want 5", m.CommitCount)
			}
			rt := bkRestoreAndOpen(t, store, m.BackupID, o)
			assertHistory(t, rt, second[1], cat(first, second[:2]), second[2:])
		})
		t.Run("older-then-restart/"+shape, func(t *testing.T) {
			root := t.TempDir()
			var o bkOptions
			first := bkRun(t, root, o, 0, 3)
			tearLastFrame(t, bkSegments(t, root)[0], shape)
			removeCheckpoints(t, root) // the killed run never wrote one
			second := bkRun(t, root, o, 1, 3)

			store := &backup.DirStore{Root: t.TempDir()}
			m, err := backup.Create(bkShim(t, root), bkNS, store, "")
			if err != nil {
				t.Fatalf("the kill -9 sequence must still back up: %v", err)
			}
			rt := bkRestoreAndOpen(t, store, m.BackupID, o)
			assertHistory(t, rt, second[2], cat(first[:2], second), first[2:])
		})
	}
}

func removeCheckpoints(t *testing.T, root string) {
	t.Helper()
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.Contains(filepath.ToSlash(path), "/snap/kdb_checkpoint_") {
			return os.Remove(path)
		}
		return nil
	})
}

// A preallocated segment ends in zero padding, which is no damage: it backs up, and restores.
func TestBackupOfAPreallocatedSegment(t *testing.T) {
	root := t.TempDir()
	o := bkOptions{preallocate: true}
	first := bkRun(t, root, o, 0, 3)
	second := bkRun(t, root, o, 1, 3)
	segs := bkSegments(t, root)
	raw, err := os.ReadFile(segs[1])
	if err != nil {
		t.Fatal(err)
	}
	offs := frameOffsets(raw)
	if end := frameEnd(raw, offs[len(offs)-1]); end >= len(raw) {
		t.Fatalf("the segment is not preallocated (frames end at %d of %d bytes)", end, len(raw))
	}

	store := &backup.DirStore{Root: t.TempDir()}
	m, err := backup.Create(bkShim(t, root), bkNS, store, "")
	if err != nil {
		t.Fatalf("a preallocated segment must back up: %v", err)
	}
	if m.CommitCount != 6 {
		t.Fatalf("backed up %d commits, want 6", m.CommitCount)
	}
	rt := bkRestoreAndOpen(t, store, m.BackupID, o)
	all := cat(first, second)
	assertHistory(t, rt, all[len(all)-1], all, nil)
}
