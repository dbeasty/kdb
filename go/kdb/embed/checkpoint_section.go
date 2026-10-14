package embed

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/mmapfile"
)

// checkpointSectionFormatVersion keeps the live tree and the commits out of the checkpoint record,
// in a section snapshot of their own that the record names (fields 16-21).
//
// Version 4 put both in the record as byte fields, and the record went through the generic codec
// whole: at 10M documents the packed tree alone is 480 MB, and a close encoded it into a buffer
// (peak heap 2.1 GB) while an open read the file into the heap and decoded it (1.5 GB). Here the
// section is streamed from the tree to the file and, at open, mapped rather than read - or, with a
// tree file, not touched for the tree at all: the record holds the tree's digest, which is what
// maps the tree segment written for it.
//
// Section layout (snapshot key checkpointSectionKey; Kotlin reads the same bytes):
//
//	"KDBCPS01" | treeLen u64 LE | commitsLen u64 LE | packed tree | packed commits | CRC-32 IEEE u32 LE
//
// The CRC covers every byte before it. The packed tree is AppendPacked's image, the packed commits
// dag.PackCheckpointCommits's. Written section first, then the record, then the previous record's
// section deleted: a crash leaves the old record with its own section, or the new one with its.
// A record whose section is missing or does not verify is unusable, so the open replays the log.
const checkpointSectionFormatVersion = 6

var checkpointSectionMagic = [8]byte{'K', 'D', 'B', 'C', 'P', 'S', '0', '1'}

const checkpointSectionHeader = 8 + 8 + 8

// checkpointSection is what a version 6 record says about its section.
type checkpointSection struct {
	key        string
	treeDigest [32]byte
	entries    int
	treeLen    int64
	commitsLen int64
}

// checkpointSectionKey names a section after what it holds, so a new one never overwrites the
// section the current record still names.
func checkpointSectionKey(namespaceID string, treeDigest, commitsDigest [32]byte) string {
	var id [16]byte
	for i := range id {
		id[i] = treeDigest[i] ^ commitsDigest[i]
	}
	return "kdb:checkpoint-section:" + namespaceID + ":" + hex.EncodeToString(id[:])
}

// checkpointPackedCommits is cp's commits packed: a snapshot hands them over packed already, and
// any listed as structs are packed here, so a checkpoint never holds a codec record per commit.
func checkpointPackedCommits(cp namespaceCheckpoint) ([]byte, error) {
	packed := cp.State.PackedCommits
	if len(cp.State.Commits) == 0 {
		return packed, nil
	}
	more, err := dag.PackCheckpointCommits(cp.State.Commits)
	if err != nil {
		return nil, err
	}
	return append(append([]byte(nil), packed...), more...), nil
}

// writeSectionedCheckpoint writes cp as version 6 through shim's snapshot streaming.
func writeSectionedCheckpoint(shim storage.PlatformIOShim, cp namespaceCheckpoint) error {
	commits, err := checkpointPackedCommits(cp)
	if err != nil {
		return err
	}
	// The tree is walked twice, streamed both times: once for the digest the section's key and the
	// record carry, then into the file. Neither pass holds the packed image.
	th := sha256.New()
	treeLen, err := cp.LiveTree.WritePacked(th)
	if err != nil {
		return err
	}
	var treeDigest [32]byte
	copy(treeDigest[:], th.Sum(nil))
	if bound, _ := cp.LiveTree.Paged(); bound {
		if err := document.SaveTreeSegmentDigest(cp.LiveTree, treeDigest); err != nil {
			// The tree segment is an accelerator; the section is the checkpoint's own copy.
			logTreeSegmentFailure(cp.NamespaceID, err)
		}
	}
	section := checkpointSection{
		key:        checkpointSectionKey(cp.NamespaceID, treeDigest, sha256.Sum256(commits)),
		treeDigest: treeDigest,
		entries:    cp.LiveTree.Size(),
		treeLen:    treeLen,
		commitsLen: int64(len(commits)),
	}
	if err := storage.WriteSnapshotStream(shim, section.key, func(w io.Writer) error {
		crc := crc32.NewIEEE()
		mw := io.MultiWriter(w, crc)
		var hdr [checkpointSectionHeader]byte
		copy(hdr[:8], checkpointSectionMagic[:])
		binary.LittleEndian.PutUint64(hdr[8:], uint64(treeLen))
		binary.LittleEndian.PutUint64(hdr[16:], uint64(len(commits)))
		if _, err := mw.Write(hdr[:]); err != nil {
			return err
		}
		n, err := cp.LiveTree.WritePacked(mw)
		if err != nil {
			return err
		}
		if n != treeLen {
			return fmt.Errorf("kdb: checkpoint: the live tree packed to %d bytes, then %d", treeLen, n)
		}
		if _, err := mw.Write(commits); err != nil {
			return err
		}
		var sum [4]byte
		binary.LittleEndian.PutUint32(sum[:], crc.Sum32())
		_, err = w.Write(sum[:])
		return err
	}); err != nil {
		return err
	}
	previous := currentSectionKey(shim, cp.NamespaceID)
	raw, err := encodeCheckpointWith(cp, &section)
	if err != nil {
		return err
	}
	if err := shim.WriteSnapshot(checkpointKey(cp.NamespaceID), raw); err != nil {
		return err
	}
	if previous != "" && previous != section.key {
		_ = shim.DeleteSnapshot(previous)
	}
	return nil
}

// currentSectionKey is the section the checkpoint on disk names, or "" when it is not version 6.
// Read before a new record replaces it, so its section can be deleted once nothing names it.
func currentSectionKey(shim storage.PlatformIOShim, namespaceID string) string {
	raw, err := shim.ReadSnapshot(checkpointKey(namespaceID))
	if err != nil || len(raw) == 0 {
		return ""
	}
	v, err := codec.DecodeBytes(raw, checkpointType, checkpointRegistry())
	if err != nil {
		return ""
	}
	rec, ok := v.(codec.RecordValue)
	if !ok {
		return ""
	}
	if ver, ok := rec.Fields[1].(codec.Int32Value); !ok || ver.V != checkpointSectionFormatVersion {
		return ""
	}
	key, _ := rec.Fields[16].(codec.StringValue)
	return key.V
}

// sectionFromRecord reads a version 6 record's section fields.
func sectionFromRecord(rec codec.RecordValue) (checkpointSection, error) {
	var s checkpointSection
	key, ok := rec.Fields[16].(codec.StringValue)
	if !ok || key.V == "" {
		return s, errors.New("kdb: checkpoint: version 6 without a section key")
	}
	s.key = key.V
	digest, err := cpHashFrom(rec.Fields[17])
	if err != nil {
		return s, err
	}
	s.treeDigest = digest.Bytes
	entries, ok1 := rec.Fields[18].(codec.Int64Value)
	treeLen, ok2 := rec.Fields[19].(codec.Int64Value)
	commitsLen, ok3 := rec.Fields[20].(codec.Int64Value)
	if !ok1 || !ok2 || !ok3 || entries.V < 0 || treeLen.V < 0 || commitsLen.V < 0 ||
		treeLen.V != entries.V*document.PackedTreeEntrySize {
		return s, errors.New("kdb: checkpoint: version 6 with malformed section sizes")
	}
	s.entries, s.treeLen, s.commitsLen = int(entries.V), treeLen.V, commitsLen.V
	return s, nil
}

// readCheckpointSection verifies the section - magic, lengths, CRC and the tree's digest - and
// returns its packed commits, in the heap, plus withTree, which hands its fn the packed tree.
//
// Verified by reading the file through a small buffer, not by mapping it: read pages go to the page
// cache, not to this process's resident set, and a mapped section touched whole for its checksum
// put 484 MB into RSS at a 10M-document open. withTree maps only the tree's bytes, and only if the
// caller needs them - with a tree file the tree is attached by digest and it is never called. A
// store without local files has the section read into the heap, as before.
func readCheckpointSection(shim storage.PlatformIOShim, s checkpointSection) (commits []byte, withTree func(fn func(tree []byte) error) error, err error) {
	want := checkpointSectionHeader + s.treeLen + s.commitsLen + 4
	checkHeader := func(hdr []byte) error {
		if !bytes.Equal(hdr[:8], checkpointSectionMagic[:]) ||
			int64(binary.LittleEndian.Uint64(hdr[8:])) != s.treeLen ||
			int64(binary.LittleEndian.Uint64(hdr[16:])) != s.commitsLen {
			return fmt.Errorf("kdb: checkpoint section %s does not match its record", s.key)
		}
		return nil
	}
	if ss, ok := shim.(storage.SnapshotStreamer); ok {
		if path, ok := ss.LocalSnapshotPath(s.key); ok {
			if commits, err := verifySectionFile(path, s, want, checkHeader); err != nil {
				return nil, nil, err
			} else {
				return commits, func(fn func(tree []byte) error) error { return withSectionTree(path, s, fn) }, nil
			}
		}
	}
	raw, err := shim.ReadSnapshot(s.key)
	if err != nil {
		return nil, nil, err
	}
	if raw == nil {
		return nil, nil, fmt.Errorf("kdb: checkpoint section %s is missing", s.key)
	}
	if int64(len(raw)) != want {
		return nil, nil, fmt.Errorf("kdb: checkpoint section %s is %d bytes, the record says %d", s.key, len(raw), want)
	}
	if err := checkHeader(raw); err != nil {
		return nil, nil, err
	}
	body := raw[:len(raw)-4]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(raw[len(raw)-4:]) {
		return nil, nil, fmt.Errorf("kdb: checkpoint section %s fails its checksum", s.key)
	}
	tree := body[checkpointSectionHeader : checkpointSectionHeader+s.treeLen]
	if sha256.Sum256(tree) != s.treeDigest {
		return nil, nil, fmt.Errorf("kdb: checkpoint section %s holds a different tree than its record names", s.key)
	}
	return body[checkpointSectionHeader+s.treeLen:], func(fn func([]byte) error) error { return fn(tree) }, nil
}

// verifySectionFile streams the section file at path through its checks and returns the commits.
func verifySectionFile(path string, s checkpointSection, want int64, checkHeader func([]byte) error) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("kdb: checkpoint section %s is missing", s.key)
		}
		return nil, err
	}
	defer f.Close()
	if st, err := f.Stat(); err != nil {
		return nil, err
	} else if st.Size() != want {
		return nil, fmt.Errorf("kdb: checkpoint section %s is %d bytes, the record says %d", s.key, st.Size(), want)
	}
	r := bufio.NewReaderSize(f, 1<<20)
	crc := crc32.NewIEEE()
	var hdr [checkpointSectionHeader]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	if err := checkHeader(hdr[:]); err != nil {
		return nil, err
	}
	crc.Write(hdr[:])
	th := sha256.New()
	if _, err := io.CopyN(io.MultiWriter(crc, th), r, s.treeLen); err != nil {
		return nil, err
	}
	if !bytes.Equal(th.Sum(nil), s.treeDigest[:]) {
		return nil, fmt.Errorf("kdb: checkpoint section %s holds a different tree than its record names", s.key)
	}
	commits := make([]byte, s.commitsLen)
	if _, err := io.ReadFull(r, commits); err != nil {
		return nil, err
	}
	crc.Write(commits)
	var sum [4]byte
	if _, err := io.ReadFull(r, sum[:]); err != nil {
		return nil, err
	}
	if crc.Sum32() != binary.LittleEndian.Uint32(sum[:]) {
		return nil, fmt.Errorf("kdb: checkpoint section %s fails its checksum", s.key)
	}
	return commits, nil
}

// withSectionTree hands fn the packed tree of the already-verified section file at path: mapped
// where the platform maps files, read into the heap otherwise. Its digest is checked again on the
// bytes fn gets, since the file was read once and mapped afterwards.
func withSectionTree(path string, s checkpointSection, fn func(tree []byte) error) error {
	use := func(tree []byte) error {
		if sha256.Sum256(tree) != s.treeDigest {
			return fmt.Errorf("kdb: checkpoint section %s changed after it was verified", s.key)
		}
		return fn(tree)
	}
	lo, hi := int(checkpointSectionHeader), int(checkpointSectionHeader+s.treeLen)
	if mmapfile.Supported {
		if m, err := mmapfile.Open(path); err == nil {
			defer m.Close()
			region, err := m.Acquire()
			if err != nil {
				return err
			}
			defer region.Release()
			var inner error
			if gerr := region.Guard(func(b []byte) {
				if len(b) < hi {
					inner = fmt.Errorf("kdb: checkpoint section %s shrank after it was verified", s.key)
					return
				}
				inner = use(b[lo:hi])
			}); gerr != nil {
				return gerr
			}
			return inner
		}
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	tree := make([]byte, s.treeLen)
	if _, err := f.ReadAt(tree, int64(lo)); err != nil {
		return err
	}
	return use(tree)
}

func logTreeSegmentFailure(namespaceID string, err error) {
	log.Printf("kdb: namespace %s: writing the tree segment for the checkpoint: %v", namespaceID, err)
}
