// Package backup implements kdb-spec-layer15 Component 60's core: a backup is a set of
// objects in an object store plus one manifest, written last, that names a consistent,
// verifiable snapshot of a namespace's delta log (sealed segments in full, the active segment
// as its CRC-verified prefix). Incremental backups reference a base manifest and re-upload only
// what it doesn't already name. Verify re-checks every named object against its recorded
// SHA-256 without restoring. Restore fetches a manifest's objects into a local directory laid
// out exactly like a data dir, so recovery.HybridRestore (and kdb-inspect restore) can consume
// it as an ordinary source.
package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/integrity"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// ObjectStore is the narrow object API a backup target must provide - satisfied by
// s3.ReplicaSink's underlying BlobStore and by DirStore for filesystem targets.
type ObjectStore interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
}

// SegmentEntry names one backed-up segment object.
type SegmentEntry struct {
	Sequence   int64  `json:"sequence"`
	FileSha256 string `json:"fileSha256"`
	SizeBytes  int64  `json:"sizeBytes"`
	Key        string `json:"key"`
	// VerifiedPrefix marks the entry as the active segment's CRC-verified prefix, stored under
	// a backup-scoped key (immutable per backup) rather than the shared sealed-segment key -
	// the segment may still be growing, and a later backup re-uploading a longer version under
	// a shared key would silently invalidate this manifest's recorded hash.
	VerifiedPrefix bool `json:"verifiedPrefix,omitempty"`
}

// Manifest is the one JSON object whose presence defines a backup (written last - see spec
// §6.1). formatVersion 1.
type Manifest struct {
	FormatVersion int               `json:"formatVersion"`
	NamespaceID   string            `json:"namespaceId"`
	BackupID      string            `json:"backupId"`
	BaseBackupID  *string           `json:"baseBackupId"`
	CreatedAt     string            `json:"createdAt"`
	HeadHashes    map[string]string `json:"headHashes"`
	CommitCount   int               `json:"commitCount"`
	Segments      []SegmentEntry    `json:"segments"`
}

// ManifestKey returns the object key a backup's manifest lives at.
func ManifestKey(namespaceID, backupID string) string {
	return "backups/" + namespaceID + "/" + backupID + "/manifest.json"
}

func activePrefixKey(namespaceID, backupID string, seq int64) string {
	return fmt.Sprintf("backups/%s/%s/active-%020d.prefix", namespaceID, backupID, seq)
}

// Create backs namespaceID up from shim into store. baseBackupID, if non-empty, makes this an
// incremental backup: segments already named (same sequence, same hash, full - not prefix) by
// the base manifest are referenced, not re-uploaded. Returns the new manifest (already
// uploaded, last).
func Create(shim storage.PlatformIOShim, namespaceID string, comp storage.CompressionCodec, store ObjectStore, baseBackupID string) (*Manifest, error) {
	ctx := context.Background()
	seqs, err := integrity.ListSequencedSegments(shim, namespaceID)
	if err != nil {
		return nil, err
	}
	if len(seqs) == 0 {
		return nil, fmt.Errorf("backup: namespace %s has no delta segments", namespaceID)
	}

	var base *Manifest
	if baseBackupID != "" {
		base, err = LoadManifest(store, namespaceID, baseBackupID)
		if err != nil {
			return nil, fmt.Errorf("backup: loading base manifest %s: %w", baseBackupID, err)
		}
	}
	baseFull := map[int64]SegmentEntry{}
	if base != nil {
		for _, e := range base.Segments {
			if !e.VerifiedPrefix {
				baseFull[e.Sequence] = e
			}
		}
	}

	backupID, err := codec.RandomUUID()
	if err != nil {
		return nil, err
	}
	m := &Manifest{
		FormatVersion: 1,
		NamespaceID:   namespaceID,
		BackupID:      backupID.String(),
		CreatedAt:     time.Now().UTC().Format(time.RFC3339),
		HeadHashes:    map[string]string{},
	}
	if base != nil {
		id := base.BackupID
		m.BaseBackupID = &id
	}

	// Every segment except the highest-sequence one is sealed and immutable (the writer only
	// ever appends to the newest); those upload in full under their shared, stable key. The
	// highest-sequence segment may still be active, so only its CRC-verified prefix is backed
	// up, under a backup-scoped key (spec §6.1's activeSegmentPrefix).
	allCommits := map[string]document.Commit{}
	for i, seq := range seqs {
		activeCandidate := i == len(seqs)-1
		prefix, _, err := integrity.VerifiedSegmentPrefix(shim, namespaceID, seq, comp)
		if err != nil {
			return nil, fmt.Errorf("backup: segment %d: %w", seq, err)
		}
		commits, err := commitsIn(prefix, comp)
		if err != nil {
			return nil, fmt.Errorf("backup: segment %d: %w", seq, err)
		}
		for h, c := range commits {
			allCommits[h] = c
		}
		sum := sha256.Sum256(prefix)
		sumHex := hex.EncodeToString(sum[:])

		if activeCandidate {
			key := activePrefixKey(namespaceID, m.BackupID, seq)
			if err := store.Put(ctx, key, prefix); err != nil {
				return nil, fmt.Errorf("backup: uploading active prefix %d: %w", seq, err)
			}
			m.Segments = append(m.Segments, SegmentEntry{
				Sequence: seq, FileSha256: sumHex, SizeBytes: int64(len(prefix)), Key: key, VerifiedPrefix: true,
			})
			continue
		}

		key := storio.SegmentNameBuilder.DeltaSequenced(namespaceID, seq)
		if baseEntry, ok := baseFull[seq]; ok && baseEntry.FileSha256 == sumHex {
			// Incremental: already uploaded by the base backup, immutable since - reference it.
			m.Segments = append(m.Segments, baseEntry)
			continue
		}
		if err := store.Put(ctx, key, prefix); err != nil {
			return nil, fmt.Errorf("backup: uploading segment %d: %w", seq, err)
		}
		m.Segments = append(m.Segments, SegmentEntry{
			Sequence: seq, FileSha256: sumHex, SizeBytes: int64(len(prefix)), Key: key,
		})
	}

	m.CommitCount = len(allCommits)
	for i, tip := range tips(allCommits) {
		name := "main"
		if i > 0 {
			name = fmt.Sprintf("tip-%d", i)
		}
		m.HeadHashes[name] = tip
	}

	body, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	// The manifest goes up last: a backup exists only once its manifest does (spec P3).
	if err := store.Put(ctx, ManifestKey(namespaceID, m.BackupID), body); err != nil {
		return nil, fmt.Errorf("backup: uploading manifest: %w", err)
	}
	return m, nil
}

// LoadManifest fetches and parses one backup's manifest.
func LoadManifest(store ObjectStore, namespaceID, backupID string) (*Manifest, error) {
	body, err := store.Get(context.Background(), ManifestKey(namespaceID, backupID))
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("backup manifest %s: %w", backupID, err)
	}
	if m.FormatVersion != 1 {
		return nil, fmt.Errorf("backup manifest %s: unsupported formatVersion %d", backupID, m.FormatVersion)
	}
	return &m, nil
}

// VerifyResult reports a manifest verification (spec §6.3).
type VerifyResult struct {
	BackupID string
	Objects  int
	Problems []string
}

func (r *VerifyResult) Clean() bool { return len(r.Problems) == 0 }

// Verify re-downloads every object the manifest names and re-hashes it against the recorded
// SHA-256 and size - detection without restore (spec §6.3, full depth).
func Verify(store ObjectStore, namespaceID, backupID string) (*VerifyResult, error) {
	m, err := LoadManifest(store, namespaceID, backupID)
	if err != nil {
		return nil, err
	}
	res := &VerifyResult{BackupID: backupID}
	for _, e := range m.Segments {
		res.Objects++
		data, err := store.Get(context.Background(), e.Key)
		if err != nil {
			res.Problems = append(res.Problems, fmt.Sprintf("segment %d (%s): missing: %v", e.Sequence, e.Key, err))
			continue
		}
		if int64(len(data)) != e.SizeBytes {
			res.Problems = append(res.Problems, fmt.Sprintf("segment %d (%s): size %d, manifest says %d", e.Sequence, e.Key, len(data), e.SizeBytes))
			continue
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != e.FileSha256 {
			res.Problems = append(res.Problems, fmt.Sprintf("segment %d (%s): SHA-256 mismatch", e.Sequence, e.Key))
		}
	}
	return res, nil
}

// FetchToDir downloads a backup's segments into outDir laid out exactly like a data directory
// (ns/<namespace>/delta/<sequenced-name>), verifying each object's hash on the way, so the
// result is directly usable as a kdb-inspect restore --source (or even opened read-only). The
// active-prefix object is written under its ordinary sequenced segment name - by construction
// it is a valid segment file (a clean prefix of one).
func FetchToDir(store ObjectStore, namespaceID, backupID, outDir string) (*Manifest, error) {
	m, err := LoadManifest(store, namespaceID, backupID)
	if err != nil {
		return nil, err
	}
	for _, e := range m.Segments {
		data, err := store.Get(context.Background(), e.Key)
		if err != nil {
			return nil, fmt.Errorf("backup fetch: segment %d (%s): %w", e.Sequence, e.Key, err)
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != e.FileSha256 {
			return nil, fmt.Errorf("backup fetch: segment %d (%s): SHA-256 mismatch - refusing to restore corrupt object", e.Sequence, e.Key)
		}
		localName := storio.SegmentNameBuilder.DeltaSequenced(namespaceID, e.Sequence)
		dest := filepath.Join(outDir, filepath.FromSlash(localName))
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(dest, data, 0o644); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// ListBackups returns the backup ids present for namespaceID, newest-last by created-at order
// being unknowable from keys alone, so sorted lexically by id.
func ListBackups(store ObjectStore, namespaceID string) ([]string, error) {
	keys, err := store.List(context.Background(), "backups/"+namespaceID+"/")
	if err != nil {
		return nil, err
	}
	var ids []string
	seen := map[string]bool{}
	for _, k := range keys {
		rest := strings.TrimPrefix(k, "backups/"+namespaceID+"/")
		id, tail, ok := strings.Cut(rest, "/")
		if !ok || tail != "manifest.json" || seen[id] {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

// commitsIn scans a verified segment prefix's frames into commits keyed by hex hash.
func commitsIn(prefix []byte, comp storage.CompressionCodec) (map[string]document.Commit, error) {
	out := map[string]document.Commit{}
	if len(prefix) == 0 {
		return out, nil
	}
	scanned, err := scanSegmentBytes(prefix, comp)
	if err != nil {
		return nil, err
	}
	for h, c := range scanned {
		out[h] = c
	}
	return out, nil
}

// tips returns the hex hashes of every commit that is not any other commit's parent - the
// head(s) of the backed-up history, recorded informationally in the manifest (replay derives
// the real head on open; this lets an operator eyeball what a backup contains).
func tips(all map[string]document.Commit) []string {
	isParent := map[string]bool{}
	for _, c := range all {
		for _, p := range c.ParentHashes {
			isParent[p.Hex()] = true
		}
	}
	var out []string
	for h := range all {
		if !isParent[h] {
			out = append(out, h)
		}
	}
	sort.Strings(out)
	return out
}
