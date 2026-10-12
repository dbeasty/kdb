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
	"errors"
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
	"github.com/limidus/kdb/go/kdb/storage/delta"
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
	// a backup-scoped key (immutable per backup) rather than a shared sealed-segment key -
	// the segment may still be growing, and a later backup re-uploading a longer version under
	// a shared key would silently invalidate this manifest's recorded hash.
	VerifiedPrefix bool `json:"verifiedPrefix,omitempty"`
}

// sealedSegmentKey is the object key a sealed segment's bytes live at: the segment's sequence
// AND the SHA-256 of exactly the bytes stored. Two uploads share a key only when they share
// content, so an object under one of these keys never changes once written, and every manifest
// that names it keeps naming the bytes it recorded.
//
// The key is outside both "backups/" (manifests, active prefixes) and the "ns/" tree an S3
// replica sink mirrors live segments into when it shares the bucket and prefix (the replica
// sink and `kdb-inspect backup --to s3` read the same KDB_S3_* configuration).
func sealedSegmentKey(namespaceID string, seq int64, sumHex string) string {
	return fmt.Sprintf("backup-segments/%s/%020d-%s.seg", namespaceID, seq, sumHex)
}

// legacySealedSegmentKey is the key backups made before content addressing stored a sealed
// segment under: the segment's own data-directory name, shared by every backup of that
// sequence whatever its bytes. A later backup of a segment whose bytes had changed (a repair
// that quarantined frames, a truncation rewrite) re-uploaded under the same key and silently
// replaced the bytes an older manifest names. New backups never write it; manifests naming it
// still verify, restore and serve as incremental bases.
func legacySealedSegmentKey(namespaceID string, seq int64) string {
	return storio.SegmentNameBuilder.DeltaSequenced(namespaceID, seq)
}

// isLegacySharedKey reports whether e names a sealed segment under the legacy shared key.
func (e SegmentEntry) isLegacySharedKey(namespaceID string) bool {
	return !e.VerifiedPrefix && e.Key == legacySealedSegmentKey(namespaceID, e.Sequence)
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
//
// A segment damaged in place (storage.TailDamaged) refuses the backup with a
// *DamagedSegmentError before anything is uploaded; a torn tail is left out of the segment's
// upload, since it was never acknowledged.
func Create(shim storage.PlatformIOShim, namespaceID string, store ObjectStore, baseBackupID string) (*Manifest, error) {
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

	// First, classify every segment before anything is uploaded, with the rule replay and
	// kdb-inspect verify apply (storage.TailKind). A segment damaged in place refuses the whole
	// backup: its verified prefix would leave out acknowledged commits, and a manifest naming it
	// would look complete. A torn tail - bytes a crash cut short, never acknowledged - is left
	// out, as it always was, and zero padding is no damage at all. Checking all of them first
	// means a refused backup uploads nothing: not even a full segment under the shared key a base
	// backup's manifest names (an incremental used to overwrite that with the damaged segment's
	// shorter prefix, corrupting the base).
	allCommits := map[string]document.Commit{}
	prefixLen := make([]int, len(seqs))
	for i, seq := range seqs {
		prefix, commits, err := integrity.VerifiedSegmentPrefix(shim, namespaceID, seq)
		if err != nil {
			var damaged *delta.DamagedSegmentError
			if errors.As(err, &damaged) {
				return nil, &DamagedSegmentError{
					NamespaceID: namespaceID, Sequence: damaged.Sequence, Offset: damaged.Offset,
					Reason: damaged.Reason, NextIntactFrame: damaged.NextIntactFrame,
					Incremental: base != nil, Err: damaged,
				}
			}
			return nil, fmt.Errorf("backup: segment %d: %w", seq, err)
		}
		prefixLen[i] = len(prefix)
		for _, c := range commits {
			allCommits[c.CommitHash.Hex()] = c.Commit
		}
	}

	// Every segment except the highest-sequence one is sealed (the writer only ever appends to
	// the newest); those upload in full under a key derived from their content (sealedSegmentKey),
	// since a sealed segment's bytes can still change - a repair that quarantines frames, a
	// truncation rewrite - and a later backup must never replace bytes a base names. The
	// highest-sequence segment may still be active, so only its CRC-verified prefix is backed
	// up, under a backup-scoped key (spec §6.1's activeSegmentPrefix). Each is re-read here
	// rather than held from the pass above, so a backup never holds the whole log in memory; a
	// segment only ever grows by appends, so the classified prefix is still its prefix.
	for i, seq := range seqs {
		activeCandidate := i == len(seqs)-1
		name := storio.SegmentNameBuilder.DeltaSequenced(namespaceID, seq)
		raw, err := shim.ReadFromSegment(name, 0, prefixLen[i])
		if err != nil {
			return nil, fmt.Errorf("backup: segment %d: %w", seq, err)
		}
		if len(raw) < prefixLen[i] {
			return nil, fmt.Errorf("backup: segment %d shrank from %d to %d bytes while it was being backed up", seq, prefixLen[i], len(raw))
		}
		prefix := raw[:prefixLen[i]]
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

		if baseEntry, ok := baseFull[seq]; ok && baseEntry.FileSha256 == sumHex &&
			baseObjectStillHolds(ctx, store, namespaceID, baseEntry) {
			// Incremental: the base backup already stored these exact bytes - reference them.
			m.Segments = append(m.Segments, baseEntry)
			continue
		}
		// Content changed since the base (or there is no base): a key derived from these bytes.
		// The base's object is never touched, so the base still restores to exactly what it
		// recorded.
		key := sealedSegmentKey(namespaceID, seq, sumHex)
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
		sum := sha256.Sum256(data)
		got := hex.EncodeToString(sum[:])
		if int64(len(data)) != e.SizeBytes {
			res.Problems = append(res.Problems, fmt.Sprintf("segment %d (%s): size %d, manifest says %d%s",
				e.Sequence, e.Key, len(data), e.SizeBytes, overwriteExplanation(store, m, e, got)))
			continue
		}
		if got != e.FileSha256 {
			res.Problems = append(res.Problems, fmt.Sprintf("segment %d (%s): SHA-256 mismatch%s",
				e.Sequence, e.Key, overwriteExplanation(store, m, e, got)))
		}
	}
	return res, nil
}

// overwriteExplanation says why a legacy shared object no longer matches m, when the store can
// show it: another backup of the namespace names the same key with exactly the bytes now stored
// there, so that backup's upload replaced this one's (the defect content-addressed keys fixed).
// Empty for a key that is not a legacy shared key.
func overwriteExplanation(store ObjectStore, m *Manifest, e SegmentEntry, gotSha string) string {
	if !e.isLegacySharedKey(m.NamespaceID) {
		return ""
	}
	if ids, err := ListBackups(store, m.NamespaceID); err == nil {
		for _, id := range ids {
			if id == m.BackupID {
				continue
			}
			other, err := LoadManifest(store, m.NamespaceID, id)
			if err != nil {
				continue
			}
			for _, oe := range other.Segments {
				if oe.Key == e.Key && oe.FileSha256 == gotSha {
					return fmt.Sprintf(": the shared object was overwritten by backup %s, which a kdb older "+
						"than content-addressed backup objects uploaded to the same key after the segment's "+
						"bytes changed; this backup's own bytes for segment %d are no longer in the store",
						id, e.Sequence)
				}
			}
		}
	}
	return ": the object at this shared key no longer holds the bytes this backup recorded (a kdb older " +
		"than content-addressed backup objects re-uploaded changed segments under one shared key, and an " +
		"S3 replica mirror sharing the bucket and prefix writes the same key)"
}

// baseObjectStillHolds reports whether an incremental may reference base entry e. A
// content-addressed object is immutable, so yes. A legacy shared key may since have been
// overwritten - by an older kdb's backup, or an S3 replica mirror writing the same key - so it
// is re-read and re-hashed first; when it no longer matches, the incremental uploads the segment
// under its own content key rather than name broken bytes.
func baseObjectStillHolds(ctx context.Context, store ObjectStore, namespaceID string, e SegmentEntry) bool {
	if !e.isLegacySharedKey(namespaceID) {
		return true
	}
	data, err := store.Get(ctx, e.Key)
	if err != nil || int64(len(data)) != e.SizeBytes {
		return false
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]) == e.FileSha256
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
		if got := hex.EncodeToString(sum[:]); got != e.FileSha256 {
			return nil, fmt.Errorf("backup fetch: segment %d (%s): SHA-256 mismatch%s - refusing to restore corrupt object",
				e.Sequence, e.Key, overwriteExplanation(store, m, e, got))
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
