package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
)

// DatabaseManifestFormatVersion is the format version of a database manifest.
const DatabaseManifestFormatVersion = 1

// DatabaseEntry names one namespace's backup within a database backup.
type DatabaseEntry struct {
	NamespaceID string `json:"namespaceId"`
	BackupID    string `json:"backupId"`
}

// DatabaseManifest groups the per-namespace backups taken together under one directory lock into
// a single restorable unit.
//
// This is what a data root with several namespaces needs and a root with one never did. Before
// namespaces could share a root, "the database" and "the namespace" were the same thing, so a
// backup of one was a backup of everything and there was nothing to group. Now that nine
// namespaces sit under one lock, backing them up one at a time gives nine manifests from nine
// different instants - each internally consistent, none consistent with the others - and no
// record of which nine belong together. This is that record: the set, and the instant.
//
// The consistency it claims is exactly the lock's. Create takes the data root's exclusive
// maintenance lock for the whole pass (see embed.LockDataDir), so no writer can advance any
// namespace while it runs, and every namespace in the manifest is captured as of the same
// quiesced moment.
type DatabaseManifest struct {
	FormatVersion int    `json:"formatVersion"`
	BackupID      string `json:"backupId"`
	CreatedAt     string `json:"createdAt"`
	// DataRoot is recorded for provenance only; a restore may target any directory.
	DataRoot string          `json:"dataRoot"`
	Entries  []DatabaseEntry `json:"entries"`
}

// DatabaseManifestKey is the object key a database manifest lives at. Deliberately outside the
// "backups/<namespace>/" prefix that per-namespace manifests use, so listing a namespace's
// backups never turns one of these up.
func DatabaseManifestKey(backupID string) string {
	return "database-backups/" + backupID + "/manifest.json"
}

// CreateDatabase backs up every namespace in namespaces from shim into store and writes the
// database manifest that groups them.
//
// The manifest is written last, and only if every namespace succeeded: a half-written database
// backup that still produced a manifest would be indistinguishable from a complete one at
// restore time. A failure part-way leaves the per-namespace backups that did complete - they are
// individually valid and cost nothing to leave - but no manifest claiming they are a database.
//
// bases, when non-nil, maps a namespace id to the backup id to take an incremental against.
//
// The caller is responsible for holding the data root's lock across this call. It is not taken
// here because the tooling that calls it needs the same lock for the enumeration that produced
// `namespaces` in the first place - taking it twice would be a second, weaker instant.
func CreateDatabase(
	shim storage.PlatformIOShim, namespaces []string, store ObjectStore, dataRoot string, bases map[string]string,
) (*DatabaseManifest, error) {
	if len(namespaces) == 0 {
		return nil, fmt.Errorf("kdb: no namespaces to back up")
	}
	id, err := codec.RandomUUID()
	if err != nil {
		return nil, err
	}
	m := &DatabaseManifest{
		FormatVersion: DatabaseManifestFormatVersion,
		BackupID:      id.String(),
		CreatedAt:     time.Now().UTC().Format(time.RFC3339),
		DataRoot:      dataRoot,
	}
	ordered := append([]string(nil), namespaces...)
	sort.Strings(ordered)
	for _, ns := range ordered {
		nsManifest, err := Create(shim, ns, store, bases[ns])
		if err != nil {
			return nil, fmt.Errorf("backing up %s: %w", ns, err)
		}
		m.Entries = append(m.Entries, DatabaseEntry{NamespaceID: ns, BackupID: nsManifest.BackupID})
	}
	body, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := store.Put(context.Background(), DatabaseManifestKey(m.BackupID), body); err != nil {
		return nil, err
	}
	return m, nil
}

// LoadDatabaseManifest reads one database manifest.
func LoadDatabaseManifest(store ObjectStore, backupID string) (*DatabaseManifest, error) {
	body, err := store.Get(context.Background(), DatabaseManifestKey(backupID))
	if err != nil {
		return nil, err
	}
	var m DatabaseManifest
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, err
	}
	if m.FormatVersion != DatabaseManifestFormatVersion {
		return nil, fmt.Errorf("kdb: database manifest %s has format version %d, want %d",
			backupID, m.FormatVersion, DatabaseManifestFormatVersion)
	}
	return &m, nil
}

// ListDatabaseBackups returns every database backup id in store, sorted.
func ListDatabaseBackups(store ObjectStore) ([]string, error) {
	keys, err := store.List(context.Background(), "database-backups/")
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	var out []string
	for _, k := range keys {
		rest := strings.TrimPrefix(k, "database-backups/")
		id, _, ok := strings.Cut(rest, "/")
		if !ok || id == "" {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

// VerifyDatabase verifies every namespace backup a database manifest names. The result is clean
// only if all of them are: a database backup is only as restorable as its worst namespace.
func VerifyDatabase(store ObjectStore, backupID string) (map[string]*VerifyResult, error) {
	m, err := LoadDatabaseManifest(store, backupID)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*VerifyResult, len(m.Entries))
	for _, e := range m.Entries {
		res, err := Verify(store, e.NamespaceID, e.BackupID)
		if err != nil {
			return out, fmt.Errorf("verifying %s: %w", e.NamespaceID, err)
		}
		out[e.NamespaceID] = res
	}
	return out, nil
}
