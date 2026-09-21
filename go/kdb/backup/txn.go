package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// The cross-namespace transaction state of a data root: <dataRoot>/txn/ (see
// embed.TxnCoordinator and docs/kdb-cross-namespace-transactions-plan.md §4.2). It is small - a
// host id, an epoch counter and one decision file per epoch that ended without being sealed - and
// it is load-bearing: a namespace log can hold the parts of a group whose writer crashed before
// deciding it, and only the decision file of that epoch says the group never committed. A
// database backup that left it behind would restore a database in which that group's parts read
// as committed.
//
// The directory name is embed's; backup reads it as plain files rather than importing embed, so
// the backup tooling does not have to link the runtime.
const txnDirName = "txn"

// TxnFile is one file of a data root's transaction state within a database backup.
type TxnFile struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// txnFileKey is the object key one transaction state file lives at.
func txnFileKey(backupID, name string) string {
	return "database-backups/" + backupID + "/txn/" + name
}

// backupTxnState copies every regular file under dataRoot/txn into store. A data root that never
// ran a cross-namespace transaction has no such directory, and backs up nothing here.
func backupTxnState(store ObjectStore, backupID, dataRoot string) ([]TxnFile, error) {
	if dataRoot == "" {
		return nil, nil
	}
	dir := filepath.Join(dataRoot, txnDirName)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []TxnFile
	for _, e := range entries {
		// Temp files are an interrupted rename's leftovers, never state.
		if !e.Type().IsRegular() || strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(body)
		if err := store.Put(context.Background(), txnFileKey(backupID, e.Name()), body); err != nil {
			return nil, err
		}
		out = append(out, TxnFile{Name: e.Name(), Size: int64(len(body)), SHA256: hex.EncodeToString(sum[:])})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// RestoreTxnState writes a database backup's transaction state into outDir/txn, verifying every
// file against the manifest first. Run it with outDir's lock held and before any namespace there
// is opened: replay consults this state, and a namespace opened without it would decide its
// groups without it.
func RestoreTxnState(store ObjectStore, m *DatabaseManifest, outDir string) error {
	if len(m.Txn) == 0 {
		return nil
	}
	bodies := make(map[string][]byte, len(m.Txn))
	for _, f := range m.Txn {
		body, err := store.Get(context.Background(), txnFileKey(m.BackupID, f.Name))
		if err != nil {
			return fmt.Errorf("fetching transaction state %s: %w", f.Name, err)
		}
		if err := checkTxnFile(f, body); err != nil {
			return err
		}
		bodies[f.Name] = body
	}
	dir := filepath.Join(outDir, txnDirName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for name, body := range bodies {
		if strings.ContainsAny(name, `/\`) || name == ".." || name == "." {
			return fmt.Errorf("kdb: transaction state file name %q is not a plain file name", name)
		}
		f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		_, werr := f.Write(body)
		serr := f.Sync()
		cerr := f.Close()
		if err := errors.Join(werr, serr, cerr); err != nil {
			return err
		}
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func checkTxnFile(f TxnFile, body []byte) error {
	sum := sha256.Sum256(body)
	if int64(len(body)) != f.Size || hex.EncodeToString(sum[:]) != f.SHA256 {
		return fmt.Errorf("kdb: transaction state file %s does not match its manifest entry (size %d, want %d)",
			f.Name, len(body), f.Size)
	}
	return nil
}

// verifyTxnState checks every transaction state file a database manifest names is present and
// intact.
func verifyTxnState(store ObjectStore, m *DatabaseManifest) []string {
	var problems []string
	for _, f := range m.Txn {
		body, err := store.Get(context.Background(), txnFileKey(m.BackupID, f.Name))
		if err != nil {
			problems = append(problems, fmt.Sprintf("transaction state %s: %v", f.Name, err))
			continue
		}
		if err := checkTxnFile(f, body); err != nil {
			problems = append(problems, err.Error())
		}
	}
	return problems
}
