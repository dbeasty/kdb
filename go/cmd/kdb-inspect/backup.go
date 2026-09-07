package main

import (
	"context"
	"fmt"
	"os"

	"github.com/limidus/kdb/go/kdb/backup"
	"github.com/limidus/kdb/go/kdb/embed"
	s3io "github.com/limidus/kdb/go/kdb/storage/io/s3"
)

// backupStore resolves --to into an ObjectStore: a filesystem directory, or "s3" to use the
// KDB_S3_* environment config (the same env the replica sink reads).
func backupStore(to string) (backup.ObjectStore, error) {
	if to == "" {
		return nil, fmt.Errorf("--to is required (a directory path, or 's3' to use KDB_S3_* env config)")
	}
	if to == "s3" {
		cfg := s3io.ConfigFromEnv()
		if cfg == nil {
			return nil, fmt.Errorf("--to s3 requires KDB_S3_BUCKET (and related KDB_S3_*) environment variables")
		}
		sink, err := s3io.OpenReplicaSink(context.Background(), *cfg)
		if err != nil {
			return nil, err
		}
		return sink.Objects(), nil
	}
	return &backup.DirStore{Root: to}, nil
}

func backupCmd(args []string) error {
	dataDir := argValue(args, "--data-dir")
	namespace := argValue(args, "--namespace")
	to := argValue(args, "--to")
	base := argValue(args, "--base-backup-id")
	if dataDir == "" {
		return fmt.Errorf("usage: kdb-inspect backup --data-dir DIR [--namespace NS] --to DIR|s3 [--base-backup-id ID] [--codec zstd|none]\n" +
			"  --namespace may be omitted to back up every namespace under the data root as one database backup")
	}
	store, err := backupStore(to)
	if err != nil {
		return err
	}
	release, err := embed.LockDataDir(dataDir)
	if err != nil {
		return fmt.Errorf("locking %s (stop kdb-service / the embedded runtime first): %w", dataDir, err)
	}
	defer release()
	shim, err := openDirShim(dataDir)
	if err != nil {
		return err
	}
	// No --namespace means the whole database: every namespace under the root, captured inside
	// the single lock taken above. That one lock is the entire point - backing the namespaces up
	// one command at a time would give N manifests from N different instants, each internally
	// consistent and none consistent with the others.
	if namespace == "" {
		namespaces, err := embed.ListNamespaces(dataDir)
		if err != nil {
			return err
		}
		if len(namespaces) == 0 {
			return fmt.Errorf("no namespaces found under %s", dataDir)
		}
		dm, err := backup.CreateDatabase(shim, namespaces, store, dataDir, nil)
		if err != nil {
			return err
		}
		fmt.Printf("database backup complete\n  backupId: %s\n  namespaces: %d\n", dm.BackupID, len(dm.Entries))
		for _, e := range dm.Entries {
			fmt.Printf("  %s -> %s\n", e.NamespaceID, e.BackupID)
		}
		fmt.Printf("restore with: kdb-inspect restore --database-backup-id %s --from-backup %s --out DIR\n", dm.BackupID, to)
		return nil
	}

	m, err := backup.Create(shim, namespace, store, base)
	if err != nil {
		return err
	}
	fmt.Printf("backup complete\n  backupId: %s\n  commits: %d\n  segments: %d\n", m.BackupID, m.CommitCount, len(m.Segments))
	if m.BaseBackupID != nil {
		fmt.Printf("  incremental over: %s\n", *m.BaseBackupID)
	}
	for name, h := range m.HeadHashes {
		fmt.Printf("  head %s: %s\n", name, h)
	}
	return nil
}

func backupVerifyCmd(args []string) error {
	namespace := argValue(args, "--namespace")
	to := argValue(args, "--to")
	id := argValue(args, "--backup-id")
	if namespace == "" || id == "" {
		return fmt.Errorf("usage: kdb-inspect backup-verify --namespace NS --to DIR|s3 --backup-id ID")
	}
	store, err := backupStore(to)
	if err != nil {
		return err
	}
	res, err := backup.Verify(store, namespace, id)
	if err != nil {
		return err
	}
	fmt.Printf("verified backup %s: %d objects\n", res.BackupID, res.Objects)
	if !res.Clean() {
		for _, p := range res.Problems {
			fmt.Printf("  PROBLEM: %s\n", p)
		}
		os.Exit(1)
	}
	fmt.Println("  clean")
	return nil
}

func backupListCmd(args []string) error {
	namespace := argValue(args, "--namespace")
	to := argValue(args, "--to")
	store, err := backupStore(to)
	if err != nil {
		return err
	}
	// No --namespace lists database backups rather than one namespace's.
	if namespace == "" {
		ids, err := backup.ListDatabaseBackups(store)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			fmt.Println("no database backups")
			return nil
		}
		for _, id := range ids {
			m, err := backup.LoadDatabaseManifest(store, id)
			if err != nil {
				fmt.Printf("%s  (manifest unreadable: %v)\n", id, err)
				continue
			}
			fmt.Printf("%s  created=%s namespaces=%d\n", id, m.CreatedAt, len(m.Entries))
		}
		return nil
	}
	ids, err := backup.ListBackups(store, namespace)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		fmt.Println("no backups")
		return nil
	}
	for _, id := range ids {
		m, err := backup.LoadManifest(store, namespace, id)
		if err != nil {
			fmt.Printf("%s  (manifest unreadable: %v)\n", id, err)
			continue
		}
		fmt.Printf("%s  created=%s commits=%d segments=%d\n", id, m.CreatedAt, m.CommitCount, len(m.Segments))
	}
	return nil
}

// backupFetchCmd downloads a backup into a local directory laid out like a data dir, so it can
// be used directly as a `restore --source backup=DIR` source (or inspected).
func backupFetchCmd(args []string) error {
	namespace := argValue(args, "--namespace")
	to := argValue(args, "--to")
	id := argValue(args, "--backup-id")
	out := argValue(args, "--out")
	if namespace == "" || id == "" || out == "" {
		return fmt.Errorf("usage: kdb-inspect backup-fetch --namespace NS --to DIR|s3 --backup-id ID --out DIR")
	}
	store, err := backupStore(to)
	if err != nil {
		return err
	}
	m, err := backup.FetchToDir(store, namespace, id, out)
	if err != nil {
		return err
	}
	fmt.Printf("fetched backup %s (%d segments, %d commits) into %s\n", m.BackupID, len(m.Segments), m.CommitCount, out)
	return nil
}
