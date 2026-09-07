package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/limidus/kdb/go/kdb/backup"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/recovery"
	"github.com/limidus/kdb/go/kdb/storage"
)

// argValues returns every value passed for a repeatable flag, e.g.
// "--source a=x --source b=y" -> ["a=x", "b=y"].
func argValues(args []string, name string) []string {
	var out []string
	for i := 0; i < len(args); i++ {
		if args[i] == name && i+1 < len(args) {
			out = append(out, args[i+1])
			i++
		}
	}
	return out
}

func restoreCmd(args []string) error {
	namespace := argValue(args, "--namespace")
	outDir := argValue(args, "--out")
	sourceArgs := argValues(args, "--source")
	fromBackup := argValue(args, "--from-backup")
	backupID := argValue(args, "--backup-id")
	databaseID := argValue(args, "--database-backup-id")
	if databaseID != "" {
		comp, err := parseCompression(argValue(args, "--codec"))
		if err != nil {
			return err
		}
		if fromBackup == "" || outDir == "" {
			return fmt.Errorf("usage: kdb-inspect restore --database-backup-id ID --from-backup DIR|s3 --out DIR [--codec zstd|none]")
		}
		return restoreDatabase(fromBackup, databaseID, outDir, comp)
	}
	if namespace == "" || outDir == "" || (len(sourceArgs) == 0 && fromBackup == "") {
		return fmt.Errorf("usage: kdb-inspect restore --namespace NS --out DIR [--source LABEL=PATH ...] [--from-backup DIR|s3 --backup-id ID] [--codec zstd|none]\n" +
			"  --database-backup-id ID restores every namespace in a database backup instead.\n" +
			"  Sources are read in the order given; a damaged local data directory and a\n" +
			"  backup directory are both just paths - list both to hybrid-restore.\n" +
			"  --from-backup fetches a manifest-verified backup (see kdb-inspect backup) and\n" +
			"  adds it as a source; combine with --source local=... for a hybrid restore.")
	}
	if (fromBackup == "") != (backupID == "") {
		return fmt.Errorf("--from-backup and --backup-id must be given together")
	}
	comp, err := parseCompression(argValue(args, "--codec"))
	if err != nil {
		return err
	}

	var sources []recovery.Source
	for _, sa := range sourceArgs {
		label, path, ok := strings.Cut(sa, "=")
		if !ok || label == "" || path == "" {
			return fmt.Errorf("--source must be LABEL=PATH, got %q", sa)
		}
		shim, err := openDirShim(path)
		if err != nil {
			return fmt.Errorf("opening source %q at %s: %w", label, path, err)
		}
		sources = append(sources, recovery.Source{Label: label, Shim: shim})
	}

	if fromBackup != "" {
		store, err := backupStore(fromBackup)
		if err != nil {
			return err
		}
		fetchDir, err := os.MkdirTemp("", "kdb-backup-fetch-*")
		if err != nil {
			return err
		}
		defer os.RemoveAll(fetchDir)
		m, err := backup.FetchToDir(store, namespace, backupID, fetchDir)
		if err != nil {
			return fmt.Errorf("fetching backup %s: %w", backupID, err)
		}
		fmt.Printf("fetched backup %s (%d commits) as restore source\n", m.BackupID, m.CommitCount)
		shim, err := openDirShim(fetchDir)
		if err != nil {
			return err
		}
		sources = append(sources, recovery.Source{Label: "backup:" + backupID, Shim: shim})
	}

	// Hold the out dir's lock while writing into it, so a service can't open it mid-restore.
	release, err := embed.LockDataDir(outDir)
	if err != nil {
		return fmt.Errorf("locking %s: %w", outDir, err)
	}
	defer release()
	outShim, err := openDirShim(outDir)
	if err != nil {
		return err
	}

	result, err := recovery.HybridRestore(sources, namespace, comp, outShim)
	if err != nil {
		return err
	}

	fmt.Printf("restored namespace %s to %s\n", namespace, outDir)
	fmt.Printf("  sources contributing data: %v\n", result.SourcesUsed)
	fmt.Printf("  commits applied: %d\n", result.AppliedCount)
	if len(result.MissingHashes) > 0 {
		fmt.Printf("  WARNING: %d commit(s) could not be resolved from any source and were not applied:\n", len(result.MissingHashes))
		for _, h := range result.MissingHashes {
			fmt.Printf("    %s\n", h)
		}
		fmt.Println("  add another --source (a peer, an older backup) that has them, or accept this as a partial restore")
	}
	return nil
}

// restoreDatabase restores every namespace a database backup names into one out directory.
//
// One lock over the out directory for the whole pass, and the namespaces go into the same root -
// which is what makes the result a database rather than a pile of separately-restored
// namespaces. It is the mirror of CreateDatabase: that captured them under one lock, this puts
// them back under one.
func restoreDatabase(fromBackup, databaseID, outDir string, comp storage.CompressionCodec) error {
	store, err := backupStore(fromBackup)
	if err != nil {
		return err
	}
	m, err := backup.LoadDatabaseManifest(store, databaseID)
	if err != nil {
		return fmt.Errorf("loading database manifest %s: %w", databaseID, err)
	}

	release, err := embed.LockDataDir(outDir)
	if err != nil {
		return fmt.Errorf("locking %s: %w", outDir, err)
	}
	defer release()
	outShim, err := openDirShim(outDir)
	if err != nil {
		return err
	}

	fmt.Printf("restoring database backup %s (taken %s, %d namespaces)\n", m.BackupID, m.CreatedAt, len(m.Entries))
	for _, e := range m.Entries {
		fetchDir, err := os.MkdirTemp("", "kdb-backup-fetch-*")
		if err != nil {
			return err
		}
		nsManifest, err := backup.FetchToDir(store, e.NamespaceID, e.BackupID, fetchDir)
		if err != nil {
			os.RemoveAll(fetchDir)
			return fmt.Errorf("fetching %s backup %s: %w", e.NamespaceID, e.BackupID, err)
		}
		shim, err := openDirShim(fetchDir)
		if err != nil {
			os.RemoveAll(fetchDir)
			return err
		}
		sources := []recovery.Source{{Label: "backup:" + e.BackupID, Shim: shim}}
		result, err := recovery.HybridRestore(sources, e.NamespaceID, comp, outShim)
		os.RemoveAll(fetchDir)
		if err != nil {
			return fmt.Errorf("restoring %s: %w", e.NamespaceID, err)
		}
		fmt.Printf("  %s: %d commit(s) applied (backup had %d)\n",
			e.NamespaceID, result.AppliedCount, nsManifest.CommitCount)
	}
	fmt.Printf("database restored to %s\n", outDir)
	return nil
}
