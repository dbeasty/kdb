package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/integrity"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// openDirShim opens a plain, unlocked, OS-backed segment shim rooted at dataDir - no S3
// replication. Commands that operate on a live data directory (verify, repair-segments, backup,
// restore's --out) take embed.LockDataDir first, so they cannot race a running kdb-service or
// embedded runtime (kdb-finish-up-plan Phase 2.11 closed the earlier no-locking gap).
func openDirShim(dataDir string) (storage.PlatformIOShim, error) {
	root := dataDir
	store, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		return nil, err
	}
	return storio.NewFileBackedPlatformIO(storio.PlatformIOConfig{RootDirectory: &root, FsyncOnFlush: true}, store), nil
}

func parseCompression(s string) (storage.CompressionCodec, error) {
	switch s {
	case "", "zstd":
		return storage.CompressionZSTD, nil
	case "none":
		return storage.CompressionNone, nil
	default:
		return 0, fmt.Errorf("unknown --codec %q (want zstd or none)", s)
	}
}

func parseLevel(s string) (integrity.Level, error) {
	switch s {
	case "", "L2":
		return integrity.L2, nil
	case "L1":
		return integrity.L1, nil
	default:
		return "", fmt.Errorf("unknown --level %q (want L1 or L2)", s)
	}
}

func verifyCmd(args []string) error {
	dataDir := argValue(args, "--data-dir")
	namespace := argValue(args, "--namespace")
	if dataDir == "" || namespace == "" {
		return fmt.Errorf("usage: kdb-inspect verify --data-dir DIR --namespace NS [--level L1|L2] [--json]")
	}
	level, err := parseLevel(argValue(args, "--level"))
	if err != nil {
		return err
	}
	asJSON := hasFlag(args, "--json")

	release, err := embed.LockDataDir(dataDir)
	if err != nil {
		return fmt.Errorf("locking %s (stop kdb-service / the embedded runtime first): %w", dataDir, err)
	}
	defer release()
	shim, err := openDirShim(dataDir)
	if err != nil {
		return err
	}
	report, err := integrity.Verify(shim, namespace, integrity.Options{Level: level})
	if err != nil {
		return err
	}

	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			return err
		}
	} else {
		printReport(report)
	}
	if !report.Clean() {
		os.Exit(1)
	}
	return nil
}

func printReport(report *integrity.Report) {
	fmt.Printf("namespace %s: %d segment(s) scanned\n", report.Namespace, len(report.Segments))
	for _, s := range report.Segments {
		fmt.Printf("  segment %d: %d byte(s), %d frame(s)\n", s.Sequence, s.SizeBytes, s.FrameCount)
	}
	if report.Clean() {
		fmt.Println("clean: no findings")
		return
	}
	fmt.Printf("%d finding(s):\n", len(report.Findings))
	for _, f := range report.Findings {
		fmt.Printf("  [%s] %s segment=%d offset=%d: %s\n", f.Level, f.Classification, f.Segment, f.Offset, f.Detail)
	}
}

func repairSegmentsCmd(args []string) error {
	dataDir := argValue(args, "--data-dir")
	namespace := argValue(args, "--namespace")
	if dataDir == "" || namespace == "" {
		return fmt.Errorf("usage: kdb-inspect repair-segments --data-dir DIR --namespace NS [--dry-run]")
	}
	dryRun := hasFlag(args, "--dry-run")

	release, err := embed.LockDataDir(dataDir)
	if err != nil {
		return fmt.Errorf("locking %s (stop kdb-service / the embedded runtime first): %w", dataDir, err)
	}
	defer release()
	shim, err := openDirShim(dataDir)
	if err != nil {
		return err
	}
	opts := integrity.Options{Level: integrity.L1}
	report, err := integrity.Verify(shim, namespace, opts)
	if err != nil {
		return err
	}
	if report.Clean() {
		fmt.Println("clean: no repair needed")
		return nil
	}
	if dryRun {
		fmt.Println("--dry-run: would attempt to repair:")
		printReport(report)
		return nil
	}

	result, err := integrity.Repair(shim, report, opts)
	if err != nil {
		return err
	}
	for _, step := range result.Steps {
		switch step.Action {
		case integrity.ActionRefused:
			fmt.Printf("REFUSED segment %d: %s\n", step.Finding.Segment, step.Detail)
			fmt.Printf("  missing commit(s): %v\n", step.MissingHashes)
			fmt.Println("  run 'kdb-inspect restore' to rebuild from a backup or peer instead")
		default:
			fmt.Printf("%s segment %d: %s\n", step.Action, step.Finding.Segment, step.Detail)
		}
	}
	if result.AnyRefused() {
		os.Exit(1)
	}
	return nil
}

func hasFlag(args []string, name string) bool {
	for _, a := range args {
		if a == name {
			return true
		}
	}
	return false
}
