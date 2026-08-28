package main

import (
	"fmt"
	"os"

	"github.com/limidus/kdb/go/kdb/inspect"
	"github.com/limidus/kdb/go/kdb/version"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	switch args[0] {
	case "version", "--version":
		fmt.Println(version.Version)
		return nil
	case "dump-wire":
		return dumpWire(args[1:])
	case "verify":
		return verifyCmd(args[1:])
	case "repair-segments":
		return repairSegmentsCmd(args[1:])
	case "restore":
		return restoreCmd(args[1:])
	case "backup":
		return backupCmd(args[1:])
	case "backup-verify":
		return backupVerifyCmd(args[1:])
	case "backup-list":
		return backupListCmd(args[1:])
	case "backup-fetch":
		return backupFetchCmd(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", args[0])
		printUsage()
		os.Exit(1)
	}
	return nil
}

func dumpWire(args []string) error {
	file := argValue(args, "--file")
	if file == "" {
		return fmt.Errorf("--file required")
	}
	pretty := true
	for _, a := range args {
		if a == "--compact" {
			pretty = false
		}
	}
	frame, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	out, err := inspect.NewWireFrameInspector().DumpFrame(frame, pretty)
	if err != nil {
		return err
	}
	fmt.Println(out)
	return nil
}

func argValue(args []string, name string) string {
	for i := 0; i < len(args); i++ {
		if args[i] == name && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

func printUsage() {
	fmt.Println(`kdb-inspect — debug JSON views and data-directory maintenance (kdb-spec-layer15)

Usage:
  kdb-inspect dump-wire --file FRAME.bin [--compact]

  kdb-inspect verify --data-dir DIR --namespace NS [--level L1|L2] [--json]
      Walk the delta log and report corruption without changing anything.

  kdb-inspect repair-segments --data-dir DIR --namespace NS [--dry-run]
      Truncate torn tails and quarantine corrupt frames where provably safe.
      Refuses (naming the missing commits) when a repair would drop history
      still referenced by later segments - run restore instead in that case.

  kdb-inspect restore --namespace NS --out DIR [--source LABEL=PATH ...] [--from-backup DIR|s3 --backup-id ID] [--codec zstd|none]
      Rebuild a namespace's delta log into DIR from the verified union of one
      or more sources (a damaged local data directory, a fetched backup, or
      both for a hybrid restore).

  kdb-inspect backup --data-dir DIR --namespace NS --to DIR|s3 [--base-backup-id ID]
      Write a manifest-defined, verifiable backup (sealed segments in full,
      the active segment as its CRC-verified prefix) to a directory or S3
      (--to s3 uses KDB_S3_* env config). With --base-backup-id, incremental.

  kdb-inspect backup-verify --namespace NS --to DIR|s3 --backup-id ID
      Re-hash every object a backup's manifest names, without restoring.

  kdb-inspect backup-list --namespace NS --to DIR|s3
  kdb-inspect backup-fetch --namespace NS --to DIR|s3 --backup-id ID --out DIR

verify, repair-segments, backup, and restore's --out all take the data
directory's exclusive lock (the same one kdb-service holds), so they refuse
to run - with a clear error - while a live writer has the directory open.`)
}
