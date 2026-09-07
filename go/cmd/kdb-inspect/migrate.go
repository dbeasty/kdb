package main

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/storage"
)

// migrateHistoryCmd converts a namespace between history strategies.
//
// Offline, and takes the data directory's exclusive maintenance lock, so
// it will refuse to run while a service or embedded runtime has the
// directory open - which is the point: it rewrites what the namespace
// claims about itself and what it keeps on disk to back that claim, and a
// writer appending commits through the middle of that would leave the two
// disagreeing.
func migrateHistoryCmd(args []string) error {
	dataDir := argValue(args, "--data-dir")
	namespace := argValue(args, "--namespace")
	to := argValue(args, "--to")
	if dataDir == "" || namespace == "" || to == "" {
		return fmt.Errorf(
			"usage: kdb-inspect migrate-history --data-dir DIR --namespace NS --to replay|objects")
	}
	target, err := storage.ParseHistoryStrategy(to)
	if err != nil {
		return err
	}
	if target == storage.HistoryStrategyUnset {
		return fmt.Errorf("--to must be \"replay\" or \"objects\"")
	}
	if err := embed.MigrateHistoryStrategy(dataDir, namespace, target); err != nil {
		return err
	}
	fmt.Printf("namespace %s now uses the %s history strategy\n", namespace, target)
	return nil
}
