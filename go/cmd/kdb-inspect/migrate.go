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
	mode := argValue(args, "--history-mode")
	if dataDir == "" || namespace == "" || (to == "" && mode == "") {
		return fmt.Errorf(
			"usage: kdb-inspect migrate-history --data-dir DIR --namespace NS " +
				"[--to replay|objects] [--history-mode full|none]")
	}
	if mode != "" {
		return migrateMode(dataDir, namespace, mode)
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

// migrateMode converts a namespace between history modes.
//
// The full → none direction is destructive in a delayed way, which is the
// worst kind to be quiet about: the conversion deletes nothing, and then
// the next maintenance pass deletes everything past the retention window.
// So it says so, in as many words, rather than printing a success line
// that looks like any other.
func migrateMode(dataDir, namespace, mode string) error {
	target, err := storage.ParseHistoryMode(mode)
	if err != nil {
		return err
	}
	if target == storage.HistoryModeUnset {
		return fmt.Errorf("--history-mode must be \"full\" or \"none\"")
	}
	if err := embed.MigrateHistoryMode(dataDir, namespace, target); err != nil {
		return err
	}
	fmt.Printf("namespace %s now runs with history=%s\n", namespace, target)
	switch target {
	case storage.HistoryModeNone:
		fmt.Println("  This is destructive from here on: the next maintenance pass will delete every")
		fmt.Println("  delta segment past the retention window, and that history cannot be recovered")
		fmt.Println("  except from a backup. Set retain.duration before running the service if the")
		fmt.Println("  default of 24h is not what you want.")
	case storage.HistoryModeFull:
		fmt.Println("  History is kept from now on. Anything already reclaimed under history=none is")
		fmt.Println("  gone; this namespace's history starts at its current state.")
	}
	return nil
}
