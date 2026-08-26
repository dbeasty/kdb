package main

import (
	"fmt"
	"strings"

	"github.com/limidus/kdb/go/kdb/recovery"
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
	if namespace == "" || outDir == "" || len(sourceArgs) == 0 {
		return fmt.Errorf("usage: kdb-inspect restore --namespace NS --out DIR --source LABEL=PATH [--source LABEL=PATH ...] [--codec zstd|none]\n" +
			"  Sources are read in the order given; a damaged local data directory and a\n" +
			"  backup directory are both just paths - list both to hybrid-restore.")
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
