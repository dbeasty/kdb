package main

import (
	"fmt"
	"os"

	"github.com/limidus/kdb/go/kdb/inspect"
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
	case "dump-wire":
		return dumpWire(args[1:])
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
	fmt.Println(`kdb-inspect — debug JSON views (non-authoritative)

Usage:
  kdb-inspect dump-wire --file FRAME.bin [--compact]`)
}
