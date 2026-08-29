package cli

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/limidus/kdb/go/kdb/version"
)

// Config holds global CLI flags.
type Config struct {
	DataDir string
	Quiet   bool
}

// Run is the kdb CLI entrypoint (mirrors dev.kdb.cli.KdbCli).
func Run(args []string) int {
	if len(args) == 1 && (args[0] == "version" || args[0] == "--version") {
		fmt.Println(version.String())
		return 0
	}
	cfg, cmd, err := parseArgs(args)
	if err != nil {
		printUsage(os.Stderr)
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 2
	}
	if cmd == nil {
		printUsage(os.Stderr)
		return 2
	}
	return execute(cfg, cmd)
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, `kdb — KDB command-line interface (Go port)

Usage:
  kdb [--data-dir DIR] [--quiet] <command> ...

Commands:
  init <namespace>
  put <namespace> <file|json>
  get <namespace> <docId>
  query <namespace> <sql>
  log <namespace>
  status <namespace>
  branch list <namespace>
  branch create <namespace> <name> [from-hash]
  branch checkout <namespace> <name>
  unlock`)
}

// ParseArgsForTest exposes argument parsing for unit tests.
func ParseArgsForTest(args []string) (Config, Command, error) {
	return parseArgs(args)
}

func parseArgs(args []string) (Config, Command, error) {
	cfg := Config{DataDir: defaultDataDir()}
	var rest []string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--data-dir":
			i++
			if i >= len(args) {
				return cfg, nil, fmt.Errorf("--data-dir requires a value")
			}
			cfg.DataDir = args[i]
		case "--quiet":
			cfg.Quiet = true
		default:
			rest = append(rest, args[i])
		}
	}
	if len(rest) == 0 {
		return cfg, nil, nil
	}
	cmd, err := parseCommand(rest)
	return cfg, cmd, err
}

func parseCommand(rest []string) (Command, error) {
	switch rest[0] {
	case "init":
		if len(rest) < 2 {
			return nil, fmt.Errorf("usage: kdb init <namespace>")
		}
		return InitCmd{Namespace: rest[1]}, nil
	case "put":
		if len(rest) < 3 {
			return nil, fmt.Errorf("usage: kdb put <namespace> <file|json>")
		}
		return PutCmd{Namespace: rest[1], Payload: rest[2]}, nil
	case "get":
		if len(rest) < 3 {
			return nil, fmt.Errorf("usage: kdb get <namespace> <docId>")
		}
		return GetCmd{Namespace: rest[1], DocID: rest[2]}, nil
	case "query":
		if len(rest) < 3 {
			return nil, fmt.Errorf("usage: kdb query <namespace> <sql>")
		}
		return QueryCmd{Namespace: rest[1], SQL: strings.Join(rest[2:], " ")}, nil
	case "log":
		if len(rest) < 2 {
			return nil, fmt.Errorf("usage: kdb log <namespace>")
		}
		return LogCmd{Namespace: rest[1]}, nil
	case "status":
		if len(rest) < 2 {
			return nil, fmt.Errorf("usage: kdb status <namespace>")
		}
		return StatusCmd{Namespace: rest[1]}, nil
	case "branch":
		return parseBranchCommand(rest[1:])
	case "unlock":
		if len(rest) != 1 {
			return nil, fmt.Errorf("usage: kdb unlock")
		}
		return UnlockCmd{}, nil
	default:
		return nil, fmt.Errorf("unknown command: %s", rest[0])
	}
}

func parseBranchCommand(rest []string) (Command, error) {
	if len(rest) == 0 {
		return nil, fmt.Errorf("usage: kdb branch list|create|checkout ...")
	}
	switch rest[0] {
	case "list":
		if len(rest) < 2 {
			return nil, fmt.Errorf("usage: kdb branch list <namespace>")
		}
		return BranchListCmd{Namespace: rest[1]}, nil
	case "create":
		if len(rest) < 3 {
			return nil, fmt.Errorf("usage: kdb branch create <namespace> <name> [from-hash]")
		}
		cmd := BranchCreateCmd{Namespace: rest[1], Name: rest[2]}
		if len(rest) >= 4 {
			cmd.FromHash = rest[3]
		}
		return cmd, nil
	case "checkout":
		if len(rest) < 3 {
			return nil, fmt.Errorf("usage: kdb branch checkout <namespace> <name>")
		}
		return BranchCheckoutCmd{Namespace: rest[1], Name: rest[2]}, nil
	default:
		return nil, fmt.Errorf("unknown branch command: %s", rest[0])
	}
}

func defaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".kdb"
	}
	return filepath.Join(home, ".kdb")
}
