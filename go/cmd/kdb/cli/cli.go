package cli

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
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
  get <namespace> <docId> [--at REV]
  query <namespace> <sql>
  log <namespace> [--limit N] [--skip N] [--oneline]
  show <namespace> <rev>
  diff <namespace> <rev> <rev>
  revert <namespace> <rev>
  status <namespace>
  branch list <namespace>
  branch create <namespace> <name> [from-hash]
  branch checkout <namespace> <name>
  tag list <namespace>
  tag create <namespace> <name> [rev] [message]
  tag delete <namespace> <name>
  unlock

A REV is a revision: head, head~10, head^, a commit hash, <hash>~2,
tag:NAME, branch:NAME, or any of those with ~N appended.`)
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
			return nil, fmt.Errorf("usage: kdb get <namespace> <docId> [--at REV]")
		}
		cmd := GetCmd{Namespace: rest[1], DocID: rest[2]}
		for i := 3; i < len(rest); i++ {
			if rest[i] == "--at" {
				i++
				if i >= len(rest) {
					return nil, fmt.Errorf("--at requires a revision")
				}
				cmd.At = rest[i]
				continue
			}
			return nil, fmt.Errorf("unknown option for get: %s", rest[i])
		}
		return cmd, nil
	case "query":
		if len(rest) < 3 {
			return nil, fmt.Errorf("usage: kdb query <namespace> <sql>")
		}
		return QueryCmd{Namespace: rest[1], SQL: strings.Join(rest[2:], " ")}, nil
	case "log":
		if len(rest) < 2 {
			return nil, fmt.Errorf("usage: kdb log <namespace> [--limit N] [--skip N] [--oneline]")
		}
		cmd := LogCmd{Namespace: rest[1], Limit: defaultLogLimit}
		for i := 2; i < len(rest); i++ {
			switch rest[i] {
			case "--oneline":
				cmd.Oneline = true
			case "--limit", "--skip":
				flag := rest[i]
				i++
				if i >= len(rest) {
					return nil, fmt.Errorf("%s requires a number", flag)
				}
				n, err := strconv.Atoi(rest[i])
				if err != nil || n < 0 {
					return nil, fmt.Errorf("%s requires a non-negative number, got %q", flag, rest[i])
				}
				if flag == "--limit" {
					cmd.Limit = n
				} else {
					cmd.Skip = n
				}
			default:
				return nil, fmt.Errorf("unknown option for log: %s", rest[i])
			}
		}
		return cmd, nil
	case "show":
		if len(rest) < 3 {
			return nil, fmt.Errorf("usage: kdb show <namespace> <rev>")
		}
		return ShowCmd{Namespace: rest[1], Revision: rest[2]}, nil
	case "diff":
		if len(rest) < 4 {
			return nil, fmt.Errorf("usage: kdb diff <namespace> <rev> <rev>")
		}
		return DiffCmd{Namespace: rest[1], From: rest[2], To: rest[3]}, nil
	case "revert":
		if len(rest) < 3 {
			return nil, fmt.Errorf("usage: kdb revert <namespace> <rev>")
		}
		return RevertCmd{Namespace: rest[1], Revision: rest[2]}, nil
	case "tag":
		return parseTagCommand(rest[1:])
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

// defaultLogLimit bounds `kdb log` when the caller does not. A log that
// walks the whole history by default is a surprise on a namespace with a
// million commits, and the flag is right there.
const defaultLogLimit = 50

func parseTagCommand(rest []string) (Command, error) {
	if len(rest) == 0 {
		return nil, fmt.Errorf("usage: kdb tag list|create|delete ...")
	}
	switch rest[0] {
	case "list":
		if len(rest) < 2 {
			return nil, fmt.Errorf("usage: kdb tag list <namespace>")
		}
		return TagListCmd{Namespace: rest[1]}, nil
	case "create":
		if len(rest) < 3 {
			return nil, fmt.Errorf("usage: kdb tag create <namespace> <name> [rev] [message]")
		}
		cmd := TagCreateCmd{Namespace: rest[1], Name: rest[2], Revision: "head"}
		if len(rest) >= 4 {
			cmd.Revision = rest[3]
		}
		if len(rest) >= 5 {
			cmd.Message = strings.Join(rest[4:], " ")
		}
		return cmd, nil
	case "delete":
		if len(rest) < 3 {
			return nil, fmt.Errorf("usage: kdb tag delete <namespace> <name>")
		}
		return TagDeleteCmd{Namespace: rest[1], Name: rest[2]}, nil
	default:
		return nil, fmt.Errorf("unknown tag command: %s", rest[0])
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
