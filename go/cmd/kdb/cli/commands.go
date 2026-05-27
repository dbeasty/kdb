package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

func execute(cfg Config, cmd Command) int {
	rt, err := openRuntime(cfg, namespaceFor(cmd))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	switch c := cmd.(type) {
	case InitCmd:
		return cmdInit(cfg, rt, c.Namespace)
	case PutCmd:
		return cmdPut(cfg, rt, c)
	case GetCmd:
		return cmdGet(cfg, rt, c)
	case QueryCmd:
		return cmdQuery(cfg, c)
	case LogCmd:
		return cmdLog(cfg, rt)
	case StatusCmd:
		return cmdStatus(cfg, rt, c.Namespace)
	case BranchListCmd:
		return cmdBranchList(cfg, rt)
	case BranchCreateCmd:
		return cmdBranchCreate(cfg, rt, c)
	case BranchCheckoutCmd:
		return cmdBranchCheckout(cfg, rt, c)
	case UnlockCmd:
		return cmdUnlock(cfg)
	default:
		fmt.Fprintf(os.Stderr, "Error: unsupported command\n")
		return 2
	}
}

func namespaceFor(cmd Command) string {
	switch c := cmd.(type) {
	case InitCmd:
		return c.Namespace
	case PutCmd:
		return c.Namespace
	case GetCmd:
		return c.Namespace
	case QueryCmd:
		return c.Namespace
	case LogCmd:
		return c.Namespace
	case StatusCmd:
		return c.Namespace
	case BranchListCmd:
		return c.Namespace
	case BranchCreateCmd:
		return c.Namespace
	case BranchCheckoutCmd:
		return c.Namespace
	default:
		return ""
	}
}

func openRuntime(cfg Config, namespaceID string) (*embed.EmbeddedKdbRuntime, error) {
	catalog := embed.CatalogFromNamespace(namespaceID)
	return embed.OpenFileRuntime(cfg.DataDir, catalog, namespaceID, schema.None())
}

func cmdInit(cfg Config, rt *embed.EmbeddedKdbRuntime, namespace string) int {
	_ = rt
	if !cfg.Quiet {
		fmt.Printf("Initialized namespace %s\n", namespace)
	}
	return 0
}

func cmdPut(cfg Config, rt *embed.EmbeddedKdbRuntime, c PutCmd) int {
	jsonText, err := readPayload(c.Payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	result, err := embed.PutJSONDocument(rt, c.Namespace, jsonText)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	if !cfg.Quiet {
		out, err := formatPutStdout(result)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return 1
		}
		fmt.Println(out)
	}
	return 0
}

// FormatPutStdoutForTest exposes put stdout formatting for unit tests.
func FormatPutStdoutForTest(result embed.PutResult) (string, error) {
	return formatPutStdout(result)
}

func formatPutStdout(result embed.PutResult) (string, error) {
	docShort := strings.ToLower(strings.ReplaceAll(result.DocID.String(), "-", ""))
	if len(docShort) > 8 {
		docShort = docShort[:8]
	}
	out, err := json.Marshal(struct {
		DocID  string `json:"docId"`
		Short  string `json:"docIdShort"`
		Commit string `json:"commit"`
	}{
		DocID:  result.DocID.String(),
		Short:  docShort,
		Commit: result.Commit.Hex(),
	})
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func cmdGet(cfg Config, rt *embed.EmbeddedKdbRuntime, c GetCmd) int {
	head, err := rt.DAG.Head()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	commit, err := rt.DAG.GetCommitOrThrow(head)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	id, err := resolveDocSelector(c.Namespace, rt.Storage, commit.DocumentTreeHash, c.DocID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	doc, err := rt.Storage.GetDocument(c.Namespace, id, commit.DocumentTreeHash)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	if doc == nil {
		fmt.Fprintf(os.Stderr, "Error: document not found: %s\n", c.DocID)
		return 1
	}
	fmt.Println(doc.JSON)
	return 0
}

func cmdQuery(cfg Config, c QueryCmd) int {
	_ = cfg
	fmt.Fprintf(os.Stderr, "Error: SQL query engine not yet ported to Go (namespace=%s sql=%q)\n", c.Namespace, c.SQL)
	return 1
}

func cmdLog(cfg Config, rt *embed.EmbeddedKdbRuntime) int {
	head, err := rt.DAG.Head()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	entries := rt.DAG.Walk(head, nil, 8192)
	for _, e := range entries {
		full, ok := e.(dag.FullEntry)
		if !ok {
			continue
		}
		fmt.Printf("%s\t%s\n", full.Commit.Hash.Hex(), full.Commit.Message)
	}
	return 0
}

func cmdStatus(cfg Config, rt *embed.EmbeddedKdbRuntime, namespace string) int {
	head, err := rt.DAG.Head()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	fmt.Printf("HEAD %s\n", head.Hex())
	fmt.Printf("namespace %s\n", namespace)
	return 0
}

func cmdBranchList(cfg Config, rt *embed.EmbeddedKdbRuntime) int {
	for _, b := range rt.DAG.ListBranches() {
		if !cfg.Quiet {
			fmt.Printf("%s\t%s\n", b.Name, b.HeadHash.Hex())
		}
	}
	return 0
}

func cmdBranchCreate(cfg Config, rt *embed.EmbeddedKdbRuntime, c BranchCreateCmd) int {
	from, err := rt.DAG.Head()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	if c.FromHash != "" {
		from, err = codec.HashFromHex(c.FromHash)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return 1
		}
	}
	b, err := rt.DAG.CreateBranch(c.Name, from)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	if !cfg.Quiet {
		fmt.Printf("branch %s at %s\n", b.Name, b.HeadHash.Hex())
	}
	return 0
}

func cmdBranchCheckout(cfg Config, rt *embed.EmbeddedKdbRuntime, c BranchCheckoutCmd) int {
	b, ok := rt.DAG.GetBranch(c.Name)
	if !ok {
		fmt.Fprintf(os.Stderr, "Error: branch not found: %s\n", c.Name)
		return 1
	}
	if err := rt.DAG.SetHead(c.Name, b.HeadHash); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	if !cfg.Quiet {
		fmt.Printf("checked out %s at %s\n", c.Name, b.HeadHash.Hex())
	}
	return 0
}

func cmdUnlock(cfg Config) int {
	lockPath := cfg.DataDir + "/.kdb.lock"
	if _, err := os.Stat(lockPath); os.IsNotExist(err) {
		if !cfg.Quiet {
			fmt.Printf("No lock file at %s\n", cfg.DataDir)
		}
		return 0
	}
	if err := os.Remove(lockPath); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	if !cfg.Quiet {
		fmt.Println("Removed stale lock file")
	}
	return 0
}

func readPayload(payload string) (string, error) {
	trimmed := strings.TrimSpace(payload)
	if strings.HasPrefix(trimmed, "{") {
		return trimmed, nil
	}
	b, err := os.ReadFile(trimmed)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
