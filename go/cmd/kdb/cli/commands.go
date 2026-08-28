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
	"github.com/limidus/kdb/go/kdb/sql"
)

func execute(cfg Config, cmd Command) int {
	// unlock must never open a runtime: opening one means acquiring the very directory lock
	// this command exists to clear, so if the lock actually is held, unlock could never reach
	// its own body (openRuntime would fail first) - and if it isn't held, opening one anyway
	// created bogus ns//delta, ns//meta dirs and a meta.json under the empty ("") namespace
	// namespaceFor returns for UnlockCmd, purely as a side effect of a command that should just
	// remove a file.
	if _, ok := cmd.(UnlockCmd); ok {
		return cmdUnlock(cfg)
	}
	rt, err := openRuntime(cfg, namespaceFor(cmd))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	// Every other command path used to return without ever calling rt.Close() - no CLI
	// invocation flushed or sealed its delta segment, relying entirely on process exit (and
	// WAL/delta replay on the next open) to make a write durable-and-clean rather than just
	// durable.
	defer rt.Close()
	switch c := cmd.(type) {
	case InitCmd:
		return cmdInit(cfg, rt, c.Namespace)
	case PutCmd:
		return cmdPut(cfg, rt, c)
	case GetCmd:
		return cmdGet(cfg, rt, c)
	case QueryCmd:
		return cmdQuery(cfg, rt, c)
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

// cmdQuery runs a SELECT against the local data directory via the real go/kdb/sql engine
// (kdb-finish-up-plan 4.E - this was a hard "not yet ported" stub). Read-only: DML/DDL over
// the CLI still goes through put / the wire server, matching the Kotlin CLI's local query
// semantics. Output is tab-separated: a header row of column names, then one row per result.
func cmdQuery(cfg Config, rt *embed.EmbeddedKdbRuntime, c QueryCmd) int {
	_ = cfg
	d := concreteDag(rt)
	if d == nil {
		fmt.Fprintf(os.Stderr, "Error: query requires an InMemoryCommitDag-backed runtime, got %T\n", rt.DAG)
		return 1
	}
	head, err := rt.DAG.Head()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	engine := sql.NewEngine(rt.Storage, d)
	result, err := engine.Execute(strings.TrimSpace(c.SQL), sql.QueryContext{
		NamespaceID: c.Namespace,
		Schema:      rt.Schema,
		AtCommit:    &head,
		MaxRows:     10_000,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	names := make([]string, len(result.Columns))
	for i, col := range result.Columns {
		names[i] = col.Name
	}
	fmt.Println(strings.Join(names, "\t"))
	for _, row := range result.Rows {
		cells := make([]string, len(row.Values))
		for i, cell := range row.Values {
			cells[i] = cellString(cell)
		}
		fmt.Println(strings.Join(cells, "\t"))
	}
	return 0
}

// concreteDag unwraps rt.DAG to the *dag.InMemoryCommitDag the SQL engine requires, whether
// the runtime is memory-backed (bare) or file-backed (PersistingCommitDAG).
func concreteDag(rt *embed.EmbeddedKdbRuntime) *dag.InMemoryCommitDag {
	switch d := rt.DAG.(type) {
	case *dag.InMemoryCommitDag:
		return d
	case *embed.PersistingCommitDAG:
		return d.Delegate()
	default:
		return nil
	}
}

func cellString(cell sql.Cell) string {
	switch v := cell.(type) {
	case sql.CellNull:
		return ""
	case sql.CellString:
		return v.Value
	case sql.CellLong:
		return fmt.Sprintf("%d", v.Value)
	case sql.CellDouble:
		return fmt.Sprintf("%g", v.Value)
	case sql.CellJSON:
		return v.JSON
	default:
		return fmt.Sprintf("%v", v)
	}
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
