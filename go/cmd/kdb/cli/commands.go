package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
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
		return cmdLog(cfg, rt, c)
	case ShowCmd:
		return cmdShow(cfg, rt, c)
	case DiffCmd:
		return cmdDiff(cfg, rt, c)
	case RevertCmd:
		return cmdRevert(cfg, rt, c)
	case TagListCmd:
		return cmdTagList(cfg, rt)
	case TagCreateCmd:
		return cmdTagCreate(cfg, rt, c)
	case TagDeleteCmd:
		return cmdTagDelete(cfg, rt, c)
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
	case ShowCmd:
		return c.Namespace
	case DiffCmd:
		return c.Namespace
	case RevertCmd:
		return c.Namespace
	case TagListCmd:
		return c.Namespace
	case TagCreateCmd:
		return c.Namespace
	case TagDeleteCmd:
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
	at := c.At
	if at == "" {
		at = "head"
	}
	target, err := resolveRevision(rt, at)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	commit, err := rt.DAG.GetCommitOrThrow(target)
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

// resolveRevision turns a revision specification into a commit hash. Every
// history command goes through it, so "head~3" means the same thing
// everywhere and an unresolvable revision is an error rather than a
// silent read of current data.
func resolveRevision(rt *embed.EmbeddedKdbRuntime, spec string) (codec.Hash, error) {
	nav, ok := rt.DAG.(dag.HistoryNavigator)
	if !ok {
		return codec.Hash{}, fmt.Errorf("this runtime's commit graph does not navigate")
	}
	return nav.ResolveRevision(spec)
}

func cmdLog(cfg Config, rt *embed.EmbeddedKdbRuntime, c LogCmd) int {
	_ = cfg
	nav, ok := rt.DAG.(dag.HistoryNavigator)
	if !ok {
		fmt.Fprintf(os.Stderr, "Error: this runtime's commit graph does not navigate\n")
		return 1
	}
	head, err := rt.DAG.Head()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	entries, err := nav.ListCommits(head, c.Skip, c.Limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	for _, e := range entries {
		if c.Oneline {
			fmt.Printf("%s %s\n", shortHash(e.Hash), e.Message)
			continue
		}
		fmt.Printf("commit %s\n", e.Hash.Hex())
		if len(e.ParentHashes) > 0 {
			parents := make([]string, len(e.ParentHashes))
			for i, p := range e.ParentHashes {
				parents[i] = shortHash(p)
			}
			fmt.Printf("parent %s\n", strings.Join(parents, " "))
		}
		fmt.Printf("date   %s\n", formatTimestamp(e.Timestamp))
		if e.OperationCount >= 0 {
			fmt.Printf("ops    %d\n", e.OperationCount)
		}
		if e.Message != "" {
			fmt.Printf("\n    %s\n", e.Message)
		}
		fmt.Println()
	}
	return 0
}

func shortHash(h codec.Hash) string {
	hex := h.Hex()
	if len(hex) > 12 {
		return hex[:12]
	}
	return hex
}

func formatTimestamp(ts codec.Timestamp) string {
	return time.UnixMicro(ts.EpochMicros()).UTC().Format(time.RFC3339)
}

func cmdShow(cfg Config, rt *embed.EmbeddedKdbRuntime, c ShowCmd) int {
	_ = cfg
	hash, err := resolveRevision(rt, c.Revision)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	commit, err := rt.DAG.GetCommitOrThrow(hash)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	fmt.Printf("commit  %s\n", commit.Hash.Hex())
	for _, p := range commit.ParentHashes {
		fmt.Printf("parent  %s\n", p.Hex())
	}
	fmt.Printf("tx      %s\n", commit.TransactionID.String())
	fmt.Printf("date    %s\n", formatTimestamp(commit.Timestamp))
	fmt.Printf("author  %s\n", commit.AuthorNodeID.String())
	fmt.Printf("tree    %s\n", commit.DocumentTreeHash.Hex())
	if commit.Message != "" {
		fmt.Printf("\n    %s\n", commit.Message)
	}
	// Operations are fetched for this one commit rather than carried by
	// every listing: they are the full text of what it wrote.
	if d := concreteDag(rt); d != nil {
		ops, err := d.CommitOperations(hash)
		if err == nil && len(ops) > 0 {
			fmt.Printf("\nchanges (%d)\n", len(ops))
			for _, op := range ops {
				switch o := op.(type) {
				case document.WriteOp:
					fmt.Printf("  write   %s\n", o.DocID.String())
				case document.DeleteOp:
					fmt.Printf("  delete  %s\n", o.DocID.String())
				}
			}
		}
	}
	return 0
}

func cmdDiff(cfg Config, rt *embed.EmbeddedKdbRuntime, c DiffCmd) int {
	_ = cfg
	diff, err := embed.DiffCommits(rt, c.From, c.To)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	if diff.IsEmpty() {
		fmt.Printf("no document differences between %s and %s\n",
			shortHash(diff.FromHash), shortHash(diff.ToHash))
		return 0
	}
	for _, e := range diff.Entries {
		switch v := e.(type) {
		case dag.DiffAdded:
			fmt.Printf("+ %s\n", v.DocID.String())
		case dag.DiffRemoved:
			fmt.Printf("- %s\n", v.DocID.String())
		case dag.DiffModified:
			fmt.Printf("~ %s\n", v.DocID.String())
		}
	}
	return 0
}

func cmdRevert(cfg Config, rt *embed.EmbeddedKdbRuntime, c RevertCmd) int {
	res, err := embed.RevertTo(rt, c.Namespace, c.Revision)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	if !cfg.Quiet {
		if res.Restored == 0 && res.Removed == 0 {
			fmt.Printf("already at %s; nothing to revert\n", shortHash(res.Target))
			return 0
		}
		fmt.Printf("reverted to %s in commit %s (%d restored, %d removed)\n",
			shortHash(res.Target), shortHash(res.Commit), res.Restored, res.Removed)
	}
	return 0
}

func cmdTagList(cfg Config, rt *embed.EmbeddedKdbRuntime) int {
	_ = cfg
	nav, ok := rt.DAG.(dag.HistoryNavigator)
	if !ok {
		fmt.Fprintf(os.Stderr, "Error: this runtime's commit graph does not navigate\n")
		return 1
	}
	tags := nav.ListTags()
	sort.Slice(tags, func(i, j int) bool { return tags[i].Name < tags[j].Name })
	for _, t := range tags {
		fmt.Printf("%s\t%s\t%s\n", t.Name, shortHash(t.CommitHash), t.Message)
	}
	return 0
}

func cmdTagCreate(cfg Config, rt *embed.EmbeddedKdbRuntime, c TagCreateCmd) int {
	if err := rt.AssertRetainsHistory(c.Namespace, "tagging a commit"); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	nav, ok := rt.DAG.(dag.HistoryNavigator)
	if !ok {
		fmt.Fprintf(os.Stderr, "Error: this runtime's commit graph does not navigate\n")
		return 1
	}
	hash, err := resolveRevision(rt, c.Revision)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	t, err := nav.CreateTag(c.Name, hash, c.Message)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	if !cfg.Quiet {
		fmt.Printf("tagged %s as %s\n", shortHash(t.CommitHash), t.Name)
	}
	return 0
}

func cmdTagDelete(cfg Config, rt *embed.EmbeddedKdbRuntime, c TagDeleteCmd) int {
	nav, ok := rt.DAG.(dag.HistoryNavigator)
	if !ok {
		fmt.Fprintf(os.Stderr, "Error: this runtime's commit graph does not navigate\n")
		return 1
	}
	if !nav.DeleteTag(c.Name) {
		fmt.Fprintf(os.Stderr, "Error: no such tag: %s\n", c.Name)
		return 1
	}
	if !cfg.Quiet {
		fmt.Printf("deleted tag %s\n", c.Name)
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
	if err := rt.AssertRetainsHistory(c.Namespace, "creating a branch"); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
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
