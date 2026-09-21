package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/limidus/kdb/go/kdb/client"
)

// stringList is a repeatable string flag.
type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, "; ") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

// xnsPart is one commit-across participant as the harness passes it.
type xnsPart struct {
	Namespace string                     `json:"namespace"`
	Base      string                     `json:"base"`
	Writes    map[string]json.RawMessage `json:"writes"`
	Deletes   []string                   `json:"deletes"`
}

// xnsOutcome is what commit-across prints: the commit per namespace, or who refused and why.
type xnsOutcome struct {
	Commits  map[string]string `json:"commits,omitempty"`
	Refused  string            `json:"refused,omitempty"`
	Conflict bool              `json:"conflict,omitempty"`
	Error    string            `json:"error,omitempty"`
}

// commitAcross runs one client.CommitAcross over the sessionless TX_COMMIT_MULTI frame and
// prints the outcome as JSON. A refusal is an outcome, not a helper failure: it prints and exits
// 3, so the harness can tell "the server said no" from "the helper broke".
func commitAcross(ctx context.Context, cf commonFlags, partsJSON string) error {
	if cf.addr == "" || partsJSON == "" {
		return fmt.Errorf("--addr and --parts are required")
	}
	var parts []xnsPart
	if err := json.Unmarshal([]byte(partsJSON), &parts); err != nil {
		return fmt.Errorf("--parts: %w", err)
	}
	txs := make([]client.Transaction, len(parts))
	for i, p := range parts {
		tx := client.Transaction{Namespace: p.Namespace, BaseVersion: p.Base, Deletes: p.Deletes}
		for id, body := range p.Writes {
			tx.Writes = append(tx.Writes, client.DocWrite{DocID: id, JSON: body})
		}
		txs[i] = tx
	}
	cl, err := cf.dial(ctx)
	if err != nil {
		return err
	}
	defer cl.Close()
	res, err := cl.CommitAcross(ctx, txs)
	var out xnsOutcome
	if err == nil {
		out.Commits = res.Commits
		return json.NewEncoder(os.Stdout).Encode(out)
	}
	var refused *client.CrossNamespaceError
	if !errors.As(err, &refused) {
		return err
	}
	out.Refused = refused.Namespace
	out.Conflict = errors.Is(err, client.ErrConflict)
	out.Error = err.Error()
	_ = json.NewEncoder(os.Stdout).Encode(out)
	os.Exit(3)
	return nil
}

// sqlTx runs the statements in one explicit client transaction, then commits it (printing the
// commit hash) or rolls it back (printing "rolled-back").
func sqlTx(ctx context.Context, cf commonFlags, stmts []string, rollback, snapshot bool) error {
	if cf.addr == "" || cf.namespace == "" || len(stmts) == 0 {
		return fmt.Errorf("--addr, --namespace and at least one --stmt are required")
	}
	cl, err := cf.dial(ctx)
	if err != nil {
		return err
	}
	defer cl.Close()
	tx, err := cl.BeginTx(ctx, cf.namespace, client.TxOptions{Snapshot: snapshot})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, stmt := range stmts {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	if rollback {
		if err := tx.Rollback(ctx); err != nil {
			return err
		}
		fmt.Println("rolled-back")
		return nil
	}
	commit, err := tx.Commit(ctx)
	if err != nil {
		return err
	}
	fmt.Println(commit)
	return nil
}

// xnsTransfers commits --rounds cross-namespace transactions, each a blind write of
// {"seq": i} to --doc-id in every namespace, and prints "acked i" as each one returns. After a
// kill -9 anywhere in the stream, every namespace must hold the same seq: all of a group's
// parts, or none of them. The operation timeout (--timeout) bounds the whole run.
func xnsTransfers(cf commonFlags, namespaces, docID string, rounds int) error {
	nss := strings.Split(namespaces, ",")
	if cf.addr == "" || len(nss) < 2 || docID == "" {
		return fmt.Errorf("--addr, --doc-id and at least two --namespaces are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), cf.timeout)
	defer cancel()
	cl, err := cf.dial(ctx)
	if err != nil {
		return err
	}
	defer cl.Close()
	for i := 1; i <= rounds; i++ {
		body := []byte(fmt.Sprintf(`{"seq":%d}`, i))
		txs := make([]client.Transaction, len(nss))
		for j, ns := range nss {
			txs[j] = client.Transaction{Namespace: ns, Writes: []client.DocWrite{{DocID: docID, JSON: body}}}
		}
		if _, err := cl.CommitAcross(ctx, txs); err != nil {
			return fmt.Errorf("round %d: %w", i, err)
		}
		fmt.Printf("acked %d\n", i)
	}
	return nil
}
