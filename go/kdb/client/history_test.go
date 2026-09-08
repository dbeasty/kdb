package client_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/embed"
)

// End to end over a real listener: the client's request crosses the wire,
// the server resolves the revision, and the reply maps back.

func TestHistoryAndRevertRoundTripAgainstAListener(t *testing.T) {
	addr, rt := startTestServer(t)
	for i := 0; i < 4; i++ {
		if _, err := embed.PutJSONDocument(rt.Runtime, "app/data", fmt.Sprintf(`{"id":"a","n":%d}`, i)); err != nil {
			t.Fatal(err)
		}
	}
	c := connectTestClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := c.History(ctx, client.HistoryRequest{Namespace: "app/data"})
	if err != nil {
		t.Fatal(err)
	}
	// Four puts plus genesis.
	if len(res.Commits) != 5 {
		t.Fatalf("want 5 commits, got %d", len(res.Commits))
	}
	if res.Commits[0].Hex != res.ResolvedHex {
		t.Fatal("the listing should start at the resolved revision")
	}
	if res.Commits[0].Timestamp.IsZero() {
		t.Fatal("commit timestamps did not survive the wire")
	}
	if res.Commits[0].Timestamp.After(time.Now().Add(time.Minute)) {
		t.Fatalf("the timestamp decoded as %v, which is not a plausible instant", res.Commits[0].Timestamp)
	}

	// Paging.
	page, err := c.History(ctx, client.HistoryRequest{Namespace: "app/data", Skip: 2, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Commits) != 2 {
		t.Fatalf("skip/limit returned %d commits", len(page.Commits))
	}
	if page.Commits[0].Hex != res.Commits[2].Hex {
		t.Fatal("skip did not land where the full listing says it should")
	}

	// Relative revisions resolve server-side.
	rel, err := c.History(ctx, client.HistoryRequest{Namespace: "app/data", From: "head~2", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if rel.Commits[0].Hex != res.Commits[2].Hex {
		t.Fatal("from=head~2 resolved to the wrong commit")
	}

	// And undo.
	rev, err := c.Revert(ctx, "app/data", "head~1")
	if err != nil {
		t.Fatal(err)
	}
	if rev.CommitHex == rev.TargetHex {
		t.Fatal("a revert must write a new commit")
	}
	if rev.TargetHex != res.Commits[1].Hex {
		t.Fatalf("revert targeted %s, want %s", rev.TargetHex, res.Commits[1].Hex)
	}
	if rev.Restored == 0 {
		t.Fatal("the revert restored nothing")
	}

	after, err := c.History(ctx, client.HistoryRequest{Namespace: "app/data"})
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Commits) != len(res.Commits)+1 {
		t.Fatalf("history went from %d to %d", len(res.Commits), len(after.Commits))
	}
	if !strings.HasPrefix(after.Commits[0].Message, "revert to ") {
		t.Fatalf("the revert commit's message is %q", after.Commits[0].Message)
	}
}

func TestHistoryAndRevertValidateLocally(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx := context.Background()

	if _, err := c.History(ctx, client.HistoryRequest{}); err == nil {
		t.Fatal("a history request with no namespace should be refused before it is sent")
	}
	if _, err := c.Revert(ctx, "", "head"); err == nil {
		t.Fatal("a revert with no namespace should be refused before it is sent")
	}
	if _, err := c.Revert(ctx, "app/data", ""); err == nil {
		t.Fatal("a revert with no revision should be refused before it is sent")
	}
}

func TestHistoryErrorsComeBackAsErrors(t *testing.T) {
	addr, rt := startTestServer(t)
	if _, err := embed.PutJSONDocument(rt.Runtime, "app/data", `{"id":"a"}`); err != nil {
		t.Fatal(err)
	}
	c := connectTestClient(t, addr)
	ctx := context.Background()

	if _, err := c.History(ctx, client.HistoryRequest{Namespace: "app/data", From: "head~99"}); err == nil {
		t.Fatal("an unresolvable revision should surface as an error")
	}
	if _, err := c.Revert(ctx, "app/data", "head~99"); err == nil {
		t.Fatal("reverting to an unresolvable revision should surface as an error")
	}
}
