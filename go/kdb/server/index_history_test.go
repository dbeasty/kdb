package server

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/fulltext"
	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/index/vector"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/wire"
)

type histDoc struct {
	status string
	n      int
	title  string
}

// TestIndexedReadsAtOldCommitsStayCorrect: with history windows far smaller than the write
// history, SQL reads and SEARCH frames at every commit - recent ones served from the windows,
// old ones by the fallbacks (a scan for SQL, a throwaway index of the documents at that commit
// for SEARCH) - return what the documents at that commit say.
func TestIndexedReadsAtOldCommitsStayCorrect(t *testing.T) {
	srv := newTestRuntime(t)
	provider, err := srv.OpenIndexes(stores.Options{
		Keyed:    index.MemoryOptions{HistoryEvents: 24},
		FullText: fulltext.Options{HistoryEvents: 8},
		Vector:   vector.Options{HistoryEvents: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	ns := srv.Runtime.DefaultNamespace
	for _, stmt := range []sql.StmtCreateIndex{
		{Name: "by_status", Table: "docs", Fields: []sql.IndexField{{Path: "status"}}, Using: "HASH"},
		{Name: "by_n", Table: "docs", Fields: []sql.IndexField{{Path: "n"}}, Using: "BTREE"},
		{Name: "by_title", Table: "docs", Fields: []sql.IndexField{{Path: "title"}}, Using: "FULLTEXT"},
	} {
		if err := provider.CreateIndex(stmt, sql.QueryContext{NamespaceID: ns}); err != nil {
			t.Fatalf("CreateIndex %s: %v", stmt.Name, err)
		}
	}
	rng := rand.New(rand.NewSource(5))
	words := []string{"alpha", "beta", "gamma", "delta"}
	var ids []codec.UUID
	state := map[codec.UUID]histDoc{}
	type snap struct {
		head codec.Hash
		docs map[codec.UUID]histDoc
	}
	var snaps []snap
	for i := 0; i < 120; i++ {
		var id codec.UUID
		if len(ids) < 25 || rng.Intn(3) == 0 {
			id, _ = codec.RandomUUID()
			ids = append(ids, id)
		} else {
			id = ids[rng.Intn(len(ids))]
		}
		d := histDoc{status: []string{"open", "done", "held"}[rng.Intn(3)], n: rng.Intn(40), title: words[rng.Intn(4)] + " " + words[rng.Intn(4)]}
		c, err := srv.Upsert(ns, id, fmt.Sprintf(`{"status":%q,"n":%d,"title":%q}`, d.status, d.n, d.title), auth.Principal{})
		if err != nil {
			t.Fatal(err)
		}
		state[id] = d
		cp := make(map[codec.UUID]histDoc, len(state))
		for k, v := range state {
			cp[k] = v
		}
		snaps = append(snaps, snap{head: c.Hash, docs: cp})
	}
	count := func(at codec.Hash, q string) string {
		ctx := sql.QueryContext{NamespaceID: ns, Schema: srv.Runtime.Schema, AtCommit: &at}
		res, err := srv.SQLEngine().Execute(q, ctx)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return cellText(res.Rows[0].Values[0])
	}
	servedFromIndex, fellBack := 0, 0
	for i, s := range snaps {
		at := s.head
		open, inRange := 0, 0
		var alpha []string
		for id, d := range s.docs {
			if d.status == "open" {
				open++
			}
			if d.n >= 10 && d.n <= 20 {
				inRange++
			}
			if strings.Contains(d.title, "alpha") {
				alpha = append(alpha, id.String())
			}
		}
		if got := count(at, `SELECT COUNT(*) FROM docs WHERE status = 'open'`); got != itoa(int64(open)) {
			t.Fatalf("commit %d: status count %s, want %d", i, got, open)
		}
		if got := count(at, `SELECT COUNT(*) FROM docs WHERE n >= 10 AND n <= 20`); got != itoa(int64(inRange)) {
			t.Fatalf("commit %d: range count %s, want %d", i, got, inRange)
		}
		_, ok, err := provider.ExactLookup(sql.QueryContext{NamespaceID: ns, AtCommit: &at}, "status", sql.CellString{Value: "open"})
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			servedFromIndex++
		} else {
			fellBack++
		}
		hits, _, err := provider.Search(context.Background(), wire.SearchMessage{
			Namespace: ns, AtCommitHex: at.Hex(), Text: &wire.SearchTextArm{Index: "by_title", Query: "alpha"}, Limit: 1000,
		})
		if err != nil {
			t.Fatalf("commit %d: search: %v", i, err)
		}
		var got []string
		for _, h := range hits {
			got = append(got, h.DocID.String())
		}
		sort.Strings(got)
		sort.Strings(alpha)
		if fmt.Sprint(got) != fmt.Sprint(alpha) {
			t.Fatalf("commit %d: search alpha got %v want %v", i, got, alpha)
		}
	}
	if servedFromIndex == 0 || fellBack == 0 {
		t.Fatalf("expected both window and fallback reads: %d from the index, %d fell back", servedFromIndex, fellBack)
	}
}
