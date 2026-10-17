package server

import (
	"strings"
	"testing"
)

// #318: EXPLAIN goes over the SQL wire like any SELECT and answers with the plan it ran.
func TestExplainOverTheWire(t *testing.T) {
	rt := newTestRuntime(t)
	seedPaddedDocs(t, rt, 5, 8)
	c := listenForTest(t, rt)
	ack := c.sessionBegin(t, "app/data", "READ_COMMITTED")
	res := c.sqlExec(t, "app/data", ack.SessionID, "EXPLAIN SELECT n FROM t WHERE n > 1 ORDER BY n LIMIT 2")
	if res.Error != nil {
		t.Fatalf("EXPLAIN failed: %s", *res.Error)
	}
	if got := strings.Join(res.Columns, ","); got != "plan,rows,rows_examined,docs_read" {
		t.Fatalf("columns %s", got)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != 4 || res.Rows[0][0] != "fullscan" || res.Rows[0][1] != "2" || res.Rows[0][3] != "5" {
		t.Fatalf("rows %v", res.Rows)
	}
}
