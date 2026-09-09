package control

import (
	"fmt"
	"testing"
)

// Lane assignment is the one piece of this package with real algorithmic content, and the one
// whose failures are invisible in a screenshot - a graph with a wrong lane still looks like a
// graph. So it is tested on shapes rather than on pixels.
//
// Every case is written newest-first, the order the log endpoint returns.

func rowsOf(t *testing.T, commits []graphInput) []graphRow {
	t.Helper()
	rows := assignLanes(commits)
	if len(rows) != len(commits) {
		t.Fatalf("want a row per commit (%d), got %d", len(commits), len(rows))
	}
	return rows
}

func TestLinearHistoryStaysInOneLane(t *testing.T) {
	rows := rowsOf(t, []graphInput{
		{Hash: "c", Parents: []string{"b"}},
		{Hash: "b", Parents: []string{"a"}},
		{Hash: "a"},
	})
	for i, r := range rows {
		if r.Lane != 0 {
			t.Errorf("row %d: linear history belongs in lane 0, got %d", i, r.Lane)
		}
		if len(r.Through) != 0 {
			t.Errorf("row %d: nothing else is running, so nothing passes through: %v", i, r.Through)
		}
		if len(r.Merges) != 0 {
			t.Errorf("row %d: no merges here: %v", i, r.Merges)
		}
	}
	if rows[0].Width != 1 {
		t.Errorf("a linear history needs one lane, got width %d", rows[0].Width)
	}
}

// TestMergeFansOutAndRejoins is the shape the whole feature exists to draw:
//
//	m        (merge of b and c)
//	|\
//	b |
//	| c
//	|/
//	a
func TestMergeFansOutAndRejoins(t *testing.T) {
	rows := rowsOf(t, []graphInput{
		{Hash: "m", Parents: []string{"b", "c"}},
		{Hash: "b", Parents: []string{"a"}},
		{Hash: "c", Parents: []string{"a"}},
		{Hash: "a"},
	})

	if rows[0].Lane != 0 {
		t.Errorf("the merge commit takes the first lane, got %d", rows[0].Lane)
	}
	if len(rows[0].Merges) != 1 {
		t.Fatalf("a two-parent merge draws one extra line, got %v", rows[0].Merges)
	}
	second := rows[0].Merges[0]
	if second == 0 {
		t.Error("the second parent must go to a different lane from the first")
	}

	// b continues the merge's own lane; c is in the lane the merge fanned out to.
	if rows[1].Lane != 0 {
		t.Errorf("the first parent continues the merge's lane, got %d", rows[1].Lane)
	}
	if rows[2].Lane != second {
		t.Errorf("the second parent belongs in the lane it was given (%d), got %d", second, rows[2].Lane)
	}

	// While b is drawn, c's lane is still running and must be drawn through it - otherwise the
	// line breaks and reappears.
	if len(rows[1].Through) != 1 || rows[1].Through[0] != second {
		t.Errorf("c's lane should pass through b's row, got %v", rows[1].Through)
	}

	// Both branches converge on a, which occupies one lane and releases the other.
	if len(rows[3].Through) != 0 {
		t.Errorf("both branches end at a, so nothing passes through it: %v", rows[3].Through)
	}
	if rows[0].Width < 2 {
		t.Errorf("this shape needs two lanes, got width %d", rows[0].Width)
	}
}

// TestConvergingBranchesReleaseTheirLanes: two lanes waiting for the same commit is the ordinary
// branch point. All but one must be freed, or the drawing grows a lane that never terminates.
func TestConvergingBranchesReleaseTheirLanes(t *testing.T) {
	rows := rowsOf(t, []graphInput{
		{Hash: "m", Parents: []string{"x", "y"}},
		{Hash: "x", Parents: []string{"base"}},
		{Hash: "y", Parents: []string{"base"}},
		{Hash: "base", Parents: []string{"root"}},
		{Hash: "root"},
	})
	// At base both branches have arrived; only its own lane may still be live afterwards.
	if len(rows[3].Through) != 0 {
		t.Errorf("base is the shared parent; the other lane must be released: %v", rows[3].Through)
	}
	if len(rows[4].Through) != 0 {
		t.Errorf("nothing runs alongside root: %v", rows[4].Through)
	}
}

// TestOctopusMergeGetsALanePerParent - three parents is unusual but expressible, and it must not
// silently drop one.
func TestOctopusMergeGetsALanePerParent(t *testing.T) {
	rows := rowsOf(t, []graphInput{
		{Hash: "o", Parents: []string{"p1", "p2", "p3"}},
		{Hash: "p1"},
		{Hash: "p2"},
		{Hash: "p3"},
	})
	if len(rows[0].Merges) != 2 {
		t.Fatalf("three parents means two extra lines beyond the first, got %v", rows[0].Merges)
	}
	seen := map[int]bool{rows[0].Lane: true}
	for _, m := range rows[0].Merges {
		if seen[m] {
			t.Errorf("each parent needs its own lane; %d was reused: %v", m, rows[0].Merges)
		}
		seen[m] = true
	}
	// Each parent is then drawn in the lane it was promised.
	lanes := map[string]int{"p1": rows[1].Lane, "p2": rows[2].Lane, "p3": rows[3].Lane}
	if lanes["p1"] != rows[0].Lane {
		t.Errorf("the first parent continues the merge's lane, got %d", lanes["p1"])
	}
	for _, name := range []string{"p2", "p3"} {
		var found bool
		for _, m := range rows[0].Merges {
			if lanes[name] == m {
				found = true
			}
		}
		if !found {
			t.Errorf("%s is in lane %d, which the merge never fanned out to (%v)",
				name, lanes[name], rows[0].Merges)
		}
	}
}

// TestDisjointHistoriesGetSeparateLanes: two roots with no relationship, which is what a log
// spanning unmerged branches looks like.
func TestDisjointHistoriesGetSeparateLanes(t *testing.T) {
	rows := rowsOf(t, []graphInput{
		{Hash: "a2", Parents: []string{"a1"}},
		{Hash: "b2", Parents: []string{"b1"}},
		{Hash: "a1"},
		{Hash: "b1"},
	})
	if rows[0].Lane == rows[1].Lane {
		t.Errorf("unrelated tips must not share a lane: both in %d", rows[0].Lane)
	}
	// a1 continues a2's lane, b1 continues b2's.
	if rows[2].Lane != rows[0].Lane {
		t.Errorf("a1 should continue a2's lane %d, got %d", rows[0].Lane, rows[2].Lane)
	}
	if rows[3].Lane != rows[1].Lane {
		t.Errorf("b1 should continue b2's lane %d, got %d", rows[1].Lane, rows[3].Lane)
	}
}

// TestALaneIsReusedOnceFree keeps the drawing narrow: a branch that has ended should not reserve a
// column for the rest of history.
func TestALaneIsReusedOnceFree(t *testing.T) {
	rows := rowsOf(t, []graphInput{
		{Hash: "m", Parents: []string{"a", "b"}},
		{Hash: "a", Parents: []string{"root"}},
		{Hash: "b", Parents: []string{"root"}},
		{Hash: "root"},
		// A second, unrelated tip appearing after everything above has terminated.
		{Hash: "z2", Parents: []string{"z1"}},
		{Hash: "z1"},
	})
	if rows[0].Width > 2 {
		t.Errorf("this history is never more than two branches wide, got width %d", rows[0].Width)
	}
	if rows[4].Lane > 1 {
		t.Errorf("the later tip should reuse a freed lane rather than opening a third: got %d",
			rows[4].Lane)
	}
}

// TestWidthIsUniform: a gutter that changed width row by row would make the graph shift sideways
// as it scrolls.
func TestWidthIsUniform(t *testing.T) {
	rows := rowsOf(t, []graphInput{
		{Hash: "m", Parents: []string{"a", "b"}},
		{Hash: "a", Parents: []string{"root"}},
		{Hash: "b", Parents: []string{"root"}},
		{Hash: "root"},
	})
	want := rows[0].Width
	for i, r := range rows {
		if r.Width != want {
			t.Fatalf("row %d has width %d, everything else %d", i, r.Width, want)
		}
	}
}

// TestTruncatedWalkDoesNotPanic: a page ends mid-history, so the oldest rows name parents that are
// not in the input at all. That is the normal case, not an error.
func TestTruncatedWalkDoesNotPanic(t *testing.T) {
	rows := rowsOf(t, []graphInput{
		{Hash: "c", Parents: []string{"b"}},
		{Hash: "b", Parents: []string{"unseen-parent"}},
	})
	if rows[1].Lane != 0 {
		t.Errorf("a commit whose parent is off the page still has a lane, got %d", rows[1].Lane)
	}
}

func TestEmptyInput(t *testing.T) {
	if rows := assignLanes(nil); len(rows) != 0 {
		t.Fatalf("no commits means no rows, got %d", len(rows))
	}
}

// TestEveryRowHasACoherentLane is a property check over a generated history: whatever the shape,
// a lane is never negative, never beyond the gutter, and never also listed as passing through.
func TestEveryRowHasACoherentLane(t *testing.T) {
	// A deterministic pseudo-history with merges every few commits.
	var commits []graphInput
	for i := 40; i > 0; i-- {
		c := graphInput{Hash: fmt.Sprintf("c%d", i)}
		if i > 1 {
			c.Parents = append(c.Parents, fmt.Sprintf("c%d", i-1))
		}
		if i%7 == 0 && i > 3 {
			c.Parents = append(c.Parents, fmt.Sprintf("c%d", i-3))
		}
		commits = append(commits, c)
	}
	rows := rowsOf(t, commits)
	for i, r := range rows {
		if r.Lane < 0 || r.Lane >= r.Width {
			t.Fatalf("row %d: lane %d outside a gutter of %d", i, r.Lane, r.Width)
		}
		for _, th := range r.Through {
			if th == r.Lane {
				t.Fatalf("row %d: its own lane %d is also listed as passing through", i, r.Lane)
			}
			if th < 0 || th >= r.Width {
				t.Fatalf("row %d: through-lane %d outside a gutter of %d", i, th, r.Width)
			}
		}
		for _, m := range r.Merges {
			if m == r.Lane {
				t.Fatalf("row %d: a merge line to its own lane %d is not a line", i, r.Lane)
			}
			if m < 0 || m >= r.Width {
				t.Fatalf("row %d: merge lane %d outside a gutter of %d", i, m, r.Width)
			}
		}
	}
}
