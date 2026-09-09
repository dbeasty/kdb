package control

// Lane assignment: turning a commit list into the drawn graph.
//
// This is the classic git-log algorithm. Walking newest-first, each lane holds the hash of the
// commit it is still waiting for. A commit takes the lane that was expecting it - or a free one if
// nothing was - and then hands that lane to its first parent, while any additional parent claims a
// lane of its own. A merge therefore fans out downward, and a branch point is where two lanes
// converge on the same expected hash and all but one are released.
//
// It is a pure function over (hash, parents) pairs on purpose: it is the one piece of this UI with
// real algorithmic content, and it belongs somewhere it can be tested exhaustively rather than
// squinted at in a browser.
//
// Lanes are computed over the whole window the client is showing, always anchored at the head it
// walked from, so they do not shift underneath a reader who loads more. That is why the client
// re-requests from the start with a larger limit rather than appending an independently-laned page:
// a lane number only means anything relative to the walk that produced it.

// graphRow is one commit's place in the drawing.
type graphRow struct {
	// Lane is the column this commit's node sits in.
	Lane int `json:"lane"`
	// Through are the other lanes with a line passing this row - branches that exist here but have
	// nothing to say at this commit. Without them the verticals break wherever another branch has
	// no commit.
	Through []int `json:"through,omitempty"`
	// Merges are the lanes this commit's second and later parents were given, drawn as lines
	// leaving this node downward. Empty for an ordinary commit.
	Merges []int `json:"merges,omitempty"`
	// Width is how many lanes are in use around this row, so a client can size the gutter once
	// rather than measuring every row.
	Width int `json:"width"`
}

// graphInput is the minimum a row needs: who it is, and who it came from.
type graphInput struct {
	Hash    string
	Parents []string
}

// assignLanes computes a lane for each commit, in the order given (newest first).
func assignLanes(commits []graphInput) []graphRow {
	rows := make([]graphRow, len(commits))
	// lanes[i] is the hash lane i is waiting for; "" means the lane is free.
	var lanes []string

	claim := func(hash string) int {
		for i, want := range lanes {
			if want == hash && hash != "" {
				return i
			}
		}
		for i, want := range lanes {
			if want == "" {
				return i
			}
		}
		lanes = append(lanes, "")
		return len(lanes) - 1
	}

	for idx, c := range commits {
		lane := claim(c.Hash)
		// Any other lane waiting for this same commit converges here and is released. Two branches
		// meeting at a shared ancestor is the ordinary case, and leaving those lanes occupied would
		// draw parallel lines that never end.
		for i := range lanes {
			if i != lane && lanes[i] == c.Hash {
				lanes[i] = ""
			}
		}

		// The lines passing this row: every other occupied lane, recorded before the parents are
		// installed, because a line "through" this row is one that neither starts nor ends here.
		through := make([]int, 0, len(lanes))
		for i, want := range lanes {
			if i != lane && want != "" {
				through = append(through, i)
			}
		}

		// The first parent continues in this commit's own lane, which is what keeps a linear
		// history in one straight column.
		if len(c.Parents) > 0 {
			lanes[lane] = c.Parents[0]
		} else {
			lanes[lane] = ""
		}
		var merges []int
		for _, parent := range c.Parents[minInt(1, len(c.Parents)):] {
			// A parent some lane is already waiting for needs no new lane: the merge line goes to
			// where that branch already is.
			target := -1
			for i, want := range lanes {
				if want == parent {
					target = i
					break
				}
			}
			if target < 0 {
				target = claim("")
				lanes[target] = parent
			}
			if target != lane {
				merges = append(merges, target)
			}
		}

		width := 0
		for i, want := range lanes {
			if want != "" && i+1 > width {
				width = i + 1
			}
		}
		if lane+1 > width {
			width = lane + 1
		}
		for _, t := range through {
			if t+1 > width {
				width = t + 1
			}
		}
		rows[idx] = graphRow{Lane: lane, Through: through, Merges: merges, Width: width}
	}

	// One gutter width for the whole drawing: a column that changed width row by row would make
	// the graph shift horizontally as it scrolls.
	widest := 0
	for _, r := range rows {
		if r.Width > widest {
			widest = r.Width
		}
	}
	for i := range rows {
		rows[i].Width = widest
	}
	return rows
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
