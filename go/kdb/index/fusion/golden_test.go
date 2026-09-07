package fusion_test

import (
	"encoding/json"
	"math"
	"os"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/fusion"
)

const fixturePath = "../../../testdata/golden/search/fusion_cases.json"

type armJSON struct {
	Weight   float64             `json:"weight"`
	Depth    int                 `json:"depth"`
	MinScore *float64            `json:"minScore"`
	Results  [][]json.RawMessage `json:"results"`
}

type caseJSON struct {
	Name     string              `json:"name"`
	Mode     string              `json:"mode"`
	Limit    int                 `json:"limit"`
	Arms     []armJSON           `json:"arms"`
	Expected [][]json.RawMessage `json:"expected"`
}

// ranked decodes the fixture's [docId, score] rows.
func ranked(t *testing.T, rows [][]json.RawMessage) []index.RankedResult {
	t.Helper()
	out := make([]index.RankedResult, 0, len(rows))
	for _, row := range rows {
		if len(row) != 2 {
			t.Fatalf("result row has %d entries, want [docId, score]", len(row))
		}
		var idStr string
		if err := json.Unmarshal(row[0], &idStr); err != nil {
			t.Fatal(err)
		}
		id, err := codec.UUIDFromString(idStr)
		if err != nil {
			t.Fatal(err)
		}
		var f float64
		if err := json.Unmarshal(row[1], &f); err != nil {
			t.Fatal(err)
		}
		out = append(out, index.RankedResult{DocID: id, Score: float32(f)})
	}
	return out
}

// TestFusionGoldenCases pins both fusion modes - and the depth, minScore, weight and tie
// rules around them - to the fixture the Kotlin tree asserts against. Scores must agree to
// 1e-9 (§8), which is only possible because both trees accumulate in float64 and round once.
func TestFusionGoldenCases(t *testing.T) {
	raw, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Cases []caseJSON `json:"cases"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Cases) < 10 {
		t.Fatalf("fixture has %d cases, spec requires at least 10", len(file.Cases))
	}
	for _, c := range file.Cases {
		t.Run(c.Name, func(t *testing.T) {
			var arms []fusion.Arm
			for _, a := range c.Arms {
				arm := fusion.Arm{Results: ranked(t, a.Results), Weight: a.Weight, Depth: a.Depth}
				if a.MinScore != nil {
					arm.HasMinScore = true
					arm.MinScore = float32(*a.MinScore)
				}
				arms = append(arms, arm)
			}
			mode := fusion.ModeRRF
			if c.Mode == "weighted" {
				mode = fusion.ModeWeightedSum
			}
			got := fusion.Fuse(arms, mode, c.Limit)
			want := ranked(t, c.Expected)
			if len(got) != len(want) {
				t.Fatalf("got %d results, want %d:\n got %v\n want %v", len(got), len(want), got, want)
			}
			for i := range got {
				if got[i].DocID != want[i].DocID {
					t.Fatalf("position %d: got %s, want %s", i, got[i].DocID, want[i].DocID)
				}
				if math.Abs(float64(got[i].Score)-float64(want[i].Score)) > 1e-9 {
					t.Errorf("position %d (%s): score %v, want %v", i, got[i].DocID, got[i].Score, want[i].Score)
				}
			}
		})
	}
}

// TestRRFScoreMatchesHandComputation derives one fixture case by hand rather than from the
// implementation, so a bug that changed both the code and the regenerated fixture together
// would still be caught.
//
// Case rrf_two_arms_overlap, k = 60, both arms weight 1:
//
//	arm A ranks doc1, doc2, doc3 (ranks 1, 2, 3)
//	arm B ranks doc2, doc4, doc1 (ranks 1, 2, 3)
//
//	doc2 = 1/(60+2) + 1/(60+1) = 1/62 + 1/61 = 0.016129032 + 0.016393443 = 0.032522473
//	doc1 = 1/(60+1) + 1/(60+3) = 1/61 + 1/63 = 0.016393443 + 0.015873017 = 0.032266457
//	doc4 = 1/(60+2)            = 1/62                                    = 0.016129032
//	doc3 = 1/(60+3)            = 1/63                                    = 0.015873017
//
// so the order is doc2, doc1, doc4, doc3 - doc2 beating doc1 only because 1/62 + 1/61 exceeds
// 1/61 + 1/63, which is the whole point of rank fusion over raw scores (doc1 tops arm A with
// score 1.0 and still loses).
func TestRRFScoreMatchesHandComputation(t *testing.T) {
	id := func(n int) codec.UUID {
		u, err := codec.UUIDFromString("00000000-0000-4000-8000-00000000000" + string(rune('0'+n)))
		if err != nil {
			t.Fatal(err)
		}
		return u
	}
	armA := []index.RankedResult{{DocID: id(1), Score: 1}, {DocID: id(2), Score: 0.5}, {DocID: id(3), Score: 0.25}}
	armB := []index.RankedResult{{DocID: id(2), Score: 0.9}, {DocID: id(4), Score: 0.8}, {DocID: id(1), Score: 0.1}}
	got := fusion.Fuse([]fusion.Arm{{Results: armA}, {Results: armB}}, fusion.ModeRRF, 0)

	want := []struct {
		id    codec.UUID
		score float64
	}{
		{id(2), 1.0/62 + 1.0/61},
		{id(1), 1.0/61 + 1.0/63},
		{id(4), 1.0 / 62},
		{id(3), 1.0 / 63},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d results, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].DocID != w.id {
			t.Fatalf("position %d: got %s, want %s", i, got[i].DocID, w.id)
		}
		if math.Abs(float64(got[i].Score)-w.score) > 1e-7 {
			t.Errorf("position %d: score %v, want %v", i, got[i].Score, w.score)
		}
	}
}

// TestWeightedSumNormalisesPerArm: an arm whose results all score the same normalises to 1.0
// for every result (§8.3), so a one-result arm cannot be silently zeroed.
func TestWeightedSumNormalisesPerArm(t *testing.T) {
	id := func(n int) codec.UUID {
		u, _ := codec.UUIDFromString("00000000-0000-4000-8000-00000000000" + string(rune('0'+n)))
		return u
	}
	arm := fusion.Arm{Results: []index.RankedResult{{DocID: id(1), Score: 0.7}, {DocID: id(2), Score: 0.7}}}
	got := fusion.Fuse([]fusion.Arm{arm}, fusion.ModeWeightedSum, 0)
	for _, r := range got {
		if math.Abs(float64(r.Score)-1.0) > 1e-9 {
			t.Errorf("%s scored %v, want 1.0", r.DocID, r.Score)
		}
	}
}
