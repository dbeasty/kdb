// Package fusion merges per-arm ranked lists into one ranking (Layer 16, Component 65).
//
// Both fusion modes read only positions or per-arm normalised scores, never raw cross-arm
// scores: BM25 is unbounded and cosine sits in [-1, 1], so adding them would mean recalibrating
// two moving scales every time either arm changed. Output order is deterministic — fused score
// descending, then document id ascending — so a Go and a Kotlin engine over the same corpus
// return the same list.
package fusion

import (
	"math"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/index"
)

// Mode selects how arms are combined.
type Mode int

const (
	// ModeRRF is reciprocal rank fusion: score = Σ weight / (k + rank).
	ModeRRF Mode = iota
	// ModeWeightedSum min-max normalises each arm to [0, 1] and sums weight · normalised score.
	ModeWeightedSum
)

// DefaultRRFK is the rank offset used by ModeRRF.
const DefaultRRFK = 60

// Arm is one ranked input list plus its fusion parameters.
type Arm struct {
	// Results must already be sorted by score descending, document id ascending.
	Results []index.RankedResult
	// Weight scales this arm's contribution. Zero is treated as 1.
	Weight float64
	// Depth truncates the arm after MinScore filtering; 0 means unlimited.
	Depth int
	// MinScore drops results scoring below it before truncation. Zero value means no floor
	// only when HasMinScore is false.
	MinScore    float32
	HasMinScore bool
}

// Fuse merges arms under mode and returns at most limit results (limit <= 0 means all).
func Fuse(arms []Arm, mode Mode, limit int) []index.RankedResult {
	type acc struct {
		id    codec.UUID
		score float64
	}
	scores := make(map[codec.UUID]*acc)
	order := make([]codec.UUID, 0)
	bump := func(id codec.UUID, delta float64) {
		a, ok := scores[id]
		if !ok {
			a = &acc{id: id}
			scores[id] = a
			order = append(order, id)
		}
		a.score += delta
	}
	for _, arm := range arms {
		w := arm.Weight
		if w == 0 {
			w = 1
		}
		list := prepare(arm)
		switch mode {
		case ModeWeightedSum:
			lo, hi := float32(math.Inf(1)), float32(math.Inf(-1))
			for _, r := range list {
				if r.Score < lo {
					lo = r.Score
				}
				if r.Score > hi {
					hi = r.Score
				}
			}
			// Normalise in float64 from the float32 inputs (and round to float32 only once, at the
			// end): both trees do the same, so fused scores are bit-identical, which the fixture
			// tolerance of 1e-9 needs at float32 precision.
			for _, r := range list {
				norm := 1.0
				if hi > lo {
					norm = (float64(r.Score) - float64(lo)) / (float64(hi) - float64(lo))
				}
				bump(r.DocID, w*norm)
			}
		default:
			for i, r := range list {
				bump(r.DocID, w/float64(DefaultRRFK+i+1))
			}
		}
	}
	out := make([]index.RankedResult, 0, len(order))
	for _, id := range order {
		out = append(out, index.RankedResult{DocID: id, Score: float32(scores[id].score)})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].DocID.String() < out[j].DocID.String()
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

func prepare(arm Arm) []index.RankedResult {
	list := arm.Results
	if arm.HasMinScore {
		kept := make([]index.RankedResult, 0, len(list))
		for _, r := range list {
			if r.Score >= arm.MinScore {
				kept = append(kept, r)
			}
		}
		list = kept
	}
	if arm.Depth > 0 && len(list) > arm.Depth {
		list = list[:arm.Depth]
	}
	return list
}
