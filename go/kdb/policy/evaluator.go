package policy

import (
	"github.com/limidus/kdb/go/kdb/codec"
)

// BoundaryPlan is one planned compaction boundary.
type BoundaryPlan struct {
	Boundary      codec.Hash
	SquashThrough codec.Hash
	Strategy      RetainStrategy
}

// Evaluator computes compaction boundary candidates from policy.
type Evaluator interface {
	BoundaryCandidates(
		policy CompactionPolicy,
		commitTimestamps map[codec.Hash]codec.Timestamp,
		tagged, branchHeads map[codec.Hash]struct{},
		head codec.Hash,
		parentOf func(codec.Hash) *codec.Hash,
	) []BoundaryPlan
}

// DefaultEvaluator is the standard compaction policy evaluator.
var DefaultEvaluator Evaluator = defaultEvaluator{}

type defaultEvaluator struct{}

func (defaultEvaluator) BoundaryCandidates(
	policy CompactionPolicy,
	commitTimestamps map[codec.Hash]codec.Timestamp,
	tagged, branchHeads map[codec.Hash]struct{},
	head codec.Hash,
	parentOf func(codec.Hash) *codec.Hash,
) []BoundaryPlan {
	if policy.SquashAfter == SquashModeNever || len(commitTimestamps) == 0 {
		return nil
	}
	now, ok := commitTimestamps[head]
	if !ok {
		return nil
	}
	protected := make(map[codec.Hash]struct{})
	for h := range tagged {
		protected[h] = struct{}{}
	}
	for h := range branchHeads {
		protected[h] = struct{}{}
	}
	ordered := linearAncestors(head, parentOf)
	var filtered []codec.Hash
	for _, h := range ordered {
		if _, ok := commitTimestamps[h]; ok {
			filtered = append(filtered, h)
		}
	}
	if len(filtered) < 2 {
		return nil
	}
	rules := append([]RetainRule(nil), policy.RetainGranularity...)
	sortRules(rules)
	var plans []BoundaryPlan
	seen := make(map[codec.Hash]struct{})
	for _, rule := range rules {
		cutoffMicros := now.EpochMicros() - rule.OlderThanMillis*1000
		var candidates []codec.Hash
		for _, h := range filtered {
			ts, ok := commitTimestamps[h]
			if !ok {
				continue
			}
			if ts.EpochMicros() <= cutoffMicros {
				candidates = append(candidates, h)
			}
		}
		if len(candidates) == 0 {
			continue
		}
		var squashable []codec.Hash
		switch rule.Strategy {
		case RetainStrategyFullHistory:
			squashable = nil
		case RetainStrategyTaggedOnly:
			for _, h := range candidates {
				if _, prot := protected[h]; !prot {
					squashable = append(squashable, h)
				}
			}
		case RetainStrategyDailySnapshots:
			squashable = dailySnapshotSquashCandidates(candidates, commitTimestamps, protected)
		}
		if len(squashable) < 2 {
			continue
		}
		boundary := squashable[0]
		if _, dup := seen[boundary]; dup {
			continue
		}
		seen[boundary] = struct{}{}
		plans = append(plans, BoundaryPlan{
			Boundary:      boundary,
			SquashThrough: squashable[len(squashable)-1],
			Strategy:      rule.Strategy,
		})
	}
	return plans
}

func sortRules(rules []RetainRule) {
	for i := 0; i < len(rules); i++ {
		for j := i + 1; j < len(rules); j++ {
			if rules[j].OlderThanMillis < rules[i].OlderThanMillis {
				rules[i], rules[j] = rules[j], rules[i]
			}
		}
	}
}

func linearAncestors(head codec.Hash, parentOf func(codec.Hash) *codec.Hash) []codec.Hash {
	var out []codec.Hash
	cur := &head
	for cur != nil {
		out = append(out, *cur)
		cur = parentOf(*cur)
	}
	return out
}

func dailySnapshotSquashCandidates(
	candidates []codec.Hash,
	timestamps map[codec.Hash]codec.Timestamp,
	protected map[codec.Hash]struct{},
) []codec.Hash {
	byDay := make(map[int64]codec.Hash)
	for _, h := range candidates {
		if _, prot := protected[h]; prot {
			continue
		}
		ts, ok := timestamps[h]
		if !ok {
			continue
		}
		day := ts.EpochMicros() / (24 * 3600 * 1_000_000)
		byDay[day] = h
	}
	keep := make(map[codec.Hash]struct{})
	for _, h := range byDay {
		keep[h] = struct{}{}
	}
	var out []codec.Hash
	for _, h := range candidates {
		if _, k := keep[h]; k {
			continue
		}
		if _, prot := protected[h]; prot {
			continue
		}
		out = append(out, h)
	}
	return out
}
