package tier

import (
	"context"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Signal is emitted when a segment changes tier.
type Signal struct {
	Namespace string
	Segment   string
	FromTier  Tier
	ToTier    Tier
}

// Tier is a storage temperature class.
type Tier int

const (
	TierHot Tier = iota
	TierWarm
	TierCold
	TierIce
)

// Manager coordinates WARM/COLD/ICE transitions.
type Manager struct {
	onTransition func(Signal)
}

func NewManager() *Manager { return &Manager{} }

func (m *Manager) OnTransition(fn func(Signal)) { m.onTransition = fn }

func (m *Manager) ArchiveCommit(_ context.Context, _ string, commit codec.Hash, location string) error {
	if m.onTransition != nil {
		m.onTransition(Signal{ToTier: TierIce, Segment: commit.Hex()})
	}
	_ = location
	return nil
}
