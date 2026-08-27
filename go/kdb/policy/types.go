package policy

import (
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// NamespaceMode controls mutability.
type NamespaceMode int

const (
	NamespaceModeMutable NamespaceMode = iota
	NamespaceModeAppendOnly
)

// HistoryMode controls history retention semantics.
type HistoryMode int

const (
	HistoryModeFull HistoryMode = iota
	HistoryModeNone
)

// SquashMode controls automatic compaction squashing.
type SquashMode int

const (
	SquashModeAuto SquashMode = iota
	SquashModeNever
)

// RetainStrategy names a retention granularity strategy.
type RetainStrategy int

const (
	RetainStrategyFullHistory RetainStrategy = iota
	RetainStrategyDailySnapshots
	RetainStrategyTaggedOnly
)

// StorageKind names a storage tier backend.
type StorageKind int

const (
	StorageKindLocal StorageKind = iota
	StorageKindLocalFS
	StorageKindObjectStore
	StorageKindArchive
)

// RetainRule is one retention granularity rule.
type RetainRule struct {
	OlderThanMillis int64
	Strategy        RetainStrategy
}

// CompactionPolicy configures DAG compaction behavior.
type CompactionPolicy struct {
	KeepTagged        bool
	KeepBranchPoints  bool
	SquashAfter       SquashMode
	RetainGranularity []RetainRule
}

// TierBand is one hot/warm/cold band.
type TierBand struct {
	MaxAgeMillis int64
	StorageKind  StorageKind
}

// IceTierBand is the archive tier.
type IceTierBand struct {
	StorageKind StorageKind
}

// TierPolicy configures tier bands.
type TierPolicy struct {
	Hot  TierBand
	Warm TierBand
	Cold TierBand
	Ice  IceTierBand
}

// GpuPromotionPolicyRef references GPU promotion thresholds.
type GpuPromotionPolicyRef struct {
	MinSegmentAgeMillis    int64
	MinSegmentSizeBytes    int64
	MaxChangeRatePerMinute float64
}

// VectorIndexPolicy configures vector index defaults.
type VectorIndexPolicy struct {
	HnswM              int
	HnswEfConstruction int
	DefaultDimensions  int
}

// NamespacePolicy is the full policy for one namespace.
type NamespacePolicy struct {
	NamespaceID           string
	Schema                *schema.KdbSchema
	Mode                  NamespaceMode
	History               HistoryMode
	Conflict              transaction.ConflictPolicy
	Compaction            CompactionPolicy
	Tiers                 TierPolicy
	IndexRetentionDefault storage.IndexRetention
	GpuPromotion          *GpuPromotionPolicyRef
	VectorIndex           VectorIndexPolicy
	Revision              int64
}

// DefaultRetainGranularity returns the default retain rules.
func DefaultRetainGranularity() []RetainRule {
	return []RetainRule{
		{OlderThanMillis: 7 * 24 * 3600 * 1000, Strategy: RetainStrategyFullHistory},
		{OlderThanMillis: 30 * 24 * 3600 * 1000, Strategy: RetainStrategyDailySnapshots},
		{OlderThanMillis: 365 * 24 * 3600 * 1000, Strategy: RetainStrategyTaggedOnly},
	}
}

// DefaultTierPolicy returns default tier bands.
func DefaultTierPolicy() TierPolicy {
	return TierPolicy{
		Hot:  TierBand{MaxAgeMillis: 7 * 24 * 3600 * 1000, StorageKind: StorageKindLocal},
		Warm: TierBand{MaxAgeMillis: 90 * 24 * 3600 * 1000, StorageKind: StorageKindLocal},
		Cold: TierBand{MaxAgeMillis: 365 * 24 * 3600 * 1000, StorageKind: StorageKindLocal},
		Ice:  IceTierBand{StorageKind: StorageKindArchive},
	}
}
