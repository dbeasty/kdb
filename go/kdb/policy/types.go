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
	// HistoryModeFull keeps every commit forever: versioning, AT VERSION,
	// and a complete graph.
	HistoryModeFull HistoryMode = iota
	// HistoryModeNone keeps the current dataset plus the Retain window.
	// Inside the window it behaves exactly as Full does; outside it, the
	// past does not exist and asking for it is an error rather than an
	// answer from head.
	HistoryModeNone
)

// StorageMode maps this policy's history mode onto the storage-layer mode
// the engine is configured with. They are deliberately separate types -
// one is policy the operator writes, the other is what a data directory
// records - and this is the single place the two are related.
func (h HistoryMode) StorageMode() storage.HistoryMode {
	if h == HistoryModeNone {
		return storage.HistoryModeNone
	}
	return storage.HistoryModeFull
}

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

// DocumentExpiryPolicy configures time-to-live for a namespace's documents (kdb-spec-layer16
// §9.5). A document is expired when the value at `$.<FieldPath>` is a timestamp - an RFC 3339
// string or a number of epoch milliseconds - at or before now − GraceMillis. Any other value
// (absent, null, a non-timestamp string, an object) means "never expires". Reads at head hide
// expired documents between sweeps; the sweeper deletes them every SweepIntervalMillis.
type DocumentExpiryPolicy struct {
	// FieldPath is a top-level field name or a dotted path ("meta.expiresAt") into the body.
	FieldPath string
	// GraceMillis keeps a document readable for this long past its timestamp. Default 0.
	GraceMillis int64
	// SweepIntervalMillis is how often the server runtime's sweeper deletes expired documents.
	// Default DefaultSweepIntervalMillis (60 s); zero or negative selects the default.
	SweepIntervalMillis int64
}

// DefaultSweepIntervalMillis is DocumentExpiryPolicy.SweepIntervalMillis's default (§9.5).
const DefaultSweepIntervalMillis int64 = 60_000

// NamespacePolicy is the full policy for one namespace.
type NamespacePolicy struct {
	NamespaceID string
	Schema      *schema.KdbSchema
	Mode        NamespaceMode
	History     HistoryMode
	// Retain bounds how much of the past HistoryModeNone keeps. Ignored
	// under HistoryModeFull, which keeps everything forever. The zero
	// value resolves to storage.DefaultRetentionDuration - see
	// storage.RetentionWindow for why it is a floor and not a ceiling.
	Retain                storage.RetentionWindow
	Conflict              transaction.ConflictPolicy
	Compaction            CompactionPolicy
	Tiers                 TierPolicy
	IndexRetentionDefault storage.IndexRetention
	GpuPromotion          *GpuPromotionPolicyRef
	VectorIndex           VectorIndexPolicy
	// DocumentExpiry is nil when the namespace's documents never expire (the default).
	DocumentExpiry *DocumentExpiryPolicy
	Revision       int64
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
