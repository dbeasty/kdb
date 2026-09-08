package policy

import (
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// DefaultMutable returns a standard mutable namespace policy.
func DefaultMutable(namespaceID string, sch *schema.KdbSchema) NamespacePolicy {
	return NamespacePolicy{
		NamespaceID: namespaceID,
		Schema:      sch,
		Mode:        NamespaceModeMutable,
		History:     HistoryModeFull,
		Conflict:    transaction.ConflictPolicyStrict,
		Compaction: CompactionPolicy{
			KeepTagged:        true,
			KeepBranchPoints:  true,
			SquashAfter:       SquashModeAuto,
			RetainGranularity: DefaultRetainGranularity(),
		},
		Tiers:                 DefaultTierPolicy(),
		IndexRetentionDefault: storage.IndexRetentionEvictable,
		VectorIndex:           VectorIndexPolicy{HnswM: 16, HnswEfConstruction: 200, DefaultDimensions: 128},
		Revision:              1,
	}
}

// AppendOnlyEvents returns an append-only event stream policy.
func AppendOnlyEvents(namespaceID string, sch schema.KdbSchema) NamespacePolicy {
	return NamespacePolicy{
		NamespaceID: namespaceID,
		Schema:      &sch,
		Mode:        NamespaceModeAppendOnly,
		History:     HistoryModeFull,
		Conflict:    transaction.ConflictPolicyAppendOnly,
		Compaction: CompactionPolicy{
			SquashAfter:       SquashModeNever,
			RetainGranularity: nil,
		},
		Tiers: DefaultTierPolicy(),
	}
}

// ScratchDocument returns a document namespace without schema.
func ScratchDocument(namespaceID string) NamespacePolicy {
	return DefaultMutable(namespaceID, nil)
}

// CacheNoHistory returns a cache namespace without history.
func CacheNoHistory(namespaceID string) NamespacePolicy {
	return NamespacePolicy{
		NamespaceID: namespaceID,
		Mode:        NamespaceModeMutable,
		History:     HistoryModeNone,
		// The default window rather than nothing: the journal holds recent
		// frames for crash recovery regardless, so a day of retention is
		// very nearly free, and it turns "no undo at all" into "undo
		// within a window".
		Retain:   storage.RetentionWindow{Duration: storage.DefaultRetentionDuration},
		Conflict: transaction.ConflictPolicyLastWrite,
		Compaction: CompactionPolicy{
			SquashAfter:       SquashModeNever,
			RetainGranularity: nil,
		},
		IndexRetentionDefault: storage.IndexRetentionEvictable,
		Tiers:                 DefaultTierPolicy(),
		Revision:              1,
	}
}
