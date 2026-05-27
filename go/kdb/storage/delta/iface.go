package delta

import "github.com/limidus/kdb/go/kdb/storage"

var (
	_ storage.DeltaSegmentWriter = (*DefaultWriter)(nil)
	_ storage.DeltaSegmentReader = (*DefaultReader)(nil)
)
