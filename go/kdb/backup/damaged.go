package backup

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/storage/delta"
)

// DamagedSegmentError refuses a backup because one of the namespace's delta segments is damaged
// in place: intact frames follow bad bytes, or a frame passes its CRC and will not decode (the
// storage.TailDamaged rule replay and kdb-inspect verify apply). The commits after the damage were
// acknowledged, so a backup of only the bytes before it would look complete and silently lack
// them - and restoring it would carry that loss into the restored namespace.
//
// Nothing was uploaded and no manifest was written. Detect with errors.As; the scanner's
// *delta.DamagedSegmentError stays reachable through Unwrap.
type DamagedSegmentError struct {
	NamespaceID string
	Sequence    int64
	// Offset is where the segment's intact frames stop.
	Offset int64
	Reason string
	// NextIntactFrame is the offset of the first intact frame after the damage, or -1 when the
	// frame at Offset is complete and passes its CRC but will not decode.
	NextIntactFrame int64
	// Incremental is set when the refused backup was an incremental one.
	Incremental bool
	Err         *delta.DamagedSegmentError
}

func (e *DamagedSegmentError) Error() string {
	what := "the frame there is complete and passes its checksum but will not decode"
	if e.NextIntactFrame >= 0 {
		what = fmt.Sprintf("intact frames follow the damage (the next at byte %d)", e.NextIntactFrame)
	}
	kind := "backup"
	if e.Incremental {
		kind = "incremental backup"
	}
	return fmt.Sprintf(
		"kdb: %s of namespace %q refused: delta segment %d is damaged at byte %d (%s); %s, so this is "+
			"not a torn tail and the commits after it were acknowledged. Backing up only the bytes before "+
			"the damage would silently leave them out, so nothing was uploaded and no manifest was written. "+
			"Run 'kdb-inspect verify --data-dir <dir> --namespace %s' to see the damage, then "+
			"'kdb-inspect repair-segments' (which repairs only what it can prove loses nothing) or restore "+
			"the namespace from a backup or a peer ('kdb-inspect restore'), and back up again",
		kind, e.NamespaceID, e.Sequence, e.Offset, e.Reason, what, e.NamespaceID)
}

func (e *DamagedSegmentError) Unwrap() error {
	if e.Err == nil {
		return nil
	}
	return e.Err
}
