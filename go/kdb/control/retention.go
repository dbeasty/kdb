package control

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Retention as an operator sees it: what this namespace keeps, what it is
// currently holding that it could release, and the three operations that
// change any of that.
//
// The three are deliberately separate endpoints rather than one, because
// they are not equally consequential and presenting them as one control
// would hide that:
//
//   - Changing the history mode writes a marker. It destroys nothing and is
//     reversible until something compacts.
//   - Changing the reclaim mode decides whether reclamation happens on its
//     own. Still destroys nothing by itself.
//   - Compacting deletes. It is the one-way door, unless an archive is
//     configured, in which case it is eviction and the segments can be
//     brought back.
//
// The status endpoint exists so a UI can put the numbers next to the
// buttons - particularly "you are on manual and holding N segments", which
// is the state that is safe but unbounded and which nobody should be sitting
// in without knowing.

type retentionStatusResponse struct {
	Namespace   string `json:"namespace"`
	HistoryMode string `json:"historyMode"`
	ReclaimMode string `json:"reclaimMode"`
	// Retain is the window, rendered the way it was configured.
	RetainDuration string `json:"retainDuration,omitempty"`
	RetainCommits  int64  `json:"retainCommits,omitempty"`
	// FloorSequence is the lowest segment still readable; anything below it has been reclaimed.
	FloorSequence int64 `json:"floorSequence"`
	// EligibleSegments and EligibleBytes are what a compaction would free right now, computed
	// without freeing any of it.
	EligibleSegments int   `json:"eligibleSegments"`
	EligibleBytes    int64 `json:"eligibleBytes"`
	// ArchiveAvailable says whether a compaction here is eviction (recoverable) or destruction.
	// The single most important thing to show beside a compact button.
	ArchiveAvailable bool `json:"archiveAvailable"`
	// Holding is true when this namespace could reclaim but is choosing not to - reclaim=manual
	// with something eligible. The state that is safe and unbounded at once.
	Holding bool `json:"holding"`
}

// handleRetentionStatus reports what this namespace keeps and what it is holding.
func (s *Server) handleRetentionStatus(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	runtime := rt.Runtime
	if runtime == nil {
		writeError(w, http.StatusServiceUnavailable, "no_runtime", "this namespace has no runtime")
		return
	}
	out := retentionStatusResponse{
		Namespace:        ns,
		HistoryMode:      runtime.HistoryMode().String(),
		ReclaimMode:      runtime.ReclaimMode().String(),
		ArchiveAvailable: runtime.ArchiveAvailable(),
	}
	window := runtime.RetentionWindow().Resolve()
	if window.Duration == storage.RetainNothing {
		out.RetainDuration = "0"
	} else if window.Duration > 0 {
		out.RetainDuration = window.Duration.String()
	}
	out.RetainCommits = window.Commits

	// A dry-run maintenance pass: it writes a checkpoint, which deletes nothing, and reports the
	// plan it did not carry out. Read-only from the caller's point of view.
	if eligible, err := runtime.EligibleForCompaction(); err == nil {
		out.EligibleSegments = eligible.EligibleSegments
		out.EligibleBytes = eligible.EligibleBytes
		out.FloorSequence = eligible.FloorSequence
	}
	out.Holding = runtime.ReclaimMode() == storage.ReclaimManual && out.EligibleSegments > 0
	writeJSON(w, http.StatusOK, out)
}

type historyModeRequest struct {
	Mode string `json:"mode"`
	// Reclaim optionally sets the reclaim mode in the same call. Empty means manual when
	// switching to none, which is what keeps the switch reversible.
	Reclaim string `json:"reclaim"`
}

// handleSetHistoryMode switches a namespace between full and none.
//
// Writes a marker and tells the engine; deletes nothing. The response carries whether history was
// lost - which is only ever true going none -> full on a namespace that has already compacted -
// so a UI can say "this does not bring the past back" at the moment it matters.
func (s *Server) handleSetHistoryMode(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	var req historyModeRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "could not read request body: "+err.Error())
		return
	}
	mode, err := storage.ParseHistoryMode(req.Mode)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if mode == storage.HistoryModeUnset {
		writeError(w, http.StatusBadRequest, "bad_request", `"mode" is required: "full" or "none"`)
		return
	}
	reclaim, err := storage.ParseReclaimMode(req.Reclaim)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	runtime := rt.Runtime
	if runtime == nil {
		writeError(w, http.StatusServiceUnavailable, "no_runtime", "this namespace has no runtime")
		return
	}
	res, err := runtime.SetHistoryMode(mode, reclaim)
	if err != nil {
		writeError(w, http.StatusConflict, "mode_switch_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace":        ns,
		"from":             res.From.String(),
		"to":               res.To.String(),
		"reclaimMode":      res.Reclaim.String(),
		"eligibleSegments": res.EligibleSegments,
		"eligibleBytes":    res.EligibleBytes,
		"historyLost":      res.HistoryLost,
		"note": modeSwitchNote(res.From, res.To, res.HistoryLost, res.Reclaim,
			res.EligibleSegments),
	})
}

// modeSwitchNote is the sentence a UI shows after a switch. Written here rather than in the UI so
// the API and the interface cannot drift into describing the same operation differently.
func modeSwitchNote(from, to storage.HistoryMode, lost bool, reclaim storage.ReclaimMode, eligible int) string {
	switch {
	case to == storage.HistoryModeNone && reclaim == storage.ReclaimManual:
		return fmt.Sprintf(
			"Nothing was deleted. This namespace may now reclaim history but will not do so until "+
				"asked - %d segment(s) are eligible. Switching back to full right now loses nothing.",
			eligible)
	case to == storage.HistoryModeNone:
		return fmt.Sprintf(
			"Nothing was deleted by the switch itself, but this namespace is on reclaim=%s and will "+
				"start releasing history on its own.", reclaim)
	case lost:
		return "History from before the earlier compaction cannot be recovered by this switch. " +
			"From now on nothing further will be reclaimed."
	case from == storage.HistoryModeNone:
		return "Nothing had been reclaimed yet, so no history was lost."
	default:
		return "No change to what this namespace keeps."
	}
}

// handleCompactHistory reclaims now, whatever the reclaim mode says.
//
// This is the destructive one. Without an archive the segments are gone; with one they are
// evicted and restorable, and the response says which happened rather than leaving the caller to
// know their own configuration.
func (s *Server) handleCompactHistory(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	runtime := rt.Runtime
	if runtime == nil {
		writeError(w, http.StatusServiceUnavailable, "no_runtime", "this namespace has no runtime")
		return
	}
	res, err := runtime.CompactHistory()
	if err != nil {
		writeError(w, http.StatusConflict, "compact_failed", err.Error())
		return
	}
	note := "Reclaimed history is gone: this namespace has no archive tier, so it cannot be restored."
	if res.Reversible {
		note = "Reclaimed segments were evicted from local disk and remain in the archive; " +
			"they can be restored."
	}
	if res.Removed == 0 {
		note = "Nothing was eligible, so nothing was reclaimed."
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace":      ns,
		"removed":        res.Removed,
		"reclaimedBytes": res.ReclaimedBytes,
		"floorSequence":  res.FloorSequence,
		"tablesMerged":   res.TablesMerged,
		"tablesRemoved":  res.TablesRemoved,
		"reversible":     res.Reversible,
		"note":           note,
	})
}

type restoreRequest struct {
	From int64 `json:"from"`
	To   int64 `json:"to"`
}

// handleRestoreFromArchive brings reclaimed segments back and lowers the floor to admit them.
func (s *Server) handleRestoreFromArchive(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	var req restoreRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "could not read request body: "+err.Error())
		return
	}
	runtime := rt.Runtime
	if runtime == nil {
		writeError(w, http.StatusServiceUnavailable, "no_runtime", "this namespace has no runtime")
		return
	}
	res, err := runtime.RestoreFromArchive(req.From, req.To)
	if err != nil {
		writeError(w, http.StatusConflict, "restore_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace":     ns,
		"restored":      res.Restored,
		"restoredBytes": res.RestoredBytes,
		"floorSequence": res.FloorSequence,
		"missing":       res.Missing,
		"tookMs":        res.Took.Milliseconds(),
	})
}
