package control

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/recovery"
)

// Promotion: making a staged restore the live namespace.
//
// This is the last piece of §8.4, and the one the plan was careful about: "swapping the live data
// directory for a staged one means stopping the process, and a button that implied otherwise would
// be lying." The button here does not imply otherwise. It does exactly three things, and says so:
//
//  1. Copies the staged namespace into the live data root, where installing it will be a rename.
//     This happens while the server serves, so it can fail before anything is committed to.
//  2. Records the intent, which the *next startup* acts on - before the data root is opened, when
//     nothing holds its lock. recovery.ApplyPromotion is that half.
//  3. Asks the process to exit so a supervisor restarts it.
//
// Step 3 is the honest limit. This process cannot restart itself (the same limit --abort-after has
// had since Component 50), so promotion is available only when the deployment says a supervisor
// exists - and if there is none, the service stays down until someone starts it. That is stated in
// the plan the operator reads before confirming, not discovered afterwards.
//
// Everything irreversible is deferred to the restart, and the namespace it replaces is moved
// aside rather than deleted. Until the restart happens the promotion can simply be abandoned.

// promotionPlan is what a promotion would do, and what it needs.
type promotionPlan struct {
	Namespace string `json:"namespace"`
	JobID     string `json:"jobId"`
	// StagedDir is the restored data root that would replace the live namespace.
	StagedDir string `json:"stagedDirectory"`
	LiveDir   string `json:"liveDirectory"`
	// Steps is the sequence in the order it happens, for a UI that should not paraphrase it.
	Steps []string `json:"steps"`
	// Blockers are the reasons this cannot proceed. Empty means it can.
	Blockers []string `json:"blockers,omitempty"`
	// Warnings are things that will happen and are worth knowing, but do not stop it.
	Warnings []string `json:"warnings,omitempty"`
	// Restart says what the operator has to arrange.
	Restart string `json:"restart"`
	// Rollback says how to undo it, which is the question anyone sensible asks second.
	Rollback string `json:"rollback"`
	// Confirm is the exact string the apply call must echo back.
	Confirm string `json:"confirmWith"`
}

// promoteRequest is the apply call's body.
type promoteRequest struct {
	// Confirm must equal the namespace being replaced. A typed confirmation on an operation that
	// ends with the process exiting is worth the friction.
	Confirm string `json:"confirm"`
}

// handlePromotionPlan describes what promoting a staged restore would do, without doing any of it.
func (s *Server) handlePromotionPlan(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	plan, status, err := s.buildPromotionPlan(r.PathValue("jobId"))
	if err != nil {
		writeError(w, status, "promotion_unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

// buildPromotionPlan resolves the job, the directories and everything standing in the way.
//
// Blockers are collected rather than returned one at a time: an operator deciding whether this is
// a route out of an incident should see every reason it is not in one answer.
func (s *Server) buildPromotionPlan(jobID string) (*promotionPlan, int, error) {
	// A snapshot, not the pointer: a restore still running writes these fields from its own
	// goroutine, and a plan built from a half-written job would describe something that was never
	// true. See snapshotJob.
	job, ok := s.snapshotJob(jobID)
	if !ok {
		return nil, http.StatusNotFound, fmt.Errorf("no restore job called %s", jobID)
	}

	rt, live := s.namespaces().Runtime(job.Namespace)
	if !live {
		return nil, http.StatusNotFound, fmt.Errorf(
			"this control plane no longer serves %s, so it cannot promote into it", job.Namespace)
	}
	liveRoot := dataRootFor(rt)
	staged := filepath.Join(job.Dir, "restored")

	plan := &promotionPlan{
		Namespace: job.Namespace,
		JobID:     job.ID,
		StagedDir: staged,
		LiveDir:   recovery.NamespaceDir(liveRoot, job.Namespace),
		Confirm:   job.Namespace,
		Steps: []string{
			"Copy the restored namespace into the live data root, under " +
				filepath.Join(liveRoot, ".kdb.promote") + ". The live namespace is not touched.",
			"Record the promotion so the next startup carries it out.",
			"Shut down: readiness off, stop admitting writes, drain in-flight writes, flush and " +
				"seal storage, exit 75.",
			"On the next startup, before the data root is opened: verify the copy, move the live " +
				"namespace to " + recovery.SupersededDir + "/, rename the copy into place.",
		},
		Restart: "This process cannot restart itself. Exit 75 is the supervisor contract " +
			"(Docker --restart=on-failure, systemd Restart=on-failure). Without a supervisor the " +
			"service stays down until someone starts it.",
		Rollback: "The namespace being replaced is moved to " + recovery.SupersededDir +
			"/ inside the data root, not deleted. To undo a promotion: stop the service, move that " +
			"directory back over ns/" + job.Namespace + ", and delete the stale checkpoint under snap/.",
	}

	if job.State != "complete" {
		plan.Blockers = append(plan.Blockers, "this restore is "+job.State+
			"; only a completed restore can be promoted")
	}
	if len(job.Missing) > 0 {
		plan.Warnings = append(plan.Warnings, fmt.Sprintf(
			"this restore is missing %d commit(s) whose parent was in no source. Promoting it "+
				"makes that shortfall the live history.", len(job.Missing)))
	}
	if liveRoot == "" {
		plan.Blockers = append(plan.Blockers,
			"this namespace is in memory; there is no directory to replace")
	}
	if !s.opts.AllowWrites {
		plan.Blockers = append(plan.Blockers,
			"this control plane is read-only; start the service with --control-write")
	}
	if !s.opts.AllowPromotion {
		plan.Blockers = append(plan.Blockers,
			"promotion is not enabled on this deployment. It ends with the process exiting for a "+
				"supervisor to restart, so it has to be asked for: --control-promote")
	}
	if s.opts.RequestRestart == nil {
		plan.Blockers = append(plan.Blockers,
			"this process did not give the control plane a way to shut itself down, so it cannot "+
				"complete a promotion")
	}
	if job.Attached != "" {
		plan.Blockers = append(plan.Blockers, "this restore is attached as "+job.Attached+
			"; detach it first so nothing is reading the copy while it is moved")
	}
	if liveRoot != "" {
		// A cross-device install cannot be a rename, and this whole design rests on the install
		// being one. Checked here so it is refused while the operator is still reading, rather
		// than discovered by a startup that has already stopped serving.
		//
		// Staging is *meant* to be on another volume ("a restore is most needed exactly when the
		// data volume is the problem"), which is exactly why the copy in step 1 exists: it lands
		// inside the live root, so the rename at startup is same-filesystem by construction. What
		// is checked is that the payload directory really is on the data root's filesystem.
		same, err := recovery.SameFilesystem(liveRoot, filepath.Join(liveRoot, ".kdb.promote"))
		if err != nil {
			plan.Warnings = append(plan.Warnings,
				"could not confirm the staged copy will land on the data directory's filesystem: "+err.Error())
		} else if !same {
			plan.Blockers = append(plan.Blockers,
				"the data root's .kdb.promote path is on a different filesystem from the data "+
					"itself, so the install could not be an atomic rename")
		}
		if free, err := freeBytes(liveRoot); err == nil {
			if size, err := dirSize(staged); err == nil {
				switch {
				case size > free:
					plan.Blockers = append(plan.Blockers, fmt.Sprintf(
						"not enough room on the data volume: the copy needs about %s and %s is free",
						humanBytes(size), humanBytes(free)))
				case size*10 > free:
					// Mentioned only when it is close enough to matter. "This 600-byte copy needs
					// 600 bytes of your 16 GiB" is noise in a dialog whose whole job is to be read.
					plan.Warnings = append(plan.Warnings, fmt.Sprintf(
						"the copy needs about %s on the data volume, which has only %s free",
						humanBytes(size), humanBytes(free)))
				}
			}
		}
	}
	if liveRoot != "" {
		// A restore rebuilds the delta log and nothing else, so a promoted copy has no tree
		// objects. That makes it a replay-strategy namespace whatever the one it replaces was, and
		// an operator finding that out from a slow historical read later would be right to be
		// annoyed. See promotedMarker.
		if _, note, err := promotedMarker(liveRoot, job.Namespace); err != nil {
			plan.Blockers = append(plan.Blockers, err.Error())
		} else if note != "" {
			plan.Warnings = append(plan.Warnings, note)
			// And if this process was told to open namespaces as objects, the promoted one would
			// be refused at open - which means the service would not come back up.
			if d, ok := s.descriptor("history.strategy"); ok && fmt.Sprint(d.Value) == "objects" {
				plan.Blockers = append(plan.Blockers,
					"this process is configured with history.strategy=objects (KDB_HISTORY_STRATEGY), "+
						"and the promoted copy would be a replay namespace: the open would be "+
						"refused and the service would not come back up. Unset that variable, or "+
						"convert the copy with kdb-inspect migrate-history before promoting it")
			}
		}
	}
	if pending, err := s.pendingPromotion(liveRoot); err == nil && pending != nil {
		plan.Blockers = append(plan.Blockers, fmt.Sprintf(
			"a promotion of %s is already staged and waiting for a restart; abandon it first "+
				"(DELETE /v1/promotion) or restart to apply it", pending.Namespace))
	}
	return plan, http.StatusOK, nil
}

// handlePromote stages the promotion and asks the process to restart.
func (s *Server) handlePromote(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	plan, status, err := s.buildPromotionPlan(r.PathValue("jobId"))
	if err != nil {
		writeError(w, status, "promotion_unavailable", err.Error())
		return
	}
	if len(plan.Blockers) > 0 {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": map[string]any{
				"code":    "promotion_blocked",
				"message": "this promotion cannot proceed",
			},
			"blockers": plan.Blockers,
			"plan":     plan,
		})
		return
	}

	var req promoteRequest
	if r.Body != nil {
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&req)
	}
	if req.Confirm != plan.Confirm {
		writeError(w, http.StatusBadRequest, "confirmation_required",
			"this replaces the live "+plan.Namespace+" and ends with the process exiting for a "+
				"supervisor to restart it. Send {\"confirm\":\""+plan.Confirm+"\"} to proceed.")
		return
	}

	rt, ok := s.namespaces().Runtime(plan.Namespace)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no runtime for "+plan.Namespace)
		return
	}
	liveRoot := dataRootFor(rt)

	// Serialized against settings patches and each other: two promotions staged at once would
	// leave one payload and two intents disagreeing about it.
	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	marker, _, err := promotedMarker(liveRoot, plan.Namespace)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "staging_failed",
			"nothing was changed: "+err.Error())
		return
	}
	intent, err := recovery.StagePromotion(liveRoot, plan.StagedDir, plan.Namespace,
		recovery.PromotionIntent{
			JobID:       plan.JobID,
			RequestedAt: s.opts.Now().UTC(),
			RequestedBy: principalName(principal),
		}, marker)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "staging_failed",
			"nothing was changed: "+err.Error())
		return
	}

	// The response goes out before the shutdown is requested. A promotion whose acknowledgement
	// died with the connection would leave the operator unable to tell a refused request from a
	// successful one, at the exact moment the server stops answering.
	writeJSON(w, http.StatusAccepted, map[string]any{
		"namespace":  intent.Namespace,
		"jobId":      intent.JobID,
		"staged":     true,
		"commits":    intent.Commits,
		"bytes":      intent.Bytes,
		"appliesOn":  "the next startup, before the data root is opened",
		"outcomeAt":  "GET /v1/promotion",
		"restarting": true,
		"note": "The copy is staged and recorded. This process is now shutting down (exit 75) so " +
			"a supervisor restarts it; the promotion is applied on the way back up. Until then it " +
			"can still be cancelled with DELETE /v1/promotion. Read GET /v1/promotion after the " +
			"restart for the result.",
	})
	flush(w)

	if err := s.opts.RequestRestart("promoting " + intent.Namespace + " from restore " + intent.JobID); err != nil {
		// The intent stays: a restart the operator arranges by hand applies it just the same.
		// Nothing here can be reported - the response has already gone.
		s.logf("promotion staged but the restart could not be requested: %v", err)
	}
}

// handlePromotionStatus reports a pending promotion and the outcome of the last one.
//
// The second half is what makes the whole flow usable: the request that mattered was made to a
// process that no longer exists, so the only place its result can live is on disk.
func (s *Server) handlePromotionStatus(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	root, err := s.defaultDataRoot()
	if err != nil {
		writeError(w, http.StatusBadRequest, "not_file_backed", err.Error())
		return
	}
	pending, pErr := recovery.PendingPromotion(root)
	last, lErr := recovery.LastPromotion(root)
	body := map[string]any{"dataRoot": root}
	if pending != nil {
		body["pending"] = pending
		body["note"] = "this promotion is staged and will be applied by the next startup. It can " +
			"still be cancelled."
	}
	if last != nil {
		body["last"] = last
	}
	if pErr != nil {
		body["pendingError"] = pErr.Error()
	}
	if lErr != nil {
		body["lastError"] = lErr.Error()
	}
	writeJSON(w, http.StatusOK, body)
}

// handleAbandonPromotion cancels a staged promotion before the restart that would apply it.
func (s *Server) handleAbandonPromotion(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	if !s.opts.AllowWrites {
		writeError(w, http.StatusForbidden, "read_only",
			"this control plane is read-only; start the service with --control-write")
		return
	}
	root, err := s.defaultDataRoot()
	if err != nil {
		writeError(w, http.StatusBadRequest, "not_file_backed", err.Error())
		return
	}
	s.applyMu.Lock()
	had, err := recovery.AbandonPromotion(root)
	s.applyMu.Unlock()
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "abandon_failed", err.Error())
		return
	}
	if !had {
		writeJSON(w, http.StatusOK, map[string]any{
			"abandoned": false,
			"note":      "there was no promotion waiting for a restart",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"abandoned": true,
		"note": "the staged copy has been removed and the next startup will do nothing. The live " +
			"namespace was never touched.",
	})
}

// pendingPromotion is PendingPromotion over a root that may be empty (an in-memory namespace).
func (s *Server) pendingPromotion(root string) (*recovery.PromotionIntent, error) {
	if root == "" {
		return nil, nil
	}
	return recovery.PendingPromotion(root)
}

// defaultDataRoot is the data root the promotion endpoints work over. Promotion is per namespace
// but the intent lives at the root, and every namespace this process serves shares one.
func (s *Server) defaultDataRoot() (string, error) {
	src := s.namespaces()
	for _, ns := range src.Namespaces() {
		if isAttached(ns) {
			continue
		}
		rt, ok := src.Runtime(ns)
		if !ok {
			continue
		}
		if root := dataRootFor(rt); root != "" {
			return root, nil
		}
	}
	return "", fmt.Errorf("this server has no file-backed namespace, so there is nothing to promote into")
}

func principalName(p auth.Principal) string {
	if p.ID == "" {
		return "anonymous"
	}
	return p.ID
}

// dirSize totals a directory tree, for the "will this fit" question.
func dirSize(root string) (int64, error) {
	var total int64
	err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

// freeBytes reports the space available on the filesystem holding root, so "will the copy fit" is
// answered before the copy starts rather than by a half-finished one.
func freeBytes(root string) (int64, error) {
	shim, err := openShim(root)
	if err != nil {
		return 0, err
	}
	return shim.AvailableBytes()
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	value, units := float64(n), []string{"KiB", "MiB", "GiB", "TiB"}
	for _, u := range units {
		value /= unit
		if value < unit {
			return fmt.Sprintf("%.1f %s", value, u)
		}
	}
	return fmt.Sprintf("%.1f PiB", value)
}

// flush pushes the response out before the caller does something that stops the server answering.
func flush(w http.ResponseWriter) {
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// logf reports something no response can carry, because the response has already gone.
func (s *Server) logf(format string, args ...any) {
	slog.Warn(fmt.Sprintf(format, args...))
}

// promotedMarker decides what namespace marker (meta.json) a promoted copy should carry, and
// returns the note an operator needs about it.
//
// A restore rebuilds the delta log. It does not write the per-commit tree objects the "objects"
// history strategy resolves against, so a restored copy cannot serve history that way however the
// namespace it replaces was built - but it can always serve it by replay, which rebuilds a
// historical tree from the log. So the marker says replay, and the difference is reported rather
// than discovered.
//
// The history *mode* is carried across unchanged. It is a retention policy ("commits may be
// reclaimed"), not a claim about which commits are present, so a restored copy holding more history
// than the mode promises is not a contradiction.
func promotedMarker(liveRoot, namespaceID string) (marker []byte, note string, err error) {
	strategy, mode, found := embed.NamespaceMarker(liveRoot, namespaceID)
	if found && strategy != "" && strategy != "replay" {
		note = "the promoted copy will serve history by replay rather than by tree objects, " +
			"because a restore rebuilds the delta log and not the objects. Historical reads will " +
			"be slower on first touch; kdb-inspect migrate-history converts it back."
	}
	marker, err = embed.EncodeNamespaceMarker(namespaceID, "replay", mode)
	if err != nil {
		return nil, "", fmt.Errorf("could not build the namespace marker for the promoted copy: %w", err)
	}
	return marker, note, nil
}
