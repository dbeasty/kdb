package control

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/backup"
	"github.com/limidus/kdb/go/kdb/integrity"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// Recovery: verify the log, back it up, and say what to do about damage.
//
// The shape of this is decided by one fact - every entry point in the offline toolkit
// (kdb-inspect) takes the data directory's exclusive lock, and this process is holding it. So the
// work splits three ways, and the split is the design rather than a limitation to apologise for:
//
//   - What the server can do because it already holds the lock: verify, and back up. Neither needs
//     to acquire anything, and neither mutates the log.
//   - What can be done alongside the live directory: restore into a *different* directory, which
//     locks only that one. Not implemented here yet; see the plan's §8.3.
//   - What genuinely requires the process to be down: repair, migrate, restore in place. The
//     control plane does not pretend to do these. It shows the findings, generates the exact
//     command, and states the precondition.
//
// The last part matters more than it looks. An operator staring at a corrupt namespace needs to
// know which of the four classifications they have, because two of them are repairable and two are
// not, and that determines whether they reach for repair or for a backup.

// verifyReport is a cached verification result.
type verifyReport struct {
	Namespace string    `json:"namespace"`
	Level     string    `json:"level"`
	RanAt     time.Time `json:"ranAt"`
	TookMS    int64     `json:"tookMs"`
	Clean     bool      `json:"clean"`
	Segments  []segment `json:"segments"`
	Findings  []finding `json:"findings"`
	// ActiveSegment is the sequence the writer is still appending to, when one is known. A finding
	// on it is expected rather than alarming - see the note field.
	ActiveSegment *int64 `json:"activeSegment,omitempty"`
	Note          string `json:"note,omitempty"`
}

type segment struct {
	Sequence  int64 `json:"sequence"`
	SizeBytes int64 `json:"sizeBytes"`
	Frames    int   `json:"frames"`
}

type finding struct {
	Level          string `json:"level"`
	Classification string `json:"classification"`
	Segment        int64  `json:"segment"`
	Offset         int    `json:"offset"`
	Detail         string `json:"detail"`
	// Repairable says whether `kdb-inspect repair-segments` can act on this one. Repair only ever
	// acts on L1 findings; an L2 finding names a gap it cannot fabricate data to fill, and the
	// answer there is a restore.
	Repairable bool `json:"repairable"`
	// OnActiveSegment marks a finding on the segment still being written, which under a live
	// server is the expected shape of an in-progress append rather than damage.
	OnActiveSegment bool `json:"onActiveSegment"`
}

// recoveryState holds the cached reports, so the UI can show the last result without re-running a
// scan that costs a walk of the whole log.
type recoveryState struct {
	mu      sync.Mutex
	reports map[string]*verifyReport
	running map[string]bool
}

func newRecoveryState() *recoveryState {
	return &recoveryState{reports: map[string]*verifyReport{}, running: map[string]bool{}}
}

// dataRootFor is the directory a namespace's files live under, or "" for a memory runtime.
func dataRootFor(rt *serverRuntime) string {
	if rt == nil || rt.Runtime == nil {
		return ""
	}
	return rt.Runtime.DataRoot
}

// openShim builds a read-only view of the data directory.
//
// Unlocked on purpose: this process already holds the directory's exclusive lock, so taking it
// again would deadlock against itself (flock is per open file description). Nothing here writes,
// so sharing the files with the live writer is safe in the one direction that matters - the worst
// case is reading a segment mid-append, which the scanner already models as a torn tail.
func openShim(dataRoot string) (storage.PlatformIOShim, error) {
	root := dataRoot
	store, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		return nil, err
	}
	return storio.NewFileBackedPlatformIO(storio.PlatformIOConfig{RootDirectory: &root}, store), nil
}

// handleIntegrity returns the last verification, if one has been run.
func (s *Server) handleIntegrity(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	s.recovery.mu.Lock()
	report := s.recovery.reports[ns]
	running := s.recovery.running[ns]
	s.recovery.mu.Unlock()

	if report == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"namespace": ns, "everRun": false, "running": running,
			"note": "no verification has been run on this namespace since the server started. " +
				"Running one walks the whole delta log, so it is on demand rather than automatic.",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": ns, "everRun": true, "running": running, "report": report,
	})
}

// handleVerify runs a verification now.
//
// L1 is framing and CRC; L2 adds commit hashes and the parent closure, which means reading every
// commit rather than every frame header, and costs accordingly. Both are reads.
func (s *Server) handleVerify(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	var req struct {
		Level string `json:"level"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&req)
	}
	level := integrity.L2
	switch strings.ToUpper(strings.TrimSpace(req.Level)) {
	case "", "L2":
		level = integrity.L2
	case "L1":
		level = integrity.L1
	default:
		writeError(w, http.StatusBadRequest, "bad_request",
			`"level" must be "L1" (framing and CRC) or "L2" (adds commit hashes and the parent closure)`)
		return
	}

	root := dataRootFor(rt)
	if root == "" {
		writeError(w, http.StatusBadRequest, "not_file_backed",
			"this namespace is in memory, so there is no delta log to verify")
		return
	}

	// One verification per namespace at a time. Two concurrent full scans of the same log is
	// wasted I/O competing with the write path, not extra safety.
	s.recovery.mu.Lock()
	if s.recovery.running[ns] {
		s.recovery.mu.Unlock()
		writeError(w, http.StatusConflict, "already_running",
			"a verification of this namespace is already in progress")
		return
	}
	s.recovery.running[ns] = true
	s.recovery.mu.Unlock()
	defer func() {
		s.recovery.mu.Lock()
		s.recovery.running[ns] = false
		s.recovery.mu.Unlock()
	}()

	shim, err := openShim(root)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "shim_failed", err.Error())
		return
	}
	started := s.opts.Now()
	raw, err := integrity.Verify(shim, ns, integrity.Options{Level: level})
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "verify_failed", err.Error())
		return
	}
	report := presentVerify(ns, string(level), raw, started, s.opts.Now())

	s.recovery.mu.Lock()
	s.recovery.reports[ns] = report
	s.recovery.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{"namespace": ns, "everRun": true, "report": report})
}

// presentVerify maps a report into the wire shape, and marks the findings an operator should not
// worry about.
func presentVerify(ns, level string, raw *integrity.Report, started, finished time.Time) *verifyReport {
	out := &verifyReport{
		Namespace: ns,
		Level:     level,
		RanAt:     finished.UTC(),
		TookMS:    finished.Sub(started).Milliseconds(),
		Clean:     raw.Clean(),
		Segments:  make([]segment, 0, len(raw.Segments)),
		Findings:  make([]finding, 0, len(raw.Findings)),
	}
	var highest int64 = -1
	for _, sg := range raw.Segments {
		out.Segments = append(out.Segments, segment{
			Sequence: sg.Sequence, SizeBytes: sg.SizeBytes, Frames: sg.FrameCount,
		})
		if sg.Sequence > highest {
			highest = sg.Sequence
		}
	}
	if highest >= 0 {
		active := highest
		out.ActiveSegment = &active
	}
	for _, f := range raw.Findings {
		onActive := out.ActiveSegment != nil && f.Segment == *out.ActiveSegment
		out.Findings = append(out.Findings, finding{
			Level:           string(f.Level),
			Classification:  string(f.Classification),
			Segment:         f.Segment,
			Offset:          f.Offset,
			Detail:          f.Detail,
			Repairable:      f.Level == integrity.L1,
			OnActiveSegment: onActive,
		})
		if onActive {
			out.Note = "A finding on the segment the writer is still appending to is the expected " +
				"shape of an in-progress write under a live server, not damage: the scan reached " +
				"bytes that were not finished being written. Re-run after a clean shutdown to see " +
				"whether it persists."
		}
	}
	sort.Slice(out.Findings, func(i, j int) bool {
		if out.Findings[i].Segment != out.Findings[j].Segment {
			return out.Findings[i].Segment < out.Findings[j].Segment
		}
		return out.Findings[i].Offset < out.Findings[j].Offset
	})
	return out
}

// handleMaintenancePlan generates the offline command for a namespace, with its preconditions.
//
// This is the honest half of §8.4. Repair, migrate and in-place restore all take the data
// directory's exclusive lock, which this process holds, so the control plane cannot run them. What
// it can do is remove every other reason the operator would get it wrong: the right binary, the
// right flags, this namespace, this data root, and what the command will and will not do.
func (s *Server) handleMaintenancePlan(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	root := dataRootFor(rt)
	if root == "" {
		writeError(w, http.StatusBadRequest, "not_file_backed",
			"this namespace is in memory; there is nothing offline to do to it")
		return
	}

	s.recovery.mu.Lock()
	report := s.recovery.reports[ns]
	s.recovery.mu.Unlock()

	type operation struct {
		Name         string   `json:"name"`
		Command      []string `json:"command"`
		Why          string   `json:"why"`
		Precondition string   `json:"precondition"`
		Applicable   bool     `json:"applicable"`
		Reason       string   `json:"reason,omitempty"`
	}

	repairable, unrepairable := 0, 0
	if report != nil {
		for _, f := range report.Findings {
			if f.OnActiveSegment {
				continue
			}
			if f.Repairable {
				repairable++
			} else {
				unrepairable++
			}
		}
	}

	ops := []operation{
		{
			Name:    "repair-segments",
			Command: []string{"kdb-inspect", "repair-segments", "--data-dir", root, "--namespace", ns},
			Why: "Truncates a torn tail, or rewrites a segment back to its last CRC-verified frame. " +
				"Bytes are copied to a quarantine file before anything is changed, so the operation " +
				"is recoverable. It only ever acts on L1 findings: an L2 finding names a gap it " +
				"cannot fabricate data to fill.",
			Precondition: "The service must not be running: this takes the data directory's " +
				"exclusive lock, and so does the server.",
			Applicable: repairable > 0,
			Reason:     applicabilityReason(report, repairable, "no repairable (L1) findings"),
		},
		{
			Name: "restore",
			Command: []string{"kdb-inspect", "restore", "--namespace", ns,
				"--source", "damaged=" + root, "--out", "<a new directory>"},
			Why: "Rebuilds the namespace as a verified union of whatever sources are available, " +
				"applied in commit order. A commit whose parent is in no source is reported rather " +
				"than applied out of order. List a backup alongside the damaged directory to " +
				"salvage both.",
			Precondition: "Writes to --out, which must be a different directory, and locks only " +
				"that one - so this can run while the service is up. Promoting the result is what " +
				"needs the service stopped.",
			Applicable: unrepairable > 0,
			Reason:     applicabilityReason(report, unrepairable, "no findings that repair cannot fix"),
		},
		{
			Name: "migrate-history",
			Command: []string{"kdb-inspect", "migrate-history", "--data-dir", root,
				"--namespace", ns, "--history-strategy", "<objects|replay>"},
			Why: "Changes how this namespace serves historical reads. The strategy and mode are " +
				"refused at open when they disagree with what is on disk, which is why this is a " +
				"migration rather than a setting.",
			Precondition: "The service must not be running.",
			Applicable:   false,
			Reason:       "only needed when deliberately changing the history strategy or mode",
		},
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"namespace":  ns,
		"dataRoot":   root,
		"operations": ops,
		"basedOn": func() any {
			if report == nil {
				return nil
			}
			return map[string]any{"ranAt": report.RanAt, "level": report.Level, "clean": report.Clean}
		}(),
		"note": "These are commands to run, not actions this control plane will take. Every one of " +
			"them needs the data directory's exclusive lock, which this process is holding, so a " +
			"server that offered to run them would be offering something it cannot do.",
	})
}

func applicabilityReason(report *verifyReport, count int, none string) string {
	if report == nil {
		return "no verification has been run, so it is not known whether this is needed"
	}
	if count == 0 {
		return none
	}
	return ""
}

// --- backups -------------------------------------------------------------------------------

// backupStore is where backups go. Directory-backed only for now: the S3 sink is a byte copier
// rather than a restorable backup target (kdb-finish-up-plan, deployment gap 8), so offering it
// here would be offering something that cannot be restored from.
func (s *Server) backupStore() (backup.ObjectStore, string, error) {
	if s.opts.BackupDir == "" {
		return nil, "", fmt.Errorf("this server has no backup directory configured; " +
			"start it with --control-backup-dir to enable backups through the control plane")
	}
	if err := os.MkdirAll(s.opts.BackupDir, 0o755); err != nil {
		return nil, "", err
	}
	return &backup.DirStore{Root: s.opts.BackupDir}, s.opts.BackupDir, nil
}

func (s *Server) handleListBackups(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	store, dir, err := s.backupStore()
	if err != nil {
		writeError(w, http.StatusNotImplemented, "backups_unconfigured", err.Error())
		return
	}
	ids, err := backup.ListBackups(store, ns)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "list_failed", err.Error())
		return
	}
	type entry struct {
		BackupID     string `json:"backupId"`
		CreatedAt    string `json:"createdAt"`
		CommitCount  int    `json:"commitCount"`
		Segments     int    `json:"segments"`
		Incremental  bool   `json:"incremental"`
		BaseBackupID string `json:"baseBackupId,omitempty"`
		SizeBytes    int64  `json:"sizeBytes"`
	}
	out := make([]entry, 0, len(ids))
	for _, id := range ids {
		m, err := backup.LoadManifest(store, ns, id)
		if err != nil {
			// A backup whose manifest will not load is listed as unreadable rather than omitted:
			// an operator counting on it needs to know it is there and broken.
			out = append(out, entry{BackupID: id, CreatedAt: "unreadable"})
			continue
		}
		e := entry{
			BackupID: m.BackupID, CreatedAt: m.CreatedAt, CommitCount: m.CommitCount,
			Segments: len(m.Segments),
		}
		if m.BaseBackupID != nil {
			e.Incremental, e.BaseBackupID = true, *m.BaseBackupID
		}
		for _, sg := range m.Segments {
			e.SizeBytes += sg.SizeBytes
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt > out[j].CreatedAt })
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": ns, "directory": dir, "backups": out,
	})
}

// handleCreateBackup backs the namespace up from the live directory.
//
// Safe while serving because backup.Create already handles a segment that is still being appended
// to: it stores that one's CRC-verified prefix and records it as such in the manifest. So an online
// backup is the algorithm that was already there rather than a weaker version of an offline one.
func (s *Server) handleCreateBackup(w http.ResponseWriter, r *http.Request, principal auth.Principal, ns string, rt *serverRuntime) {
	var req struct {
		BaseBackupID string `json:"baseBackupId"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&req)
	}
	store, dir, err := s.backupStore()
	if err != nil {
		writeError(w, http.StatusNotImplemented, "backups_unconfigured", err.Error())
		return
	}
	root := dataRootFor(rt)
	if root == "" {
		writeError(w, http.StatusBadRequest, "not_file_backed",
			"this namespace is in memory; there is nothing on disk to back up")
		return
	}
	shim, err := openShim(root)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "shim_failed", err.Error())
		return
	}
	started := s.opts.Now()
	m, err := backup.Create(shim, ns, store, req.BaseBackupID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "backup_failed", err.Error())
		return
	}
	var size int64
	prefixes := 0
	for _, sg := range m.Segments {
		size += sg.SizeBytes
		if sg.VerifiedPrefix {
			prefixes++
		}
	}
	body := map[string]any{
		"namespace": ns, "directory": dir, "backupId": m.BackupID,
		"createdAt": m.CreatedAt, "commitCount": m.CommitCount,
		"segments": len(m.Segments), "sizeBytes": size,
		"tookMs": s.opts.Now().Sub(started).Milliseconds(),
	}
	if m.BaseBackupID != nil {
		body["baseBackupId"] = *m.BaseBackupID
		body["incremental"] = true
	}
	if prefixes > 0 {
		body["verifiedPrefixSegments"] = prefixes
		body["note"] = "This was taken from a live namespace, so the segment still being written " +
			"was stored as its CRC-verified prefix. Everything acknowledged before the backup " +
			"started is in it; a write that landed mid-backup may not be."
	}
	writeJSON(w, http.StatusOK, body)
}

// handleVerifyBackup checks a backup against its manifest.
func (s *Server) handleVerifyBackup(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	store, _, err := s.backupStore()
	if err != nil {
		writeError(w, http.StatusNotImplemented, "backups_unconfigured", err.Error())
		return
	}
	id := r.PathValue("id")
	res, err := backup.Verify(store, ns, id)
	if err != nil {
		writeError(w, http.StatusNotFound, "verify_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": ns, "backupId": id, "clean": res.Clean(),
		"objects": res.Objects, "problems": res.Problems,
		"note": "An unverified backup is a guess. This re-reads every object the manifest names " +
			"and checks it against the hash recorded for it.",
	})
}

// handleCheckpoints reports what the next open would have to replay.
//
// The number that predicts restart time, and the visible consequence of turning checkpoints off.
func (s *Server) handleCheckpoints(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	root := dataRootFor(rt)
	if root == "" {
		writeJSON(w, http.StatusOK, map[string]any{
			"namespace": ns, "fileBacked": false,
			"note": "an in-memory namespace has nothing to replay and nothing to checkpoint",
		})
		return
	}
	shim, err := openShim(root)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "shim_failed", err.Error())
		return
	}
	seqs, err := integrity.ListSequencedSegments(shim, ns)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "list_failed", err.Error())
		return
	}
	var total int64
	for _, seq := range seqs {
		if info, err := os.Stat(segmentPath(root, ns, seq)); err == nil {
			total += info.Size()
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": ns, "fileBacked": true,
		"segments": len(seqs), "deltaLogBytes": total,
		"checkpointPresent": checkpointExists(root, ns),
		"note": "A checkpoint lets the next open skip the delta log. Without one - or with one the " +
			"log no longer matches - every open replays the whole log, and these numbers are what " +
			"that costs.",
	})
}

func segmentPath(root, ns string, seq int64) string {
	return filepath.Join(root, "ns", filepath.FromSlash(ns), "delta", storio.DeltaSequencedFileName(seq))
}

// checkpointExists reports whether a checkpoint file is on disk for this namespace. Best-effort and
// deliberately shallow: whether it is *usable* is a question only the open path can answer, and it
// answers it by falling back to a full replay.
func checkpointExists(root, ns string) bool {
	var found bool
	_ = filepath.Walk(filepath.Join(root, "snap"), func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if strings.Contains(p, "checkpoint") && strings.HasSuffix(p, filepath.Base(ns)) {
			found = true
		}
		return nil
	})
	return found
}

var _ = context.Background
