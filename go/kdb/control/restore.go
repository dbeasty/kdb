package control

import (
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
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/recovery"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Restore into a staging directory, then browse the result before trusting it.
//
// This is the piece of §8 worth having. Restoring *in place* needs the service stopped, because it
// rewrites the directory the server has locked. Restoring *elsewhere* does not: `restore --out`
// locks only its output directory. So a running server can rebuild a namespace into a scratch
// directory while it keeps serving, and then - because a read-only open takes a *shared* lock -
// attach that copy alongside the live one and let the operator look at it with the same commit
// graph, document browser and SQL console they use on production.
//
// That turns "restore" from an act of faith into an inspection. The question an operator actually
// has is not "can I restore" but "is this backup any good", and the only honest way to answer it
// is to open the thing and look.
//
// Promotion is deliberately absent. Swapping the live data directory for a staged one means
// stopping the process, and a button that implied otherwise would be lying.

// restoreJob is one staging restore, past or present.
type restoreJob struct {
	ID        string     `json:"id"`
	Namespace string     `json:"namespace"`
	State     string     `json:"state"`
	StartedAt time.Time  `json:"startedAt"`
	EndedAt   *time.Time `json:"endedAt,omitempty"`
	Dir       string     `json:"directory"`
	// Sources are the labels that contributed at least one commit, which is how an operator learns
	// whether the backup carried the restore or the damaged log did.
	Sources []string `json:"sourcesUsed,omitempty"`
	Applied int      `json:"appliedCommits"`
	// Missing are commits whose parent was in no source. Reported rather than applied out of
	// order: a partial restore that looked complete would be the worst possible outcome here.
	Missing  []string `json:"missingHashes,omitempty"`
	Error    string   `json:"error,omitempty"`
	Attached string   `json:"attachedAs,omitempty"`
	Note     string   `json:"note,omitempty"`
}

type restoreState struct {
	mu   sync.Mutex
	jobs map[string]*restoreJob
	// attached maps a pseudo-namespace id to the read-only runtime serving a staged copy.
	attached map[string]*serverRuntime
	// closers releases an attached runtime on detach or shutdown.
	closers map[string]func()
}

func newRestoreState() *restoreState {
	return &restoreState{
		jobs:     map[string]*restoreJob{},
		attached: map[string]*serverRuntime{},
		closers:  map[string]func(){},
	}
}

// attachedNamespaces is the overlay the server's namespace resolution consults alongside its
// configured source, so a staged copy is reachable by every read endpoint without the service's
// own wiring having to know about it.
func (s *Server) attachedNamespaces() map[string]*serverRuntime {
	s.restore.mu.Lock()
	defer s.restore.mu.Unlock()
	out := make(map[string]*serverRuntime, len(s.restore.attached))
	for id, rt := range s.restore.attached {
		out[id] = rt
	}
	return out
}

// stagingRoot is where staged restores are written.
func (s *Server) stagingRoot() (string, error) {
	if s.opts.StagingDir == "" {
		return "", fmt.Errorf("this server has no staging directory configured; start it with " +
			"--control-staging-dir to restore a backup for inspection")
	}
	return s.opts.StagingDir, nil
}

type startRestoreRequest struct {
	// BackupID restores from a backup in the configured backup directory.
	BackupID string `json:"backupId"`
	// IncludeLive adds the live data directory as a second source, so a damaged log and a backup
	// can be salvaged together - which is what "hybrid" restore means and the reason
	// HybridRestore takes a list rather than one source.
	IncludeLive bool `json:"includeLive"`
}

// handleStartRestore restores into a fresh staging directory.
func (s *Server) handleStartRestore(w http.ResponseWriter, r *http.Request, principal auth.Principal, ns string, rt *serverRuntime) {
	var req startRestoreRequest
	if r.Body != nil {
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&req)
	}
	root, err := s.stagingRoot()
	if err != nil {
		writeError(w, http.StatusNotImplemented, "staging_unconfigured", err.Error())
		return
	}
	if req.BackupID == "" && !req.IncludeLive {
		writeError(w, http.StatusBadRequest, "bad_request",
			`give "backupId", or "includeLive" to rebuild from the live log alone, or both to `+
				`salvage a damaged log against a backup`)
		return
	}
	store, _, storeErr := s.backupStore()
	if req.BackupID != "" && storeErr != nil {
		writeError(w, http.StatusNotImplemented, "backups_unconfigured", storeErr.Error())
		return
	}
	liveRoot := dataRootFor(rt)
	if req.IncludeLive && liveRoot == "" {
		writeError(w, http.StatusBadRequest, "not_file_backed",
			"this namespace is in memory, so there is no log to include as a source")
		return
	}

	jobID := fmt.Sprintf("%d", s.opts.Now().UnixNano())
	dir := filepath.Join(root, jobID)
	job := &restoreJob{
		ID: jobID, Namespace: ns, State: "running",
		StartedAt: s.opts.Now().UTC(), Dir: dir,
	}
	s.restore.mu.Lock()
	s.restore.jobs[jobID] = job
	s.restore.mu.Unlock()

	// Asynchronous: a restore reads and rewrites every segment, so it is minutes on a real
	// namespace and must not hold an HTTP request open for it.
	go s.runRestore(job, ns, req, store, liveRoot)

	writeJSON(w, http.StatusAccepted, map[string]any{
		"job": job,
		"note": "restoring into a staging directory; this does not touch the live namespace. Poll " +
			"the job for its result, then attach it to look at what came back before trusting it.",
	})
}

func (s *Server) runRestore(job *restoreJob, ns string, req startRestoreRequest, store backup.ObjectStore, liveRoot string) {
	fail := func(err error) {
		s.restore.mu.Lock()
		defer s.restore.mu.Unlock()
		now := s.opts.Now().UTC()
		job.State, job.Error, job.EndedAt = "failed", err.Error(), &now
	}

	if err := os.MkdirAll(job.Dir, 0o755); err != nil {
		fail(err)
		return
	}

	var sources []recovery.Source
	// The backup first, fetched and manifest-verified into its own directory. FetchToDir re-hashes
	// every object against the manifest, so a corrupt backup fails here rather than contributing
	// bad frames to the union.
	if req.BackupID != "" {
		fetched := filepath.Join(job.Dir, "_backup")
		if err := os.MkdirAll(fetched, 0o755); err != nil {
			fail(err)
			return
		}
		if _, err := backup.FetchToDir(store, ns, req.BackupID, fetched); err != nil {
			fail(fmt.Errorf("fetching backup %s: %w", req.BackupID, err))
			return
		}
		shim, err := openShim(fetched)
		if err != nil {
			fail(err)
			return
		}
		sources = append(sources, recovery.Source{Label: "backup:" + req.BackupID, Shim: shim})
	}
	if req.IncludeLive {
		shim, err := openShim(liveRoot)
		if err != nil {
			fail(err)
			return
		}
		sources = append(sources, recovery.Source{Label: "live", Shim: shim})
	}

	out := filepath.Join(job.Dir, "restored")
	if err := os.MkdirAll(out, 0o755); err != nil {
		fail(err)
		return
	}
	// Lock the output directory for the restore, exactly as kdb-inspect does. It is a fresh
	// directory nothing else has, so this cannot contend with the live namespace's own lock.
	release, err := embed.LockDataDir(out)
	if err != nil {
		fail(fmt.Errorf("locking the staging directory: %w", err))
		return
	}
	outShim, err := openShim(out)
	if err != nil {
		release()
		fail(err)
		return
	}
	result, err := recovery.HybridRestore(sources, ns, storage.CompressionZSTD, outShim)
	release()
	if err != nil {
		fail(err)
		return
	}

	s.restore.mu.Lock()
	defer s.restore.mu.Unlock()
	now := s.opts.Now().UTC()
	job.State, job.EndedAt = "complete", &now
	job.Sources, job.Applied, job.Missing = result.SourcesUsed, result.AppliedCount, result.MissingHashes
	if len(result.MissingHashes) > 0 {
		job.Note = fmt.Sprintf("%d commit(s) could not be applied because their parent was in no "+
			"source. What restored is consistent as far as it goes, but it is not the whole history: "+
			"add another source, or accept the shortfall knowingly.", len(result.MissingHashes))
	}
}

// snapshotJob copies one job's state under the lock, or reports that there is no such job.
//
// The copy is the point. A restore runs on its own goroutine and writes these fields as it
// progresses, so handing a reader the *pointer* - which is what this used to do - races the writer
// and, before the race detector ever sees it, can serialize a torn job: "complete" beside an
// applied-count that had not been written yet. Every reader takes a copy instead.
func (s *Server) snapshotJob(id string) (restoreJob, bool) {
	s.restore.mu.Lock()
	defer s.restore.mu.Unlock()
	job, ok := s.restore.jobs[id]
	if !ok || job == nil {
		return restoreJob{}, false
	}
	return copyJob(job), true
}

// snapshotJobs copies every job, newest first.
func (s *Server) snapshotJobs() []restoreJob {
	s.restore.mu.Lock()
	out := make([]restoreJob, 0, len(s.restore.jobs))
	for _, j := range s.restore.jobs {
		out = append(out, copyJob(j))
	}
	s.restore.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	return out
}

// copyJob deep-copies the slices as well as the struct: the writer replaces them wholesale, so a
// shared backing array would outlive the lock that made reading it safe.
func copyJob(j *restoreJob) restoreJob {
	out := *j
	out.Sources = append([]string(nil), j.Sources...)
	out.Missing = append([]string(nil), j.Missing...)
	if j.EndedAt != nil {
		ended := *j.EndedAt
		out.EndedAt = &ended
	}
	return out
}

func (s *Server) handleRestoreJob(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	job, ok := s.snapshotJob(r.PathValue("jobId"))
	if !ok {
		writeError(w, http.StatusNotFound, "not_found",
			"no restore job called "+r.PathValue("jobId"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job})
}

func (s *Server) handleListRestoreJobs(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	jobs := s.snapshotJobs()
	writeJSON(w, http.StatusOK, map[string]any{
		"jobs": jobs,
		"note": "a staged restore is a copy in a scratch directory. Attaching one makes it " +
			"browsable read-only alongside the live namespace; promoting it needs the service stopped.",
	})
}

// handleAttachRestore opens a completed staged restore read-only and exposes it as a namespace.
//
// Read-only is not a policy applied on top; it is how the runtime is opened. OpenReadOnlyFileRuntime
// takes a *shared* directory lock and creates neither a WAL nor a delta writer, so every write path
// on it returns ErrReadOnly rather than failing somewhere deeper. That is what makes it safe to
// have a second view of a database open in the same process.
func (s *Server) handleAttachRestore(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	id := r.PathValue("jobId")
	job, ok := s.snapshotJob(id)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no restore job called "+id)
		return
	}
	already := job.Attached
	if job.State != "complete" {
		writeError(w, http.StatusConflict, "not_complete",
			"this restore is "+job.State+"; there is nothing to attach yet")
		return
	}
	if already != "" {
		writeJSON(w, http.StatusOK, map[string]any{
			"attachedAs": already, "job": job, "note": "already attached",
		})
		return
	}

	out := filepath.Join(job.Dir, "restored")
	rt, err := embed.OpenReadOnlyFileRuntime(out, embed.CatalogFromNamespace(job.Namespace), job.Namespace, schema.None())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "attach_failed",
			"could not open the staged copy read-only: "+err.Error())
		return
	}
	srt := server.NewKdbServerRuntime(rt)
	// The attached id is deliberately not the real namespace's: two views of the same namespace
	// have to be distinguishable everywhere, or an operator reads the staged copy believing it is
	// production.
	alias := "staged/" + id + "/" + job.Namespace

	s.restore.mu.Lock()
	s.restore.attached[alias] = srt
	s.restore.closers[alias] = func() { rt.Close() }
	if live := s.restore.jobs[id]; live != nil {
		live.Attached = alias
	}
	s.restore.mu.Unlock()
	job, _ = s.snapshotJob(id)

	writeJSON(w, http.StatusOK, map[string]any{
		"attachedAs": alias,
		"job":        job,
		"note": "attached read-only. It appears in the namespace list and every read view works on " +
			"it - commit log, diffs, documents, SQL - so the restored data can be checked with the " +
			"same tools used on the live namespace. Writes to it are refused by the runtime itself.",
	})
}

// handleDetachRestore closes an attached copy, and optionally deletes the staging directory.
func (s *Server) handleDetachRestore(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	id := r.PathValue("jobId")
	var req struct {
		Delete bool `json:"delete"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&req)
	}

	s.restore.mu.Lock()
	job := s.restore.jobs[id]
	if job == nil {
		s.restore.mu.Unlock()
		writeError(w, http.StatusNotFound, "not_found", "no restore job called "+id)
		return
	}
	if alias := job.Attached; alias != "" {
		if closer, ok := s.restore.closers[alias]; ok {
			closer()
		}
		delete(s.restore.attached, alias)
		delete(s.restore.closers, alias)
		job.Attached = ""
	}
	dir := job.Dir
	if req.Delete {
		delete(s.restore.jobs, id)
	}
	s.restore.mu.Unlock()

	body := map[string]any{"detached": true, "jobId": id}
	if req.Delete {
		// Removing a directory is the one destructive thing here, and it only ever removes a
		// staging directory this server created for this job - never the live data root.
		if err := os.RemoveAll(dir); err != nil {
			body["deleteError"] = err.Error()
		} else {
			body["deleted"] = dir
		}
	}
	writeJSON(w, http.StatusOK, body)
}

// closeAttached releases every attached copy. Called on shutdown so a staged restore does not
// outlive the process holding its shared lock.
func (s *Server) closeAttached() {
	s.restore.mu.Lock()
	defer s.restore.mu.Unlock()
	for alias, closer := range s.restore.closers {
		closer()
		delete(s.restore.attached, alias)
		delete(s.restore.closers, alias)
	}
}

// isAttached reports whether a namespace id names a staged copy rather than a live namespace.
func isAttached(ns string) bool { return strings.HasPrefix(ns, "staged/") }

// refuseIfStaged rejects a mutating request aimed at a staged copy, and reports whether it did.
//
// The runtime would refuse it anyway - a read-only open creates no WAL and no delta writer - but it
// surfaces from down there as "runtime is open read-only" with no hint of which of the two
// namespaces on screen the caller was actually addressing. Refusing here says that.
func refuseIfStaged(w http.ResponseWriter, ns string) bool {
	if !isAttached(ns) {
		return false
	}
	writeError(w, http.StatusForbidden, "staged_copy",
		"this is a staged restore attached for inspection, not a live namespace. It is open "+
			"read-only and cannot be written to; promoting it needs the service stopped.")
	return true
}
