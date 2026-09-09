package control

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
)

// Creating and deleting refs, and comparing two of them.
//
// The DAG has had CreateBranch, DeleteBranch, CreateTag and DeleteTag all along; nothing exposed
// them, so §9 screen 5 was a pair of read-only lists. What these add is the part that is not a
// method call:
//
//   - A durability caveat that has to be said out loud. A commit is in the delta log; a *ref* is
//     not. Branches and tags live in the namespace's checkpoint, which is written at close and
//     after a full replay - so a ref created here survives a clean shutdown, and a kill -9 before
//     the next checkpoint loses it. That is a property of the storage format, not something this
//     endpoint can fix, and an operator tagging a release needs to know which of the two they are
//     relying on. Every mutation says so in its response, and the UI repeats it.
//   - Refusing what would quietly do nothing or quietly do harm: a tag over a different commit, a
//     branch name that is already taken, deleting the default branch, deleting the branch being
//     read at.
//
// Compare is the git-viewer question the commit log cannot answer - "what is different between
// these two points" - and is a read, so it needs no write permission.

type createBranchRequest struct {
	Name string `json:"name"`
	// From is any revision spec: a hash, head~3, tag:v1, a branch name. Empty means the current
	// head, which is what "branch here" means.
	From string `json:"from"`
}

type createTagRequest struct {
	Name    string `json:"name"`
	At      string `json:"at"`
	Message string `json:"message"`
}

// refDurabilityNote is the caveat, in one place so every answer says the same thing.
const refDurabilityNote = "a ref is not a commit: branches and tags are recorded in this " +
	"namespace's checkpoint, which is written on a clean shutdown and after a full replay. This " +
	"one survives a normal restart; a kill -9 before the next checkpoint would lose it, while the " +
	"commit it names is in the delta log and cannot be lost."

func (s *Server) handleCreateBranch(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	if refuseIfStaged(w, ns) {
		return
	}
	var req createBranchRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "could not read request body: "+err.Error())
		return
	}
	name, err := validRefName(req.Name, "branch")
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	d, err := s.commitDAGFor(rt)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "no_dag", err.Error())
		return
	}
	from, err := s.resolveRevisionFor(rt, req.From)
	if err != nil {
		s.writeRevisionError(w, err)
		return
	}

	branch, err := d.CreateBranch(name, from)
	if err != nil {
		// "branch exists" is the caller's answer to fix, not a server fault; anything else is the
		// graph refusing, most likely a commit it does not hold.
		status := http.StatusServiceUnavailable
		if strings.Contains(err.Error(), "exists") {
			status = http.StatusConflict
		}
		writeError(w, status, "create_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"namespace": ns,
		"branch": map[string]any{
			"name": branch.Name, "head": branch.HeadHash.Hex(),
			"shortHead": shortHash(branch.HeadHash.Hex()),
		},
		"note": refDurabilityNote,
	})
}

func (s *Server) handleDeleteBranch(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	if refuseIfStaged(w, ns) {
		return
	}
	name := r.PathValue("name")
	d, err := s.commitDAGFor(rt)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "no_dag", err.Error())
		return
	}
	if err := d.DeleteBranch(name); err != nil {
		// The DAG refuses the default branch by name, which is the one deletion that would leave
		// the namespace without a head. Reported as a 409 rather than a 404 so it does not read as
		// "no such branch".
		status := http.StatusNotFound
		if strings.Contains(err.Error(), "default branch") {
			status = http.StatusConflict
		}
		writeError(w, status, "delete_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": ns, "deleted": name,
		"note": "the branch is gone; every commit it pointed at is still in the log and still " +
			"reachable by hash. " + refDurabilityNote,
	})
}

func (s *Server) handleCreateTag(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	if refuseIfStaged(w, ns) {
		return
	}
	var req createTagRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "could not read request body: "+err.Error())
		return
	}
	name, err := validRefName(req.Name, "tag")
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	d, err := s.commitDAGFor(rt)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "no_dag", err.Error())
		return
	}
	at, err := s.resolveRevisionFor(rt, req.At)
	if err != nil {
		s.writeRevisionError(w, err)
		return
	}

	tag, err := d.CreateTag(name, at, req.Message)
	if err != nil {
		status := http.StatusServiceUnavailable
		if strings.Contains(err.Error(), "already names") {
			// Re-tagging is refused rather than moved. A tag that quietly moved would make every
			// reference to it - including a retention decision already taken on it - mean
			// something else.
			status = http.StatusConflict
		}
		writeError(w, status, "create_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"namespace": ns,
		"tag": map[string]any{
			"name": tag.Name, "head": tag.CommitHash.Hex(),
			"shortHead": shortHash(tag.CommitHash.Hex()), "message": tag.Message,
		},
		"note": "this tag is also a retention root: compaction will not squash the commit it " +
			"names while it exists. " + refDurabilityNote,
	})
}

func (s *Server) handleDeleteTag(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	if refuseIfStaged(w, ns) {
		return
	}
	name := r.PathValue("name")
	d, err := s.commitDAGFor(rt)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "no_dag", err.Error())
		return
	}
	if !d.DeleteTag(name) {
		writeError(w, http.StatusNotFound, "not_found", "no tag called "+name)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": ns, "deleted": name,
		"note": "the tag is gone, and with it the retention root it was: the commit it named is " +
			"now squashable by compaction unless something else retains it. " + refDurabilityNote,
	})
}

// handleCompare diffs any two revisions.
//
// The commit log answers "what changed at this commit". This answers "what is different between
// these two points", which is the question a branch or a tag is usually asked - and the one the
// graph cannot answer however well it is drawn.
func (s *Server) handleCompare(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	fromSpec := r.URL.Query().Get("from")
	toSpec := r.URL.Query().Get("to")
	if fromSpec == "" || toSpec == "" {
		writeError(w, http.StatusBadRequest, "bad_request",
			"both ?from= and ?to= are required; each is any revision spec - a hash, a branch, "+
				"tag:v1, or head~3")
		return
	}

	from, to, entries, err := s.diffRevisions(rt, fromSpec, toSpec)
	if err != nil {
		if isUnknownRevision(err) {
			s.writeRevisionError(w, err)
			return
		}
		writeError(w, http.StatusServiceUnavailable, "diff_unavailable",
			"this comparison could not be computed: "+err.Error())
		return
	}

	body := map[string]any{
		"namespace": ns,
		"from":      from.Hex(), "to": to.Hex(),
		"fromSpec": fromSpec, "toSpec": toSpec,
		"entries": entries,
		"counts":  countChanges(entries),
	}
	// How the two points are related, which changes what the diff means: "behind" is a
	// fast-forward, "diverged" is a merge waiting to happen. Best-effort - a walk that cannot
	// reach far enough says nothing rather than guessing.
	if rel := s.relate(rt, from, to); rel != "" {
		body["relationship"] = rel
	}
	writeJSON(w, http.StatusOK, body)
}

// relate reports how the two points sit relative to each other: "same", "ahead" (to's history
// contains from, so the comparison is a fast-forward), "behind", or "diverged".
//
// It changes what the diff means. The same set of entries reads as "these changes are waiting to be
// merged" between diverged branches and as "this is simply older" against an ancestor, and a
// comparison that did not say which would leave the reader to work it out from the graph.
//
// InMemoryCommitDag.IsAncestor does the work, and it prunes by generation number where the graph
// has them - it "visits only what it has to and allocates a visited set proportional to that, not
// to history" - so this costs nothing on a long history that has not diverged.
func (s *Server) relate(rt *serverRuntime, from, to codec.Hash) string {
	if from == to {
		return "same"
	}
	d, err := s.commitDAGFor(rt)
	if err != nil {
		return ""
	}
	fromIsAncestor := d.IsAncestor(from, to)
	toIsAncestor := d.IsAncestor(to, from)
	switch {
	case fromIsAncestor && toIsAncestor:
		return "same"
	case fromIsAncestor:
		return "ahead"
	case toIsAncestor:
		return "behind"
	default:
		return "diverged"
	}
}

// validRefName keeps a name usable in a URL path and in a revision spec.
//
// The DAG only refuses an empty tag name, so the rest of this is here: a ref called "head~2", or
// one with a slash in it, would be a name that means something else everywhere it is read back.
func validRefName(name, kind string) (string, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "", fmt.Errorf("a %s needs a name", kind)
	}
	if len(trimmed) > 200 {
		return "", fmt.Errorf("that %s name is too long", kind)
	}
	for _, bad := range []string{"/", "\\", " ", "~", "^", ":", "?", "*", "[", "]", "@{", ".."} {
		if strings.Contains(trimmed, bad) {
			return "", fmt.Errorf("a %s name cannot contain %q: it would collide with the "+
				"revision syntax (head~3, tag:v1) or with a URL path", kind, bad)
		}
	}
	if strings.HasPrefix(trimmed, "-") || strings.HasPrefix(trimmed, ".") {
		return "", fmt.Errorf("a %s name cannot start with %q", kind, trimmed[:1])
	}
	// A 64-hex name would be indistinguishable from a commit hash wherever a revision is parsed.
	if _, err := codec.HashFromHex(trimmed); err == nil {
		return "", errors.New("that name is a commit hash, which every revision spec would read " +
			"as the commit rather than as this ref")
	}
	return trimmed, nil
}
