package control

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
)

// Restore points: one name, every namespace.
//
// A commit belongs to one namespace, and so does a revert. That is a fact
// about the engine, not an omission — one runtime is one DAG — but it makes a
// per-namespace restore almost useless to anyone whose data spans several. Put
// a game's board back without its scoreboard and the database disagrees with
// itself: a finished match the board says is still in progress.
//
// So a restore point is one *name*, tagged in every namespace at one instant.
// Nothing new is stored. A tag is already an immutable name for a commit and
// already a retention root, `tag:NAME` already resolves in every namespace, and
// embed.RevertTo already restores every document in a namespace atomically. All
// that was missing was the instant, and the loop.
//
// What this does not promise, and says so everywhere a person can see it: the
// restore is *not atomic across namespaces*. It is N atomic reverts behind an
// all-or-nothing plan gate, with an undo point taken first. Because a revert
// goes forward — a new commit whose tree is the old one's — a half-finished
// restore is recoverable by restoring the undo point. That is the honest
// ceiling without a cross-namespace transaction, and pretending otherwise
// would be the one failure an operator could not reason about.

// restorePointWalkStart and restorePointWalkCap bound the walk that places a
// point in its namespace's history. Vars rather than consts so a test can
// shrink the window instead of writing fifty thousand commits.
var (
	restorePointWalkStart = 1_000
	restorePointWalkCap   = 50_000
)

// captureAttempts is how many times a capture re-reads every head looking for
// two consecutive agreeing reads. See handleCreateRestorePoint.
const captureAttempts = 5

// restorePointNamespace is where one point sits in one namespace.
type restorePointNamespace struct {
	Namespace   string `json:"namespace"`
	Commit      string `json:"commit"`
	ShortCommit string `json:"shortCommit"`
	// Depth is how many commits back from this namespace's head the point
	// sits, in the same walk the commit log shows; 0 is head itself. -1 when
	// it could not be placed, and then exactly one of the flags below says why.
	Depth int `json:"depth"`
	// Unreachable: not an ancestor of head. An abandoned branch, or behind an
	// archived commit the walk cannot see past. A restore cannot use it.
	Unreachable bool `json:"unreachable,omitempty"`
	// BeyondWindow: reachable, but deeper than the walk went.
	BeyondWindow bool `json:"beyondWindow,omitempty"`
	// Stub: archived in place. The name resolves; its documents do not.
	Stub bool `json:"stub,omitempty"`
}

// restorePoint is one name across every namespace that has it.
type restorePoint struct {
	Name      string `json:"name"`
	CreatedAt string `json:"createdAt,omitempty"`
	Message   string `json:"message,omitempty"`
	// Complete is set when every namespace this control plane serves has this
	// name. An incomplete point cannot put the database back to one instant,
	// which is the only thing it was for.
	Complete bool     `json:"complete"`
	Missing  []string `json:"missing,omitempty"`
	// Restorable is Complete and every member placed and not stubbed — the
	// single flag a UI needs before offering the button.
	Restorable bool `json:"restorable"`
	// HandMade marks a point whose member tags were not created together, so
	// the timestamp this list is ordered by is not one instant. Somebody made
	// these by hand, in several calls, and the order is a guess.
	HandMade bool                    `json:"handMade,omitempty"`
	In       []restorePointNamespace `json:"in"`

	// createdMillis is CreatedAt as an instant, which is what the list is sorted by. Never the
	// string: RFC3339Nano drops trailing zeros, so "…49.34Z" (340ms) sorts after "…49.341Z"
	// (341ms) as text, and captures a few milliseconds apart came back in the wrong order.
	createdMillis int64
}

// handMadeSkew is how far apart member tags may be created and still be taken
// as one capture. Generous: a capture writes N tags in a loop, and a busy host
// can stretch that, while a person making them by hand takes seconds at best.
const handMadeSkew = 2 * time.Second

type placement struct {
	depth int
	stub  bool
}

// placeCommits finds where each wanted commit sits in a walk back from one
// namespace's head, doubling the window until every one is placed or the cap
// is reached.
//
// Doubling rather than one walk at the cap: the common case is a handful of
// points near the tip, which the first thousand answers, and the walk holds
// the DAG's read lock for its whole length. Re-walking a longer window costs
// at most twice the final one and yields the lock between rounds, which a
// single fifty-thousand-entry walk does not.
func (s *Server) placeCommits(srt *serverRuntime, head codec.Hash, want map[codec.Hash]struct{}) (
	found map[codec.Hash]placement, exhausted bool, err error) {

	nav, err := s.navigatorFor(srt)
	if err != nil {
		return nil, false, err
	}
	found = make(map[codec.Hash]placement, len(want))
	for n := restorePointWalkStart; ; n *= 2 {
		if n > restorePointWalkCap {
			n = restorePointWalkCap
		}
		infos, lerr := nav.ListCommits(head, 0, n)
		if lerr != nil {
			return nil, false, lerr
		}
		clear(found)
		for i, info := range infos {
			if _, ok := want[info.Hash]; !ok {
				continue
			}
			found[info.Hash] = placement{depth: i, stub: info.Stubbed}
		}
		// Short of the window asked for means the walk ran out of reachable
		// history, so anything still missing is not an ancestor of head — a
		// fact about the commit, not a limit of the window.
		if len(found) == len(want) || len(infos) < n || n == restorePointWalkCap {
			return found, len(found) < len(want) && len(infos) == n, nil
		}
	}
}

// collectRestorePoints gathers every tag in every namespace, grouped by name.
func (s *Server) collectRestorePoints(only string) ([]restorePoint, []string, error) {
	src := s.namespaces()
	all := src.Namespaces()

	type member struct {
		ns     restorePointNamespace
		at     codec.Timestamp
		msg    string
		hasTag bool
	}
	byName := map[string][]member{}

	for _, id := range all {
		srt, ok := src.Runtime(id)
		if !ok {
			continue
		}
		d, err := s.commitDAGFor(srt)
		if err != nil {
			return nil, nil, err
		}
		tags := d.ListTags()
		if len(tags) == 0 {
			// No walk at all: placing is for tags, and most namespaces have
			// none. This is what keeps the endpoint cheap on a big database.
			continue
		}
		want := make(map[codec.Hash]struct{}, len(tags))
		for _, t := range tags {
			if only == "" || t.Name == only {
				want[t.CommitHash] = struct{}{}
			}
		}
		if len(want) == 0 {
			continue
		}
		head, err := s.resolveRevisionFor(srt, "head")
		if err != nil {
			return nil, nil, err
		}
		placed, exhausted, err := s.placeCommits(srt, head, want)
		if err != nil {
			return nil, nil, err
		}
		for _, t := range tags {
			if only != "" && t.Name != only {
				continue
			}
			m := member{
				ns: restorePointNamespace{
					Namespace: id, Commit: t.CommitHash.Hex(),
					ShortCommit: shortHash(t.CommitHash.Hex()), Depth: -1,
				},
				at: t.CreatedAt, msg: t.Message, hasTag: true,
			}
			if at, ok := placed[t.CommitHash]; ok {
				m.ns.Depth, m.ns.Stub = at.depth, at.stub
			} else if exhausted && d.IsAncestor(t.CommitHash, head) {
				m.ns.BeyondWindow = true
			} else {
				m.ns.Unreachable = true
			}
			byName[t.Name] = append(byName[t.Name], m)
		}
	}

	points := make([]restorePoint, 0, len(byName))
	for name, members := range byName {
		p := restorePoint{Name: name, Restorable: true}
		have := map[string]bool{}
		var oldest, newest codec.Timestamp
		for i, m := range members {
			p.In = append(p.In, m.ns)
			have[m.ns.Namespace] = true
			if p.Message == "" {
				p.Message = m.msg
			}
			if i == 0 || m.at.EpochMillis < oldest.EpochMillis {
				oldest = m.at
			}
			if i == 0 || m.at.EpochMillis > newest.EpochMillis {
				newest = m.at
			}
			if m.ns.Depth < 0 || m.ns.Stub {
				p.Restorable = false
			}
		}
		sort.Slice(p.In, func(i, j int) bool { return p.In[i].Namespace < p.In[j].Namespace })
		for _, id := range all {
			if !have[id] {
				p.Missing = append(p.Missing, id)
			}
		}
		p.Complete = len(p.Missing) == 0
		if !p.Complete {
			p.Restorable = false
		}
		// The point is dated by its oldest member, so a capture is dated when
		// it began rather than when it happened to finish.
		p.CreatedAt = millisToRFC3339(oldest.EpochMillis)
		p.createdMillis = oldest.EpochMillis
		if newest.EpochMillis-oldest.EpochMillis > handMadeSkew.Milliseconds() {
			p.HandMade = true
		}
		points = append(points, p)
	}

	// Newest first.
	//
	// Timestamp leads, because it is the only thing comparable between points
	// that cover different namespaces — depth is per namespace, and members
	// disagree. It is sound because a capture writes every member in one
	// operation, which is exactly what HandMade flags the exceptions to.
	//
	// Depth breaks the ties, and has to: two captures moments apart share a
	// millisecond, and then the clock cannot separate them while the history
	// plainly can. A point nearer its namespace's head is the newer one, so
	// the shallowest member is the tiebreak — the one piece of ordering
	// information that does not depend on a clock at all.
	sortRestorePoints(points)
	return points, all, nil
}

const restorePointNote = "a restore point is one name tagged in every namespace at one instant. " +
	"Restoring it is one revert per namespace, not one transaction across them: an undo point is " +
	"taken first so a restore that stops partway can be walked back."

func (s *Server) handleRestorePoints(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	points, all, err := s.collectRestorePoints("")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "walk_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespaces": all,
		"points":     points,
		"note":       restorePointNote,
	})
}

type createRestorePointRequest struct {
	Name    string `json:"name"`
	Message string `json:"message"`
	// Namespaces limits the point to a subset. Empty means every namespace
	// this control plane serves, which is the only value that makes a point
	// able to put the whole database back to one instant.
	Namespaces []string `json:"namespaces,omitempty"`
	Durable    bool     `json:"durable"`
}

// captureHeads reads every target namespace's head twice over, and only
// answers when two consecutive reads agree.
//
// There is no cross-namespace snapshot to take, and drain is one-way, so this
// is the honest substitute: the same optimistic shape the revert handshake
// already uses, widened to N. A point whose members were never simultaneous
// would restore the database to an instant that never existed, so a capture
// that cannot settle refuses rather than records one.
func (s *Server) captureHeads(targets []string) (map[string]codec.Hash, []string, error) {
	src := s.namespaces()
	read := func() (map[string]codec.Hash, error) {
		out := make(map[string]codec.Hash, len(targets))
		for _, id := range targets {
			srt, ok := src.Runtime(id)
			if !ok {
				return nil, fmt.Errorf("control: no runtime for namespace %q", id)
			}
			h, err := s.resolveRevisionFor(srt, "head")
			if err != nil {
				return nil, err
			}
			out[id] = h
		}
		return out, nil
	}

	prev, err := read()
	if err != nil {
		return nil, nil, err
	}
	for i := 0; i < captureAttempts; i++ {
		next, err := read()
		if err != nil {
			return nil, nil, err
		}
		var moved []string
		for id, h := range next {
			if prev[id] != h {
				moved = append(moved, id)
			}
		}
		if len(moved) == 0 {
			return next, nil, nil
		}
		prev = next
		time.Sleep(time.Duration(i+1) * 20 * time.Millisecond)
	}
	var moved []string
	last, err := read()
	if err != nil {
		return nil, nil, err
	}
	for id, h := range last {
		if prev[id] != h {
			moved = append(moved, id)
		}
	}
	sort.Strings(moved)
	return nil, moved, nil
}

func (s *Server) handleCreateRestorePoint(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	if !s.opts.AllowWrites {
		writeError(w, http.StatusForbidden, "read_only",
			"this control plane is read-only; start the service with --control-write")
		return
	}
	var req createRestorePointRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "could not read request body: "+err.Error())
		return
	}
	name, err := validRefName(req.Name, "restore point")
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	point, code, status, err := s.createRestorePoint(name, req.Message, req.Namespaces, req.Durable)
	if err != nil {
		writeError(w, status, code, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"point": point, "note": restorePointNote})
}

// createRestorePoint is the whole capture, shared by the create endpoint and
// by the undo point a restore takes before it moves anything.
func (s *Server) createRestorePoint(name, message string, only []string, durable bool) (
	*restorePoint, string, int, error) {

	src := s.namespaces()
	targets := only
	if len(targets) == 0 {
		targets = src.Namespaces()
	}
	if len(targets) == 0 {
		return nil, "no_runtime", http.StatusInternalServerError,
			fmt.Errorf("control: this control plane serves no namespaces")
	}

	// Every target must be able to keep the point before any of them takes
	// one. A point that cannot restore one collection is not a restore point,
	// and finding that out halfway through is worse than not starting.
	for _, id := range targets {
		srt, ok := src.Runtime(id)
		if !ok {
			return nil, "unknown_namespace", http.StatusNotFound,
				fmt.Errorf("control: no namespace %q", id)
		}
		if srt.Runtime == nil {
			return nil, "no_runtime", http.StatusInternalServerError,
				fmt.Errorf("control: namespace %q has no embedded runtime", id)
		}
		if err := srt.Runtime.AssertRetainsHistory(id, "creating a restore point"); err != nil {
			return nil, "history_not_retained", http.StatusConflict, err
		}
	}

	heads, moved, err := s.captureHeads(targets)
	if err != nil {
		return nil, "capture_failed", http.StatusInternalServerError, err
	}
	if heads == nil {
		return nil, "too_busy", http.StatusConflict, fmt.Errorf(
			"could not read every namespace at one instant: %v kept moving. A restore point whose "+
				"members were never simultaneous would name a state the database was never in, so "+
				"none was created. Try again when writes are quieter", moved)
	}

	// Tag every target, and undo the ones already made if any refuses — a
	// half-made point is the thing this is supposed to prevent.
	made := make([]string, 0, len(targets))
	undo := func() {
		for _, id := range made {
			if srt, ok := src.Runtime(id); ok {
				if d, derr := s.commitDAGFor(srt); derr == nil {
					d.DeleteTag(name)
				}
			}
		}
	}
	for _, id := range targets {
		srt, _ := src.Runtime(id)
		d, derr := s.commitDAGFor(srt)
		if derr != nil {
			undo()
			return nil, "no_dag", http.StatusInternalServerError, derr
		}
		if _, terr := d.CreateTag(name, heads[id], message); terr != nil {
			undo()
			return nil, "create_failed", http.StatusConflict, fmt.Errorf(
				"could not name %q in namespace %q: %w (no restore point was created)", name, id, terr)
		}
		made = append(made, id)
	}

	if durable {
		// The name is checkpoint-only state until something writes one, and a
		// restore point whose whole promise is being there later should not
		// depend on a clean shutdown. Failures are reported, not unwound: the
		// point exists either way.
		for _, id := range targets {
			if srt, ok := src.Runtime(id); ok && srt.Runtime != nil {
				_, _ = srt.Runtime.Maintain()
			}
		}
	}

	points, _, err := s.collectRestorePoints(name)
	if err != nil || len(points) == 0 {
		return nil, "walk_failed", http.StatusInternalServerError,
			fmt.Errorf("the restore point was created but could not be read back: %v", err)
	}
	return &points[0], "", 0, nil
}

func (s *Server) handleRestoreToPoint(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	if !s.opts.AllowWrites {
		writeError(w, http.StatusForbidden, "read_only",
			"this control plane is read-only; start the service with --control-write")
		return
	}
	name := r.PathValue("name")
	points, _, err := s.collectRestorePoints(name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "walk_failed", err.Error())
		return
	}
	if len(points) == 0 {
		writeError(w, http.StatusNotFound, "not_found", "no restore point named "+name)
		return
	}
	point := points[0]
	if !point.Complete {
		writeError(w, http.StatusConflict, "incomplete_point", fmt.Sprintf(
			"restore point %q is missing from %v, so restoring it would leave those namespaces "+
				"where they are while the rest moved back", name, point.Missing))
		return
	}
	if !point.Restorable {
		writeError(w, http.StatusConflict, "point_not_restorable", fmt.Sprintf(
			"restore point %q names a commit that cannot be restored in every namespace: one or "+
				"more members is archived or no longer on its namespace's history", name))
		return
	}

	src := s.namespaces()
	spec := "tag:" + name

	// Plan every namespace before touching any. This is where a reclaimed
	// tree or a missing commit is caught, and catching it here means nothing
	// has moved yet.
	for _, m := range point.In {
		srt, ok := src.Runtime(m.Namespace)
		if !ok || srt.Runtime == nil {
			writeError(w, http.StatusInternalServerError, "no_runtime",
				"namespace "+m.Namespace+" has no embedded runtime")
			return
		}
		if _, err := embed.DiffCommits(srt.Runtime, "head", spec); err != nil {
			writeError(w, http.StatusServiceUnavailable, "plan_unavailable", fmt.Sprintf(
				"namespace %s cannot be restored to %q: %v. Nothing was changed",
				m.Namespace, name, err))
			return
		}
	}

	// The way back, taken before the way there. A revert goes forward, so this
	// is what makes a restore that stops partway recoverable.
	undoName := "before-" + name + "-" + strconv.FormatInt(time.Now().UTC().Unix(), 10)
	undo, code, status, err := s.createRestorePoint(undoName, "taken automatically before restoring "+name, nil, false)
	if err != nil {
		writeError(w, status, code, "could not take an undo point before restoring, so nothing was "+
			"changed: "+err.Error())
		return
	}

	type outcome struct {
		Namespace string `json:"namespace"`
		Restored  int    `json:"restored"`
		Removed   int    `json:"removed"`
		Commit    string `json:"commit"`
	}
	done := make([]outcome, 0, len(point.In))
	for _, m := range point.In {
		srt, _ := src.Runtime(m.Namespace)
		res, rerr := embed.RevertTo(srt.Runtime, m.Namespace, spec)
		if rerr != nil {
			writeErrorWithDetail(w, http.StatusServiceUnavailable, "restore_incomplete", fmt.Sprintf(
				"namespace %s failed to restore: %v. %d namespace(s) had already moved. Restore %q "+
					"to put them back", m.Namespace, rerr, len(done), undoName),
				map[string]any{"undoPoint": undoName, "restored": done, "failedAt": m.Namespace})
			return
		}
		done = append(done, outcome{
			Namespace: m.Namespace, Restored: res.Restored,
			Removed: res.Removed, Commit: res.Commit.Hex(),
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"point":     point.Name,
		"undoPoint": undo.Name,
		"restored":  done,
		"note": "every namespace moved. Each one gained a new commit, so this is undoable the same " +
			"way it was done: restore " + undo.Name + ".",
	})
}

// sortRestorePoints orders points newest first: by capture instant, then by depth, then by name.
// See the comment at its call site for why each key is there.
func sortRestorePoints(points []restorePoint) {
	minDepth := func(p restorePoint) int {
		best := -1
		for _, m := range p.In {
			if m.Depth < 0 {
				continue
			}
			if best < 0 || m.Depth < best {
				best = m.Depth
			}
		}
		return best
	}
	sort.Slice(points, func(i, j int) bool {
		a, b := points[i], points[j]
		if a.createdMillis != b.createdMillis {
			return a.createdMillis > b.createdMillis
		}
		da, db := minDepth(a), minDepth(b)
		if da != db && da >= 0 && db >= 0 {
			return da < db
		}
		return a.Name < b.Name
	})
}
