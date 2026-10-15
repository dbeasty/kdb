// kdb-qtask-sim reproduces the workload qtask (a task-management backend migrating from MongoDB)
// puts on kdb-service, over the real wire protocol, so the qtask production plan
// (docs/kdb-qtask-production-plan.md) can be measured from this repository without qtask, Docker
// or MongoDB. It issues the SQL qtask's KDB store issues (src/data/kdb/store.ts in qtask):
//
//   - lean reads:  SELECT kdb_id, <fields> FROM docs WHERE _c = ? AND (...) AND kdb_id > ?
//     ORDER BY kdb_id LIMIT 5000, paged by the last id;
//   - keyed reads: SELECT kdb_id, _doc FROM docs WHERE kdb_id IN (<= 100 ids), paged;
//   - scans:       SELECT kdb_id, _doc FROM docs WHERE _c = ? AND (...), paged by 2000;
//   - counts:      SELECT COUNT(*) FROM docs WHERE _c = ? AND (...);
//   - writes:      whole-document replaces, conditional on the content hash for updates.
//
// over a generated dataset shaped like qtask's (one namespace, every collection tagged by _c,
// tasks carrying 768-number embeddings, a skewed user distribution).
//
//	kdb-qtask-sim load  -addr 127.0.0.1:9090 -tasks 100000
//	kdb-qtask-sim bench -addr 127.0.0.1:9090 -profile mixed -users 8 -duration 60s
//	kdb-qtask-sim probe -addr 127.0.0.1:9090           # each statement once, timed
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
)

type config struct {
	addr      string
	namespace string
	token     string
	users     int
	tasks     int
	zipf      float64
	seed      int64
	dims      int
	batch     int
	profile   string
	sessions  int
	duration  time.Duration
	warmup    time.Duration
	timeout   time.Duration
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: kdb-qtask-sim load|bench|probe [flags]")
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	var c config
	fs.StringVar(&c.addr, "addr", "127.0.0.1:9090", "kdb-service SQL wire address")
	fs.StringVar(&c.namespace, "namespace", "qtask/docs", "namespace")
	fs.StringVar(&c.token, "token", "", "auth token")
	fs.IntVar(&c.users, "user-count", 1000, "users in the dataset")
	fs.IntVar(&c.tasks, "tasks", 100000, "tasks in the dataset (other collections scale with it)")
	fs.Float64Var(&c.zipf, "skew", 0.6, "user weight exponent: user i weighs 1/(i+1)^skew (qtask bench 0.6, rehearsal 0.9)")
	fs.Int64Var(&c.seed, "seed", 1, "dataset seed")
	fs.IntVar(&c.dims, "dims", 768, "embedding length (0: none)")
	fs.IntVar(&c.batch, "batch", 200, "documents per load commit")
	fs.StringVar(&c.profile, "profile", "mixed", "bench profile: mixed, read-heavy, write-heavy, claim")
	fs.IntVar(&c.sessions, "users", 8, "concurrent simulated users (bench)")
	fs.DurationVar(&c.duration, "duration", 30*time.Second, "measured window (bench)")
	fs.DurationVar(&c.warmup, "warmup", 5*time.Second, "warmup before the window (bench)")
	fs.DurationVar(&c.timeout, "timeout", 30*time.Second, "per-request timeout")
	_ = fs.Parse(os.Args[2:])

	ds := newDataset(c)
	var err error
	switch cmd {
	case "load":
		err = load(c, ds)
	case "bench":
		err = bench(c, ds)
	case "probe":
		err = probe(c, ds)
	default:
		err = fmt.Errorf("unknown command %q", cmd)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// ---------------------------------------------------------------------------------------------
// Dataset: deterministic from (seed, users, tasks), so bench can name documents load wrote
// without reading them back.

type dataset struct {
	c          config
	userIDs    []string
	userWeight []float64 // cumulative, for sampling
	userTasks  [][]int   // task ordinals per user
	userProjs  [][]int   // project ordinals per user
	projects   int
	jobs       int
	comments   int
	activities int
	notes      int
	ttl        int
}

// docID is the kdb_id of the n'th document of a collection: a UUID whose first bytes name the
// collection, as qtask's ids do (an ObjectId left-padded into a UUID).
func docID(coll byte, n int) string {
	return fmt.Sprintf("00000000-6890-1a2b-%02x01-%012x", coll, n)
}

const (
	cUsers      = 0x01
	cProjects   = 0x02
	cTasks      = 0x03
	cJobs       = 0x04
	cComments   = 0x05
	cActivities = 0x06
	cNotes      = 0x07
	cTTL        = 0x08
	cNew        = 0x20
)

func newDataset(c config) *dataset {
	d := &dataset{c: c}
	d.projects = c.users * 5
	d.jobs = c.tasks + c.tasks/10
	d.comments = c.tasks / 2
	d.activities = c.tasks * 2
	d.notes = c.users * 50
	d.ttl = c.tasks / 10
	w := make([]float64, c.users)
	sw := make([]float64, c.users)
	total, stotal := 0.0, 0.0
	for i := range w {
		w[i] = 1 / math.Pow(float64(i+1), c.zipf)
		sw[i] = math.Sqrt(w[i])
		total += w[i]
		stotal += sw[i]
	}
	d.userWeight = make([]float64, c.users)
	acc := 0.0
	for i := range w {
		acc += w[i] / total
		d.userWeight[i] = acc
		d.userIDs = append(d.userIDs, docID(cUsers, i))
	}
	d.userTasks = make([][]int, c.users)
	d.userProjs = make([][]int, c.users)
	for p := 0; p < d.projects; p++ {
		u := p % c.users
		d.userProjs[u] = append(d.userProjs[u], p)
	}
	t := 0
	for u := 0; u < c.users && t < c.tasks; u++ {
		n := int(math.Round(float64(c.tasks) * sw[u] / stotal))
		if c.zipf >= 0.85 {
			n = int(math.Round(float64(c.tasks) * w[u] / total))
		}
		for k := 0; k < n && t < c.tasks; k++ {
			d.userTasks[u] = append(d.userTasks[u], t)
			t++
		}
	}
	for ; t < c.tasks; t++ {
		d.userTasks[t%c.users] = append(d.userTasks[t%c.users], t)
	}
	return d
}

func (d *dataset) pickUser(r *rand.Rand) int {
	x := r.Float64()
	return sort.SearchFloat64s(d.userWeight, x)
}

func (d *dataset) taskOwner(t int) int {
	// Linear in users, fine for load.
	for u, ts := range d.userTasks {
		if len(ts) > 0 && t >= ts[0] && t <= ts[len(ts)-1] {
			return u
		}
	}
	return 0
}

var words = strings.Fields("roof permit invoice paint deck window plumbing electric drywall tile cabinet " +
	"order schedule inspect quote measure deliver install review client vendor budget repair")

func sentence(r *rand.Rand, n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(words[r.Intn(len(words))])
	}
	return b.String()
}

func embedding(r *rand.Rand, dims int) []float64 {
	out := make([]float64, dims)
	for i := range out {
		out[i] = math.Round((r.Float64()*0.2-0.1)*1e17) / 1e17
	}
	return out
}

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func isoAt(r *rand.Rand) string {
	return epoch.Add(time.Duration(r.Int63n(int64(200 * 24 * time.Hour)))).Format("2006-01-02T15:04:05.000Z")
}

var statuses = []string{"todo", "in_progress", "done", "blocked"}

func (d *dataset) task(t int, r *rand.Rand) map[string]any {
	u := d.taskOwner(t)
	projs := d.userProjs[u]
	m := map[string]any{
		"_id": fmt.Sprintf("t%d", t), "_c": "tasks", "userId": d.userIDs[u], "assigneeId": d.userIDs[u],
		"title": sentence(r, 4), "description": sentence(r, 5+r.Intn(35)),
		"status": statuses[r.Intn(len(statuses))], "priority": "medium", "dueDate": isoAt(r),
		"tags": []string{words[r.Intn(len(words))]}, "percentComplete": r.Intn(101), "hoursSpent": r.Intn(40),
		"steps": []any{}, "materials": []any{}, "laborLines": []any{}, "subtasks": []any{}, "links": []any{},
		"sortOrder": t % 1000, "createdAt": isoAt(r), "updatedAt": isoAt(r),
	}
	if len(projs) > 0 {
		m["projectIds"] = []string{docID(cProjects, projs[r.Intn(len(projs))])}
	}
	for i := r.Intn(5); i > 0; i-- {
		m["steps"] = append(m["steps"].([]any), map[string]any{"_id": fmt.Sprintf("s%d", i), "text": sentence(r, 4), "done": r.Intn(2) == 0})
	}
	if d.c.dims > 0 && r.Intn(10) != 0 {
		m["embedding"] = embedding(r, d.c.dims)
	}
	return m
}

type genDoc struct {
	id   string
	body map[string]any
}

// generate yields every document of the dataset in load order.
func (d *dataset) generate(yield func(genDoc) error) error {
	r := rand.New(rand.NewSource(d.c.seed))
	for i := 0; i < d.c.users; i++ {
		if err := yield(genDoc{d.userIDs[i], map[string]any{"_c": "users", "email": fmt.Sprintf("user%d@sim.test", i),
			"displayName": sentence(r, 2), "preferences": map[string]any{"theme": "light"}, "createdAt": isoAt(r),
			"_u_users_email": fmt.Sprintf(`["user%d@sim.test"]`, i)}}); err != nil {
			return err
		}
	}
	for p := 0; p < d.projects; p++ {
		u := p % d.c.users
		body := map[string]any{"_c": "projects", "userId": d.userIDs[u], "name": sentence(r, 3), "description": sentence(r, 10),
			"parentId": nil, "_nulls": []string{"parentId"}, "status": "active", "collaborators": []any{},
			"trackingRollup": map[string]any{"hours": 0, "cost": 0}, "createdAt": isoAt(r)}
		if r.Intn(10) < 3 {
			body["collaborators"] = []any{map[string]any{"userId": d.userIDs[r.Intn(d.c.users)], "role": "editor"}}
		}
		if d.c.dims > 0 && r.Intn(10) != 0 {
			body["embedding"] = embedding(r, d.c.dims)
		}
		if err := yield(genDoc{docID(cProjects, p), body}); err != nil {
			return err
		}
	}
	for t := 0; t < d.c.tasks; t++ {
		if err := yield(genDoc{docID(cTasks, t), d.task(t, r)}); err != nil {
			return err
		}
	}
	for j := 0; j < d.jobs; j++ {
		status := "completed"
		if j%5 == 0 || j >= d.c.tasks {
			status = "pending"
		}
		ent := j % d.c.tasks
		if err := yield(genDoc{docID(cJobs, j), map[string]any{"_c": "embeddingJobs", "entityType": "task",
			"entityId": fmt.Sprintf("t%d", ent), "taskId": fmt.Sprintf("t%d", ent), "status": status, "attempts": 0,
			"dirty": false, "createdAt": isoAt(r), "updatedAt": isoAt(r),
			"_u_embeddingJobs_entityType_entityId": fmt.Sprintf(`["task","t%d","%d"]`, ent, j)}}); err != nil {
			return err
		}
	}
	for k := 0; k < d.comments; k++ {
		t := r.Intn(d.c.tasks)
		if err := yield(genDoc{docID(cComments, k), map[string]any{"_c": "comments", "taskId": fmt.Sprintf("t%d", t),
			"userId": d.userIDs[d.taskOwner(t)], "body": sentence(r, 3+r.Intn(27)), "createdAt": isoAt(r)}}); err != nil {
			return err
		}
	}
	for a := 0; a < d.activities; a++ {
		t := a / 2
		if err := yield(genDoc{docID(cActivities, a), map[string]any{"_c": "activities", "taskId": fmt.Sprintf("t%d", t),
			"userId": d.userIDs[d.taskOwner(t)], "action": "updated", "details": map[string]any{}, "source": "web",
			"createdAt": isoAt(r)}}); err != nil {
			return err
		}
	}
	for n := 0; n < d.notes; n++ {
		if err := yield(genDoc{docID(cNotes, n), map[string]any{"_c": "notifications", "userId": d.userIDs[n%d.c.users],
			"type": "comment", "payload": map[string]any{"preview": sentence(r, 6)}, "read": r.Intn(3) == 0,
			"createdAt": isoAt(r)}}); err != nil {
			return err
		}
	}
	// TTL collection (qtask's llmCallMetrics, OAuth codes): _expireAt in epoch ms, a few already past.
	now := time.Now()
	for x := 0; x < d.ttl; x++ {
		exp := now.Add(time.Duration(r.Intn(48)-2) * time.Hour)
		if err := yield(genDoc{docID(cTTL, x), map[string]any{"_c": "llmCallMetrics", "requestId": fmt.Sprintf("r%d", x),
			"model": "m", "success": true, "_expireAt": exp.UnixMilli(), "createdAt": isoAt(r)}}); err != nil {
			return err
		}
	}
	return nil
}

// indexes are the lookup indexes and unique constraints qtask's store declares.
var hashIndexes = []string{"_c", "email", "userId", "projectId", "projectIds", "status", "priority", "tags",
	"assigneeId", "parentId", "collaborators.userId", "taskId", "read", "entityType", "entityId", "requestId",
	"model", "success", "keyHash"}
var btreeIndexes = []string{"dueDate", "expiresAt", "startedAt", "day"}
var uniqueIndexes = []string{"_u_users_email", "_u_embeddingJobs_entityType_entityId"}

func indexName(path string) string {
	return "qtask_i_" + strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, path)
}

func connect(c config) (*client.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return client.Connect(ctx, c.addr, c.token)
}

func load(c config, d *dataset) error {
	cl, err := connect(c)
	if err != nil {
		return err
	}
	defer cl.Close()
	ctx := context.Background()
	start := time.Now()
	for _, f := range uniqueIndexes {
		if _, err := cl.Exec(ctx, c.namespace, fmt.Sprintf("CREATE UNIQUE INDEX qtask_%s ON docs (%s)", f, f), nil); err != nil && !strings.Contains(err.Error(), "already") {
			return fmt.Errorf("unique %s: %w", f, err)
		}
	}
	for _, f := range hashIndexes {
		if _, err := cl.Exec(ctx, c.namespace, fmt.Sprintf("CREATE INDEX %s ON docs (%s) USING HASH", indexName(f), f), nil); err != nil && !strings.Contains(err.Error(), "already") {
			return fmt.Errorf("index %s: %w", f, err)
		}
	}
	for _, f := range btreeIndexes {
		if _, err := cl.Exec(ctx, c.namespace, fmt.Sprintf("CREATE INDEX %s ON docs (%s) USING BTREE", indexName(f), f), nil); err != nil && !strings.Contains(err.Error(), "already") {
			return fmt.Errorf("index %s: %w", f, err)
		}
	}
	fmt.Printf("indexes declared in %v\n", time.Since(start).Round(time.Millisecond))
	base, err := cl.ReplaceJSON(ctx, c.namespace, docID(0x7f, 0), []byte(`{"_c":"meta","sim":true}`))
	if err != nil {
		return err
	}
	var batch []client.DocWrite
	total, bytes := 0, 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		for attempt := 0; ; attempt++ {
			h, err := cl.Commit(ctx, client.Transaction{Namespace: c.namespace, BaseVersion: base, Writes: batch})
			if err == nil {
				base = h
				break
			}
			if attempt > 20 {
				return err
			}
			time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
		}
		total += len(batch)
		batch = batch[:0]
		if total%20000 < c.batch {
			fmt.Printf("  %d documents, %.1f MB, %v\n", total, float64(bytes)/1e6, time.Since(start).Round(time.Second))
		}
		return nil
	}
	err = d.generate(func(g genDoc) error {
		body, err := json.Marshal(g.body)
		if err != nil {
			return err
		}
		bytes += len(body)
		batch = append(batch, client.DocWrite{DocID: g.id, JSON: body, Replace: true})
		if len(batch) >= c.batch {
			return flush()
		}
		return nil
	})
	if err == nil {
		err = flush()
	}
	if err != nil {
		return err
	}
	fmt.Printf("loaded %d documents (%.1f MB JSON) in %v\n", total, float64(bytes)/1e6, time.Since(start).Round(time.Second))
	return nil
}

// ---------------------------------------------------------------------------------------------
// The statements.

type sim struct {
	c  config
	d  *dataset
	cl *client.Client
	r  *rand.Rand
}

const leanPageRows = 5000
const scanPageRows = 2000
const keyedBatch = 100

// lean is P-LEAN: a lean read of ids plus a few fields, paged by the last id.
func (s *sim) lean(ctx context.Context, coll, where string, cols []string, args []any) ([][]any, error) {
	var out [][]any
	after := ""
	for {
		q := fmt.Sprintf("SELECT kdb_id, %s FROM docs WHERE _c = ?%s AND kdb_id > ? ORDER BY kdb_id LIMIT %d",
			strings.Join(cols, ", "), whereSuffix(where), leanPageRows)
		a := append(append([]any{coll}, args...), after)
		_, rows, err := s.cl.QueryValues(ctx, s.c.namespace, q, a)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
		if len(rows) < leanPageRows {
			return out, nil
		}
		after = fmt.Sprint(rows[len(rows)-1][0])
	}
}

func whereSuffix(where string) string {
	if where == "" {
		return ""
	}
	return " AND (" + where + ")"
}

// keyed is P-KEYED: whole documents by id, in IN-lists of keyedBatch.
func (s *sim) keyed(ctx context.Context, ids []string) (int, error) {
	n := 0
	for len(ids) > 0 {
		k := min(len(ids), keyedBatch)
		part := ids[:k]
		ids = ids[k:]
		ph := strings.TrimSuffix(strings.Repeat("?, ", len(part)), ", ")
		args := make([]any, len(part))
		for i, id := range part {
			args[i] = id
		}
		err := s.cl.QueryRawPages(ctx, s.c.namespace, "SELECT kdb_id, _doc FROM docs WHERE kdb_id IN ("+ph+")", args, scanPageRows,
			func(_ []string, rows [][]string) error { n += len(rows); return nil })
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// scan is P-SCAN: whole documents of a collection matching a pushed filter, paged.
func (s *sim) scan(ctx context.Context, coll, where string, args []any) (int, error) {
	n := 0
	err := s.cl.QueryRawPages(ctx, s.c.namespace, "SELECT kdb_id, _doc FROM docs WHERE _c = ?"+whereSuffix(where),
		append([]any{coll}, args...), scanPageRows, func(_ []string, rows [][]string) error { n += len(rows); return nil })
	return n, err
}

func (s *sim) count(ctx context.Context, coll, where string, args []any) (int, error) {
	_, rows, err := s.cl.QueryRaw(ctx, s.c.namespace, "SELECT COUNT(*) FROM docs WHERE _c = ?"+whereSuffix(where), append([]any{coll}, args...))
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 {
		return 0, fmt.Errorf("count: %d rows", len(rows))
	}
	var n int
	_, err = fmt.Sscan(rows[0][0], &n)
	return n, err
}

// accessFilter is the task access filter: own tasks, or tasks in the user's projects.
func (s *sim) accessFilter(u int) (string, []any) {
	projs := s.d.userProjs[u]
	ph := strings.TrimSuffix(strings.Repeat("?, ", len(projs)), ", ")
	args := []any{s.d.userIDs[u]}
	for _, p := range projs {
		args = append(args, docID(cProjects, p))
	}
	for _, p := range projs {
		args = append(args, docID(cProjects, p))
	}
	return fmt.Sprintf("(userId = ?) OR (projectIds IN (%s)) OR (projectId IN (%s))", ph, ph), args
}

// listTasks is qtask's task list: lean rows of every accessible task, sort and page in process,
// fetch the 200 chosen whole; plus the count, which is a second lean read.
func (s *sim) listTasks(ctx context.Context, u int) error {
	where, args := s.accessFilter(u)
	cols := []string{"_id", "_nulls", "userId", "projectIds", "projectId", "staging", "sortOrder", "createdAt"}
	var rows [][]any
	var countRows int
	var wg sync.WaitGroup
	var e1, e2 error
	wg.Add(2)
	go func() { defer wg.Done(); rows, e1 = s.lean(ctx, "tasks", where, cols, args) }()
	go func() {
		defer wg.Done()
		r, err := s.lean(ctx, "tasks", where, []string{"_id", "staging"}, args)
		countRows, e2 = len(r), err
	}()
	wg.Wait()
	if err := errors.Join(e1, e2); err != nil {
		return err
	}
	_ = countRows
	sort.Slice(rows, func(i, j int) bool { return fmt.Sprint(rows[i][7]) < fmt.Sprint(rows[j][7]) })
	ids := make([]string, 0, 200)
	for _, r := range rows {
		if len(ids) == 200 {
			break
		}
		ids = append(ids, fmt.Sprint(r[0]))
	}
	_, err := s.keyed(ctx, ids)
	return err
}

func (s *sim) randomTask(u int) (int, bool) {
	ts := s.d.userTasks[u]
	if len(ts) == 0 {
		return 0, false
	}
	return ts[s.r.Intn(len(ts))], true
}

func (s *sim) getTask(ctx context.Context, u int) error {
	t, ok := s.randomTask(u)
	if !ok {
		return nil
	}
	if _, _, err := s.cl.GetJSON(ctx, s.c.namespace, docID(cTasks, t)); err != nil && !isNotFound(err) {
		return err
	}
	if _, err := s.scan(ctx, "comments", "taskId = ?", []any{fmt.Sprintf("t%d", t)}); err != nil {
		return err
	}
	_, err := s.keyed(ctx, []string{s.d.userIDs[u]})
	return err
}

func (s *sim) unreadCount(ctx context.Context, u int) error {
	_, err := s.count(ctx, "notifications", "(userId = ?) AND (read = ?)", []any{s.d.userIDs[u], false})
	return err
}

func isNotFound(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "not found")
}

// updateTask is P-UPD on a task: read with hash, change, replace if unchanged; then log an
// activity (a new document).
func (s *sim) updateTask(ctx context.Context, u int) error {
	t, ok := s.randomTask(u)
	if !ok {
		return nil
	}
	id := docID(cTasks, t)
	for attempt := 0; attempt < 60; attempt++ {
		body, hash, err := s.cl.GetJSONWithHash(ctx, s.c.namespace, id)
		if err != nil {
			if isNotFound(err) {
				return nil
			}
			return err
		}
		var m map[string]any
		if err := json.Unmarshal(body, &m); err != nil {
			return err
		}
		m["percentComplete"] = s.r.Intn(101)
		m["updatedAt"] = time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
		nb, _ := json.Marshal(m)
		_, err = s.cl.ReplaceIf(ctx, s.c.namespace, id, nb, hash)
		if err == nil {
			break
		}
		if !strings.Contains(strings.ToLower(err.Error()), "precondition") {
			return err
		}
		time.Sleep(time.Duration(s.r.Intn(8)) * time.Millisecond)
	}
	return s.newDoc(ctx, map[string]any{"_c": "activities", "taskId": fmt.Sprintf("t%d", t), "userId": s.d.userIDs[u],
		"action": "updated", "details": map[string]any{}, "source": "web", "createdAt": time.Now().UTC().Format(time.RFC3339)})
}

var newSeq atomic.Int64

func (s *sim) newDoc(ctx context.Context, body map[string]any) error {
	b, _ := json.Marshal(body)
	id := docID(cNew, int(newSeq.Add(1))+s.r.Intn(1<<20)<<24)
	_, err := s.cl.ReplaceJSON(ctx, s.c.namespace, id, b)
	return err
}

func (s *sim) createTask(ctx context.Context, u int) error {
	m := s.d.task(s.r.Intn(s.d.c.tasks), s.r)
	m["userId"] = s.d.userIDs[u]
	delete(m, "embedding")
	return s.newDoc(ctx, m)
}

// claim is qtask's queue claim: lean read of every pending job, pick the oldest, claim it with
// a conditional replace.
func (s *sim) claim(ctx context.Context) error {
	rows, err := s.lean(ctx, "embeddingJobs", "(status = ?) AND (attempts < ?)", []string{"_id", "status", "attempts", "createdAt"}, []any{"pending", 3})
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	pick := rows[s.r.Intn(min(len(rows), 50))]
	id := fmt.Sprint(pick[0])
	body, hash, err := s.cl.GetJSONWithHash(ctx, s.c.namespace, id)
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return err
	}
	if m["status"] != "pending" {
		return nil
	}
	m["status"] = "processing"
	nb, _ := json.Marshal(m)
	if _, err := s.cl.ReplaceIf(ctx, s.c.namespace, id, nb, hash); err != nil && !strings.Contains(strings.ToLower(err.Error()), "precondition") {
		return err
	}
	// Requeue so the queue never drains.
	m["status"] = "pending"
	nb, _ = json.Marshal(m)
	_, err = s.cl.ReplaceJSON(ctx, s.c.namespace, id, nb)
	return err
}

type action struct {
	name   string
	weight float64
	write  bool
	run    func(*sim, context.Context, int) error
}

var actions = []action{
	{"listTasks", 18, false, (*sim).listTasks},
	{"getTask", 14, false, (*sim).getTask},
	{"unreadCount", 12, false, (*sim).unreadCount},
	{"me", 3, false, func(s *sim, ctx context.Context, u int) error {
		_, _, err := s.cl.GetJSON(ctx, s.c.namespace, s.d.userIDs[u])
		return err
	}},
	{"updateTask", 12, true, (*sim).updateTask},
	{"createTask", 4, true, (*sim).createTask},
}

func weightsFor(profile string) []float64 {
	w := make([]float64, len(actions))
	for i, a := range actions {
		w[i] = a.weight
		switch profile {
		case "read-heavy":
			if a.write {
				w[i] *= 0.2
			}
		case "write-heavy":
			if a.write {
				w[i] *= 3
			} else {
				w[i] *= 0.35
			}
		}
	}
	return w
}

type opStats struct {
	mu   sync.Mutex
	lat  map[string][]time.Duration
	errs map[string]int
	last map[string]string
}

func (o *opStats) add(name string, d time.Duration, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err != nil {
		o.errs[name]++
		o.last[name] = err.Error()
		return
	}
	o.lat[name] = append(o.lat[name], d)
}

func bench(c config, d *dataset) error {
	clients := make([]*client.Client, c.sessions)
	for i := range clients {
		cl, err := connect(c)
		if err != nil {
			return err
		}
		defer cl.Close()
		clients[i] = cl
	}
	w := weightsFor(c.profile)
	cum := make([]float64, len(w))
	acc := 0.0
	for i, x := range w {
		acc += x
		cum[i] = acc
	}
	stats := &opStats{lat: map[string][]time.Duration{}, errs: map[string]int{}, last: map[string]string{}}
	var measuring atomic.Bool
	var total atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < c.sessions; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := &sim{c: c, d: d, cl: clients[i], r: rand.New(rand.NewSource(c.seed*1000 + int64(i)))}
			for {
				select {
				case <-stop:
					return
				default:
				}
				name := "claim"
				var run func(context.Context) error
				if c.profile == "claim" {
					run = s.claim
				} else {
					x := s.r.Float64() * acc
					a := actions[sort.SearchFloat64s(cum, x)]
					u := d.pickUser(s.r)
					name = a.name
					run = func(ctx context.Context) error { return a.run(s, ctx, u) }
				}
				ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
				t0 := time.Now()
				err := run(ctx)
				el := time.Since(t0)
				cancel()
				if measuring.Load() {
					stats.add(name, el, err)
					if err == nil {
						total.Add(1)
					}
				}
			}
		}(i)
	}
	time.Sleep(c.warmup)
	measuring.Store(true)
	t0 := time.Now()
	time.Sleep(c.duration)
	measuring.Store(false)
	window := time.Since(t0)
	close(stop)
	wg.Wait()
	fmt.Printf("profile %s, %d users, %v: %.1f actions/s\n", c.profile, c.sessions, window.Round(time.Second), float64(total.Load())/window.Seconds())
	names := make([]string, 0, len(stats.lat))
	for n := range stats.lat {
		names = append(names, n)
	}
	for n := range stats.errs {
		if _, ok := stats.lat[n]; !ok {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	fmt.Printf("%-14s %8s %9s %9s %9s %6s\n", "action", "n", "p50", "p95", "p99", "errors")
	for _, n := range names {
		l := stats.lat[n]
		sort.Slice(l, func(i, j int) bool { return l[i] < l[j] })
		q := func(p float64) time.Duration {
			if len(l) == 0 {
				return 0
			}
			return l[int(p*float64(len(l)-1))].Round(100 * time.Microsecond)
		}
		fmt.Printf("%-14s %8d %9v %9v %9v %6d\n", n, len(l), q(0.5), q(0.95), q(0.99), stats.errs[n])
		if stats.errs[n] > 0 {
			fmt.Printf("    last error: %s\n", stats.last[n])
		}
	}
	return nil
}

// probe runs each statement shape once against the heaviest user and prints its time.
func probe(c config, d *dataset) error {
	cl, err := connect(c)
	if err != nil {
		return err
	}
	defer cl.Close()
	s := &sim{c: c, d: d, cl: cl, r: rand.New(rand.NewSource(c.seed))}
	ctx := context.Background()
	u := 0
	where, args := s.accessFilter(u)
	timeIt := func(name string, f func() (string, error)) {
		t0 := time.Now()
		info, err := f()
		el := time.Since(t0).Round(100 * time.Microsecond)
		if err != nil {
			fmt.Printf("%-44s %10v  ERROR %v\n", name, el, err)
			return
		}
		fmt.Printf("%-44s %10v  %s\n", name, el, info)
	}
	timeIt("count tasks (namespace-wide collection)", func() (string, error) {
		n, err := s.count(ctx, "tasks", "", nil)
		return fmt.Sprint(n), err
	})
	timeIt("count all documents", func() (string, error) {
		_, rows, err := cl.QueryRaw(ctx, c.namespace, "SELECT COUNT(*) FROM docs", nil)
		return fmt.Sprint(rows), err
	})
	timeIt("count unread notifications (user 0)", func() (string, error) {
		n, err := s.count(ctx, "notifications", "(userId = ?) AND (read = ?)", []any{d.userIDs[u], false})
		return fmt.Sprint(n), err
	})
	timeIt("count heaviest user's tasks (exact)", func() (string, error) {
		n, err := s.count(ctx, "tasks", "userId = ?", []any{d.userIDs[u]})
		return fmt.Sprint(n), err
	})
	timeIt("lean read, heaviest user's accessible tasks", func() (string, error) {
		rows, err := s.lean(ctx, "tasks", where, []string{"_id", "userId", "projectIds", "projectId", "sortOrder", "createdAt"}, args)
		return fmt.Sprintf("%d rows", len(rows)), err
	})
	timeIt("lean read, pending jobs (claim)", func() (string, error) {
		rows, err := s.lean(ctx, "embeddingJobs", "(status = ?) AND (attempts < ?)", []string{"_id", "status", "attempts", "createdAt"}, []any{"pending", 3})
		return fmt.Sprintf("%d rows", len(rows)), err
	})
	ids := make([]string, 0, 200)
	for _, t := range d.userTasks[u] {
		if len(ids) == 200 {
			break
		}
		ids = append(ids, docID(cTasks, t))
	}
	timeIt("keyed read, 200 tasks", func() (string, error) {
		n, err := s.keyed(ctx, ids)
		return fmt.Sprintf("%d docs", n), err
	})
	timeIt("keyed read, 100 tasks after _c = ?", func() (string, error) {
		ph := strings.TrimSuffix(strings.Repeat("?, ", 100), ", ")
		a := []any{"tasks"}
		for _, id := range ids[:min(100, len(ids))] {
			a = append(a, id)
		}
		n := 0
		err := cl.QueryRawPages(ctx, c.namespace, "SELECT kdb_id, _doc FROM docs WHERE _c = ? AND kdb_id IN ("+ph+")", a, scanPageRows,
			func(_ []string, rows [][]string) error { n += len(rows); return nil })
		return fmt.Sprintf("%d docs", n), err
	})
	timeIt("scan heaviest user's tasks (whole docs)", func() (string, error) {
		n, err := s.scan(ctx, "tasks", "userId = ?", []any{d.userIDs[u]})
		return fmt.Sprintf("%d docs", n), err
	})
	timeIt("list tasks (lean x2 + keyed 200)", func() (string, error) {
		return "", s.listTasks(ctx, u)
	})
	return nil
}
