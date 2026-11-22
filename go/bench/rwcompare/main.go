// rwcompare measures KDB's basic document reads and writes against MongoDB's with the same
// workload, the same documents and the same client shape, so that "how far are we from Mongo"
// has one reproducible answer instead of a different harness per question.
//
// It lives in its own module so the MongoDB driver never becomes a dependency of KDB itself
// (KDB is meant to stay small enough to embed in a service).
//
// Workloads are YCSB-shaped (Cooper et al., "Benchmarking Cloud Serving Systems with YCSB"):
// ten 100-byte fields per document (~1 KB), keyed by id, a load phase of single inserts, then
// timed cells of point reads, single-field updates and mixes of the two. Each cell runs a fixed
// number of operations rather than a fixed time, so two runs of a write cell end with stores of
// the same size (time-based cells on a growing store compare different store sizes).
//
// Targets:
//
//	kdb    - a kdb-service over its wire protocol (go/kdb/client): Upsert / GetJSON
//	mongo  - a mongod over its wire protocol (official Go driver): insertOne / findOne / updateOne $set
//
// Run both servers in containers with the same CPU and memory limits and run this binary on the
// same Docker network (scripts in this directory), so neither side pays Docker Desktop's port
// proxy and the other does not.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

const fields = 10

// store is the four operations a cell needs; one value is shared by every worker.
type store interface {
	Insert(ctx context.Context, w int, id string, doc [fields]string) error
	Read(ctx context.Context, w int, id string) (int, error)
	Update(ctx context.Context, w int, id string, field int, value string) error
	Close() error
}

func main() {
	target := flag.String("target", "kdb", "kdb or mongo")
	addr := flag.String("addr", "127.0.0.1:9090", "kdb-service address (kdb) or mongodb:// URI (mongo)")
	label := flag.String("label", "", "free-form label copied into every result line (e.g. image tag, durability)")
	ns := flag.String("namespace", "bench/ycsb", "KDB namespace / Mongo database.collection")
	docs := flag.Int("docs", 100_000, "documents written by the load phase (the read/update key space)")
	loadConc := flag.Int("load-concurrency", 32, "workers in the load phase")
	skipLoad := flag.Bool("skip-load", false, "the store already holds -docs documents written by a previous run with the same -seed")
	cells := flag.String("cells", "read,update,mixed50,read95,insert", "comma-separated workloads: read, update, mixed50 (YCSB A), read95 (YCSB B), insert")
	concs := flag.String("concurrency", "1,8,32", "comma-separated worker counts; every cell runs at each")
	ops := flag.Int("ops", 0, "operations per cell; 0 = 20k at concurrency 1, 100k up to 8, 200k above")
	dist := flag.String("dist", "zipf", "key choice for read/update: zipf (YCSB's 0.99 skew) or uniform")
	seed := flag.Uint64("seed", 42, "document ids and values derive from this")
	mongoJ := flag.Bool("mongo-j", false, "Mongo writes with {j:true} (journal synced before the ack) instead of the default {w:1}")
	jsonOut := flag.String("json", "", "append one JSON object per cell to this file")
	flag.Parse()

	ctx := context.Background()
	var st store
	var err error
	// Retried for a minute: the server container may still be starting.
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(time.Second) {
		switch *target {
		case "kdb":
			st, err = openKDB(ctx, *addr, *ns, max(maxInt(parseInts(*concs)), *loadConc))
		case "mongo":
			st, err = openMongo(ctx, *addr, *ns, max(maxInt(parseInts(*concs)), *loadConc), *mongoJ)
		default:
			fmt.Fprintf(os.Stderr, "unknown -target %q\n", *target)
			os.Exit(2)
		}
		if err == nil || time.Now().After(deadline) {
			break
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer st.Close()

	ids := makeIDs(*seed, *docs)
	vals := makeValues(*seed+1, 4096)

	if !*skipLoad {
		start := time.Now()
		res := runCell(ctx, st, cellSpec{name: "load", conc: *loadConc, ops: *docs}, ids, vals, nil, *dist, *seed)
		res.print(*target, *label)
		res.write(*jsonOut, *target, *label)
		fmt.Printf("# loaded %d docs in %s\n", *docs, time.Since(start).Round(time.Millisecond))
	}

	inserted := atomic.Int64{}
	for _, cell := range strings.Split(*cells, ",") {
		for _, c := range parseInts(*concs) {
			n := *ops
			if n == 0 {
				n = defaultOps(c)
			}
			spec := cellSpec{name: strings.TrimSpace(cell), conc: c, ops: n}
			res := runCell(ctx, st, spec, ids, vals, &inserted, *dist, *seed+uint64(c))
			res.print(*target, *label)
			res.write(*jsonOut, *target, *label)
		}
	}
}

func defaultOps(conc int) int {
	switch {
	case conc <= 1:
		return 20_000
	case conc <= 8:
		return 100_000
	default:
		return 200_000
	}
}

type cellSpec struct {
	name string
	conc int
	ops  int
}

type cellResult struct {
	spec     cellSpec
	elapsed  time.Duration
	reads    []time.Duration
	writes   []time.Duration
	errors   int64
	firstErr string
}

// runCell runs spec.ops operations split across spec.conc workers and records every latency.
func runCell(ctx context.Context, st store, spec cellSpec, ids []string, vals []string, inserted *atomic.Int64, dist string, seed uint64) cellResult {
	var next atomic.Int64
	var errs atomic.Int64
	var firstErr atomic.Value
	reads := make([][]time.Duration, spec.conc)
	writes := make([][]time.Duration, spec.conc)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < spec.conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, uint64(w)+1))
			var zipf *rand.Zipf
			if dist == "zipf" && len(ids) > 1 {
				// YCSB's zipfian constant is 0.99; math/rand's Zipf needs s > 1, so 1.01 is the
				// nearest legal skew. Scrambled below so the hot keys are not the first loaded.
				zipf = rand.NewZipf(rng, 1.01, 1, uint64(len(ids)-1))
			}
			pick := func() string {
				if zipf != nil {
					k := zipf.Uint64()
					return ids[(k*0x9E3779B97F4A7C15)%uint64(len(ids))]
				}
				return ids[rng.IntN(len(ids))]
			}
			for {
				i := next.Add(1) - 1
				if i >= int64(spec.ops) {
					return
				}
				isRead := false
				switch spec.name {
				case "read":
					isRead = true
				case "mixed50":
					isRead = rng.IntN(2) == 0
				case "read95":
					isRead = rng.IntN(100) < 95
				}
				t0 := time.Now()
				var err error
				switch {
				case spec.name == "load":
					err = st.Insert(ctx, w, ids[i], docFor(i, vals))
				case spec.name == "insert":
					n := inserted.Add(1)
					id := extraID(seed, n)
					err = st.Insert(ctx, w, id, docFor(n, vals))
				case isRead:
					_, err = st.Read(ctx, w, pick())
				default:
					err = st.Update(ctx, w, pick(), rng.IntN(fields), vals[rng.IntN(len(vals))])
				}
				d := time.Since(t0)
				if err != nil {
					if errs.Add(1) == 1 {
						firstErr.Store(err.Error())
					}
					continue
				}
				if isRead {
					reads[w] = append(reads[w], d)
				} else {
					writes[w] = append(writes[w], d)
				}
			}
		}(w)
	}
	wg.Wait()
	res := cellResult{spec: spec, elapsed: time.Since(start), errors: errs.Load()}
	if v := firstErr.Load(); v != nil {
		res.firstErr = v.(string)
	}
	res.reads = slices.Concat(reads...)
	res.writes = slices.Concat(writes...)
	slices.Sort(res.reads)
	slices.Sort(res.writes)
	return res
}

func pct(s []time.Duration, p float64) time.Duration {
	if len(s) == 0 {
		return 0
	}
	i := int(math.Ceil(p/100*float64(len(s)))) - 1
	return s[max(0, min(i, len(s)-1))]
}

func (r cellResult) opsPerSec() float64 {
	return float64(len(r.reads)+len(r.writes)) / r.elapsed.Seconds()
}

func (r cellResult) print(target, label string) {
	line := fmt.Sprintf("%-6s %-10s %-8s c=%-3d ops/s=%9.0f", target, label, r.spec.name, r.spec.conc, r.opsPerSec())
	if len(r.reads) > 0 {
		line += fmt.Sprintf("  read p50=%-8s p99=%-8s", us(pct(r.reads, 50)), us(pct(r.reads, 99)))
	}
	if len(r.writes) > 0 {
		line += fmt.Sprintf("  write p50=%-8s p99=%-8s", us(pct(r.writes, 50)), us(pct(r.writes, 99)))
	}
	if r.errors > 0 {
		line += fmt.Sprintf("  ERRORS=%d first=%q", r.errors, r.firstErr)
	}
	fmt.Println(line)
}

func (r cellResult) write(path, target, label string) {
	if path == "" {
		return
	}
	row := map[string]any{
		"target": target, "label": label, "cell": r.spec.name, "concurrency": r.spec.conc,
		"ops": len(r.reads) + len(r.writes), "seconds": r.elapsed.Seconds(), "opsPerSec": r.opsPerSec(),
		"readP50us": pct(r.reads, 50).Microseconds(), "readP99us": pct(r.reads, 99).Microseconds(),
		"writeP50us": pct(r.writes, 50).Microseconds(), "writeP99us": pct(r.writes, 99).Microseconds(),
		"errors": r.errors, "time": time.Now().UTC().Format(time.RFC3339),
	}
	b, _ := json.Marshal(row)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintln(os.Stderr, "json:", err)
		return
	}
	defer f.Close()
	f.Write(append(b, '\n'))
}

func us(d time.Duration) string {
	if d >= 10*time.Millisecond {
		return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', 1, 64) + "ms"
	}
	return strconv.FormatInt(d.Microseconds(), 10) + "us"
}

// --- documents --------------------------------------------------------------------------

func makeIDs(seed uint64, n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = extraID(seed, int64(-i-1))
	}
	return ids
}

// extraID derives a UUID-shaped id (KDB requires UUID doc ids; Mongo takes it as a string _id).
func extraID(seed uint64, n int64) string {
	r := rand.New(rand.NewPCG(seed, uint64(n)))
	a, b := r.Uint64(), r.Uint64()
	a = (a &^ (0xf << 12)) | (4 << 12)   // version 4
	b = (b &^ (0x3 << 62)) | (0x2 << 62) // RFC 4122 variant
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", a>>32, (a>>16)&0xffff, a&0xffff, b>>48, b&0xffffffffffff)
}

func makeValues(seed uint64, n int) []string {
	r := rand.New(rand.NewPCG(seed, 7))
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	vals := make([]string, n)
	buf := make([]byte, 100)
	for i := range vals {
		for j := range buf {
			buf[j] = alphabet[r.IntN(len(alphabet))]
		}
		vals[i] = string(buf)
	}
	return vals
}

func docFor(i int64, vals []string) [fields]string {
	var d [fields]string
	for f := range d {
		d[f] = vals[(int(i)*fields+f)%len(vals)]
	}
	return d
}

// --- KDB --------------------------------------------------------------------------------

type kdbStore struct {
	conns []*client.Client
	ns    string
}

func openKDB(ctx context.Context, addr, ns string, conns int) (*kdbStore, error) {
	s := &kdbStore{ns: ns}
	for i := 0; i < conns; i++ {
		c, err := client.Connect(ctx, addr, "")
		if err != nil {
			s.Close()
			return nil, err
		}
		s.conns = append(s.conns, c)
	}
	return s, nil
}

// conn gives each worker a connection of its own - the shape a pooled Mongo client has too.
func (s *kdbStore) conn(w int) *client.Client {
	return s.conns[w%len(s.conns)]
}

func kdbJSON(doc [fields]string) []byte {
	b := make([]byte, 0, 1200)
	b = append(b, '{')
	for f, v := range doc {
		if f > 0 {
			b = append(b, ',')
		}
		b = append(b, `"field`...)
		b = strconv.AppendInt(b, int64(f), 10)
		b = append(b, `":"`...)
		b = append(b, v...)
		b = append(b, '"')
	}
	return append(b, '}')
}

func (s *kdbStore) Insert(ctx context.Context, w int, id string, doc [fields]string) error {
	_, err := s.conn(w).Upsert(ctx, s.ns, id, kdbJSON(doc))
	return err
}

func (s *kdbStore) Read(ctx context.Context, w int, id string) (int, error) {
	b, _, err := s.conn(w).GetJSON(ctx, s.ns, id)
	if errors.Is(err, client.ErrNotFound) {
		return 0, fmt.Errorf("read %s: not found", id)
	}
	return len(b), err
}

func (s *kdbStore) Update(ctx context.Context, w int, id string, field int, value string) error {
	body := `{"field` + strconv.Itoa(field) + `":"` + value + `"}`
	_, err := s.conn(w).Upsert(ctx, s.ns, id, []byte(body))
	return err
}

func (s *kdbStore) Close() error {
	for _, c := range s.conns {
		c.Close()
	}
	return nil
}

// --- MongoDB ----------------------------------------------------------------------------

type mongoStore struct {
	c    *mongo.Client
	coll *mongo.Collection
}

func openMongo(ctx context.Context, uri, ns string, pool int, journaled bool) (*mongoStore, error) {
	if !strings.HasPrefix(uri, "mongodb") {
		uri = "mongodb://" + uri
	}
	c, err := mongo.Connect(options.Client().ApplyURI(uri).SetMaxPoolSize(uint64(pool)).SetMinPoolSize(uint64(pool)))
	if err != nil {
		return nil, err
	}
	if err := c.Ping(ctx, nil); err != nil {
		return nil, err
	}
	db, coll, _ := strings.Cut(strings.ReplaceAll(ns, "/", "."), ".")
	if coll == "" {
		coll = "docs"
	}
	copts := options.Collection()
	if journaled {
		copts.SetWriteConcern(writeconcern.Journaled())
	} else {
		copts.SetWriteConcern(writeconcern.W1())
	}
	return &mongoStore{c: c, coll: c.Database(db).Collection(coll, copts)}, nil
}

func (s *mongoStore) Insert(ctx context.Context, _ int, id string, doc [fields]string) error {
	d := make(bson.D, 0, fields+1)
	d = append(d, bson.E{Key: "_id", Value: id})
	for f, v := range doc {
		d = append(d, bson.E{Key: "field" + strconv.Itoa(f), Value: v})
	}
	_, err := s.coll.InsertOne(ctx, d)
	return err
}

func (s *mongoStore) Read(ctx context.Context, _ int, id string) (int, error) {
	raw, err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Raw()
	return len(raw), err
}

func (s *mongoStore) Update(ctx context.Context, _ int, id string, field int, value string) error {
	_, err := s.coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "field" + strconv.Itoa(field), Value: value}}}})
	return err
}

func (s *mongoStore) Close() error { return s.c.Disconnect(context.Background()) }

// --- flags ------------------------------------------------------------------------------

func parseInts(s string) []int {
	var out []int
	for _, p := range strings.Split(s, ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(p)); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	return out
}

func maxInt(xs []int) int {
	m := 1
	for _, x := range xs {
		m = max(m, x)
	}
	return m
}
