package driver

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// A database is everything the connections of one data source share: one memory group, or one
// file data root. database/sql pools connections, and every connection to the same data has to
// reach the same runtimes - for a file data root because its directory lock admits one opener,
// and for either because each namespace's write lock is what keeps two connections' commits from
// racing, the job the server's write gate does for its clients.
//
// The driver cannot use the server package for this: kdb/driver is an entry point of the
// embeddable source bundle, which promises not to pull in the server or wire layers
// (go/embedbundle). So this is the small in-process equivalent: a namespace lock where the server
// has a write gate, and the same embed cross-namespace protocol underneath.
type database struct {
	key      string
	mode     Mode
	dataRoot string
	catalog  string
	host     *embed.Host
	coord    *embed.TxnCoordinator
	dropOn   bool

	mu         sync.Mutex
	namespaces map[string]*namespace
	refs       int
}

type registry struct {
	mu  sync.Mutex
	dbs map[string]*database
}

var databases = &registry{dbs: make(map[string]*database)}

func (r *registry) clearAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, db := range r.dbs {
		db.closeLocked()
	}
	r.dbs = make(map[string]*database)
}

// acquire returns the database parsed names, opening it on first use, with one reference taken
// for the caller.
func (r *registry) acquire(parsed ParsedURL) (*database, error) {
	var key string
	switch parsed.Mode {
	case ModeMemory:
		// Keyed by isolation group rather than namespace, so memory:///demo/users and
		// memory:///demo/orders are two namespaces of one database - which is what lets a
		// transaction span them - exactly as two namespaces under one file data root are.
		key = "memory:" + isolateKey(parsed.MemoryParams)
	case ModeFile:
		if parsed.DataRoot == "" {
			return nil, fmt.Errorf("file URL missing data root")
		}
		abs, err := filepath.Abs(parsed.DataRoot)
		if err != nil {
			return nil, err
		}
		key = "file:" + abs
	default:
		return nil, fmt.Errorf("unsupported mode")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if db, ok := r.dbs[key]; ok {
		db.refs++
		return db, nil
	}
	db := &database{
		key:        key,
		mode:       parsed.Mode,
		dataRoot:   parsed.DataRoot,
		catalog:    parsed.Catalog,
		dropOn:     dropOnClose(parsed.MemoryParams),
		namespaces: make(map[string]*namespace),
		refs:       1,
	}
	if parsed.Mode == ModeFile {
		host, err := embed.OpenFileHost(parsed.DataRoot, embed.FileRuntimeOptions{})
		if err != nil {
			return nil, err
		}
		db.host = host
		db.coord = host.Transactions()
	} else {
		db.coord = embed.NewMemoryTxnCoordinator()
	}
	r.dbs[key] = db
	return db, nil
}

// release drops one reference. A file database closes when its last connection does - its
// directory lock must not outlive them. A memory database is kept for the next connection
// unless its URL asked for dropOnClose, which is the point of a shared in-memory database.
func (r *registry) release(db *database) {
	r.mu.Lock()
	defer r.mu.Unlock()
	db.refs--
	if db.refs > 0 {
		return
	}
	if db.mode == ModeFile || db.dropOn {
		db.closeLocked()
		delete(r.dbs, db.key)
	}
}

func (db *database) closeLocked() {
	if db.host != nil {
		_ = db.host.Close()
		db.host = nil
	}
}

// exists reports whether namespace id is open in this database or, for a file database, present
// under its data root.
func (db *database) exists(id string) bool {
	db.mu.Lock()
	_, open := db.namespaces[id]
	db.mu.Unlock()
	if open {
		return true
	}
	return db.mode == ModeFile && embed.NamespaceExists(db.dataRoot, id)
}

// namespace returns namespace id, opening it on first use. create permits opening one that does
// not exist yet; a read never should.
func (db *database) namespace(id string, create bool) (*namespace, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if ns, ok := db.namespaces[id]; ok {
		return ns, nil
	}
	if !create && !(db.mode == ModeFile && embed.NamespaceExists(db.dataRoot, id)) {
		return nil, fmt.Errorf("%w: %s", ErrUnknownNamespace, id)
	}
	var rt *embed.EmbeddedKdbRuntime
	var err error
	if db.mode == ModeFile {
		if db.host == nil {
			return nil, fmt.Errorf("kdb driver: database is closed")
		}
		rt, err = db.host.Namespace(embed.CatalogFromNamespace(id), id, schema.None())
	} else {
		rt, err = embed.OpenMemoryRuntime(embed.CatalogFromNamespace(id), id, schema.None())
	}
	if err != nil {
		return nil, err
	}
	ns, err := newNamespace(id, rt)
	if err != nil {
		return nil, err
	}
	db.namespaces[id] = ns
	return ns, nil
}

// openNamespaces returns every namespace open right now, sorted by id - the lock order.
func (db *database) openNamespaces() []*namespace {
	db.mu.Lock()
	defer db.mu.Unlock()
	out := make([]*namespace, 0, len(db.namespaces))
	for _, ns := range db.namespaces {
		out = append(out, ns)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

// namespace is one namespace's runtime plus the state every connection writing it shares.
type namespace struct {
	id        string
	rt        *embed.EmbeddedKdbRuntime
	d         *dag.InMemoryCommitDag
	persister *embed.PersistingCommitDAG

	engine  transaction.Engine
	uniques *transaction.UniqueKeyRegistry

	// write serializes commits into this namespace, from the check that a transaction may land
	// through queueing it on the log. Taken in namespace-id order when a transaction spans
	// several, so two such transactions cannot deadlock.
	write sync.Mutex

	schemaMu  sync.RWMutex
	sqlEngine sql.Engine

	// fenced refuses writes after a cross-namespace transaction this namespace took part in
	// failed once published; see embed.GroupFailedError.
	fenced atomic.Pointer[error]
}

func newNamespace(id string, rt *embed.EmbeddedKdbRuntime) (*namespace, error) {
	ns := &namespace{id: id, rt: rt, uniques: transaction.NewUniqueKeyRegistry()}
	switch concrete := rt.DAG.(type) {
	case *dag.InMemoryCommitDag:
		ns.d = concrete
	case *embed.PersistingCommitDAG:
		ns.d, ns.persister = concrete.Delegate(), concrete
	default:
		return nil, fmt.Errorf("kdb driver: unsupported commit graph %T", rt.DAG)
	}
	ns.engine = transaction.NewEngineWithOptions(transaction.ConflictPolicyStrict, nil,
		transaction.EngineOptions{UniqueKeys: ns.uniques, Preconditions: true})
	ns.sqlEngine = sql.NewEngine(rt.Storage, ns.d)
	if err := ns.rebuildUniques(); err != nil {
		return nil, err
	}
	return ns, nil
}

func (ns *namespace) schema() schema.KdbSchema {
	ns.schemaMu.RLock()
	defer ns.schemaMu.RUnlock()
	return ns.rt.Schema
}

// setSchemaChecked installs sch unless the data already violates a unique constraint it
// declares, in which case the previous schema stays.
func (ns *namespace) setSchemaChecked(sch schema.KdbSchema) error {
	ns.write.Lock()
	defer ns.write.Unlock()
	ns.schemaMu.Lock()
	previous := ns.rt.Schema
	ns.rt.Schema = sch
	ns.schemaMu.Unlock()
	if err := ns.rebuildUniques(); err != nil {
		ns.schemaMu.Lock()
		ns.rt.Schema = previous
		ns.schemaMu.Unlock()
		_ = ns.rebuildUniques()
		return err
	}
	return nil
}

func (ns *namespace) rebuildUniques() error {
	head, err := ns.d.Head()
	if err != nil {
		return err
	}
	c, ok := ns.d.GetCommit(head)
	if !ok {
		return nil
	}
	return ns.uniques.Rebuild(ns.id, ns.rt.Storage, c.DocumentTreeHash, ns.schema())
}

func (ns *namespace) head() (codec.Hash, error) { return ns.d.Head() }

func (ns *namespace) fence(err error) { ns.fenced.CompareAndSwap(nil, &err) }

func (ns *namespace) fenceErr() error {
	if p := ns.fenced.Load(); p != nil {
		return *p
	}
	return nil
}

func isolateKey(params map[string]string) string {
	if params["unique"] == "true" {
		var b [16]byte
		_, _ = rand.Read(b[:])
		return hex.EncodeToString(b[:])
	}
	return params["isolate"]
}

func dropOnClose(params map[string]string) bool {
	return params["dropOnClose"] == "true"
}
