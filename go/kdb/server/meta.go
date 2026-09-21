package server

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
)

// MetaNamespace holds every namespace's definitions - schema and index DDL - as documents, so they
// are versioned, durable and replicated by the same peer sync that carries data. Reserved (its
// name begins with '_'): never reachable over SQL or the document wire, only through peer sync
// and this package.
const MetaNamespace = "_kdb/meta"

// MetaStore records definition changes into MetaNamespace and applies the definitions it holds to
// the namespaces this process serves - its own changes at startup (which is also what makes a
// schema set by CREATE TABLE survive a restart), and a peer's as they arrive.
//
// One document per definition, with an id derived from what it defines, so two nodes that define
// the same thing produce the same document: agreement is a no-op and disagreement is an ordinary
// same-document conflict in MetaNamespace's queue, resolved like any other.
type MetaStore struct {
	meta *KdbServerRuntime
	set  *NamespaceSet

	kick chan struct{}
	stop chan struct{}
	done chan struct{}
	once sync.Once
	// applyMu serializes reconciliation, so a startup pass and a triggered one never race.
	applyMu sync.Mutex
}

// metaDoc is one definition. Kind "schema" carries Schema (hex of schema.ToBytes); kind "index"
// carries the CREATE INDEX statement that defines it, or Dropped for an index removed since - a
// tombstone rather than a deletion, so a node reconciling knows to drop it rather than merely not
// knowing about it.
type metaDoc struct {
	Kind      string            `json:"kind"`
	Namespace string            `json:"namespace"`
	Schema    string            `json:"schema,omitempty"`
	Name      string            `json:"name,omitempty"`
	Table     string            `json:"table,omitempty"`
	Fields    []sql.IndexField  `json:"fields,omitempty"`
	Using     string            `json:"using,omitempty"`
	Unique    bool              `json:"unique,omitempty"`
	With      map[string]string `json:"with,omitempty"`
	Dropped   bool              `json:"dropped,omitempty"`
}

func metaSchemaID(ns string) codec.UUID { return codec.DerivedUUID("kdb:meta/schema/" + ns) }
func metaIndexID(ns, name string) codec.UUID {
	return codec.DerivedUUID("kdb:meta/index/" + ns + "/" + name)
}

// NewMetaStore returns a store over the meta runtime, applying to the namespaces in set, and wires
// every commit the meta namespace takes - local or replicated - to a reconciliation pass.
func NewMetaStore(meta *KdbServerRuntime, set *NamespaceSet) *MetaStore {
	m := &MetaStore{meta: meta, set: set, kick: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	previous := meta.CommitListener
	meta.CommitListener = func(ns string, c document.Commit) {
		if previous != nil {
			previous(ns, c)
		}
		select {
		case m.kick <- struct{}{}:
		default:
		}
	}
	for _, rt := range set.Runtimes() {
		rt.Meta = m
	}
	go m.loop()
	return m
}

// Close stops reconciling in the background.
func (m *MetaStore) Close() {
	m.once.Do(func() { close(m.stop) })
	<-m.done
}

func (m *MetaStore) loop() {
	defer close(m.done)
	for {
		select {
		case <-m.stop:
			return
		case <-m.kick:
			_ = m.ReconcileAll()
		}
	}
}

func (m *MetaStore) put(id codec.UUID, d metaDoc) error {
	body, err := json.Marshal(d)
	if err != nil {
		return err
	}
	// Unchanged definitions write nothing: a no-op commit would still be a commit every peer
	// then has to fetch.
	if cur, _, found, err := m.meta.GetDocument(MetaNamespace, id); err == nil && found && cur == string(body) {
		return nil
	}
	_, err = m.meta.systemUpsert(id, string(body))
	return err
}

// RecordSchema records ns's schema.
func (m *MetaStore) RecordSchema(ns string, sch schema.KdbSchema) error {
	if m == nil {
		return nil
	}
	raw, err := sch.ToBytes()
	if err != nil {
		return err
	}
	return m.put(metaSchemaID(ns), metaDoc{Kind: "schema", Namespace: ns, Schema: hex.EncodeToString(raw)})
}

// RecordIndex records an index created on ns.
func (m *MetaStore) RecordIndex(ns string, stmt sql.StmtCreateIndex) error {
	if m == nil {
		return nil
	}
	return m.put(metaIndexID(ns, stmt.Name), metaDoc{
		Kind: "index", Namespace: ns, Name: stmt.Name, Table: stmt.Table, Fields: stmt.Fields,
		Using: stmt.Using, Unique: stmt.Unique, With: stmt.With,
	})
}

// RecordDropIndex records an index dropped from ns.
func (m *MetaStore) RecordDropIndex(ns, name string) error {
	if m == nil {
		return nil
	}
	return m.put(metaIndexID(ns, name), metaDoc{Kind: "index", Namespace: ns, Name: name, Dropped: true})
}

// ReconcileAll applies every definition the meta namespace holds to the namespaces this process
// serves. A definition that cannot be applied - a schema the namespace's data violates, an index
// that will not build - is recorded in that namespace's conflict queue rather than stopping the
// rest.
func (m *MetaStore) ReconcileAll() error {
	m.applyMu.Lock()
	defer m.applyMu.Unlock()
	docs, err := m.docs()
	if err != nil {
		return err
	}
	for _, d := range docs {
		rt, ok := m.set.Get(d.Namespace)
		if !ok {
			continue // not served here; applied if and when this process opens it (ApplyTo)
		}
		m.apply(rt, d)
	}
	return nil
}

func (m *MetaStore) docs() ([]metaDoc, error) {
	_, head, ok, err := m.meta.dag.HeadCommit()
	if err != nil || !ok {
		return nil, err
	}
	var docs []metaDoc
	err = m.meta.Runtime.Storage.ScanDocuments(MetaNamespace, head.DocumentTreeHash, 256, func(batch []document.Document) error {
		for _, d := range batch {
			var md metaDoc
			if json.Unmarshal([]byte(d.JSON), &md) == nil && md.Namespace != "" {
				docs = append(docs, md)
			}
		}
		return nil
	})
	sort.Slice(docs, func(i, j int) bool {
		if docs[i].Namespace != docs[j].Namespace {
			return docs[i].Namespace < docs[j].Namespace
		}
		return docs[i].Kind > docs[j].Kind // schema before index: an index may need the schema's fields
	})
	return docs, err
}

// ApplyTo attaches rt to the store and applies its namespace's stored definitions - for a
// namespace opened after startup, before it is added to the set.
func (m *MetaStore) ApplyTo(rt *KdbServerRuntime) {
	if m == nil {
		return
	}
	rt.Meta = m
	m.applyMu.Lock()
	defer m.applyMu.Unlock()
	docs, err := m.docs()
	if err != nil {
		return
	}
	for _, d := range docs {
		if d.Namespace == rt.Runtime.DefaultNamespace {
			m.apply(rt, d)
		}
	}
}

func (m *MetaStore) apply(rt *KdbServerRuntime, d metaDoc) {
	rt.metaApplying.Store(true)
	defer rt.metaApplying.Store(false)
	var err error
	switch d.Kind {
	case "schema":
		err = m.applySchema(rt, d)
	case "index":
		err = m.applyIndex(rt, d)
	}
	if err != nil {
		_, _ = rt.Conflicts.Record(peersync.ConflictEntry{
			ID:        peersync.ConflictID("meta-apply", d.Namespace, d.Kind, d.Name),
			Kind:      "meta-apply",
			Namespace: d.Namespace,
			Detail:    fmt.Sprintf("replicated %s definition %q could not be applied here: %v", d.Kind, d.Name, err),
		})
		return
	}
	_ = rt.Conflicts.Remove(peersync.ConflictID("meta-apply", d.Namespace, d.Kind, d.Name))
}

func (m *MetaStore) applySchema(rt *KdbServerRuntime, d metaDoc) error {
	raw, err := hex.DecodeString(d.Schema)
	if err != nil {
		return err
	}
	sch, err := schema.FromBytes(raw)
	if err != nil {
		return err
	}
	if rt.Schema().SchemaHash == sch.SchemaHash {
		return nil
	}
	return rt.SetSchemaChecked(sch)
}

func (m *MetaStore) applyIndex(rt *KdbServerRuntime, d metaDoc) error {
	p, ok := rt.SQLIndexProvider().(*RegistryIndexProvider)
	if !ok {
		return nil // this namespace runs without indexes; nothing to apply
	}
	var existing *index.Descriptor
	for _, desc := range p.registry.Descriptors() {
		if desc.IndexName() == d.Name {
			dd := desc
			existing = &dd
		}
	}
	ctx := sql.QueryContext{NamespaceID: d.Namespace}
	if d.Dropped {
		if existing == nil {
			return nil
		}
		return p.DropIndex(sql.StmtDropIndex{Name: d.Name}, ctx)
	}
	stmt := sql.StmtCreateIndex{Name: d.Name, Table: d.Table, Fields: d.Fields, Using: d.Using, Unique: d.Unique, With: d.With}
	want, err := descriptorFor(stmt, d.Namespace)
	if err != nil {
		return err
	}
	if existing != nil {
		if sameIndex(*existing, want) {
			return nil
		}
		if err := p.DropIndex(sql.StmtDropIndex{Name: d.Name}, ctx); err != nil {
			return err
		}
	}
	return p.CreateIndex(stmt, ctx)
}

func sameIndex(a, b index.Descriptor) bool {
	return reflect.DeepEqual(a.Fields, b.Fields) && a.Type == b.Type && a.Unique == b.Unique &&
		reflect.DeepEqual(a.Options, b.Options)
}

// systemUpsert writes one document as the runtime itself, bypassing RBAC - definitions are
// recorded because a principal already had the right to change them.
func (s *KdbServerRuntime) systemUpsert(docID codec.UUID, body string) (document.Commit, error) {
	return s.systemCommit([]document.Op{document.WriteOp{DocID: docID, Patch: body}}, "kdb:meta")
}

// systemCommit commits ops as the runtime itself: no RBAC, and allowed on a namespace that
// refuses client writes (a projection).
func (s *KdbServerRuntime) systemCommit(ops []document.Op, message string) (document.Commit, error) {
	head, err := s.Runtime.DAG.Head()
	if err != nil {
		return document.Commit{}, err
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		return document.Commit{}, err
	}
	defer s.pinBaseTree(head)()
	tx := s.authored(document.Transaction{
		ID: txID, BaseVersion: head, Timestamp: codec.TimestampNow(), Operations: ops,
	})
	return s.runTransaction(tx, auth.Principal{}, txOptions{system: true}, func() (transactionResult, error) {
		return s.UpsertEngine.Commit(tx, s.dag, s.Runtime.Storage, s.Schema(), nil, message)
	})
}
