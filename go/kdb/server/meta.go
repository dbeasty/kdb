package server

import (
	"context"
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
	// seen holds, per definition document, the body this node last applied or recorded.
	// Reconciliation applies only a version it has not seen: a definition changed locally is
	// recorded before anything reconciles, and a replicated one is applied once - so no
	// reconciliation pass can put an older version back over a newer local one.
	seenMu sync.Mutex
	seen   map[codec.UUID]string
}

// metaDoc is one definition. Kind "schema" carries Schema (hex of schema.ToBytes); kind "index"
// carries the CREATE INDEX statement that defines it, or Dropped for an index removed since - a
// tombstone rather than a deletion, so a node reconciling knows to drop it rather than merely not
// knowing about it.
type metaDoc struct {
	// id and raw are the document's id and body as stored; not part of the JSON.
	id  codec.UUID
	raw string

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
	// Home is a single-home assignment (kind "home"); an empty Node means multi-leader.
	Home *Home `json:"home,omitempty"`
}

func metaSchemaID(ns string) codec.UUID { return codec.DerivedUUID("kdb:meta/schema/" + ns) }
func metaHomeID(ns string) codec.UUID   { return codec.DerivedUUID("kdb:meta/home/" + ns) }
func metaIndexID(ns, name string) codec.UUID {
	return codec.DerivedUUID("kdb:meta/index/" + ns + "/" + name)
}

// NewMetaStore returns a store over the meta runtime, applying to the namespaces in set, and wires
// every commit the meta namespace takes - local or replicated - to a reconciliation pass.
func NewMetaStore(meta *KdbServerRuntime, set *NamespaceSet) *MetaStore {
	m := &MetaStore{meta: meta, set: set, kick: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}), seen: map[codec.UUID]string{}}
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
	m.markSeen(id, string(body))
	// Unchanged definitions write nothing: a no-op commit would still be a commit every peer
	// then has to fetch.
	if cur, _, found, err := m.meta.GetDocument(MetaNamespace, id); err == nil && found && cur == string(body) {
		return nil
	}
	_, err = m.meta.systemUpsert(id, string(body))
	return err
}

func (m *MetaStore) markSeen(id codec.UUID, body string) {
	m.seenMu.Lock()
	m.seen[id] = body
	m.seenMu.Unlock()
}

// fresh reports whether body is a version of id this node has not yet applied or recorded.
func (m *MetaStore) fresh(id codec.UUID, body string) bool {
	m.seenMu.Lock()
	defer m.seenMu.Unlock()
	return m.seen[id] != body
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

// AssignHome makes node (reachable for clients at addr) the only node that accepts ns's writes,
// or with an empty node returns ns to multi-leader. Every assignment raises the fence past the
// last one this node knows of, so commits the previous home makes after it has been replaced are
// refused wherever they arrive.
//
// Assignments are made on one node and replicate like any definition; making two assignments on
// two nodes at once is a same-document conflict in the metadata namespace, surfaced for an
// operator rather than resolved by whichever arrives last.
func (m *MetaStore) AssignHome(ns, node, addr string) (Home, error) {
	if m == nil {
		return Home{}, fmt.Errorf("no metadata namespace: single-home ownership needs one")
	}
	h := Home{Node: node, Addr: addr, Fence: 1}
	if cur, _, found, err := m.meta.GetDocument(MetaNamespace, metaHomeID(ns)); err == nil && found {
		var d metaDoc
		if json.Unmarshal([]byte(cur), &d) == nil && d.Home != nil {
			h.Fence = d.Home.Fence + 1
		}
	}
	if rt, ok := m.set.Get(ns); ok && node != "" {
		// Stop this node taking writes under the old assignment, let any already admitted finish,
		// then take the head as the handover point: everything written here before the
		// assignment is in its history.
		stopping := h
		stopping.Since = "" // no handover point yet: nothing to wait for if this node is the new home
		rt.SetHome(&stopping)
		release, err := rt.writeGate.acquire(context.Background())
		if err != nil {
			return Home{}, err
		}
		head, herr := rt.dag.Head()
		release()
		if herr != nil {
			return Home{}, herr
		}
		h.Since = head.Hex()
	}
	if err := m.put(metaHomeID(ns), metaDoc{Kind: "home", Namespace: ns, Home: &h}); err != nil {
		return Home{}, err
	}
	// Applied here at once, not only when the reconciler gets to it: a caller that just moved
	// the home must not be able to write to the old one a moment later on this node.
	if rt, ok := m.set.Get(ns); ok {
		m.apply(rt, metaDoc{Kind: "home", Namespace: ns, Home: &h})
	}
	return h, nil
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
				md.id, md.raw = d.ID, d.JSON
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
	if d.raw != "" && !m.fresh(d.id, d.raw) {
		return
	}
	var err error
	switch d.Kind {
	case "schema":
		err = m.applySchema(rt, d)
	case "index":
		err = m.applyIndex(rt, d)
	case "home":
		if d.Home == nil || d.Home.Node == "" {
			rt.SetHome(nil)
		} else {
			h := *d.Home
			rt.SetHome(&h)
		}
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
	if d.raw != "" {
		m.markSeen(d.id, d.raw)
	}
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
		return p.dropIndexLocal(sql.StmtDropIndex{Name: d.Name}, ctx)
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
		if err := p.dropIndexLocal(sql.StmtDropIndex{Name: d.Name}, ctx); err != nil {
			return err
		}
	}
	return p.createIndexLocal(stmt, ctx)
}

func sameIndex(a, b index.Descriptor) bool {
	return reflect.DeepEqual(a.Fields, b.Fields) && a.Type == b.Type && a.Unique == b.Unique &&
		reflect.DeepEqual(a.Options, b.Options)
}

// systemUpsert replaces one document as the runtime itself, bypassing RBAC - definitions are
// recorded because a principal already had the right to change them. Replaced, not merged: a
// record's omitted fields (dropped=false, no handover point) must not keep their old values.
func (s *KdbServerRuntime) systemUpsert(docID codec.UUID, body string) (document.Commit, error) {
	return s.systemCommit(replacing([]document.Op{document.WriteOp{DocID: docID, Patch: body}}), "kdb:meta")
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

// Placement lists every namespace this process knows a single-home assignment for.
func (m *MetaStore) Placement() (map[string]Home, error) {
	if m == nil {
		return nil, nil
	}
	m.applyMu.Lock()
	defer m.applyMu.Unlock()
	docs, err := m.docs()
	if err != nil {
		return nil, err
	}
	out := map[string]Home{}
	for _, d := range docs {
		if d.Kind == "home" && d.Home != nil && d.Home.Node != "" {
			out[d.Namespace] = *d.Home
		}
	}
	return out, nil
}
