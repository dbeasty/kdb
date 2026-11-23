// Package docdb is a small document API for embedding KDB in a Go service: open a directory, then
// get, put, merge and delete JSON documents by id from as many goroutines as you like.
//
// It is the path kdb-service's own clients take, minus the network: every write goes through the
// server runtime's write gate (server.KdbServerRuntime), so concurrent writers are ordered, a
// merge applies to the document as it is when the write lands, and the work that can be done off
// the gate (validation, the merge, the version's hashing) is. embed.PutJSONDocument, the older
// entry point, does none of that and is not safe for concurrent callers.
//
// It adds no dependency: it is embed and server, both already in the embeddable source bundle
// (go/embedbundle/entrypoints.txt lists it too). For anything beyond these operations - SQL,
// transactions, history, peer sync - DB.Runtime returns the server runtime underneath.
package docdb

import (
	"errors"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// Options configure Open. The zero value opens with the embedded runtime's defaults: durable
// (synced) writes and a 64 MiB document cache.
type Options struct {
	// Runtime is passed to embed.OpenFileRuntimeWithOptions: durability, cache budget, history
	// mode and the rest. embed.FileRuntimeOptionsFromEnv() reads them from KDB_* variables.
	Runtime embed.FileRuntimeOptions
	// Schema validates every write; nil (schema.None()) accepts any JSON object.
	Schema *schema.KdbSchema
	// Principal is who writes are attributed to and authorized as. The zero value is anonymous,
	// which the default (allow-all) authorization accepts.
	Principal auth.Principal
}

// DB is one open namespace. Its methods are safe for concurrent use.
type DB struct {
	rt        *server.KdbServerRuntime
	ns        string
	principal auth.Principal
	// mu is held shared by every operation and exclusively by Close, which so waits for the
	// operations in flight and leaves none running against a closed runtime.
	mu     sync.RWMutex
	closed bool
}

// ErrClosed is returned by every method of a closed DB.
var ErrClosed = errors.New("docdb: closed")

// Open opens (creating if needed) namespace in the data directory dir. A namespace is a
// slash-separated id such as "app/users"; its first segment names the catalog.
func Open(dir, namespace string, opts Options) (*DB, error) {
	catalog, _, _ := strings.Cut(namespace, "/")
	sch := schema.None()
	if opts.Schema != nil {
		sch = *opts.Schema
	}
	rt, err := embed.OpenFileRuntimeWithOptions(dir, catalog, namespace, sch, opts.Runtime)
	if err != nil {
		return nil, err
	}
	return &DB{rt: server.NewKdbServerRuntime(rt), ns: namespace, principal: opts.Principal}, nil
}

// Get returns the document stored under id at the current head.
func (db *DB) Get(id codec.UUID) (json string, found bool, err error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return "", false, ErrClosed
	}
	json, _, found, err = db.rt.GetDocument(db.ns, id)
	return json, found, err
}

// Put stores json - a JSON object - as the whole document id, replacing whatever was there, and
// returns the commit that recorded it. Concurrent writes to one document are ordered; the last
// one wins.
func (db *DB) Put(id codec.UUID, json string) (codec.Hash, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return codec.Hash{}, ErrClosed
	}
	c, err := db.rt.Replace(db.ns, id, json, db.principal)
	return c.Hash, err
}

// Merge sets the top-level fields of patch - a JSON object - on document id, keeping the fields
// patch does not mention, and creates the document from patch if it does not exist. The merge
// applies to the document as it is when the write lands, so concurrent merges of different
// fields all survive.
func (db *DB) Merge(id codec.UUID, patch string) (codec.Hash, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return codec.Hash{}, ErrClosed
	}
	c, err := db.rt.Upsert(db.ns, id, patch, db.principal)
	return c.Hash, err
}

// Delete removes document id. Deleting a document that does not exist is not an error.
func (db *DB) Delete(id codec.UUID) (codec.Hash, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return codec.Hash{}, ErrClosed
	}
	c, err := db.rt.Delete(db.ns, id, db.principal)
	return c.Hash, err
}

// Runtime is the server runtime underneath, for everything this package does not wrap. It must
// not be used after Close.
func (db *DB) Runtime() *server.KdbServerRuntime { return db.rt }

// Close waits for the operations in flight, then flushes and closes the namespace. Calls after
// Close return ErrClosed; closing twice is harmless.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	db.rt.Release()
	return nil
}
