package control

import (
	"net/http"
	"sort"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/server"
)

// The index half of §9 screen 8.
//
// Schema was built; indexes were not, and the schema view already hints at them - a field carries
// "indexed" and "unique" flags. Those are the schema's *intent*. This is what actually exists: the
// registry's descriptors, which are a different thing and can disagree with the schema (an index
// created through SQL is not in the schema, and a schema field marked indexed whose store failed to
// open is not in the registry).
//
// The operationally interesting field is none of the descriptor's own: it is how far behind head
// the registry is. An index is written on the commit path and snapshotted every 64 commits
// (indexFlushEvery), so "current" is a question with a real answer, and a query planned against a
// stale index is the kind of thing that is invisible until someone notices wrong results.

func (s *Server) handleIndexes(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	provider, ok := rt.SQLIndexProvider().(*server.RegistryIndexProvider)
	if !ok || provider == nil || provider.Registry() == nil {
		// A runtime with no index provider is the pre-Layer-16 behaviour, not a failure: every
		// lookup is a full scan. Said plainly, because "no indexes" and "indexes unavailable" would
		// otherwise look the same.
		writeJSON(w, http.StatusOK, map[string]any{
			"namespace": ns, "indexes": []any{}, "available": false,
			"note": "this namespace has no index registry, so every lookup is a full scan and the " +
				"search functions are planning errors. That is the engine's behaviour without " +
				"indexes, not a fault.",
		})
		return
	}
	registry := provider.Registry()

	type view struct {
		Name   string   `json:"name"`
		Field  string   `json:"field"`
		Fields []string `json:"fields,omitempty"`
		Type   string   `json:"type"`
		Unique bool     `json:"unique"`
		// SchemaVersion is the schema this index was built against. A mismatch with the live
		// schema is what a rebuild fixes.
		SchemaVersion int               `json:"schemaVersion"`
		IndexID       string            `json:"indexId"`
		Options       map[string]string `json:"options,omitempty"`
	}

	descriptors := registry.Descriptors()
	out := make([]view, 0, len(descriptors))
	for _, d := range descriptors {
		name := d.Options["index_name"]
		if name == "" {
			name = d.FieldName
		}
		v := view{
			Name: name, Field: d.FieldName, Type: d.Type.String(), Unique: d.Unique,
			SchemaVersion: d.SchemaVersion, IndexID: d.IndexID.String(),
		}
		if len(d.Fields) > 1 {
			v.Fields = append([]string(nil), d.Fields...)
		}
		if len(d.Options) > 0 {
			v.Options = d.Options
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	body := map[string]any{
		"namespace": ns,
		"indexes":   out,
		"available": true,
	}

	// How current the registry is. This is the question the descriptors cannot answer and the one
	// that matters: an index behind head plans queries against documents that have moved.
	if head, ok := registry.Head(); ok {
		body["indexedThrough"] = head.Hex()
		body["indexedThroughShort"] = shortHash(head.Hex())
		if d, err := s.commitDAGFor(rt); err == nil {
			if dagHead, err := d.Head(); err == nil {
				body["head"] = dagHead.Hex()
				current := dagHead == head
				body["current"] = current
				if !current {
					body["note"] = "the registry is not at head. Indexes are updated on the commit " +
						"path, so this is normally a snapshot lag rather than missing data - what " +
						"is not yet snapshotted is replayed from the log on the next open."
				}
			}
		}
	} else if len(out) > 0 {
		body["note"] = "these indexes exist but have not been marked at any commit yet: nothing " +
			"has been written since they were created."
	}
	if len(out) == 0 {
		body["note"] = "no indexes are defined. CREATE INDEX through the SQL console adds one; " +
			"until then every lookup is a full scan, which is correct and simply slower."
	}
	writeJSON(w, http.StatusOK, body)
}
