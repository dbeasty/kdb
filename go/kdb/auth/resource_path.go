package auth

import "strings"

// ResourcePath is a structured view of a KDB resource address: database, optionally scoped
// down to a collection and a single document. Storage/transaction code keeps addressing
// resources with a flat namespaceId string ("database" or "database/collection"); this type
// exists only in the authorization layer, which needs to resolve grants at database, collection,
// and document granularity. See docs/kdb-rbac-plan.md.
type ResourcePath struct {
	Database   string
	Collection string // empty if not scoped to a collection
	DocumentID string // empty if not scoped to a document
}

func NewResourcePath(namespace string, documentID string) ResourcePath {
	if idx := strings.IndexByte(namespace, '/'); idx >= 0 {
		return ResourcePath{
			Database:   namespace[:idx],
			Collection: namespace[idx+1:],
			DocumentID: documentID,
		}
	}
	return ResourcePath{Database: namespace, DocumentID: documentID}
}

func (r ResourcePath) NamespaceID() string {
	if r.Collection == "" {
		return r.Database
	}
	return r.Database + "/" + r.Collection
}

// CandidatePaths returns grant-match candidates, most specific first: document, then
// collection, then database. A database-level grant covers every collection and document
// beneath it, and a collection-level grant covers every document in it.
func (r ResourcePath) CandidatePaths() []string {
	candidates := make([]string, 0, 3)
	if r.Collection != "" && r.DocumentID != "" {
		candidates = append(candidates, r.Database+"/"+r.Collection+"/"+r.DocumentID)
	}
	if r.Collection != "" {
		candidates = append(candidates, r.Database+"/"+r.Collection)
	}
	candidates = append(candidates, r.Database)
	return candidates
}
