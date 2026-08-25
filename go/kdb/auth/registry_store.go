package auth

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Reserved namespaces for the RBAC registry, matching kdb-auth-store's RegistryAuthStore.kt
// (dev.kdb.auth.store.RegistryAuthStore.USERS_NAMESPACE/ROLES_NAMESPACE).
const (
	UsersNamespace = "_system/users"
	RolesNamespace = "_system/roles"
)

// UserRecord is a persisted user. PasswordHash/PasswordSalt are hex-encoded PBKDF2 output (see
// HashPassword) - never expose these fields outside this package.
type UserRecord struct {
	ID           string   `json:"id"`
	PasswordHash string   `json:"passwordHash"`
	PasswordSalt string   `json:"passwordSalt"`
	Roles        []string `json:"roles"`
}

// RoleRecord is a persisted named grant bundle.
type RoleRecord struct {
	Name   string   `json:"name"`
	Grants []string `json:"grants"`
}

// RegistryAuthStore persists users/roles as documents inside KDB itself (written through the
// DAG, not a static config file), matching kdb-auth-store's RegistryAuthStore.kt so a user
// created against a Go deployment is durable the same way as a Kotlin one and a password hash
// verifies against both (component 38 spec §5).
//
// userDag/roleDag are the dag.CommitDAG interface, not the concrete *dag.InMemoryCommitDag
// transaction.Engine.Commit requires - deliberately, so this store works unmodified against a
// file-backed *embed.PersistingCommitDAG (test 9's restart-durability requirement) without
// needing transaction.Engine's conflict-detection machinery at all. That's an intentional scope
// choice, not an oversight: a single-writer admin registry has no real concurrent-write scenario
// worth optimistic-concurrency conflict detection for (matching Kotlin's own choice of
// ConflictPolicy.LAST_WRITE here) - each write just appends directly via dag.AppendCommit.
type RegistryAuthStore struct {
	userDag      dag.CommitDAG
	roleDag      dag.CommitDAG
	storage      storage.Adapter
	authorNodeID codec.UUID

	mu sync.Mutex
}

// NewRegistryAuthStore builds a registry store over userDag/roleDag (each single-namespace, per
// this codebase's CommitDAG convention - see UsersNamespace/RolesNamespace) sharing storage.
func NewRegistryAuthStore(userDag, roleDag dag.CommitDAG, store storage.Adapter) (*RegistryAuthStore, error) {
	authorNodeID, err := codec.RandomUUID()
	if err != nil {
		return nil, err
	}
	return &RegistryAuthStore{userDag: userDag, roleDag: roleDag, storage: store, authorNodeID: authorNodeID}, nil
}

var (
	// ErrUserAlreadyExists is returned by CreateUser for a duplicate id.
	ErrUserAlreadyExists = fmt.Errorf("user already exists")
	// ErrUserNotFound is returned by user-mutating calls for an unknown id.
	ErrUserNotFound = fmt.Errorf("user not found")
	// ErrRoleAlreadyExists is returned by CreateRole for a duplicate name.
	ErrRoleAlreadyExists = fmt.Errorf("role already exists")
	// ErrRoleNotFound is returned by role-mutating calls for an unknown name.
	ErrRoleNotFound = fmt.Errorf("role not found")
)

// CreateUser hashes password (see HashPassword) and persists a new user record.
func (s *RegistryAuthStore) CreateUser(id, password string, roles []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, err := s.readUser(id); err != nil {
		return err
	} else if existing != nil {
		return fmt.Errorf("%w: %s", ErrUserAlreadyExists, id)
	}
	hashHex, saltHex, err := HashPassword(password)
	if err != nil {
		return err
	}
	if roles == nil {
		roles = []string{}
	}
	return s.writeUser(UserRecord{ID: id, PasswordHash: hashHex, PasswordSalt: saltHex, Roles: roles})
}

// GetUser returns the user record for id, or nil if unknown.
func (s *RegistryAuthStore) GetUser(id string) (*UserRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readUser(id)
}

// VerifyPassword reports whether password matches id's stored hash. False for unknown users too,
// so callers can't distinguish "wrong password" from "no such user" via this check alone.
func (s *RegistryAuthStore) VerifyPassword(id, password string) (bool, error) {
	s.mu.Lock()
	record, err := s.readUser(id)
	s.mu.Unlock()
	if err != nil {
		return false, err
	}
	if record == nil {
		return false, nil
	}
	return VerifyPassword(password, record.PasswordHash, record.PasswordSalt), nil
}

// DeleteUser removes a user record.
func (s *RegistryAuthStore) DeleteUser(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, err := s.readUser(id)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("%w: %s", ErrUserNotFound, id)
	}
	return s.deleteDoc(s.userDag, UsersNamespace, userDocID(id))
}

// AssignRole adds role to id's role set (a no-op if already present).
func (s *RegistryAuthStore) AssignRole(id, role string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, err := s.readUser(id)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("%w: %s", ErrUserNotFound, id)
	}
	existing.Roles = addUnique(existing.Roles, role)
	return s.writeUser(*existing)
}

// RevokeRole removes role from id's role set (a no-op if not present).
func (s *RegistryAuthStore) RevokeRole(id, role string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, err := s.readUser(id)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("%w: %s", ErrUserNotFound, id)
	}
	existing.Roles = removeAll(existing.Roles, role)
	return s.writeUser(*existing)
}

// CreateRole persists a new named grant bundle.
func (s *RegistryAuthStore) CreateRole(name string, grants []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, err := s.readRole(name); err != nil {
		return err
	} else if existing != nil {
		return fmt.Errorf("%w: %s", ErrRoleAlreadyExists, name)
	}
	if grants == nil {
		grants = []string{}
	}
	return s.writeRole(RoleRecord{Name: name, Grants: grants})
}

// GetRole returns the role record for name, or nil if unknown.
func (s *RegistryAuthStore) GetRole(name string) (*RoleRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readRole(name)
}

// UpdateGrants replaces name's grant set.
func (s *RegistryAuthStore) UpdateGrants(name string, grants []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, err := s.readRole(name)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("%w: %s", ErrRoleNotFound, name)
	}
	return s.writeRole(RoleRecord{Name: name, Grants: grants})
}

// DeleteRole removes a role record.
func (s *RegistryAuthStore) DeleteRole(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, err := s.readRole(name)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("%w: %s", ErrRoleNotFound, name)
	}
	return s.deleteDoc(s.roleDag, RolesNamespace, roleDocID(name))
}

// GrantsByRole returns roleName -> grants, the shape PrincipalHasPermission expects.
func (s *RegistryAuthStore) GrantsByRole() (map[string][]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	treeHash, ok, err := s.currentTreeHash(s.roleDag)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]string)
	if !ok {
		return out, nil
	}
	err = s.storage.ScanDocuments(RolesNamespace, treeHash, 256, func(docs []document.Document) error {
		for _, doc := range docs {
			var rec RoleRecord
			if err := json.Unmarshal([]byte(doc.JSON), &rec); err != nil {
				return err
			}
			out[rec.Name] = rec.Grants
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *RegistryAuthStore) readUser(id string) (*UserRecord, error) {
	treeHash, ok, err := s.currentTreeHash(s.userDag)
	if err != nil || !ok {
		return nil, err
	}
	doc, err := s.storage.GetDocument(UsersNamespace, userDocID(id), treeHash)
	if err != nil || doc == nil {
		return nil, err
	}
	var rec UserRecord
	if err := json.Unmarshal([]byte(doc.JSON), &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (s *RegistryAuthStore) readRole(name string) (*RoleRecord, error) {
	treeHash, ok, err := s.currentTreeHash(s.roleDag)
	if err != nil || !ok {
		return nil, err
	}
	doc, err := s.storage.GetDocument(RolesNamespace, roleDocID(name), treeHash)
	if err != nil || doc == nil {
		return nil, err
	}
	var rec RoleRecord
	if err := json.Unmarshal([]byte(doc.JSON), &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// currentTreeHash resolves the head commit's document tree. ok is false before the first commit
// (empty registry) - storage indexes documents by tree hash, not commit hash, per the maturity
// audit's note that these differ.
func (s *RegistryAuthStore) currentTreeHash(d dag.CommitDAG) (codec.Hash, bool, error) {
	head, err := d.Head()
	if err != nil {
		return codec.Hash{}, false, err
	}
	commit, ok := d.GetCommit(head)
	if !ok {
		return codec.Hash{}, false, nil
	}
	return commit.DocumentTreeHash, true, nil
}

func (s *RegistryAuthStore) writeUser(record UserRecord) error {
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return s.commitWrite(s.userDag, UsersNamespace, userDocID(record.ID), string(body))
}

func (s *RegistryAuthStore) writeRole(record RoleRecord) error {
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return s.commitWrite(s.roleDag, RolesNamespace, roleDocID(record.Name), string(body))
}

func (s *RegistryAuthStore) commitWrite(d dag.CommitDAG, namespaceID string, docID codec.UUID, jsonBody string) error {
	head, err := d.Head()
	if err != nil {
		return err
	}
	headCommit, err := d.GetCommitOrThrow(head)
	if err != nil {
		return err
	}
	doc, err := document.FromJSONWithID(docID, jsonBody)
	if err != nil {
		return err
	}
	if err := s.storage.PutDocument(namespaceID, doc); err != nil {
		return err
	}
	newTree, err := s.storage.CommitTree(namespaceID, headCommit.DocumentTreeHash)
	if err != nil {
		return err
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		return err
	}
	tx := document.Transaction{
		ID:           txID,
		BaseVersion:  head,
		Operations:   []document.Op{document.WriteOp{DocID: docID, Patch: jsonBody}},
		Timestamp:    codec.TimestampNow(),
		AuthorNodeID: s.authorNodeID,
	}
	_, err = d.AppendCommit(tx, head, newTree, nil, "")
	return err
}

func (s *RegistryAuthStore) deleteDoc(d dag.CommitDAG, namespaceID string, docID codec.UUID) error {
	head, err := d.Head()
	if err != nil {
		return err
	}
	headCommit, err := d.GetCommitOrThrow(head)
	if err != nil {
		return err
	}
	if err := s.storage.DeleteDocument(namespaceID, docID); err != nil {
		return err
	}
	newTree, err := s.storage.CommitTree(namespaceID, headCommit.DocumentTreeHash)
	if err != nil {
		return err
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		return err
	}
	tx := document.Transaction{
		ID:           txID,
		BaseVersion:  head,
		Operations:   []document.Op{document.DeleteOp{DocID: docID}},
		Timestamp:    codec.TimestampNow(),
		AuthorNodeID: s.authorNodeID,
	}
	_, err = d.AppendCommit(tx, head, newTree, nil, "")
	return err
}

func userDocID(id string) codec.UUID {
	return deterministicDocID("user:" + id)
}

func roleDocID(name string) codec.UUID {
	return deterministicDocID("role:" + name)
}

func deterministicDocID(key string) codec.UUID {
	digest := document.SHA256Digest([]byte(key))
	id, _ := codec.UUIDFromBytes(digest[:16])
	return id
}

func addUnique(list []string, v string) []string {
	for _, existing := range list {
		if existing == v {
			return list
		}
	}
	return append(append([]string{}, list...), v)
}

func removeAll(list []string, v string) []string {
	out := make([]string, 0, len(list))
	for _, existing := range list {
		if existing != v {
			out = append(out, existing)
		}
	}
	return out
}
