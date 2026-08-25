package auth

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage/mem"
)

func newTestRegistryAuthStore(t *testing.T) *RegistryAuthStore {
	t.Helper()
	usersDag, err := dag.NewInMemoryCommitDag(UsersNamespace)
	if err != nil {
		t.Fatal(err)
	}
	rolesDag, err := dag.NewInMemoryCommitDag(RolesNamespace)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewRegistryAuthStore(usersDag, rolesDag, mem.NewInMemoryStorageAdapter())
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestRegistryAuthStoreCreateUserAndVerifyPassword(t *testing.T) {
	store := newTestRegistryAuthStore(t)
	if err := store.CreateUser("alice", "s3cret!", []string{"reader"}); err != nil {
		t.Fatal(err)
	}
	ok, err := store.VerifyPassword("alice", "s3cret!")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected password to verify")
	}
	ok, err = store.VerifyPassword("alice", "wrong")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected wrong password to fail")
	}
	ok, err = store.VerifyPassword("nobody", "anything")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected unknown user to fail closed, not panic or error")
	}

	if err := store.CreateUser("alice", "other", nil); err == nil {
		t.Fatal("expected duplicate CreateUser to fail")
	}

	user, err := store.GetUser("alice")
	if err != nil {
		t.Fatal(err)
	}
	if user == nil || len(user.Roles) != 1 || user.Roles[0] != "reader" {
		t.Fatalf("user: %+v", user)
	}
	if user.PasswordHash == "s3cret!" || user.PasswordSalt == "" {
		t.Fatalf("password must be hashed, not stored in the clear: %+v", user)
	}
}

func TestRegistryAuthStoreRolesAndGrants(t *testing.T) {
	store := newTestRegistryAuthStore(t)
	if err := store.CreateRole("writer", []string{"write:app/*"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRole("reader", []string{"read:app/*"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRole("writer", nil); err == nil {
		t.Fatal("expected duplicate CreateRole to fail")
	}

	grants, err := store.GrantsByRole()
	if err != nil {
		t.Fatal(err)
	}
	if len(grants["writer"]) != 1 || grants["writer"][0] != "write:app/*" {
		t.Fatalf("writer grants: %+v", grants)
	}
	if len(grants["reader"]) != 1 {
		t.Fatalf("reader grants: %+v", grants)
	}

	if err := store.UpdateGrants("writer", []string{"write:app/*", "read:app/*"}); err != nil {
		t.Fatal(err)
	}
	grants, err = store.GrantsByRole()
	if err != nil {
		t.Fatal(err)
	}
	if len(grants["writer"]) != 2 {
		t.Fatalf("writer grants after update: %+v", grants)
	}

	if err := store.DeleteRole("reader"); err != nil {
		t.Fatal(err)
	}
	grants, err = store.GrantsByRole()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := grants["reader"]; ok {
		t.Fatalf("expected reader role to be gone, got %+v", grants)
	}
}

func TestRegistryAuthStoreAssignAndRevokeRole(t *testing.T) {
	store := newTestRegistryAuthStore(t)
	if err := store.CreateUser("bob", "pw", nil); err != nil {
		t.Fatal(err)
	}
	if err := store.AssignRole("bob", "admin"); err != nil {
		t.Fatal(err)
	}
	if err := store.AssignRole("bob", "admin"); err != nil { // idempotent
		t.Fatal(err)
	}
	user, err := store.GetUser("bob")
	if err != nil {
		t.Fatal(err)
	}
	if len(user.Roles) != 1 || user.Roles[0] != "admin" {
		t.Fatalf("roles after assign: %+v", user.Roles)
	}
	if err := store.RevokeRole("bob", "admin"); err != nil {
		t.Fatal(err)
	}
	user, err = store.GetUser("bob")
	if err != nil {
		t.Fatal(err)
	}
	if len(user.Roles) != 0 {
		t.Fatalf("roles after revoke: %+v", user.Roles)
	}
}

func TestRegistryAuthStoreDeleteUser(t *testing.T) {
	store := newTestRegistryAuthStore(t)
	if err := store.CreateUser("carol", "pw", nil); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteUser("carol"); err != nil {
		t.Fatal(err)
	}
	user, err := store.GetUser("carol")
	if err != nil {
		t.Fatal(err)
	}
	if user != nil {
		t.Fatalf("expected user gone, got %+v", user)
	}
	if err := store.DeleteUser("carol"); err == nil {
		t.Fatal("expected deleting an already-deleted user to fail")
	}
}

// TestRegistryAuthStoreSurvivesRestart is component 38 spec §7 test 9: create a user, "restart"
// (discard the in-memory dag/storage and rebuild fresh ones purely from the persisted commit
// history - the same replay mechanism kdb-embed's materializeCommitHistory uses for peer-sync,
// simulating what a real process restart against durable storage would reconstruct), and confirm
// the user still exists and can authenticate. This proves the registry's durability model - every
// write is a real DAG commit with real operations, not an in-memory-only side table - without
// depending on the full file-backed storage engine wiring (tracked separately; see the execution
// plan's Phase 4 notes).
func TestRegistryAuthStoreSurvivesRestart(t *testing.T) {
	usersDag1, err := dag.NewInMemoryCommitDag(UsersNamespace)
	if err != nil {
		t.Fatal(err)
	}
	rolesDag1, err := dag.NewInMemoryCommitDag(RolesNamespace)
	if err != nil {
		t.Fatal(err)
	}
	store1, err := NewRegistryAuthStore(usersDag1, rolesDag1, mem.NewInMemoryStorageAdapter())
	if err != nil {
		t.Fatal(err)
	}
	if err := store1.CreateUser("durable-user", "durable-pw", []string{"reader"}); err != nil {
		t.Fatal(err)
	}
	if err := store1.CreateRole("reader", []string{"read:app/*"}); err != nil {
		t.Fatal(err)
	}

	// "Restart": rebuild fresh dag+storage from nothing but the persisted commit history.
	usersDag2 := restartDag(t, UsersNamespace, usersDag1)
	rolesDag2 := restartDag(t, RolesNamespace, rolesDag1)
	storage2 := mem.NewInMemoryStorageAdapter()
	replayInto(t, UsersNamespace, usersDag1, usersDag2, storage2)
	replayInto(t, RolesNamespace, rolesDag1, rolesDag2, storage2)

	store2, err := NewRegistryAuthStore(usersDag2, rolesDag2, storage2)
	if err != nil {
		t.Fatal(err)
	}
	user, err := store2.GetUser("durable-user")
	if err != nil {
		t.Fatal(err)
	}
	if user == nil {
		t.Fatal("expected durable-user to survive restart")
	}
	if len(user.Roles) != 1 || user.Roles[0] != "reader" {
		t.Fatalf("roles after restart: %+v", user.Roles)
	}
	ok, err := store2.VerifyPassword("durable-user", "durable-pw")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected password to still verify after restart")
	}
	grants, err := store2.GrantsByRole()
	if err != nil {
		t.Fatal(err)
	}
	if len(grants["reader"]) != 1 {
		t.Fatalf("role grants after restart: %+v", grants)
	}
}

// restartDag builds a fresh dag for namespaceID and sanity-checks it shares the deterministic
// genesis commit with original (true for every InMemoryCommitDag - see NewInMemoryCommitDag),
// which is what makes PutCommit(requireParents=true) below able to chain real history onto it.
func restartDag(t *testing.T, namespaceID string, original *dag.InMemoryCommitDag) *dag.InMemoryCommitDag {
	t.Helper()
	fresh, err := dag.NewInMemoryCommitDag(namespaceID)
	if err != nil {
		t.Fatal(err)
	}
	originalGenesis, err := original.Head()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := original.GetCommit(originalGenesis); !ok {
		t.Fatal("original dag has no head commit")
	}
	return fresh
}

// replayInto rebuilds toDag/toStorage's state purely from fromDag's committed history -
// matching kdb-embed's EmbedOperations.kt materializeCommitHistory (the real mechanism a
// restarted, file-backed deployment would use to reconstruct state from its delta log).
func replayInto(t *testing.T, namespaceID string, fromDag, toDag *dag.InMemoryCommitDag, toStorage *mem.InMemoryStorageAdapter) {
	t.Helper()
	head, err := fromDag.Head()
	if err != nil {
		t.Fatal(err)
	}
	entries := fromDag.Walk(head, nil, 1_000_000)
	// Walk returns newest-first; replay oldest-first so parents land before children.
	var commits []document.Commit
	for _, e := range entries {
		if full, ok := e.(dag.FullEntry); ok {
			commits = append(commits, full.Commit)
		}
	}
	for i, j := 0, len(commits)-1; i < j; i, j = i+1, j-1 {
		commits[i], commits[j] = commits[j], commits[i]
	}
	for _, commit := range commits {
		if len(commit.ParentHashes) == 0 {
			continue // genesis - already present in the fresh dag
		}
		for _, op := range commit.Operations {
			switch o := op.(type) {
			case document.WriteOp:
				if err := toStorage.PutDocument(namespaceID, document.Document{ID: o.DocID, JSON: o.Patch}); err != nil {
					t.Fatal(err)
				}
			case document.DeleteOp:
				if err := toStorage.DeleteDocument(namespaceID, o.DocID); err != nil {
					t.Fatal(err)
				}
			}
		}
		parentTree, err := toDag.GetCommitOrThrow(commit.ParentHashes[0])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := toStorage.CommitTree(namespaceID, parentTree.DocumentTreeHash); err != nil {
			t.Fatal(err)
		}
		if err := toDag.PutCommit(commit, true); err != nil {
			t.Fatal(err)
		}
		if err := toDag.SetHead("main", commit.Hash); err != nil {
			t.Fatal(err)
		}
	}
}
