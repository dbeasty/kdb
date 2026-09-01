package embed

import (
	"testing"
)

// TestFileAuthRegistrySurvivesRestart is Phase 2.7's core requirement: a user created against
// a --data-dir deployment must still exist - and authenticate, with roles and grants intact -
// after the process restarts. Unlike kdb/auth's own restart test (which simulates replay
// in-memory), this goes through the real file-backed delta log: open, write, close, reopen from
// disk.
func TestFileAuthRegistrySurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	reg1, err := OpenFileAuthRegistryExclusive(dir)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if err := reg1.Store.CreateRole("admin", []string{"read:app/*", "sync:app/*"}); err != nil {
		t.Fatalf("create role: %v", err)
	}
	if err := reg1.Store.CreateUser("alice", "s3cret", []string{"admin"}); err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := reg1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reg2, err := OpenFileAuthRegistryExclusive(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reg2.Close()

	user, err := reg2.Store.GetUser("alice")
	if err != nil {
		t.Fatalf("get user after restart: %v", err)
	}
	if user == nil {
		t.Fatal("alice did not survive restart")
	}
	if len(user.Roles) != 1 || user.Roles[0] != "admin" {
		t.Fatalf("roles after restart: %+v", user.Roles)
	}
	ok, err := reg2.Store.VerifyPassword("alice", "s3cret")
	if err != nil || !ok {
		t.Fatalf("password no longer verifies after restart: ok=%v err=%v", ok, err)
	}
	if ok, _ := reg2.Store.VerifyPassword("alice", "wrong"); ok {
		t.Fatal("wrong password verified")
	}
	grants, err := reg2.Store.GrantsByRole()
	if err != nil {
		t.Fatalf("grants: %v", err)
	}
	if len(grants["admin"]) != 2 {
		t.Fatalf("admin grants after restart: %+v", grants)
	}
}

// TestFileAuthRegistryExclusiveRespectsDirLock: the bootstrap CLI must not race a running
// service - the directory lock refuses a second exclusive open.
func TestFileAuthRegistryExclusiveRespectsDirLock(t *testing.T) {
	dir := t.TempDir()
	reg, err := OpenFileAuthRegistryExclusive(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer reg.Close()

	if _, err := OpenFileAuthRegistryExclusive(dir); err == nil {
		t.Fatal("second exclusive open should fail while the lock is held")
	}
}
