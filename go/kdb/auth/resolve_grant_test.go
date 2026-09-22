package auth

import "testing"

// TestResolveGrantMatchesLikeKotlin: the "resolve" kind settles conflicts a resolver authority
// owns (ConflictResolveAction). Grants are strings shared with the Kotlin tree - Kotlin's
// GRANT RESOLVE ON COLLECTION app.data produces "resolve:app/data" - so this mirrors
// PermissionMatchingTest.resolveGrantIsAnOrdinaryKindScopedLikeAnyOther case for case.
func TestResolveGrantMatchesLikeKotlin(t *testing.T) {
	roles := map[string][]string{"resolver": {"resolve:app/data"}, "writer": {"write:app/data"}}
	resolver := Principal{ID: "r", Roles: map[string]struct{}{"resolver": {}}}
	writer := Principal{ID: "w", Roles: map[string]struct{}{"writer": {}}}
	kind, res := actionToResource(ConflictResolveAction{Namespace: "app/data"})
	if kind != "resolve" || PermissionKind("resolve:app/data") != "resolve" {
		t.Fatalf("ConflictResolveAction must check the resolve kind, got %q", kind)
	}
	if !PrincipalHasPermission(resolver, roles, "resolve", res) {
		t.Fatal("resolve:app/data must cover app/data")
	}
	_, other := actionToResource(ConflictResolveAction{Namespace: "app/other"})
	if PrincipalHasPermission(resolver, roles, "resolve", other) {
		t.Fatal("resolve:app/data must not cover app/other")
	}
	if PrincipalHasPermission(resolver, roles, "write", res) {
		t.Fatal("resolve must not imply write")
	}
	if PrincipalHasPermission(writer, roles, "resolve", res) {
		t.Fatal("write must not imply resolve")
	}
}
