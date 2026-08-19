package auth

import "testing"

func TestWildcardMatchesChildNamespace(t *testing.T) {
	if !PermissionMatchesPath("read:demo/*", "demo/users") {
		t.Error("expected wildcard grant to match child namespace")
	}
	if !PermissionMatchesPath("read:demo/*", "demo") {
		t.Error("expected wildcard grant to match its own prefix")
	}
	if PermissionMatchesPath("read:demo/*", "other/users") {
		t.Error("expected wildcard grant not to match unrelated namespace")
	}
}

func testRoles() map[string][]string {
	return map[string][]string{
		"db-writer":         {"write:orders"},
		"wildcard-writer":   {"write:orders/*"},
		"collection-reader": {"read:orders/invoices"},
		"doc-writer":        {"write:orders/invoices/doc-1"},
	}
}

func principalWith(role string) Principal {
	return Principal{ID: "u", Roles: map[string]struct{}{role: {}}}
}

func TestDatabaseGrantCoversEveryCollectionAndDocumentBeneathIt(t *testing.T) {
	roles := testRoles()
	p := principalWith("db-writer")
	if !PrincipalHasPermission(p, roles, "write", ResourcePath{Database: "orders"}) {
		t.Error("expected database-level grant on database resource")
	}
	if !PrincipalHasPermission(p, roles, "write", ResourcePath{Database: "orders", Collection: "invoices"}) {
		t.Error("expected database-level grant to cover a collection")
	}
	if !PrincipalHasPermission(p, roles, "write", ResourcePath{Database: "orders", Collection: "invoices", DocumentID: "doc-1"}) {
		t.Error("expected database-level grant to cover a document")
	}
	if PrincipalHasPermission(p, roles, "write", ResourcePath{Database: "shipping"}) {
		t.Error("expected no leak to unrelated database")
	}
}

func TestCollectionGrantDoesNotLeakToSiblingCollections(t *testing.T) {
	roles := testRoles()
	p := principalWith("collection-reader")
	if !PrincipalHasPermission(p, roles, "read", ResourcePath{Database: "orders", Collection: "invoices", DocumentID: "doc-1"}) {
		t.Error("expected collection-level grant to cover a document in it")
	}
	if PrincipalHasPermission(p, roles, "read", ResourcePath{Database: "orders", Collection: "shipments"}) {
		t.Error("expected no leak to sibling collection")
	}
}

func TestDocumentGrantDoesNotLeakToSiblingDocuments(t *testing.T) {
	roles := testRoles()
	p := principalWith("doc-writer")
	if !PrincipalHasPermission(p, roles, "write", ResourcePath{Database: "orders", Collection: "invoices", DocumentID: "doc-1"}) {
		t.Error("expected document-level grant on the named document")
	}
	if PrincipalHasPermission(p, roles, "write", ResourcePath{Database: "orders", Collection: "invoices", DocumentID: "doc-2"}) {
		t.Error("expected no leak to sibling document")
	}
}

func TestNamespaceOnlyOverloadStaysCompatible(t *testing.T) {
	roles := testRoles()
	p := principalWith("wildcard-writer")
	if !PrincipalHasNamespacePermission(p, roles, "write", "orders/invoices") {
		t.Error("expected namespace-only overload to keep working")
	}
}
