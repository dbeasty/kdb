package auth

import "strings"

// PermissionKind returns the "kind" half of a "kind:pattern" grant string, or "" if malformed.
func PermissionKind(permission string) string {
	colon := strings.IndexByte(permission, ':')
	if colon <= 0 {
		return ""
	}
	return permission[:colon]
}

// PermissionMatchesPath matches a grant's pattern ("kind:pattern") against a resource path
// string, honoring the "/*" prefix-wildcard convention ("orders/*" matches "orders" and
// everything under it).
func PermissionMatchesPath(permission string, path string) bool {
	colon := strings.IndexByte(permission, ':')
	if colon <= 0 {
		return false
	}
	pattern := permission[colon+1:]
	if strings.HasSuffix(pattern, "/*") {
		prefix := strings.TrimSuffix(pattern, "/*")
		return path == prefix || strings.HasPrefix(path, prefix+"/")
	}
	return path == pattern
}

// PrincipalHasPermission resolves whether principal has kind permission on resource, checking
// grants from most specific (document) to least specific (database).
func PrincipalHasPermission(principal Principal, roles map[string][]string, kind string, resource ResourcePath) bool {
	for _, candidate := range resource.CandidatePaths() {
		for role := range principal.Roles {
			for _, grant := range roles[role] {
				if PermissionKind(grant) == kind && PermissionMatchesPath(grant, candidate) {
					return true
				}
			}
		}
	}
	return false
}

// PrincipalHasNamespacePermission is the namespace-only overload, for wire-layer checks that
// don't know about a specific document.
func PrincipalHasNamespacePermission(principal Principal, roles map[string][]string, kind string, namespace string) bool {
	return PrincipalHasPermission(principal, roles, kind, NewResourcePath(namespace, ""))
}
