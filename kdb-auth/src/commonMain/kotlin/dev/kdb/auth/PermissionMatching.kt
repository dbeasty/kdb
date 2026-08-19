package dev.kdb.auth

public fun permissionKind(permission: String): String? {
    val colon = permission.indexOf(':')
    if (colon <= 0) return null
    return permission.substring(0, colon)
}

/** Matches a grant's pattern (`kind:pattern`) against a resource path string, honoring the
 * trailing-wildcard convention (a pattern ending in `/star` matches its prefix and everything
 * beneath it — see the `endsWith`/`removeSuffix` check below for the literal suffix). */
public fun permissionMatchesPath(
    permission: String,
    path: String,
): Boolean {
    val colon = permission.indexOf(':')
    if (colon <= 0) return false
    val pattern = permission.substring(colon + 1)
    if (pattern.endsWith("/*")) {
        val prefix = pattern.removeSuffix("/*")
        return path == prefix || path.startsWith("$prefix/")
    }
    return path == pattern
}

@Deprecated(
    "Use permissionMatchesPath; kept for source compatibility.",
    ReplaceWith("permissionMatchesPath(permission, namespace)"),
)
public fun permissionMatchesNamespace(
    permission: String,
    namespace: String,
): Boolean = permissionMatchesPath(permission, namespace)

/** Namespace-only overload, preserved for existing callers (wire-layer checks that don't know
 * about a specific document). Resolves as a [ResourcePath] with no `documentId`. */
public fun principalHasPermission(
    principal: Principal,
    roles: Map<String, Set<String>>,
    kind: String,
    namespace: String,
): Boolean = principalHasPermission(principal, roles, kind, ResourcePath.of(namespace))

/**
 * Resolves whether [principal] has [kind] permission on [resource], checking grants from most
 * specific (document) to least specific (database) — a database-level grant covers every
 * collection and document beneath it, and a collection-level grant covers every document in it.
 */
public fun principalHasPermission(
    principal: Principal,
    roles: Map<String, Set<String>>,
    kind: String,
    resource: ResourcePath,
): Boolean {
    for (candidate in resource.candidatePaths()) {
        for (role in principal.roles) {
            val grants = roles[role] ?: continue
            for (grant in grants) {
                if (permissionKind(grant) == kind && permissionMatchesPath(grant, candidate)) {
                    return true
                }
            }
        }
    }
    return false
}
