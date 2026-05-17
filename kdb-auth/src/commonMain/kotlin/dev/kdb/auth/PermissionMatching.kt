package dev.kdb.auth

public fun permissionMatchesNamespace(
    permission: String,
    namespace: String,
): Boolean {
    val colon = permission.indexOf(':')
    if (colon <= 0) return false
    val pattern = permission.substring(colon + 1)
    if (pattern.endsWith("/*")) {
        val prefix = pattern.removeSuffix("/*")
        return namespace == prefix || namespace.startsWith("$prefix/")
    }
    return namespace == pattern
}

public fun permissionKind(permission: String): String? {
    val colon = permission.indexOf(':')
    if (colon <= 0) return null
    return permission.substring(0, colon)
}

public fun principalHasPermission(
    principal: Principal,
    roles: Map<String, Set<String>>,
    kind: String,
    namespace: String,
): Boolean {
    for (role in principal.roles) {
        val grants = roles[role] ?: continue
        for (grant in grants) {
            if (permissionKind(grant) == kind && permissionMatchesNamespace(grant, namespace)) {
                return true
            }
        }
    }
    return false
}
