package dev.kdb.auth.store

import kotlinx.serialization.Serializable

/** Persisted user record. [passwordHash]/[passwordSalt] are hex-encoded PBKDF2 output — see
 * [PasswordHasher]. Never expose these fields outside the store/authenticator. */
@Serializable
public data class UserRecord(
    val id: String,
    val passwordHash: String,
    val passwordSalt: String,
    val roles: Set<String> = emptySet(),
)

@Serializable
public data class RoleRecord(
    val name: String,
    val grants: Set<String> = emptySet(),
)

public class UserAlreadyExistsException(userId: String) : RuntimeException("user already exists: $userId")

public class UserNotFoundException(userId: String) : RuntimeException("user not found: $userId")

public class RoleAlreadyExistsException(role: String) : RuntimeException("role already exists: $role")

public class RoleNotFoundException(role: String) : RuntimeException("role not found: $role")

/**
 * CRUD for principals. Implementations back this with durable storage (see
 * [RegistryAuthStore]) so roles/users can be added and removed at runtime instead of being
 * fixed at server startup. See docs/kdb-rbac-plan.md.
 */
public interface UserStore {
    public suspend fun createUser(
        id: String,
        password: String,
        roles: Set<String> = emptySet(),
    )

    public suspend fun getUser(id: String): UserRecord?

    public suspend fun listUsers(): List<UserRecord>

    public suspend fun updateCredentials(
        id: String,
        newPassword: String,
    )

    public suspend fun deleteUser(id: String)

    public suspend fun assignRole(
        id: String,
        role: String,
    )

    public suspend fun revokeRole(
        id: String,
        role: String,
    )

    /** Verifies [password] against the stored hash for [id]; false for unknown users too, so
     * callers can't distinguish "wrong password" from "no such user" via this check alone. */
    public suspend fun verifyPassword(
        id: String,
        password: String,
    ): Boolean
}

/** CRUD for named grant bundles. See [UserStore]. */
public interface RoleStore {
    public suspend fun createRole(
        name: String,
        grants: Set<String> = emptySet(),
    )

    public suspend fun getRole(name: String): RoleRecord?

    public suspend fun listRoles(): List<RoleRecord>

    public suspend fun updateGrants(
        name: String,
        grants: Set<String>,
    )

    public suspend fun deleteRole(name: String)
}
