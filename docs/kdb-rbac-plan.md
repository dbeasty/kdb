# KDB User Management & Resource-Scoped RBAC — Plan

Status: proposal (not yet a numbered spec component). Author context: written after auditing
current `kdb-auth`/`kdb-auth-static` state, 2026-08-19.

## Current state (as of this writing)

KDB has an auth *skeleton* but no real user management and no resource-grained authorization:

- `kdb-auth` (`AuthTypes.kt`, `AuthEngine.kt`, `PermissionMatching.kt`) defines `Principal(id,
  roles: Set<String>, claims)`, an `Authenticator`/`Authorizer` pair, and a sealed `AuthAction`
  (`SessionBegin`, `SqlExec`, `TxCommit`, `PeerSync`) — every action carries a single
  `namespace: String`.
- Permissions are strings like `"write:orders/*"`. `principalHasPermission` resolves them
  against `roles: Map<roleName, Set<grant>>` with a namespace-prefix match
  (`PermissionMatching.kt`). There's no separate concept of database, collection, or document —
  `namespaceId` is the *only* scoping unit anywhere in the system.
- `kdb-auth-static` is the only implementation: a JSON file (`StaticAuthConfig`: `users: Map<user,
  StaticUserConfig(secret, roles)>`, `roles: Map<roleName, List<grant>>`) loaded once at
  startup. No user CRUD, no password hashing beyond a raw `secret` field, no token issuance —
  bearer tokens are literally parsed as `"user:pass"`.
- `go/kdb/auth` mirrors this 1:1 in Go.
- Enforcement happens only at the wire layer: `SqlWireHost` calls `authorize(principal,
  AuthAction.X(namespace, ...))` on connect/session-begin/sql-exec/tx-commit. `StorageAdapter`
  and `TransactionEngine` take no `Principal`/token at all and enforce nothing — `docs/kdb-spec.md`
  explicitly calls this out as an open gap ("Rights validation boundary").
- There is no `Collection` or `Database` type. `namespaceId: String` is the entire data-model
  hierarchy today (see `StorageAdapter.kt`, `DocumentLockManager.kt`).

**Answer to "do we have different roles per document/collection": no.** Roles today are a flat
set of namespace-prefix grants, checked only at the wire boundary, with no per-document override
and no separate collection/database concept to hang a role on.

## Goals

1. Real user management: create/list/update/delete users, rotate credentials, assign/revoke
   roles — via an admin API/DDL, not a static file (the static file becomes one *provider*
   among others, not the only one).
2. Roles as named, reusable permission bundles that can be created and deleted at runtime.
3. Authorization scoped to the actual resource hierarchy: **database → collection → document**,
   with grants at any level and deterministic inheritance/override resolution.
4. Enforcement moved down into the layer that actually mutates data (`StorageAdapter` /
   `TransactionEngine`), not just the wire layer — closing the spec's flagged gap.
5. Kotlin/Go parity maintained throughout (existing project convention).

## Design

### 1. Resource hierarchy

Today `namespaceId` conflates "database" and "collection". Introduce a structured resource path
instead of a bare string, without breaking existing callers:

```
ResourcePath = database[/collection[/documentId]]
```

- Keep `namespaceId: String` as the wire format (e.g. `"orders/invoices"`), but parse it through
  a new `ResourcePath` value type in `kdb-auth` (and its Go mirror) wherever authorization
  decisions are made. Storage/transaction code does not need to change its `namespaceId: String`
  signatures — only the authorization layer needs to understand the hierarchy.
- Document-level scoping requires passing `docId` into the `AuthAction` variants that touch a
  single document (`putDocument`, `deleteDocument`, `getDocument`) — these currently exist only
  at the `StorageAdapter` level with no `AuthAction` counterpart at all. Add a `DocAction` (or
  extend `AuthAction`) carrying `(namespace, docId)` for this purpose. Collection/DB-wide ops
  (`scanDocuments`, `commitTree`) keep the existing namespace-only actions.

### 2. Permission grants

Extend the grant string format from `kind:namespacePattern` to `kind:database[/collection[/docId]]`,
keeping `/*` as the existing prefix wildcard so current configs (`write:orders/*`) keep working
unchanged — they just now mean "all collections under database orders" instead of an opaque
namespace prefix.

Resolution order (most specific wins; deny-by-default, no explicit deny grants in v1 to keep
resolution simple — matches the existing model of "the closest applicable grant"):

```
document grant  >  collection grant  >  database grant
```

`principalHasPermission` becomes `principalHasPermission(principal, roleGrants, kind,
resourcePath)`, walking from most-specific to least-specific and returning on first match. This
is a backward-compatible generalization of the existing prefix-match logic, not a rewrite.

### 3. User & role store (the actual "user management" piece)

Add a new module `kdb-auth-store` (KMP, mirrored in `go/kdb/auth`) defining:

- `UserStore` interface: `createUser`, `getUser`, `listUsers`, `updateCredentials`, `deleteUser`,
  `assignRole(user, role)`, `revokeRole(user, role)`.
- `RoleStore` interface: `createRole(name, grants)`, `getRole`, `listRoles`, `updateGrants`,
  `deleteRole`.
- `PasswordHasher` abstraction (start with a standard slow hash — Argon2id or scrypt; the current
  raw `secret` field is not production-safe and should not ship as-is beyond the static/dev
  provider).
- Two backends:
  - `StaticAuthConfig`-backed (today's provider), kept for local/dev use, now implementing
    `UserStore`/`RoleStore` as a read-only adapter over the JSON file.
  - **System-collection-backed** (`kdb-auth-system-store`): users and roles are stored as
    documents in reserved collections (e.g. `_system/users`, `_system/roles`) inside KDB itself,
    written through the normal `StorageAdapter`/`TransactionEngine` path. This gives
    persistence, replication, and transactional updates for free, and is the real "add/remove
    roles at runtime" mechanism the user is asking for. Bootstrapping needs a superuser bypass
    (a fixed root credential from server config, not stored in `_system/users`) to avoid a
    chicken-and-egg problem on first boot.
- `AuthEngine` gains a `UserStore`/`RoleStore`-backed `Authenticator`/`Authorizer` implementation
  (`DynamicAuthEngine`) that reads from these stores instead of a fixed in-memory map, with a
  short-TTL cache to avoid a store round-trip per request.

### 4. Enforcement at the storage/transaction boundary

This is the part the spec flags as unresolved. Plan:

- `TransactionEngine`/`DefaultTransactionEngine` (Kotlin) and its Go mirror gain an optional
  `Principal` (or pre-resolved permission token) parameter threaded from `SqlWireHost` through
  transaction begin → per-write validation → commit. Each document write/delete inside a
  transaction is checked against the resolved `ResourcePath` (database/collection/docId) before
  being applied, using the same `principalHasPermission` resolution as the wire layer.
  `DocumentLockManager` already keys locks by `(namespaceId, docId)` — the same key shape is
  reused for the authorization check, so this is additive, not a redesign of the lock manager.
  This is intentionally a Transaction Engine responsibility, not a `StorageAdapter` one, since only
  the Transaction Engine sees writes before they're durable and can reject them per Component 7
  and Component 9's split of responsibilities (Storage Adapter is exec, Transaction Engine is
  policy/DAG).
- Wire-layer checks in `SqlWireHost` stay as an early-rejection fast path (avoid parsing a whole
  SQL statement for an unauthorized session), but the storage-boundary check becomes the source
  of truth, closing the gap where a bug or new call path in `SqlWireHost` could let an
  unauthorized write through unchecked.

### 5. Admin surface (create/remove roles, grant on doc/collection/db)

Two ways in, both driving the same `UserStore`/`RoleStore`:

- SQL-like DDL through the existing SQL surface (`kdb-sql`), e.g.:
  ```sql
  CREATE ROLE analyst;
  GRANT read ON DATABASE orders TO analyst;
  GRANT write ON COLLECTION orders.invoices TO analyst;
  GRANT write ON DOCUMENT orders.invoices.<docId> TO analyst;
  REVOKE write ON COLLECTION orders.invoices FROM analyst;
  CREATE USER alice WITH PASSWORD '...' ROLES (analyst);
  DROP ROLE analyst;
  ```
  This requires new grammar/AST nodes in `kdb-sql` and execution against `RoleStore`/`UserStore`
  in `SqlWireHost`, gated behind an `admin`/superuser permission kind.
- A programmatic admin API (Kotlin/Go function calls on `UserStore`/`RoleStore` directly) for
  embedded use (`kdb-embed`), since not every embedding wants a SQL front door.

## Phasing

1. **Resource path + grant format generalization** — `ResourcePath`, extended grant string
   parsing, `DocAction`, updated `principalHasPermission`. Backward compatible; existing static
   configs keep working. No storage/transaction changes yet.
2. **`kdb-auth-store` + system-collection-backed `UserStore`/`RoleStore`** — real user/role CRUD,
   persisted in KDB itself, `DynamicAuthEngine`. Static config remains as a dev/bootstrap
   provider.
3. **Storage/transaction-boundary enforcement** — thread `Principal` through
   `TransactionEngine`/Go mirror, per-write checks, close the spec-flagged gap. This is the
   highest-risk phase (touches the hot write path) and should land with its own perf pass, given
   the recent WAL group-commit throughput work.
4. **SQL DDL surface** (`CREATE ROLE`/`GRANT`/`REVOKE`/`CREATE USER`) in `kdb-sql` +
   `SqlWireHost`, plus the embedded admin API.
5. **Migration**: document how existing `StaticAuthConfig` deployments migrate users/roles into
   the system-collection store (one-shot import command), then treat the static provider as
   dev-only going forward.

## Open questions to resolve before implementation

- Should document-level grants be common enough to warrant caching per-document ACLs, or are
  they rare enough that collection/database grants cover the 95% case and document grants stay
  an uncommon escape hatch? Affects whether the `DynamicAuthEngine` cache needs document
  granularity or can stay collection-level.
- Superuser/bootstrap credential mechanism — server config flag vs. first-run wizard.
- Whether `_system/*` collections need to be hidden from normal `scanDocuments`/schema
  introspection, and whether they get their own namespace-policy defaults (no eviction, etc. —
  see `kdb-namespace-policy`).
