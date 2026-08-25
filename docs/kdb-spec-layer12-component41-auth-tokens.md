# Component 41 — Auth: Session/Token Issuance

Layer 12 (renumbered from Revision 1's Component 35 — see the gap analysis §7). **Revised in
Revision 2**: scope narrows. Revision 1 wrote this against `kdb-auth-static`'s plaintext config
file as the only real auth implementation. That's no longer true — real RBAC now exists
(`kdb-auth-store`'s `RegistryAuthStore`/`DynamicAuthEngine`, PBKDF2 password hashing, per-document
write authorization, `CREATE USER`/`GRANT`/`REVOKE` SQL admin surface, all confirmed real by the
maturity audit). What RBAC did **not** add is session/bearer-token issuance — a "token" in
`DynamicAuthEngine` is still literally `user:password` resolved per connection
(`resolveUserAndSecret` splits the credential string on `:`), not an opaque token looked up
against something the application itself wrote. This component's actual job is unchanged despite
the richer surrounding auth landscape: let a wire connection authenticate against an
application-issued session token, stored as an ordinary KDB document, rather than against
credentials re-sent on every connection.

Depends on `kdb-auth`'s existing `AuthEngine`/`Authenticator`/`Authorizer`/`Principal`/
`ConnectionContextParsing`, and — new this revision — `kdb-auth-store`'s `DynamicAuthEngine` and
`RegistryAuthStore` (the thing this component now sits *alongside*, not `StaticAuthEngine`, which
is comparatively marginal now that a real dynamic store exists).

## 1. Purpose

Zolik (and any application storing its own users/sessions *as KDB documents*, which is the natural
thing to do once KDB is the backend at all) needs to authenticate a wire connection against a token
that was itself issued by the *application* and written into KDB moments earlier — "authenticate
this connection using a session token I created programmatically," not "re-send my username and
password on every connection" (which is what `DynamicAuthEngine` requires today, even with real
RBAC underneath it) and not "authenticate against a hand-edited config file" (which is all
`StaticAuthEngine` ever offered). This component adds that authenticator without touching
`DynamicAuthEngine`/`RegistryAuthStore` (which stay exactly as they are for the "prove who you are
with a password" step — this component consumes their output, a validated `Principal`, at token
*issuance* time, but does not replace the password check itself) or `StaticAuthEngine` (still valid
for dev/ops/service accounts). Zolik already has a session shape
(`models.Session{token, guestName, userId, createdAt, expiresAt}`), and this component's job is to
let `kdb-auth` validate against *that* shape, stored as an ordinary KDB document, rather than
design a new one.

## 2. Dependencies

- `kdb-auth`'s existing `AuthEngine`/`Authenticator`/`Authorizer`/`Principal` interfaces — extended
  with a new implementation, not modified.
- `kdb-auth-store`'s `RegistryAuthStore`/`DynamicAuthEngine` — this component's natural issuance
  path is "log in via `DynamicAuthEngine` once, mint a session document on success, hand back its
  token" — not a replacement for the password/grant check, a layer that runs *after* it succeeds.
- `kdb-embed`/`kdb-server`'s document read path — a token authenticator needs to look up a
  `sessions`-namespace document by token, an exact-match hash-index lookup (Component 8/12).

## 3. Public Interface

```kotlin
// kdb-auth/src/commonMain/kotlin/dev/kdb/auth/token/TokenAuthEngine.kt
package dev.kdb.auth.token

// A document-backed session lookup, kept deliberately generic (not "Zolik's Session shape"
// specifically) so any application can adopt this by pointing it at its own sessions namespace
// and field names.
data class TokenAuthConfig(
    val sessionsNamespace: String,       // e.g. "zolik/sessions"
    val tokenField: String = "token",    // schema field the session doc is keyed/indexed on
    val expiresAtField: String = "expiresAt",
    val principalIdField: String = "userId",  // may be empty for a guest session
)

class TokenAuthEngine(
    private val config: TokenAuthConfig,
    private val documentReader: DocumentReader,   // narrow read-only capability, not a full
                                                    // EmbedRuntime/StorageAdapter — see §5
) : Authenticator {
    override suspend fun authenticate(credentials: ConnectionCredentials): AuthResult
}

interface DocumentReader {
    suspend fun findByField(namespace: String, field: String, value: String): KdbDocument?
}

sealed class AuthResult {
    data class Authenticated(val principal: Principal) : AuthResult()
    data class Rejected(val reason: RejectReason) : AuthResult()
}

enum class RejectReason { TOKEN_NOT_FOUND, TOKEN_EXPIRED, MALFORMED_CREDENTIALS }
```

```kotlin
// kdb-auth/src/commonMain/kotlin/dev/kdb/auth/token/CompositeAuthEngine.kt
// Lets a deployment accept static config-file admin/service accounts (StaticAuthEngine),
// username/password RBAC logins (DynamicAuthEngine), AND application-issued session tokens
// (this component) on the same server, trying each in turn — all three coexist, none shadows
// the others.
class CompositeAuthEngine(
    private val engines: List<Authenticator>,
) : Authenticator {
    override suspend fun authenticate(credentials: ConnectionCredentials): AuthResult
}
```

```kotlin
// New this revision: the issuance half — not part of kdb-auth's core (which stays
// validate-only per §8's non-goals), but worth specifying here since a caller needs a
// concrete recipe for "log in, then get a token to hand back to the client," not just the
// validation side of that flow.
package dev.kdb.auth.token

// Given a successful DynamicAuthEngine authentication, mint and store a session document.
// This is a thin helper over an ordinary document write (Upsert-shaped — see gap analysis §2 —
// not a CAS write), not new engine machinery.
class SessionIssuer(
    private val config: TokenAuthConfig,
    private val documentWriter: DocumentWriter,   // symmetric narrow interface to DocumentReader
) {
    suspend fun issue(principal: Principal, ttl: Duration): SessionToken
    suspend fun revoke(token: String): Unit  // deletes the session document — DeleteByToken's analogue
}

data class SessionToken(val token: String, val expiresAt: Instant)

interface DocumentWriter {
    suspend fun upsert(namespace: String, docId: String, json: String): Unit
    suspend fun delete(namespace: String, docId: String): Unit
}
```

## 4. Data Structures

No new document/wire types beyond `TokenAuthConfig`/`SessionToken` (server-side configuration and
a plain issuance-result value, not persisted or sent over the wire as a distinct frame type).
Reuses `kdb-auth`'s existing `Principal`.

## 5. Contracts

- `authenticate` receives whatever `ConnectionContextParsing.kt` already extracts from the
  connection handshake (`x-kdb-api-key`/`Authorization: Bearer <token>`) — this component consumes
  the bearer-token value from that existing parsing, it does not add a new header convention.
- Looks up the token via `documentReader.findByField(sessionsNamespace, tokenField, token)` — an
  exact-match hash-index lookup, O(1) regardless of session table size.
- **Expiry is checked here, not delegated to a KDB TTL feature** — matches Zolik's own Mongo-backed
  code, which doesn't use a TTL index either. `expiresAt` is read from the found document and
  compared against the current time at authentication time; `TOKEN_EXPIRED` is distinguished from
  `TOKEN_NOT_FOUND` only for observability, not because the caller should behave differently (§6).
- **This component's `TokenAuthEngine` half never issues, hashes, or checks passwords** — that
  stays `DynamicAuthEngine`/`RegistryAuthStore`'s job. `SessionIssuer` is the seam between them: it
  runs *after* a `DynamicAuthEngine.authenticate` call already succeeded, and its only
  responsibility is minting the follow-on session document. This split matters because it means
  `TokenAuthEngine` (validation) can be fully unit-tested with a fake `DocumentReader` and no
  password/RBAC machinery involved at all — see test list.
- `SessionIssuer.issue`/`revoke` write via `Upsert`/delete, matching the gap analysis §2's finding
  that session writes are unconditional, not CAS — do not anchor them on a `BaseVersion`.
- `CompositeAuthEngine` tries engines in the order given and returns the first `Authenticated`
  result; if all reject, it returns the *last* engine's `Rejected` reason (arbitrary but
  deterministic tie-break).

## 6. Error Cases

- `AuthResult.Rejected(TOKEN_NOT_FOUND)` — no session document matches; deliberately
  indistinguishable from `MALFORMED_CREDENTIALS` at the wire-response level (don't leak "valid
  format, unknown token" vs. "not even well-formed" to an unauthenticated caller).
- `AuthResult.Rejected(TOKEN_EXPIRED)` — found, but past `expiresAt`.
- `AuthResult.Rejected(MALFORMED_CREDENTIALS)` — no bearer token present at all.
- Underlying document-store failure during lookup is **not** mapped to `Rejected` — propagates as a
  genuine exception, so a storage outage is never indistinguishable from "invalid token."

## 7. Test Cases

1. **Valid, unexpired token authenticates**, `Principal` populated from `principalIdField`.
2. **Unknown token rejected** with `TOKEN_NOT_FOUND`.
3. **Expired token rejected** with `TOKEN_EXPIRED`.
4. **Token exactly at the expiry boundary** — explicit inclusive/exclusive edge behavior.
5. **Guest session (empty `principalIdField`) still authenticates**.
6. **Missing bearer token** → `MALFORMED_CREDENTIALS`.
7. **`CompositeAuthEngine` falls through**: `StaticAuthEngine` reject → `DynamicAuthEngine` reject
   → `TokenAuthEngine` success, proving all three coexist rather than one shadowing the others —
   the three-way case Revision 1's two-engine version didn't need to prove.
8. **`CompositeAuthEngine` rejects when all three reject**, deterministic reason per §5.
9. **Storage-layer failure during lookup propagates as an exception**, not `Rejected`.
10. **Session document with malformed/missing `expiresAt`** — treat as invalid, not "never expires."
11. **`SessionIssuer.issue` after a real `DynamicAuthEngine.authenticate` success** produces a
    token that `TokenAuthEngine.authenticate` subsequently accepts — the full round trip, new this
    revision, proving the two halves actually compose.
12. **`SessionIssuer.revoke` deletes the session document**, and a subsequent `authenticate` call
    with that token returns `TOKEN_NOT_FOUND`, not a stale success.

## 8. Non-Goals

- JWT support — unchanged from Revision 1; Zolik's session model is an opaque bearer token backed
  by a document lookup, not a self-contained signed token.
- Replacing `DynamicAuthEngine`'s password/grant checking, or `RegistryAuthStore`'s user/role
  storage — this component issues and validates the *follow-on* token, it does not re-implement
  login.
- Rate limiting or brute-force protection on token lookup — an application-layer or reverse-proxy
  concern.
- Porting `SessionIssuer`/`TokenAuthEngine` to `go/kdb/auth` as part of *this* component — Component
  38 already scopes porting `RegistryAuthStore`/`PasswordHasher`/`AuthorizingTransactionEngine` to
  Go; whether session-token issuance needs a Go-side equivalent too depends on whether Zolik's
  Go-native path (Component 38/40) ends up needing it, which isn't settled yet — flag as a likely
  fast-follow on Component 38 rather than folding it into this component's scope now.

## 9. Implementation Notes

- Build `TokenAuthEngine`/`SessionIssuer` against the narrow `DocumentReader`/`DocumentWriter`
  interfaces, not directly against `EmbedRuntime`/`StorageAdapter`, so both can be unit-tested with
  trivial in-memory fakes.
- Verify the connection-rejection response doesn't leak `RejectReason` verbatim to the client
  (a pre-existing property to check across all `Authenticator` implementations, not introduce fresh
  here).
- If Zolik's session model evolves (refresh tokens, etc.), `TokenAuthConfig`'s field-name mapping
  means reconfiguration, not a `kdb-auth` code change, as long as the shape stays "one document,
  indexed token field, expiry field."

## 10. Estimated Lines

450–750 NBNC (up modestly from Revision 1's 350–600, to cover the new `SessionIssuer` half and the
three-way `CompositeAuthEngine` test): ~150 for `TokenAuthEngine`, ~100 for `SessionIssuer`, ~100
for `CompositeAuthEngine`'s three-engine composition, ~100–400 for tests.
