package dev.kdb.auth

public interface Authenticator {
    public suspend fun authenticate(credentials: AuthCredentials): Principal
}

public interface Authorizer {
    public suspend fun authorize(
        principal: Principal,
        action: AuthAction,
    )
}

public interface AuthEngine {
    public val authenticator: Authenticator
    public val authorizer: Authorizer
}

public object AllowAllAuth : AuthEngine {
    private val anonymous = Principal(id = "anonymous")

    override val authenticator: Authenticator =
        object : Authenticator {
            override suspend fun authenticate(credentials: AuthCredentials): Principal = anonymous
        }

    override val authorizer: Authorizer =
        object : Authorizer {
            override suspend fun authorize(
                principal: Principal,
                action: AuthAction,
            ) = Unit
        }
}
