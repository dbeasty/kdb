package auth

import (
	"context"
	"fmt"
	"strings"
)

// NewRegistryAuthEngine returns an Engine backed by store instead of a fixed set of credentials
// - matching kdb-auth-store's DynamicAuthEngine.kt (dynamicAuthEngine): every
// authenticate/authorize call reads the current registry, so user/role changes take effect
// immediately without a server restart.
func NewRegistryAuthEngine(store *RegistryAuthStore) Engine {
	return &registryAuthEngine{store: store}
}

type registryAuthEngine struct {
	store *RegistryAuthStore
}

func (e *registryAuthEngine) Authenticator() Authenticator {
	return registryAuthenticator{store: e.store}
}

func (e *registryAuthEngine) Authorizer() Authorizer {
	return registryAuthorizer{store: e.store}
}

type registryAuthenticator struct{ store *RegistryAuthStore }

func (a registryAuthenticator) Authenticate(_ context.Context, creds Credentials) (Principal, error) {
	user, password, err := resolveUserAndSecret(creds)
	if err != nil {
		return Principal{}, err
	}
	record, err := a.store.GetUser(user)
	if err != nil {
		return Principal{}, err
	}
	if record == nil {
		return Principal{}, fmt.Errorf("unknown user: %s", user)
	}
	ok, err := a.store.VerifyPassword(user, password)
	if err != nil {
		return Principal{}, err
	}
	if !ok {
		return Principal{}, fmt.Errorf("invalid credentials for user: %s", user)
	}
	roles := make(map[string]struct{}, len(record.Roles))
	for _, r := range record.Roles {
		roles[r] = struct{}{}
	}
	return Principal{ID: user, Roles: roles}, nil
}

func resolveUserAndSecret(creds Credentials) (user, password string, err error) {
	if creds.User != nil && creds.Password != nil {
		return *creds.User, *creds.Password, nil
	}
	if creds.Token != nil {
		if idx := strings.IndexByte(*creds.Token, ':'); idx > 0 {
			return (*creds.Token)[:idx], (*creds.Token)[idx+1:], nil
		}
	}
	return "", "", fmt.Errorf("missing credentials")
}

type registryAuthorizer struct{ store *RegistryAuthStore }

func (a registryAuthorizer) Authorize(_ context.Context, principal Principal, action Action) error {
	roleGrants, err := a.store.GrantsByRole()
	if err != nil {
		return err
	}
	kind, resource := actionToResource(action)
	if !PrincipalHasPermission(principal, roleGrants, kind, resource) {
		docSuffix := ""
		if resource.DocumentID != "" {
			docSuffix = "/" + resource.DocumentID
		}
		return fmt.Errorf("principal %s lacks %s on %s%s", principal.ID, kind, resource.NamespaceID(), docSuffix)
	}
	return nil
}

// actionToResource maps a wire-layer Action to the (kind, resource) pair PrincipalHasPermission
// checks, matching kdb-auth-store's DynamicAuthEngine.kt authorizer switch.
func actionToResource(action Action) (kind string, resource ResourcePath) {
	switch a := action.(type) {
	case SessionBeginAction:
		return "read", NewResourcePath(a.Namespace, "")
	case SqlExecAction:
		if a.ReadOnly {
			return "read", NewResourcePath(a.Namespace, "")
		}
		return "write", NewResourcePath(a.Namespace, "")
	case TxCommitAction:
		return "write", NewResourcePath(a.Namespace, "")
	case PeerSyncAction:
		return "sync", NewResourcePath(a.Namespace, "")
	case DocumentWriteAction:
		return "write", NewResourcePath(a.Namespace, a.DocID)
	case DocumentDeleteAction:
		return "write", NewResourcePath(a.Namespace, a.DocID)
	case DocumentReadAction:
		return "read", NewResourcePath(a.Namespace, a.DocID)
	case AdminAction:
		return "admin", NewResourcePath(a.Scope, "")
	default:
		return "unknown", NewResourcePath("", "")
	}
}
