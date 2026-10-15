package auth

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// U12: a token authenticates as its user with the user's roles, as a bearer token or as the
// password; revoking it refuses it from then on without touching the user's password or other
// tokens; an expired, tampered or unknown token is refused; the registry keeps no secret.
func TestAccessTokensAuthenticateAndRevoke(t *testing.T) {
	s := newTestRegistryAuthStore(t)
	if err := s.CreateUser("svc", "pw", []string{"reader"}); err != nil {
		t.Fatal(err)
	}
	authn := registryAuthenticator{store: s}
	tok, rec, err := s.CreateToken("svc", "qtask-api", 0)
	if err != nil {
		t.Fatal(err)
	}
	other, _, err := s.CreateToken("svc", "worker", 0)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(rec.SecretSHA256, tok[len(TokenPrefix)+len(rec.TokenID)+1:]) {
		t.Fatal("the record holds the secret")
	}
	p, err := authn.Authenticate(context.Background(), Credentials{Token: &tok})
	if err != nil || p.ID != "svc" {
		t.Fatalf("bearer token: %v %v", p, err)
	}
	if _, ok := p.Roles["reader"]; !ok {
		t.Fatalf("token principal lacks the user's roles: %v", p.Roles)
	}
	user := "svc"
	if p, err := authn.Authenticate(context.Background(), Credentials{User: &user, Password: &tok}); err != nil || p.ID != "svc" {
		t.Fatalf("token as password: %v %v", p, err)
	}
	tampered := tok[:len(tok)-2] + "xx"
	if _, err := authn.Authenticate(context.Background(), Credentials{Token: &tampered}); err == nil {
		t.Fatal("tampered token accepted")
	}
	if err := s.RevokeToken(rec.TokenID); err != nil {
		t.Fatal(err)
	}
	if _, err := authn.Authenticate(context.Background(), Credentials{Token: &tok}); err == nil {
		t.Fatal("revoked token accepted")
	}
	if _, err := authn.Authenticate(context.Background(), Credentials{Token: &other}); err != nil {
		t.Fatalf("revoking one token refused another: %v", err)
	}
	pw := "pw"
	if _, err := authn.Authenticate(context.Background(), Credentials{User: &user, Password: &pw}); err != nil {
		t.Fatalf("revoking a token broke the password: %v", err)
	}
	short, _, err := s.CreateToken("svc", "short", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.verifyToken(short, time.Now().Add(time.Second)); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expired token: %v", err)
	}
	list, err := s.ListTokens()
	if err != nil || len(list) != 3 {
		t.Fatalf("list: %d tokens (%v)", len(list), err)
	}
	if _, _, err := s.CreateToken("nobody", "", 0); err == nil {
		t.Fatal("token issued for an unknown user")
	}
}

// The token format pinned with fixed vectors, so the Kotlin registry store (kdb-auth-store
// RegistryAuthStoreTokenTest) can share a registry with this one: the same record bytes under
// the same document id, and the same token string accepted.
func TestAccessTokenFixedVector(t *testing.T) {
	const (
		token  = "kdbt_0123456789abcdef_c2VjcmV0LXNlY3JldC1zZWNyZXQtc2VjcmV0LXNlY3I"
		docID  = "7f6f4dd1-927f-28f6-317c-fc029e481bb9"
		record = `{"kind":"token","tokenId":"0123456789abcdef","user":"svc","label":"qtask-api",` +
			`"secretSha256":"2c4580586d9cf21dc4222d31dee8d845a7ed9bd9bf8a0937391bc96cb1a2c25a",` +
			`"createdAt":"2026-10-15T10:00:00Z","expiresAt":"2099-01-01T00:00:00Z"}`
	)
	if got := tokenDocID("0123456789abcdef").String(); got != docID {
		t.Fatalf("token doc id: %s", got)
	}
	id, secret, ok := parseToken(token)
	if !ok || id != "0123456789abcdef" {
		t.Fatalf("parse: %q %v", id, ok)
	}
	rec := TokenRecord{Kind: "token", TokenID: id, User: "svc", Label: "qtask-api", SecretSHA256: secretDigest(secret),
		CreatedAt: "2026-10-15T10:00:00Z", ExpiresAt: "2099-01-01T00:00:00Z"}
	body, err := json.Marshal(rec)
	if err != nil || string(body) != record {
		t.Fatalf("record bytes:\n%s\nwant\n%s (%v)", body, record, err)
	}
	s := newTestRegistryAuthStore(t)
	if err := s.CreateUser("svc", "pw", []string{"reader"}); err != nil {
		t.Fatal(err)
	}
	if err := s.writeToken(rec); err != nil {
		t.Fatal(err)
	}
	tok := token
	if p, err := (registryAuthenticator{store: s}).Authenticate(context.Background(), Credentials{Token: &tok}); err != nil || p.ID != "svc" {
		t.Fatalf("fixed token: %v %v", p, err)
	}
}
