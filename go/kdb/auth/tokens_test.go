package auth

import (
	"context"
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
