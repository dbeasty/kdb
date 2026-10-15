package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// Revocable access tokens (qtask production plan U12).
//
// A "user:password" credential is sent on every connection and can only be withdrawn by changing
// the password, which breaks every client at once. A token names one client: it is issued for a
// user, carries that user's roles, can expire, and can be revoked on its own. Its secret is 32
// random bytes, so the registry keeps only its SHA-256 (a slow password hash buys nothing against
// a secret nobody can guess, and would be paid on every connection).
//
// Token records live in the users registry namespace, under ids no user id can produce
// ("token:<id>"), so they persist with the users they belong to.

// TokenPrefix starts every token string: kdbt_<id>_<secret>.
const TokenPrefix = "kdbt_"

// TokenRecord is a persisted token. SecretSHA256 is the hex SHA-256 of the secret part.
type TokenRecord struct {
	Kind         string `json:"kind"` // always "token"
	TokenID      string `json:"tokenId"`
	User         string `json:"user"`
	Label        string `json:"label,omitempty"`
	SecretSHA256 string `json:"secretSha256"`
	CreatedAt    string `json:"createdAt"`
	ExpiresAt    string `json:"expiresAt,omitempty"`
	Revoked      bool   `json:"revoked,omitempty"`
}

// ErrTokenNotFound is returned for an unknown token id.
var ErrTokenNotFound = fmt.Errorf("token not found")

func tokenDocID(id string) codec.UUID { return deterministicDocID("token:" + id) }

// IsAccessToken reports whether s has the shape of a token string (TokenPrefix).
func IsAccessToken(s string) bool { return strings.HasPrefix(s, TokenPrefix) }

// parseToken splits kdbt_<id>_<secret>.
func parseToken(s string) (id, secret string, ok bool) {
	if !IsAccessToken(s) {
		return "", "", false
	}
	rest := s[len(TokenPrefix):]
	i := strings.IndexByte(rest, '_')
	if i <= 0 || i == len(rest)-1 {
		return "", "", false
	}
	return rest[:i], rest[i+1:], true
}

func secretDigest(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// CreateToken issues a token for user (who must exist), valid for ttl (0: until revoked), and
// returns the token string - shown once; only its digest is kept - and its record.
func (s *RegistryAuthStore) CreateToken(user, label string, ttl time.Duration) (string, TokenRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if u, err := s.readUser(user); err != nil {
		return "", TokenRecord{}, err
	} else if u == nil {
		return "", TokenRecord{}, fmt.Errorf("%w: %s", ErrUserNotFound, user)
	}
	idBytes := make([]byte, 8)
	secretBytes := make([]byte, 32)
	if _, err := rand.Read(idBytes); err != nil {
		return "", TokenRecord{}, err
	}
	if _, err := rand.Read(secretBytes); err != nil {
		return "", TokenRecord{}, err
	}
	id := hex.EncodeToString(idBytes)
	secret := base64.RawURLEncoding.EncodeToString(secretBytes)
	now := time.Now().UTC()
	rec := TokenRecord{Kind: "token", TokenID: id, User: user, Label: label, SecretSHA256: secretDigest(secret), CreatedAt: now.Format(time.RFC3339)}
	if ttl > 0 {
		rec.ExpiresAt = now.Add(ttl).Format(time.RFC3339)
	}
	if err := s.writeToken(rec); err != nil {
		return "", TokenRecord{}, err
	}
	return TokenPrefix + id + "_" + secret, rec, nil
}

// RevokeToken revokes a token by id; every later connection presenting it is refused.
func (s *RegistryAuthStore) RevokeToken(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.readToken(id)
	if err != nil {
		return err
	}
	if rec == nil {
		return fmt.Errorf("%w: %s", ErrTokenNotFound, id)
	}
	rec.Revoked = true
	return s.writeToken(*rec)
}

// ListTokens returns every token record (no secrets: the registry holds none), oldest first.
func (s *RegistryAuthStore) ListTokens() ([]TokenRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	treeHash, ok, err := s.currentTreeHash(s.userDag)
	if err != nil || !ok {
		return nil, err
	}
	var out []TokenRecord
	err = s.storage.ScanDocuments(UsersNamespace, treeHash, 256, func(batch []document.Document) error {
		for _, d := range batch {
			if !strings.Contains(d.JSON, `"kind":"token"`) {
				continue
			}
			var rec TokenRecord
			if json.Unmarshal([]byte(d.JSON), &rec) == nil && rec.Kind == "token" {
				out = append(out, rec)
			}
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt < out[j].CreatedAt })
	return out, err
}

// verifyToken resolves a token string to its live record: known, digest matching, not revoked,
// not expired, and its user still present.
func (s *RegistryAuthStore) verifyToken(token string, now time.Time) (*TokenRecord, *UserRecord, error) {
	id, secret, ok := parseToken(token)
	if !ok {
		return nil, nil, fmt.Errorf("malformed access token")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.readToken(id)
	if err != nil {
		return nil, nil, err
	}
	if rec == nil || subtle.ConstantTimeCompare([]byte(rec.SecretSHA256), []byte(secretDigest(secret))) != 1 {
		return nil, nil, fmt.Errorf("invalid access token")
	}
	if rec.Revoked {
		return nil, nil, fmt.Errorf("access token %s has been revoked", id)
	}
	if rec.ExpiresAt != "" {
		if exp, err := time.Parse(time.RFC3339, rec.ExpiresAt); err != nil || !now.Before(exp) {
			return nil, nil, fmt.Errorf("access token %s has expired", id)
		}
	}
	user, err := s.readUser(rec.User)
	if err != nil {
		return nil, nil, err
	}
	if user == nil {
		return nil, nil, fmt.Errorf("access token %s belongs to a user that no longer exists", id)
	}
	return rec, user, nil
}

func (s *RegistryAuthStore) readToken(id string) (*TokenRecord, error) {
	treeHash, ok, err := s.currentTreeHash(s.userDag)
	if err != nil || !ok {
		return nil, err
	}
	doc, err := s.storage.GetDocument(UsersNamespace, tokenDocID(id), treeHash)
	if err != nil || doc == nil {
		return nil, err
	}
	var rec TokenRecord
	if err := json.Unmarshal([]byte(doc.JSON), &rec); err != nil {
		return nil, err
	}
	if rec.Kind != "token" {
		return nil, nil
	}
	return &rec, nil
}

func (s *RegistryAuthStore) writeToken(rec TokenRecord) error {
	body, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return s.commitWrite(s.userDag, UsersNamespace, tokenDocID(rec.TokenID), string(body))
}
