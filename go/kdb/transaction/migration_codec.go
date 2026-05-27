package transaction

import (
	"encoding/hex"
	"strings"

	"github.com/limidus/kdb/go/kdb/schema"
)

// EncodeMigration returns lowercase hex wire bytes for a schema migration.
func EncodeMigration(m schema.SchemaMigration) (string, error) {
	b, err := m.ToBytes()
	if err != nil {
		return "", err
	}
	return strings.ToLower(hex.EncodeToString(b)), nil
}

// DecodeMigration parses a migration from lowercase hex payload.
func DecodeMigration(payload string) (schema.SchemaMigration, error) {
	b, err := hex.DecodeString(strings.TrimSpace(payload))
	if err != nil {
		return schema.SchemaMigration{}, err
	}
	return schema.MigrationFromBytes(b)
}
