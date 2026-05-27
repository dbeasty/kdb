package codec

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// ParseUUID parses RFC 4122 UUID strings.
func ParseUUID(s string) (UUID, error) {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "-", "")
	if len(s) != 32 {
		return UUID{}, fmt.Errorf("invalid uuid: %q", s)
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return UUID{}, fmt.Errorf("invalid uuid: %w", err)
	}
	return UUIDFromBytes(b)
}

// TimestampFromISO8601 parses an ISO-8601 instant into microsecond resolution.
func TimestampFromISO8601(s string) (Timestamp, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339, s)
	}
	if err != nil {
		return Timestamp{}, fmt.Errorf("invalid ISO-8601 timestamp: %w", err)
	}
	micros := t.UnixNano() / 1000
	return TimestampFromEpochMicros(micros), nil
}
