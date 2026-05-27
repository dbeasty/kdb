package index

import (
	"fmt"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
)

func snapshotLines(puts []putEvent, deletes []deleteEvent) []string {
	lines := make([]string, 0, len(puts)+len(deletes))
	for _, e := range puts {
		lines = append(lines, fmt.Sprintf("P|%s|%s|%s", e.entry.DocID, e.entry.CommitHash.Hex(), encodeKeyLine(e.entry.Key)))
	}
	for _, e := range deletes {
		lines = append(lines, fmt.Sprintf("D|%s|%s", e.docID, e.atCommit.Hex()))
	}
	return lines
}

func restoreSnapshotLines(lines []string, ingestPut func(Entry), ingestDelete func(codec.UUID, codec.Hash)) error {
	for _, ln := range lines {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		parts := strings.SplitN(ln, "|", 4)
		switch parts[0] {
		case "P":
			doc, err := codec.UUIDFromString(parts[1])
			if err != nil {
				return err
			}
			commit, err := codec.HashFromHex(parts[2])
			if err != nil {
				return err
			}
			keyLine := "NULL"
			if len(parts) > 3 {
				keyLine = parts[3]
			}
			ingestPut(Entry{DocID: doc, Key: decodeKeyLine(keyLine), CommitHash: commit})
		case "D":
			if len(parts) < 3 {
				return fmt.Errorf("snapshot delete line malformed")
			}
			doc, err := codec.UUIDFromString(parts[1])
			if err != nil {
				return err
			}
			commit, err := codec.HashFromHex(parts[2])
			if err != nil {
				return err
			}
			ingestDelete(doc, commit)
		default:
			return fmt.Errorf("snapshot line corrupt")
		}
	}
	return nil
}

func encodeKeyLine(k Key) string {
	switch v := k.(type) {
	case NullKey:
		return "NULL"
	case BoolKey:
		return fmt.Sprintf("B:%v", v.Value)
	case Int32Key:
		return fmt.Sprintf("I:%d", v.Value)
	case Int64Key:
		return fmt.Sprintf("L:%d", v.Value)
	case Float64Key:
		return fmt.Sprintf("F:%v", v.Value)
	case TimestampKey:
		return fmt.Sprintf("T:%d", v.EpochMillis)
	case StringKey:
		return "S:" + escapeKeyLine(v.Value)
	case UUIDKey:
		return "U:" + v.ID.String()
	case VectorKey:
		panic("VECTOR keys cannot be snapshotted in memory index")
	case CompositeKey:
		parts := make([]string, len(v.Parts))
		for i, p := range v.Parts {
			parts[i] = encodeKeyLine(p)
		}
		return "C:" + strings.Join(parts, ",")
	default:
		return "NULL"
	}
}

func decodeKeyLine(line string) Key {
	switch {
	case line == "NULL":
		return NullKey{}
	case strings.HasPrefix(line, "B:"):
		return BoolKey{Value: strings.TrimPrefix(line, "B:") == "true"}
	case strings.HasPrefix(line, "I:"):
		var n int32
		fmt.Sscanf(strings.TrimPrefix(line, "I:"), "%d", &n)
		return Int32Key{Value: n}
	case strings.HasPrefix(line, "L:"):
		var n int64
		fmt.Sscanf(strings.TrimPrefix(line, "L:"), "%d", &n)
		return Int64Key{Value: n}
	case strings.HasPrefix(line, "F:"):
		var f float64
		fmt.Sscanf(strings.TrimPrefix(line, "F:"), "%v", &f)
		return Float64Key{Value: f}
	case strings.HasPrefix(line, "T:"):
		var n int64
		fmt.Sscanf(strings.TrimPrefix(line, "T:"), "%d", &n)
		return TimestampKey{EpochMillis: n}
	case strings.HasPrefix(line, "S:"):
		return StringKey{Value: unescapeKeyLine(strings.TrimPrefix(line, "S:"))}
	case strings.HasPrefix(line, "U:"):
		id, _ := codec.UUIDFromString(strings.TrimPrefix(line, "U:"))
		return UUIDKey{ID: id}
	case strings.HasPrefix(line, "C:"):
		raw := strings.TrimPrefix(line, "C:")
		parts := strings.Split(raw, ",")
		keys := make([]Key, len(parts))
		for i, p := range parts {
			keys[i] = decodeKeyLine(p)
		}
		return CompositeKey{Parts: keys}
	default:
		return NullKey{}
	}
}

func escapeKeyLine(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.ReplaceAll(s, ",", "\\,")
	return s
}

func unescapeKeyLine(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			b.WriteByte(s[i+1])
			i++
		} else {
			b.WriteByte(s[i])
		}
	}
	return b.String()
}
