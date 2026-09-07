package index

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
)

// snapshotLines emits the log in sequence order, merging the two already-sorted event
// slices. Order is the whole content of the snapshot: emitting every put before every delete
// would replay a document's own "delete the old key, then file the new one" update backwards,
// so a restored index would have pruned exactly the entry it had just written.
func snapshotLines(puts []putEvent, deletes []deleteEvent) []string {
	lines := make([]string, 0, len(puts)+len(deletes))
	p, d := 0, 0
	for p < len(puts) || d < len(deletes) {
		if d >= len(deletes) || (p < len(puts) && puts[p].seq < deletes[d].seq) {
			e := puts[p]
			p++
			lines = append(lines, fmt.Sprintf("P|%s|%s|%s", e.entry.DocID, e.entry.CommitHash.Hex(), encodeKeyLine(e.entry.Key)))
			continue
		}
		e := deletes[d]
		d++
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
			if len(parts) < 3 {
				return fmt.Errorf("snapshot put line malformed")
			}
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

// encodeKeyLine is the canonical key encoding (see KeyString). Strings escape the three
// structural characters (backslash, pipe, comma); vectors use ';' between elements so a
// vector nested in a composite never collides with the composite's ',' separator.
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
		return "F:" + strconv.FormatFloat(v.Value, 'g', -1, 64)
	case TimestampKey:
		return fmt.Sprintf("T:%d", v.EpochMillis)
	case StringKey:
		return "S:" + escapeKeyLine(v.Value)
	case UUIDKey:
		return "U:" + v.ID.String()
	case VectorKey:
		parts := make([]string, len(v.embedding))
		for i, f := range v.embedding {
			parts[i] = formatFloat32(f)
		}
		return "V:" + strings.Join(parts, ";")
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
		n, _ := strconv.ParseInt(strings.TrimPrefix(line, "I:"), 10, 32)
		return Int32Key{Value: int32(n)}
	case strings.HasPrefix(line, "L:"):
		n, _ := strconv.ParseInt(strings.TrimPrefix(line, "L:"), 10, 64)
		return Int64Key{Value: n}
	case strings.HasPrefix(line, "F:"):
		f, _ := strconv.ParseFloat(strings.TrimPrefix(line, "F:"), 64)
		return Float64Key{Value: f}
	case strings.HasPrefix(line, "T:"):
		n, _ := strconv.ParseInt(strings.TrimPrefix(line, "T:"), 10, 64)
		return TimestampKey{EpochMillis: n}
	case strings.HasPrefix(line, "S:"):
		return StringKey{Value: unescapeKeyLine(strings.TrimPrefix(line, "S:"))}
	case strings.HasPrefix(line, "U:"):
		id, _ := codec.UUIDFromString(strings.TrimPrefix(line, "U:"))
		return UUIDKey{ID: id}
	case strings.HasPrefix(line, "V:"):
		raw := strings.TrimPrefix(line, "V:")
		if raw == "" {
			return NewVectorKey(nil)
		}
		parts := strings.Split(raw, ";")
		vec := make([]float32, len(parts))
		for i, p := range parts {
			f, _ := strconv.ParseFloat(p, 32)
			vec[i] = float32(f)
		}
		return VectorKey{embedding: vec}
	case strings.HasPrefix(line, "C:"):
		parts := splitUnescaped(strings.TrimPrefix(line, "C:"), ',')
		keys := make([]Key, len(parts))
		for i, p := range parts {
			keys[i] = decodeKeyLine(p)
		}
		return CompositeKey{Parts: keys}
	default:
		return NullKey{}
	}
}

// splitUnescaped splits on sep, skipping separators preceded by a backslash. Escape sequences
// are left in place for the part decoders to unescape.
func splitUnescaped(s string, sep byte) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' {
			i++
			continue
		}
		if s[i] == sep {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return append(parts, s[start:])
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
