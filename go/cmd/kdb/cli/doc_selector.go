package cli

import (
	"fmt"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

func resolveDocSelector(namespaceID string, store storage.Adapter, atTreeHash codec.Hash, input string) (codec.UUID, error) {
	if id, err := codec.ParseUUID(input); err == nil {
		return id, nil
	}
	s := strings.TrimSpace(input)
	s = strings.TrimPrefix(s, "0x")
	s = strings.ToLower(strings.ReplaceAll(s, "-", ""))
	if len(s) < 8 {
		return codec.UUID{}, fmt.Errorf("doc id prefix too short: %q", input)
	}
	if len(s) > 32 {
		return codec.UUID{}, fmt.Errorf("doc id too long: %q", input)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') {
			continue
		}
		return codec.UUID{}, fmt.Errorf("invalid doc id prefix: %q", input)
	}

	var matches []codec.UUID
	_ = store.ScanDocuments(namespaceID, atTreeHash, 256, func(batch []document.Document) error {
		for _, d := range batch {
			full := strings.ToLower(strings.ReplaceAll(d.ID.String(), "-", ""))
			if strings.HasPrefix(full, s) {
				matches = append(matches, d.ID)
				if len(matches) > 16 {
					return nil
				}
			}
		}
		return nil
	})

	if len(matches) == 0 {
		return codec.UUID{}, fmt.Errorf("document not found: %s", input)
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	var b strings.Builder
	b.WriteString("ambiguous document id prefix: ")
	b.WriteString(input)
	b.WriteString(" (matches: ")
	for i := 0; i < len(matches) && i < 5; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(matches[i].String())
	}
	if len(matches) > 5 {
		b.WriteString(", ...")
	}
	b.WriteString(")")
	return codec.UUID{}, fmt.Errorf("%s", b.String())
}

// ResolveDocSelectorForTest exposes doc selector parsing for unit tests.
func ResolveDocSelectorForTest(namespaceID string, store storage.Adapter, atTreeHash codec.Hash, input string) (codec.UUID, error) {
	return resolveDocSelector(namespaceID, store, atTreeHash, input)
}
