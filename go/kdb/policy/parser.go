package policy

import (
	"encoding/json"
	"regexp"
	"strings"

	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// Parser parses namespace policy DSL or JSON.
type Parser interface {
	Parse(source string) (NamespacePolicy, error)
	ParseJSON(jsonStr string, sch *schema.KdbSchema) (NamespacePolicy, error)
}

// DefaultParser is the standard policy parser.
type DefaultParser struct{}

// NewDefaultParser returns a parser instance.
func NewDefaultParser() *DefaultParser { return &DefaultParser{} }

func (p *DefaultParser) Parse(source string) (NamespacePolicy, error) {
	trimmed := strings.TrimSpace(source)
	if strings.HasPrefix(trimmed, "{") {
		return p.ParseJSON(trimmed, nil)
	}
	return p.ParseJSON(dslToJSON(trimmed), nil)
}

func (p *DefaultParser) ParseJSON(jsonStr string, sch *schema.KdbSchema) (NamespacePolicy, error) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &raw); err != nil {
		return NamespacePolicy{}, err
	}
	ns := "default"
	if v, ok := raw["namespaceId"].(string); ok {
		ns = v
	}
	mode := NamespaceModeMutable
	if v, ok := raw["mode"].(string); ok && strings.EqualFold(v, "APPEND_ONLY") {
		mode = NamespaceModeAppendOnly
	}
	history := HistoryModeFull
	if v, ok := raw["history"].(string); ok && strings.EqualFold(v, "NONE") {
		history = HistoryModeNone
	}
	conflict := transaction.ConflictPolicyStrict
	if v, ok := raw["conflict"].(string); ok {
		switch strings.ToUpper(v) {
		case "APPEND_ONLY":
			conflict = transaction.ConflictPolicyAppendOnly
		case "LAST_WRITE":
			conflict = transaction.ConflictPolicyLastWrite
		}
	}
	squash := SquashModeAuto
	if comp, ok := raw["compaction"].(map[string]any); ok {
		if v, ok := comp["squashAfter"].(string); ok && strings.EqualFold(v, "NEVER") {
			squash = SquashModeNever
		}
	}
	return NamespacePolicy{
		NamespaceID: ns,
		Schema:      sch,
		Mode:        mode,
		History:     history,
		Conflict:    conflict,
		Compaction: CompactionPolicy{
			KeepTagged:        true,
			KeepBranchPoints:  true,
			SquashAfter:       squash,
			RetainGranularity: DefaultRetainGranularity(),
		},
		Tiers:    DefaultTierPolicy(),
		Revision: 1,
	}, nil
}

func dslToJSON(dsl string) string {
	ns := "default"
	mode := "MUTABLE"
	history := "FULL"
	conflict := "STRICT"
	squash := "AUTO"
	if m := regexp.MustCompile(`namespace\s*\(\s*"([^"]+)"\s*\)`).FindStringSubmatch(dsl); len(m) > 1 {
		ns = m[1]
	}
	if strings.Contains(strings.ToUpper(dsl), "APPEND_ONLY") {
		mode = "APPEND_ONLY"
	}
	if strings.Contains(strings.ToLower(dsl), "history = none") {
		history = "NONE"
	}
	if strings.Contains(strings.ToUpper(dsl), "ALWAYS_ACCEPT") || strings.Contains(strings.ToUpper(dsl), "APPEND_ONLY") {
		conflict = "APPEND_ONLY"
	}
	if strings.Contains(strings.ToUpper(dsl), "LAST_WRITE") {
		conflict = "LAST_WRITE"
	}
	if strings.Contains(strings.ToLower(dsl), "squashafter = never") {
		squash = "NEVER"
	}
	return `{"namespaceId":"` + ns + `","mode":"` + mode + `","history":"` + history + `","conflict":"` + conflict + `","compaction":{"squashAfter":"` + squash + `"}}`
}
