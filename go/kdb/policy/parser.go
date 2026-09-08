package policy

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
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
	var expiry *DocumentExpiryPolicy
	if raw, ok := raw["documentExpiry"].(map[string]any); ok {
		field, _ := raw["fieldPath"].(string)
		if field == "" {
			return NamespacePolicy{}, fmt.Errorf("policy: documentExpiry.fieldPath is required")
		}
		expiry = &DocumentExpiryPolicy{FieldPath: field, SweepIntervalMillis: DefaultSweepIntervalMillis}
		if v, ok := raw["graceMillis"].(float64); ok {
			expiry.GraceMillis = int64(v)
		}
		if v, ok := raw["sweepIntervalMillis"].(float64); ok && v > 0 {
			expiry.SweepIntervalMillis = int64(v)
		}
	}
	retain, err := parseRetain(raw)
	if err != nil {
		return NamespacePolicy{}, err
	}
	return NamespacePolicy{
		NamespaceID:    ns,
		Retain:         retain,
		DocumentExpiry: expiry,
		Schema:         sch,
		Mode:           mode,
		History:        history,
		Conflict:       conflict,
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

// parseRetain reads the retain block that bounds HistoryModeNone's window:
//
//	"retain": { "duration": "24h", "commits": 10000 }
//
// An absent block is the zero window, which resolves to the default. A
// present but unparseable one is an error rather than a default, because
// this setting decides what gets deleted and a typo must never quietly
// become a shorter window than the operator wrote.
func parseRetain(raw map[string]any) (storage.RetentionWindow, error) {
	block, ok := raw["retain"].(map[string]any)
	if !ok {
		return storage.RetentionWindow{}, nil
	}
	var w storage.RetentionWindow
	if v, ok := block["duration"]; ok {
		s, isString := v.(string)
		if !isString {
			return storage.RetentionWindow{}, fmt.Errorf("policy: retain.duration must be a string like \"24h\"")
		}
		d, err := storage.ParseRetentionDuration(s)
		if err != nil {
			return storage.RetentionWindow{}, err
		}
		w.Duration = d
	}
	if v, ok := block["commits"]; ok {
		n, isNumber := v.(float64)
		if !isNumber || n < 0 {
			return storage.RetentionWindow{}, fmt.Errorf("policy: retain.commits must be a non-negative number")
		}
		w.Commits = int64(n)
	}
	return w, nil
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
	retain := ""
	if m := regexp.MustCompile(`(?is)retain\s*\{[^}]*duration\s*=\s*"([^"]+)"`).FindStringSubmatch(dsl); len(m) > 1 {
		retain += `"duration":"` + m[1] + `"`
	}
	if m := regexp.MustCompile(`(?is)retain\s*\{[^}]*commits\s*=\s*(\d+)`).FindStringSubmatch(dsl); len(m) > 1 {
		if retain != "" {
			retain += ","
		}
		retain += `"commits":` + m[1]
	}
	if retain != "" {
		retain = `,"retain":{` + retain + `}`
	}
	return `{"namespaceId":"` + ns + `","mode":"` + mode + `","history":"` + history + `","conflict":"` + conflict + `","compaction":{"squashAfter":"` + squash + `"}` + retain + `}`
}
