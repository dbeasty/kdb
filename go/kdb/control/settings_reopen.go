package control

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/embed"
)

// Settings that are read when a namespace opens, changed by reopening it.
//
// config.MutabilityNamespaceReopen has been a documented class with no
// implementation since it was written - the refusal said so. These are the
// settings in it, each paired with the field it writes, so a change is
// applied to the options actually in force rather than to a set assembled
// from somewhere else. That distinction is the whole reason the reopener
// takes an edit function: rebuilding options from the descriptors would
// revert every setting a previous change had already made.
//
// A reopen is not free. The namespace is unroutable while it happens, and
// the outcome reports how long for, because that is the price and an
// operator deciding whether to do it again needs to see it.

// reopenSetting is one setting changed by reopening the namespace.
type reopenSetting struct {
	// parse validates the requested value and returns the concrete type edit expects.
	parse func(any) (any, error)
	// edit writes the parsed value into a namespace's storage options.
	edit func(*embed.StorageOptions, any)
}

// reopenSettings is the whole set. Adding one means naming the StorageOptions field it writes -
// there is no generic path, deliberately: a setting nobody has mapped is a setting that would
// silently do nothing.
func reopenSettings() map[string]reopenSetting {
	return map[string]reopenSetting{
		"cache.documentBytes": {
			parse: parseNonNegativeInt64("cache.documentBytes"),
			edit:  func(o *embed.StorageOptions, v any) { o.DocumentCacheBytes = v.(int64) },
		},
		"cache.historyTreeBytes": {
			parse: parseNonNegativeInt64("cache.historyTreeBytes"),
			edit:  func(o *embed.StorageOptions, v any) { o.HistoryTreeCacheBytes = v.(int64) },
		},
		"storage.checkpoints": {
			parse: parseOnOff("storage.checkpoints"),
			edit: func(o *embed.StorageOptions, v any) {
				// The setting is "checkpoints on", the option is "checkpoints
				// disabled" - inverted here rather than in the help text,
				// which should read the way an operator thinks.
				o.DisableCheckpoints = !v.(bool)
			},
		},
		"graph.ancestryPruning": {
			parse: parseOnOff("graph.ancestryPruning"),
			edit:  func(o *embed.StorageOptions, v any) { o.Graph.AncestryPruning = v.(bool) },
		},
		"history.anchorInterval": {
			parse: parseNonNegativeInt("history.anchorInterval", 0),
			edit:  func(o *embed.StorageOptions, v any) { o.Graph.AnchorInterval = v.(int) },
		},
	}
}

func parseOnOff(key string) func(any) (any, error) {
	return func(v any) (any, error) {
		switch t := v.(type) {
		case bool:
			return t, nil
		case string:
			switch t {
			case "on", "true", "yes", "1":
				return true, nil
			case "off", "false", "no", "0":
				return false, nil
			}
		}
		return nil, fmt.Errorf("%s must be on or off", key)
	}
}

// applyReopenChange validates and applies one reopen-class change, filling in the outcome.
//
// Split from the patch loop because the reporting differs: a successful reopen has a cost the
// caller should see, and a dry run of one has to say that it would interrupt service rather than
// only that the value is valid.
func (s *Server) applyReopenChange(out *changeOutcome, change settingChange, setting reopenSetting, dryRun bool) {
	parsed, err := setting.parse(change.Value)
	if err != nil {
		out.Refused = err.Error()
		return
	}
	if s.opts.Reopener == nil {
		out.Refused = "this setting is read when a namespace is opened, and this process was not " +
			"given a way to reopen one; restart to change it"
		return
	}
	if dryRun {
		out.Applied = true
		out.To = parsed
		out.Warnings = fmt.Sprintf(
			"dry run: nothing was changed. Applying this reopens %d namespace(s), each briefly "+
				"unavailable while it does", len(s.opts.Reopener.ReopenableNamespaces()))
		return
	}
	note, err := s.applyByReopening(change.Key, setting, parsed)
	if err != nil {
		out.Refused = err.Error()
		return
	}
	out.Applied = true
	out.To = parsed
	out.Warnings = note
}

// applyByReopening changes one namespace-reopen setting on every namespace that can take it.
//
// Reports the reopen's cost in the outcome's warning rather than swallowing it: an operator who
// has just made every namespace briefly unavailable should be told, not left to infer it from a
// latency graph.
func (s *Server) applyByReopening(key string, setting reopenSetting, value any) (string, error) {
	if s.opts.Reopener == nil {
		return "", fmt.Errorf(
			"%s is read when a namespace is opened, and this process was not given a way to "+
				"reopen one; restart to change it", key)
	}
	namespaces := s.opts.Reopener.ReopenableNamespaces()
	if len(namespaces) == 0 {
		return "", fmt.Errorf("%s cannot be changed here: no namespace can be reopened", key)
	}
	var total float64
	for _, ns := range namespaces {
		took, err := s.opts.Reopener.ReopenNamespace(ns, func(o *embed.StorageOptions) {
			setting.edit(o, value)
		})
		if err != nil {
			// Stop at the first failure. Pressing on would leave some
			// namespaces reopened and some not, under a setting the caller
			// believes applied everywhere.
			return "", fmt.Errorf("reopening %s to change %s: %w", ns, key, err)
		}
		total += took.Seconds()
	}
	return fmt.Sprintf(
		"applied by reopening %d namespace(s), which were unavailable for %.0fms in total",
		len(namespaces), total*1000), nil
}
