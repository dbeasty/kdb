package control

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/config"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Changing a setting on a running server.
//
// Only a few settings can move, and which ones is not a matter of taste: it is whichever ones the
// engine has a setter for that is documented safe to call while serving traffic. That set is small
// and this file is the whole of it. Everything else is reported with its mutability class and
// refused, which is more useful than a generic "cannot change that" because the class says what
// *would* work - a restart, or reopening the namespace.
//
// Two rules shape the API:
//
//   - A change is applied to the live process. Whether it is also written back to the config file
//     is a separate, opt-in decision (see Options.AllowSettingsPersist), because in a
//     GitOps-managed deployment that file belongs to a deployment tool and a server editing it is
//     a surprise, not a feature.
//   - Anything applied and not persisted is drift, and drift is reported. A change that silently
//     vanishes on the next restart is a trap, so /v1/settings/drift exists and the UI shows it.

// settingChange is one requested change.
type settingChange struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

type patchSettingsRequest struct {
	Changes []settingChange `json:"changes"`
	// ExpectRevision is a compare-and-swap against the settings revision the caller last read, so
	// two operators cannot silently overwrite each other. Zero skips the check.
	ExpectRevision int64 `json:"expectRevision"`
	// Persist also writes the change to the config file, when the deployment allows it.
	Persist bool `json:"persist"`
	// DryRun validates and reports what would happen, changing nothing.
	DryRun bool `json:"dryRun"`
}

// changeOutcome is what happened to one requested change.
type changeOutcome struct {
	Key      string `json:"key"`
	Applied  bool   `json:"applied"`
	From     any    `json:"from,omitempty"`
	To       any    `json:"to,omitempty"`
	Refused  string `json:"refused,omitempty"`
	Restart  string `json:"mutability,omitempty"`
	Persist  string `json:"persisted,omitempty"`
	Warnings string `json:"warning,omitempty"`
}

// liveSetting is a setting this process can change while it runs.
type liveSetting struct {
	// parse turns the requested JSON value into the concrete value, rejecting anything the engine
	// would not accept. Validation happens here rather than inside apply so a dry run is exact.
	parse func(any) (any, error)
	// apply installs the parsed value. It runs under the server's settings lock, so two concurrent
	// patches cannot interleave halfway through a multi-field setter.
	apply func(s *Server, value any) error
}

// liveSettings is the whole set. Adding one means pointing at an engine setter whose own
// documentation says it is safe under load - not merely at a field that happens to be writable.
func liveSettings() map[string]liveSetting {
	return map[string]liveSetting{
		// The memory trio go through one setter, KdbServerRuntime.SetMemoryBudget, whose comment
		// says it is "safe to call again to change it, and safe to call concurrently with in-flight
		// work - grants already issued against the previous Admission release against that same
		// instance". Because it takes all four values at once, changing one reads the other three
		// from the current descriptors; see applyMemory.
		"memory.budgetMB": {
			parse: parseNonNegativeInt("memory.budgetMB", -1),
			apply: func(s *Server, v any) error { return s.applyMemory("memory.budgetMB", v) },
		},
		"memory.reserveMB": {
			parse: parseNonNegativeInt("memory.reserveMB", 0),
			apply: func(s *Server, v any) error { return s.applyMemory("memory.reserveMB", v) },
		},
		"governance.scanRowBudget": {
			parse: parseNonNegativeInt("governance.scanRowBudget", 0),
			apply: func(s *Server, v any) error { return s.applyMemory("governance.scanRowBudget", v) },
		},
		"log.level": {
			parse: func(v any) (any, error) {
				name, ok := v.(string)
				if !ok {
					return nil, fmt.Errorf("log.level must be a string")
				}
				if _, err := config.ParseLogLevel(name); err != nil {
					return nil, err
				}
				return strings.ToLower(strings.TrimSpace(name)), nil
			},
			apply: func(s *Server, v any) error {
				if s.opts.LogLevel == nil {
					return fmt.Errorf("this process did not hand the control plane its log level holder")
				}
				lvl, err := config.ParseLogLevel(v.(string))
				if err != nil {
					return err
				}
				s.opts.LogLevel.Set(lvl)
				return nil
			},
		},
		// The maintenance cadence. Every one of these goes through a setter on
		// MaintenanceScheduler whose own doc comment says it is safe to call while the loop is
		// running and while a pass is in flight - a change lands on the next wake-up and never
		// interrupts a pass that is already reclaiming.
		"governance.maintenanceInterval": {
			parse: parsePositiveDuration("governance.maintenanceInterval"),
			apply: func(s *Server, v any) error {
				return s.eachScheduler("governance.maintenanceInterval", func(sched *embed.MaintenanceScheduler) error {
					return sched.SetInterval(v.(time.Duration))
				})
			},
		},
		"governance.maintenanceSweep": {
			parse: parsePositiveDuration("governance.maintenanceSweep"),
			apply: func(s *Server, v any) error {
				return s.eachScheduler("governance.maintenanceSweep", func(sched *embed.MaintenanceScheduler) error {
					return sched.SetSweep(v.(time.Duration))
				})
			},
		},
		"governance.maintenanceMaxDefer": {
			parse: func(v any) (any, error) {
				f, ok := v.(float64)
				if !ok {
					return nil, fmt.Errorf("governance.maintenanceMaxDefer must be a number")
				}
				if f != float64(int(f)) {
					return nil, fmt.Errorf("governance.maintenanceMaxDefer must be a whole number")
				}
				n := int(f)
				if n == 0 {
					return nil, fmt.Errorf(
						"governance.maintenanceMaxDefer of 0 is ambiguous: use a positive count to " +
							"bound how long write load may postpone a pass, or -1 to never force one " +
							"through (which risks a busy server never reclaiming)")
				}
				return n, nil
			},
			apply: func(s *Server, v any) error {
				return s.eachScheduler("governance.maintenanceMaxDefer", func(sched *embed.MaintenanceScheduler) error {
					return sched.SetMaxDefer(v.(int))
				})
			},
		},
		// The reclaim axis. Applied to the scheduler *and* the namespace: the preset sets the
		// loop's cadence, and the mode is what the pass itself consults before deleting
		// anything, so setting only one of the two would leave a namespace whose cadence and
		// behaviour disagree.
		"reclaim.mode": {
			parse: func(v any) (any, error) {
				text, ok := v.(string)
				if !ok {
					return nil, fmt.Errorf("reclaim.mode must be a string: manual, immediate, balanced or lazy")
				}
				m, err := storage.ParseReclaimMode(text)
				if err != nil {
					return nil, err
				}
				if m == storage.ReclaimUnset {
					return nil, fmt.Errorf("reclaim.mode cannot be set back to unset; name the mode you want")
				}
				return m, nil
			},
			apply: func(s *Server, v any) error { return s.applyReclaimMode(v.(storage.ReclaimMode)) },
		},
		// EmbeddedKdbRuntime.SetRetentionWindow, which refuses a full-history namespace rather
		// than storing a window that would never be consulted. Unlike the cadence settings this
		// one changes what is *deleted*, and shortening it makes already-written segments
		// eligible on the very next pass - see the warning applyRetention attaches.
		"retain.duration": {
			parse: func(v any) (any, error) {
				text, ok := v.(string)
				if !ok {
					return nil, fmt.Errorf("retain.duration must be a string such as \"24h\", \"7d\" or \"0\"")
				}
				d, err := storage.ParseRetentionDuration(strings.TrimSpace(text))
				if err != nil {
					return nil, err
				}
				return d, nil
			},
			apply: func(s *Server, v any) error {
				return s.applyRetention(func(w *storage.RetentionWindow) { w.Duration = v.(time.Duration) })
			},
		},
		"retain.commits": {
			parse: parseNonNegativeInt64("retain.commits"),
			apply: func(s *Server, v any) error {
				return s.applyRetention(func(w *storage.RetentionWindow) { w.Commits = v.(int64) })
			},
		},
		// InMemoryCommitDag.SetOperationsBudget: "Safe to call while the DAG is being read and
		// written; it evicts under the same lock every other retention path takes." It no-ops
		// without an operations loader installed, which is reported rather than hidden.
		"cache.commitOpsBytes": {
			parse: parseNonNegativeInt64("cache.commitOpsBytes"),
			apply: func(s *Server, v any) error {
				bytes := v.(int64)
				if bytes <= 0 {
					return fmt.Errorf("cache.commitOpsBytes must be a positive byte count; " +
						"there is no way to ask for the derived default back without a restart")
				}
				applied := 0
				src := s.namespaces()
				for _, ns := range src.Namespaces() {
					rt, ok := src.Runtime(ns)
					if !ok {
						continue
					}
					d, err := s.commitDAGFor(rt)
					if err != nil {
						continue
					}
					d.SetOperationsBudget(bytes)
					applied++
				}
				if applied == 0 {
					return fmt.Errorf("no namespace accepted the budget")
				}
				return nil
			},
		},
	}
}

// parsePositiveDuration reads a duration string such as "5m". Zero is refused rather than taken
// as "disable": from the control plane a 0 cadence reads as "stop maintaining", and stopping a
// loop is a lifecycle decision that belongs to whoever started it, not to a settings patch.
func parsePositiveDuration(key string) func(any) (any, error) {
	return func(v any) (any, error) {
		text, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("%s must be a duration string such as \"5m\"", key)
		}
		d, err := time.ParseDuration(strings.TrimSpace(text))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("%s must be positive; it cannot be used to turn maintenance off", key)
		}
		return d, nil
	}
}

// eachScheduler applies fn to every maintenance loop this process is running, and refuses rather
// than reporting a hollow success when there are none - a setting that "applied" to nothing would
// show as in force while changing nothing at all.
func (s *Server) eachScheduler(key string, fn func(*embed.MaintenanceScheduler) error) error {
	if s.opts.Maintenance == nil {
		return fmt.Errorf(
			"%s cannot be changed here: this process is not running any maintenance loops "+
				"(it was started with maintenance-interval=0, or its control plane was not given them)", key)
	}
	scheds := s.opts.Maintenance.Schedulers()
	applied := 0
	for _, sched := range scheds {
		if sched == nil {
			continue
		}
		if err := fn(sched); err != nil {
			return err
		}
		applied++
	}
	if applied == 0 {
		return fmt.Errorf("%s cannot be changed here: no namespace is running a maintenance loop", key)
	}
	return nil
}

// applyReclaimMode sets the reclaim preset on every maintenance loop and the mode on every
// namespace behind it.
//
// Both halves, because they answer different questions and a namespace whose cadence says
// "immediate" while its mode says "manual" would tick busily and reclaim nothing. The scheduler
// carries the mode down to the namespace itself (ApplyReclaimMode), so a process running loops
// needs only the first call; a process running none still sets the mode, which is what a later
// explicit compaction and any future loop will read.
func (s *Server) applyReclaimMode(mode storage.ReclaimMode) error {
	applied := 0
	if s.opts.Maintenance != nil {
		for _, sched := range s.opts.Maintenance.Schedulers() {
			if sched == nil {
				continue
			}
			if err := sched.ApplyReclaimMode(mode); err != nil {
				return err
			}
			applied++
		}
	}
	if applied > 0 {
		return nil
	}
	// No loops: set the mode directly, so the setting still means something on a process that
	// reclaims only when asked.
	src := s.namespaces()
	if src == nil {
		return fmt.Errorf("this control plane has no namespaces to set a reclaim mode on")
	}
	for _, ns := range src.Namespaces() {
		rt, ok := src.Runtime(ns)
		if !ok || rt == nil || rt.Runtime == nil {
			continue
		}
		if err := rt.Runtime.SetReclaimMode(mode); err != nil {
			continue
		}
		applied++
	}
	if applied == 0 {
		return fmt.Errorf("no namespace accepted the reclaim mode")
	}
	return nil
}

// applyRetention edits the retention window of every namespace that can hold one.
//
// Read-modify-write against each namespace's *current* window rather than against one assembled
// from the descriptors: retain.duration and retain.commits are two halves of one value, and a
// namespace whose window has already been changed live must not have that change reverted by a
// patch to the other half.
//
// A namespace that refuses (history=full) is skipped rather than failing the whole patch, but at
// least one has to accept or this reports failure - see eachScheduler for the same reasoning.
func (s *Server) applyRetention(edit func(*storage.RetentionWindow)) error {
	src := s.namespaces()
	if src == nil {
		return fmt.Errorf("this control plane has no namespaces to apply a retention window to")
	}
	applied, refused := 0, 0
	var lastRefusal error
	for _, ns := range src.Namespaces() {
		rt, ok := src.Runtime(ns)
		if !ok || rt == nil || rt.Runtime == nil {
			continue
		}
		w := rt.Runtime.RetentionWindow()
		edit(&w)
		if err := rt.Runtime.SetRetentionWindow(w); err != nil {
			refused++
			lastRefusal = err
			continue
		}
		applied++
	}
	if applied == 0 {
		if lastRefusal != nil {
			return fmt.Errorf("no namespace accepted the retention window: %w", lastRefusal)
		}
		return fmt.Errorf("no namespace accepted the retention window")
	}
	return nil
}

func parseNonNegativeInt(key string, floor int) func(any) (any, error) {
	return func(v any) (any, error) {
		f, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("%s must be a number", key)
		}
		if f != float64(int(f)) {
			return nil, fmt.Errorf("%s must be a whole number", key)
		}
		n := int(f)
		if n < floor {
			return nil, fmt.Errorf("%s must be at least %d", key, floor)
		}
		return n, nil
	}
}

func parseNonNegativeInt64(key string) func(any) (any, error) {
	return func(v any) (any, error) {
		f, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("%s must be a number", key)
		}
		if f < 0 {
			return nil, fmt.Errorf("%s must not be negative", key)
		}
		return int64(f), nil
	}
}

// applyMemory installs the memory trio.
//
// SetMemoryBudget takes budget, reject fraction, reserve and scan-row budget together, so changing
// one means supplying the other two from what is currently in force. They are read from the
// descriptors, which are the record of what this process is running on.
func (s *Server) applyMemory(changing string, value any) error {
	budgetMB := s.currentInt("memory.budgetMB")
	reserveMB := s.currentInt("memory.reserveMB")
	scanRows := s.currentInt("governance.scanRowBudget")
	switch changing {
	case "memory.budgetMB":
		budgetMB = value.(int)
	case "memory.reserveMB":
		reserveMB = value.(int)
	case "governance.scanRowBudget":
		scanRows = value.(int)
	}
	if budgetMB < 0 {
		// Governance off. SetMemoryBudget reads a zero limit as "disable entirely", which is what
		// -1 means on the flag.
		budgetMB = 0
	}

	applied := 0
	src := s.namespaces()
	for _, ns := range src.Namespaces() {
		rt, ok := src.Runtime(ns)
		if !ok {
			continue
		}
		rt.SetMemoryBudget(uint64(budgetMB)*1024*1024, memoryRejectFraction,
			int64(reserveMB)*1024*1024, int64(scanRows))
		applied++
	}
	if applied == 0 {
		return fmt.Errorf("no namespace accepted the budget")
	}
	return nil
}

// memoryRejectFraction is the zone entry point the service uses at startup (0.85). Kept identical
// so a live change does not silently move the pressure zones as a side effect of adjusting a
// budget.
const memoryRejectFraction = 0.85

func (s *Server) currentInt(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, d := range s.settings {
		if d.Key != key {
			continue
		}
		switch v := d.Value.(type) {
		case int:
			return v
		case int64:
			return int(v)
		case float64:
			return int(v)
		}
	}
	return 0
}

// handlePatchSettings applies changes, or reports exactly why each was refused.
func (s *Server) handlePatchSettings(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	var req patchSettingsRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "could not read request body: "+err.Error())
		return
	}
	if len(req.Changes) == 0 {
		writeError(w, http.StatusBadRequest, "bad_request", `"changes" is required and must not be empty`)
		return
	}
	if !s.opts.AllowWrites {
		writeError(w, http.StatusForbidden, "read_only",
			"this control plane is read-only; start the service with --control-write to change settings")
		return
	}
	if req.Persist && !s.opts.AllowSettingsPersist {
		writeError(w, http.StatusForbidden, "persist_disabled",
			"this deployment does not let the server write its own config file. The change can still "+
				"be applied to the running process - drop \"persist\" - and it will show up under "+
				"/v1/settings/drift so it is not silently lost on the next restart.")
		return
	}

	// Serialize the whole patch: the memory trio share one setter, and two concurrent patches each
	// reading the other's half-applied state would install a combination neither asked for.
	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	if req.ExpectRevision != 0 && req.ExpectRevision != s.SettingsRevision() {
		writeError(w, http.StatusConflict, "revision_moved",
			fmt.Sprintf("settings have changed since you read them (expected revision %d, now %d): "+
				"re-read and review before applying", req.ExpectRevision, s.SettingsRevision()))
		return
	}

	live := liveSettings()
	outcomes := make([]changeOutcome, 0, len(req.Changes))
	// Persisting happens once, after the loop: a patch is one operator action, and rewriting the
	// config file per key would leave intermediate states on disk that nobody asked for.
	var toPersist []persistRequest
	changed := 0
	for _, change := range req.Changes {
		out := changeOutcome{Key: change.Key, To: change.Value}
		descriptor, known := s.descriptor(change.Key)
		if !known {
			out.Refused = "no such setting"
			outcomes = append(outcomes, out)
			continue
		}
		out.From = descriptor.Value
		out.Restart = string(descriptor.Mutability)

		setter, mutable := live[change.Key]
		if !mutable {
			out.Refused = refusalFor(descriptor.Mutability)
			outcomes = append(outcomes, out)
			continue
		}
		parsed, err := setter.parse(change.Value)
		if err != nil {
			out.Refused = err.Error()
			outcomes = append(outcomes, out)
			continue
		}
		if req.DryRun {
			// Counted as a change so the status logic below can tell "nothing would apply" from
			// "everything would" - a dry run that hides an invalid value is worse than no dry run.
			out.Applied = true
			out.To = parsed
			out.Warnings = "dry run: nothing was changed"
			changed++
			if req.Persist {
				// The persist checks run in a dry run too. Whether a value can be written down is
				// most of what an operator is reviewing, and finding out afterwards that it could
				// not defeats the point of reviewing at all.
				toPersist = append(toPersist, persistRequest{key: change.Key, value: parsed})
			}
			outcomes = append(outcomes, out)
			continue
		}
		if err := setter.apply(s, parsed); err != nil {
			out.Refused = err.Error()
			outcomes = append(outcomes, out)
			continue
		}
		s.recordApplied(change.Key, parsed, principal)
		out.Applied = true
		out.To = parsed
		changed++
		if req.Persist {
			toPersist = append(toPersist, persistRequest{key: change.Key, value: parsed})
		}
		outcomes = append(outcomes, out)
	}

	// The live change stands whether or not it could be written down: the operator asked for both,
	// and refusing to apply because the file is unwritable would leave them with neither. What
	// they must not be left with is the impression that it was persisted, so every key that asked
	// carries the answer, and persistedAll says it once for a caller that would rather not walk
	// the list.
	persistedAll := len(toPersist) > 0
	if len(toPersist) > 0 {
		results := s.persistApplied(toPersist, req.DryRun)
		for i := range outcomes {
			result, ok := results[outcomes[i].Key]
			if !ok {
				continue
			}
			outcomes[i].Persist = result.note
			if !result.written {
				persistedAll = false
				continue
			}
			if !req.DryRun {
				s.recordPersisted(outcomes[i].Key, outcomes[i].To)
			}
		}
	}

	// Partial success stays a 200 with per-key outcomes, because something did happen and the
	// caller needs to see which parts. Nothing applying at all is a 400: the request was wrong
	// rather than raced, and that includes a dry run, where surfacing the rejection is the point.
	status := http.StatusOK
	if changed == 0 {
		status = http.StatusBadRequest
	}
	body := map[string]any{
		"revision": s.SettingsRevision(),
		"changes":  outcomes,
		"dryRun":   req.DryRun,
		"drift":    s.driftKeys(),
	}
	if req.Persist {
		body["persistedAll"] = persistedAll
		body["configPath"] = s.opts.ConfigPath
	}
	writeJSON(w, status, body)
}

// refusalFor explains what would work instead, from the setting's mutability class. "Cannot change
// that" on its own sends an operator looking; naming the class tells them where to look.
func refusalFor(m config.Mutability) string {
	switch m {
	case config.MutabilityLive:
		// Reachable, and the message has to be honest about the mismatch: the setting's class says
		// the engine could change it under load, but this control plane has no setter wired for it.
		// memory.limitMB is the one today - a deprecated alias whose live path is memory.budgetMB.
		return "this setting can be changed on a running server in principle, but this control " +
			"plane has no setter for it; if it is a deprecated alias, change the setting it aliases"
	case config.MutabilityNewConnections:
		return "this setting is read when a listener or connection is created, so it cannot be " +
			"changed for the ones already running; restart to change it"
	case config.MutabilityNamespaceReopen:
		return "this setting is read when a namespace is opened; it needs that namespace closed " +
			"and reopened, which this control plane cannot yet do"
	case config.MutabilityImmutable:
		return "this setting describes what is already on disk and disagreeing with it is refused " +
			"at open; changing it is a migration (kdb-inspect migrate-history), not a setting"
	default:
		return "this setting is read once at startup; restart to change it"
	}
}

// handleSettingByKey returns one descriptor with its full help text.
func (s *Server) handleSettingByKey(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	key := r.PathValue("key")
	d, ok := s.descriptor(key)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no setting called "+key)
		return
	}
	_, mutable := liveSettings()[key]
	writeJSON(w, http.StatusOK, map[string]any{
		"setting":     d.Redact(),
		"liveMutable": mutable,
		"refusal":     refusalIfImmutable(d, mutable),
	})
}

func refusalIfImmutable(d config.SettingDescriptor, mutable bool) any {
	if mutable {
		return nil
	}
	return refusalFor(d.Mutability)
}

// handleSettingsDrift reports what a restart would undo.
func (s *Server) handleSettingsDrift(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	type item struct {
		Key       string `json:"key"`
		Running   any    `json:"running"`
		AtStartup any    `json:"atStartup"`
		ChangedBy string `json:"changedBy,omitempty"`
	}
	s.mu.RLock()
	startup := s.startupSettings
	current := s.settings
	s.mu.RUnlock()

	byKey := map[string]config.SettingDescriptor{}
	for _, d := range startup {
		byKey[d.Key] = d
	}
	out := make([]item, 0)
	for _, d := range current {
		was, ok := byKey[d.Key]
		if !ok || fmt.Sprint(was.Value) == fmt.Sprint(d.Value) {
			continue
		}
		out = append(out, item{
			Key: d.Key, Running: d.Redact().Value, AtStartup: was.Redact().Value,
			ChangedBy: d.SourceDetail,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	writeJSON(w, http.StatusOK, map[string]any{
		"revision": s.SettingsRevision(),
		"drift":    out,
		"note": "these values differ from the ones this process started with, so a restart would " +
			"undo them. Persisting a change to the config file is a separate, opt-in step.",
	})
}

func (s *Server) driftKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	byKey := map[string]any{}
	for _, d := range s.startupSettings {
		byKey[d.Key] = d.Value
	}
	var out []string
	for _, d := range s.settings {
		if was, ok := byKey[d.Key]; ok && fmt.Sprint(was) != fmt.Sprint(d.Value) {
			out = append(out, d.Key)
		}
	}
	sort.Strings(out)
	return out
}

func (s *Server) descriptor(key string) (config.SettingDescriptor, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, d := range s.settings {
		if d.Key == key {
			return d, true
		}
	}
	return config.SettingDescriptor{}, false
}

// recordApplied updates the reported value and attributes the change, so /v1/settings shows what
// is actually in force and who put it there rather than what the process started with.
func (s *Server) recordApplied(key string, value any, principal auth.Principal) {
	s.mu.Lock()
	defer s.mu.Unlock()
	who := principal.ID
	if who == "" {
		who = "anonymous"
	}
	for i := range s.settings {
		if s.settings[i].Key != key {
			continue
		}
		s.settings[i].Value = value
		s.settings[i].Source = config.SourceRuntime
		s.settings[i].SourceDetail = fmt.Sprintf("changed at %s by %s",
			s.opts.Now().UTC().Format(time.RFC3339), who)
		break
	}
	s.revision++
}

// SettingsRevision is the counter a caller compares against with expectRevision.
func (s *Server) SettingsRevision() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revision
}

var _ = strconv.Itoa
var _ = slog.LevelInfo
