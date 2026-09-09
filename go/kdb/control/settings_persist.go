package control

import (
	"errors"
	"fmt"
	"os"

	"github.com/limidus/kdb/go/kdb/config"
)

// Making a live setting survive a restart.
//
// The live half of a setting change is settings_apply.go. This is the other half of §7.4: writing
// the new value into the --config file so the next startup resolves it too. A change applied and
// not written down is drift, and drift is a trap - the server is running on a value nobody can
// find in a file.
//
// Persisting is refused rather than half-done in four situations, and which one it is matters to
// whoever asked, so each says something different:
//
//   - The deployment did not opt in (--control-settings-persist). Handled in handlePatchSettings,
//     before anything is applied.
//   - There is no --config file to write to.
//   - The setting has no config-file field at all - the environment-only surface, where the
//     durable home is an environment variable this process cannot set for its successor.
//   - The setting is set on the command line or in the environment, which outrank the file. Here
//     the write would succeed and change nothing, which is worse than refusing: it looks like the
//     value is safe when the next restart will still take the flag's.

// persistRequest is one applied change waiting to be written down.
type persistRequest struct {
	key   string
	value any
}

// persistResult is what happened to one of them.
type persistResult struct {
	// note is what to tell the operator, whether or not it was written. Never empty.
	note string
	// written is true only if the value is now in the config file.
	written bool
}

// persistApplied writes the given changes to the config file in one replacement, and reports what
// happened per key.
//
// One write for the whole patch, not one per key: a patch is a single operator action, and
// rewriting the file three times would leave two intermediate states on disk that nobody asked
// for. If the write fails, nothing was persisted and every key is told the same reason - the live
// changes stand regardless, which is why the caller reports these as notes on an otherwise
// successful apply rather than as an error for the request.
//
// dryRun runs every check and reports what would happen, touching no file.
func (s *Server) persistApplied(reqs []persistRequest, dryRun bool) map[string]persistResult {
	out := make(map[string]persistResult, len(reqs))
	if len(reqs) == 0 {
		return out
	}

	// Sort the keys that can be written from the ones that cannot, before opening anything: a
	// patch where nothing is persistable should not rewrite the file at all.
	var writable []persistRequest
	for _, req := range reqs {
		if reason := s.persistRefusal(req.key); reason != "" {
			out[req.key] = persistResult{note: reason}
			continue
		}
		writable = append(writable, req)
	}
	if len(writable) == 0 {
		return out
	}

	file, err := s.configFileOnDisk()
	if err != nil {
		for _, req := range writable {
			out[req.key] = persistResult{note: "not written: " + err.Error() +
				". The change is live and appears under /v1/settings/drift"}
		}
		return out
	}
	// Staged into the freshly-read file first, so a value the file writer would reject stops this
	// before the file is touched.
	for _, req := range writable {
		if err := config.SetInFile(file, req.key, req.value); err != nil {
			for _, r := range writable {
				out[r.key] = persistResult{note: fmt.Sprintf(
					"not written: %s could not be encoded for the config file (%v), so nothing "+
						"was persisted. The changes are live and appear under /v1/settings/drift",
					req.key, err)}
			}
			return out
		}
	}
	if dryRun {
		for _, req := range writable {
			out[req.key] = persistResult{note: "would be written to " + s.opts.ConfigPath}
		}
		return out
	}
	if err := config.WriteServiceFile(s.opts.ConfigPath, file); err != nil {
		for _, req := range writable {
			out[req.key] = persistResult{note: fmt.Sprintf(
				"not written: %v. The change is live and appears under /v1/settings/drift", err)}
		}
		return out
	}
	for _, req := range writable {
		out[req.key] = persistResult{written: true, note: "written to " + s.opts.ConfigPath}
	}
	return out
}

// persistRefusal explains why a key cannot go in the config file, or returns "" if it can.
func (s *Server) persistRefusal(key string) string {
	if s.opts.ConfigPath == "" {
		return "not written: this process was started without --config, so there is no file to " +
			"write to. The change is live and appears under /v1/settings/drift"
	}
	if !config.PersistableInFile(key) {
		return "not written: " + key + " has no config-file field. Its durable home is its " +
			"environment variable (see /v1/settings for which), which this process cannot set " +
			"for its own successor. The change is live and appears under /v1/settings/drift"
	}
	// The precedence trap. The write would succeed and change nothing, because file < environment
	// < explicitly-set flag: the next startup would read the file, then have the flag overwrite
	// it, and the operator would conclude the value had been persisted when it had not.
	if was, ok := s.startupDescriptor(key); ok {
		switch was.Source {
		case config.SourceFlag:
			return "not written: " + key + " is set on the command line (" + was.SourceDetail +
				"), which outranks the config file, so writing it there would change nothing on " +
				"the next startup. Remove it from the command line first, or keep the change live"
		case config.SourceEnv:
			return "not written: " + key + " is set in the environment (" + was.SourceDetail +
				"), which outranks the config file, so writing it there would change nothing on " +
				"the next startup. Unset it first, or keep the change live"
		}
	}
	return ""
}

// configFileOnDisk re-reads the config file, so a persist merges into whatever is there now rather
// than into the copy this process parsed at startup.
//
// That matters when something else has edited the file since - a deployment tool, or an operator
// with an editor. Writing this process's startup copy back would silently revert their change.
func (s *Server) configFileOnDisk() (*config.ServiceFile, error) {
	if s.opts.ConfigPath == "" {
		return nil, errors.New("this process was started without --config, so there is no file to write to")
	}
	file, err := config.LoadServiceFile(s.opts.ConfigPath)
	if err == nil {
		return file, nil
	}
	// Gone since startup is recoverable: the operator asked for a value to be written down, and an
	// absent file can hold one. Unparseable is not - merging into a file this process cannot read
	// means guessing at what to keep, and the honest answer is to say what is wrong with it.
	if errors.Is(err, os.ErrNotExist) {
		return &config.ServiceFile{}, nil
	}
	return nil, fmt.Errorf("the config file at %s cannot be read (%v), so a change cannot be "+
		"merged into it", s.opts.ConfigPath, err)
}

func (s *Server) startupDescriptor(key string) (config.SettingDescriptor, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, d := range s.startupSettings {
		if d.Key == key {
			return d, true
		}
	}
	return config.SettingDescriptor{}, false
}

// recordPersisted moves a key's startup value to what was just written to the file.
//
// This is what makes the drift banner go away, and it is only correct because it runs after a
// successful write: drift is "what a restart would undo", and once the file holds the new value a
// restart would not undo it. Doing this on a failed or refused write would hide exactly the thing
// drift exists to show.
func (s *Server) recordPersisted(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.startupSettings {
		if s.startupSettings[i].Key != key {
			continue
		}
		s.startupSettings[i].Value = value
		s.startupSettings[i].Source = config.SourceConfigFile
		s.startupSettings[i].SourceDetail = "config file"
		break
	}
	// The running descriptor keeps its "runtime" source, because that is where the value came
	// from - but on its own that reads as ephemeral, which this value no longer is. The detail
	// line is where an operator scanning the settings table finds out it will survive a restart
	// without having to cross-reference the drift report.
	for i := range s.settings {
		if s.settings[i].Key != key {
			continue
		}
		s.settings[i].SourceDetail += ", written to the config file"
		break
	}
}
