package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// Writing a setting back to the config file.
//
// This is the second half of docs/kdb-control-ui-plan.md §7.4. A setting changed on a running
// server and not written down vanishes at the next restart, which is a trap; the control plane
// reports that as drift, and this is how an operator makes a change stick instead.
//
// Two properties matter more than convenience here, because the failure mode of getting them
// wrong is a server that does not come back up:
//
//   - The file is replaced atomically, and only after the bytes about to be installed have been
//     read back through LoadServiceFile. A half-written or unparseable config file is not a
//     degraded state, it is a process that exits at startup.
//   - Only fields that are actually set are written. Every ServiceFile field is a pointer without
//     omitempty, so a naive round-trip would turn a four-line config into thirty lines of nulls -
//     semantically identical and unrecognisable to whoever has to read it next.

// PersistableInFile reports whether a setting can be written to a config file at all.
//
// False for two different reasons, and the caller should say which: the setting has no config-file
// field (cache.commitOpsBytes and the rest of the environment-only surface), or it has one but no
// writer because it cannot be changed on a running process anyway.
func PersistableInFile(key string) bool {
	for _, spec := range serviceSpecs() {
		if spec.key == key {
			return spec.setInFile != nil
		}
	}
	return false
}

// SetInFile installs value into f under key, without writing anything to disk.
//
// value is the parsed Go value the control plane applied - an int for a byte or MiB count, a
// string for an enumerated name - not the JSON it arrived as.
func SetInFile(f *ServiceFile, key string, value any) error {
	if f == nil {
		return fmt.Errorf("no config file to write %s into", key)
	}
	for _, spec := range serviceSpecs() {
		if spec.key != key {
			continue
		}
		if spec.setInFile == nil {
			return fmt.Errorf("%s cannot be written to a config file", key)
		}
		return spec.setInFile(f, value)
	}
	return fmt.Errorf("no setting called %s", key)
}

// WriteServiceFile writes f to path, atomically, and only if the result parses.
//
// The file it produces is the same shape LoadServiceFile reads: an object holding just the fields
// that are set. Keys come out in alphabetical order, which is not necessarily the order they were
// in - there is no way to preserve that through a decode, and a stable order is worth more than
// an arbitrary one.
func WriteServiceFile(path string, f *ServiceFile) error {
	if f == nil {
		return fmt.Errorf("nothing to write")
	}
	body, err := marshalServiceFile(f)
	if err != nil {
		return err
	}

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".kdb-config-*.json")
	if err != nil {
		return fmt.Errorf("could not create a temporary file next to %s: %w", path, err)
	}
	tmpName := tmp.Name()
	// Any early return from here on leaves nothing behind. Remove is harmless after the rename,
	// which is why it is unconditional rather than guarded by a success flag.
	defer os.Remove(tmpName)

	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		return fmt.Errorf("could not write %s: %w", tmpName, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("could not flush %s: %w", tmpName, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("could not close %s: %w", tmpName, err)
	}

	// Read it back the way startup will. This is the check that matters: a config file this
	// process cannot parse is a config file the next boot cannot parse either, and the operator
	// would find out at the worst possible moment.
	if _, err := LoadServiceFile(tmpName); err != nil {
		return fmt.Errorf("refusing to install a config file that does not parse: %w", err)
	}

	// Carry over the existing file's permissions. A config file deliberately made unreadable to
	// other users must not become world-readable because a setting was persisted.
	if info, err := os.Stat(path); err == nil {
		if err := os.Chmod(tmpName, info.Mode().Perm()); err != nil {
			return fmt.Errorf("could not match %s's permissions: %w", path, err)
		}
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("could not install %s: %w", path, err)
	}
	// The rename is only durable once the directory entry is. Without this the file can be back to
	// its old contents - or missing - after a crash, which is the case this whole feature exists
	// to guard against.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// marshalServiceFile renders f as the config file it would be read back from: set fields only,
// two-space indented, newline-terminated.
func marshalServiceFile(f *ServiceFile) ([]byte, error) {
	raw, err := json.Marshal(f)
	if err != nil {
		return nil, fmt.Errorf("could not encode the config: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("could not re-read the encoded config: %w", err)
	}
	pruneNulls(fields)

	// Encoded by hand rather than by marshalling the map, so the nested TLS block can be pruned
	// and indented like everything else and the output has one obvious shape.
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	buf.WriteString("{\n")
	for i, k := range keys {
		name, _ := json.Marshal(k)
		buf.WriteString("  ")
		buf.Write(name)
		buf.WriteString(": ")
		var indented bytes.Buffer
		if err := json.Indent(&indented, fields[k], "  ", "  "); err != nil {
			return nil, fmt.Errorf("could not format %s: %w", k, err)
		}
		buf.Write(indented.Bytes())
		if i < len(keys)-1 {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString("}\n")
	return buf.Bytes(), nil
}

// pruneNulls drops unset fields, recursing into nested objects so a TLS block that is present but
// half-filled does not carry nulls either.
func pruneNulls(fields map[string]json.RawMessage) {
	for k, v := range fields {
		if bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			delete(fields, k)
			continue
		}
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(v, &nested); err != nil {
			continue // not an object; leave it exactly as encoded
		}
		pruneNulls(nested)
		if len(nested) == 0 {
			delete(fields, k)
			continue
		}
		re, err := json.Marshal(nested)
		if err != nil {
			continue
		}
		fields[k] = re
	}
}

// setIntInFile builds a writer for an integer setting, rejecting anything below floor - which is
// the same bound the flag documents, so a persisted value cannot be one the next startup would
// refuse.
func setIntInFile(set func(*ServiceFile, int), floor int) func(*ServiceFile, any) error {
	return func(f *ServiceFile, v any) error {
		n, ok := toInt(v)
		if !ok {
			return fmt.Errorf("expected a whole number, got %T", v)
		}
		if n < floor {
			return fmt.Errorf("must be at least %d, got %d", floor, n)
		}
		set(f, n)
		return nil
	}
}

// toInt accepts the shapes a value arrives in: an int from a Go caller, an int64 or a float64 from
// one that went through JSON.
func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		if n != float64(int(n)) {
			return 0, false
		}
		return int(n), true
	}
	return 0, false
}
