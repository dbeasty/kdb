package backup

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// RestoredDefinitionsFile is the file a restore leaves in a namespace's directory with the
// definitions its backup carried (Manifest.Definitions). The server adopts them the first time
// it opens the namespace and removes the file, so a restored namespace comes back with its
// indexes and unique constraints (qtask production plan U5).
const RestoredDefinitionsFile = "restored-definitions.json"

// RestoredDefinitionsPath is where RestoredDefinitionsFile lives for namespaceID under dataRoot:
// beside the namespace's index directory (<dataRoot>/ns/<namespace>/).
func RestoredDefinitionsPath(dataRoot, namespaceID string) string {
	return filepath.Join(dataRoot, "ns", filepath.FromSlash(namespaceID), RestoredDefinitionsFile)
}

// WriteRestoredDefinitions leaves m's definitions for the server to adopt; a no-op when the
// backup carries none.
func WriteRestoredDefinitions(dataRoot string, m *Manifest) error {
	if m == nil || len(m.Definitions) == 0 {
		return nil
	}
	path := RestoredDefinitionsPath(dataRoot, m.NamespaceID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, m.Definitions, 0o644)
}

// ReadRestoredDefinitions returns the definitions a restore left for namespaceID, if any.
func ReadRestoredDefinitions(dataRoot, namespaceID string) (json.RawMessage, bool, error) {
	b, err := os.ReadFile(RestoredDefinitionsPath(dataRoot, namespaceID))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// RemoveRestoredDefinitions removes the file once its definitions are adopted.
func RemoveRestoredDefinitions(dataRoot, namespaceID string) error {
	err := os.Remove(RestoredDefinitionsPath(dataRoot, namespaceID))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}
