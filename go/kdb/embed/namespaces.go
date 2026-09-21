package embed

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ListNamespaces returns every namespace that exists under dataRoot, sorted.
//
// A namespace is a directory under dataRoot/ns holding a meta.json - the marker ensureNamespaceDirs
// writes, and the same one namespaceDirExists consults to decide whether a namespace is
// pre-existing. Namespace ids contain '/' (a catalog segment and a name, "zolik/matches"), so
// this is a walk rather than a single ReadDir, and the id is the path relative to dataRoot/ns
// with separators normalised back to slashes.
//
// This is what makes whole-database maintenance possible: with several namespaces under one root
// and one lock over all of them, tooling can operate on the database rather than on whichever
// namespace it was told about. A missing ns directory is not an error - an empty data root has no
// namespaces yet, which is a fact, not a failure.
func ListNamespaces(dataRoot string) ([]string, error) {
	nsRoot := filepath.Join(dataRoot, "ns")
	info, err := os.Stat(nsRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if !info.IsDir() {
		return nil, nil
	}

	var out []string
	err = filepath.WalkDir(nsRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		if _, statErr := os.Stat(filepath.Join(path, "meta.json")); statErr != nil {
			return nil
		}
		rel, relErr := filepath.Rel(nsRoot, path)
		if relErr != nil || rel == "." {
			return nil
		}
		out = append(out, filepath.ToSlash(rel))
		// A namespace directory holds delta/ and meta/, never another namespace, so there is
		// nothing below it worth walking.
		return fs.SkipDir
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

// CatalogsOf groups namespace ids by their catalog segment, for tooling that reports per catalog.
func CatalogsOf(namespaces []string) map[string][]string {
	out := make(map[string][]string)
	for _, ns := range namespaces {
		cat := ns
		if i := strings.IndexByte(ns, '/'); i >= 0 {
			cat = ns[:i]
		}
		out[cat] = append(out[cat], ns)
	}
	return out
}

// ValidateNamespaceID refuses a namespace id that is not safe to turn into a directory under the
// data root: empty, too long, a segment that is empty, "." or "..", or a character outside
// [A-Za-z0-9._-]. Namespaces beginning with "_" are reserved for the process itself (the
// "_system/..." auth registry) and refused too.
//
// Needed wherever a namespace id arrives from a client rather than from configuration: opening a
// namespace creates its directory, so an unchecked id is a path a remote caller chose.
func ValidateNamespaceID(id string) error {
	if id == "" || len(id) > 200 {
		return fmt.Errorf("kdb: invalid namespace id %q: must be 1-200 characters", id)
	}
	if strings.HasPrefix(id, "_") {
		return fmt.Errorf("kdb: invalid namespace id %q: names beginning with '_' are reserved", id)
	}
	for _, seg := range strings.Split(id, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("kdb: invalid namespace id %q: empty, '.' or '..' segment", id)
		}
		for _, r := range seg {
			ok := r == '.' || r == '_' || r == '-' ||
				(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
			if !ok {
				return fmt.Errorf("kdb: invalid namespace id %q: character %q is not allowed", id, r)
			}
		}
	}
	return nil
}

// NamespaceExists reports whether namespaceID has a directory under dataRoot. The id must already
// have passed ValidateNamespaceID.
func NamespaceExists(dataRoot, namespaceID string) bool {
	return namespaceDirExists(dataRoot, namespaceID)
}
