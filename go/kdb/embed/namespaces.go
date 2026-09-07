package embed

import (
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
