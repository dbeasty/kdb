package main

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestClosureCoversEntryPointsWithoutCgo computes the bundle's package closure the same way a
// release does and checks the two properties the bundle depends on: every entry point in
// entrypoints.txt is in it, and no package in it has a cgo file on any platform (the bundle is
// cross-compiled with CGO_ENABLED=0 by scripts/verify-bundle.sh). The package and file counts
// are deliberately not asserted - they change with every package added and live in
// docs/kdb-release-plan.md §2.
func TestClosureCoversEntryPointsWithoutCgo(t *testing.T) {
	if testing.Short() {
		t.Skip("runs `go list -deps` once per platform")
	}
	moduleDir, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	modulePath, err := goOutput(moduleDir, "list", "-m")
	if err != nil {
		t.Fatalf("go list -m: %v", err)
	}
	modulePath = strings.TrimSpace(modulePath)

	entryPoints, err := readEntrypoints("entrypoints.txt")
	if err != nil {
		t.Fatal(err)
	}
	pkgs, err := closure(moduleDir, modulePath, entryPoints)
	if err != nil {
		t.Fatal(err)
	}

	inClosure := make(map[string]bool, len(pkgs))
	for _, p := range pkgs {
		inClosure[p] = true
	}
	for _, e := range entryPoints {
		if !inClosure[modulePath+"/"+e] {
			t.Errorf("entry point %s is not in the computed closure", e)
		}
	}

	// Check every .go file in each package directory, not just the files `go list` picks for
	// one platform: copyPackages ships the whole directory, so a cgo file behind any build tag
	// would ship too.
	for _, pkg := range pkgs {
		rel := strings.TrimPrefix(strings.TrimPrefix(pkg, modulePath), "/")
		dir := filepath.Join(moduleDir, filepath.FromSlash(rel))
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
				continue
			}
			path := filepath.Join(dir, e.Name())
			f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatalf("parsing %s: %v", path, err)
			}
			for _, imp := range f.Imports {
				if p, _ := strconv.Unquote(imp.Path.Value); p == "C" {
					t.Errorf("%s (package %s, in the bundle closure) uses cgo", path, pkg)
				}
			}
		}
	}
}
