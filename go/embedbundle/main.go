// Command embedbundle builds the embeddable Go source bundle described in
// docs/kdb-release-plan.md: a self-contained, compiling, testable copy of the packages an
// in-process consumer of KDB can reach, zipped for release.
//
// Usage (run from the go/ module root):
//
//	go run ./embedbundle -version 0.1.0 -commit <sha> -date 2026-09-01T00:00:00Z -out ../dist/bundle
//
// The scope is never hand-maintained: it is the transitive closure of `go list -deps` over the
// entry points in entrypoints.txt, unioned across every GOOS/GOARCH in platforms so build-tagged
// platform siblings (e.g. dir_lock_other.go) are never silently dropped by a single-platform
// `go list`.
package main

import (
	"archive/zip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// platforms is the union closure's coverage: every OS/ARCH the bundle claims to support must be
// listed here, or a package only imported on that platform can be silently missed.
var platforms = []struct{ os, arch string }{
	{"linux", "amd64"},
	{"linux", "arm64"},
	{"darwin", "arm64"},
	{"windows", "amd64"},
	{"js", "wasm"},
}

const bundleModulePkg = "root" // package name of the module root (go/doc.go); bundleinfo.go joins it.

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "embedbundle:", err)
		os.Exit(1)
	}
}

func run() error {
	version := flag.String("version", "", "release version, e.g. 0.1.0 (required)")
	commit := flag.String("commit", "unknown", "full git commit SHA the bundle was cut from")
	dateStr := flag.String("date", "", "RFC3339 UTC build date; defaults to now (required for a reproducible build)")
	out := flag.String("out", "dist/bundle", "output directory to stage and zip into")
	entrypointsFile := flag.String("entrypoints", "embedbundle/entrypoints.txt", "path to the entry-point list")
	flag.Parse()

	if *version == "" {
		return fmt.Errorf("-version is required")
	}
	date := time.Now().UTC()
	if *dateStr != "" {
		d, err := time.Parse(time.RFC3339, *dateStr)
		if err != nil {
			return fmt.Errorf("-date: %w", err)
		}
		date = d.UTC()
	}

	moduleDir, err := os.Getwd()
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(moduleDir, "go.mod")); err != nil {
		return fmt.Errorf("must be run from the go/ module root (no go.mod here): %w", err)
	}
	modulePath, err := goOutput(moduleDir, "list", "-m")
	if err != nil {
		return fmt.Errorf("go list -m: %w", err)
	}
	modulePath = strings.TrimSpace(modulePath)

	entryPoints, err := readEntrypoints(*entrypointsFile)
	if err != nil {
		return err
	}
	fmt.Printf("entry points: %v\n", entryPoints)

	pkgs, err := closure(moduleDir, modulePath, entryPoints)
	if err != nil {
		return err
	}
	fmt.Printf("closure: %d packages\n", len(pkgs))

	bundleName := fmt.Sprintf("kdb-go-embed-%s", *version)
	stageRoot := filepath.Join(*out, bundleName)
	if err := os.RemoveAll(stageRoot); err != nil {
		return err
	}
	if err := os.MkdirAll(stageRoot, 0o755); err != nil {
		return err
	}

	if err := copyPackages(moduleDir, stageRoot, pkgs); err != nil {
		return err
	}
	if err := copyModFiles(moduleDir, stageRoot); err != nil {
		return err
	}
	if err := writeBundleInfo(stageRoot, *version, *commit, date, entryPoints); err != nil {
		return err
	}
	if err := writeEmbedding(stageRoot, *version, entryPoints, pkgs); err != nil {
		return err
	}

	fmt.Println("go mod tidy...")
	if err := tidy(stageRoot); err != nil {
		return err
	}

	manifest, err := buildManifest(stageRoot, *version, *commit, date, entryPoints, pkgs)
	if err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(stageRoot, "bundle.json"), manifest); err != nil {
		return err
	}

	zipPath := filepath.Join(*out, bundleName+".zip")
	if err := writeDeterministicZip(*out, bundleName, zipPath, date); err != nil {
		return err
	}
	// The staged directory was scratch for building the archive; only the archive itself is a
	// release artifact. Leaving it behind would double up every file in the release's checksums
	// and dist listing, and `make bundle-verify` always re-extracts from the zip anyway.
	if err := os.RemoveAll(stageRoot); err != nil {
		return err
	}
	fmt.Println("wrote", zipPath)
	return nil
}

// readEntrypoints parses entrypoints.txt: one import path (relative to the module root) per
// line, "#" starts a trailing or whole-line comment, blank lines ignored.
func readEntrypoints(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	var out []string
	for _, line := range strings.Split(string(data), "\n") {
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s: no entry points found", path)
	}
	return out, nil
}

// closure computes the union, across every platform in platforms, of `go list -deps` over
// entryPoints, restricted to packages under modulePath. It also verifies each platform actually
// resolves (a broken build tag combination fails loudly here, not at extraction time).
func closure(moduleDir, modulePath string, entryPoints []string) ([]string, error) {
	set := map[string]bool{}
	relArgs := make([]string, len(entryPoints))
	for i, e := range entryPoints {
		relArgs[i] = "./" + e
	}
	args := append([]string{"list", "-deps"}, relArgs...)
	for _, p := range platforms {
		cmd := exec.Command("go", args...)
		cmd.Dir = moduleDir
		cmd.Env = append(os.Environ(), "GOOS="+p.os, "GOARCH="+p.arch)
		outBytes, err := cmd.Output()
		if err != nil {
			return nil, fmt.Errorf("go list -deps (GOOS=%s GOARCH=%s): %w", p.os, p.arch, wrapStderr(err))
		}
		for _, line := range strings.Split(strings.TrimSpace(string(outBytes)), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			if line == modulePath || strings.HasPrefix(line, modulePath+"/") {
				set[line] = true
			}
		}
	}
	pkgs := make([]string, 0, len(set))
	for p := range set {
		pkgs = append(pkgs, p)
	}
	sort.Strings(pkgs)
	return pkgs, nil
}

func wrapStderr(err error) error {
	if ee, ok := err.(*exec.ExitError); ok {
		return fmt.Errorf("%w\n%s", err, ee.Stderr)
	}
	return err
}

// copyPackages copies each package's whole directory (every file, not just what `go list`
// reports for one platform), keyed by import path relative to modulePath.
func copyPackages(moduleDir, stageRoot string, pkgs []string) error {
	modulePath, err := goOutput(moduleDir, "list", "-m")
	if err != nil {
		return err
	}
	modulePath = strings.TrimSpace(modulePath)
	for _, pkg := range pkgs {
		rel := strings.TrimPrefix(pkg, modulePath)
		rel = strings.TrimPrefix(rel, "/")
		if rel == "" {
			continue // the module root package (kdb/embed etc. never resolves to "")
		}
		src := filepath.Join(moduleDir, filepath.FromSlash(rel))
		dst := filepath.Join(stageRoot, filepath.FromSlash(rel))
		if err := copyDir(src, dst); err != nil {
			return fmt.Errorf("copying %s: %w", pkg, err)
		}
	}
	return nil
}

// copyDir copies the files directly inside src (not subdirectories - those are separate Go
// packages and, if reachable, are already in the closure with their own copyDir call).
func copyDir(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if err := copyFile(filepath.Join(src, e.Name()), filepath.Join(dst, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o644)
}

// copyModFiles copies go.mod, go.sum and the repo LICENSE, and strips the gobind tool
// directive - it exists to support golang.org/x/mobile bindings for the wasm demo, which the
// bundle doesn't include, and dragging it along pulls x/mobile, x/mod and x/tools into the
// bundle's dependency graph for nothing. `go mod tidy` (tidy, called after this) prunes the rest.
func copyModFiles(moduleDir, stageRoot string) error {
	modData, err := os.ReadFile(filepath.Join(moduleDir, "go.mod"))
	if err != nil {
		return err
	}
	var kept []string
	for _, line := range strings.Split(string(modData), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "tool golang.org/x/mobile") {
			continue
		}
		kept = append(kept, line)
	}
	if err := os.WriteFile(filepath.Join(stageRoot, "go.mod"), []byte(strings.Join(kept, "\n")), 0o644); err != nil {
		return err
	}
	if err := copyFile(filepath.Join(moduleDir, "go.sum"), filepath.Join(stageRoot, "go.sum")); err != nil {
		return err
	}
	repoRoot := filepath.Dir(moduleDir)
	if err := copyFile(filepath.Join(repoRoot, "LICENSE"), filepath.Join(stageRoot, "LICENSE")); err != nil {
		return err
	}
	return nil
}

func tidy(stageRoot string) error {
	cmd := exec.Command("go", "mod", "tidy")
	cmd.Dir = stageRoot
	cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod")
	outBytes, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("go mod tidy: %w\n%s", err, outBytes)
	}
	return nil
}

func writeBundleInfo(stageRoot, version, commit string, date time.Time, entryPoints []string) error {
	var eps strings.Builder
	for i, e := range entryPoints {
		if i > 0 {
			eps.WriteString(", ")
		}
		fmt.Fprintf(&eps, "%q", strings.TrimSpace(strings.SplitN(e, "#", 2)[0]))
	}
	content := fmt.Sprintf(`// Code generated by go/embedbundle. DO NOT EDIT.
//
// bundleinfo.go identifies exactly which release drop of the embeddable KDB source this is, so
// a consumer that vendors it can report which version it linked without cross-checking bundle.json.
package %s

// Version, Commit and BuildDate identify this bundle drop (docs/kdb-release-plan.md).
const (
	BundleVersion   = %q
	BundleCommit    = %q
	BundleBuildDate = %q
)

// BundleEntryPoints are the packages the bundle's dependency closure was computed from.
var BundleEntryPoints = []string{%s}
`, bundleModulePkg, version, commit, date.Format(time.RFC3339), eps.String())
	return os.WriteFile(filepath.Join(stageRoot, "bundleinfo.go"), []byte(content), 0o644)
}

func writeEmbedding(stageRoot, version string, entryPoints, pkgs []string) error {
	var pkgList strings.Builder
	for _, p := range pkgs {
		fmt.Fprintf(&pkgList, "- `%s`\n", p)
	}
	var epList strings.Builder
	for _, e := range entryPoints {
		fmt.Fprintf(&epList, "- `%s`\n", strings.TrimSpace(strings.SplitN(e, "#", 2)[0]))
	}
	content := fmt.Sprintf(`# Embedding KDB (Go)

This is a self-contained copy of the KDB Go engine — the subset of
github.com/limidus/kdb/go that an in-process consumer can reach. See
docs/kdb-release-plan.md in the main repository for how this bundle is built and how to
reproduce it from a release tag.

Bundle version %s. See bundle.json for the exact commit, build date and per-file checksums,
and bundleinfo.go for the same identity as importable Go constants (import the module root as
"github.com/limidus/kdb/go", then root.BundleVersion etc.).

Pre-1.0: this is a %s release. The embedding API can change between releases; pin a specific
bundle version rather than tracking the source repository's main branch.

## Use it

	unzip kdb-go-embed-%s.zip -d third_party/
	go mod edit -replace github.com/limidus/kdb/go=./third_party/kdb-go-embed-%s
	go mod tidy

	import (
	    "database/sql"
	    _ "github.com/limidus/kdb/go/kdb/driver"
	)

rewrite-module.sh is included for projects that would rather re-home the code under their own
module path instead of using the replace directive above.

## Entry points

%s
## What's included (%d packages)

The full transitive dependency closure of the entry points above, computed with
go list -deps and unioned across linux/amd64, linux/arm64, darwin/arm64, windows/amd64 and
js/wasm so no platform-specific file is silently dropped.

%s
## What's excluded

Everything only reachable from the server, native transport, wire protocol, CLI, peer sync,
backup/recovery/integrity tooling, or the kdb-inspect/kdb-interop test harnesses — none of it is
importable by an embedder of kdb/embed, kdb/driver, kdb/sql, kdb/query/hybrid or kdb/index.
`, version, version, version, version, epList.String(), len(pkgs), pkgList.String())
	if err := os.WriteFile(filepath.Join(stageRoot, "EMBEDDING.md"), []byte(content), 0o644); err != nil {
		return err
	}
	return writeRewriteScript(stageRoot)
}

func writeRewriteScript(stageRoot string) error {
	content := `#!/usr/bin/env bash
# Re-home this bundle under a different Go module path, for a project that would rather import
# it as its own module than use a "replace" directive (see EMBEDDING.md for that simpler path).
#
# Usage: ./rewrite-module.sh github.com/yourorg/yourproject/third_party/kdb
set -euo pipefail
old="github.com/limidus/kdb/go"
new="${1:?usage: rewrite-module.sh <new-module-path>}"
go mod edit -module "$new"
grep -rl "$old" --include='*.go' . | xargs sed -i.bak "s#$old#$new#g"
find . -name '*.bak' -delete
go mod tidy
echo "rewritten to module $new"
`
	return os.WriteFile(filepath.Join(stageRoot, "rewrite-module.sh"), []byte(content), 0o755)
}

type bundleManifest struct {
	Version     string            `json:"version"`
	Commit      string            `json:"commit"`
	BuildDate   string            `json:"buildDate"`
	EntryPoints []string          `json:"entryPoints"`
	Packages    []string          `json:"packages"`
	Files       map[string]string `json:"files"` // relative path -> sha256
}

func buildManifest(stageRoot, version, commit string, date time.Time, entryPoints, pkgs []string) (*bundleManifest, error) {
	var eps []string
	for _, e := range entryPoints {
		eps = append(eps, strings.TrimSpace(strings.SplitN(e, "#", 2)[0]))
	}
	files := map[string]string{}
	err := filepath.Walk(stageRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, err := filepath.Rel(stageRoot, path)
		if err != nil {
			return err
		}
		sum, err := sha256File(path)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(rel)] = sum
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &bundleManifest{
		Version:     version,
		Commit:      commit,
		BuildDate:   date.Format(time.RFC3339),
		EntryPoints: eps,
		Packages:    pkgs,
		Files:       files,
	}, nil
}

func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

// writeDeterministicZip zips bundleName's directory (found inside stageParent) into zipPath with
// a fixed entry order and a fixed modified time, so two builds of the same source produce a
// byte-identical archive regardless of when or on what machine they run (docs/kdb-release-plan.md
// §3). The standard `zip` CLI does not give this guarantee - entry order and mtimes vary - hence
// building it by hand with archive/zip.
func writeDeterministicZip(stageParent, bundleName, zipPath string, modTime time.Time) error {
	root := filepath.Join(stageParent, bundleName)
	var paths []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(paths)

	f, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	defer f.Close()
	zw := zip.NewWriter(f)
	for _, path := range paths {
		rel, err := filepath.Rel(stageParent, path)
		if err != nil {
			return err
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		hdr, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		hdr.Method = zip.Deflate
		hdr.Modified = modTime
		// Executable scripts (rewrite-module.sh) keep 0755; everything else is 0644. Fixed,
		// not copied from the filesystem, so umask differences between build hosts can't
		// perturb the archive.
		mode := os.FileMode(0o644)
		if strings.HasSuffix(hdr.Name, ".sh") {
			mode = 0o755
		}
		hdr.SetMode(mode)
		w, err := zw.CreateHeader(hdr)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	return zw.Close()
}

func goOutput(dir string, args ...string) (string, error) {
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	b, err := cmd.Output()
	if err != nil {
		return "", wrapStderr(err)
	}
	return string(b), nil
}
