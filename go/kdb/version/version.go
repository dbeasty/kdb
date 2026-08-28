// Package version holds the single build-time version string shared by every KDB binary
// (kdb, kdb-service, kdb-inspect). The default marks a from-source build; release builds
// override it via:
//
//	go build -ldflags "-X github.com/limidus/kdb/go/kdb/version.Version=v1.2.3"
//
// The Makefile's build-go target injects the repo's VERSION file automatically. The string is
// surfaced by each binary's --version flag, the service's startup banner, and the admin
// endpoint's /healthz response.
package version

// Version is the build version, overridden at link time for releases.
var Version = "0.0.0-dev"
